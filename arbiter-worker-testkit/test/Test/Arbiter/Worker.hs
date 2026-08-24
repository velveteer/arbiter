{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Worker (spec) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types
  ( JobRead
  , ObservabilityHooks (..)
  , attempts
  , claimSeq
  , claimedBy
  , defaultJob
  , defaultObservabilityHooks
  , payload
  , primaryKey
  , setGroupKey
  , setMaxAttempts
  )
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (JobHandler, executeStatement, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Core.Queues qualified as Q
import Arbiter.Core.Sql.Query (raw)
import Arbiter.Core.Worker qualified as WR
import Arbiter.Simple
  ( SimpleConnectionPool (..)
  , SimpleDb
  , SimpleEnv (..)
  , createSimpleEnvWithPool
  , disableListener
  , runSimpleDb
  )
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Test.Setup (cleanupData, createSharedPool, execute_, setupOnce)
import Arbiter.Worker (mergedChildResults, runReaperOp, runWorkerPool)
import Arbiter.Worker.Config
  ( WorkerConfig (..)
  , ackAll
  , ackAllWith
  , ackWith
  , defaultBatchedWorkerConfig
  , getListenerReady
  , getWorkerState
  , nack
  , shutdownWorker
  , transactionalWorkerConfig
  )
import Arbiter.Worker.Logger (silentLogConfig)
import Arbiter.Worker.WorkerState (WorkerState (..))
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, throwIO, try)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.Either (isRight)
import Data.Foldable (for_, toList, traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust, fromMaybe, isJust, isNothing)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (diffUTCTime, getCurrentTime)
import Data.UUID.V4 qualified as UUID
import Database.PostgreSQL.Simple (Only (..), execute, query)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Notification (Notification (..), getNotification)
import System.Directory qualified as Dir
import System.Timeout (timeout)
import Test.Hspec
  ( Spec
  , around
  , beforeAll
  , describe
  , expectationFailure
  , it
  , runIO
  , shouldBe
  , shouldContain
  , shouldMatchList
  , shouldNotContain
  , shouldReturn
  , shouldSatisfy
  )
import UnliftIO.Async (withAsync)
import UnliftIO.Async qualified as Async

import Arbiter.Worker.TestKit (workerSpec)

type WorkerTestRegistry = '[QueueWithResult "arbiter_worker_test" WorkerTestPayload (Maybe [Text])]

noResult :: (Monad n, Monoid r) => (c -> j -> n ()) -> c -> j -> n r
noResult h conn job = h conn job >> pure mempty

testSchema :: Text
testSchema = "arbiter_worker_test"

testTable :: Text
testTable = "arbiter_worker_test"

-- | Take a job's row lock on a connection of the test's own.
lockJobRow :: PG.Connection -> Int64 -> IO (Either SomeException [Only Int64])
lockJobRow conn jobId = try (PG.query conn lockSql (Only jobId))
  where
    lockSql = fromString . T.unpack $ "SELECT id FROM " <> testSchema <> "." <> testTable <> " WHERE id = ? FOR UPDATE"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable True) $ do
  sharedPool <- runIO (createSharedPool connStr)
  sharedEnv <- runIO (createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema)
  around (withPool sharedEnv) $ do
    workerSpec @WorkerTestPayload
      SimpleTask
      FailingTask
      (\f _conn job -> f job)
      runSimpleDb

    describe "Reaper op bounding" $ do
      it "completes an op longer than the timeout when each statement is within it" $ \env -> do
        let sleep = void $ executeStatement (raw "DO $$ BEGIN PERFORM pg_sleep(0.4); END $$")
        r <-
          runSimpleDb env $
            runReaperOp silentLogConfig testSchema 1 "test-reaper-slow-op" 0 $ do
              sleep
              sleep
              sleep
              pure (42 :: Int)
        r `shouldBe` Just 42
      it "aborts a stuck statement at the timeout without killing the caller" $ \env -> do
        r <-
          runSimpleDb env
            $ runReaperOp silentLogConfig testSchema 0.5 "test-reaper-stuck-op" 0
            $ executeStatement (raw "DO $$ BEGIN PERFORM pg_sleep(5); END $$")
        r `shouldBe` Nothing

    describe "Transactional Atomicity" $ do
      it "rolls back user operations when handler fails" $ \env -> withTestOpsTable env $ do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler conn job = do
              liftIO
                $ void
                $ execute
                  conn
                  (fromString . T.unpack $ "INSERT INTO " <> testSchema <> ".test_operations (job_id, operation) VALUES (?, ?)")
                  (primaryKey job, "processed" :: Text)
              throwRetryable "Simulated failure"

        void
          $ runSimpleDb env
          $ HL.insertJob
          $ setMaxAttempts (Just 1)
          $ setGroupKey (Just "g1")
          $ defaultJob (SimpleTask "WillFail")

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 (noResult handler)

        withLinkedAsync
          (runSimpleDb env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1})
          $ \_ -> do
            waitUntil 10_000 $ do
              dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
              pure (length dlqJobs == 1)

            dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            length dlqJobs `shouldBe` 1

            count <- queryOpsCount env
            count `shouldBe` 0

      it "commits user operations when handler succeeds" $ \env -> withTestOpsTable env $ do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler conn job = do
              liftIO
                $ void
                $ execute
                  conn
                  (fromString . T.unpack $ "INSERT INTO " <> testSchema <> ".test_operations (job_id, operation) VALUES (?, ?)")
                  (primaryKey job, "processed" :: Text)
              pure ()

        void $ runSimpleDb env $ HL.insertJob $ setGroupKey (Just "g1") $ defaultJob (SimpleTask "WillSucceed")

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 (noResult handler)

        withLinkedAsync
          (runSimpleDb env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1})
          $ \_ -> do
            waitUntil 10_000 $ (== 1) <$> queryOpsCount env

            count <- queryOpsCount env
            count `shouldBe` 1

      it "manual commit inside handler persists despite subsequent failure" $ \env -> withTestOpsTable env $ do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler conn job = do
              liftIO
                $ void
                $ execute
                  conn
                  (fromString . T.unpack $ "INSERT INTO " <> testSchema <> ".test_operations (job_id, operation) VALUES (?, ?)")
                  (primaryKey job, "processed" :: Text)
              -- User manually commits the transaction (violates our transaction semantics)
              liftIO $ PG.commit conn
              throwRetryable "Simulated failure after commit"

        void
          $ runSimpleDb env
          $ HL.insertJob
          $ setMaxAttempts (Just 1)
          $ setGroupKey (Just "g1")
          $ defaultJob (SimpleTask "ManualCommit")

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 (noResult handler)

        withLinkedAsync
          (runSimpleDb env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1})
          $ \_ -> do
            waitUntil 10_000 $ do
              dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
              pure (length dlqJobs == 1)

            dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            length dlqJobs `shouldBe` 1

            -- User's manual commit survives despite handler failure
            count <- queryOpsCount env
            count `shouldBe` 1

    describe "Graceful Shutdown" $ do
      it "graceful shutdown waits for in-flight jobs to complete" $ \env -> do
        -- Track job completion
        completedRef <- newIORef False
        startedRef <- newIORef False

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              liftIO $ atomicModifyIORef' startedRef $ \_ -> (True, ())
              -- Simulate long-running job
              liftIO $ threadDelay 2_000_000
              liftIO $ atomicModifyIORef' completedRef $ \_ -> (True, ())
              pure ()

        -- Insert a job
        let job = setGroupKey (Just "g1") $ defaultJob (SimpleTask "LongJob")
        void $ runSimpleDb env $ HL.insertJob job

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 (noResult handler)

        let configWithTimeout =
              config
                { workerCount = 1
                , pollInterval = 0.1
                , gracefulShutdownTimeout = Just 10 -- 10 second timeout (plenty of time)
                }

        -- Start worker and wait for job to start processing
        withLinkedAsync (runSimpleDb env $ runWorkerPool configWithTimeout) $ \worker -> do
          -- Wait for job to start
          waitUntil 10_000 $ readIORef startedRef

          -- Trigger shutdown while job is running
          shutdownWorker configWithTimeout

          -- Wait for worker to exit (should complete job first)
          Async.wait worker

          -- Verify job completed despite shutdown
          completed <- readIORef completedRef
          completed `shouldBe` True

      it "graceful shutdown times out if jobs take too long" $ \env -> do
        -- Track if job started and completed
        startedRef <- newIORef False
        completedRef <- newIORef False

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              liftIO $ atomicModifyIORef' startedRef $ \_ -> (True, ())
              -- Very long running job that exceeds timeout
              liftIO $ threadDelay 10_000_000 -- 10 seconds
              liftIO $ atomicModifyIORef' completedRef $ \_ -> (True, ())
              pure ()

        -- Insert a job
        let job = setGroupKey (Just "g1") $ defaultJob (SimpleTask "VeryLongJob")
        void $ runSimpleDb env $ HL.insertJob job

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 (noResult handler)

        let configWithShortTimeout =
              config
                { workerCount = 1
                , pollInterval = 0.05 -- Faster polling for test
                , gracefulShutdownTimeout = Just 1 -- Only 1 second timeout
                }

        withLinkedAsync (runSimpleDb env $ runWorkerPool configWithShortTimeout) $ \worker -> do
          -- Wait for job to actually start processing
          waitUntil 10_000 $ readIORef startedRef

          -- Measure just the shutdown duration, not startup/claim overhead
          startTime <- liftIO getCurrentTime
          shutdownWorker configWithShortTimeout
          Async.wait worker
          endTime <- liftIO getCurrentTime

          let elapsed = diffUTCTime endTime startTime
          -- Shutdown should take ~1s (the graceful timeout), not 10s (the job duration)
          elapsed `shouldSatisfy` (< 5)

        -- Job should NOT have completed (we timed out and cancelled it)
        completed <- readIORef completedRef
        completed `shouldBe` False

    describe "Liveness Probe" $ do
      it "creates a health check file when liveness is enabled" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ threadDelay 500_000

        -- Get system temp directory and create liveness file path
        tmpDir <- Dir.getTemporaryDirectory
        let livenessPath = tmpDir <> "/arbiter-test-liveness"

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 (noResult handler)
        let configWithLiveness =
              config
                { livenessFile = Just livenessPath
                , workerCount = 1
                , pollInterval = 0.1
                }

        withLinkedAsync (runSimpleDb env $ runWorkerPool configWithLiveness) $ \worker -> do
          -- Wait for liveness probe to create the file
          waitUntil 10_000 $ Dir.doesFileExist livenessPath

          -- Check that the specific liveness file was created
          exists <- Dir.doesFileExist livenessPath
          exists `shouldBe` True

          -- Shutdown and verify cleanup
          shutdownWorker configWithLiveness
          _ <- Async.waitCatch worker
          waitUntil 5_000 $ not <$> Dir.doesFileExist livenessPath
          cleaned <- Dir.doesFileExist livenessPath
          cleaned `shouldBe` False

    describe "Batched Job Mode" $ do
      it "ackAll skips a job reclaimed mid-batch" $ \env -> do
        successRef <- newIORef ([] :: [WorkerTestPayload])
        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \xs -> (payload job : xs, ())
                }
        let batchHandler jobs cbs = do
              let js = toList jobs
              -- Simulate a reclaim of "ca-stolen": a claim bumps both counters, so the bulk ack won't match it.
              liftIO $
                traverse_
                  ( \j ->
                      when (payload j == SimpleTask "ca-stolen") $ do
                        let pool = fromJust (connectionPool (simplePool env))
                        withResource pool $ \conn ->
                          void $
                            execute
                              conn
                              ( fromString . T.unpack $
                                  "UPDATE "
                                    <> Schema.jobQueueTable testSchema testTable
                                    <> " SET attempts = attempts + 1, claim_seq = claim_seq + 1 WHERE id = ?"
                              )
                              (Only (primaryKey j))
                  )
                  js
              ackAll cbs js
        let jobs =
              [ setGroupKey (Just "ca") $ defaultJob (SimpleTask "ca-keep1")
              , setGroupKey (Just "ca") $ defaultJob (SimpleTask "ca-stolen")
              , setGroupKey (Just "ca") $ defaultJob (SimpleTask "ca-keep2")
              ]
        void $ runSimpleDb env $ HL.insertJobsBatch jobs
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          defaultBatchedWorkerConfig 1 10 batchHandler
        withLinkedAsync (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.05, observabilityHooks = hooks})) $ \_ -> do
          waitUntil 10_000 $ (== 2) . length <$> readIORef successRef
          successes <- readIORef successRef
          -- onJobSuccess fired only for the survivors. The reclaimed job was skipped.
          successes `shouldMatchList` [SimpleTask "ca-keep1", SimpleTask "ca-keep2"]

      it "rollup ackAllWith stores each job's result for the parent" $ \env -> do
        finalRef <- newIORef ([] :: [Text])
        let resultFor :: WorkerTestPayload -> Maybe [Text]
            resultFor p = case p of
              SimpleTask "rb-ca" -> Just ["alpha"]
              SimpleTask "rb-cb" -> Just ["beta"]
              _ -> Nothing
            isReducer p = case p of SimpleTask "rb-reducer" -> True; _ -> False
            handler jobs cbs =
              if all (isReducer . payload) jobs
                then
                  traverse_
                    ( \j -> do
                        (merged, _dlq) <- mergedChildResults j
                        liftIO $ atomicModifyIORef' finalRef $ \_ -> (fromMaybe [] merged, ())
                        ackWith cbs j merged
                    )
                    (toList jobs)
                else ackAllWith cbs (map (\j -> (j, resultFor (payload j))) (toList jobs))
        runSimpleDb env
          $ void
          $ HL.insertJobTree
          $ defaultJob (SimpleTask "rb-reducer")
            <~~ (defaultJob (SimpleTask "rb-ca") :| [defaultJob (SimpleTask "rb-cb")])
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          defaultBatchedWorkerConfig 1 10 handler
        withLinkedAsync (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.1})) $ \_ -> do
          waitUntil 10_000 $ (== 2) . length <$> readIORef finalRef
          final <- readIORef finalRef
          final `shouldMatchList` ["alpha", "beta"]

    describe "Fan-out/fan-in with rollup" $ do
      it "worker auto-appends handler results; finalizer reads merged state" $ \env -> do
        finalResultRef <- newIORef ([] :: [Text])

        let handler _conn job = case payload job of
              SimpleTask "mapper-a" -> pure (Just ["sales", "growth"])
              SimpleTask "mapper-b" -> pure (Just ["revenue"])
              SimpleTask "mapper-c" -> pure (Just ["forecast", "trend"])
              SimpleTask "reducer" -> do
                (merged, _dlq) <- mergedChildResults job
                liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                pure merged
              _ -> pure Nothing

        -- Insert the rollup tree
        runSimpleDb env
          $ void
          $ HL.insertJobTree
          $ defaultJob (SimpleTask "reducer")
            <~~ ( defaultJob (SimpleTask "mapper-a")
                    :| [ defaultJob (SimpleTask "mapper-b")
                       , defaultJob (SimpleTask "mapper-c")
                       ]
                )

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 handler

        withLinkedAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 3
                    , pollInterval = 0.1
                    }
                )
          )
          $ \_ -> do
            waitUntil 10_000 $ (== 5) . length <$> readIORef finalResultRef

            -- The reducer handler should have received all 5 words
            finalResult <- readIORef finalResultRef
            length finalResult `shouldBe` 5
            finalResult `shouldMatchList` ["sales", "growth", "revenue", "forecast", "trend"]

      it "nested rollup: section finalizers explicitly merge and propagate" $ \env -> do
        --   root (rollup) ← merges section results
        --   ├── section-1 (rollup) ← merges mapper results, returns merged
        --   │   ├── mapper-1a  → ["sales", "growth"]
        --   │   └── mapper-1b  → ["revenue"]
        --   └── section-2 (rollup) ← merges mapper results, returns merged
        --       ├── mapper-2a  → ["forecast"]
        --       └── mapper-2b  → ["trend"]
        finalResultRef <- newIORef ([] :: [Text])

        let handler _conn job = case payload job of
              SimpleTask "mapper-1a" -> pure (Just ["sales", "growth"])
              SimpleTask "mapper-1b" -> pure (Just ["revenue"])
              SimpleTask "mapper-2a" -> pure (Just ["forecast"])
              SimpleTask "mapper-2b" -> pure (Just ["trend"])
              SimpleTask "section-1" -> fst <$> mergedChildResults job
              SimpleTask "section-2" -> fst <$> mergedChildResults job
              SimpleTask "root" -> do
                (merged, _dlq) <- mergedChildResults job
                liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                pure merged
              _ -> pure Nothing

        runSimpleDb env
          $ void
          $ HL.insertJobTree
          $ JT.rollup (defaultJob (SimpleTask "root"))
          $ ( defaultJob (SimpleTask "section-1")
                <~~ (defaultJob (SimpleTask "mapper-1a") :| [defaultJob (SimpleTask "mapper-1b")])
            )
            :| [ defaultJob (SimpleTask "section-2")
                   <~~ (defaultJob (SimpleTask "mapper-2a") :| [defaultJob (SimpleTask "mapper-2b")])
               ]

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 handler

        withLinkedAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 3
                    , pollInterval = 0.1
                    }
                )
          )
          $ \_ -> do
            waitUntil 15_000 $ (== 5) . length <$> readIORef finalResultRef

            -- Root handler should receive all 5 words merged from both sections
            finalResult <- readIORef finalResultRef
            length finalResult `shouldBe` 5
            finalResult
              `shouldMatchList` ["sales", "growth", "revenue", "forecast", "trend"]

      it "completing a rollup parent stores its result and acks" $ \env -> do
        finalResultRef <- newIORef ([] :: [Text])

        let handler (job :| _) cbs =
              case payload job of
                -- ackWith stores the child result for the parent and acks it
                SimpleTask "child-a" -> ackWith cbs job (Just ["alpha"])
                SimpleTask "child-b" -> ackWith cbs job (Just ["beta", "gamma"])
                SimpleTask "manual-reducer" -> do
                  (merged, _dlq) <- mergedChildResults job
                  liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                  ackWith cbs job merged
                _ -> ackWith cbs job Nothing

        -- Insert the rollup tree
        runSimpleDb env
          $ void
          $ HL.insertJobTree
          $ defaultJob (SimpleTask "manual-reducer")
            <~~ ( defaultJob (SimpleTask "child-a")
                    :| [defaultJob (SimpleTask "child-b")]
                )

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          defaultBatchedWorkerConfig 3 1 handler

        withLinkedAsync
          ( runSimpleDb env $
              runWorkerPool (config {pollInterval = 0.1})
          )
          $ \_ -> do
            waitUntil 10_000 $ (== 3) . length <$> readIORef finalResultRef

            -- Finalizer should have received child results
            finalResult <- readIORef finalResultRef
            length finalResult `shouldBe` 3
            finalResult `shouldMatchList` ["alpha", "beta", "gamma"]

      it "batched mode: two rollup parents claimed in one batch each get their own child results" $ \env -> do
        -- Two ungrouped rollup parents are batched together once their children
        -- complete. Each parent must receive only its own merged children, keyed by
        -- primary key, not a mix across the batch. No group keys are needed.
        receivedRef <- newIORef (Map.empty :: Map.Map Text [Text])
        batchSizeRef <- newIORef (0 :: Int)

        let isReducer p = case p of SimpleTask n -> "reducer" `T.isPrefixOf` n; _ -> False
            handler jobs cbs = do
              let reducerCount = length (filter (isReducer . payload) (toList jobs))
              when (reducerCount > 0) $
                liftIO $
                  atomicModifyIORef' batchSizeRef $
                    \n -> (max n reducerCount, ())
              for_ jobs $ \job -> case payload job of
                SimpleTask "child-1a" -> ackWith cbs job (Just ["a1"])
                SimpleTask "child-1b" -> ackWith cbs job (Just ["b1"])
                SimpleTask "child-2a" -> ackWith cbs job (Just ["a2"])
                SimpleTask "child-2b" -> ackWith cbs job (Just ["b2"])
                SimpleTask name -> do
                  (merged, _dlq) <- mergedChildResults job
                  liftIO $ atomicModifyIORef' receivedRef $ \m -> (Map.insert name (fromMaybe [] merged) m, ())
                  ackWith cbs job merged
                _ -> ackWith cbs job Nothing

        -- Two independent rollup trees, all ungrouped. The four children drain in
        -- one ungrouped batch, then both parents unblock and batch together.
        runSimpleDb env
          $ void
          $ HL.insertJobTree
          $ defaultJob (SimpleTask "reducer-1")
            <~~ (defaultJob (SimpleTask "child-1a") :| [defaultJob (SimpleTask "child-1b")])
        runSimpleDb env
          $ void
          $ HL.insertJobTree
          $ defaultJob (SimpleTask "reducer-2")
            <~~ (defaultJob (SimpleTask "child-2a") :| [defaultJob (SimpleTask "child-2b")])

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          defaultBatchedWorkerConfig 1 10 handler

        withLinkedAsync
          ( runSimpleDb env $
              runWorkerPool (config {pollInterval = 0.1})
          )
          $ \_ -> do
            waitUntil 10_000 $ (== 2) . Map.size <$> readIORef receivedRef

            received <- readIORef receivedRef
            Map.findWithDefault [] "reducer-1" received `shouldMatchList` ["a1", "b1"]
            Map.findWithDefault [] "reducer-2" received `shouldMatchList` ["a2", "b2"]

            -- Both parents were handled together in a single batch.
            batchSize <- readIORef batchSizeRef
            batchSize `shouldBe` 2

      it "DLQ snapshot round-trip preserves child results" $ \env -> do
        attemptRef <- newIORef (0 :: Int)
        finalResultRef <- newIORef ([] :: [Text])

        let handler _conn job = case payload job of
              SimpleTask "dlq-child-a" -> pure (Just ["x"])
              SimpleTask "dlq-child-b" -> pure (Just ["y", "z"])
              SimpleTask "dlq-reducer" -> do
                attempt <- liftIO $ atomicModifyIORef' attemptRef $ \n -> (n + 1, n + 1)
                if attempt == 1
                  then throwRetryable "Intentional failure on first attempt"
                  else do
                    (merged, _dlq) <- mergedChildResults job
                    liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                    pure merged
              _ -> pure Nothing

        -- Insert the rollup tree
        Right (_parent :| _children) <-
          runSimpleDb env
            $ HL.insertJobTree
            $ (setMaxAttempts (Just 1) $ defaultJob (SimpleTask "dlq-reducer"))
              <~~ ( defaultJob (SimpleTask "dlq-child-a")
                      :| [defaultJob (SimpleTask "dlq-child-b")]
                  )

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 handler

        let cfg =
              config
                { workerCount = 3
                , pollInterval = 0.1
                }

        -- Phase 1: Run workers - children succeed, reducer fails → DLQ
        withLinkedAsync (runSimpleDb env $ runWorkerPool cfg) $ \_ ->
          waitUntil 10_000 $ do
            dlqJobs <- runSimpleDb env $ HL.listDLQJobs @WorkerTestPayload 10 0
            pure $ any (\d -> payload (DLQ.jobSnapshot d) == SimpleTask "dlq-reducer") dlqJobs

        -- Verify reducer is in DLQ with snapshot
        dlqJobs <- runSimpleDb env $ HL.listDLQJobs @WorkerTestPayload 10 0
        let reducerDlq = filter (\d -> payload (DLQ.jobSnapshot d) == SimpleTask "dlq-reducer") dlqJobs
        length reducerDlq `shouldBe` 1

        -- Phase 2: Retry from DLQ - reducer should see preserved results from snapshot
        let dlqId = DLQ.dlqPrimaryKey (head reducerDlq)
        mRetried <- runSimpleDb env $ HL.retryFromDLQ @WorkerTestPayload dlqId
        case mRetried of
          Nothing -> expectationFailure "retryFromDLQ returned Nothing"
          Just retried -> payload retried `shouldBe` SimpleTask "dlq-reducer"

        withLinkedAsync (runSimpleDb env $ runWorkerPool cfg) $ \_ ->
          waitUntil 10_000 $ not . null <$> readIORef finalResultRef

        -- The retried reducer should have received the preserved child results
        finalResult <- readIORef finalResultRef
        length finalResult `shouldBe` 3
        finalResult `shouldMatchList` ["x", "y", "z"]

      it "DLQ tree recovery: both parent and child in DLQ" $ \env -> do
        attemptRef <- newIORef (0 :: Int)
        finalResultRef <- newIORef ([] :: [Text])

        let handler _conn job = case payload job of
              SimpleTask "recover-child-ok" -> pure (Just ["alpha"])
              SimpleTask "recover-child-fail" -> throwRetryable "Permanent child failure"
              SimpleTask "recover-reducer" -> do
                attempt <- liftIO $ atomicModifyIORef' attemptRef $ \n -> (n + 1, n + 1)
                if attempt == 1
                  then throwRetryable "Reducer fails first time"
                  else do
                    (merged, _dlq) <- mergedChildResults job
                    liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                    pure merged
              _ -> pure Nothing

        -- Insert rollup tree: reducer + 2 children
        Right (_parent :| _children) <-
          runSimpleDb env
            $ HL.insertJobTree
            $ (setMaxAttempts (Just 1) $ defaultJob (SimpleTask "recover-reducer"))
              <~~ ( defaultJob (SimpleTask "recover-child-ok")
                      :| [setMaxAttempts (Just 1) $ defaultJob (SimpleTask "recover-child-fail")]
                  )

        -- Phase 1: Worker runs - child-ok succeeds, child-fail DLQs, reducer wakes, reducer DLQs
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 10 handler

        let cfg =
              config
                { workerCount = 3
                , pollInterval = 0.1
                }

        withLinkedAsync (runSimpleDb env $ runWorkerPool cfg) $ \_ ->
          waitUntil 15_000 $ do
            dlqJobs <- runSimpleDb env $ HL.listDLQJobs @WorkerTestPayload 10 0
            pure (length dlqJobs == 2)

        -- Both child-fail and reducer should be in DLQ
        dlqJobs <- runSimpleDb env $ HL.listDLQJobs @WorkerTestPayload 10 0
        let dlqPayloads = map (payload . DLQ.jobSnapshot) dlqJobs
        dlqPayloads `shouldContain` [SimpleTask "recover-child-fail"]
        dlqPayloads `shouldContain` [SimpleTask "recover-reducer"]

        -- Phase 2: Retry child-fail from DLQ → auto-retries reducer (suspended)
        let childDlq = head $ filter (\d -> payload (DLQ.jobSnapshot d) == SimpleTask "recover-child-fail") dlqJobs
        mRetried <- runSimpleDb env $ HL.retryFromDLQ @WorkerTestPayload (DLQ.dlqPrimaryKey childDlq)
        case mRetried of
          Nothing -> expectationFailure "retryFromDLQ returned Nothing"
          Just retried -> payload retried `shouldBe` SimpleTask "recover-child-fail"

        -- Phase 3: Run workers again - child-fail still fails, goes back to DLQ,
        -- but reducer wakes with partial results from snapshot
        withLinkedAsync (runSimpleDb env $ runWorkerPool cfg) $ \_ ->
          waitUntil 15_000 $ not . null <$> readIORef finalResultRef

        -- The retried reducer (second attempt) should have received at least child-ok's result
        finalResult <- readIORef finalResultRef
        finalResult `shouldBe` ["alpha"]

    describe "Worker Registry" $ do
      it "registers, stamps claimed_by, and reconciles pause from the registry" $ \env -> do
        processedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job =
              liftIO $ atomicModifyIORef' processedRef $ \n -> (n + 1, ())

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 2 (noResult handler)
        let config = baseConfig {workerCount = 2, pollInterval = 0.1}
            wid = workerId config

        void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "first"))

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ (>= 1) <$> readIORef processedRef

          rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
          map WR.workerId rows `shouldContain` [wid]
          map WR.queueName rows `shouldContain` [testTable]

          -- Pause so the worker doesn't race us for the next claim.
          void $ runSimpleDb env $ Ops.setWorkerPaused testSchema wid True
          waitUntil 5_000 $ (== Paused) <$> getWorkerState config

          -- Insert and manually claim to read back claimed_by.
          void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "attribution"))
          claimed <-
            runSimpleDb env $
              Ops.claimNextVisibleJobsAs @_ @WorkerTestPayload testSchema testTable 1 60 wid
          case claimed of
            (j : _) -> claimedBy j `shouldBe` Just wid
            [] -> expectationFailure "expected a job to be claimable for the claimed_by assertion"

          void $ runSimpleDb env $ Ops.setWorkerPaused testSchema wid False
          waitUntil 5_000 $ (== Running) <$> getWorkerState config

      it "re-registers if the registry row is swept out from under it" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = pure ()

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let config =
              baseConfig
                { workerCount = 1
                , pollInterval = 0.1
                , workerHeartbeatInterval = 0.2
                }
            wid = workerId config

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ do
            rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
            pure $ wid `elem` map WR.workerId rows

          _ <- runSimpleDb env $ Ops.deregisterWorker testSchema wid
          rowsAfterDelete <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
          map WR.workerId rowsAfterDelete `shouldNotContain` [wid]

          -- Insert a job so the dispatcher signals the heartbeat MVar.
          void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "wake"))

          waitUntil 5_000 $ do
            rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
            pure $ wid `elem` map WR.workerId rows

      it "paused worker keeps heartbeating and survives the sweeper" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = pure ()

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let config =
              baseConfig
                { workerCount = 1
                , pollInterval = 0.1
                , workerHeartbeatInterval = 0.2
                , workerStaleThreshold = 1
                }
            wid = workerId config

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ do
            rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
            pure $ wid `elem` map WR.workerId rows

          void $ runSimpleDb env $ Ops.setWorkerPaused testSchema wid True
          waitUntil 5_000 $ (== Paused) <$> getWorkerState config

          -- Sit past stale_threshold_secs while paused.
          threadDelay 2_000_000
          void $ runSimpleDb env $ Ops.sweepStaleWorkers testSchema

          rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
          map WR.workerId rows `shouldContain` [wid]

    describe "Queue pause" $ do
      it "stamps paused_at on first pause and clears it on resume" $ \env -> do
        void $ runSimpleDb env $ Ops.ensureQueue testSchema testTable
        void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable True
        Just row1 <- runSimpleDb env $ Ops.getQueue testSchema testTable
        Q.paused row1 `shouldBe` True
        Q.pausedAt row1 `shouldSatisfy` isJust

        void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable False
        Just row2 <- runSimpleDb env $ Ops.getQueue testSchema testTable
        Q.paused row2 `shouldBe` False
        Q.pausedAt row2 `shouldBe` Nothing

      it "preserves paused_at on idempotent re-pause" $ \env -> do
        void $ runSimpleDb env $ Ops.ensureQueue testSchema testTable
        void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable True
        Just first <- runSimpleDb env $ Ops.getQueue testSchema testTable
        let original = Q.pausedAt first
        original `shouldSatisfy` isJust

        threadDelay 1_100_000 -- 1.1s so NOW() would differ if the SQL bumped it
        void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable True
        Just second <- runSimpleDb env $ Ops.getQueue testSchema testTable
        Q.pausedAt second `shouldBe` original

      it "lists workers filtered by liveness across all queues" $ \env -> do
        liveWid <- liftIO UUID.nextRandom
        staleWid <- liftIO UUID.nextRandom
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema liveWid testTable Nothing (Just 1) 300 Nothing
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema staleWid testTable Nothing (Just 1) 300 Nothing

        -- Age both rows past the query threshold, then bump only the live row.
        threadDelay 1_200_000
        void $ runSimpleDb env $ Ops.heartbeatWorker testSchema liveWid

        -- No liveness filter returns both rows.
        allRows <- runSimpleDb env $ Ops.listWorkers testSchema Nothing Nothing
        let allIds = map WR.workerId allRows
        allIds `shouldSatisfy` (liveWid `elem`)
        allIds `shouldSatisfy` (staleWid `elem`)

        -- Queueless live filter at 1s threshold keeps the freshly-heartbeated row only.
        liveOnly <- runSimpleDb env $ Ops.listWorkers testSchema Nothing (Just 1)
        let liveIds = map WR.workerId liveOnly
        liveIds `shouldSatisfy` (liveWid `elem`)
        liveIds `shouldSatisfy` (staleWid `notElem`)

      it "propagates queue pause to local pauseVar via heartbeat reconcile" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = pure ()
        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let config = baseConfig {workerCount = 1, pollInterval = 0.1}

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ (== Running) <$> getWorkerState config

          void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable True
          waitUntil 5_000 $ (== Paused) <$> getWorkerState config

          void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable False
          waitUntil 5_000 $ (== Running) <$> getWorkerState config

      it "propagates queue pause via NOTIFY at steady state under one pollInterval" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = pure ()
        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let config = baseConfig {workerCount = 1, pollInterval = 5.0}

            -- Steady-state toggles must complete via NOTIFY since the next
            -- heartbeat tick is one pollInterval away.
            timed paused = do
              let expected = if paused then Paused else Running
              start <- getCurrentTime
              void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable paused
              waitUntil 5_000 $ (== expected) <$> getWorkerState config
              elapsed <- (`diffUTCTime` start) <$> getCurrentTime
              elapsed `shouldSatisfy` (< 1.0)

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 10_000 $ (== Running) <$> getWorkerState config
          waitUntil 10_000 $ getListenerReady config
          timed True
          timed False
          timed True
          timed False

      it "keeps a pause NOTIFY that lands while a heartbeat reading is in flight" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = pure ()
        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let config = baseConfig {workerCount = 1, pollInterval = 5.0, workerHeartbeatInterval = 1.0}

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 10_000 $ (== Running) <$> getWorkerState config
          waitUntil 10_000 $ getListenerReady config

          -- Holding the worker's registry row blocks the pool's next heartbeat,
          -- whose reading of the queue's pause state predates the pause below.
          released <- newEmptyMVar
          let holdRow = runSimpleDb env $ withDbTransaction $ do
                void $ Ops.heartbeatWorker testSchema (workerId config)
                liftIO $ takeMVar released
          withAsync holdRow $ \_ -> do
            threadDelay 1_500_000
            void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable True
            waitUntil 5_000 $ (== Paused) <$> getWorkerState config
            putMVar released ()
            -- The blocked heartbeat completes here, one interval before the next.
            threadDelay 500_000
            getWorkerState config `shouldReturn` Paused

      it "claims immediately on unpause without waiting another poll cycle" $ \env -> do
        processedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job =
              liftIO $ atomicModifyIORef' processedRef $ \n -> (n + 1, ())
        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let config = baseConfig {workerCount = 1, pollInterval = 2.0}

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 10_000 $ (== Running) <$> getWorkerState config
          waitUntil 10_000 $ getListenerReady config

          void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable True
          waitUntil 5_000 $ (== Paused) <$> getWorkerState config

          void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "post-resume"))

          start <- getCurrentTime
          void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable False
          waitUntil 10_000 $ (>= 1) <$> readIORef processedRef
          elapsed <- (`diffUTCTime` start) <$> getCurrentTime
          elapsed `shouldSatisfy` (< 3.0)

      it "setWorkerPaused only targets the addressed worker" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = pure ()
        baseA :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        baseB :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let cfgA = baseA {workerCount = 1, pollInterval = 5.0}
            cfgB = baseB {workerCount = 1, pollInterval = 5.0}
            widA = workerId cfgA
            widB = workerId cfgB

        withLinkedAsync (runSimpleDb env $ runWorkerPool cfgA) $ \_ ->
          withLinkedAsync (runSimpleDb env $ runWorkerPool cfgB) $ \_ -> do
            -- Wait on the registry rows. getWorkerState reads only TVars and
            -- would return Running before the workers have actually registered.
            waitUntil 10_000 $ do
              rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
              let ids = map WR.workerId rows
              pure (widA `elem` ids && widB `elem` ids)
            -- And on subscription, so the pause NOTIFY is not sent before LISTEN.
            waitUntil 10_000 $ getListenerReady cfgA
            waitUntil 10_000 $ getListenerReady cfgB

            start <- getCurrentTime
            void $ runSimpleDb env $ Ops.setWorkerPaused testSchema widA True
            waitUntil 5_000 $ (== Paused) <$> getWorkerState cfgA
            elapsed <- (`diffUTCTime` start) <$> getCurrentTime
            elapsed `shouldSatisfy` (< 1.0)

            stateB <- getWorkerState cfgB
            stateB `shouldBe` Running

      it "setQueuePaused fans out to every worker in the queue" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = pure ()
        baseA :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        baseB :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let cfgA = baseA {workerCount = 1, pollInterval = 5.0}
            cfgB = baseB {workerCount = 1, pollInterval = 5.0}
            widA = workerId cfgA
            widB = workerId cfgB

        withLinkedAsync (runSimpleDb env $ runWorkerPool cfgA) $ \_ ->
          withLinkedAsync (runSimpleDb env $ runWorkerPool cfgB) $ \_ -> do
            waitUntil 10_000 $ do
              rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
              let ids = map WR.workerId rows
              pure (widA `elem` ids && widB `elem` ids)
            -- And on subscription, so the pause NOTIFY is not sent before LISTEN.
            waitUntil 10_000 $ getListenerReady cfgA
            waitUntil 10_000 $ getListenerReady cfgB

            start <- getCurrentTime
            void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable True
            waitUntil 5_000 $ (== Paused) <$> getWorkerState cfgA
            waitUntil 5_000 $ (== Paused) <$> getWorkerState cfgB
            elapsed <- (`diffUTCTime` start) <$> getCurrentTime
            elapsed `shouldSatisfy` (< 1.0)

    describe "Force cancel" $ do
      it "interrupts a long-running handler and removes the job" $ \env -> do
        startedRef <- newIORef False
        completedRef <- newIORef False
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              liftIO $ writeIORef startedRef True
              liftIO $ threadDelay 30_000_000
              liftIO $ writeIORef completedRef True

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let config = baseConfig {workerCount = 1, pollInterval = 0.2}

        Just job <- runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "long"))

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef

          start <- getCurrentTime
          n <- runSimpleDb env $ Ops.forceCancelJob testSchema testTable (primaryKey job)
          n `shouldBe` 1

          -- Handler should be interrupted well before its 30s sleep finishes.
          waitUntil 5_000 $ do
            mJob <- runSimpleDb env $ HL.getJobById @WorkerTestPayload (primaryKey job)
            pure (isNothing mJob)
          elapsed <- (`diffUTCTime` start) <$> getCurrentTime
          elapsed `shouldSatisfy` (< 3.0)

          -- The handler must not have run to completion.
          completed <- readIORef completedRef
          completed `shouldBe` False

          -- And it should not have produced a DLQ entry.
          dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          dlqJobs `shouldBe` []

      it "interrupts a CPU-bound handler (no DB I/O)" $ \env -> do
        -- Handler runs a tight IORef-bumping loop with NO blocking I/O. We
        -- probe whether the loop stops after force-cancel by sampling the
        -- counter twice. Under a masked child, async exceptions only land at
        -- interruptible points (STM retry, threadDelay, libpq) - the loop
        -- below has none, so a masked child keeps incrementing forever.
        startedRef <- newIORef False
        counterRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              liftIO $ writeIORef startedRef True
              let go = do
                    atomicModifyIORef' counterRef (\n -> (n + 1, ()))
                    go
              liftIO go

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let config = baseConfig {workerCount = 1, pollInterval = 0.2}

        Just job <- runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "cpu"))

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef
          n <- runSimpleDb env $ Ops.forceCancelJob testSchema testTable (primaryKey job)
          n `shouldBe` 1
          -- Let cancellation propagate.
          threadDelay 500_000
          c1 <- readIORef counterRef
          threadDelay 500_000
          c2 <- readIORef counterRef
          -- Counter must freeze: if the handler is still alive, it would
          -- bump millions of times in 500ms.
          c2 `shouldBe` c1

      it "cancelling one job of a batch interrupts the whole batch handler" $ \env -> do
        -- A batch runs in a single handler thread, so all its job ids point at
        -- the same async. Targeting one job throws into that thread and tears
        -- down the in-flight batch.
        startedRef <- newIORef False
        completedRef <- newIORef False
        let batchHandler _jobs _cbs = do
              liftIO $ writeIORef startedRef True
              liftIO $ threadDelay 30_000_000
              liftIO $ writeIORef completedRef True

        let jobs =
              [ setGroupKey (Just "bc") $ defaultJob (SimpleTask "bc-1")
              , setGroupKey (Just "bc") $ defaultJob (SimpleTask "bc-2")
              ]
        inserted <- runSimpleDb env $ HL.insertJobsBatch jobs
        let firstId = primaryKey (head inserted)

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          defaultBatchedWorkerConfig 1 10 batchHandler
        threadDelay 100_000

        withLinkedAsync (runSimpleDb env $ runWorkerPool config {pollInterval = 0.1}) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef

          start <- getCurrentTime
          -- Cancel only the first job. The whole batch thread unwinds.
          n <- runSimpleDb env $ Ops.forceCancelJob testSchema testTable firstId
          n `shouldBe` 1
          waitUntil 5_000 $ do
            mJob <- runSimpleDb env $ HL.getJobById @WorkerTestPayload firstId
            pure (isNothing mJob)
          elapsed <- (`diffUTCTime` start) <$> getCurrentTime
          elapsed `shouldSatisfy` (< 3.0)

          -- The handler was interrupted, not run to completion.
          readIORef completedRef `shouldReturn` False
          -- No DLQ entries from the cancel.
          dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          dlqJobs `shouldBe` []

      it "interrupts a running handler in poll-only mode via the flag" $ \env -> do
        -- No listener, so the only path to interruption is the heartbeat
        -- polling cancel_requested_at and throwing into the handler.
        startedRef <- newIORef False
        completedRef <- newIORef False
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              liftIO $ writeIORef startedRef True
              liftIO $ threadDelay 30_000_000
              liftIO $ writeIORef completedRef True

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          transactionalWorkerConfig 1 (noResult handler)
        let config =
              baseConfig
                { workerCount = 1
                , pollInterval = 0.2
                , jobHeartbeatInterval = 0.3
                , visibilityTimeout = 3
                }

        Just job <- runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "poll-cancel"))

        withLinkedAsync (runSimpleDb (disableListener env) $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef

          start <- getCurrentTime
          n <- runSimpleDb env $ Ops.forceCancelJob testSchema testTable (primaryKey job)
          n `shouldBe` 1

          waitUntil 5_000 $ do
            mJob <- runSimpleDb env $ HL.getJobById @WorkerTestPayload (primaryKey job)
            pure (isNothing mJob)
          elapsed <- (`diffUTCTime` start) <$> getCurrentTime
          elapsed `shouldSatisfy` (< 3.0)

          readIORef completedRef `shouldReturn` False
          dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          dlqJobs `shouldBe` []

      it "flags a job that is claimed concurrently with the force-cancel" $ \env -> do
        Just job <- runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "concurrent-claim"))
        let jid = primaryKey job
            pool = fromJust (connectionPool (simplePool env))
            claimSql =
              fromString . T.unpack $
                "UPDATE "
                  <> testSchema
                  <> "."
                  <> testTable
                  <> " SET claimed_by = '00000000-0000-0000-0000-000000000abc'::uuid"
                  <> ", not_visible_until = NOW() + interval '60 second', attempts = attempts + 1, claim_seq = claim_seq + 1 WHERE id = ?"

        connB <- PG.connectPostgreSQL connStr
        void $ PG.execute_ connB "BEGIN"
        void $ PG.execute connB claimSql (Only jid)

        cancelledCount <-
          withAsync (runSimpleDb env $ Ops.forceCancelJob testSchema testTable jid) $ \fc -> do
            threadDelay 300_000
            void $ PG.execute_ connB "COMMIT"
            PG.close connB
            Async.wait fc

        cancelledCount `shouldBe` 1
        [Only flagged] <-
          withResource pool $ \c ->
            PG.query
              c
              ( fromString . T.unpack $
                  "SELECT cancel_requested_at IS NOT NULL FROM " <> testSchema <> "." <> testTable <> " WHERE id = ?"
              )
              (Only jid)
              :: IO [Only Bool]
        flagged `shouldBe` True

      it "notifies the worker when force-cancel deletes its lease-lapsed claimed job" $ \env -> do
        wid <- UUID.nextRandom
        Just job <- runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "lapsed-cancel"))
        let jid = primaryKey job
            pool = fromJust (connectionPool (simplePool env))
        claimed <- runSimpleDb env (HL.claimNextVisibleJobsAs 1 60 wid) :: IO [JobRead WorkerTestPayload]
        length claimed `shouldBe` 1

        void $
          withResource pool $ \c ->
            PG.execute
              c
              ( fromString . T.unpack $
                  "UPDATE " <> testSchema <> "." <> testTable <> " SET not_visible_until = NOW() - interval '1 second' WHERE id = ?"
              )
              (Only jid)

        lconn <- PG.connectPostgreSQL connStr
        let chan = Schema.cancelNotifyChannel testSchema testTable
        void $ PG.execute_ lconn (fromString . T.unpack $ "LISTEN \"" <> chan <> "\"")

        n <- runSimpleDb env $ Ops.forceCancelJob testSchema testTable jid
        n `shouldBe` 1

        runSimpleDb env (HL.getJobById @WorkerTestPayload jid)
          >>= (`shouldSatisfy` isNothing)

        mNotif <- timeout 2_000_000 (getNotification lconn)
        PG.close lconn
        case mNotif of
          Nothing -> expectationFailure "expected a cancel NOTIFY for the deleted lease-lapsed job"
          Just notif -> notificationData notif `shouldSatisfy` BSC.isInfixOf (BSC.pack (show jid))

      it "does not deadlock against a concurrent ack of the last child" $ \env -> do
        Right (parent :| [child]) <-
          runSimpleDb env
            $ HL.insertJobTree
            $ JT.rollup
              (defaultJob (SimpleTask "dl-parent"))
              (JT.leaf (defaultJob (SimpleTask "dl-child")) :| [])
        let pid = primaryKey parent
            cid = primaryKey child

        connA <- PG.connectPostgreSQL connStr
        void $ PG.execute_ connA "BEGIN"
        void $ lockJobRow connA cid

        (efc, epA) <-
          withAsync (try (runSimpleDb env $ Ops.forceCancelJob testSchema testTable pid) :: IO (Either SomeException Int64)) $ \fc -> do
            threadDelay 300_000
            epA <- lockJobRow connA pid
            void (try (PG.execute_ connA "COMMIT") :: IO (Either SomeException Int64))
            efc <- Async.wait fc
            pure (efc, epA)
        PG.close connA

        efc `shouldSatisfy` isRight
        epA `shouldSatisfy` isRight

      it "settles a failed batch children-first, so a tree lock cannot deadlock it" $ \env -> do
        startedRef <- newIORef False
        goVar <- newEmptyMVar
        let batchHandler _jobs _cbs = liftIO $ do
              writeIORef startedRef True
              takeMVar goVar
              throwIO (userError "dlb-boom")
        let jobs =
              [ setMaxAttempts (Just 1) $ setGroupKey (Just "dlb") $ defaultJob (SimpleTask "dlb-1")
              , setMaxAttempts (Just 1) $ setGroupKey (Just "dlb") $ defaultJob (SimpleTask "dlb-2")
              ]
        inserted <- runSimpleDb env $ HL.insertJobsBatch jobs
        let ids = map primaryKey inserted
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          defaultBatchedWorkerConfig 1 10 batchHandler

        withLinkedAsync
          (runSimpleDb env $ runWorkerPool config {pollInterval = 0.05, jobHeartbeatInterval = 30, visibilityTimeout = 60})
          $ \_ -> do
            waitUntil 5_000 $ readIORef startedRef

            connA <- PG.connectPostgreSQL connStr
            void $ PG.execute_ connA "BEGIN"
            -- Hold the batch's higher id, the row a force-cancel over the tree takes first.
            void $ lockJobRow connA (maximum ids)
            putMVar goVar ()
            threadDelay 300_000
            -- The failure transaction must not already hold the lower id.
            eLo <- lockJobRow connA (minimum ids)
            void (try (PG.execute_ connA "COMMIT") :: IO (Either SomeException Int64))
            PG.close connA

            eLo `shouldSatisfy` isRight
            waitUntil 10_000 $ do
              dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
              pure (length dlqJobs == 2)

      it "deletes a flagged job the handler already nacked" $ \env -> do
        -- A nack keeps the claim, so a later cancel flags the row rather than deleting it.
        nackedRef <- newIORef False
        let jobs =
              [ setGroupKey (Just "fcn") $ defaultJob (SimpleTask "fcn-1")
              , setGroupKey (Just "fcn") $ defaultJob (SimpleTask "fcn-2")
              ]
        inserted <- runSimpleDb env $ HL.insertJobsBatch jobs
        let firstId = primaryKey (head inserted)
            batchHandler batch cbs = do
              traverse_ (\j -> when (primaryKey j == firstId) (nack cbs j)) batch
              liftIO $ writeIORef nackedRef True
              liftIO $ threadDelay 30_000_000

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload <-
          defaultBatchedWorkerConfig 1 10 batchHandler
        let config = baseConfig {pollInterval = 0.1, jobHeartbeatInterval = 0.3, visibilityTimeout = 60}

        withLinkedAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ readIORef nackedRef

          runSimpleDb env (Ops.forceCancelJob testSchema testTable firstId) `shouldReturn` 1

          waitUntil 10_000 $ do
            mJob <- runSimpleDb env $ HL.getJobById @WorkerTestPayload firstId
            pure (isNothing mJob)

          dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          dlqJobs `shouldBe` []

      it "refuses a stale worker's ack after a reclaim and nack restored attempts" $ \env -> do
        -- A nack restores the attempt it consumed, so attempts alone repeats across claims.
        w1 <- UUID.nextRandom
        w2 <- UUID.nextRandom
        Just job <- runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "aba"))
        let jid = primaryKey job
            pool = fromJust (connectionPool (simplePool env))
            expire =
              fromString . T.unpack $
                "UPDATE " <> testSchema <> "." <> testTable <> " SET not_visible_until = NOW() - interval '1 second' WHERE id = ?"

        [stale] <- runSimpleDb env (HL.claimNextVisibleJobsAs 1 60 w1) :: IO [JobRead WorkerTestPayload]
        primaryKey stale `shouldBe` jid
        void $ withResource pool $ \c -> PG.execute c expire (Only jid)

        [held] <- runSimpleDb env (HL.claimNextVisibleJobsAs 1 60 w2) :: IO [JobRead WorkerTestPayload]
        primaryKey held `shouldBe` jid
        runSimpleDb env (HL.nackJob held) `shouldReturn` 1

        -- The nack put attempts back to what w1 recorded, so an attempts-keyed
        -- predicate would match here.
        reread <- runSimpleDb env $ HL.getJobById @WorkerTestPayload jid
        fmap attempts reread `shouldBe` Just (attempts stale)

        -- w1 is stale: every finalize it can still issue must match no row.
        runSimpleDb env (HL.setVisibilityTimeoutBatch 60 [stale])
          `shouldReturn` [HL.JobReclaimed jid (claimSeq stale) (claimSeq held)]
        runSimpleDb env (HL.nackJob stale) `shouldReturn` 0
        runSimpleDb env (HL.ackJob stale) `shouldReturn` 0
        runSimpleDb env (HL.getJobById @WorkerTestPayload jid) >>= (`shouldSatisfy` isJust)

        -- w2 still owns it and can finish.
        runSimpleDb env (HL.ackJob held) `shouldReturn` 1

      it "does not deadlock a tree cancel against a concurrent lock walk" $ \env -> do
        Right (parent :| [child]) <-
          runSimpleDb env
            $ HL.insertJobTree
            $ JT.rollup
              (defaultJob (SimpleTask "tc-parent"))
              (JT.leaf (defaultJob (SimpleTask "tc-child")) :| [])
        let pid = primaryKey parent
            cid = primaryKey child

        connA <- PG.connectPostgreSQL connStr
        void $ PG.execute_ connA "BEGIN"
        -- Hold the child, the row a tree cancel takes first.
        void $ lockJobRow connA cid

        (etc, epA) <-
          withAsync (try (runSimpleDb env $ Ops.cancelJobTree testSchema testTable cid) :: IO (Either SomeException Int64)) $ \tc -> do
            threadDelay 300_000
            epA <- lockJobRow connA pid
            void (try (PG.execute_ connA "COMMIT") :: IO (Either SomeException Int64))
            etc <- Async.wait tc
            pure (etc, epA)
        PG.close connA

        etc `shouldSatisfy` isRight
        epA `shouldSatisfy` isRight

    describe "Sweeper" $ do
      it "deletes a stale unpaused worker row" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema wid testTable Nothing (Just 1) 1 Nothing
        threadDelay 1_500_000
        n <- runSimpleDb env $ Ops.sweepStaleWorkers testSchema
        n `shouldSatisfy` (>= 1)
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        map WR.workerId rows `shouldNotContain` [wid]

      it "deletes a stale paused worker row" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema wid testTable Nothing (Just 1) 1 Nothing
        void $ runSimpleDb env $ Ops.setWorkerPaused testSchema wid True
        threadDelay 1_500_000
        n <- runSimpleDb env $ Ops.sweepStaleWorkers testSchema
        n `shouldSatisfy` (>= 1)
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        map WR.workerId rows `shouldNotContain` [wid]

      it "deletes a stale shutting-down worker row" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema wid testTable Nothing (Just 1) 1 Nothing
        void $ runSimpleDb env $ Ops.markWorkerShuttingDown testSchema wid
        threadDelay 1_500_000
        n <- runSimpleDb env $ Ops.sweepStaleWorkers testSchema
        n `shouldSatisfy` (>= 1)
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        map WR.workerId rows `shouldNotContain` [wid]

      it "preserves paused state across re-registration" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema wid testTable Nothing (Just 1) 300 Nothing
        void $ runSimpleDb env $ Ops.setWorkerPaused testSchema wid True
        -- Re-register with different metadata to confirm upsert touches the row.
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema wid testTable (Just "fresh-host") (Just 1) 300 Nothing
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> do
            WR.paused row `shouldBe` True
            WR.hostName row `shouldBe` Just "fresh-host"
          _ -> expectationFailure "expected exactly one row for the worker"

    describe "Worker health" $ do
      it "reports a freshly-registered worker as live" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema wid testTable Nothing (Just 1) 300 Nothing
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> WR.health row `shouldBe` WR.Live
          _ -> expectationFailure "expected exactly one row for the worker"

      it "reports a worker past its stale threshold as stale" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema wid testTable Nothing (Just 1) 1 Nothing
        threadDelay 1_500_000
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> WR.health row `shouldBe` WR.Stale
          _ -> expectationFailure "expected exactly one row for the worker"

      it "reports a fresh shutting-down worker as draining" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runSimpleDb env
          $ Ops.registerWorker testSchema wid testTable Nothing (Just 1) 300 Nothing
        void $ runSimpleDb env $ Ops.markWorkerShuttingDown testSchema wid
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> WR.health row `shouldBe` WR.Draining
          _ -> expectationFailure "expected exactly one row for the worker"

    describe "Cron queue filter" $ do
      it "filters cron schedules by queue" $ \env -> do
        let otherQueue = "other_queue"
        runSimpleDb env $ do
          void $ Ops.upsertCronDefault testSchema "cron-here" testTable "* * * * *" "AllowOverlap" Nothing
          void $ Ops.upsertCronDefault testSchema "cron-elsewhere" otherQueue "* * * * *" "AllowOverlap" Nothing

        hereOnly <- runSimpleDb env $ Ops.listCronSchedules testSchema (Just testTable)
        map CS.name hereOnly `shouldContain` ["cron-here"]
        map CS.name hereOnly `shouldNotContain` ["cron-elsewhere"]

        elsewhereOnly <- runSimpleDb env $ Ops.listCronSchedules testSchema (Just otherQueue)
        map CS.name elsewhereOnly `shouldContain` ["cron-elsewhere"]
        map CS.name elsewhereOnly `shouldNotContain` ["cron-here"]

        all_ <- runSimpleDb env $ Ops.listCronSchedules testSchema Nothing
        map CS.name all_ `shouldSatisfy` (\ns -> "cron-here" `elem` ns && "cron-elsewhere" `elem` ns)

-- Helper to create a connection with data cleanup

-- | Clean the queue for one test. The env is built once for the suite: its LISTEN
-- hub holds a pool connection for as long as the env lives, and the shared pool
-- only has five.
withPool :: SimpleEnv WorkerTestRegistry -> (SimpleEnv WorkerTestRegistry -> IO a) -> IO a
withPool env action = do
  let pool = fromJust (connectionPool (simplePool env))
  withResource pool $ \conn -> cleanupData testSchema testTable conn
  action env

withTestOpsTable :: SimpleEnv WorkerTestRegistry -> IO a -> IO a
withTestOpsTable env action = do
  let pool = fromJust (connectionPool (simplePool env))
  withResource pool (cleanupData testSchema testTable)
  withResource pool $ \conn -> do
    void $ execute_ conn $ "CREATE TABLE IF NOT EXISTS " <> testSchema <> ".test_operations (job_id INT, operation TEXT)"
    void $ execute_ conn $ "TRUNCATE " <> testSchema <> ".test_operations"
  result <- action
  withResource pool $ \conn ->
    void $ execute_ conn $ "DROP TABLE IF EXISTS " <> testSchema <> ".test_operations"
  pure result

queryOpsCount :: SimpleEnv WorkerTestRegistry -> IO Int
queryOpsCount env = do
  let pool = fromJust (connectionPool (simplePool env))
  withResource pool $ \conn -> do
    [Only count] <-
      query
        conn
        (fromString . T.unpack $ "SELECT COUNT(*) FROM " <> testSchema <> ".test_operations WHERE operation = ?")
        (Only ("processed" :: Text))
        :: IO [Only Int]
    pure count

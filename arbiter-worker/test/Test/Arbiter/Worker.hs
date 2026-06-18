{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Worker (spec) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types
  ( Job (..)
  , ObservabilityHooks (..)
  , defaultJob
  , defaultObservabilityHooks
  )
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.Queues qualified as Q
import Arbiter.Core.Worker qualified as WR
import Arbiter.Simple
  ( SimpleConnectionPool (..)
  , SimpleDb
  , SimpleEnv (..)
  , createSimpleEnvWithPool
  , runSimpleDb
  )
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (cleanupData, createSharedPool, execute_, setupOnce)
import Control.Concurrent (threadDelay)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Foldable (for_, toList, traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust, isJust, isNothing)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (diffUTCTime, getCurrentTime)
import Data.UUID.V4 qualified as UUID
import Database.PostgreSQL.Simple (Only (..), execute, query)
import Database.PostgreSQL.Simple qualified as PG
import System.Directory qualified as Dir
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

import Arbiter.Worker (mergedChildResults, runWorkerPool)
import Arbiter.Worker.Config
  ( WorkerConfig (..)
  , ackAll
  , ackAllWith
  , ackWith
  , defaultBatchedRollupWorkerConfig
  , defaultBatchedWorkerConfig
  , defaultWorkerConfig
  , getWorkerState
  , shutdownWorker
  )
import Arbiter.Worker.TestKit (workerSpec)
import Arbiter.Worker.WorkerState (WorkerState (..))

type WorkerTestRegistry = '[ '("arbiter_worker_test", WorkerTestPayload)]

testSchema :: Text
testSchema = "arbiter_worker_test"

testTable :: Text
testTable = "arbiter_worker_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable True) $ do
  sharedPool <- runIO (createSharedPool connStr)
  around (withPool sharedPool) $ do
    workerSpec @WorkerTestPayload @WorkerTestRegistry connStr SimpleTask FailingTask (\f _conn job -> f job) runSimpleDb

    describe "Transactional Atomicity" $ do
      it "rolls back user operations when handler fails" $ \env -> withTestOpsTable env $ do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler conn job = do
              liftIO $
                void $
                  execute
                    conn
                    (fromString . T.unpack $ "INSERT INTO " <> testSchema <> ".test_operations (job_id, operation) VALUES (?, ?)")
                    (primaryKey job, "processed" :: Text)
              throwRetryable "Simulated failure"

        void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "WillFail")) {groupKey = Just "g1", maxAttempts = Just 1}

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
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
              liftIO $
                void $
                  execute
                    conn
                    (fromString . T.unpack $ "INSERT INTO " <> testSchema <> ".test_operations (job_id, operation) VALUES (?, ?)")
                    (primaryKey job, "processed" :: Text)
              pure ()

        void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "WillSucceed")) {groupKey = Just "g1"}

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          (runSimpleDb env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1})
          $ \_ -> do
            waitUntil 10_000 $ (== 1) <$> queryOpsCount env

            count <- queryOpsCount env
            count `shouldBe` 1

      it "manual commit inside handler persists despite subsequent failure" $ \env -> withTestOpsTable env $ do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler conn job = do
              liftIO $
                void $
                  execute
                    conn
                    (fromString . T.unpack $ "INSERT INTO " <> testSchema <> ".test_operations (job_id, operation) VALUES (?, ?)")
                    (primaryKey job, "processed" :: Text)
              -- User manually commits the transaction (violates our transaction semantics)
              liftIO $ PG.commit conn
              throwRetryable "Simulated failure after commit"

        void $
          runSimpleDb env $
            HL.insertJob (defaultJob (SimpleTask "ManualCommit")) {groupKey = Just "g1", maxAttempts = Just 1}

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
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
        let job = (defaultJob (SimpleTask "LongJob")) {groupKey = Just "g1"}
        void $ runSimpleDb env $ HL.insertJob job

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        let configWithTimeout =
              config
                { workerCount = 1
                , pollInterval = 0.1
                , gracefulShutdownTimeout = Just 10 -- 10 second timeout (plenty of time)
                }

        -- Start worker and wait for job to start processing
        withAsync (runSimpleDb env $ runWorkerPool configWithTimeout) $ \worker -> do
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
        let job = (defaultJob (SimpleTask "VeryLongJob")) {groupKey = Just "g1"}
        void $ runSimpleDb env $ HL.insertJob job

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        let configWithShortTimeout =
              config
                { workerCount = 1
                , pollInterval = 0.05 -- Faster polling for test
                , gracefulShutdownTimeout = Just 1 -- Only 1 second timeout
                }

        withAsync (runSimpleDb env $ runWorkerPool configWithShortTimeout) $ \worker -> do
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

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithLiveness =
              config
                { livenessFile = Just livenessPath
                , workerCount = 1
                , pollInterval = 0.1
                }

        withAsync (runSimpleDb env $ runWorkerPool configWithLiveness) $ \worker -> do
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
              -- Simulate a reclaim of "ca-stolen": bump its attempts so the bulk ack won't match it.
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
                                  "UPDATE " <> Schema.jobQueueTable testSchema testTable <> " SET attempts = attempts + 1 WHERE id = ?"
                              )
                              (Only (primaryKey j))
                  )
                  js
              ackAll cbs js
        let jobs =
              [ (defaultJob (SimpleTask "ca-keep1")) {groupKey = Just "ca"}
              , (defaultJob (SimpleTask "ca-stolen")) {groupKey = Just "ca"}
              , (defaultJob (SimpleTask "ca-keep2")) {groupKey = Just "ca"}
              ]
        void $ runSimpleDb env $ HL.insertJobsBatch jobs
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler
        withAsync (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.05, observabilityHooks = hooks})) $ \_ -> do
          waitUntil 10_000 $ (== 2) . length <$> readIORef successRef
          successes <- readIORef successRef
          -- onJobSuccess fired only for the survivors; the reclaimed job was skipped.
          successes `shouldMatchList` [SimpleTask "ca-keep1", SimpleTask "ca-keep2"]

      it "rollup ackAllWith stores each job's result for the parent" $ \env -> do
        finalRef <- newIORef ([] :: [Text])
        let resultFor p = case p of
              SimpleTask "rb-ca" -> ["alpha"]
              SimpleTask "rb-cb" -> ["beta"]
              _ -> []
            isReducer p = case p of SimpleTask "rb-reducer" -> True; _ -> False
            handler jobs cbs =
              if all (isReducer . payload) jobs
                then
                  traverse_
                    ( \j -> do
                        (merged, _dlq) <- mergedChildResults j
                        liftIO $ atomicModifyIORef' finalRef $ \_ -> (merged, ())
                        ackWith cbs j merged
                    )
                    (toList jobs)
                else ackAllWith cbs (map (\j -> (j, resultFor (payload j))) (toList jobs))
        runSimpleDb env $
          void $
            HL.insertJobTree $
              defaultJob (SimpleTask "rb-reducer")
                <~~ (defaultJob (SimpleTask "rb-ca") :| [defaultJob (SimpleTask "rb-cb")])
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultBatchedRollupWorkerConfig connStr 1 10 handler
        withAsync (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.1})) $ \_ -> do
          waitUntil 10_000 $ (== 2) . length <$> readIORef finalRef
          final <- readIORef finalRef
          final `shouldMatchList` ["alpha", "beta"]

    describe "Fan-out/fan-in with rollup" $ do
      it "worker auto-appends handler results; finalizer reads merged state" $ \env -> do
        finalResultRef <- newIORef ([] :: [Text])

        let handler _conn job = case payload job of
              SimpleTask "mapper-a" -> pure ["sales", "growth"]
              SimpleTask "mapper-b" -> pure ["revenue"]
              SimpleTask "mapper-c" -> pure ["forecast", "trend"]
              SimpleTask "reducer" -> do
                (merged, _dlq) <- mergedChildResults job
                liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (merged, ())
                pure merged
              _ -> pure []

        -- Insert the rollup tree
        runSimpleDb env $
          void $
            HL.insertJobTree $
              defaultJob (SimpleTask "reducer")
                <~~ ( defaultJob (SimpleTask "mapper-a")
                        :| [ defaultJob (SimpleTask "mapper-b")
                           , defaultJob (SimpleTask "mapper-c")
                           ]
                    )

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
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
              SimpleTask "mapper-1a" -> pure ["sales", "growth"]
              SimpleTask "mapper-1b" -> pure ["revenue"]
              SimpleTask "mapper-2a" -> pure ["forecast"]
              SimpleTask "mapper-2b" -> pure ["trend"]
              SimpleTask "section-1" -> fst <$> mergedChildResults job
              SimpleTask "section-2" -> fst <$> mergedChildResults job
              SimpleTask "root" -> do
                (merged, _dlq) <- mergedChildResults job
                liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (merged, ())
                pure merged
              _ -> pure []

        runSimpleDb env $
          void $
            HL.insertJobTree $
              JT.rollup (defaultJob (SimpleTask "root")) $
                ( defaultJob (SimpleTask "section-1")
                    <~~ (defaultJob (SimpleTask "mapper-1a") :| [defaultJob (SimpleTask "mapper-1b")])
                )
                  :| [ defaultJob (SimpleTask "section-2")
                         <~~ (defaultJob (SimpleTask "mapper-2a") :| [defaultJob (SimpleTask "mapper-2b")])
                     ]

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
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
                SimpleTask "child-a" -> ackWith cbs job (["alpha"] :: [Text])
                SimpleTask "child-b" -> ackWith cbs job ["beta", "gamma"]
                SimpleTask "manual-reducer" -> do
                  (merged, _dlq) <- mergedChildResults job
                  liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (merged, ())
                  ackWith cbs job merged
                _ -> ackWith cbs job []

        -- Insert the rollup tree
        runSimpleDb env $
          void $
            HL.insertJobTree $
              defaultJob (SimpleTask "manual-reducer")
                <~~ ( defaultJob (SimpleTask "child-a")
                        :| [defaultJob (SimpleTask "child-b")]
                    )

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultBatchedRollupWorkerConfig connStr 3 1 handler

        withAsync
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
                SimpleTask "child-1a" -> ackWith cbs job ["a1"]
                SimpleTask "child-1b" -> ackWith cbs job ["b1"]
                SimpleTask "child-2a" -> ackWith cbs job ["a2"]
                SimpleTask "child-2b" -> ackWith cbs job ["b2"]
                SimpleTask name -> do
                  (merged, _dlq) <- mergedChildResults job
                  liftIO $ atomicModifyIORef' receivedRef $ \m -> (Map.insert name merged m, ())
                  ackWith cbs job merged
                _ -> ackWith cbs job []

        -- Two independent rollup trees, all ungrouped. The four children drain in
        -- one ungrouped batch, then both parents unblock and batch together.
        runSimpleDb env $
          void $
            HL.insertJobTree $
              defaultJob (SimpleTask "reducer-1")
                <~~ (defaultJob (SimpleTask "child-1a") :| [defaultJob (SimpleTask "child-1b")])
        runSimpleDb env $
          void $
            HL.insertJobTree $
              defaultJob (SimpleTask "reducer-2")
                <~~ (defaultJob (SimpleTask "child-2a") :| [defaultJob (SimpleTask "child-2b")])

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultBatchedRollupWorkerConfig connStr 1 10 handler

        withAsync
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
              SimpleTask "dlq-child-a" -> pure ["x"]
              SimpleTask "dlq-child-b" -> pure ["y", "z"]
              SimpleTask "dlq-reducer" -> do
                attempt <- liftIO $ atomicModifyIORef' attemptRef $ \n -> (n + 1, n + 1)
                if attempt == 1
                  then throwRetryable "Intentional failure on first attempt"
                  else do
                    (merged, _dlq) <- mergedChildResults job
                    liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (merged, ())
                    pure merged
              _ -> pure []

        -- Insert the rollup tree
        Right (_parent :| _children) <-
          runSimpleDb env $
            HL.insertJobTree $
              (defaultJob (SimpleTask "dlq-reducer")) {maxAttempts = Just 1}
                <~~ ( defaultJob (SimpleTask "dlq-child-a")
                        :| [defaultJob (SimpleTask "dlq-child-b")]
                    )

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        let cfg =
              config
                { workerCount = 3
                , pollInterval = 0.1
                }

        -- Phase 1: Run workers - children succeed, reducer fails → DLQ
        withAsync (runSimpleDb env $ runWorkerPool cfg) $ \_ ->
          waitUntil 10_000 $ do
            dlqJobs <- runSimpleDb env $ HL.listDLQJobs @_ @WorkerTestRegistry @WorkerTestPayload 10 0
            pure $ any (\d -> payload (DLQ.jobSnapshot d) == SimpleTask "dlq-reducer") dlqJobs

        -- Verify reducer is in DLQ with snapshot
        dlqJobs <- runSimpleDb env $ HL.listDLQJobs @_ @WorkerTestRegistry @WorkerTestPayload 10 0
        let reducerDlq = filter (\d -> payload (DLQ.jobSnapshot d) == SimpleTask "dlq-reducer") dlqJobs
        length reducerDlq `shouldBe` 1

        -- Phase 2: Retry from DLQ - reducer should see preserved results from snapshot
        let dlqId = DLQ.dlqPrimaryKey (head reducerDlq)
        mRetried <- runSimpleDb env $ HL.retryFromDLQ @_ @WorkerTestRegistry @WorkerTestPayload dlqId
        case mRetried of
          Nothing -> expectationFailure "retryFromDLQ returned Nothing"
          Just retried -> payload retried `shouldBe` SimpleTask "dlq-reducer"

        withAsync (runSimpleDb env $ runWorkerPool cfg) $ \_ ->
          waitUntil 10_000 $ not . null <$> readIORef finalResultRef

        -- The retried reducer should have received the preserved child results
        finalResult <- readIORef finalResultRef
        length finalResult `shouldBe` 3
        finalResult `shouldMatchList` ["x", "y", "z"]

      it "DLQ tree recovery: both parent and child in DLQ" $ \env -> do
        attemptRef <- newIORef (0 :: Int)
        finalResultRef <- newIORef ([] :: [Text])

        let handler _conn job = case payload job of
              SimpleTask "recover-child-ok" -> pure ["alpha"]
              SimpleTask "recover-child-fail" -> throwRetryable "Permanent child failure"
              SimpleTask "recover-reducer" -> do
                attempt <- liftIO $ atomicModifyIORef' attemptRef $ \n -> (n + 1, n + 1)
                if attempt == 1
                  then throwRetryable "Reducer fails first time"
                  else do
                    (merged, _dlq) <- mergedChildResults job
                    liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (merged, ())
                    pure merged
              _ -> pure []

        -- Insert rollup tree: reducer + 2 children
        Right (_parent :| _children) <-
          runSimpleDb env $
            HL.insertJobTree $
              (defaultJob (SimpleTask "recover-reducer")) {maxAttempts = Just 1}
                <~~ ( defaultJob (SimpleTask "recover-child-ok")
                        :| [(defaultJob (SimpleTask "recover-child-fail")) {maxAttempts = Just 1}]
                    )

        -- Phase 1: Worker runs - child-ok succeeds, child-fail DLQs, reducer wakes, reducer DLQs
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        let cfg =
              config
                { workerCount = 3
                , pollInterval = 0.1
                }

        withAsync (runSimpleDb env $ runWorkerPool cfg) $ \_ ->
          waitUntil 15_000 $ do
            dlqJobs <- runSimpleDb env $ HL.listDLQJobs @_ @WorkerTestRegistry @WorkerTestPayload 10 0
            pure (length dlqJobs == 2)

        -- Both child-fail and reducer should be in DLQ
        dlqJobs <- runSimpleDb env $ HL.listDLQJobs @_ @WorkerTestRegistry @WorkerTestPayload 10 0
        let dlqPayloads = map (payload . DLQ.jobSnapshot) dlqJobs
        dlqPayloads `shouldContain` [SimpleTask "recover-child-fail"]
        dlqPayloads `shouldContain` [SimpleTask "recover-reducer"]

        -- Phase 2: Retry child-fail from DLQ → auto-retries reducer (suspended)
        let childDlq = head $ filter (\d -> payload (DLQ.jobSnapshot d) == SimpleTask "recover-child-fail") dlqJobs
        mRetried <- runSimpleDb env $ HL.retryFromDLQ @_ @WorkerTestRegistry @WorkerTestPayload (DLQ.dlqPrimaryKey childDlq)
        case mRetried of
          Nothing -> expectationFailure "retryFromDLQ returned Nothing"
          Just retried -> payload retried `shouldBe` SimpleTask "recover-child-fail"

        -- Phase 3: Run workers again - child-fail still fails, goes back to DLQ,
        -- but reducer wakes with partial results from snapshot
        withAsync (runSimpleDb env $ runWorkerPool cfg) $ \_ ->
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

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 2 handler
        let config = baseConfig {workerCount = 2, pollInterval = 0.1}
            wid = workerId config

        void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "first"))

        withAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
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

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        let config =
              baseConfig
                { workerCount = 1
                , pollInterval = 0.1
                , workerHeartbeatInterval = 0.2
                }
            wid = workerId config

        withAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
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

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        let config =
              baseConfig
                { workerCount = 1
                , pollInterval = 0.1
                , workerHeartbeatInterval = 0.2
                , workerStaleThreshold = 1
                }
            wid = workerId config

        withAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
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
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema liveWid testTable Nothing (Just 1) 300 Nothing
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema staleWid testTable Nothing (Just 1) 300 Nothing

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
        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        let config = baseConfig {workerCount = 1, pollInterval = 0.1}

        withAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ (== Running) <$> getWorkerState config

          void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable True
          waitUntil 5_000 $ (== Paused) <$> getWorkerState config

          void $ runSimpleDb env $ Ops.setQueuePaused testSchema testTable False
          waitUntil 5_000 $ (== Running) <$> getWorkerState config

      it "propagates queue pause via NOTIFY at steady state under one pollInterval" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = pure ()
        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
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

        withAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 10_000 $ (== Running) <$> getWorkerState config
          timed True
          timed False
          timed True
          timed False

      it "claims immediately on unpause without waiting another poll cycle" $ \env -> do
        processedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job =
              liftIO $ atomicModifyIORef' processedRef $ \n -> (n + 1, ())
        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        let config = baseConfig {workerCount = 1, pollInterval = 2.0}

        withAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 10_000 $ (== Running) <$> getWorkerState config

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
        baseA :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        baseB :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        let cfgA = baseA {workerCount = 1, pollInterval = 5.0}
            cfgB = baseB {workerCount = 1, pollInterval = 5.0}
            widA = workerId cfgA
            widB = workerId cfgB

        withAsync (runSimpleDb env $ runWorkerPool cfgA) $ \_ ->
          withAsync (runSimpleDb env $ runWorkerPool cfgB) $ \_ -> do
            -- Wait on the registry rows; getWorkerState reads only TVars and
            -- would return Running before the workers have actually registered.
            waitUntil 10_000 $ do
              rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
              let ids = map WR.workerId rows
              pure (widA `elem` ids && widB `elem` ids)

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
        baseA :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        baseB :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        let cfgA = baseA {workerCount = 1, pollInterval = 5.0}
            cfgB = baseB {workerCount = 1, pollInterval = 5.0}
            widA = workerId cfgA
            widB = workerId cfgB

        withAsync (runSimpleDb env $ runWorkerPool cfgA) $ \_ ->
          withAsync (runSimpleDb env $ runWorkerPool cfgB) $ \_ -> do
            waitUntil 10_000 $ do
              rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
              let ids = map WR.workerId rows
              pure (widA `elem` ids && widB `elem` ids)

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

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        let config = baseConfig {workerCount = 1, pollInterval = 0.2}

        Just job <- runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "long"))

        withAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef

          start <- getCurrentTime
          n <- runSimpleDb env $ Ops.forceCancelJob testSchema testTable (primaryKey job)
          n `shouldBe` 1

          -- Handler should be interrupted well before its 30s sleep finishes.
          waitUntil 5_000 $ do
            mJob <- runSimpleDb env $ HL.getJobById @_ @WorkerTestRegistry @WorkerTestPayload (primaryKey job)
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

        baseConfig :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        let config = baseConfig {workerCount = 1, pollInterval = 0.2}

        Just job <- runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "cpu"))

        withAsync (runSimpleDb env $ runWorkerPool config) $ \_ -> do
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
              [ (defaultJob (SimpleTask "bc-1")) {groupKey = Just "bc"}
              , (defaultJob (SimpleTask "bc-2")) {groupKey = Just "bc"}
              ]
        inserted <- runSimpleDb env $ HL.insertJobsBatch jobs
        let firstId = primaryKey (head inserted)

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler
        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool config {pollInterval = 0.1}) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef

          start <- getCurrentTime
          -- Cancel only the first job. The whole batch thread unwinds.
          n <- runSimpleDb env $ Ops.forceCancelJob testSchema testTable firstId
          n `shouldBe` 1
          waitUntil 5_000 $ do
            mJob <- runSimpleDb env $ HL.getJobById @_ @WorkerTestRegistry @WorkerTestPayload firstId
            pure (isNothing mJob)
          elapsed <- (`diffUTCTime` start) <$> getCurrentTime
          elapsed `shouldSatisfy` (< 3.0)

          -- The handler was interrupted, not run to completion.
          readIORef completedRef `shouldReturn` False
          -- No DLQ entries from the cancel.
          dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          dlqJobs `shouldBe` []

    describe "Sweeper" $ do
      it "deletes a stale unpaused worker row" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema wid testTable Nothing (Just 1) 1 Nothing
        threadDelay 1_500_000
        n <- runSimpleDb env $ Ops.sweepStaleWorkers testSchema
        n `shouldSatisfy` (>= 1)
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        map WR.workerId rows `shouldNotContain` [wid]

      it "deletes a stale paused worker row" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema wid testTable Nothing (Just 1) 1 Nothing
        void $ runSimpleDb env $ Ops.setWorkerPaused testSchema wid True
        threadDelay 1_500_000
        n <- runSimpleDb env $ Ops.sweepStaleWorkers testSchema
        n `shouldSatisfy` (>= 1)
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        map WR.workerId rows `shouldNotContain` [wid]

      it "deletes a stale shutting-down worker row" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema wid testTable Nothing (Just 1) 1 Nothing
        void $ runSimpleDb env $ Ops.markWorkerShuttingDown testSchema wid
        threadDelay 1_500_000
        n <- runSimpleDb env $ Ops.sweepStaleWorkers testSchema
        n `shouldSatisfy` (>= 1)
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        map WR.workerId rows `shouldNotContain` [wid]

      it "preserves paused state across re-registration" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema wid testTable Nothing (Just 1) 300 Nothing
        void $ runSimpleDb env $ Ops.setWorkerPaused testSchema wid True
        -- Re-register with different metadata to confirm upsert touches the row.
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema wid testTable (Just "fresh-host") (Just 1) 300 Nothing
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> do
            WR.paused row `shouldBe` True
            WR.hostName row `shouldBe` Just "fresh-host"
          _ -> expectationFailure "expected exactly one row for the worker"

    describe "Worker health" $ do
      it "reports a freshly-registered worker as live" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema wid testTable Nothing (Just 1) 300 Nothing
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> WR.health row `shouldBe` WR.Live
          _ -> expectationFailure "expected exactly one row for the worker"

      it "reports a worker past its stale threshold as stale" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema wid testTable Nothing (Just 1) 1 Nothing
        threadDelay 1_500_000
        rows <- runSimpleDb env $ Ops.listWorkers testSchema (Just testTable) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> WR.health row `shouldBe` WR.Stale
          _ -> expectationFailure "expected exactly one row for the worker"

      it "reports a fresh shutting-down worker as draining" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void $
          runSimpleDb env $
            Ops.registerWorker testSchema wid testTable Nothing (Just 1) 300 Nothing
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
-- Create shared pool for all tests (5 connections)
withPool :: Pool PG.Connection -> (SimpleEnv WorkerTestRegistry -> IO a) -> IO a
withPool sharedPool action = do
  let env = createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema
  withResource sharedPool $ \conn -> cleanupData testSchema testTable conn
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

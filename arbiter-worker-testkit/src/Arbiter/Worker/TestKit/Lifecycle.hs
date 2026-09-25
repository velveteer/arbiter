{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

-- | Worker lifecycle test suite, instantiated for each 'Arbiter.Core.MonadArbiter.MonadArbiter' backend.
module Arbiter.Worker.TestKit.Lifecycle
  ( lifecycleSpec
  ) where

import Arbiter.Core.Codec (Col (..), pval)
import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Exceptions (throwBranchCancel, throwRetryable)
import Arbiter.Core.HighLevel (QueueOperation, RegistryAdmissionPolicies)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types
  ( JobRead
  , ObservabilityHooks (..)
  , Stored
  , attempts
  , claimSeq
  , claimedBy
  , decodeStored
  , defaultJob
  , defaultObservabilityHooks
  , payload
  , primaryKey
  , setGroupKey
  , setMaxAttempts
  )
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf, ResultOf, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables)
import Arbiter.Core.Queues qualified as Q
import Arbiter.Core.Worker qualified as WR
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Test.Setup (execStatement, execute_, withConn)
import Arbiter.Worker (WorkerState (..), mergedChildResults, runReaperOp, runWorkerPool)
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
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, finally, throwIO, try)
import Control.Monad (replicateM, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson qualified as Aeson
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.Lazy qualified as BL
import Data.Either (isRight)
import Data.Foldable (for_, toList, traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.List (find)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, isNothing)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (diffUTCTime, getCurrentTime)
import Data.UUID.V4 qualified as UUID
import Database.PostgreSQL.Simple (Only (..), query)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Notification (Notification (..), getNotification)
import System.Directory qualified as Dir
import System.Timeout (timeout)
import Test.Hspec
  ( Spec
  , before
  , describe
  , expectationFailure
  , it
  , shouldBe
  , shouldContain
  , shouldMatchList
  , shouldNotContain
  , shouldReturn
  , shouldSatisfy
  )
import UnliftIO.Async (withAsync)
import UnliftIO.Async qualified as Async

import Arbiter.Worker.TestKit.Backend (TestBackend (..))
import Arbiter.Worker.TestKit.Rows (reclaimJob)

-- | Worker lifecycle suite. The queue under test declares @Maybe [Text]@ as its result type.
lifecycleSpec
  :: forall payload m env
   . ( Eq payload
     , QueueOperation m payload
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     , ResultOf m payload ~ Maybe [Text]
     , Show payload
     )
  => TestBackend payload m env
  -> Spec
lifecycleSpec TestBackend {schema, table, connStr, mkSimple, mkEnv, pollOnly, mkHandler, runCommand, runM} =
  before mkEnv $ do
    describe "Reaper op bounding" $ do
      it "completes an op longer than the timeout when each statement is within it" $ \env -> do
        let sleep = runCommand "DO $$ BEGIN PERFORM pg_sleep(0.4); END $$"
        result <-
          runM env $
            runReaperOp silentLogConfig schema 1 "test-reaper-slow-op" 0 $ do
              sleep
              sleep
              sleep
              pure (42 :: Int)
        result `shouldBe` Just 42
      it "aborts a stuck statement at the timeout without killing the caller" $ \env -> do
        result <-
          runM env
            $ runReaperOp silentLogConfig schema 0.5 "test-reaper-stuck-op" 0
            $ runCommand "DO $$ BEGIN PERFORM pg_sleep(5); END $$"
        result `shouldBe` Nothing

    describe "Transactional Atomicity" $ do
      it "rolls back user operations when handler fails" $ \env -> withOpsTable $ do
        let handler :: JobRead payload -> m ()
            handler job = do
              recordOp schema (primaryKey job)
              throwRetryable "Simulated failure"

        void
          $ runM env
          $ HL.insertJob
          $ setMaxAttempts (Just 1)
          $ setGroupKey (Just "g1")
          $ defaultJob (mkSimple "WillFail")

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler (noResult handler))

        withLinkedAsync
          (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1})
          $ \_ -> do
            waitUntil 10_000 $ do
              dlqJobs <- runM env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob payload]
              pure (length dlqJobs == 1)

            dlqJobs <- runM env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob payload]
            length dlqJobs `shouldBe` 1

            count <- opsCount
            count `shouldBe` 0

      it "commits user operations when handler succeeds" $ \env -> withOpsTable $ do
        let handler :: JobRead payload -> m ()
            handler job = recordOp schema (primaryKey job)

        void $ runM env $ HL.insertJob $ setGroupKey (Just "g1") $ defaultJob (mkSimple "WillSucceed")

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler (noResult handler))

        withLinkedAsync
          (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1})
          $ \_ -> do
            waitUntil 10_000 $ (== 1) <$> opsCount

            count <- opsCount
            count `shouldBe` 1

      it "manual commit inside handler persists despite subsequent failure" $ \env -> withOpsTable $ do
        let handler :: JobRead payload -> m ()
            handler job = do
              recordOp schema (primaryKey job)
              -- Manual commit violates the worker transaction boundary.
              runCommand "COMMIT"
              throwRetryable "Simulated failure after commit"

        void
          $ runM env
          $ HL.insertJob
          $ setMaxAttempts (Just 1)
          $ setGroupKey (Just "g1")
          $ defaultJob (mkSimple "ManualCommit")

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler (noResult handler))

        withLinkedAsync
          (runM env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1})
          $ \_ -> do
            waitUntil 10_000 $ do
              dlqJobs <- runM env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob payload]
              pure (length dlqJobs == 1)

            dlqJobs <- runM env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob payload]
            length dlqJobs `shouldBe` 1

            -- User's manual commit survives despite handler failure
            count <- opsCount
            count `shouldBe` 1

    describe "Graceful Shutdown" $ do
      it "graceful shutdown waits for in-flight jobs to complete" $ \env -> do
        -- Track job completion
        completedRef <- newIORef False
        startedRef <- newIORef False

        let handler :: JobRead payload -> m ()
            handler _job = do
              liftIO $ atomicModifyIORef' startedRef $ \_ -> (True, ())
              -- Simulate long-running job
              liftIO $ threadDelay 2_000_000
              liftIO $ atomicModifyIORef' completedRef $ \_ -> (True, ())

        -- Insert a job
        let job = setGroupKey (Just "g1") $ defaultJob (mkSimple "LongJob")
        void $ runM env $ HL.insertJob job

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler (noResult handler))

        let configWithTimeout =
              config
                { workerCount = 1
                , pollInterval = 0.1
                , gracefulShutdownTimeout = Just 10 -- 10 second timeout (plenty of time)
                }

        -- Start worker and wait for job to start processing
        withLinkedAsync (runM env $ runWorkerPool configWithTimeout) $ \worker -> do
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

        let handler :: JobRead payload -> m ()
            handler _job = do
              liftIO $ atomicModifyIORef' startedRef $ \_ -> (True, ())
              -- Very long running job that exceeds timeout
              liftIO $ threadDelay 10_000_000 -- 10 seconds
              liftIO $ atomicModifyIORef' completedRef $ \_ -> (True, ())

        -- Insert a job
        let job = setGroupKey (Just "g1") $ defaultJob (mkSimple "VeryLongJob")
        void $ runM env $ HL.insertJob job

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler (noResult handler))

        let configWithShortTimeout =
              config
                { workerCount = 1
                , pollInterval = 0.05 -- Faster polling for test
                , gracefulShutdownTimeout = Just 1 -- Only 1 second timeout
                }

        withLinkedAsync (runM env $ runWorkerPool configWithShortTimeout) $ \worker -> do
          -- Wait for the job to start processing
          waitUntil 10_000 $ readIORef startedRef

          -- Measure the shutdown duration.
          startTime <- liftIO getCurrentTime
          shutdownWorker configWithShortTimeout
          Async.wait worker
          endTime <- liftIO getCurrentTime

          let elapsed = diffUTCTime endTime startTime
          -- Shutdown takes about 1s, the graceful timeout.
          elapsed `shouldSatisfy` (< 5)

        -- The job did not complete. The timeout cancelled it.
        completed <- readIORef completedRef
        completed `shouldBe` False

    describe "Liveness Probe" $ do
      it "creates a health check file when liveness is enabled" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = liftIO $ threadDelay 500_000

        -- Get system temp directory and create liveness file path
        tmpDir <- Dir.getTemporaryDirectory
        let livenessPath = tmpDir <> "/arbiter-test-liveness"

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler (noResult handler))
        let configWithLiveness =
              config
                { livenessFile = Just livenessPath
                , workerCount = 1
                , pollInterval = 0.1
                }

        withLinkedAsync (runM env $ runWorkerPool configWithLiveness) $ \worker -> do
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
        successRef <- newIORef ([] :: [payload])
        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \seen -> (payload job : seen, ())
                }
        let batchHandler jobs cbs = do
              let batchJobs = toList jobs
              -- Simulate a reclaim of "ca-stolen". A claim bumps both counters. The bulk ack skips it.
              liftIO $
                traverse_
                  (\job -> when (payload job == mkSimple "ca-stolen") $ reclaimJob connStr schema table (primaryKey job))
                  batchJobs
              ackAll cbs batchJobs
        let jobs =
              [ setGroupKey (Just "ca") $ defaultJob (mkSimple "ca-keep1")
              , setGroupKey (Just "ca") $ defaultJob (mkSimple "ca-stolen")
              , setGroupKey (Just "ca") $ defaultJob (mkSimple "ca-keep2")
              ]
        void $ runM env $ HL.insertJobsBatch jobs
        config :: WorkerConfig m payload <- defaultBatchedWorkerConfig 1 10 batchHandler
        withLinkedAsync (runM env $ runWorkerPool (config {pollInterval = 0.05, observabilityHooks = hooks})) $ \_ -> do
          waitUntil 10_000 $ (== 2) . length <$> readIORef successRef
          successes <- readIORef successRef
          -- onJobSuccess fired only for the survivors. The reclaimed job was skipped.
          successes `shouldMatchList` [mkSimple "ca-keep1", mkSimple "ca-keep2"]

      it "rollup ackAllWith stores each job's result for the parent" $ \env -> do
        finalRef <- newIORef ([] :: [Text])
        let resultFor :: payload -> Maybe [Text]
            resultFor task = lookup task [(mkSimple "rb-ca", ["alpha"]), (mkSimple "rb-cb", ["beta"])]
            isReducer task = task == mkSimple "rb-reducer"
            handler jobs cbs =
              if all (isReducer . payload) jobs
                then
                  traverse_
                    ( \parent -> do
                        (merged, _dlq) <- mergedChildResults parent
                        liftIO $ atomicModifyIORef' finalRef $ \_ -> (fromMaybe [] merged, ())
                        ackWith cbs parent merged
                    )
                    (toList jobs)
                else ackAllWith cbs (map (\job -> (job, resultFor (payload job))) (toList jobs))
        runM env
          $ void
          $ HL.insertJobTree
          $ defaultJob (mkSimple "rb-reducer")
            <~~ (defaultJob (mkSimple "rb-ca") :| [defaultJob (mkSimple "rb-cb")])
        config :: WorkerConfig m payload <- defaultBatchedWorkerConfig 1 10 handler
        withLinkedAsync (runM env $ runWorkerPool (config {pollInterval = 0.1})) $ \_ -> do
          waitUntil 10_000 $ (== 2) . length <$> readIORef finalRef
          final <- readIORef finalRef
          final `shouldMatchList` ["alpha", "beta"]

    describe "Fan-out/fan-in with rollup" $ do
      it "worker auto-appends handler results; finalizer reads merged state" $ \env -> do
        finalResultRef <- newIORef ([] :: [Text])

        let handler job
              | payload job == mkSimple "mapper-a" = pure (Just ["sales", "growth"])
              | payload job == mkSimple "mapper-b" = pure (Just ["revenue"])
              | payload job == mkSimple "mapper-c" = pure (Just ["forecast", "trend"])
              | payload job == mkSimple "reducer" = do
                  (merged, _dlq) <- mergedChildResults job
                  liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                  pure merged
              | otherwise = pure Nothing

        -- Insert the rollup tree
        runM env
          $ void
          $ HL.insertJobTree
          $ defaultJob (mkSimple "reducer")
            <~~ ( defaultJob (mkSimple "mapper-a")
                    :| [ defaultJob (mkSimple "mapper-b")
                       , defaultJob (mkSimple "mapper-c")
                       ]
                )

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler handler)

        withLinkedAsync
          ( runM env $
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

        let handler job
              | payload job == mkSimple "mapper-1a" = pure (Just ["sales", "growth"])
              | payload job == mkSimple "mapper-1b" = pure (Just ["revenue"])
              | payload job == mkSimple "mapper-2a" = pure (Just ["forecast"])
              | payload job == mkSimple "mapper-2b" = pure (Just ["trend"])
              | payload job == mkSimple "section-1" = fst <$> mergedChildResults job
              | payload job == mkSimple "section-2" = fst <$> mergedChildResults job
              | payload job == mkSimple "root" = do
                  (merged, _dlq) <- mergedChildResults job
                  liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                  pure merged
              | otherwise = pure Nothing

        runM env
          $ void
          $ HL.insertJobTree
          $ JT.rollup (defaultJob (mkSimple "root"))
          $ ( defaultJob (mkSimple "section-1")
                <~~ (defaultJob (mkSimple "mapper-1a") :| [defaultJob (mkSimple "mapper-1b")])
            )
            :| [ defaultJob (mkSimple "section-2")
                   <~~ (defaultJob (mkSimple "mapper-2a") :| [defaultJob (mkSimple "mapper-2b")])
               ]

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler handler)

        withLinkedAsync
          ( runM env $
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

        let handler (job :| _) cbs
              -- ackWith stores the child result for the parent and acks it
              | payload job == mkSimple "child-a" = ackWith cbs job (Just ["alpha"])
              | payload job == mkSimple "child-b" = ackWith cbs job (Just ["beta", "gamma"])
              | payload job == mkSimple "manual-reducer" = do
                  (merged, _dlq) <- mergedChildResults job
                  liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                  ackWith cbs job merged
              | otherwise = ackWith cbs job Nothing

        -- Insert the rollup tree
        runM env
          $ void
          $ HL.insertJobTree
          $ defaultJob (mkSimple "manual-reducer")
            <~~ ( defaultJob (mkSimple "child-a")
                    :| [defaultJob (mkSimple "child-b")]
                )

        config :: WorkerConfig m payload <- defaultBatchedWorkerConfig 3 1 handler

        withLinkedAsync
          ( runM env $
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
        -- complete. Each parent receives its own merged children, keyed by primary
        -- key. No group keys are needed.
        receivedRef <- newIORef (Map.empty :: Map.Map Text [Text])
        batchSizeRef <- newIORef (0 :: Int)

        let reducers = ["reducer-1", "reducer-2"] :: [Text]
            reducerName task = find ((== task) . mkSimple) reducers
            childResult task =
              lookup
                task
                [ (mkSimple "child-1a", ["a1"])
                , (mkSimple "child-1b", ["b1"])
                , (mkSimple "child-2a", ["a2"])
                , (mkSimple "child-2b", ["b2"])
                ]
            handler jobs cbs = do
              let reducerCount = length (filter (isJust . reducerName . payload) (toList jobs))
              when (reducerCount > 0) $
                liftIO $
                  atomicModifyIORef' batchSizeRef $
                    \largest -> (max largest reducerCount, ())
              for_ jobs $ \job -> case reducerName (payload job) of
                Just name -> do
                  (merged, _dlq) <- mergedChildResults job
                  liftIO $ atomicModifyIORef' receivedRef $ \collected -> (Map.insert name (fromMaybe [] merged) collected, ())
                  ackWith cbs job merged
                Nothing -> ackWith cbs job (childResult (payload job))

        -- Two independent rollup trees, all ungrouped. The four children drain in
        -- one ungrouped batch, then both parents unblock and batch together.
        runM env
          $ void
          $ HL.insertJobTree
          $ defaultJob (mkSimple "reducer-1")
            <~~ (defaultJob (mkSimple "child-1a") :| [defaultJob (mkSimple "child-1b")])
        runM env
          $ void
          $ HL.insertJobTree
          $ defaultJob (mkSimple "reducer-2")
            <~~ (defaultJob (mkSimple "child-2a") :| [defaultJob (mkSimple "child-2b")])

        config :: WorkerConfig m payload <- defaultBatchedWorkerConfig 1 10 handler

        withLinkedAsync
          ( runM env $
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

        let handler job
              | payload job == mkSimple "dlq-child-a" = pure (Just ["x"])
              | payload job == mkSimple "dlq-child-b" = pure (Just ["y", "z"])
              | payload job == mkSimple "dlq-reducer" = do
                  attempt <- liftIO $ atomicModifyIORef' attemptRef $ \count -> (count + 1, count + 1)
                  if attempt == 1
                    then throwRetryable "Intentional failure on first attempt"
                    else do
                      (merged, _dlq) <- mergedChildResults job
                      liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                      pure merged
              | otherwise = pure Nothing

        -- Insert the rollup tree
        Right (_parent :| _children) <-
          runM env
            $ HL.insertJobTree
            $ (setMaxAttempts (Just 1) $ defaultJob (mkSimple "dlq-reducer"))
              <~~ ( defaultJob (mkSimple "dlq-child-a")
                      :| [defaultJob (mkSimple "dlq-child-b")]
                  )

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler handler)

        let cfg =
              config
                { workerCount = 3
                , pollInterval = 0.1
                }

        -- Phase 1: Run workers - children succeed, reducer fails → DLQ
        withLinkedAsync (runM env $ runWorkerPool cfg) $ \_ ->
          waitUntil 10_000 $ do
            dlqJobs <- runM env $ HL.listDLQJobs @payload 10 0
            pure $ any (\dlqJob -> payload (DLQ.jobSnapshot dlqJob) == mkSimple "dlq-reducer") dlqJobs

        -- Verify reducer is in DLQ with snapshot
        dlqJobs <- runM env $ HL.listDLQJobs @payload 10 0
        let reducerDlq = find (\dlqJob -> payload (DLQ.jobSnapshot dlqJob) == mkSimple "dlq-reducer") dlqJobs

        -- Phase 2: Retry from DLQ - reducer should see preserved results from snapshot
        dlqId <- maybe (fail "dlq-reducer is not in the DLQ") (pure . DLQ.dlqPrimaryKey) reducerDlq
        mRetried <- runM env $ HL.retryFromDLQ @payload dlqId
        case mRetried of
          Nothing -> expectationFailure "retryFromDLQ returned Nothing"
          Just retried -> payload retried `shouldBe` mkSimple "dlq-reducer"

        withLinkedAsync (runM env $ runWorkerPool cfg) $ \_ ->
          waitUntil 10_000 $ not . null <$> readIORef finalResultRef

        -- The retried reducer should have received the preserved child results
        finalResult <- readIORef finalResultRef
        length finalResult `shouldBe` 3
        finalResult `shouldMatchList` ["x", "y", "z"]

      it "DLQ tree recovery: both parent and child in DLQ" $ \env -> do
        attemptRef <- newIORef (0 :: Int)
        finalResultRef <- newIORef ([] :: [Text])

        let handler job
              | payload job == mkSimple "recover-child-ok" = pure (Just ["alpha"])
              | payload job == mkSimple "recover-child-fail" = throwRetryable "Permanent child failure"
              | payload job == mkSimple "recover-reducer" = do
                  attempt <- liftIO $ atomicModifyIORef' attemptRef $ \count -> (count + 1, count + 1)
                  if attempt == 1
                    then throwRetryable "Reducer fails first time"
                    else do
                      (merged, _dlq) <- mergedChildResults job
                      liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (fromMaybe [] merged, ())
                      pure merged
              | otherwise = pure Nothing

        -- Insert rollup tree: reducer + 2 children
        Right (_parent :| _children) <-
          runM env
            $ HL.insertJobTree
            $ (setMaxAttempts (Just 1) $ defaultJob (mkSimple "recover-reducer"))
              <~~ ( defaultJob (mkSimple "recover-child-ok")
                      :| [setMaxAttempts (Just 1) $ defaultJob (mkSimple "recover-child-fail")]
                  )

        -- Phase 1: Worker runs - child-ok succeeds, child-fail DLQs, reducer wakes, reducer DLQs
        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler handler)

        let cfg =
              config
                { workerCount = 3
                , pollInterval = 0.1
                }

        withLinkedAsync (runM env $ runWorkerPool cfg) $ \_ ->
          waitUntil 15_000 $ do
            dlqJobs <- runM env $ HL.listDLQJobs @payload 10 0
            pure (length dlqJobs == 2)

        -- Both child-fail and reducer should be in DLQ
        dlqJobs <- runM env $ HL.listDLQJobs @payload 10 0
        let dlqPayloads = map (payload . DLQ.jobSnapshot) dlqJobs
        dlqPayloads `shouldContain` [mkSimple "recover-child-fail"]
        dlqPayloads `shouldContain` [mkSimple "recover-reducer"]

        -- Phase 2: Retry child-fail from DLQ → auto-retries reducer (suspended)
        childDlq <-
          maybe (fail "recover-child-fail is not in the DLQ") pure $
            find (\dlqJob -> payload (DLQ.jobSnapshot dlqJob) == mkSimple "recover-child-fail") dlqJobs
        mRetried <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey childDlq)
        case mRetried of
          Nothing -> expectationFailure "retryFromDLQ returned Nothing"
          Just retried -> payload retried `shouldBe` mkSimple "recover-child-fail"

        -- Phase 3: Run workers again - child-fail still fails, goes back to DLQ,
        -- but reducer wakes with partial results from snapshot
        withLinkedAsync (runM env $ runWorkerPool cfg) $ \_ ->
          waitUntil 15_000 $ not . null <$> readIORef finalResultRef

        -- The retried reducer (second attempt) should have received at least child-ok's result
        finalResult <- readIORef finalResultRef
        finalResult `shouldBe` ["alpha"]

    describe "Worker Registry" $ do
      it "registers, stamps claimed_by, and reconciles pause from the registry" $ \env -> do
        processedRef <- newIORef (0 :: Int)
        let handler :: JobRead payload -> m ()
            handler _job =
              liftIO $ atomicModifyIORef' processedRef $ \count -> (count + 1, ())

        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 2 (mkHandler (noResult handler))
        let config = baseConfig {workerCount = 2, pollInterval = 0.1}
            wid = workerId config

        void $ runM env $ HL.insertJob (defaultJob (mkSimple "first"))

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ (>= 1) <$> readIORef processedRef

          rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
          map WR.workerId rows `shouldContain` [wid]
          map WR.queueName rows `shouldContain` [table]

          -- Pause the worker before the next claim.
          void $ runM env $ Ops.setWorkerPaused schema wid True
          waitUntil 5_000 $ (== Paused) <$> getWorkerState config

          -- Insert and manually claim to read back claimed_by.
          void $ runM env $ HL.insertJob (defaultJob (mkSimple "attribution"))
          claimed <-
            runM env $
              Ops.claimNextVisibleJobsAs @_ @payload schema table 1 60 wid
          case claimed of
            (claimedJob : _) -> claimedBy claimedJob `shouldBe` Just wid
            [] -> expectationFailure "expected a job to be claimable for the claimed_by assertion"

          void $ runM env $ Ops.setWorkerPaused schema wid False
          waitUntil 5_000 $ (== Running) <$> getWorkerState config

      it "re-registers if the registry row is swept out from under it" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = pure ()

        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config =
              baseConfig
                { workerCount = 1
                , pollInterval = 0.1
                , workerHeartbeatInterval = 0.2
                }
            wid = workerId config

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ do
            rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
            pure $ wid `elem` map WR.workerId rows

          _ <- runM env $ Ops.deregisterWorker schema wid
          rowsAfterDelete <- runM env $ Ops.listWorkers schema (Just table) Nothing
          map WR.workerId rowsAfterDelete `shouldNotContain` [wid]

          -- Insert a job. The dispatcher then signals the heartbeat.
          void $ runM env $ HL.insertJob (defaultJob (mkSimple "wake"))

          waitUntil 5_000 $ do
            rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
            pure $ wid `elem` map WR.workerId rows

      it "paused worker keeps heartbeating and survives the sweeper" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = pure ()

        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config =
              baseConfig
                { workerCount = 1
                , pollInterval = 0.1
                , workerHeartbeatInterval = 0.2
                , workerStaleThreshold = 1
                }
            wid = workerId config

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ do
            rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
            pure $ wid `elem` map WR.workerId rows

          void $ runM env $ Ops.setWorkerPaused schema wid True
          waitUntil 5_000 $ (== Paused) <$> getWorkerState config

          -- Sit past stale_threshold_secs while paused.
          threadDelay 2_000_000
          void $ runM env $ Ops.sweepStaleWorkers schema

          rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
          map WR.workerId rows `shouldContain` [wid]

      it "re-registers with the queue's pause state" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = pure ()
        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config = baseConfig {workerCount = 1, pollInterval = 5.0, workerHeartbeatInterval = 2.0}
            wid = workerId config
            registered = do
              rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
              pure $ wid `elem` map WR.workerId rows

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ (== Running) <$> getWorkerState config
          waitUntil 10_000 $ getListenerReady config
          -- Let the tick the startup claim signalled pass.
          threadDelay 2_500_000

          void $ runM env $ Ops.deregisterWorker schema wid
          -- The pause fans out per registry row. A worker without one hears nothing.
          void $ runM env $ Ops.setQueuePaused schema table True
          threadDelay 300_000
          getWorkerState config `shouldReturn` Running

          -- A claim signals the heartbeat, whose tick finds the row gone and re-registers.
          void $ runM env $ HL.insertJob (defaultJob (mkSimple "wake"))
          waitUntil 5_000 registered
          waitUntil 500 $ (== Paused) <$> getWorkerState config
          void $ runM env $ Ops.setQueuePaused schema table False

      it "starts paused when the registry insert fails, then registers on a later heartbeat" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = pure ()
        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config = baseConfig {workerCount = 1, pollInterval = 0.1, workerHeartbeatInterval = 0.2}
            wid = workerId config
            registry = schema <> ".arbiter_workers"
            hidden = schema <> ".arbiter_workers_hidden"

        conn <- PG.connectPostgreSQL connStr
        execute_ conn ("ALTER TABLE " <> registry <> " RENAME TO arbiter_workers_hidden")
        let restore = execute_ conn ("ALTER TABLE " <> hidden <> " RENAME TO arbiter_workers")
        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          (threadDelay 500_000 *> getWorkerState config) `finally` restore >>= (`shouldBe` Paused)
          waitUntil 5_000 $ (== Running) <$> getWorkerState config
          rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
          map WR.workerId rows `shouldContain` [wid]
        PG.close conn

    describe "Queue pause" $ do
      it "stamps paused_at on first pause and clears it on resume" $ \env -> do
        void $ runM env $ Ops.ensureQueue schema table
        void $ runM env $ Ops.setQueuePaused schema table True
        Just row1 <- runM env $ Ops.getQueue schema table
        Q.paused row1 `shouldBe` True
        Q.pausedAt row1 `shouldSatisfy` isJust

        void $ runM env $ Ops.setQueuePaused schema table False
        Just row2 <- runM env $ Ops.getQueue schema table
        Q.paused row2 `shouldBe` False
        Q.pausedAt row2 `shouldBe` Nothing

      it "preserves paused_at on idempotent re-pause" $ \env -> do
        void $ runM env $ Ops.ensureQueue schema table
        void $ runM env $ Ops.setQueuePaused schema table True
        Just first <- runM env $ Ops.getQueue schema table
        let original = Q.pausedAt first
        original `shouldSatisfy` isJust

        threadDelay 1_100_000 -- 1.1s, enough for NOW() to differ
        void $ runM env $ Ops.setQueuePaused schema table True
        Just second <- runM env $ Ops.getQueue schema table
        Q.pausedAt second `shouldBe` original

      it "lists workers filtered by liveness across all queues" $ \env -> do
        liveWid <- liftIO UUID.nextRandom
        staleWid <- liftIO UUID.nextRandom
        void
          $ runM env
          $ Ops.registerWorker schema liveWid table Nothing (Just 1) 300 Nothing
        void
          $ runM env
          $ Ops.registerWorker schema staleWid table Nothing (Just 1) 300 Nothing

        -- Age both rows past the query threshold, then bump only the live row.
        threadDelay 1_200_000
        void $ runM env $ Ops.heartbeatWorker schema liveWid

        -- No liveness filter returns both rows.
        allRows <- runM env $ Ops.listWorkers schema Nothing Nothing
        let allIds = map WR.workerId allRows
        allIds `shouldSatisfy` (liveWid `elem`)
        allIds `shouldSatisfy` (staleWid `elem`)

        -- Queueless live filter at 1s threshold keeps the freshly-heartbeated row only.
        liveOnly <- runM env $ Ops.listWorkers schema Nothing (Just 1)
        let liveIds = map WR.workerId liveOnly
        liveIds `shouldSatisfy` (liveWid `elem`)
        liveIds `shouldSatisfy` (staleWid `notElem`)

      it "propagates queue pause to local pauseVar via heartbeat reconcile" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = pure ()
        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config = baseConfig {workerCount = 1, pollInterval = 0.1}

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ (== Running) <$> getWorkerState config

          void $ runM env $ Ops.setQueuePaused schema table True
          waitUntil 5_000 $ (== Paused) <$> getWorkerState config

          void $ runM env $ Ops.setQueuePaused schema table False
          waitUntil 5_000 $ (== Running) <$> getWorkerState config

      it "propagates queue pause via NOTIFY at steady state under one pollInterval" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = pure ()
        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config = baseConfig {workerCount = 1, pollInterval = 5.0}

            -- Steady-state toggles complete via NOTIFY. The next heartbeat tick
            -- is one pollInterval away.
            timed paused = do
              let expected = if paused then Paused else Running
              start <- getCurrentTime
              void $ runM env $ Ops.setQueuePaused schema table paused
              waitUntil 5_000 $ (== expected) <$> getWorkerState config
              elapsed <- (`diffUTCTime` start) <$> getCurrentTime
              elapsed `shouldSatisfy` (< 1.0)

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 10_000 $ (== Running) <$> getWorkerState config
          waitUntil 10_000 $ getListenerReady config
          timed True
          timed False
          timed True
          timed False

      it "keeps a pause NOTIFY that lands while a heartbeat reading is in flight" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = pure ()
        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config = baseConfig {workerCount = 1, pollInterval = 5.0, workerHeartbeatInterval = 1.0}

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 10_000 $ (== Running) <$> getWorkerState config
          waitUntil 10_000 $ getListenerReady config

          -- Holding the worker's registry row blocks the pool's next heartbeat.
          -- Its reading of the queue's pause state predates the pause below.
          released <- newEmptyMVar
          let holdRow = runM env $ withDbTransaction $ do
                void $ Ops.heartbeatWorker schema (workerId config)
                liftIO $ takeMVar released
          withAsync holdRow $ \_ -> do
            threadDelay 1_500_000
            void $ runM env $ Ops.setQueuePaused schema table True
            waitUntil 5_000 $ (== Paused) <$> getWorkerState config
            putMVar released ()
            -- The blocked heartbeat completes here, one interval before the next.
            threadDelay 500_000
            getWorkerState config `shouldReturn` Paused

      it "claims immediately on unpause without waiting another poll cycle" $ \env -> do
        processedRef <- newIORef (0 :: Int)
        let handler :: JobRead payload -> m ()
            handler _job =
              liftIO $ atomicModifyIORef' processedRef $ \count -> (count + 1, ())
        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config = baseConfig {workerCount = 1, pollInterval = 2.0}

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 10_000 $ (== Running) <$> getWorkerState config
          waitUntil 10_000 $ getListenerReady config

          void $ runM env $ Ops.setQueuePaused schema table True
          waitUntil 5_000 $ (== Paused) <$> getWorkerState config

          void $ runM env $ HL.insertJob (defaultJob (mkSimple "post-resume"))

          start <- getCurrentTime
          void $ runM env $ Ops.setQueuePaused schema table False
          waitUntil 10_000 $ (>= 1) <$> readIORef processedRef
          elapsed <- (`diffUTCTime` start) <$> getCurrentTime
          elapsed `shouldSatisfy` (< 3.0)

      it "setWorkerPaused only targets the addressed worker" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = pure ()
        baseA :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        baseB :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let cfgA = baseA {workerCount = 1, pollInterval = 5.0}
            cfgB = baseB {workerCount = 1, pollInterval = 5.0}
            widA = workerId cfgA
            widB = workerId cfgB

        withLinkedAsync (runM env $ runWorkerPool cfgA) $ \_ ->
          withLinkedAsync (runM env $ runWorkerPool cfgB) $ \_ -> do
            -- Wait on the registry rows. getWorkerState reads only TVars.
            waitUntil 10_000 $ do
              rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
              let ids = map WR.workerId rows
              pure (widA `elem` ids && widB `elem` ids)
            -- And on subscription, before the pause NOTIFY is sent.
            waitUntil 10_000 $ getListenerReady cfgA
            waitUntil 10_000 $ getListenerReady cfgB

            start <- getCurrentTime
            void $ runM env $ Ops.setWorkerPaused schema widA True
            waitUntil 5_000 $ (== Paused) <$> getWorkerState cfgA
            elapsed <- (`diffUTCTime` start) <$> getCurrentTime
            elapsed `shouldSatisfy` (< 1.0)

            stateB <- getWorkerState cfgB
            stateB `shouldBe` Running

      it "setQueuePaused fans out to every worker in the queue" $ \env -> do
        let handler :: JobRead payload -> m ()
            handler _job = pure ()
        baseA :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        baseB :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let cfgA = baseA {workerCount = 1, pollInterval = 5.0}
            cfgB = baseB {workerCount = 1, pollInterval = 5.0}
            widA = workerId cfgA
            widB = workerId cfgB

        withLinkedAsync (runM env $ runWorkerPool cfgA) $ \_ ->
          withLinkedAsync (runM env $ runWorkerPool cfgB) $ \_ -> do
            waitUntil 10_000 $ do
              rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
              let ids = map WR.workerId rows
              pure (widA `elem` ids && widB `elem` ids)
            -- And on subscription, before the pause NOTIFY is sent.
            waitUntil 10_000 $ getListenerReady cfgA
            waitUntil 10_000 $ getListenerReady cfgB

            start <- getCurrentTime
            void $ runM env $ Ops.setQueuePaused schema table True
            waitUntil 5_000 $ (== Paused) <$> getWorkerState cfgA
            waitUntil 5_000 $ (== Paused) <$> getWorkerState cfgB
            elapsed <- (`diffUTCTime` start) <$> getCurrentTime
            elapsed `shouldSatisfy` (< 1.0)

    describe "Force cancel" $ do
      it "interrupts a long-running handler and removes the job" $ \env -> do
        startedRef <- newIORef False
        completedRef <- newIORef False
        let handler :: JobRead payload -> m ()
            handler _job = do
              liftIO $ writeIORef startedRef True
              liftIO $ threadDelay 30_000_000
              liftIO $ writeIORef completedRef True

        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config = baseConfig {workerCount = 1, pollInterval = 0.2}

        Just job <- runM env $ HL.insertJob (defaultJob (mkSimple "long"))

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef

          start <- getCurrentTime
          cancelled <- runM env $ Ops.forceCancelJob schema table (primaryKey job)
          cancelled `shouldBe` 1

          -- Handler should be interrupted well before its 30s sleep finishes.
          waitUntil 5_000 $ do
            mJob <- runM env $ HL.getJobById @payload (primaryKey job)
            pure (isNothing mJob)
          elapsed <- (`diffUTCTime` start) <$> getCurrentTime
          elapsed `shouldSatisfy` (< 3.0)

          -- The handler did not run to completion.
          completed <- readIORef completedRef
          completed `shouldBe` False

          -- The cancel produced no DLQ entry.
          dlqJobs <- runM env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob payload]
          dlqJobs `shouldBe` []

      it "interrupts a CPU-bound handler (no DB I/O)" $ \env -> do
        -- The handler runs a tight IORef-bumping loop with no blocking I/O. The
        -- test samples the counter twice after the force-cancel. The loop has no
        -- interruptible point. A masked child keeps incrementing.
        startedRef <- newIORef False
        counterRef <- newIORef (0 :: Int)
        let handler :: JobRead payload -> m ()
            handler _job = do
              liftIO $ writeIORef startedRef True
              let go = do
                    atomicModifyIORef' counterRef (\count -> (count + 1, ()))
                    go
              liftIO go

        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config = baseConfig {workerCount = 1, pollInterval = 0.2}

        Just job <- runM env $ HL.insertJob (defaultJob (mkSimple "cpu"))

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef
          cancelled <- runM env $ Ops.forceCancelJob schema table (primaryKey job)
          cancelled `shouldBe` 1
          -- Let cancellation propagate.
          threadDelay 500_000
          countBefore <- readIORef counterRef
          threadDelay 500_000
          countAfter <- readIORef counterRef
          -- The counter freezes. A live handler bumps it millions of times in 500ms.
          countAfter `shouldBe` countBefore

      it "cancelling one job of a batch interrupts the whole batch handler" $ \env -> do
        -- A batch runs in a single handler thread. All its job ids point at the
        -- same async. Targeting one job throws into that thread and tears down
        -- the in-flight batch.
        startedRef <- newIORef False
        completedRef <- newIORef False
        let batchHandler _jobs _cbs = do
              liftIO $ writeIORef startedRef True
              liftIO $ threadDelay 30_000_000
              liftIO $ writeIORef completedRef True

        let jobs =
              [ setGroupKey (Just "bc") $ defaultJob (mkSimple "bc-1")
              , setGroupKey (Just "bc") $ defaultJob (mkSimple "bc-2")
              ]
        inserted <- runM env $ HL.insertJobsBatch jobs
        let firstId = primaryKey (head inserted)

        config :: WorkerConfig m payload <- defaultBatchedWorkerConfig 1 10 batchHandler
        threadDelay 100_000

        withLinkedAsync (runM env $ runWorkerPool config {pollInterval = 0.1}) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef

          start <- getCurrentTime
          -- Cancel only the first job. The whole batch thread unwinds.
          cancelled <- runM env $ Ops.forceCancelJob schema table firstId
          cancelled `shouldBe` 1
          waitUntil 5_000 $ do
            mJob <- runM env $ HL.getJobById @payload firstId
            pure (isNothing mJob)
          elapsed <- (`diffUTCTime` start) <$> getCurrentTime
          elapsed `shouldSatisfy` (< 3.0)

          -- The handler was interrupted.
          readIORef completedRef `shouldReturn` False
          -- No DLQ entries from the cancel.
          dlqJobs <- runM env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob payload]
          dlqJobs `shouldBe` []

      it "interrupts a running handler in poll-only mode via the flag" $ \env -> do
        let pollEnv = pollOnly env
        -- With no listener, the heartbeat polls cancel_requested_at and throws
        -- into the handler.
        startedRef <- newIORef False
        completedRef <- newIORef False
        let handler :: JobRead payload -> m ()
            handler _job = do
              liftIO $ writeIORef startedRef True
              liftIO $ threadDelay 30_000_000
              liftIO $ writeIORef completedRef True

        baseConfig :: WorkerConfig m payload <- transactionalWorkerConfig 1 (mkHandler (noResult handler))
        let config =
              baseConfig
                { workerCount = 1
                , pollInterval = 0.2
                , jobHeartbeatInterval = 0.3
                , visibilityTimeout = 3
                }

        Just job <- runM env $ HL.insertJob (defaultJob (mkSimple "poll-cancel"))

        withLinkedAsync (runM pollEnv $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ readIORef startedRef

          start <- getCurrentTime
          cancelled <- runM env $ Ops.forceCancelJob schema table (primaryKey job)
          cancelled `shouldBe` 1

          waitUntil 5_000 $ do
            mJob <- runM env $ HL.getJobById @payload (primaryKey job)
            pure (isNothing mJob)
          elapsed <- (`diffUTCTime` start) <$> getCurrentTime
          elapsed `shouldSatisfy` (< 3.0)

          readIORef completedRef `shouldReturn` False
          dlqJobs <- runM env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob payload]
          dlqJobs `shouldBe` []

      it "flags a job that is claimed concurrently with the force-cancel" $ \env -> do
        Just job <- runM env $ HL.insertJob (defaultJob (mkSimple "concurrent-claim"))
        let jid = primaryKey job
            claimSql =
              fromString . T.unpack $
                "UPDATE "
                  <> schema
                  <> "."
                  <> table
                  <> " SET claimed_by = '00000000-0000-0000-0000-000000000abc'::uuid"
                  <> ", not_visible_until = NOW() + interval '60 second', attempts = attempts + 1, claim_seq = claim_seq + 1 WHERE id = ?"

        connB <- PG.connectPostgreSQL connStr
        void $ PG.execute_ connB "BEGIN"
        void $ PG.execute connB claimSql (Only jid)

        cancelledCount <-
          withAsync (runM env $ Ops.forceCancelJob schema table jid) $ \cancelAsync -> do
            threadDelay 300_000
            void $ PG.execute_ connB "COMMIT"
            PG.close connB
            Async.wait cancelAsync

        cancelledCount `shouldBe` 1
        [Only flagged] <-
          withConn connStr $ \conn ->
            PG.query
              conn
              ( fromString . T.unpack $
                  "SELECT cancel_requested_at IS NOT NULL FROM " <> Schema.jobQueueTable schema table <> " WHERE id = ?"
              )
              (Only jid)
              :: IO [Only Bool]
        flagged `shouldBe` True

      it "notifies the worker when force-cancel deletes its lease-lapsed claimed job" $ \env -> do
        wid <- UUID.nextRandom
        Just job <- runM env $ HL.insertJob (defaultJob (mkSimple "lapsed-cancel"))
        let jid = primaryKey job
        claimed <- runM env (HL.claimNextVisibleJobsAs 1 60 wid) :: IO [JobRead payload]
        length claimed `shouldBe` 1

        void $
          withConn connStr $ \conn ->
            PG.execute
              conn
              ( fromString . T.unpack $
                  "UPDATE " <> Schema.jobQueueTable schema table <> " SET not_visible_until = NOW() - interval '1 second' WHERE id = ?"
              )
              (Only jid)

        lconn <- PG.connectPostgreSQL connStr
        let chan = Schema.cancelNotifyChannel schema table
        void $ PG.execute_ lconn (fromString . T.unpack $ "LISTEN \"" <> chan <> "\"")

        cancelled <- runM env $ Ops.forceCancelJob schema table jid
        cancelled `shouldBe` 1

        runM env (HL.getJobById @payload jid)
          >>= (`shouldSatisfy` isNothing)

        mNotif <- timeout 2_000_000 (getNotification lconn)
        PG.close lconn
        case mNotif of
          Nothing -> expectationFailure "expected a cancel NOTIFY for the deleted lease-lapsed job"
          Just notif -> notificationData notif `shouldSatisfy` BSC.isInfixOf (BSC.pack (show jid))

      it "does not deadlock against a concurrent ack of the last child" $ \env -> do
        Right (parent :| [child]) <-
          runM env
            $ HL.insertJobTree
            $ JT.rollup
              (defaultJob (mkSimple "dl-parent"))
              (JT.leaf (defaultJob (mkSimple "dl-child")) :| [])
        let pid = primaryKey parent
            cid = primaryKey child

        connA <- PG.connectPostgreSQL connStr
        void $ PG.execute_ connA "BEGIN"
        void $ lockRow connA cid

        (efc, epA) <-
          withAsync (try (runM env $ Ops.forceCancelJob schema table pid) :: IO (Either SomeException Int64)) $ \cancelAsync -> do
            threadDelay 300_000
            epA <- lockRow connA pid
            void (try (PG.execute_ connA "COMMIT") :: IO (Either SomeException Int64))
            efc <- Async.wait cancelAsync
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
              [ setMaxAttempts (Just 1) $ setGroupKey (Just "dlb") $ defaultJob (mkSimple "dlb-1")
              , setMaxAttempts (Just 1) $ setGroupKey (Just "dlb") $ defaultJob (mkSimple "dlb-2")
              ]
        inserted <- runM env $ HL.insertJobsBatch jobs
        let ids = map primaryKey inserted
        config :: WorkerConfig m payload <- defaultBatchedWorkerConfig 1 10 batchHandler

        withLinkedAsync
          (runM env $ runWorkerPool config {pollInterval = 0.05, jobHeartbeatInterval = 30, visibilityTimeout = 60})
          $ \_ -> do
            waitUntil 5_000 $ readIORef startedRef

            connA <- PG.connectPostgreSQL connStr
            void $ PG.execute_ connA "BEGIN"
            -- Hold the batch's higher id, the row a force-cancel over the tree takes first.
            void $ lockRow connA (maximum ids)
            putMVar goVar ()
            threadDelay 300_000
            -- The failure transaction does not yet hold the lower id.
            eLo <- lockRow connA (minimum ids)
            void (try (PG.execute_ connA "COMMIT") :: IO (Either SomeException Int64))
            PG.close connA

            eLo `shouldSatisfy` isRight
            waitUntil 10_000 $ do
              dlqJobs <- runM env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob payload]
              pure (length dlqJobs == 2)

      for_ [False, True] $ \nested ->
        it
          ("takes branch-cancel advisory locks before settlement row locks (" <> (if nested then "nested" else "standalone") <> ")")
          $ \env -> do
            (jid, lockId) <-
              if nested
                then do
                  Right (root :| [_, child]) <-
                    runM env
                      $ HL.insertJobTree
                      $ JT.rollup
                        (defaultJob (mkSimple "branch-grandparent"))
                        ( JT.rollup
                            (defaultJob (mkSimple "branch-parent"))
                            (JT.leaf (defaultJob (mkSimple "branch-child")) :| [])
                            :| []
                        )
                  pure (primaryKey child, primaryKey root)
                else do
                  Just job <- runM env $ HL.insertJob (defaultJob (mkSimple "branch-lock-order"))
                  pure (primaryKey job, primaryKey job)
            let handler _jobs _callbacks = throwBranchCancel "cancel branch"
            base :: WorkerConfig m payload <- defaultBatchedWorkerConfig 1 1 handler
            let config = base {pollInterval = 0.05, logConfig = silentLogConfig}
            withConn connStr $ \conn -> do
              void $ PG.execute_ conn "BEGIN"
              flip finally (void $ PG.execute_ conn "ROLLBACK") $ do
                -- An external cancel takes this advisory lock before touching the row.
                void
                  ( PG.query
                      conn
                      "SELECT pg_advisory_xact_lock(hashtextextended(?, ?))::text"
                      (schema <> "." <> table, lockId)
                      :: IO [Only Text]
                  )
                withLinkedAsync (runM (pollOnly env) $ runWorkerPool config) $ \_ -> do
                  -- Wait until settlement requests the advisory lock we hold.
                  waitUntil 5_000 $ do
                    blocked <-
                      PG.query_
                        conn
                        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted AND pg_backend_pid() = ANY(pg_blocking_pids(pid)))"
                        :: IO [Only Bool]
                    pure (blocked == [Only True])
                  -- Settlement must not already hold this row. NOWAIT makes an
                  -- inverted order fail deterministically instead of relying on a victim.
                  result <-
                    try
                      ( PG.query
                          conn
                          (fromString . T.unpack $ "SELECT id FROM " <> Schema.jobQueueTable schema table <> " WHERE id = ? FOR UPDATE NOWAIT")
                          (Only jid)
                          :: IO [Only Int64]
                      )
                      :: IO (Either SomeException [Only Int64])
                  void $ PG.execute_ conn "ROLLBACK"
                  -- Keep the cleanup transaction valid after releasing the held locks.
                  void $ PG.execute_ conn "BEGIN"
                  result `shouldSatisfy` isRight
                  waitUntil 5_000 $ isNothing <$> runM env (HL.getJobById @payload jid)

      it "deletes a flagged job the handler already nacked" $ \env -> do
        -- A nack keeps the claim. A later cancel flags the row.
        nackedRef <- newIORef False
        let jobs =
              [ setGroupKey (Just "fcn") $ defaultJob (mkSimple "fcn-1")
              , setGroupKey (Just "fcn") $ defaultJob (mkSimple "fcn-2")
              ]
        inserted <- runM env $ HL.insertJobsBatch jobs
        let firstId = primaryKey (head inserted)
            batchHandler batch cbs = do
              traverse_ (\job -> when (primaryKey job == firstId) (nack cbs job)) batch
              liftIO $ writeIORef nackedRef True
              liftIO $ threadDelay 30_000_000

        baseConfig :: WorkerConfig m payload <- defaultBatchedWorkerConfig 1 10 batchHandler
        let config = baseConfig {pollInterval = 0.1, jobHeartbeatInterval = 0.3, visibilityTimeout = 60}

        withLinkedAsync (runM env $ runWorkerPool config) $ \_ -> do
          waitUntil 5_000 $ readIORef nackedRef

          runM env (Ops.forceCancelJob schema table firstId) `shouldReturn` 1

          waitUntil 10_000 $ do
            mJob <- runM env $ HL.getJobById @payload firstId
            pure (isNothing mJob)

          dlqJobs <- runM env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob payload]
          dlqJobs `shouldBe` []

      it "refuses a stale worker's ack after a reclaim and nack restored attempts" $ \env -> do
        -- A nack restores the attempt it consumed. The attempts value repeats across claims.
        staleWorker <- UUID.nextRandom
        holdingWorker <- UUID.nextRandom
        Just job <- runM env $ HL.insertJob (defaultJob (mkSimple "aba"))
        let jid = primaryKey job
            expire =
              fromString . T.unpack $
                "UPDATE " <> Schema.jobQueueTable schema table <> " SET not_visible_until = NOW() - interval '1 second' WHERE id = ?"

        [stale] <- runM env (HL.claimNextVisibleJobsAs 1 60 staleWorker) :: IO [JobRead payload]
        primaryKey stale `shouldBe` jid
        void $ withConn connStr $ \conn -> PG.execute conn expire (Only jid)

        [held] <- runM env (HL.claimNextVisibleJobsAs 1 60 holdingWorker) :: IO [JobRead payload]
        primaryKey held `shouldBe` jid
        runM env (HL.nackJob held) `shouldReturn` 1

        -- The nack put attempts back to what the stale worker recorded. An
        -- attempts-keyed predicate would match here.
        reread <- runM env $ HL.getJobById @payload jid
        fmap attempts reread `shouldBe` Just (attempts stale)

        -- Every finalize the stale worker can still issue matches no row.
        runM env (HL.setVisibilityTimeoutBatch 60 [stale])
          `shouldReturn` [HL.JobReclaimed jid (claimSeq stale) (claimSeq held)]
        runM env (HL.nackJob stale) `shouldReturn` 0
        runM env (HL.ackJob stale) `shouldReturn` 0
        runM env (HL.getJobById @payload jid) >>= (`shouldSatisfy` isJust)

        -- The holding worker still owns it and can finish.
        runM env (HL.ackJob held) `shouldReturn` 1

      it "does not deadlock a tree cancel against a concurrent lock walk" $ \env -> do
        Right (parent :| [child]) <-
          runM env
            $ HL.insertJobTree
            $ JT.rollup
              (defaultJob (mkSimple "tc-parent"))
              (JT.leaf (defaultJob (mkSimple "tc-child")) :| [])
        let pid = primaryKey parent
            cid = primaryKey child

        connA <- PG.connectPostgreSQL connStr
        void $ PG.execute_ connA "BEGIN"
        -- Hold the child, the row a tree cancel takes first.
        void $ lockRow connA cid

        (etc, epA) <-
          withAsync (try (runM env $ Ops.cancelJobTree schema table cid) :: IO (Either SomeException Int64)) $ \cancelAsync -> do
            threadDelay 300_000
            epA <- lockRow connA pid
            void (try (PG.execute_ connA "COMMIT") :: IO (Either SomeException Int64))
            etc <- Async.wait cancelAsync
            pure (etc, epA)
        PG.close connA

        etc `shouldSatisfy` isRight
        epA `shouldSatisfy` isRight

    describe "Sweeper" $ do
      it "deletes a stale unpaused worker row" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runM env
          $ Ops.registerWorker schema wid table Nothing (Just 1) 1 Nothing
        threadDelay 1_500_000
        swept <- runM env $ Ops.sweepStaleWorkers schema
        swept `shouldSatisfy` (>= 1)
        rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
        map WR.workerId rows `shouldNotContain` [wid]

      it "deletes a stale paused worker row" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runM env
          $ Ops.registerWorker schema wid table Nothing (Just 1) 1 Nothing
        void $ runM env $ Ops.setWorkerPaused schema wid True
        threadDelay 1_500_000
        swept <- runM env $ Ops.sweepStaleWorkers schema
        swept `shouldSatisfy` (>= 1)
        rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
        map WR.workerId rows `shouldNotContain` [wid]

      it "deletes a stale shutting-down worker row" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runM env
          $ Ops.registerWorker schema wid table Nothing (Just 1) 1 Nothing
        void $ runM env $ Ops.markWorkerShuttingDown schema wid
        threadDelay 1_500_000
        swept <- runM env $ Ops.sweepStaleWorkers schema
        swept `shouldSatisfy` (>= 1)
        rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
        map WR.workerId rows `shouldNotContain` [wid]

      it "preserves paused state across re-registration" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runM env
          $ Ops.registerWorker schema wid table Nothing (Just 1) 300 Nothing
        void $ runM env $ Ops.setWorkerPaused schema wid True
        -- Re-register with different metadata to confirm upsert touches the row.
        void
          $ runM env
          $ Ops.registerWorker schema wid table (Just "fresh-host") (Just 1) 300 Nothing
        rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> do
            WR.paused row `shouldBe` True
            WR.hostName row `shouldBe` Just "fresh-host"
          _ -> expectationFailure "expected exactly one row for the worker"

    describe "Worker health" $ do
      it "reports a freshly-registered worker as live" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runM env
          $ Ops.registerWorker schema wid table Nothing (Just 1) 300 Nothing
        rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> WR.health row `shouldBe` WR.Live
          _ -> expectationFailure "expected exactly one row for the worker"

      it "reports a worker past its stale threshold as stale" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runM env
          $ Ops.registerWorker schema wid table Nothing (Just 1) 1 Nothing
        threadDelay 1_500_000
        rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> WR.health row `shouldBe` WR.Stale
          _ -> expectationFailure "expected exactly one row for the worker"

      it "reports a fresh shutting-down worker as draining" $ \env -> do
        wid <- liftIO UUID.nextRandom
        void
          $ runM env
          $ Ops.registerWorker schema wid table Nothing (Just 1) 300 Nothing
        void $ runM env $ Ops.markWorkerShuttingDown schema wid
        rows <- runM env $ Ops.listWorkers schema (Just table) Nothing
        case filter ((== wid) . WR.workerId) rows of
          [row] -> WR.health row `shouldBe` WR.Draining
          _ -> expectationFailure "expected exactly one row for the worker"

    describe "Cron schedule defaults" $ do
      it "leaves an unchanged schedule's updated_at alone" $ \env -> do
        let upsert expr = Ops.upsertCronDefault schema "cron-steady" table expr "AllowOverlap" Nothing True
            readBack = runM env $ Ops.getCronScheduleByName schema "cron-steady"
        void $ runM env (upsert "* * * * *")
        Just first <- readBack
        void $ runM env (upsert "* * * * *")
        Just second <- readBack
        CS.updatedAt second `shouldBe` CS.updatedAt first
        void $ runM env (upsert "*/5 * * * *")
        Just third <- readBack
        CS.updatedAt third `shouldSatisfy` (> CS.updatedAt first)

      it "registers a schedule with initiallyEnabled False as disabled" $ \env -> do
        void
          $ runM env
          $ Ops.upsertCronDefault schema "cron-suspended" table "* * * * *" "AllowOverlap" Nothing False
        Just CS.CronScheduleRow {CS.enabled = isEnabled} <-
          runM env $ Ops.getCronScheduleByName schema "cron-suspended"
        isEnabled `shouldBe` False

      it "leaves an existing row's enabled state alone on re-upsert" $ \env -> do
        let readBack = runM env $ Ops.getCronScheduleByName schema "cron-resurrected"
        void
          $ runM env
          $ Ops.upsertCronDefault schema "cron-resurrected" table "* * * * *" "AllowOverlap" Nothing True
        void
          $ runM env
          $ Ops.upsertCronDefault schema "cron-resurrected" table "*/5 * * * *" "AllowOverlap" Nothing False
        Just CS.CronScheduleRow {CS.enabled = isEnabled} <- readBack
        isEnabled `shouldBe` True

    describe "Cron queue filter" $ do
      it "filters cron schedules by queue" $ \env -> do
        let otherQueue = "other_queue"
        runM env $ do
          void $ Ops.upsertCronDefault schema "cron-here" table "* * * * *" "AllowOverlap" Nothing True
          void $ Ops.upsertCronDefault schema "cron-elsewhere" otherQueue "* * * * *" "AllowOverlap" Nothing True

        hereOnly <- runM env $ Ops.listCronSchedules schema (Just table)
        map CS.name hereOnly `shouldContain` ["cron-here"]
        map CS.name hereOnly `shouldNotContain` ["cron-elsewhere"]

        elsewhereOnly <- runM env $ Ops.listCronSchedules schema (Just otherQueue)
        map CS.name elsewhereOnly `shouldContain` ["cron-elsewhere"]
        map CS.name elsewhereOnly `shouldNotContain` ["cron-here"]

        all_ <- runM env $ Ops.listCronSchedules schema Nothing
        map CS.name all_ `shouldSatisfy` (\names -> "cron-here" `elem` names && "cron-elsewhere" `elem` names)

    describe "Undecodable payload" $ do
      it "dead-letters a row its payload type rejects and runs the rest of the claim" $ \env -> do
        completedRef <- newIORef []
        config :: WorkerConfig m payload <-
          transactionalWorkerConfig 10 $
            mkHandler (noResult (\job -> liftIO $ atomicModifyIORef' completedRef (\seen -> (payload job : seen, ()))))
        Just poison <- runM env $ HL.insertJob (defaultJob (mkSimple "poison"))
        void $ runM env $ HL.insertJob (defaultJob (mkSimple "sibling"))
        void $
          withConn connStr $ \conn ->
            PG.execute
              conn
              (fromString . T.unpack $ "UPDATE " <> Schema.jobQueueTable schema table <> " SET payload = '{\"bogus\": 1}' WHERE id = ?")
              (Only (primaryKey poison))

        withAsync (runM env $ runWorkerPool config {workerCount = 2, pollInterval = 0.1, visibilityTimeout = 60}) $ \_ -> do
          waitUntil 5_000 $ (== [mkSimple "sibling"]) <$> readIORef completedRef
          dlq <-
            withConn connStr $ \conn ->
              PG.query_ conn (fromString . T.unpack $ "SELECT job_id, last_error FROM " <> Schema.jobQueueDLQTable schema table)
                :: IO [(Int64, Text)]
          map fst dlq `shouldBe` [primaryKey poison]
          map snd dlq `shouldSatisfy` all (T.isPrefixOf "Failed to decode job payload")

      it "claims again after an all-poison claim rather than waiting for the next poll" $ \env -> do
        completedRef <- newIORef []
        config :: WorkerConfig m payload <-
          transactionalWorkerConfig 10 $
            mkHandler (noResult (\job -> liftIO $ atomicModifyIORef' completedRef (\seen -> (payload job : seen, ()))))
        poison <- replicateM 5 $ runM env $ HL.insertJob (defaultJob (mkSimple "poison"))
        void $ runM env $ HL.insertJob (defaultJob (mkSimple "sibling"))
        void $
          withConn connStr $ \conn ->
            PG.execute
              conn
              ( fromString . T.unpack $
                  "UPDATE " <> Schema.jobQueueTable schema table <> " SET payload = '{\"bogus\": 1}' WHERE id IN ?"
              )
              (Only (PG.In (map primaryKey (catMaybes poison))))

        withAsync (runM env $ runWorkerPool config {workerCount = 2, pollInterval = 30, visibilityTimeout = 60}) $ \_ ->
          waitUntil 5_000 $ (== [mkSimple "sibling"]) <$> readIORef completedRef

      it "keeps the decoded jobs of a claim when a rejected row cannot be dead-lettered" $ \env -> do
        Just poison <- runM env $ HL.insertJob (defaultJob (mkSimple "poison"))
        void $ runM env $ HL.insertJob (defaultJob (mkSimple "sibling"))
        let dlqTbl = Schema.jobQueueDLQTable schema table
            poisonId = T.pack (show (primaryKey poison))
        claimed <-
          withConn connStr $ \conn -> do
            void $
              PG.execute
                conn
                (fromString . T.unpack $ "UPDATE " <> Schema.jobQueueTable schema table <> " SET payload = '{\"bogus\": 1}' WHERE id = ?")
                (Only (primaryKey poison))
            execute_ conn ("ALTER TABLE " <> dlqTbl <> " ADD CONSTRAINT reject_poison CHECK (job_id <> " <> poisonId <> ")")
            (runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload])
              `finally` execute_ conn ("ALTER TABLE " <> dlqTbl <> " DROP CONSTRAINT reject_poison")
        map payload claimed `shouldBe` [mkSimple "sibling"]
        runM env (Ops.listDLQJobs schema table 100 0 :: m [DLQ.DLQJob (Stored payload)]) >>= (`shouldBe` [])
        Just held <- runM env (Ops.getJobById @_ @payload schema table (primaryKey poison))
        claimedBy held `shouldSatisfy` isJust

      it "lists and encodes a row its payload type rejects, decoding only on request" $ \env -> do
        Just poison <- runM env $ HL.insertJob (defaultJob (mkSimple "poison"))
        void $
          withConn connStr $ \conn ->
            PG.execute
              conn
              (fromString . T.unpack $ "UPDATE " <> Schema.jobQueueTable schema table <> " SET payload = '{\"bogus\": 1}' WHERE id = ?")
              (Only (primaryKey poison))
        moved <- runM env $ Ops.moveToDLQ Ops.TakeLocks schema table "poison" poison
        moved `shouldBe` 1

        [entry] <- runM env (Ops.listDLQJobs schema table 100 0) :: IO [DLQ.DLQJob (Stored payload)]
        let row = DLQ.jobSnapshot entry
        primaryKey row `shouldBe` primaryKey poison
        -- JSONB spaces its output. A payload spliced from the row keeps that spacing.
        BL.toStrict (Aeson.encode entry) `shouldSatisfy` BSC.isInfixOf "\"payload\":{\"bogus\": 1}"
        Aeson.toJSON (payload row) `shouldBe` Aeson.object ["bogus" Aeson..= (1 :: Int)]
        decodeStored (payload row) `shouldSatisfy` either (T.isPrefixOf "Failed to decode job payload") (const False)
        case Aeson.eitherDecode (Aeson.encode row) :: Either String (JobRead (Stored payload)) of
          Left err -> expectationFailure err
          Right decoded -> Aeson.toJSON (payload decoded) `shouldBe` Aeson.toJSON (payload row)
  where
    lockRow = lockJobRow schema table
    withOpsTable :: IO a -> IO a
    withOpsTable = withTestOpsTable connStr schema
    opsCount = queryOpsCount connStr schema

noResult :: (Functor n) => (j -> n ()) -> j -> n (Maybe [Text])
noResult handler job = Nothing <$ handler job

opsTable :: Text -> Text
opsTable schema = schema <> ".test_operations"

-- | Record an operation for the job on the handler's connection.
recordOp :: (MonadArbiter n) => Text -> Int64 -> n ()
recordOp schema jobId =
  void $
    execStatement
      ("INSERT INTO " <> opsTable schema <> " (job_id, operation) VALUES (?, ?)")
      [pval CInt8 jobId, pval CText "processed"]

-- | Take a job's row lock on a connection of the test's own.
lockJobRow :: Text -> Text -> PG.Connection -> Int64 -> IO (Either SomeException [Only Int64])
lockJobRow schema table conn jobId = try (PG.query conn lockSql (Only jobId))
  where
    lockSql = fromString . T.unpack $ "SELECT id FROM " <> Schema.jobQueueTable schema table <> " WHERE id = ? FOR UPDATE"

withTestOpsTable :: ByteString -> Text -> IO a -> IO a
withTestOpsTable connStr schema action = do
  withConn connStr $ \conn -> do
    execute_ conn $ "CREATE TABLE IF NOT EXISTS " <> opsTable schema <> " (job_id INT, operation TEXT)"
    execute_ conn $ "TRUNCATE " <> opsTable schema
  result <- action
  withConn connStr $ \conn ->
    execute_ conn $ "DROP TABLE IF EXISTS " <> opsTable schema
  pure result

queryOpsCount :: ByteString -> Text -> IO Int
queryOpsCount connStr schema =
  withConn connStr $ \conn -> do
    [Only count] <-
      query
        conn
        (fromString . T.unpack $ "SELECT COUNT(*) FROM " <> opsTable schema <> " WHERE operation = ?")
        (Only ("processed" :: Text))
        :: IO [Only Int]
    pure count

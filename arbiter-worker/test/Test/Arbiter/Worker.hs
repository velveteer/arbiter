{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Worker (spec) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Exceptions (throwBranchCancel, throwNack, throwPermanent, throwRetryable, throwTreeCancel)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types
  ( Job (..)
  , JobRead
  , ObservabilityHooks (..)
  , defaultJob
  , defaultObservabilityHooks
  )
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.PoolConfig (PoolConfig, poolConfigForWorkers)
import Arbiter.Core.Queues qualified as Q
import Arbiter.Core.Worker qualified as WR
import Arbiter.Simple
  ( SimpleConnectionPool (..)
  , SimpleDb
  , SimpleEnv (..)
  , createSimpleEnvWithConfig
  , createSimpleEnvWithPool
  , runSimpleDb
  )
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (cleanupData, execute_, setupOnce)
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Foldable (for_, toList, traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (fromJust, isJust, isNothing)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes, withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (diffUTCTime, getCurrentTime)
import Data.UUID.V4 qualified as UUID
import Database.PostgreSQL.Simple (Only (..), close, connectPostgreSQL, execute, query)
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
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config
  ( BatchCallbacks
  , WorkerConfig (..)
  , ack
  , ackAll
  , ackAllWith
  , ackWith
  , cancelBranch
  , cancelTree
  , defaultBatchedRollupWorkerConfig
  , defaultBatchedWorkerConfig
  , defaultWorkerConfig
  , failPermanent
  , failRetry
  , getWorkerState
  , nack
  , shutdownWorker
  )
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
    describe "Worker Pool" $ do
      it "processes jobs successfully" $ \env -> do
        -- Track completed jobs
        completedRef <- newIORef []

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = do
              liftIO $ atomicModifyIORef' completedRef $ \jobs -> (payload job : jobs, ())
              pure ()

        -- Insert some jobs
        let jobs =
              [ (defaultJob (SimpleTask "Job 1"))
                  { groupKey = Just "g1"
                  }
              , (defaultJob (SimpleTask "Job 2"))
                  { groupKey = Just "g2"
                  }
              , (defaultJob (SimpleTask "Job 3"))
                  { groupKey = Just "g3"
                  }
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        -- Start worker pool
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        -- Run worker pool with automatic cleanup
        withAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 3
                    , pollInterval = 0.1 -- Poll every 100ms for faster tests
                    }
                )
          )
          $ \_ -> do
            waitUntil 10_000 $ (== 3) . length <$> readIORef completedRef

            -- Check that all jobs were completed
            completed <- readIORef completedRef
            length completed `shouldBe` 3
            completed `shouldMatchList` [SimpleTask "Job 1", SimpleTask "Job 2", SimpleTask "Job 3"]

      it "respects worker count concurrency limit" $ \env -> do
        -- Track concurrent workers
        activeRef <- newIORef (0 :: Int)
        maxActiveRef <- newIORef (0 :: Int)
        completedRef <- newIORef (0 :: Int)

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              -- Increment active count and capture new value atomically
              active <- liftIO $ atomicModifyIORef' activeRef $ \n -> (n + 1, n + 1)

              -- Update max if needed
              liftIO $ atomicModifyIORef' maxActiveRef $ \maxN -> (max maxN active, ())

              -- Simulate work
              liftIO $ threadDelay 500_000 -- 500ms

              -- Decrement active count
              liftIO $ atomicModifyIORef' activeRef $ \n -> (n - 1, ())

              liftIO $ atomicModifyIORef' completedRef $ \n -> (n + 1, ())

        -- Insert jobs (more than worker count)
        let jobs =
              map
                ( \i ->
                    (defaultJob (SimpleTask (T.pack $ "Job " <> show @Int i)))
                      { groupKey = Just (T.pack $ "g" <> show i)
                      }
                )
                [1 .. 10]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        -- Start worker pool with limited workers
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 3
                    , pollInterval = 0.05
                    }
                )
          )
          $ \_ -> do
            waitUntil 10_000 $ (== 10) <$> readIORef completedRef

            -- All jobs should have completed
            completed <- readIORef completedRef
            completed `shouldBe` 10

            -- Concurrency should have reached > 1 (proving parallel execution)
            maxActive <- readIORef maxActiveRef
            maxActive `shouldSatisfy` (> 1)

            -- But never exceeded the worker count
            maxActive `shouldSatisfy` (<= 3)

      let runSaturationScenario :: PoolConfig -> SimpleEnv WorkerTestRegistry -> IO Int
          runSaturationScenario poolCfg checkEnv = do
            let workerCnt = 10
            workerEnv <-
              createSimpleEnvWithConfig
                (Proxy @WorkerTestRegistry)
                connStr
                testSchema
                poolCfg

            let jobNames = ["long-" <> T.pack (show i) | i <- [1 .. workerCnt]]
            runSimpleDb workerEnv $
              forM_ jobNames $ \name ->
                void $ HL.insertJob (defaultJob (SimpleTask name))

            let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
                handler _conn _job = liftIO $ threadDelay 10_000_000 -- well past visibilityTimeout
            config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
              runSimpleDb workerEnv $ defaultWorkerConfig connStr workerCnt handler

            withAsync
              ( runSimpleDb workerEnv $
                  runWorkerPool
                    ( config
                        { workerCount = workerCnt
                        , visibilityTimeout = 3
                        , jobHeartbeatInterval = 1
                        , pollInterval = 0.1
                        , jitter = NoJitter
                        }
                    )
              )
              $ \_ -> do
                threadDelay 8_000_000 -- past the 3s lease
                reclaimed <-
                  runSimpleDb checkEnv $
                    HL.claimNextVisibleJobs
                      @(SimpleDb WorkerTestRegistry IO)
                      @WorkerTestRegistry
                      @WorkerTestPayload
                      workerCnt
                      3
                pure (length reclaimed)

      it "heartbeat keeps leases alive when every worker holds a long transaction" $ \env -> do
        reclaimed <- runSaturationScenario (poolConfigForWorkers 10) env
        reclaimed `shouldBe` 0

      it "retries failed jobs up to max attempts" $ \env -> do
        -- Track attempts per job
        attemptsRef <- newIORef (0 :: Int)

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = do
              attempts <- liftIO $ atomicModifyIORef' attemptsRef $ \n -> (n + 1, n + 1)
              case payload job of
                FailingTask maxFails ->
                  when (attempts < maxFails) $
                    throwRetryable "Not yet!"
                _ -> pure ()

        -- Insert a job that fails twice before succeeding
        let job =
              (defaultJob (FailingTask 3))
                { groupKey = Just "g1"
                }
        void $ runSimpleDb env $ HL.insertJob job

        -- Start worker pool
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 1
                    , pollInterval = 0.1
                    , maxAttempts = 5 -- Allow enough retries
                    , jitter = NoJitter -- Predictable timing for test
                    }
                )
          )
          $ \_ -> do
            waitUntil 15_000 $ (== 3) <$> readIORef attemptsRef

            -- Check that the job was attempted 3 times
            attempts <- readIORef attemptsRef
            attempts `shouldBe` 3

      it "moves jobs to DLQ after max attempts" $ \env -> do
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = throwRetryable "Always fails"

        -- Insert a failing job
        let job =
              (defaultJob (SimpleTask "Doomed"))
                { groupKey = Just "g1"
                }
        void $ runSimpleDb env $ HL.insertJob job

        -- Start worker pool with low max attempts
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 1
                    , pollInterval = 0.1
                    , maxAttempts = 1
                    }
                )
          )
          $ \_ -> do
            waitUntil 10_000 $ do
              dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
              pure (length dlqJobs == 1)

            -- Check DLQ
            dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            length dlqJobs `shouldBe` 1

            -- Verify the job in DLQ
            let dlqJob = head dlqJobs
            (payload $ DLQ.jobSnapshot dlqJob) `shouldBe` SimpleTask "Doomed"
            (attempts $ DLQ.jobSnapshot dlqJob) `shouldBe` 1

      it "permanent exception goes straight to DLQ on first attempt" $ \env -> do
        retryCalls <- newIORef (0 :: Int)
        dlqCalls <- newIORef (0 :: Int)

        let hooks =
              defaultObservabilityHooks
                { onJobRetry = \_ _ -> liftIO $ atomicModifyIORef' retryCalls (\n -> (n + 1, ()))
                , onJobFailedAndMovedToDLQ = \_ _ -> liftIO $ atomicModifyIORef' dlqCalls (\n -> (n + 1, ()))
                }

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = throwPermanent "Unrecoverable error"

        void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "PermanentFail"))

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 1
                    , pollInterval = 0.1
                    , maxAttempts = 10 -- High max attempts to prove it's not exhaustion
                    , observabilityHooks = hooks
                    }
                )
          )
          $ \_ -> do
            waitUntil 10_000 $ (== 1) <$> readIORef dlqCalls

            -- Verify: went straight to DLQ, no retry
            retryCount <- readIORef retryCalls
            retryCount `shouldBe` 0

            dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            length dlqJobs `shouldBe` 1
            (payload $ DLQ.jobSnapshot $ head dlqJobs) `shouldBe` SimpleTask "PermanentFail"
            (attempts $ DLQ.jobSnapshot $ head dlqJobs) `shouldBe` 1

    describe "Head-of-Line Blocking" $ do
      it "processes jobs in the same group serially" $ \env -> do
        -- Track job completion order
        orderRef <- newIORef []

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = do
              liftIO $ threadDelay 200_000 -- Small delay
              liftIO $ atomicModifyIORef' orderRef $ \order -> (payload job : order, ())
              pure ()

        -- Insert multiple jobs in the same group
        let jobs =
              [ (defaultJob (SimpleTask "First"))
                  { groupKey = Just "g1"
                  }
              , (defaultJob (SimpleTask "Second"))
                  { groupKey = Just "g1"
                  }
              , (defaultJob (SimpleTask "Third"))
                  { groupKey = Just "g1"
                  }
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        -- Start worker pool
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 3 -- Multiple workers, but should still be serial per group
                    , pollInterval = 0.05
                    }
                )
          )
          $ \_ -> do
            waitUntil 10_000 $ (== 3) . length <$> readIORef orderRef

            -- Check that jobs were processed in order
            order <- readIORef orderRef
            reverse order `shouldBe` [SimpleTask "First", SimpleTask "Second", SimpleTask "Third"]

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

        void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "WillFail")) {groupKey = Just "g1"}

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          (runSimpleDb env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, maxAttempts = 1})
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

        void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "ManualCommit")) {groupKey = Just "g1"}

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          (runSimpleDb env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, maxAttempts = 1})
          $ \_ -> do
            waitUntil 10_000 $ do
              dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
              pure (length dlqJobs == 1)

            dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            length dlqJobs `shouldBe` 1

            -- User's manual commit survives despite handler failure
            count <- queryOpsCount env
            count `shouldBe` 1

      it "skipping a job's completion leaves it in the queue" $ \env -> do
        -- Clean up data from previous tests
        let pool = fromJust (connectionPool (simplePool env)) in withResource pool (cleanupData testSchema testTable)

        processedRef <- newIORef False

        let handler _jobs _cbs = do
              liftIO $ atomicModifyIORef' processedRef (const (True, ()))
              -- Handler succeeds but does NOT complete the job
              pure ()

        -- Insert a job
        Just insertedJob <- runSimpleDb env (HL.insertJob (defaultJob (SimpleTask "ManualAck")))
        let jobId = primaryKey insertedJob

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 1 handler
        let manualConfig = config {pollInterval = 0.1}

        -- Run worker pool briefly
        withAsync (runSimpleDb env $ runWorkerPool manualConfig) $ \_ ->
          waitUntil 10_000 $ readIORef processedRef

        -- Verify the handler ran
        processed <- readIORef processedRef
        processed `shouldBe` True

        -- Verify the job is STILL in the queue (not completed)
        let pool = fromJust (connectionPool (simplePool env))
         in withResource pool $ \conn -> do
              jobRows <-
                query
                  conn
                  (fromString . T.unpack $ "SELECT COUNT(*) FROM " <> Schema.jobQueueTable testSchema testTable <> " WHERE id = ?")
                  (Only jobId)
                  :: IO [Only Int]
              case jobRows of
                [Only count] -> count `shouldBe` 1
                _ -> expectationFailure "Expected one row from COUNT query"

      it "completing a job acks it and fires onJobSuccess" $ \env -> do
        -- Clean up data from previous tests
        let pool = fromJust (connectionPool (simplePool env)) in withResource pool (cleanupData testSchema testTable)

        successRef <- newIORef ([] :: [Int64])

        let handler (job :| _) cbs = ack cbs job
            hooks =
              defaultObservabilityHooks
                { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \js -> (primaryKey job : js, ())
                }

        -- Insert a job
        void $ runSimpleDb env (HL.insertJob (defaultJob (SimpleTask "ManualAckSuccess")))

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 1 handler
        let manualConfig = config {pollInterval = 0.1, observabilityHooks = hooks}

        -- Run worker pool briefly
        withAsync (runSimpleDb env $ runWorkerPool manualConfig) $ \_ ->
          waitUntil 10_000 $ do
            let pool = fromJust (connectionPool (simplePool env))
            withResource pool $ \conn -> do
              jobRows <-
                query conn (fromString . T.unpack $ "SELECT COUNT(*) FROM " <> Schema.jobQueueTable testSchema testTable) ()
                  :: IO [Only Int]
              case jobRows of
                [Only count] -> pure (count == 0)
                _ -> pure False

        -- Verify the job is NOT in the queue anymore (was acked)
        let pool = fromJust (connectionPool (simplePool env))
         in withResource pool $ \conn -> do
              jobRows <-
                query conn (fromString . T.unpack $ "SELECT COUNT(*) FROM " <> Schema.jobQueueTable testSchema testTable) ()
                  :: IO [Only Int]
              case jobRows of
                [Only count] -> count `shouldBe` 0
                _ -> expectationFailure "Expected one row from COUNT query"

        -- onJobSuccess now fires for manual jobs (the behavior this redesign fixes)
        successes <- readIORef successRef
        length successes `shouldBe` 1

      it "completing a reclaimed job skips without firing onJobSuccess" $ \env -> do
        -- Clean up data from previous tests
        let pool = fromJust (connectionPool (simplePool env)) in withResource pool (cleanupData testSchema testTable)

        successRef <- newIORef ([] :: [Int64])

        let handler (job :| _) cbs = do
              -- Simulate the job being reclaimed: ack it out from under completion.
              void $ HL.ackJob job
              ack cbs job
            hooks =
              defaultObservabilityHooks
                { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \js -> (primaryKey job : js, ())
                }

        void $ runSimpleDb env (HL.insertJob (defaultJob (SimpleTask "ManualReclaimed")))

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 1 handler
        let manualConfig = config {pollInterval = 0.1, observabilityHooks = hooks}

        withAsync (runSimpleDb env $ runWorkerPool manualConfig) $ \_ -> do
          -- Wait for the job to be gone (handler acked it).
          waitUntil 10_000 $ do
            let pool = fromJust (connectionPool (simplePool env))
            withResource pool $ \conn -> do
              jobRows <-
                query conn (fromString . T.unpack $ "SELECT COUNT(*) FROM " <> Schema.jobQueueTable testSchema testTable) ()
                  :: IO [Only Int]
              case jobRows of
                [Only count] -> pure (count == 0)
                _ -> pure False
          -- Give any erroneous onJobSuccess time to land.
          threadDelay 200_000

        -- completion threw JobNotFound (ack found 0 rows), so onJobSuccess never fired.
        successes <- readIORef successRef
        length successes `shouldBe` 0

    describe "Observability Hooks" $ do
      it "onJobClaimed is called with start time when job is claimed" $ \env -> do
        claimedRef <- newIORef []

        let hooks =
              defaultObservabilityHooks
                { onJobClaimed = \job startTime -> liftIO $ atomicModifyIORef' claimedRef $ \jobs -> ((primaryKey job, startTime) : jobs, ())
                }

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = pure ()

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1}

        -- Insert a job
        let job = (defaultJob (SimpleTask "Test")) {groupKey = Just "g1"}
        beforeInsert <- getCurrentTime
        void $ runSimpleDb env $ HL.insertJob job

        -- Run worker pool
        withAsync (runSimpleDb env $ runWorkerPool configWithHooks) $ \_ -> do
          waitUntil 10_000 $ (== 1) . length <$> readIORef claimedRef
          claimed <- readIORef claimedRef
          length claimed `shouldBe` 1
          -- Start time should be after job insertion
          let (_, startTime) = head claimed
          startTime `shouldSatisfy` (>= beforeInsert)

      it "onJobSuccess is called with start and end times" $ \env -> do
        successRef <- newIORef []

        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \job startTime endTime ->
                    liftIO $ atomicModifyIORef' successRef $ \results -> ((primaryKey job, startTime, endTime) : results, ())
                }

        -- Handler that takes a measurable amount of time
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ threadDelay 200_000 -- 200ms
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1}

        -- Insert a job
        let job = (defaultJob (SimpleTask "Test")) {groupKey = Just "g1"}
        beforeInsert <- getCurrentTime
        void $ runSimpleDb env $ HL.insertJob job

        -- Run worker pool
        withAsync (runSimpleDb env $ runWorkerPool configWithHooks) $ \_ -> do
          waitUntil 10_000 $ (== 1) . length <$> readIORef successRef
          results <- readIORef successRef
          length results `shouldBe` 1
          let (_, startTime, endTime) = head results
              duration = diffUTCTime endTime startTime
          -- Start time should be after we inserted the job
          startTime `shouldSatisfy` (>= beforeInsert)
          -- End time should be after start time
          endTime `shouldSatisfy` (> startTime)
          -- Duration should reflect the handler's 200ms sleep
          duration `shouldSatisfy` (>= 0.2)

      it "onJobFailure is called with start and end times on failure" $ \env -> do
        failureRef <- newIORef []

        let hooks =
              defaultObservabilityHooks
                { onJobFailure = \job _err startTime endTime ->
                    liftIO $ atomicModifyIORef' failureRef $ \results -> ((primaryKey job, startTime, endTime) : results, ())
                }

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              liftIO $ threadDelay 200_000 -- 200ms before failing
              throwRetryable "Test failure"

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1, maxAttempts = 1}

        -- Insert a job
        let job = (defaultJob (SimpleTask "Test")) {groupKey = Just "g1"}
        beforeInsert <- getCurrentTime
        void $ runSimpleDb env $ HL.insertJob job

        -- Run worker pool
        withAsync (runSimpleDb env $ runWorkerPool configWithHooks) $ \_ -> do
          waitUntil 10_000 $ (== 1) . length <$> readIORef failureRef
          results <- readIORef failureRef
          length results `shouldBe` 1
          let (_, startTime, endTime) = head results
              duration = diffUTCTime endTime startTime
          -- Start time should be after we inserted the job
          startTime `shouldSatisfy` (>= beforeInsert)
          -- End time should be after start time
          endTime `shouldSatisfy` (> startTime)
          -- Duration should reflect the handler's 200ms sleep before failure
          duration `shouldSatisfy` (>= 0.2)

      it "onJobHeartbeat is called periodically during job execution" $ \env -> do
        heartbeatRef <- newIORef (0 :: Int)

        let hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \_ _currentTime _startTime ->
                    liftIO $ atomicModifyIORef' heartbeatRef $ \count -> (count + 1, ())
                }

        -- Handler that takes 3 seconds (should trigger multiple heartbeats)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ threadDelay 3_000_000

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithHooks =
              config
                { observabilityHooks = hooks
                , workerCount = 1
                , pollInterval = 0.1
                , jobHeartbeatInterval = 1 -- 1 second heartbeat
                }

        -- Insert a job
        let job = (defaultJob (SimpleTask "LongRunning")) {groupKey = Just "g1"}
        void $ runSimpleDb env $ HL.insertJob job

        -- Run worker pool
        withAsync (runSimpleDb env $ runWorkerPool configWithHooks) $ \_ -> do
          waitUntil 10_000 $ (>= 2) <$> readIORef heartbeatRef
          heartbeatCount <- readIORef heartbeatRef
          -- Should have at least 2 heartbeats (3 second job with 1 second interval)
          heartbeatCount `shouldSatisfy` (>= 2)

      it "onJobRetry is called with backoff delay on retriable failure" $ \env -> do
        retryRef <- newIORef []

        let hooks =
              defaultObservabilityHooks
                { onJobRetry = \job backoff ->
                    liftIO $ atomicModifyIORef' retryRef $ \rs -> ((primaryKey job, backoff) : rs, ())
                }

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = throwRetryable "retry me"

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1, maxAttempts = 3}

        let job = (defaultJob (SimpleTask "RetryHook")) {groupKey = Just "g1"}
        void $ runSimpleDb env $ HL.insertJob job

        withAsync (runSimpleDb env $ runWorkerPool configWithHooks) $ \_ -> do
          waitUntil 15_000 $ (== 2) . length <$> readIORef retryRef
          retries <- readIORef retryRef
          -- Should have exactly 2 retries (attempts 1 and 2 retry; attempt 3 goes to DLQ)
          length retries `shouldBe` 2
          -- Backoff delays should be positive
          forM_ retries $ \(_, backoff) -> backoff `shouldSatisfy` (> 0)

      it "onJobFailedAndMovedToDLQ is called when job exhausts retries" $ \env -> do
        dlqRef <- newIORef []

        let hooks =
              defaultObservabilityHooks
                { onJobFailedAndMovedToDLQ = \errMsg job ->
                    liftIO $ atomicModifyIORef' dlqRef $ \rs -> ((errMsg, primaryKey job) : rs, ())
                }

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = throwRetryable "always fails"

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithHooks = config {observabilityHooks = hooks, workerCount = 1, pollInterval = 0.1, maxAttempts = 1}

        let job = (defaultJob (SimpleTask "DLQHook")) {groupKey = Just "g1"}
        void $ runSimpleDb env $ HL.insertJob job

        withAsync (runSimpleDb env $ runWorkerPool configWithHooks) $ \_ -> do
          waitUntil 10_000 $ (== 1) . length <$> readIORef dlqRef
          dlqCalls <- readIORef dlqRef
          length dlqCalls `shouldBe` 1
          let (errMsg, _) = head dlqCalls
          errMsg `shouldBe` "always fails"

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
      it "processes a batch of jobs from the same group together" $ \env -> do
        -- Track which batches were processed
        batchesRef <- newIORef []

        let batchHandler jobs cbs = do
              let jobPayloads = map payload (toList jobs)
              liftIO $ atomicModifyIORef' batchesRef $ \batches -> (jobPayloads : batches, ())
              traverse_ (ack cbs) jobs

        -- Insert multiple jobs in the same group
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-3")) {groupKey = Just "g1"}
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        -- Start worker pool in batched mode
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050 -- Longer poll to ensure all jobs are visible
                }

        -- Ensure all jobs are inserted and visible before starting worker
        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) . length <$> readIORef batchesRef

          -- Check that all jobs were processed in a single batch
          batches <- readIORef batchesRef
          length batches `shouldBe` 1
          let processedJobs = head batches
          processedJobs `shouldMatchList` [SimpleTask "G1-1", SimpleTask "G1-2", SimpleTask "G1-3"]

      it "processes multiple groups as separate batches" $ \env -> do
        -- Track which batches were processed
        batchesRef <- newIORef []

        let batchHandler jobs cbs = do
              let jobPayloads = map payload (toList jobs)
              liftIO $ atomicModifyIORef' batchesRef $ \batches -> (jobPayloads : batches, ())
              traverse_ (ack cbs) jobs

        -- Insert jobs in different groups
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G2-1")) {groupKey = Just "g2"}
              , (defaultJob (SimpleTask "G2-2")) {groupKey = Just "g2"}
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 2 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ (== 2) . length <$> readIORef batchesRef

          -- Check that jobs were processed in separate batches per group
          batches <- readIORef batchesRef
          length batches `shouldBe` 2

      it "respects batch size limit" $ \env -> do
        -- Track batch sizes
        batchSizesRef <- newIORef []

        let batchHandler jobs cbs = do
              let batchSize = length jobs
              liftIO $ atomicModifyIORef' batchSizesRef $ \sizes -> (batchSize : sizes, ())
              traverse_ (ack cbs) jobs

        -- Insert 5 jobs in the same group, but batch size is 2
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-3")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-4")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-5")) {groupKey = Just "g1"}
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 2 batchHandler -- Batch size of 2
        let batchedConfig =
              config
                { pollInterval = 0.050
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ (== 3) . length <$> readIORef batchSizesRef

          -- Check that batches respect the size limit
          batchSizes <- readIORef batchSizesRef
          -- Should have processed in batches of 2, 2, and 1
          batchSizes `shouldMatchList` [2, 2, 1]

      it "retries entire batch on failure" $ \env -> do
        -- Track how many times we see each batch
        attemptsRef <- newIORef (0 :: Int)

        let batchHandler jobs cbs = do
              attempts <- liftIO $ atomicModifyIORef' attemptsRef $ \n -> (n + 1, n + 1)
              if attempts < 2 then throwRetryable "Batch failed!" else traverse_ (ack cbs) jobs

        -- Insert a batch of jobs
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050
                , maxAttempts = 3
                , jitter = NoJitter
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 15_000 $ (== 2) <$> readIORef attemptsRef

          -- Check that the batch was attempted twice (failed once, succeeded once)
          attempts <- readIORef attemptsRef
          attempts `shouldBe` 2

      it "moves entire batch to DLQ on max failures" $ \env -> do
        let batchHandler _jobs _cbs = throwRetryable "Always fails"

        -- Insert a batch of jobs
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050
                , maxAttempts = 1
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ do
            dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            pure (length dlqJobs == 2)

          -- Check that all jobs are in DLQ
          dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          length dlqJobs `shouldBe` 2
          map (payload . DLQ.jobSnapshot) dlqJobs `shouldMatchList` [SimpleTask "G1-1", SimpleTask "G1-2"]

      it "uses each job's own maxAttempts in a batch" $ \env -> do
        let batchHandler _jobs _cbs = throwRetryable "Always fails"

        -- Two jobs in one batch with different maxAttempts.
        let jobs =
              [ (defaultJob (SimpleTask "G1-1"))
                  { groupKey = Just "g1"
                  , maxAttempts = Just 2
                  }
              , (defaultJob (SimpleTask "G1-2"))
                  { groupKey = Just "g1"
                  , maxAttempts = Just 3
                  }
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050
                , maxAttempts = 5
                , jitter = NoJitter -- Predictable timing
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 15_000 $ do
            dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            pure (length dlqJobs == 2)

          dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          length dlqJobs `shouldBe` 2
          -- Each job DLQs after its own maxAttempts, not a batch-wide minimum.
          let dlqFor name = head $ filter ((== SimpleTask name) . payload . DLQ.jobSnapshot) dlqJobs
          (attempts . DLQ.jobSnapshot $ dlqFor "G1-1") `shouldBe` 2
          (attempts . DLQ.jobSnapshot $ dlqFor "G1-2") `shouldBe` 3

      it "completed jobs survive a throwNack while the rest reprocess" $ \env -> do
        callCountRef <- newIORef (0 :: Int)
        seenRef <- newIORef ([] :: [[WorkerTestPayload]])
        completedRef <- newIORef ([] :: [WorkerTestPayload])

        let batchHandler jobs cbs = do
              n <- liftIO $ atomicModifyIORef' callCountRef $ \c -> (c + 1, c + 1)
              liftIO $ atomicModifyIORef' seenRef $ \bs -> (map payload (toList jobs) : bs, ())
              let finish j = do
                    ack cbs j
                    liftIO $ atomicModifyIORef' completedRef $ \xs -> (payload j : xs, ())
              if n == 1
                then do
                  -- Complete G1-1, then bail on the rest of the batch.
                  traverse_ finish (filter ((== SimpleTask "G1-1") . payload) (toList jobs))
                  throwNack
                else traverse_ finish jobs

        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-3")) {groupKey = Just "g1"}
              ]
        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler
        let batchedConfig =
              config
                { pollInterval = 0.1
                , visibilityTimeout = 2
                , jobHeartbeatInterval = 1
                }

        threadDelay 100_000
        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          let names = map SimpleTask ["G1-1", "G1-2", "G1-3"]
          waitUntil 15_000 $ do
            completed <- readIORef completedRef
            pure $ all (`elem` completed) names

          seen <- readIORef seenRef
          let occurrences p = length (filter (p `elem`) seen)
          -- G1-1 was completed in the first batch and never reprocessed.
          occurrences (SimpleTask "G1-1") `shouldBe` 1
          -- The jobs left unfinalized by the throw were nacked and reprocessed.
          occurrences (SimpleTask "G1-2") `shouldSatisfy` (>= 2)
          occurrences (SimpleTask "G1-3") `shouldSatisfy` (>= 2)

      it "throwNack in single-job mode reprocesses without recording a failure" $ \env -> do
        callsRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
              when (n == 1) throwNack
        void $ runSimpleDb env $ HL.insertJob (defaultJob (SimpleTask "tn-single"))
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 1 handler
        withAsync
          (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.1, visibilityTimeout = 2, jobHeartbeatInterval = 1}))
          $ \_ -> do
            -- The nacked job comes back for a second attempt, then succeeds.
            waitUntil 15_000 $ (>= 2) <$> readIORef callsRef
            dlq <- runSimpleDb env $ HL.listDLQJobs 100 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            filter (== SimpleTask "tn-single") (map (payload . DLQ.jobSnapshot) dlq) `shouldBe` []

      it "calls heartbeat for all jobs in batch" $ \env -> do
        heartbeatJobsRef <- newIORef []

        let hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \job _currentTime _startTime ->
                    liftIO $ atomicModifyIORef' heartbeatJobsRef $ \jobs -> (primaryKey job : jobs, ())
                }

        -- Handler that takes long enough to trigger heartbeats
        let batchHandler jobs cbs = do
              liftIO $ threadDelay 3_000_000
              traverse_ (ack cbs) jobs

        -- Insert a batch of jobs
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              ]

        insertedJobs <- runSimpleDb env $ HL.insertJobsBatch jobs
        let jobIds = map primaryKey insertedJobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler

        let batchedConfig =
              config
                { observabilityHooks = hooks
                , pollInterval = 0.050
                , jobHeartbeatInterval = 1 -- 1 second heartbeat
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ do
            hbs <- readIORef heartbeatJobsRef
            let job1Count = length $ filter (== head jobIds) hbs
                job2Count = length $ filter (== jobIds !! 1) hbs
            pure (job1Count >= 2 && job2Count >= 2)

          -- Check that heartbeats were called for all jobs in the batch
          heartbeatJobs <- readIORef heartbeatJobsRef
          -- Each job should have at least 2 heartbeats
          let job1Heartbeats = length $ filter (== head jobIds) heartbeatJobs
          let job2Heartbeats = length $ filter (== jobIds !! 1) heartbeatJobs
          job1Heartbeats `shouldSatisfy` (>= 2)
          job2Heartbeats `shouldSatisfy` (>= 2)

      it "a throw fails only the jobs not yet completed" $ \env -> do
        failuresRef <- newIORef ([] :: [Int64])
        successRef <- newIORef ([] :: [Int64])

        let hooks =
              defaultObservabilityHooks
                { onJobFailure = \job _ _ _ ->
                    liftIO $ atomicModifyIORef' failuresRef $ \fs -> (primaryKey job : fs, ())
                , onJobSuccess = \job _ _ ->
                    liftIO $ atomicModifyIORef' successRef $ \ss -> (primaryKey job : ss, ())
                }

        let batchHandler
              :: NonEmpty (JobRead WorkerTestPayload)
              -> BatchCallbacks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
              -> SimpleDb WorkerTestRegistry IO ()
            batchHandler jobs cbs = do
              -- Complete first 2 jobs
              traverse_ (ack cbs) (take 2 (toList jobs))
              -- 3rd job fails
              throwRetryable "Third job failed"

        -- Insert 3 jobs in same group
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-3")) {groupKey = Just "g1"}
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050
                , observabilityHooks = hooks
                , jitter = NoJitter
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) . length <$> readIORef failuresRef

          -- Only 1 failure should be reported (3rd job), not 3
          -- The first 2 jobs were completed before the throw
          failures <- readIORef failuresRef
          length failures `shouldBe` 1

          -- the completion callback fired onJobSuccess for the 2 completed jobs
          successes <- readIORef successRef
          length successes `shouldBe` 2

      it "completing every job in a batch removes them from the queue" $ \env -> do
        processedRef <- newIORef False

        let batchHandler
              :: NonEmpty (JobRead WorkerTestPayload)
              -> BatchCallbacks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
              -> SimpleDb WorkerTestRegistry IO ()
            batchHandler jobs cbs = do
              -- Complete all jobs
              traverse_ (ack cbs) jobs
              liftIO $ atomicModifyIORef' processedRef $ \_ -> (True, ())

        -- Insert batch of jobs
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ readIORef processedRef

          -- Jobs should be processed and acked
          processed <- readIORef processedRef
          processed `shouldBe` True

          -- Jobs should be gone from queue
          remainingJobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
          length remainingJobs `shouldBe` 0

      it "ackAll bulk-acks the batch and fires onJobSuccess for each" $ \env -> do
        successRef <- newIORef ([] :: [Int64])
        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \js -> (primaryKey job : js, ())
                }

        let batchHandler
              :: NonEmpty (JobRead WorkerTestPayload)
              -> BatchCallbacks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
              -> SimpleDb WorkerTestRegistry IO ()
            batchHandler jobs cbs = ackAll cbs (toList jobs)

        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-3")) {groupKey = Just "g1"}
              ]
        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler
        let batchedConfig = config {pollInterval = 0.050, observabilityHooks = hooks}

        threadDelay 100_000
        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ (== 3) . length <$> readIORef successRef

          -- onJobSuccess fired once per job, and all are gone from the queue.
          successes <- readIORef successRef
          length successes `shouldBe` 3
          remaining <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
          length remaining `shouldBe` 0

      it "failPermanent callback DLQs one job while the rest complete" $ \env -> do
        successRef <- newIORef ([] :: [WorkerTestPayload])
        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \xs -> (payload job : xs, ())
                }
        let batchHandler jobs cbs =
              traverse_
                (\j -> if payload j == SimpleTask "fp-bad" then failPermanent cbs j "bad input" else ack cbs j)
                (toList jobs)
        let jobs =
              [ (defaultJob (SimpleTask "fp-good1")) {groupKey = Just "fp"}
              , (defaultJob (SimpleTask "fp-bad")) {groupKey = Just "fp"}
              , (defaultJob (SimpleTask "fp-good2")) {groupKey = Just "fp"}
              ]
        void $ runSimpleDb env $ HL.insertJobsBatch jobs
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler
        withAsync (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.05, observabilityHooks = hooks})) $ \_ -> do
          waitUntil 10_000 $ (== 2) . length <$> readIORef successRef
          dlq <- runSimpleDb env $ HL.listDLQJobs 100 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          filter (== SimpleTask "fp-bad") (map (payload . DLQ.jobSnapshot) dlq) `shouldBe` [SimpleTask "fp-bad"]
          successes <- readIORef successRef
          successes `shouldMatchList` [SimpleTask "fp-good1", SimpleTask "fp-good2"]

      it "failRetry callback retries a job and DLQs it at its maxAttempts" $ \env -> do
        callsRef <- newIORef (0 :: Int)
        let batchHandler jobs cbs = do
              liftIO $ atomicModifyIORef' callsRef (\n -> (n + 1, ()))
              traverse_ (\j -> failRetry cbs j "transient") (toList jobs)
        let jobs = [(defaultJob (SimpleTask "fr-job")) {groupKey = Just "fr", maxAttempts = Just 2}]
        void $ runSimpleDb env $ HL.insertJobsBatch jobs
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler
        withAsync (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.05, jitter = NoJitter})) $ \_ -> do
          waitUntil 15_000 $ do
            dlq <- runSimpleDb env $ HL.listDLQJobs 100 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            pure (any ((== SimpleTask "fr-job") . payload . DLQ.jobSnapshot) dlq)
          dlq <- runSimpleDb env $ HL.listDLQJobs 100 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          let mine = filter ((== SimpleTask "fr-job") . payload . DLQ.jobSnapshot) dlq
          (attempts . DLQ.jobSnapshot $ head mine) `shouldBe` 2
          -- The handler ran twice (retry), not once.
          calls <- readIORef callsRef
          calls `shouldBe` 2

      it "nack callback leaves a job to be reprocessed with no failure" $ \env -> do
        callsRef <- newIORef (0 :: Int)
        let batchHandler jobs cbs = do
              n <- liftIO $ atomicModifyIORef' callsRef (\c -> (c + 1, c + 1))
              if n == 1 then traverse_ (nack cbs) (toList jobs) else traverse_ (ack cbs) (toList jobs)
        let jobs = [(defaultJob (SimpleTask "nk-job")) {groupKey = Just "nk"}]
        void $ runSimpleDb env $ HL.insertJobsBatch jobs
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler
        withAsync
          (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.1, visibilityTimeout = 2, jobHeartbeatInterval = 1}))
          $ \_ -> do
            -- The nacked job comes back for a second attempt.
            waitUntil 15_000 $ (>= 2) <$> readIORef callsRef
            -- It never recorded a failure.
            dlq <- runSimpleDb env $ HL.listDLQJobs 100 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            filter (== SimpleTask "nk-job") (map (payload . DLQ.jobSnapshot) dlq) `shouldBe` []

      it "cancelTree callback deletes the entire tree" $ \env -> do
        Right (root :| _) <-
          runSimpleDb env $
            HL.insertJobTree $
              defaultJob (SimpleTask "ct-root")
                <~~ (defaultJob (SimpleTask "ct-c1") :| [defaultJob (SimpleTask "ct-c2")])
        let rootId = primaryKey root
        let batchHandler jobs cbs = traverse_ (\j -> cancelTree cbs j "abort") (toList jobs)
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler
        withAsync (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.05})) $ \_ ->
          waitUntil 10_000 $ do
            let pool = fromJust (connectionPool (simplePool env))
            withResource pool $ \conn -> do
              rows <-
                query
                  conn
                  (fromString . T.unpack $ "SELECT COUNT(*) FROM " <> Schema.jobQueueTable testSchema testTable <> " WHERE id = ?")
                  (Only rootId)
                  :: IO [Only Int]
              pure (rows == [Only 0])

      it "cancelBranch callback deletes the job's branch" $ \env -> do
        Right (root :| _) <-
          runSimpleDb env $
            HL.insertJobTree $
              defaultJob (SimpleTask "cb-root")
                <~~ (defaultJob (SimpleTask "cb-c1") :| [defaultJob (SimpleTask "cb-c2")])
        let rootId = primaryKey root
        let batchHandler jobs cbs = traverse_ (\j -> cancelBranch cbs j "branch failed") (toList jobs)
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler
        withAsync (runSimpleDb env $ runWorkerPool (config {pollInterval = 0.05})) $ \_ ->
          waitUntil 10_000 $ do
            let pool = fromJust (connectionPool (simplePool env))
            withResource pool $ \conn -> do
              rows <-
                query
                  conn
                  (fromString . T.unpack $ "SELECT COUNT(*) FROM " <> Schema.jobQueueTable testSchema testTable <> " WHERE id = ?")
                  (Only rootId)
                  :: IO [Only Int]
              pure (rows == [Only 0])

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

      it "uncompleted jobs remain in the queue after the handler returns" $ \env -> do
        processedRef <- newIORef False

        let batchHandler
              :: NonEmpty (JobRead WorkerTestPayload)
              -> BatchCallbacks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
              -> SimpleDb WorkerTestRegistry IO ()
            batchHandler jobs cbs = do
              -- Only complete the first job, leave the second untouched
              ack cbs (head $ toList jobs)
              liftIO $ atomicModifyIORef' processedRef $ \_ -> (True, ())

        -- Insert batch of jobs
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"}
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"}
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ ->
          waitUntil 10_000 $ readIORef processedRef

        -- Handler ran
        readIORef processedRef >>= (`shouldBe` True)

        -- The un-acked job should still be in the queue (just invisible until visibility expires)
        let pool = fromJust (connectionPool (simplePool env))
         in withResource pool $ \conn -> do
              jobRows <-
                query
                  conn
                  (fromString . T.unpack $ "SELECT COUNT(*) FROM " <> Schema.jobQueueTable testSchema testTable)
                  ()
                  :: IO [Only Int]
              case jobRows of
                [Only count] -> count `shouldBe` 1
                _ -> expectationFailure "Expected one row from COUNT query"

      it "ungrouped jobs are batched together (not as singletons)" $ \env -> do
        -- Track batch sizes to verify ungrouped jobs are batched together
        batchSizesRef <- newIORef []

        let batchHandler jobs cbs = do
              let batchSize = length jobs
              liftIO $ atomicModifyIORef' batchSizesRef $ \sizes -> (batchSize : sizes, ())
              traverse_ (ack cbs) jobs

        -- Insert multiple ungrouped jobs (group_key = Nothing)
        let jobs =
              [ defaultJob (SimpleTask "Ungrouped-1") -- No groupKey
              , defaultJob (SimpleTask "Ungrouped-2") -- No groupKey
              , defaultJob (SimpleTask "Ungrouped-3") -- No groupKey
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 3 10 batchHandler -- Large batch size, 3 workers
        let batchedConfig =
              config
                { pollInterval = 0.050
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) . length <$> readIORef batchSizesRef

          -- All ungrouped jobs form a single batch
          batchSizes <- readIORef batchSizesRef
          length batchSizes `shouldBe` 1
          batchSizes `shouldMatchList` [3]

      it "mixed grouped and ungrouped jobs are batched correctly" $ \env -> do
        -- Track batches: grouped jobs together, ungrouped separate
        batchPayloadsRef <- newIORef []

        let batchHandler jobs cbs = do
              let payloads = map payload (toList jobs)
              liftIO $ atomicModifyIORef' batchPayloadsRef $ \batches -> (payloads : batches, ())
              traverse_ (ack cbs) jobs

        -- Insert mix of grouped and ungrouped jobs
        let jobs =
              [ (defaultJob (SimpleTask "G1-1")) {groupKey = Just "g1"} -- Group 1
              , (defaultJob (SimpleTask "G1-2")) {groupKey = Just "g1"} -- Group 1
              , defaultJob (SimpleTask "Ungrouped-1") -- Ungrouped
              , defaultJob (SimpleTask "Ungrouped-2") -- Ungrouped
              , (defaultJob (SimpleTask "G2-1")) {groupKey = Just "g2"} -- Group 2
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 4 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ (== 3) . length <$> readIORef batchPayloadsRef

          -- Should have 3 batches:
          -- - Group g1 batch (2 jobs)
          -- - Group g2 batch (1 job)
          -- - Ungrouped batch (2 jobs)
          batches <- readIORef batchPayloadsRef
          length batches `shouldBe` 3

          -- Find the group batches by size
          let batchSizes = map length batches
          batchSizes `shouldMatchList` [2, 2, 1]

          -- Verify g1 jobs are together
          let g1Batch = filter (\b -> SimpleTask "G1-1" `elem` b) batches
          length g1Batch `shouldBe` 1
          head g1Batch `shouldMatchList` [SimpleTask "G1-1", SimpleTask "G1-2"]

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
              defaultJob (SimpleTask "dlq-reducer")
                <~~ ( defaultJob (SimpleTask "dlq-child-a")
                        :| [defaultJob (SimpleTask "dlq-child-b")]
                    )

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        let cfg =
              config
                { workerCount = 3
                , pollInterval = 0.1
                , maxAttempts = 1 -- Go to DLQ after first failure
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
              defaultJob (SimpleTask "recover-reducer")
                <~~ ( defaultJob (SimpleTask "recover-child-ok")
                        :| [defaultJob (SimpleTask "recover-child-fail")]
                    )

        -- Phase 1: Worker runs - child-ok succeeds, child-fail DLQs, reducer wakes, reducer DLQs
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        let cfg =
              config
                { workerCount = 3
                , pollInterval = 0.1
                , maxAttempts = 1
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

    describe "Tree and Branch Cancel" $ do
      it "throwTreeCancel deletes the entire tree (not DLQ'd)" $ \env -> do
        -- 3-level tree: root → mid (rollup) → leaf1, leaf2
        runSimpleDb env $
          void $
            HL.insertJobTree $
              JT.rollup
                (defaultJob (SimpleTask "tc-root"))
                ( JT.rollup
                    (defaultJob (SimpleTask "tc-mid"))
                    (JT.leaf (defaultJob (SimpleTask "tc-leaf1")) :| [JT.leaf (defaultJob (SimpleTask "tc-leaf2"))])
                    :| []
                )

        let handler _conn job = case payload job of
              SimpleTask "tc-leaf1" -> liftIO (throwTreeCancel "abort everything")
              _ -> pure []

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 1
                    , pollInterval = 0.1
                    }
                )
          )
          $ \_ ->
            waitUntil 10_000 $ null <$> (runSimpleDb env $ HL.listJobs @_ @WorkerTestRegistry @WorkerTestPayload 100 0)

        -- Entire tree should be gone (deleted, not DLQ'd)
        let treePayloads =
              [ SimpleTask "tc-root"
              , SimpleTask "tc-mid"
              , SimpleTask "tc-leaf1"
              , SimpleTask "tc-leaf2"
              ]
        jobs <- runSimpleDb env $ HL.listJobs @_ @WorkerTestRegistry @WorkerTestPayload 100 0
        forM_ treePayloads $ \p ->
          map payload jobs `shouldNotContain` [p]

        dlqJobs <- runSimpleDb env $ HL.listDLQJobs @_ @WorkerTestRegistry @WorkerTestPayload 100 0
        forM_ treePayloads $ \p ->
          map (payload . DLQ.jobSnapshot) dlqJobs `shouldNotContain` [p]

      it "throwBranchCancel deletes branch but resumes grandparent" $ \env -> do
        -- 3-level tree: root → mid (rollup) → leaf1, leaf2
        rootProcessedRef <- newIORef False

        runSimpleDb env $
          void $
            HL.insertJobTree $
              JT.rollup
                (defaultJob (SimpleTask "bc-root"))
                ( JT.rollup
                    (defaultJob (SimpleTask "bc-mid"))
                    (JT.leaf (defaultJob (SimpleTask "bc-leaf1")) :| [JT.leaf (defaultJob (SimpleTask "bc-leaf2"))])
                    :| []
                )

        let handler _conn job = case payload job of
              SimpleTask "bc-leaf1" -> liftIO (throwBranchCancel "abort this branch")
              SimpleTask "bc-root" -> do
                liftIO $ atomicModifyIORef' rootProcessedRef $ \_ -> (True, ())
                pure []
              _ -> pure []

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        withAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 1
                    , pollInterval = 0.1
                    }
                )
          )
          $ \_ -> waitUntil 10_000 $ readIORef rootProcessedRef

        -- Root should have been resumed and processed after branch cancel
        readIORef rootProcessedRef `shouldReturn` True

        -- Nothing in DLQ
        dlqJobs <- runSimpleDb env $ HL.listDLQJobs @_ @WorkerTestRegistry @WorkerTestPayload 100 0
        let allPayloads = [SimpleTask "bc-root", SimpleTask "bc-mid", SimpleTask "bc-leaf1", SimpleTask "bc-leaf2"]
        forM_ allPayloads $ \p ->
          map (payload . DLQ.jobSnapshot) dlqJobs `shouldNotContain` [p]

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
createSharedPool :: ByteString -> IO (Pool PG.Connection)
createSharedPool connStr =
  newPool $
    setNumStripes (Just 1) $
      defaultPoolConfig
        (connectPostgreSQL connStr)
        close
        60
        5

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

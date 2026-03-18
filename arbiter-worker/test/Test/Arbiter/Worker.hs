{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Worker (spec) where

import Arbiter.Core.Exceptions (throwBranchCancel, throwPermanent, throwRetryable, throwTreeCancel)
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
import Arbiter.Core.MonadArbiter (BatchedJobHandler, JobHandler)
import Arbiter.Simple (SimpleConnectionPool (..), SimpleDb, SimpleEnv (..), createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (cleanupData, execute_, setupOnce)
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (toJSON)
import Data.ByteString (ByteString)
import Data.Foldable (toList)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (fromJust, fromMaybe)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes, withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (diffUTCTime, getCurrentTime)
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
import UnliftIO.MVar qualified as MVar

import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config
  ( LivenessConfig (..)
  , WorkerConfig (..)
  , defaultBatchedWorkerConfig
  , defaultRollupWorkerConfig
  , defaultWorkerConfig
  , shutdownWorker
  )

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

      it "requires manual ack when useWorkerTransaction = False" $ \env -> do
        -- Clean up data from previous tests
        let pool = fromJust (connectionPool (simplePool env)) in withResource pool (cleanupData testSchema testTable)

        processedRef <- newIORef False

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              liftIO $ atomicModifyIORef' processedRef (const (True, ()))
              -- Handler succeeds but does NOT call ackJob
              pure ()

        -- Insert a job
        Just insertedJob <- runSimpleDb env (HL.insertJob (defaultJob (SimpleTask "ManualAck")))
        let jobId = primaryKey insertedJob

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithoutTx =
              config
                { useWorkerTransaction = False
                , workerCount = 1
                , pollInterval = 0.1
                }

        -- Run worker pool briefly
        withAsync (runSimpleDb env $ runWorkerPool configWithoutTx) $ \_ ->
          waitUntil 10_000 $ readIORef processedRef

        -- Verify the handler ran
        processed <- readIORef processedRef
        processed `shouldBe` True

        -- Verify the job is STILL in the queue (not acked)
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

      it "manual ack removes job when useWorkerTransaction = False" $ \env -> do
        -- Clean up data from previous tests
        let pool = fromJust (connectionPool (simplePool env)) in withResource pool (cleanupData testSchema testTable)

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = do
              -- Handler manually acks the job
              void $ HL.ackJob job

        -- Insert a job
        void $ runSimpleDb env (HL.insertJob (defaultJob (SimpleTask "ManualAckSuccess")))

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithoutTx =
              config
                { useWorkerTransaction = False
                , workerCount = 1
                , pollInterval = 0.1
                }

        -- Run worker pool briefly
        withAsync (runSimpleDb env $ runWorkerPool configWithoutTx) $ \_ ->
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
                , heartbeatInterval = 1 -- 1 second heartbeat
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
        -- Create liveness MVar and run worker pool briefly
        livenessMVar <- MVar.newMVar ()

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ threadDelay 500_000

        -- Get system temp directory and create liveness file path
        tmpDir <- Dir.getTemporaryDirectory
        let livenessPath = tmpDir <> "/arbiter-test-liveness"

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler
        let configWithLiveness =
              config
                { livenessConfig = Just (LivenessConfig livenessPath livenessMVar 1) -- 1 second interval
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

        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn jobs = do
              let jobPayloads = map payload (toList jobs)
              liftIO $ atomicModifyIORef' batchesRef $ \batches -> (jobPayloads : batches, ())
              pure ()

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

        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn jobs = do
              let jobPayloads = map payload (toList jobs)
              liftIO $ atomicModifyIORef' batchesRef $ \batches -> (jobPayloads : batches, ())
              pure ()

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

        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn jobs = do
              let batchSize = length jobs
              liftIO $ atomicModifyIORef' batchSizesRef $ \sizes -> (batchSize : sizes, ())
              pure ()

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

        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn _jobs = do
              attempts <- liftIO $ atomicModifyIORef' attemptsRef $ \n -> (n + 1, n + 1)
              when (attempts < 2) $ throwRetryable "Batch failed!"

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
        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn _jobs = throwRetryable "Always fails"

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

      it "uses minimum maxAttempts across batch" $ \env -> do
        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn _jobs = throwRetryable "Always fails"

        -- Insert jobs with different maxAttempts
        let jobs =
              [ (defaultJob (SimpleTask "G1-1"))
                  { groupKey = Just "g1"
                  , maxAttempts = Just 3 -- Lower
                  }
              , (defaultJob (SimpleTask "G1-2"))
                  { groupKey = Just "g1"
                  , maxAttempts = Just 10 -- Higher
                  }
              ]

        void $ runSimpleDb env $ HL.insertJobsBatch jobs

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultBatchedWorkerConfig connStr 1 10 batchHandler

        let batchedConfig =
              config
                { pollInterval = 0.050
                , maxAttempts = 5 -- Default, but should use minimum of job-specific
                , jitter = NoJitter -- Predictable timing
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          -- Wait for 3 attempts with exponential backoff (2s + 4s + processing)
          waitUntil 15_000 $ do
            dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
            pure (length dlqJobs == 2)

          -- Both jobs should be in DLQ after 3 attempts (minimum maxAttempts)
          -- not after 10 attempts (maximum) or 5 (config default)
          dlqJobs <- runSimpleDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob WorkerTestPayload]
          length dlqJobs `shouldBe` 2
          -- Verify both jobs attempted exactly 3 times (the minimum)
          let attempts1 = attempts . DLQ.jobSnapshot $ head dlqJobs
          let attempts2 = attempts . DLQ.jobSnapshot $ dlqJobs !! 1
          attempts1 `shouldBe` 3
          attempts2 `shouldBe` 3

      it "calls heartbeat for all jobs in batch" $ \env -> do
        heartbeatJobsRef <- newIORef []

        let hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \job _currentTime _startTime ->
                    liftIO $ atomicModifyIORef' heartbeatJobsRef $ \jobs -> (primaryKey job : jobs, ())
                }

        -- Handler that takes long enough to trigger heartbeats
        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn _jobs = liftIO $ threadDelay 3_000_000

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
                , heartbeatInterval = 1 -- 1 second heartbeat
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

      it "manual mode: only reports failure for un-acked jobs" $ \env -> do
        failuresRef <- newIORef ([] :: [Int64])

        let hooks =
              defaultObservabilityHooks
                { onJobFailure = \job _ _ _ ->
                    liftIO $ atomicModifyIORef' failuresRef $ \fs -> (primaryKey job : fs, ())
                }

        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn jobs = do
              -- Manually ack first 2 jobs
              forM_ (take 2 $ toList jobs) $ \job -> do
                void $ HL.ackJob job
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
                { useWorkerTransaction = False
                , pollInterval = 0.050
                , observabilityHooks = hooks
                , jitter = NoJitter
                }

        threadDelay 100_000

        withAsync (runSimpleDb env $ runWorkerPool batchedConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) . length <$> readIORef failuresRef

          -- Only 1 failure should be reported (3rd job), not 3
          -- The first 2 jobs were acked before the throw
          failures <- readIORef failuresRef
          length failures `shouldBe` 1

      it "manual mode: acking all jobs in batch removes them from queue" $ \env -> do
        processedRef <- newIORef False

        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn jobs = do
              liftIO $ atomicModifyIORef' processedRef $ \_ -> (True, ())
              -- Manually ack all jobs
              forM_ jobs $ \job -> void $ HL.ackJob job

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
                { useWorkerTransaction = False
                , pollInterval = 0.050
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

      it "manual mode: un-acked jobs remain in queue after handler returns" $ \env -> do
        processedRef <- newIORef False

        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn jobs = do
              liftIO $ atomicModifyIORef' processedRef $ \_ -> (True, ())
              -- Only ack the first job, leave the second un-acked
              void $ HL.ackJob (head $ toList jobs)

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
                { useWorkerTransaction = False
                , pollInterval = 0.050
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

        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn jobs = do
              let batchSize = length jobs
              liftIO $ atomicModifyIORef' batchSizesRef $ \sizes -> (batchSize : sizes, ())
              pure ()

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

        let batchHandler :: BatchedJobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            batchHandler _conn jobs = do
              let payloads = map payload (toList jobs)
              liftIO $ atomicModifyIORef' batchPayloadsRef $ \batches -> (payloads : batches, ())
              pure ()

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

    describe "Claim Throttle" $ do
      it "claimThrottle limits claim rate" $ \env -> do
        completedRef <- newIORef (0 :: Int)

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              liftIO $ atomicModifyIORef' completedRef $ \n -> (n + 1, ())
              pure ()

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload () <-
          runSimpleDb env $ defaultWorkerConfig connStr 10 handler

        let throttledConfig =
              config
                { workerCount = 10
                , pollInterval = 0.1
                , claimThrottle = Just (pure (2, 1)) -- max 2 claims per 1 second
                }

        withAsync (runSimpleDb env $ runWorkerPool throttledConfig) $ \_ -> do
          -- Wait for startup claim to pass (finds nothing)
          threadDelay 500_000

          -- Insert 10 jobs with distinct groups so they can run concurrently
          let jobs =
                map
                  ( \i ->
                      (defaultJob (SimpleTask (T.pack $ "Throttled-" <> show @Int i)))
                        { groupKey = Just (T.pack $ "tg" <> show i)
                        }
                  )
                  [1 .. 10]

          void $ runSimpleDb env $ HL.insertJobsBatch jobs

          startTime <- getCurrentTime

          -- Wait for all 10 jobs to complete, with a deadline to avoid hanging
          let deadline = 15 -- seconds
              waitForCompletion = do
                completed <- readIORef completedRef
                now <- getCurrentTime
                let elapsed = diffUTCTime now startTime
                if completed >= 10
                  then pure ()
                  else
                    if elapsed > deadline
                      then expectationFailure $ "Timed out after " <> show elapsed <> "s with only " <> show completed <> " jobs completed"
                      else threadDelay 200_000 >> waitForCompletion
          waitForCompletion

          endTime <- getCurrentTime
          let elapsed = diffUTCTime endTime startTime

          -- 10 jobs at 2/second = 5 windows. First is immediate, 4 more at 1s each.
          -- Lower bound: throttle is actually slowing things down
          elapsed `shouldSatisfy` (>= 4)
          -- Upper bound: throttle isn't broken/stalling (10s is generous)
          elapsed `shouldSatisfy` (< 10)

    describe "Fan-out/fan-in with rollup" $ do
      it "worker auto-appends handler results; finalizer reads merged state" $ \env -> do
        finalResultRef <- newIORef ([] :: [Text])

        let handler childResults _dlqFailures _conn job = case payload job of
              SimpleTask "mapper-a" -> pure ["sales", "growth"]
              SimpleTask "mapper-b" -> pure ["revenue"]
              SimpleTask "mapper-c" -> pure ["forecast", "trend"]
              SimpleTask "reducer" -> do
                liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (childResults, ())
                pure childResults
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
          runSimpleDb env $ defaultRollupWorkerConfig connStr 10 handler

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

        let handler childResults _dlqFailures _conn job = case payload job of
              SimpleTask "mapper-1a" -> pure ["sales", "growth"]
              SimpleTask "mapper-1b" -> pure ["revenue"]
              SimpleTask "mapper-2a" -> pure ["forecast"]
              SimpleTask "mapper-2b" -> pure ["trend"]
              SimpleTask "section-1" -> pure childResults
              SimpleTask "section-2" -> pure childResults
              SimpleTask "root" -> do
                liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (childResults, ())
                pure childResults
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
          runSimpleDb env $ defaultRollupWorkerConfig connStr 10 handler

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

      it "manual mode: user inserts results and acks for rollup" $ \env -> do
        finalResultRef <- newIORef ([] :: [Text])

        let handler childResults _dlqFailures _conn job = case payload job of
              SimpleTask "child-a" -> do
                let result = ["alpha"] :: [Text]
                -- Manual mode: user inserts result before acking
                void $
                  HL.insertResult @_ @WorkerTestRegistry @WorkerTestPayload (fromMaybe 0 $ parentId job) (primaryKey job) (toJSON result)
                void $ HL.ackJob job
                pure result
              SimpleTask "child-b" -> do
                let result = ["beta", "gamma"] :: [Text]
                void $
                  HL.insertResult @_ @WorkerTestRegistry @WorkerTestPayload (fromMaybe 0 $ parentId job) (primaryKey job) (toJSON result)
                void $ HL.ackJob job
                pure result
              SimpleTask "manual-reducer" -> do
                liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (childResults, ())
                void $ HL.ackJob job
                pure childResults
              _ -> do
                void $ HL.ackJob job
                pure []

        -- Insert the rollup tree
        runSimpleDb env $
          void $
            HL.insertJobTree $
              defaultJob (SimpleTask "manual-reducer")
                <~~ ( defaultJob (SimpleTask "child-a")
                        :| [defaultJob (SimpleTask "child-b")]
                    )

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultRollupWorkerConfig connStr 10 handler

        withAsync
          ( runSimpleDb env $
              runWorkerPool
                ( config
                    { workerCount = 3
                    , pollInterval = 0.1
                    , useWorkerTransaction = False
                    }
                )
          )
          $ \_ -> do
            waitUntil 10_000 $ (== 3) . length <$> readIORef finalResultRef

            -- Finalizer should have received child results
            finalResult <- readIORef finalResultRef
            length finalResult `shouldBe` 3
            finalResult `shouldMatchList` ["alpha", "beta", "gamma"]

      it "DLQ snapshot round-trip preserves child results" $ \env -> do
        attemptRef <- newIORef (0 :: Int)
        finalResultRef <- newIORef ([] :: [Text])

        let handler childResults _dlqFailures _conn job = case payload job of
              SimpleTask "dlq-child-a" -> pure ["x"]
              SimpleTask "dlq-child-b" -> pure ["y", "z"]
              SimpleTask "dlq-reducer" -> do
                attempt <- liftIO $ atomicModifyIORef' attemptRef $ \n -> (n + 1, n + 1)
                if attempt == 1
                  then throwRetryable "Intentional failure on first attempt"
                  else do
                    liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (childResults, ())
                    pure childResults
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
          runSimpleDb env $ defaultRollupWorkerConfig connStr 10 handler

        let cfg =
              config
                { workerCount = 3
                , pollInterval = 0.1
                , maxAttempts = 1 -- Go to DLQ after first failure
                }

        -- Phase 1: Run workers — children succeed, reducer fails → DLQ
        withAsync (runSimpleDb env $ runWorkerPool cfg) $ \_ ->
          waitUntil 10_000 $ do
            dlqJobs <- runSimpleDb env $ HL.listDLQJobs @_ @WorkerTestRegistry @WorkerTestPayload 10 0
            pure $ any (\d -> payload (DLQ.jobSnapshot d) == SimpleTask "dlq-reducer") dlqJobs

        -- Verify reducer is in DLQ with snapshot
        dlqJobs <- runSimpleDb env $ HL.listDLQJobs @_ @WorkerTestRegistry @WorkerTestPayload 10 0
        let reducerDlq = filter (\d -> payload (DLQ.jobSnapshot d) == SimpleTask "dlq-reducer") dlqJobs
        length reducerDlq `shouldBe` 1

        -- Phase 2: Retry from DLQ — reducer should see preserved results from snapshot
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

        let handler childResults _dlqFailures _conn job = case payload job of
              SimpleTask "recover-child-ok" -> pure ["alpha"]
              SimpleTask "recover-child-fail" -> throwRetryable "Permanent child failure"
              SimpleTask "recover-reducer" -> do
                attempt <- liftIO $ atomicModifyIORef' attemptRef $ \n -> (n + 1, n + 1)
                if attempt == 1
                  then throwRetryable "Reducer fails first time"
                  else do
                    liftIO $ atomicModifyIORef' finalResultRef $ \_ -> (childResults, ())
                    pure childResults
              _ -> pure []

        -- Insert rollup tree: reducer + 2 children
        Right (_parent :| _children) <-
          runSimpleDb env $
            HL.insertJobTree $
              defaultJob (SimpleTask "recover-reducer")
                <~~ ( defaultJob (SimpleTask "recover-child-ok")
                        :| [defaultJob (SimpleTask "recover-child-fail")]
                    )

        -- Phase 1: Worker runs — child-ok succeeds, child-fail DLQs, reducer wakes, reducer DLQs
        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultRollupWorkerConfig connStr 10 handler

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

        -- Phase 3: Run workers again — child-fail still fails, goes back to DLQ,
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

        let handler _childResults _dlqFailures _conn job = case payload job of
              SimpleTask "tc-leaf1" -> liftIO (throwTreeCancel "abort everything")
              _ -> pure []

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultRollupWorkerConfig connStr 10 handler

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

        let handler _childResults _dlqFailures _conn job = case payload job of
              SimpleTask "bc-leaf1" -> liftIO (throwBranchCancel "abort this branch")
              SimpleTask "bc-root" -> do
                liftIO $ atomicModifyIORef' rootProcessedRef $ \_ -> (True, ())
                pure []
              _ -> pure []

        config :: WorkerConfig (SimpleDb WorkerTestRegistry IO) WorkerTestPayload [Text] <-
          runSimpleDb env $ defaultRollupWorkerConfig connStr 10 handler

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

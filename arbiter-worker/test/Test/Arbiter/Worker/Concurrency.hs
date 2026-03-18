{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Concurrency and exception safety tests for the worker pool
--
-- These tests validate critical race conditions and exception handling that
-- could lead to duplicate job processing or incorrect state.
module Test.Arbiter.Worker.Concurrency (spec) where

import Arbiter.Core.Job.Types
  ( Job (..)
  , JobRead
  , ObservabilityHooks (..)
  , defaultJob
  )
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Simple (SimpleDb, createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (cleanupData, setupOnce)
import Control.Concurrent (threadDelay)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes, withResource)
import Data.Proxy (Proxy (..))
import Data.Text qualified as T
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Test.Hspec (Spec, beforeAll, describe, it, runIO, shouldBe, shouldContain)
import UnliftIO (withAsync)

import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (WorkerConfig (..), defaultWorkerConfig)

-- | Test schema name
testSchema :: T.Text
testSchema = "arbiter_worker_concurrency_test"

-- | Local test payload type with correct schema name
data WorkerConcurrencyTestPayload
  = SimpleTask T.Text
  | FailingTask Int
  | SlowTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Local test registry
type WorkerConcurrencyTestRegistry = '[ '("arbiter_worker_concurrency_test", WorkerConcurrencyTestPayload)]

-- Table name for tests
testTable :: T.Text
testTable = "arbiter_worker_concurrency_test"

-- Create shared pool for all tests
createSharedPool :: ByteString -> IO (Pool PG.Connection)
createSharedPool connStr =
  newPool $
    setNumStripes (Just 1) $
      defaultPoolConfig
        (connectPostgreSQL connStr)
        close
        60
        5

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable True) $ do
  sharedPool <- runIO $ createSharedPool connStr
  let getEnv = do
        let env = createSimpleEnvWithPool (Proxy @WorkerConcurrencyTestRegistry) sharedPool testSchema
        withResource sharedPool $ \conn -> cleanupData testSchema testTable conn
        pure env

  describe "Job Reclaim During Processing" $ do
    it "gracefully skips retry when job is reclaimed by another worker" $ do
      env <- getEnv

      -- Track which hooks were called
      failureCalls <- newIORef (0 :: Int)
      successCalls <- newIORef (0 :: Int)
      handlerCompleted <- newIORef False

      let hooks =
            ObservabilityHooks
              { onJobClaimed = \_ _ -> pure ()
              , onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' successCalls (\n -> (n + 1, ()))
              , onJobFailure = \_ _ _ _ -> liftIO $ atomicModifyIORef' failureCalls (\n -> (n + 1, ()))
              , onJobRetry = \_ _ -> pure ()
              , onJobFailedAndMovedToDLQ = \_ _ -> pure ()
              , onJobHeartbeat = \_ _ _ -> pure ()
              }

      -- Insert a job
      jobId <- runSimpleDb env $ do
        Just job <- Ops.insertJob testSchema testTable (defaultJob (SlowTask 1))
        pure (primaryKey job)

      -- Handler completes successfully, but another worker has reclaimed the job
      -- so the ack will fail (attempts mismatch). The worker should detect this
      -- as a "job gone" situation and skip retry/DLQ entirely.
      let jobHandler :: JobHandler (SimpleDb WorkerConcurrencyTestRegistry IO) WorkerConcurrencyTestPayload ()
          jobHandler _conn _job = do
            -- Simulate another worker claiming the job (increment attempts in DB)
            liftIO $ simulateAnotherWorkerClaim connStr jobId
            liftIO $ atomicModifyIORef' handlerCompleted (\_ -> (True, ()))
            pure ()

      config <- runSimpleDb env $ defaultWorkerConfig connStr 10 jobHandler
      let configWithHooks =
            config
              { observabilityHooks = hooks
              , pollInterval = 0.1
              }

      -- Run worker pool and wait for handler to complete
      withAsync
        (runSimpleDb env $ runWorkerPool configWithHooks)
        $ \_ -> do
          waitUntil 10_000 $ readIORef handlerCompleted
          -- Allow time for ack failure and isJobGoneException path to complete
          threadDelay 500_000

      -- Verify: neither hook was called (job-gone path skips both)
      failureCount <- readIORef failureCalls
      successCount <- readIORef successCalls
      failureCount `shouldBe` 0
      successCount `shouldBe` 0

      -- Verify: Job is still in queue (ack failed, transaction rolled back)
      allJobs <- runSimpleDb env $ Ops.listJobs testSchema testTable 10 0 :: IO [JobRead WorkerConcurrencyTestPayload]
      map primaryKey allJobs `shouldContain` [jobId]

    it "onJobFailure fires when handler throws a retryable exception" $ do
      env <- getEnv

      failureCalls <- newIORef (0 :: Int)
      successCalls <- newIORef (0 :: Int)

      let hooks =
            ObservabilityHooks
              { onJobClaimed = \_ _ -> pure ()
              , onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' successCalls (\n -> (n + 1, ()))
              , onJobFailure = \_ _ _ _ -> liftIO $ atomicModifyIORef' failureCalls (\n -> (n + 1, ()))
              , onJobRetry = \_ _ -> pure ()
              , onJobFailedAndMovedToDLQ = \_ _ -> pure ()
              , onJobHeartbeat = \_ _ _ -> pure ()
              }

      -- Insert a job
      void $ runSimpleDb env $ Ops.insertJob testSchema testTable (defaultJob (SimpleTask "will-fail"))

      -- Handler that always throws (retryable exception, not job-gone)
      let jobHandler :: JobHandler (SimpleDb WorkerConcurrencyTestRegistry IO) WorkerConcurrencyTestPayload ()
          jobHandler _conn _job = error "intentional failure"

      config <- runSimpleDb env $ defaultWorkerConfig connStr 10 jobHandler
      let configWithHooks =
            config
              { observabilityHooks = hooks
              , pollInterval = 0.1
              , maxAttempts = 1 -- Move to DLQ immediately
              }

      -- Run worker pool until onJobFailure fires
      withAsync
        (runSimpleDb env $ runWorkerPool configWithHooks)
        $ \_ ->
          waitUntil 10_000 $ (== 1) <$> readIORef failureCalls

      -- Verify: onJobFailure was called, not onJobSuccess
      failureCount <- readIORef failureCalls
      successCount <- readIORef successCalls
      failureCount `shouldBe` 1
      successCount `shouldBe` 0

  describe "Heartbeat Stolen Job Detection" $ do
    it "heartbeat cancels handler via race when job is stolen mid-processing" $ do
      env <- getEnv

      handlerStarted <- newIORef False
      handlerCompleted <- newIORef False

      jobId <- runSimpleDb env $ do
        Just job <- Ops.insertJob testSchema testTable (defaultJob (SlowTask 1))
        pure (primaryKey job)

      let jobHandler :: JobHandler (SimpleDb WorkerConcurrencyTestRegistry IO) WorkerConcurrencyTestPayload ()
          jobHandler _conn _job = liftIO $ do
            atomicModifyIORef' handlerStarted (\_ -> (True, ()))
            threadDelay 200_000
            simulateAnotherWorkerClaim connStr jobId
            threadDelay 5_000_000
            -- Should never reach here: heartbeat detects theft and cancels via race
            atomicModifyIORef' handlerCompleted (\_ -> (True, ()))

      config <- runSimpleDb env $ defaultWorkerConfig connStr 10 jobHandler
      let configWithHooks =
            config
              { pollInterval = 0.1
              , visibilityTimeout = 2
              , heartbeatInterval = 1
              }

      withAsync
        (runSimpleDb env $ runWorkerPool configWithHooks)
        $ \_ ->
          waitUntil 10_000 $ readIORef handlerStarted

      -- Verify the handler actually started (not vacuously passing)
      started <- readIORef handlerStarted
      started `shouldBe` True

      completed <- readIORef handlerCompleted
      completed `shouldBe` False

      -- Job still in queue (handler was cancelled before ack)
      allJobs <- runSimpleDb env $ Ops.listJobs testSchema testTable 10 0 :: IO [JobRead WorkerConcurrencyTestPayload]
      map primaryKey allJobs `shouldContain` [jobId]

  describe "Worker Loop Exception Safety" $ do
    it "continues processing after handler exceptions" $ do
      env <- getEnv

      -- Track jobs processed
      processedCount <- newIORef (0 :: Int)

      let hooks =
            ObservabilityHooks
              { onJobClaimed = \_ _ -> pure ()
              , onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' processedCount (\n -> (n + 1, ()))
              , onJobFailure = \_ _ _ _ -> liftIO $ atomicModifyIORef' processedCount (\n -> (n + 1, ()))
              , onJobRetry = \_ _ -> pure ()
              , onJobFailedAndMovedToDLQ = \_ _ -> pure ()
              , onJobHeartbeat = \_ _ _ -> pure ()
              }

      -- Insert jobs that always fail (FailingTask 999 means fail 999 times)
      runSimpleDb env $ do
        void $ Ops.insertJob testSchema testTable (defaultJob (FailingTask 999))
        void $ Ops.insertJob testSchema testTable (defaultJob (FailingTask 999))
        void $ Ops.insertJob testSchema testTable (defaultJob (FailingTask 999))

      -- Use default handler (FailingTask will throw exceptions)
      let jobHandler :: JobHandler (SimpleDb WorkerConcurrencyTestRegistry IO) WorkerConcurrencyTestPayload ()
          jobHandler _conn job = do
            case payload job of
              FailingTask n | n > 0 -> error "Failing task"
              _ -> pure ()

      config <- runSimpleDb env $ defaultWorkerConfig connStr 10 jobHandler
      let configWithHooks =
            config
              { observabilityHooks = hooks
              , workerCount = 1
              , pollInterval = 0.1
              , maxAttempts = 1 -- Fail immediately to DLQ
              }

      -- Run worker pool
      withAsync
        (runSimpleDb env $ runWorkerPool configWithHooks)
        $ \_ ->
          waitUntil 10_000 $ (== 3) <$> readIORef processedCount

      -- Verify: All jobs were processed (even though they failed)
      processed <- readIORef processedCount
      processed `shouldBe` 3

  describe "Heartbeat" $ do
    it "prevents job reclaim during long-running processing" $ do
      env <- getEnv

      -- Track when handler starts and completes
      handlerStarted <- newIORef False
      handlerCompleted <- newIORef False

      let jobHandler :: JobHandler (SimpleDb WorkerConcurrencyTestRegistry IO) WorkerConcurrencyTestPayload ()
          jobHandler _conn _job = do
            liftIO $ atomicModifyIORef' handlerStarted (\_ -> (True, ()))
            liftIO $ threadDelay 4_000_000 -- 4 seconds (longer than visibility timeout)
            liftIO $ atomicModifyIORef' handlerCompleted (\_ -> (True, ()))
            pure ()

      -- Insert a slow job
      Just _job <- runSimpleDb env $ Ops.insertJob testSchema testTable (defaultJob (SlowTask 4))

      -- Start worker with short visibility timeout and heartbeat
      config <- runSimpleDb env $ defaultWorkerConfig connStr 10 jobHandler
      let workerConfig =
            config
              { workerCount = 1
              , pollInterval = 0.1
              , visibilityTimeout = 2 -- 2 seconds
              , heartbeatInterval = 1 -- 1 second heartbeat
              }

      withAsync
        (runSimpleDb env $ runWorkerPool workerConfig)
        $ \_ -> do
          -- Wait for handler to start
          waitUntil 10_000 $ readIORef handlerStarted
          started <- readIORef handlerStarted
          started `shouldBe` True

          -- Wait past visibility timeout (heartbeat should extend it)
          threadDelay 2_500_000

          -- Try to claim the job with a second worker
          -- Heartbeat should have extended visibility, preventing reclaim
          claimedJobs <-
            runSimpleDb
              env
              ( Ops.claimNextVisibleJobs testSchema testTable 10 2
                  :: SimpleDb WorkerConcurrencyTestRegistry IO [JobRead WorkerConcurrencyTestPayload]
              )

          -- Should be 0 because heartbeat extended visibility
          length claimedJobs `shouldBe` (0 :: Int)

          -- Wait for handler to finish and verify it completed successfully
          waitUntil 10_000 $ readIORef handlerCompleted
          completed <- readIORef handlerCompleted
          completed `shouldBe` True

-- | Helper: Simulate another worker claiming a job by incrementing attempts.
-- This opens a fresh connection (outside any worker transaction) to increment
-- the attempts counter, causing the worker's ack to fail due to attempts mismatch.
simulateAnotherWorkerClaim :: ByteString -> Int64 -> IO ()
simulateAnotherWorkerClaim connStr jobId = do
  conn <- PG.connectPostgreSQL connStr
  void $
    PG.execute
      conn
      "UPDATE arbiter_worker_concurrency_test.arbiter_worker_concurrency_test SET attempts = attempts + 1 WHERE id = ?"
      (PG.Only jobId)
  PG.close conn

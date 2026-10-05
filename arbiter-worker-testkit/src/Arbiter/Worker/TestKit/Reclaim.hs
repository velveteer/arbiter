{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Reclaimed-job, stolen-job, and handler-exception tests, instantiated for each backend.
module Arbiter.Worker.TestKit.Reclaim (reclaimSpec) where

import Arbiter.Core.HighLevel (QueueOperation, RegistryAdmissionPolicies)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
  ( ObservabilityHooks (..)
  , defaultJob
  , defaultObservabilityHooks
  , payload
  , primaryKey
  , setMaxAttempts
  )
import Arbiter.Core.MonadArbiter (RegistryOf, ResultOf)
import Arbiter.Core.QueueRegistry (RegistryTables)
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)
import Control.Concurrent (threadDelay)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Test.Hspec

import Arbiter.Worker.TestKit.Backend (TestBackend (..))
import Arbiter.Worker.TestKit.Rows (reclaimJob)

-- | The tag on the failing payloads. The handler fails each payload with this tag.
alwaysFailing :: Int
alwaysFailing = 999

-- | Reclaim, heartbeat theft, and worker-loop exception safety suite. The queue
-- under test declares @()@ as its result type.
reclaimSpec
  :: forall payload m env
   . ( Eq payload
     , QueueOperation m payload
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     , ResultOf m payload ~ ()
     )
  => TestBackend payload m env
  -> Spec
reclaimSpec TestBackend {schema, table, connStr, mkSimple, mkFailing, mkEnv, mkHandler, runM} =
  before mkEnv $ do
    describe "Job Reclaim During Processing" $ do
      it "gracefully skips retry when job is reclaimed by another worker" $ \env -> do
        failureCalls <- newIORef (0 :: Int)
        successCalls <- newIORef (0 :: Int)
        unavailableCalls <- newIORef (0 :: Int)
        handlerCompleted <- newIORef False

        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' successCalls (\count -> (count + 1, ()))
                , onJobFailure = \_ _ _ _ -> liftIO $ atomicModifyIORef' failureCalls (\count -> (count + 1, ()))
                , onJobUnavailable = \_ _ -> liftIO $ atomicModifyIORef' unavailableCalls (\count -> (count + 1, ()))
                }

        Just inserted <- runM env $ HL.insertJob (defaultJob (mkSimple "slow"))
        let jobId = primaryKey inserted

        let jobHandler _job = liftIO $ do
              reclaimJob connStr schema table jobId
              atomicModifyIORef' handlerCompleted (\_ -> (True, ()))

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler jobHandler)
        let configWithHooks =
              config
                { observabilityHooks = hooks
                , pollInterval = 0.1
                }

        withLinkedAsync
          (runM env $ runWorkerPool configWithHooks)
          $ \_ -> do
            waitUntil 10_000 $ readIORef handlerCompleted
            threadDelay 500_000

        failureCount <- readIORef failureCalls
        successCount <- readIORef successCalls
        failureCount `shouldBe` 0
        successCount `shouldBe` 0

        unavailableCount <- readIORef unavailableCalls
        unavailableCount `shouldBe` 1

        allJobs <- runM env $ HL.listJobs @payload 10 0
        map primaryKey allJobs `shouldContain` [jobId]

      it "onJobFailure fires when handler throws a retryable exception" $ \env -> do
        failureCalls <- newIORef (0 :: Int)
        successCalls <- newIORef (0 :: Int)

        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' successCalls (\count -> (count + 1, ()))
                , onJobFailure = \_ _ _ _ -> liftIO $ atomicModifyIORef' failureCalls (\count -> (count + 1, ()))
                }

        void
          $ runM env
          $ HL.insertJob (setMaxAttempts (Just 1) $ defaultJob (mkSimple "will-fail"))

        let jobHandler _job = error "intentional failure"

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler jobHandler)
        let configWithHooks =
              config
                { observabilityHooks = hooks
                , pollInterval = 0.1
                }

        withLinkedAsync
          (runM env $ runWorkerPool configWithHooks)
          $ \_ ->
            waitUntil 10_000 $ (== 1) <$> readIORef failureCalls

        failureCount <- readIORef failureCalls
        successCount <- readIORef successCalls
        failureCount `shouldBe` 1
        successCount `shouldBe` 0

    describe "Heartbeat Stolen Job Detection" $ do
      it "heartbeat cancels handler via race when job is stolen mid-processing" $ \env -> do
        handlerCompleted <- newIORef False
        unavailableCalls <- newIORef (0 :: Int)

        let hooks =
              defaultObservabilityHooks
                { onJobUnavailable = \_ _ -> liftIO $ atomicModifyIORef' unavailableCalls (\count -> (count + 1, ()))
                }

        Just inserted <- runM env $ HL.insertJob (defaultJob (mkSimple "slow"))
        let jobId = primaryKey inserted

        let jobHandler _job = liftIO $ do
              reclaimJob connStr schema table jobId
              threadDelay 5_000_000
              atomicModifyIORef' handlerCompleted (\_ -> (True, ()))

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler jobHandler)
        let configWithHooks =
              config
                { observabilityHooks = hooks
                , pollInterval = 0.1
                , visibilityTimeout = 2
                , jobHeartbeatInterval = 1
                }

        withLinkedAsync
          (runM env $ runWorkerPool configWithHooks)
          $ \_ ->
            waitUntil 10_000 $ (== 1) <$> readIORef unavailableCalls

        completed <- readIORef handlerCompleted
        completed `shouldBe` False

        allJobs <- runM env $ HL.listJobs @payload 10 0
        map primaryKey allJobs `shouldContain` [jobId]

    describe "Worker Loop Exception Safety" $ do
      it "continues processing after handler exceptions" $ \env -> do
        processedCount <- newIORef (0 :: Int)

        let hooks =
              defaultObservabilityHooks
                { onJobSuccess = \_ _ _ -> liftIO $ atomicModifyIORef' processedCount (\count -> (count + 1, ()))
                , onJobFailure = \_ _ _ _ -> liftIO $ atomicModifyIORef' processedCount (\count -> (count + 1, ()))
                }

        runM env $
          traverse_
            (void . HL.insertJob . setMaxAttempts (Just 1) . defaultJob . mkFailing)
            [alwaysFailing, alwaysFailing, alwaysFailing]

        let jobHandler job = when (payload job == mkFailing alwaysFailing) $ error "Failing task"

        config :: WorkerConfig m payload <- transactionalWorkerConfig 10 (mkHandler jobHandler)
        let configWithHooks =
              config
                { observabilityHooks = hooks
                , workerCount = 1
                , pollInterval = 0.1
                }

        withLinkedAsync
          (runM env $ runWorkerPool configWithHooks)
          $ \_ ->
            waitUntil 10_000 $ (== 3) <$> readIORef processedCount

        processed <- readIORef processedCount
        processed `shouldBe` 3

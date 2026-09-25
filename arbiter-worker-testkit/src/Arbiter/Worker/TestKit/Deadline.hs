{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Fence tests: the deadlines the worker holds a handler to without asking the database.
module Arbiter.Worker.TestKit.Deadline (deadlineSpec) where

import Arbiter.Core.Exceptions (JobForceCancelled (..))
import Arbiter.Core.HighLevel (QueueOperation, RegistryAdmissionPolicies)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ (DLQJob (..))
import Arbiter.Core.Job.Types
  ( JobRead
  , JobWrite
  , ObservabilityHooks (..)
  , defaultJob
  , defaultObservabilityHooks
  , lastError
  , payload
  , primaryKey
  , setGroupKey
  , setMaxAttempts
  )
import Arbiter.Core.MonadArbiter (JobHandler, RegistryOf, ResultOf)
import Arbiter.Core.QueueRegistry (RegistryTables)
import Arbiter.Core.Trace (capturingContextIO)
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config
  ( BatchCallbacks (..)
  , WorkerConfig (..)
  , ackAll
  , defaultBatchedWorkerConfig
  , transactionalWorkerConfig
  )
import Arbiter.Worker.Heartbeat (HeartbeatGuard, newHeartbeatGuard)
import Arbiter.Worker.Heartbeat.Guard (Batch (..), guardBatch, leaseExpiredReason, reclaimedReason, runHeartbeatGuard)
import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), defaultLogConfig, silentLogConfig)
import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, fromException, uninterruptibleMask_)
import Control.Monad (void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Either (isRight)
import Data.Foldable (toList)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.List (partition)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Maybe (fromMaybe, isJust)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime, getCurrentTime)
import GHC.Clock (getMonotonicTime)
import Test.Hspec (Spec, before, describe, it, shouldBe, shouldSatisfy)
import UnliftIO (MonadUnliftIO, finally, mask_, tryAny, withRunInIO)
import UnliftIO.Async (async, poll, waitCatch, withAsync)

import Arbiter.Worker.TestKit.Backend (TestBackend (..))
import Arbiter.Worker.TestKit.Rows (flagCancelled, holdRowLock, reclaimJob, releaseRow, rowCount, takeClaimHolder)

-- | Longer than any deadline under test.
handlerSleepMicros :: Int
handlerSleepMicros = 30_000_000

-- | Cleanup slow enough that the heartbeat outlives the fence throw.
unwindMicros :: Int
unwindMicros = 4_000_000

-- | Masked interval longer than the deadline.
maskedMicros :: Int
maskedMicros = 4_000_000

-- | A heartbeat hook slow enough to cover another batch's whole deadline.
slowHookMicros :: Int
slowHookMicros = 8_000_000

-- | Longer than 'maskedMicros'.
stuckRegistrationMillis :: Int
stuckRegistrationMillis = 8_000

-- | A pause after the registration returns.
afterRegistrationMicros :: Int
afterRegistrationMicros = 500_000

-- | How long two sibling handlers run.
siblingRunMicros :: Int
siblingRunMicros = 6_000_000

-- | A pause for both sibling batches to register before the cancel.
beforeCancelMicros :: Int
beforeCancelMicros = 700_000

-- | How long a sibling batch's row stays locked.
rowLockMicros :: Int
rowLockMicros = 4_000_000

-- | The window, in seconds after the lock, in which the free batch has to beat.
lockedBeatFrom, lockedBeatTo :: Double
lockedBeatFrom = 1
lockedBeatTo = 3.5

-- | How long a grouped handler runs after enqueuing into its group.
groupedHandlerMicros :: Int
groupedHandlerMicros = 2_500_000

-- | A pause into the first handler's run before the fenced job is inserted.
beforeFencedInsertMicros :: Int
beforeFencedInsertMicros = 1_300_000

-- | The most seconds a fenced job may take to reach the DLQ.
fenceBudgetSeconds :: Double
fenceBudgetSeconds = 5

-- | How long a poll waits for the worker to reach a state.
waitMillis :: Int
waitMillis = 10_000

-- | 'waitMillis' for a state a backoff or a fence delays.
slowWaitMillis :: Int
slowWaitMillis = 15_000

-- | 'waitMillis' for a DLQ move.
dlqWaitMillis :: Int
dlqWaitMillis = 20_000

-- | Run @action@ with the batch registered with the guard, as the pool does.
withJobsHeartbeat
  :: (MonadUnliftIO m)
  => HeartbeatGuard payload
  -> UTCTime
  -> NonEmpty (JobRead payload)
  -> m [JobRead payload]
  -> m a
  -> m a
withJobsHeartbeat guard startTime jobs pending action = do
  inherit <- capturingContextIO
  withRunInIO $ \run -> guardBatch guard (Batch jobs (run pending) startTime inherit) (run action)

-- | Deadline suite. The queue under test declares @()@ as its result type.
deadlineSpec
  :: forall payload m env
   . ( Eq payload
     , QueueOperation m payload
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     , ResultOf m payload ~ ()
     )
  => TestBackend payload m env
  -> Spec
deadlineSpec TestBackend {schema, table, connStr, mkSimple, mkEnv, mkHandler, runM} =
  before mkEnv $ do
    describe "Guard registration" $ do
      it "returns once a signal in flight meets the unregister" $ \env -> do
        job <- inserted env (defaultJob (mkSimple "slow"))
        config <- transactionalWorkerConfig 1 idleHandler
        guard <- runM env (newHeartbeatGuard config {maxJobDuration = Just 1, logConfig = silentLogConfig})
        startTime <- getCurrentTime
        withAsync (runHeartbeatGuard guard) $ \_ -> do
          registration <-
            async . runM env . mask_ $ do
              withJobsHeartbeat guard startTime (job :| []) (pure []) (liftIO (uninterruptibleMask_ (threadDelay maskedMicros)))
              liftIO (threadDelay afterRegistrationMicros)
          waitUntil stuckRegistrationMillis (isJust <$> poll registration)
          outcome <- waitCatch registration
          outcome `shouldSatisfy` isRight

    describe "Shared extend" $ do
      it "stops only the batch whose job another worker reclaimed" $ \env -> do
        startedRef <- newIORef ([] :: [Int64])
        finishedRef <- newIORef ([] :: [Int64])
        reasonsRef <- newIORef ([] :: [(Int64, Text)])
        beatsRef <- newIORef ([] :: [(Int64, Double)])
        let handler :: JobHandler m payload ()
            handler = mkHandler $ \job -> liftIO $ do
              atomicModifyIORef' startedRef (\started -> (primaryKey job : started, ()))
              threadDelay siblingRunMicros
              atomicModifyIORef' finishedRef (\finished -> (primaryKey job : finished, ()))
            hooks :: ObservabilityHooks m payload
            hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \job _ _ ->
                    liftIO (stamp beatsRef job)
                , onJobUnavailable = \job reason ->
                    liftIO $ atomicModifyIORef' reasonsRef (\seen -> ((primaryKey job, reason) : seen, ()))
                }
        stolenId <- insertedId env
        keptId <- insertedId env

        config <- transactionalWorkerConfig 2 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 20
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 2) . length <$> readIORef startedRef
          reclaimJob connStr schema table stolenId
          waitUntil waitMillis $ not . null <$> readIORef reasonsRef
          reclaimedAt <- getMonotonicTime
          waitUntil slowWaitMillis $ not . null <$> readIORef finishedRef
          reasons <- readIORef reasonsRef
          reasons `shouldBe` [(stolenId, reclaimedReason)]
          finished <- readIORef finishedRef
          finished `shouldBe` [keptId]
          beats <- readIORef beatsRef
          [() | (jobId, at) <- beats, jobId == keptId, at > reclaimedAt] `shouldSatisfy` (not . null)

    describe "Shared guard" $ do
      it "stops only the batch whose live claim the cancel names" $ \env -> do
        config <- transactionalWorkerConfig 1 idleHandler
        let guardConfig = config {jobHeartbeatInterval = 0.5, visibilityTimeout = 20, logConfig = silentLogConfig}
        handedId <- insertedPlainId env
        _ <- insertedPlainId env
        batch <- runM env (HL.claimNextVisibleJobsAs @payload 2 20 (workerId config))
        (handed, kept) <- case partition ((== handedId) . primaryKey) batch of
          ([job], [sibling]) -> pure (job, sibling)
          _ -> fail "expected two claimed jobs"
        releaseRow connStr schema table handedId
        reclaimed <- runM env (HL.claimNextVisibleJobsAs @payload 1 20 (workerId config)) >>= single
        guard <- runM env (newHeartbeatGuard guardConfig)
        startTime <- getCurrentTime
        withAsync (runHeartbeatGuard guard) $ \_ -> do
          keeper <-
            async . runM env $
              withJobsHeartbeat guard startTime (handed :| [kept]) (pure [kept]) (liftIO (threadDelay siblingRunMicros))
          holder <-
            async . runM env $
              withJobsHeartbeat guard startTime (reclaimed :| []) (pure [reclaimed]) (liftIO (threadDelay siblingRunMicros))
          threadDelay beforeCancelMicros
          flagCancelled connStr schema table handedId
          holderOutcome <- waitCatch holder
          keeperOutcome <- waitCatch keeper
          either isForceCancelled (const False) holderOutcome `shouldBe` True
          keeperOutcome `shouldSatisfy` isRight

      it "beats a batch while a sibling batch's row is locked" $ \env -> do
        startedRef <- newIORef ([] :: [Int64])
        beatsRef <- newIORef ([] :: [(Int64, Double)])
        let handler :: JobHandler m payload ()
            handler = mkHandler $ \job -> liftIO $ do
              atomicModifyIORef' startedRef (\started -> (primaryKey job : started, ()))
              threadDelay handlerSleepMicros
            hooks :: ObservabilityHooks m payload
            hooks = defaultObservabilityHooks {onJobHeartbeat = \job _ _ -> liftIO (stamp beatsRef job)}
        lockedId <- insertedId env
        freeId <- insertedId env
        config <- transactionalWorkerConfig 2 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 20
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }
        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 2) . length <$> readIORef startedRef
          lockedFrom <- getMonotonicTime
          holdRowLock connStr schema table lockedId rowLockMicros
          beats <- readIORef beatsRef
          [() | (jobId, at) <- beats, jobId == freeId, at > lockedFrom + lockedBeatFrom, at < lockedFrom + lockedBeatTo]
            `shouldSatisfy` (not . null)

      it "extends a grouped job whose handler enqueued into its group" $ \env -> do
        loggedRef <- newIORef ([] :: [Text])
        finishedRef <- newIORef (0 :: Int)
        let handler :: JobHandler m payload ()
            handler = mkHandler $ \job ->
              when (payload job == mkSimple "slow") $ do
                void (HL.insertJob (setGroupKey (Just "shared") (defaultJob (mkSimple "child"))))
                liftIO (threadDelay groupedHandlerMicros)
                liftIO (atomicModifyIORef' finishedRef (\count -> (count + 1, ())))
            capture _level msg _ctx = atomicModifyIORef' loggedRef (\messages -> (msg : messages, ()))
        parent <- inserted env (setGroupKey (Just "shared") (setMaxAttempts (Just 1) (defaultJob (mkSimple "slow"))))
        config :: WorkerConfig m payload <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 20
                , jobHeartbeatInterval = 1
                , logConfig = defaultLogConfig {logDestination = LogCallback capture}
                }
        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 1) <$> readIORef finishedRef
          waitUntil waitMillis $ (== 0) <$> rowCount connStr schema table (primaryKey parent)
          logged <- readIORef loggedRef
          filter (T.isInfixOf "deadlock" . T.toLower) logged `shouldBe` []
          dlq <- listDLQ env
          map (primaryKey . jobSnapshot) dlq `shouldBe` []

    describe "Job deadline" $ do
      it "interrupts a handler that outruns the maximum job duration" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        let handler :: JobHandler m payload ()
            handler = mkHandler $ \_job -> liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))

        void (insertedId env)

        config :: WorkerConfig m payload <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = silentLogConfig
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 1) <$> readIORef startedRef
          waitUntil dlqWaitMillis $ not . null <$> listDLQ env
          dlq <- listDLQ env
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          map (lastError . jobSnapshot) dlq
            `shouldBe` [Just "handler ran past the maximum job duration of 1s"]

      it "fences a batch on time while another batch's heartbeat hook is slow" $ \env -> do
        startedRef <- newIORef ([] :: [(Int64, Double)])
        let handler :: JobHandler m payload ()
            handler = mkHandler $ \job -> liftIO $ do
              now <- getMonotonicTime
              atomicModifyIORef' startedRef (\started -> ((primaryKey job, now) : started, ()))
              threadDelay handlerSleepMicros
        slowId <- insertedId env
        let hooks :: ObservabilityHooks m payload
            hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \job _ _ ->
                    when (primaryKey job == slowId) (liftIO (threadDelay slowHookMicros))
                }

        config <- transactionalWorkerConfig 2 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 20
                , jobHeartbeatInterval = 1
                , maxJobDuration = Just 2
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 1) . length <$> readIORef startedRef
          threadDelay beforeFencedInsertMicros
          fencedId <- insertedId env
          waitUntil waitMillis $ (== 2) . length <$> readIORef startedRef
          waitUntil slowWaitMillis $ any ((== fencedId) . primaryKey . jobSnapshot) <$> listDLQ env
          fencedAt <- getMonotonicTime
          started <- readIORef startedRef
          let startedAt = fromMaybe 0 (lookup fencedId started)
          (fencedAt - startedAt) `shouldSatisfy` (< fenceBudgetSeconds)

      it "reports stopping a handler that had already finalized its batch" $ \env -> do
        loggedRef <- newIORef ([] :: [Text])
        finishedRef <- newIORef (0 :: Int)
        let handler
              :: NonEmpty (JobRead payload)
              -> BatchCallbacks m payload ()
              -> m ()
            handler jobs callbacks = do
              ackAll callbacks (toList jobs)
              liftIO $ do
                threadDelay handlerSleepMicros
                atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            capture _level msg _ctx = atomicModifyIORef' loggedRef (\messages -> (msg : messages, ()))

        void (insertedPlainId env)

        config <- defaultBatchedWorkerConfig 1 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = defaultLogConfig {logDestination = LogCallback capture}
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil slowWaitMillis $ any ("finalized" `T.isInfixOf`) <$> readIORef loggedRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          logged <- readIORef loggedRef
          filter ("maximum job duration" `T.isInfixOf`) logged `shouldSatisfy` (not . null)

      it "keeps beating while a masked handler holds off the deadline" $ \env -> do
        startedRef <- newIORef (Nothing :: Maybe Double)
        finishedRef <- newIORef (0 :: Int)
        beatsRef <- newIORef ([] :: [Double])
        let handler :: JobHandler m payload ()
            handler = mkHandler $ \_job -> liftIO $ do
              getMonotonicTime >>= writeIORef startedRef . Just
              uninterruptibleMask_ (threadDelay maskedMicros)
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            hooks :: ObservabilityHooks m payload
            hooks = defaultObservabilityHooks {onJobHeartbeat = \_ _ _ -> recordBeat beatsRef}

        void (insertedId env)

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , visibilityTimeout = 6
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ isJust <$> readIORef startedRef
          waitUntil dlqWaitMillis $ not . null <$> listDLQ env
          started <- maybe 0 id <$> readIORef startedRef
          beats <- readIORef beatsRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          length (filter (< started + maskedSeconds) beats) `shouldSatisfy` (>= 2)

      it "interrupts a handler that catches sync exceptions" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        let handler :: JobHandler m payload ()
            handler = mkHandler $ \_job -> liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              void (tryAny (threadDelay handlerSleepMicros))
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))

        void (insertedId env)

        config :: WorkerConfig m payload <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = silentLogConfig
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 1) <$> readIORef startedRef
          waitUntil dlqWaitMillis $ not . null <$> listDLQ env
          finished <- readIORef finishedRef
          finished `shouldBe` 0

      it "keeps the lease while a fenced handler unwinds" $ \env -> do
        startedRef <- newIORef (Nothing :: Maybe Double)
        finishedRef <- newIORef (0 :: Int)
        beatsRef <- newIORef ([] :: [Double])
        reasonsRef <- newIORef ([] :: [Text])
        let handler :: JobHandler m payload ()
            handler = mkHandler $ \_job ->
              liftIO
                ( do
                    getMonotonicTime >>= writeIORef startedRef . Just
                    threadDelay handlerSleepMicros
                    atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
                )
                `finally` liftIO (threadDelay unwindMicros)
            hooks :: ObservabilityHooks m payload
            hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \_ _ _ -> recordBeat beatsRef
                , onJobUnavailable = \_ reason ->
                    liftIO $ atomicModifyIORef' reasonsRef (\seen -> (reason : seen, ()))
                }

        void (insertedId env)

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , visibilityTimeout = 3
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ isJust <$> readIORef startedRef
          waitUntil dlqWaitMillis $ not . null <$> listDLQ env
          started <- maybe 0 id <$> readIORef startedRef
          beats <- readIORef beatsRef
          reasons <- readIORef reasonsRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          reasons `shouldBe` []
          dlq <- listDLQ env
          map (lastError . jobSnapshot) dlq
            `shouldBe` [Just "handler ran past the maximum job duration of 1s"]
          length (filter (> started + 1) beats) `shouldSatisfy` (>= 2)

    describe "Lease fence" $ do
      it "does not carry the fence past a row its extend did not move" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        reasonsRef <- newIORef ([] :: [Text])
        let handler :: JobHandler m payload ()
            handler = mkHandler $ \_job -> liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            hooks :: ObservabilityHooks m payload
            hooks =
              defaultObservabilityHooks
                { onJobUnavailable = \_ reason ->
                    liftIO $ atomicModifyIORef' reasonsRef (\seen -> (reason : seen, ()))
                }

        void (insertedPlainId env)

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 5
                , jobHeartbeatInterval = 2
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runM env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 1) <$> readIORef startedRef
          takeClaimHolder connStr schema table
          waitUntil slowWaitMillis $ not . null <$> readIORef reasonsRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          reasons <- readIORef reasonsRef
          reasons `shouldBe` [leaseExpiredReason]
  where
    inserted :: env -> JobWrite payload -> IO (JobRead payload)
    inserted env job = runM env (HL.insertJob job) >>= maybe (fail "insert returned no job") pure
    insertedPlainId :: env -> IO Int64
    insertedPlainId env = primaryKey <$> inserted env (defaultJob (mkSimple "slow"))
    insertedId :: env -> IO Int64
    insertedId env = primaryKey <$> inserted env (setMaxAttempts (Just 1) (defaultJob (mkSimple "slow")))
    idleHandler :: JobHandler m payload ()
    idleHandler = mkHandler (\_job -> pure ())
    listDLQ :: env -> IO [DLQJob payload]
    listDLQ env = runM env (HL.listDLQJobs 10 0)

maskedSeconds :: Double
maskedSeconds = fromIntegral maskedMicros / 1_000_000

recordBeat :: (MonadIO m) => IORef [Double] -> m ()
recordBeat beatsRef = liftIO $ getMonotonicTime >>= \now -> atomicModifyIORef' beatsRef (\beats -> (now : beats, ()))

single :: [a] -> IO a
single [x] = pure x
single _ = fail "expected exactly one job"

isForceCancelled :: SomeException -> Bool
isForceCancelled exc = isJust (fromException exc :: Maybe JobForceCancelled)

stamp :: IORef [(Int64, Double)] -> JobRead payload -> IO ()
stamp ref job = getMonotonicTime >>= \now -> atomicModifyIORef' ref (\seen -> ((primaryKey job, now) : seen, ()))

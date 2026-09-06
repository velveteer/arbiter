{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Fence tests: the deadlines the worker holds a handler to without asking the database.
module Test.Arbiter.Worker.Deadline (spec) where

import Arbiter.Core.Exceptions (JobForceCancelled (..))
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
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Core.Trace (capturingContextIO)
import Arbiter.Simple (SimpleDb, SimpleEnv, createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (cleanupData, createSharedPool, setupOnce)
import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, fromException, uninterruptibleMask_)
import Control.Monad (void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.Either (isRight)
import Data.Foldable (toList)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.List (partition)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Maybe (fromMaybe, isJust)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime, getCurrentTime)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Clock (getMonotonicTime)
import Test.Hspec (Spec, around, beforeAll, describe, it, runIO, shouldBe, shouldSatisfy)
import UnliftIO (MonadUnliftIO, bracket, finally, mask_, tryAny, withRunInIO)
import UnliftIO.Async (async, poll, waitCatch, withAsync)

import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config
  ( BatchCallbacks (..)
  , WorkerConfig (..)
  , ackAll
  , defaultBatchedWorkerConfig
  , transactionalWorkerConfig
  )
import Arbiter.Worker.Heartbeat (HeartbeatGuard, newHeartbeatGuard, runHeartbeatGuard)
import Arbiter.Worker.Heartbeat.Guard (Batch (..), guardBatch, leaseExpiredReason, reclaimedReason)
import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), defaultLogConfig, silentLogConfig)

type WorkerTestRegistry = '[Queue "arbiter_worker_deadline_test" WorkerTestPayload]

testSchema :: Text
testSchema = "arbiter_worker_deadline_test"

testTable :: Text
testTable = "arbiter_worker_deadline_test"

-- | Longer than any deadline under test.
handlerSleepMicros :: Int
handlerSleepMicros = 30_000_000

-- | Cleanup slow enough that the heartbeat outlives the fence throw.
unwindMicros :: Int
unwindMicros = 4_000_000

-- | A masked stretch longer than the deadline, so delivery has to wait it out.
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

-- | Insert a job and return it.
inserted :: SimpleEnv WorkerTestRegistry -> JobWrite WorkerTestPayload -> IO (JobRead WorkerTestPayload)
inserted env job = runSimpleDb env (HL.insertJob job) >>= maybe (fail "insert returned no job") pure

-- | Insert a slow job with the default attempt budget and return its id.
insertedPlainId :: SimpleEnv WorkerTestRegistry -> IO Int64
insertedPlainId env = primaryKey <$> inserted env (defaultJob (SlowTask 30))

-- | Insert a single-attempt slow job and return its id.
insertedId :: SimpleEnv WorkerTestRegistry -> IO Int64
insertedId env = primaryKey <$> inserted env (setMaxAttempts (Just 1) (defaultJob (SlowTask 30)))

-- | A handler that finishes at once.
idleHandler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
idleHandler _conn _job = pure ()

-- | Run @action@ with the batch registered with the guard, as the pool does.
withJobsHeartbeat
  :: (MonadUnliftIO m)
  => HeartbeatGuard WorkerTestPayload
  -> UTCTime
  -> NonEmpty (JobRead WorkerTestPayload)
  -> m [JobRead WorkerTestPayload]
  -> m a
  -> m a
withJobsHeartbeat guard startTime jobs pending action = do
  inherit <- capturingContextIO
  withRunInIO $ \run -> guardBatch guard (Batch jobs (run pending) startTime inherit) (run action)

withPool :: Pool PG.Connection -> (SimpleEnv WorkerTestRegistry -> IO a) -> IO a
withPool sharedPool action = do
  env <- createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema
  withResource sharedPool $ \conn -> cleanupData testSchema testTable conn
  action env

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable True) $ do
  sharedPool <- runIO (createSharedPool connStr)
  around (withPool sharedPool) $ do
    describe "Guard registration" $ do
      it "returns once a signal in flight meets the unregister" $ \env -> do
        job <- inserted env (defaultJob (SlowTask 1))
        config <- transactionalWorkerConfig 1 idleHandler
        guard <- runSimpleDb env (newHeartbeatGuard config {maxJobDuration = Just 1, logConfig = silentLogConfig})
        startTime <- getCurrentTime
        withAsync (runSimpleDb env (runHeartbeatGuard guard)) $ \_ -> do
          registration <-
            async . runSimpleDb env . mask_ $ do
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
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = liftIO $ do
              atomicModifyIORef' startedRef (\started -> (primaryKey job : started, ()))
              threadDelay siblingRunMicros
              atomicModifyIORef' finishedRef (\finished -> (primaryKey job : finished, ()))
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
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

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 2) . length <$> readIORef startedRef
          reclaimJob connStr stolenId
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
        batch <- runSimpleDb env (HL.claimNextVisibleJobsAs @WorkerTestPayload 2 20 (workerId config))
        (handed, kept) <- case partition ((== handedId) . primaryKey) batch of
          ([job], [sibling]) -> pure (job, sibling)
          _ -> fail "expected two claimed jobs"
        releaseRow connStr handedId
        reclaimed <- runSimpleDb env (HL.claimNextVisibleJobsAs @WorkerTestPayload 1 20 (workerId config)) >>= single
        guard <- runSimpleDb env (newHeartbeatGuard guardConfig)
        startTime <- getCurrentTime
        withAsync (runSimpleDb env (runHeartbeatGuard guard)) $ \_ -> do
          keeper <-
            async . runSimpleDb env $
              withJobsHeartbeat guard startTime (handed :| [kept]) (pure [kept]) (liftIO (threadDelay siblingRunMicros))
          holder <-
            async . runSimpleDb env $
              withJobsHeartbeat guard startTime (reclaimed :| []) (pure [reclaimed]) (liftIO (threadDelay siblingRunMicros))
          threadDelay beforeCancelMicros
          flagCancelled connStr handedId
          holderOutcome <- waitCatch holder
          keeperOutcome <- waitCatch keeper
          either isForceCancelled (const False) holderOutcome `shouldBe` True
          keeperOutcome `shouldSatisfy` isRight

      it "beats a batch while a sibling batch's row is locked" $ \env -> do
        startedRef <- newIORef ([] :: [Int64])
        beatsRef <- newIORef ([] :: [(Int64, Double)])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = liftIO $ do
              atomicModifyIORef' startedRef (\started -> (primaryKey job : started, ()))
              threadDelay handlerSleepMicros
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
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
        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 2) . length <$> readIORef startedRef
          lockedFrom <- getMonotonicTime
          holdRowLock connStr lockedId rowLockMicros
          beats <- readIORef beatsRef
          [() | (jobId, at) <- beats, jobId == freeId, at > lockedFrom + lockedBeatFrom, at < lockedFrom + lockedBeatTo]
            `shouldSatisfy` (not . null)

      it "extends a grouped job whose handler enqueued into its group" $ \env -> do
        loggedRef <- newIORef ([] :: [Text])
        finishedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = case payload job of
              SlowTask _ -> do
                void (HL.insertJob (setGroupKey (Just "shared") (defaultJob (SimpleTask "child"))))
                liftIO (threadDelay groupedHandlerMicros)
                liftIO (atomicModifyIORef' finishedRef (\count -> (count + 1, ())))
              _ -> pure ()
            capture _level msg _ctx = atomicModifyIORef' loggedRef (\messages -> (msg : messages, ()))
        parent <- inserted env (setGroupKey (Just "shared") (setMaxAttempts (Just 1) (defaultJob (SlowTask 3))))
        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 20
                , jobHeartbeatInterval = 1
                , logConfig = defaultLogConfig {logDestination = LogCallback capture}
                }
        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 1) <$> readIORef finishedRef
          waitUntil waitMillis $ (== 0) <$> rowCount connStr (primaryKey parent)
          logged <- readIORef loggedRef
          filter (T.isInfixOf "deadlock" . T.toLower) logged `shouldBe` []
          dlq <- listDLQ env
          map (primaryKey . jobSnapshot) dlq `shouldBe` []

    describe "Job deadline" $ do
      it "interrupts a handler that outruns the maximum job duration" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))

        void (insertedId env)

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 1) <$> readIORef startedRef
          waitUntil dlqWaitMillis $ not . null <$> listDLQ env
          dlq <- listDLQ env
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          map (lastError . jobSnapshot) dlq
            `shouldBe` [Just "handler ran past the maximum job duration of 1s"]

      it "fences a batch on time while another batch's heartbeat hook is slow" $ \env -> do
        startedRef <- newIORef ([] :: [(Int64, Double)])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = liftIO $ do
              now <- getMonotonicTime
              atomicModifyIORef' startedRef (\started -> ((primaryKey job, now) : started, ()))
              threadDelay handlerSleepMicros
        slowId <- insertedId env
        let hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
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

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
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
              :: NonEmpty (JobRead WorkerTestPayload)
              -> BatchCallbacks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
              -> SimpleDb WorkerTestRegistry IO ()
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

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil slowWaitMillis $ any ("finalized" `T.isInfixOf`) <$> readIORef loggedRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          logged <- readIORef loggedRef
          filter ("maximum job duration" `T.isInfixOf`) logged `shouldSatisfy` (not . null)

      it "keeps beating while a masked handler holds off the deadline" $ \env -> do
        startedRef <- newIORef (Nothing :: Maybe Double)
        finishedRef <- newIORef (0 :: Int)
        beatsRef <- newIORef ([] :: [Double])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              getMonotonicTime >>= writeIORef startedRef . Just
              uninterruptibleMask_ (threadDelay maskedMicros)
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
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

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
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
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              void (tryAny (threadDelay handlerSleepMicros))
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))

        void (insertedId env)

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 1) <$> readIORef startedRef
          waitUntil dlqWaitMillis $ not . null <$> listDLQ env
          finished <- readIORef finishedRef
          finished `shouldBe` 0

      it "keeps the lease while a fenced handler unwinds" $ \env -> do
        startedRef <- newIORef (Nothing :: Maybe Double)
        finishedRef <- newIORef (0 :: Int)
        beatsRef <- newIORef ([] :: [Double])
        reasonsRef <- newIORef ([] :: [Text])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job =
              liftIO
                ( do
                    getMonotonicTime >>= writeIORef startedRef . Just
                    threadDelay handlerSleepMicros
                    atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
                )
                `finally` liftIO (threadDelay unwindMicros)
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
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

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
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
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
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

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil waitMillis $ (== 1) <$> readIORef startedRef
          takeClaimHolder connStr
          waitUntil slowWaitMillis $ not . null <$> readIORef reasonsRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          reasons <- readIORef reasonsRef
          reasons `shouldBe` [leaseExpiredReason]

listDLQ :: SimpleEnv WorkerTestRegistry -> IO [DLQJob WorkerTestPayload]
listDLQ env = runSimpleDb env (HL.listDLQJobs 10 0)

maskedSeconds :: Double
maskedSeconds = fromIntegral maskedMicros / 1_000_000

recordBeat :: (MonadIO m) => IORef [Double] -> m ()
recordBeat beatsRef = liftIO $ getMonotonicTime >>= \now -> atomicModifyIORef' beatsRef (\beats -> (now : beats, ()))

-- | Take the claim without bumping its token. The extend then reports 'VisibilityUnchanged'.
takeClaimHolder :: ByteString -> IO ()
takeClaimHolder connStr = withFreshConn connStr $ \conn ->
  void $
    PG.execute_
      conn
      ( fromString
          ( T.unpack
              ( "UPDATE "
                  <> qualifiedTable
                  <> " SET claimed_by = '00000000-0000-0000-0000-000000000009'::uuid WHERE claimed_by IS NOT NULL"
              )
          )
      )

-- | Run one UPDATE on a job row over a fresh connection.
updateJob :: ByteString -> Text -> Int64 -> IO ()
updateJob connStr setClause jobId = withFreshConn connStr $ \conn ->
  void $
    PG.execute
      conn
      (fromString (T.unpack ("UPDATE " <> qualifiedTable <> " SET " <> setClause <> " WHERE id = ?")))
      (PG.Only jobId)

-- | Take the claim under a new token, as another worker's claim does.
reclaimJob :: ByteString -> Int64 -> IO ()
reclaimJob connStr = updateJob connStr "attempts = attempts + 1, claim_seq = claim_seq + 1"

qualifiedTable :: Text
qualifiedTable = testSchema <> "." <> testTable

withFreshConn :: ByteString -> (PG.Connection -> IO a) -> IO a
withFreshConn connStr = bracket (connectPostgreSQL connStr) close

single :: [a] -> IO a
single [x] = pure x
single _ = fail "expected exactly one job"

isForceCancelled :: SomeException -> Bool
isForceCancelled exc = isJust (fromException exc :: Maybe JobForceCancelled)

stamp :: IORef [(Int64, Double)] -> JobRead WorkerTestPayload -> IO ()
stamp ref job = getMonotonicTime >>= \now -> atomicModifyIORef' ref (\seen -> ((primaryKey job, now) : seen, ()))

-- | Hold a row lock on one job for @micros@, as a transaction touching it would.
holdRowLock :: ByteString -> Int64 -> Int -> IO ()
holdRowLock connStr jobId micros = withFreshConn connStr $ \conn -> do
  PG.begin conn
  _ <-
    PG.query
      conn
      (fromString (T.unpack ("SELECT id FROM " <> qualifiedTable <> " WHERE id = ? FOR UPDATE")))
      (PG.Only jobId)
      :: IO [PG.Only Int64]
  threadDelay micros
  PG.commit conn

-- | Flag a job cancelled under its lease, as a force-cancel does, without the NOTIFY.
flagCancelled :: ByteString -> Int64 -> IO ()
flagCancelled connStr = updateJob connStr "cancel_requested_at = NOW(), claim_seq = claim_seq + 1"

-- | Release a claim and make the row claimable now.
releaseRow :: ByteString -> Int64 -> IO ()
releaseRow connStr = updateJob connStr "claimed_by = NULL, not_visible_until = NULL"

-- | How many rows carry the job id.
rowCount :: ByteString -> Int64 -> IO Int
rowCount connStr jobId = withFreshConn connStr $ \conn -> do
  [PG.Only count] <-
    PG.query
      conn
      (fromString (T.unpack ("SELECT count(*)::int FROM " <> qualifiedTable <> " WHERE id = ?")))
      (PG.Only jobId)
  pure count

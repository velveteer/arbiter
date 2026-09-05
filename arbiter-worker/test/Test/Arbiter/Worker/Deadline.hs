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
import Arbiter.Simple (SimpleDb, SimpleEnv, createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (cleanupData, createSharedPool, setupOnce)
import Control.Concurrent
  ( MVar
  , forkIO
  , forkOn
  , forkOnWithUnmask
  , killThread
  , newEmptyMVar
  , putMVar
  , takeMVar
  , threadDelay
  , yield
  )
import Control.Exception (SomeAsyncException, SomeException, evaluate, fromException, uninterruptibleMask_)
import Control.Exception qualified as E
import Control.Monad (forever, replicateM, void, when)
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
import Data.Time (NominalDiffTime, addUTCTime, getCurrentTime)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Clock (getMonotonicTime)
import Test.Hspec (Spec, around, beforeAll, describe, it, runIO, shouldBe, shouldSatisfy)
import UnliftIO (bracket, bracket_, finally, mask_, tryAny)
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
import Arbiter.Worker.Heartbeat (newHeartbeatGuard, runHeartbeatGuard, withJobsHeartbeat)
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

-- | How long a hung extend sleeps on the server. Longer than the lease under test.
hangSeconds :: Int
hangSeconds = 8

-- | The fence must give up on a hung extend well before the extend returns.
hungFenceMillis :: Int
hungFenceMillis = 5_000

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

-- | Registrations raced against a fence already due.
registerRaceRounds :: Int
registerRaceRounds = 500

-- | How long each raced registration's handler runs.
registerRaceMicros :: Int
registerRaceMicros = 2_000

-- | The capability the raced handler and the spinner share.
pinnedCapability :: Int
pinnedCapability = 0

-- | How long the spinner holds the capability each time the raced handler yields.
spinnerHoldSeconds :: Double
spinnerHoldSeconds = 0.001

-- | Allocation per spinner step, so the hold stays preemptible.
spinnerWork :: Int
spinnerWork = 256

-- | Lease left at register, so the first beat lands just before the lease.
lateExtendRemaining :: NominalDiffTime
lateExtendRemaining = 0.3

-- | How long the server holds the late extend. Past the lease, inside the retry pause.
lateExtendSleep :: Double
lateExtendSleep = 0.13

-- | How long the handler under a late extend runs.
lateExtendRunMicros :: Int
lateExtendRunMicros = 1_000_000

-- | Insert a slow job with the default attempt budget and return its id.
insertedPlainId :: SimpleEnv WorkerTestRegistry -> IO Int64
insertedPlainId env =
  runSimpleDb env (HL.insertJob (defaultJob (SlowTask 30)))
    >>= maybe (fail "insert returned no job") (pure . primaryKey)

-- | Insert a single-attempt slow job and return its id.
insertedId :: SimpleEnv WorkerTestRegistry -> IO Int64
insertedId env =
  runSimpleDb env (HL.insertJob (setMaxAttempts (Just 1) (defaultJob (SlowTask 30))))
    >>= maybe (fail "insert returned no job") (pure . primaryKey)

withPool :: Pool PG.Connection -> (SimpleEnv WorkerTestRegistry -> IO a) -> IO a
withPool sharedPool action = do
  env <- createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema
  withResource sharedPool $ \conn -> cleanupData testSchema testTable conn
  action env

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable True) $ do
  sharedPool <- runIO (createSharedPool connStr)
  around (withPool sharedPool) $ do
    describe "Heartbeat scheduling" $ do
      it "stops retrying once the lease is spent" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        loggedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job =
              liftIO (atomicModifyIORef' startedRef (\count -> (count + 1, ())) >> threadDelay handlerSleepMicros)
                `finally` liftIO (threadDelay unwindMicros)
            capture _level msg _ctx =
              when ("Heartbeat error" `T.isInfixOf` msg) $
                atomicModifyIORef' loggedRef (\count -> (count + 1, ()))

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 30))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 4
                , jobHeartbeatInterval = 1
                , logConfig = defaultLogConfig {logDestination = LogCallback capture}
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          attempts <-
            bracket_ (blockRowUpdates connStr) (unblockRowUpdates connStr) $ do
              threadDelay (unwindMicros + 6_000_000)
              readIORef loggedRef
          attempts `shouldSatisfy` (< 25)

      it "keeps beating at its interval after a failed extend" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        beatsRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay 9_000_000
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \_ _ _ -> liftIO (atomicModifyIORef' beatsRef (\count -> (count + 1, ())))
                }

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 9))

        config <- transactionalWorkerConfig 1 handler
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
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          bracket_ (blockRowUpdates connStr) (unblockRowUpdates connStr) (threadDelay 1_500_000)
          threadDelay 5_000_000
          beats <- readIORef beatsRef
          beats `shouldSatisfy` (>= 3)

    describe "Guard registration" $ do
      it "returns once a signal in flight meets the unregister" $ \env -> do
        job <- runSimpleDb env (HL.insertJob (defaultJob (SlowTask 1))) >>= maybe (fail "insert returned no job") pure
        let idle :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            idle _conn _job = pure ()
        config <- transactionalWorkerConfig 1 idle
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

      -- The raced window is only hit often under @+RTS -C0@.
      it "keeps a signal due at register inside the handler boundary" $ \env -> do
        job <- runSimpleDb env (HL.insertJob (defaultJob (SlowTask 1))) >>= maybe (fail "insert returned no job") pure
        let idle :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            idle _conn _job = pure ()
        config <- transactionalWorkerConfig 1 idle
        guard <-
          runSimpleDb
            env
            (newHeartbeatGuard config {visibilityTimeout = 1, jobHeartbeatInterval = 10, logConfig = silentLogConfig})
        startTime <- addUTCTime (-5) <$> getCurrentTime
        withAsync (runSimpleDb env (runHeartbeatGuard guard)) $ \_ ->
          withPinnedSpinner $ do
            outcomes <- replicateM registerRaceRounds $ do
              done <- newEmptyMVar :: IO (MVar (Either SomeException ()))
              -- The fork makes the capability switch at its next heap check.
              _ <-
                forkOn pinnedCapability $
                  E.try
                    ( forkIO (pure ())
                        *> runSimpleDb env (withJobsHeartbeat guard startTime (job :| []) (pure [job]) (liftIO (threadDelay registerRaceMicros)))
                    )
                    >>= putMVar done
              takeMVar done
            length (filter escapedGuard outcomes) `shouldBe` 0

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
                    liftIO $ getMonotonicTime >>= \now -> atomicModifyIORef' beatsRef (\beats -> ((primaryKey job, now) : beats, ()))
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
          waitUntil 10_000 $ (== 2) . length <$> readIORef startedRef
          reclaimJob connStr stolenId
          waitUntil 10_000 $ not . null <$> readIORef reasonsRef
          reclaimedAt <- getMonotonicTime
          waitUntil 15_000 $ not . null <$> readIORef finishedRef
          reasons <- readIORef reasonsRef
          reasons `shouldBe` [(stolenId, "reclaimed by another worker")]
          finished <- readIORef finishedRef
          finished `shouldBe` [keptId]
          beats <- readIORef beatsRef
          [() | (jobId, at) <- beats, jobId == keptId, at > reclaimedAt] `shouldSatisfy` (not . null)

    describe "Shared guard" $ do
      it "stops only the batch whose live claim the cancel names" $ \env -> do
        let idle :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            idle _conn _job = pure ()
        config <- transactionalWorkerConfig 1 idle
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
          threadDelay 700_000
          flagCancelled connStr handedId
          holderOutcome <- waitCatch holder
          keeperOutcome <- waitCatch keeper
          either isForceCancelled (const False) holderOutcome `shouldBe` True
          keeperOutcome `shouldSatisfy` isRight

      it "fences a batch registered while another batch's extend hangs" $ \env -> do
        startedRef <- newIORef ([] :: [(Int64, Double)])
        endedRef <- newIORef ([] :: [(Int64, Double)])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = liftIO $ do
              stamp startedRef job
              threadDelay handlerSleepMicros `finally` (stamp endedRef job >> threadDelay handlerSleepMicros)
        _ <- insertedId env
        config <- transactionalWorkerConfig 2 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 20
                , jobHeartbeatInterval = 1
                , maxJobDuration = Just 2
                , logConfig = silentLogConfig
                }
        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) . length <$> readIORef startedRef
          bracket_ (hangExtends connStr) (unblockRowUpdates connStr) $ do
            threadDelay 3_500_000
            fencedId <- insertedId env
            waitUntil 10_000 $ isJust . lookup fencedId <$> readIORef startedRef
            waitUntil 20_000 $ isJust . lookup fencedId <$> readIORef endedRef
            started <- readIORef startedRef
            ended <- readIORef endedRef
            (fromMaybe 0 (lookup fencedId ended) - fromMaybe 0 (lookup fencedId started)) `shouldSatisfy` (< 4)
        awaitHungUpdates connStr

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
          waitUntil 10_000 $ (== 2) . length <$> readIORef startedRef
          lockedFrom <- getMonotonicTime
          holdRowLock connStr lockedId 4_000_000
          beats <- readIORef beatsRef
          [() | (jobId, at) <- beats, jobId == freeId, at > lockedFrom + 1, at < lockedFrom + 3.5] `shouldSatisfy` (not . null)

      it "signals a handler again once a beat finds it still running" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        firstRef <- newIORef (Nothing :: Maybe Double)
        endedRef <- newIORef (Nothing :: Maybe Double)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              ( threadDelay handlerSleepMicros `E.catch` \(_ :: SomeException) -> do
                  getMonotonicTime >>= writeIORef firstRef . Just
                  threadDelay handlerSleepMicros
                )
                `finally` (getMonotonicTime >>= writeIORef endedRef . Just)
        jobId <- insertedId env
        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 20
                , jobHeartbeatInterval = 0.5
                , logConfig = silentLogConfig
                }
        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          flagCancelled connStr jobId
          waitUntil 10_000 $ isJust <$> readIORef firstRef
          waitUntil 5_000 $ isJust <$> readIORef endedRef
          first <- fromMaybe 0 <$> readIORef firstRef
          ended <- fromMaybe 0 <$> readIORef endedRef
          (ended - first) `shouldSatisfy` (< 3)

      it "delivers one signal when the lease and the deadline pass together" $ \env -> do
        job <- runSimpleDb env (HL.insertJob (defaultJob (SlowTask 1))) >>= maybe (fail "insert returned no job") pure
        let idle :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            idle _conn _job = pure ()
        config <- transactionalWorkerConfig 1 idle
        guard <-
          runSimpleDb env $
            newHeartbeatGuard
              config {maxJobDuration = Just 0.001, visibilityTimeout = 1, jobHeartbeatInterval = 10, logConfig = silentLogConfig}
        signalsRef <- newIORef (0 :: Int)
        startTime <- addUTCTime (-5) <$> getCurrentTime
        withAsync (runSimpleDb env (runHeartbeatGuard guard)) $ \_ -> do
          outcome <-
            tryAny . runSimpleDb env $
              withJobsHeartbeat guard startTime (job :| []) (pure [job]) (liftIO (countSignals signalsRef 1_500_000))
          outcome `shouldSatisfy` isRight
          signals <- readIORef signalsRef
          signals `shouldBe` 1

      it "extends a grouped job whose handler enqueued into its group" $ \env -> do
        loggedRef <- newIORef ([] :: [Text])
        finishedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn job = case payload job of
              SlowTask _ -> do
                void (HL.insertJob (setGroupKey (Just "shared") (defaultJob (SimpleTask "child"))))
                liftIO (threadDelay 2_500_000)
                liftIO (atomicModifyIORef' finishedRef (\count -> (count + 1, ())))
              _ -> pure ()
            capture _level msg _ctx = atomicModifyIORef' loggedRef (\messages -> (msg : messages, ()))
        parent <-
          runSimpleDb env (HL.insertJob (setGroupKey (Just "shared") (setMaxAttempts (Just 1) (defaultJob (SlowTask 3)))))
            >>= maybe (fail "insert returned no job") pure
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
          waitUntil 10_000 $ (== 1) <$> readIORef finishedRef
          waitUntil 10_000 $ (== 0) <$> rowCount connStr (primaryKey parent)
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

        runSimpleDb env
          $ void
          $ HL.insertJob (setMaxAttempts (Just 1) (defaultJob (SlowTask 30)))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          waitUntil 20_000 $ not . null <$> listDLQ env
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
          waitUntil 10_000 $ (== 1) . length <$> readIORef startedRef
          threadDelay 1_300_000
          fencedId <- insertedId env
          waitUntil 10_000 $ (== 2) . length <$> readIORef startedRef
          waitUntil 15_000 $ any ((== fencedId) . primaryKey . jobSnapshot) <$> listDLQ env
          fencedAt <- getMonotonicTime
          started <- readIORef startedRef
          let startedAt = fromMaybe 0 (lookup fencedId started)
          (fencedAt - startedAt) `shouldSatisfy` (< 5)

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

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 30))

        config <- defaultBatchedWorkerConfig 1 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = defaultLogConfig {logDestination = LogCallback capture}
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 15_000 $ any ("finalized" `T.isInfixOf`) <$> readIORef loggedRef
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

        runSimpleDb env
          $ void
          $ HL.insertJob (setMaxAttempts (Just 1) (defaultJob (SlowTask 30)))

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
          waitUntil 10_000 $ isJust <$> readIORef startedRef
          waitUntil 20_000 $ not . null <$> listDLQ env
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

        runSimpleDb env
          $ void
          $ HL.insertJob (setMaxAttempts (Just 1) (defaultJob (SlowTask 30)))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          waitUntil 20_000 $ not . null <$> listDLQ env
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

        runSimpleDb env
          $ void
          $ HL.insertJob (setMaxAttempts (Just 1) (defaultJob (SlowTask 30)))

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
          waitUntil 10_000 $ isJust <$> readIORef startedRef
          waitUntil 20_000 $ not . null <$> listDLQ env
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
      it "stops a handler once its heartbeat can no longer reach the database" $ \env -> do
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

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 30))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 3
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          (reasons, finished) <-
            bracket_ (blockRowUpdates connStr) (unblockRowUpdates connStr) $ do
              waitUntil 20_000 $ not . null <$> readIORef reasonsRef
              (,) <$> readIORef reasonsRef <*> readIORef finishedRef
          finished `shouldBe` 0
          reasons `shouldBe` ["lease expired without renewal"]

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

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 30))

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
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          takeClaimHolder connStr
          waitUntil 15_000 $ not . null <$> readIORef reasonsRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          reasons <- readIORef reasonsRef
          reasons `shouldBe` ["lease expired without renewal"]

      it "stops a handler whose extend hangs past the lease" $ \env -> do
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

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 30))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 3
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          (reasons, finished) <-
            bracket_ (hangRowUpdates connStr) (unblockRowUpdates connStr) $ do
              waitUntil hungFenceMillis $ not . null <$> readIORef reasonsRef
              (,) <$> readIORef reasonsRef <*> readIORef finishedRef
          finished `shouldBe` 0
          reasons `shouldBe` ["lease expired without renewal"]
        awaitHungUpdates connStr

      it "keeps a handler whose extend lands after the lease" $ \env -> do
        let idle :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            idle _conn _job = pure ()
        config <- transactionalWorkerConfig 1 idle
        let guardConfig = config {visibilityTimeout = 2, jobHeartbeatInterval = 1, logConfig = silentLogConfig}
        _ <- insertedPlainId env
        job <- runSimpleDb env (HL.claimNextVisibleJobsAs @WorkerTestPayload 1 20 (workerId config)) >>= single
        guard <- runSimpleDb env (newHeartbeatGuard guardConfig)
        startTime <- addUTCTime (lateExtendRemaining - visibilityTimeout guardConfig) <$> getCurrentTime
        withAsync (runSimpleDb env (runHeartbeatGuard guard)) $ \_ ->
          bracket_ (delayRowUpdates connStr) (unblockRowUpdates connStr) $ do
            outcome <-
              tryAny . runSimpleDb env $
                withJobsHeartbeat guard startTime (job :| []) (pure [job]) (liftIO (threadDelay lateExtendRunMicros))
            outcome `shouldSatisfy` isRight

      it "survives a failed heartbeat when the retry pause fits the lease" $ \env ->
        survivesOneFailedHeartbeat connStr env 8 4 11_000_000
      it "survives a failed heartbeat under a short visibility timeout" $ \env ->
        survivesOneFailedHeartbeat connStr env 5 2 8_000_000

-- | One extend fails. The assertion is the run count.
survivesOneFailedHeartbeat
  :: ByteString
  -> SimpleEnv WorkerTestRegistry
  -> NominalDiffTime
  -- ^ Visibility timeout.
  -> NominalDiffTime
  -- ^ Heartbeat interval. The block spans it and fails the extend due one interval in.
  -> Int
  -- ^ Handler run time in microseconds. It must outlast the lease.
  -> IO ()
survivesOneFailedHeartbeat connStr env timeout interval handlerMicros = do
  startedRef <- newIORef (0 :: Int)
  finishedRef <- newIORef (0 :: Int)
  let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
      handler _conn _job = liftIO $ do
        atomicModifyIORef' startedRef (\count -> (count + 1, ()))
        threadDelay handlerMicros
        atomicModifyIORef' finishedRef (\count -> (count + 1, ()))

  runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 1))

  config <- transactionalWorkerConfig 1 handler
  let workerConfig =
        config
          { pollInterval = 0.2
          , jitter = NoJitter
          , visibilityTimeout = timeout
          , jobHeartbeatInterval = interval
          , logConfig = silentLogConfig
          }
      blockMicros = ceiling (interval * 1_000_000) + 500_000

  withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
    waitUntil 10_000 $ (== 1) <$> readIORef startedRef
    bracket_ (blockRowUpdates connStr) (unblockRowUpdates connStr) (threadDelay blockMicros)
    waitUntil (handlerMicros `div` 1000 + 4_000) $ (== 1) <$> readIORef finishedRef
    started <- readIORef startedRef
    started `shouldBe` 1

listDLQ :: SimpleEnv WorkerTestRegistry -> IO [DLQJob WorkerTestPayload]
listDLQ env = runSimpleDb env (HL.listDLQJobs 10 0)

maskedSeconds :: Double
maskedSeconds = fromIntegral maskedMicros / 1_000_000

recordBeat :: (MonadIO m) => IORef [Double] -> m ()
recordBeat beatsRef = liftIO $ getMonotonicTime >>= \now -> atomicModifyIORef' beatsRef (\beats -> (now : beats, ()))

-- | Hold every row update on the server for 'lateExtendSleep'.
delayRowUpdates :: ByteString -> IO ()
delayRowUpdates = installUpdateTrigger ("BEGIN PERFORM pg_sleep(" <> T.pack (show lateExtendSleep) <> "); RETURN NEW; END;")

-- | Whether a guard signal left the handler boundary still asynchronous.
escapedGuard :: Either SomeException () -> Bool
escapedGuard = either (\exc -> isJust (fromException exc :: Maybe SomeAsyncException)) (const False)

-- | Run @act@ while a spinner on 'pinnedCapability' takes each yield for 'spinnerHoldSeconds'.
withPinnedSpinner :: IO a -> IO a
withPinnedSpinner act =
  bracket (forkOnWithUnmask pinnedCapability (\unmask -> unmask (forever (hold >> yield)))) killThread (const act)
  where
    hold = do
      release <- (+ spinnerHoldSeconds) <$> getMonotonicTime
      let spin = do
            void (evaluate (length (replicate spinnerWork ())))
            now <- getMonotonicTime
            when (now < release) spin
      spin

-- | Fail every row update. Once a job is claimed, the heartbeat's only statement is an update.
blockRowUpdates :: ByteString -> IO ()
blockRowUpdates = installUpdateTrigger "BEGIN RAISE EXCEPTION 'update blocked'; END;"

-- | Stall every row update on the server for 'hangSeconds', then let it through.
hangRowUpdates :: ByteString -> IO ()
hangRowUpdates = installUpdateTrigger ("BEGIN PERFORM pg_sleep(" <> T.pack (show hangSeconds) <> "); RETURN NEW; END;")

-- | Wait for the stalled extends to finish on the server, so the next test starts clean.
awaitHungUpdates :: ByteString -> IO ()
awaitHungUpdates connStr =
  waitUntil ((hangSeconds + 4) * 1_000) $ withFreshConn connStr $ \conn -> do
    [PG.Only sleeping] <-
      PG.query_
        conn
        "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%pg_sleep%' AND pid <> pg_backend_pid()"
    pure (sleeping == (0 :: Int))

installUpdateTrigger :: Text -> ByteString -> IO ()
installUpdateTrigger body connStr = withFreshConn connStr $ \conn -> do
  void $
    PG.execute_
      conn
      ( fromString
          ( T.unpack
              ( "CREATE OR REPLACE FUNCTION "
                  <> blockFunction
                  <> "() RETURNS trigger LANGUAGE plpgsql AS $$ "
                  <> body
                  <> " $$"
              )
          )
      )
  void $
    PG.execute_
      conn
      ( fromString
          ( T.unpack
              ( "CREATE TRIGGER block_update BEFORE UPDATE ON "
                  <> qualifiedTable
                  <> " FOR EACH ROW EXECUTE FUNCTION "
                  <> blockFunction
                  <> "()"
              )
          )
      )

unblockRowUpdates :: ByteString -> IO ()
unblockRowUpdates connStr = withFreshConn connStr $ \conn -> do
  void $ PG.execute_ conn (fromString (T.unpack ("DROP TRIGGER IF EXISTS block_update ON " <> qualifiedTable)))
  void $ PG.execute_ conn (fromString (T.unpack ("DROP FUNCTION IF EXISTS " <> blockFunction <> "()")))

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

-- | Take the claim under a new token, as another worker's claim does.
reclaimJob :: ByteString -> Int64 -> IO ()
reclaimJob connStr jobId = withFreshConn connStr $ \conn ->
  void $
    PG.execute
      conn
      ( fromString
          (T.unpack ("UPDATE " <> qualifiedTable <> " SET attempts = attempts + 1, claim_seq = claim_seq + 1 WHERE id = ?"))
      )
      (PG.Only jobId)

qualifiedTable :: Text
qualifiedTable = testSchema <> "." <> testTable

blockFunction :: Text
blockFunction = testSchema <> ".block_update"

withFreshConn :: ByteString -> (PG.Connection -> IO a) -> IO a
withFreshConn connStr = bracket (connectPostgreSQL connStr) close

single :: [a] -> IO a
single [x] = pure x
single _ = fail "expected exactly one job"

isForceCancelled :: SomeException -> Bool
isForceCancelled exc = isJust (fromException exc :: Maybe JobForceCancelled)

stamp :: IORef [(Int64, Double)] -> JobRead WorkerTestPayload -> IO ()
stamp ref job = getMonotonicTime >>= \now -> atomicModifyIORef' ref (\seen -> ((primaryKey job, now) : seen, ()))

-- | Count the asynchronous exceptions that land while sleeping for @micros@.
countSignals :: IORef Int -> Int -> IO ()
countSignals ref micros = do
  deadline <- (+ fromIntegral micros / 1_000_000) <$> getMonotonicTime
  let loop = do
        now <- getMonotonicTime
        when (now < deadline) $
          threadDelay (ceiling ((deadline - now) * 1_000_000))
            `E.catch` \(_ :: SomeException) -> atomicModifyIORef' ref (\count -> (count + 1, ())) >> loop
  loop

-- | Stall every extend on the server for 'hangSeconds'. Claims and releases pass.
hangExtends :: ByteString -> IO ()
hangExtends =
  installUpdateTrigger
    ( "BEGIN IF OLD.claimed_by IS NOT NULL AND NEW.claimed_by IS NOT DISTINCT FROM OLD.claimed_by THEN PERFORM pg_sleep("
        <> T.pack (show hangSeconds)
        <> "); END IF; RETURN NEW; END;"
    )

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
flagCancelled connStr jobId = withFreshConn connStr $ \conn ->
  void $
    PG.execute
      conn
      ( fromString
          (T.unpack ("UPDATE " <> qualifiedTable <> " SET cancel_requested_at = NOW(), claim_seq = claim_seq + 1 WHERE id = ?"))
      )
      (PG.Only jobId)

-- | Release a claim and make the row claimable now.
releaseRow :: ByteString -> Int64 -> IO ()
releaseRow connStr jobId = withFreshConn connStr $ \conn ->
  void $
    PG.execute
      conn
      (fromString (T.unpack ("UPDATE " <> qualifiedTable <> " SET claimed_by = NULL, not_visible_until = NULL WHERE id = ?")))
      (PG.Only jobId)

-- | How many rows carry the job id.
rowCount :: ByteString -> Int64 -> IO Int
rowCount connStr jobId = withFreshConn connStr $ \conn -> do
  [PG.Only count] <-
    PG.query
      conn
      (fromString (T.unpack ("SELECT count(*)::int FROM " <> qualifiedTable <> " WHERE id = ?")))
      (PG.Only jobId)
  pure count

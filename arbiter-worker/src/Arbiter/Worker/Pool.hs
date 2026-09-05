{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Execution of one worker pool and its job lifecycle. Multi-queue
-- orchestration lives in "Arbiter.Worker.MultiQueue".
module Arbiter.Worker.Pool
  ( runWorkerPool
  , runReaperOp
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.JobResult
import Arbiter.Core.Listen qualified as Listen
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Arbiter.Core.Trace (ConsumeShape (..), consumeSpanFor)
import Control.Exception qualified as E
import Control.Monad (forever, replicateM, unless, void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Bool (bool)
import Data.Foldable (traverse_)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (NominalDiffTime)
import System.Directory (removeFile)
import UnliftIO
  ( MonadUnliftIO
  , atomically
  , checkSTM
  , finally
  , newTVarIO
  , readTVar
  , tryAny
  , waitAnyCatch
  , writeTVar
  )
import UnliftIO.Async qualified as Async
import UnliftIO.Chan (newChan)
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.STM (STM, TVar)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.ChannelHandlers
  ( handleCancelNotif
  , handleCronRunNotif
  , handlePauseNotif
  )
import Arbiter.Worker.Config
import Arbiter.Worker.Cron (CronJob (..), runCronScheduler)
import Arbiter.Worker.Dispatcher
import Arbiter.Worker.Heartbeat (newHeartbeatGuard, runHeartbeatGuard)
import Arbiter.Worker.Logger
import Arbiter.Worker.Logger.Internal (tryWarn, tryWarnWith)
import Arbiter.Worker.Processing (workerLoop)
import Arbiter.Worker.Reaper (MaintenancePace (..), reaperLoop, runReaperOp)
import Arbiter.Worker.Retry (spawnRetried)

-- ---------------------------------------------------------------------------
-- Worker Pool
-- ---------------------------------------------------------------------------

-- | The span shape for claims made by a pool.
poolSpanShape :: WorkerConfig m payload -> ConsumeShape
poolSpanShape = bool PerJob PerBatch . (> 1) . handlerBatchSize

-- | The pace the pool's reaper keeps.
reaperPace :: WorkerConfig m payload -> MaintenancePace
reaperPace config =
  MaintenancePace
    { paceWindow = reaperInterval config
    , paceSparseWindow = reaperSparseInterval config
    , paceBucketIdle = reaperBucketIdle config
    }

-- | Run a worker pool: a dispatcher and its worker threads.
runWorkerPool
  :: forall payload m
   . ( Arb.RegistryAdmissionPolicies (RegistryOf m)
     , EncodeJobResult (ResultOf m payload)
     , QueueOperation m payload
     , RegistryTables (RegistryOf m)
     )
  => WorkerConfig m payload
  -> m ()
runWorkerPool config = do
  either (liftIO . E.throwIO . WorkerConfigException) pure (validateWorkerConfig config)
  let workerCap = workerCount config
      queueName = Arb.queueTable @payload @m
      -- Built once for the pool.
      consumeSpan = consumeSpanFor queueName (poolSpanShape config)

  schemaName <- getSchema
  workQueue <- newChan
  queuedCount <- newTVarIO 0
  busyWorkerCount <- newTVarIO 0
  workerFinishedVar <- newTVarIO False
  runningJobs <- STM.newTVarIO Map.empty
  guard <- newHeartbeatGuard config
  statements <-
    Arb.mkJobStatements @payload (handlerBatchSize config) workerCap (visibilityTimeout config) (workerId config)

  tryAny (registerSelf config schemaName queueName)
    >>= either
      ( \exception ->
          warnEx (logConfig config) "Worker registry insert failed, starting paused" exception
            *> atomically (writePause config True)
      )
      (traverse_ (atomically . writePause config))

  dispatcherNotifVar <- STM.newTVarIO Nothing
  cronRunVar <- STM.newTVarIO False
  let createChannel = TE.encodeUtf8 (Schema.notificationChannelForTable queueName)
      pauseChannel = TE.encodeUtf8 (Schema.pauseNotifyChannel schemaName queueName)
      cancelChannel = TE.encodeUtf8 (Schema.cancelNotifyChannel schemaName queueName)
      cronRunChannel = TE.encodeUtf8 (Schema.cronRunNotifyChannel schemaName)
      cronNames = Set.fromList (map name (cronJobs config))
      cronHandlers =
        if null (cronJobs config)
          then []
          else [(cronRunChannel, handleCronRunNotif cronNames cronRunVar)]
      handlers =
        [ (createChannel, atomically . STM.writeTVar dispatcherNotifVar . Just)
        , (pauseChannel, handlePauseNotif config)
        , (cancelChannel, handleCancelNotif config runningJobs)
        ]
          <> cronHandlers

  evalContT $ do
    withLivenessFile config
    mListener <- lift getListener
    listenerReady <- case mListener of
      Nothing -> do
        lift $ tryLog (logConfig config) Info "No listen connection, running poll-only"
        pure (pure True)
      Just listener ->
        ContT $
          Listen.withChannels listener (hubLogFor (logConfig config)) handlers
    void . ContT $ Async.withAsync (publishListenerReady config listenerReady)
    let spawn = spawnRetried (workerStateVar config) (logConfig config) queueName
    heartbeat <-
      spawn "Worker heartbeat" $
        heartbeatLoop config schemaName queueName
    jobGuard <-
      spawn "Job heartbeat guard" $
        void (runHeartbeatGuard guard)
    dispatcher <-
      spawn "Dispatcher" $
        runDispatcher config workerCap statements workQueue queuedCount busyWorkerCount workerFinishedVar dispatcherNotifVar
    workers <-
      replicateM workerCap
        $ spawn "Worker thread"
        $ workerLoop config consumeSpan runningJobs guard statements workQueue queuedCount busyWorkerCount workerFinishedVar
    crons <-
      unlessNull (cronJobs config)
        $ spawn "Cron scheduler"
        $ runCronScheduler (workerStateVar config) cronRunVar (logConfig config) schemaName queueName (cronJobs config)
    reaper <-
      spawn "Reaper" $
        reaperLoop (logConfig config) (onMaintenance config) (reaperPace config) (reaperTimeout config)

    (_, res) <- waitAnyCatch (dispatcher : jobGuard : reaper : heartbeat : crons <> workers)
    case res of
      Left exception ->
        lift $ tryLog (logConfig config) Error $ "Thread pool exception: " <> displayEx exception
      Right _ -> pure ()

    lift $ shutdownPool config schemaName queuedCount busyWorkerCount

-- | Flip 'listenerReadyVar' once the pool's channels are subscribed. Runs
-- alongside the pool. Startup does not wait on it.
publishListenerReady :: (MonadUnliftIO m) => WorkerConfig n payload -> STM Bool -> m ()
publishListenerReady config ready =
  atomically $ do
    ready >>= checkSTM
    writeTVar (listenerReadyVar config) True

-- | Remove the liveness file when the pool exits, after the drain.
withLivenessFile :: (MonadUnliftIO m) => WorkerConfig n payload -> ContT r m ()
withLivenessFile config =
  traverse_
    (\path -> ContT $ \continue -> continue () `finally` (void . tryAny . liftIO . removeFile) path)
    (livenessFile config)

-- | Re-insert the worker's registry row, returning the effective paused state for the
-- caller to seed 'pauseVar' with.
registerSelf :: (MonadArbiter m) => WorkerConfig n payload -> SchemaName -> Text -> m (Maybe Bool)
registerSelf config schemaName queueName =
  Ops.registerWorker
    schemaName
    (workerId config)
    queueName
    (workerHost config)
    (Just (fromIntegral (workerCount config)))
    (workerStaleThreshold config)
    (workerMetadata config)

-- | Mark shutting-down, drain, then deregister. Every write is best-effort and logged
-- when it fails.
shutdownPool
  :: (MonadArbiter m)
  => WorkerConfig n payload
  -> SchemaName
  -> TVar Int
  -> TVar Int
  -> m ()
shutdownPool config schemaName queuedCount busyCount = do
  shutdownWorker config
  let wid = workerId config
      logCfg = logConfig config
  tryWarn logCfg "Failed to mark worker shutting down" (Ops.markWorkerShuttingDown schemaName wid)
  drainPool logCfg (gracefulShutdownTimeout config) queuedCount busyCount
  tryWarn logCfg "Failed to deregister worker" (Ops.deregisterWorker schemaName wid)

-- | Wait for the work queue to drain and all worker threads to go idle,
-- optionally bounded by a timeout. Logs the entry, periodic progress (every
-- 10s) when no timeout is set, and the result.
drainPool
  :: (MonadUnliftIO m)
  => LogConfig
  -> Maybe NominalDiffTime
  -> TVar Int
  -> TVar Int
  -> m ()
drainPool logCfg mTimeout queuedCount busyCount = do
  tryLog logCfg Info "Starting graceful shutdown. Draining in-flight jobs..."
  result <- case mTimeout of
    Nothing -> Right () <$ drainLoop
    Just timeoutSecs ->
      Async.race (threadDelay (Ops.micros timeoutSecs)) waitForDrain
  case result of
    Right () -> tryLog logCfg Info "All workers are now idle. Graceful shutdown complete."
    Left () -> tryLog logCfg Warning "Graceful shutdown timed out. Some jobs may still be in-flight."
  where
    waitForDrain = atomically $ do
      queued <- readTVar queuedCount
      checkSTM (queued == 0)
      busy <- readTVar busyCount
      checkSTM (busy == 0)
    drainLoop = do
      drainOrTick <- Async.race (threadDelay 10_000_000) waitForDrain
      case drainOrTick of
        Right () -> pure ()
        Left () -> do
          (busy, qLen) <-
            atomically $
              (,)
                <$> readTVar busyCount
                <*> readTVar queuedCount
          tryLog logCfg Info $
            "Graceful shutdown: waiting for "
              <> T.pack (show (busy :: Int))
              <> " busy worker(s), "
              <> T.pack (show (qLen :: Int))
              <> " job(s) in queue..."
          drainLoop

-- | Ticks at @pollInterval@, gated by a proof-of-work signal unless paused.
-- Each tick bumps @arbiter_workers.last_heartbeat@, reconciles the registry
-- @paused@ flag into local state, and re-registers if the sweeper deleted
-- the row.
heartbeatLoop
  :: (MonadArbiter m)
  => WorkerConfig n payload
  -> SchemaName
  -> Text
  -- ^ Queue name (used when the row needs re-registering).
  -> m ()
heartbeatLoop config schemaName queueName = do
  gate <- newFailureGate
  tick gate
  forever $ throttledWait *> tick gate
  where
    logCfg = logConfig config
    sig = heartbeatSignal config
    readShuttingDown = (== ShuttingDown) <$> STM.readTVar (workerStateVar config)
    cadenceMicros = Ops.micros (workerHeartbeatInterval config)
    throttledWait = do
      delayVar <- STM.registerDelay cadenceMicros
      atomically $ do
        STM.readTVar delayVar >>= checkSTM
        paused <- STM.readTVar (pauseVar config)
        unless paused $ STM.takeTMVar sig
    tick gate = do
      traverse_
        (\path -> tryWarn logCfg "Liveness probe write failed" (liftIO $ writeFile path ""))
        (livenessFile config)
      epoch <- STM.atomically $ STM.readTVar (pauseEpoch config)
      result <-
        tryReported logCfg Warning gate "Worker registry heartbeat" $
          Ops.heartbeatWorker schemaName (workerId config)
      traverse_ (maybe (reregister epoch) (reconcile epoch)) result
    reregister epoch = do
      shutting <- STM.atomically readShuttingDown
      unless shutting $ do
        tryLog logCfg Warning "Worker registry row missing, re-registering"
        tryWarnWith logCfg "Worker re-registration failed" Nothing (registerSelf config schemaName queueName)
          >>= traverse_ (reconcile epoch)
    reconcile epoch registryPaused =
      STM.atomically $ do
        shutting <- readShuttingDown
        unless shutting $ writePauseIfCurrent config epoch registryPaused

-- | @[]@ when the list is empty, else a singleton holding @act@'s result.
unlessNull :: (Applicative f) => [a] -> f b -> f [b]
unlessNull items act = if null items then pure [] else (: []) <$> act

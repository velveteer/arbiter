{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Entry point for running a worker pool that fetches and executes jobs.
module Arbiter.Worker
  ( -- * Running Workers
    runWorkerPool

    -- * Multi-Queue Workers
  , NamedWorkerPool (..)
  , namedWorkerPool
  , shutdownPools
  , runWorkerPools
  , runSelectedWorkerPools
  , getEnabledQueues

    -- * Pool Sizing
  , poolConfigForWorkers

    -- * Job Result
  , module Arbiter.Core.JobResult

    -- * Rollup Child Results
  , childResults
  , mergedChildResults
  , mergeChildResults

    -- * Re-exports
  , module Arbiter.Worker.Config
  , module Arbiter.Worker.BackoffStrategy
  , module Arbiter.Worker.Logger
  , module Arbiter.Worker.WorkerState

    -- * Reaper
  , runReaperOp

    -- * Cron
  , CronJob (..)
  , OverlapPolicy (..)
  , BackfillPolicy (..)
  , cronJob
  , initCronSchedules
  , overlapPolicyToText
  , overlapPolicyFromText
  , validateCronScheduleUpdate
  , updateCronScheduleChecked
  ) where

import Arbiter.Core.Concurrency.Spec (registryConcurrencyPolicies)
import Arbiter.Core.Exceptions
  ( BranchCancelException (..)
  , JobException (..)
  , JobForceCancelled (..)
  , JobGoneException (..)
  , JobNackException (..)
  , JobPermanentException (..)
  , JobRetryableException (..)
  , ParsingException (..)
  , TreeCancelException (..)
  , namedJobIds
  , throwJobGoneIds
  )
import Arbiter.Core.HighLevel (JobOperation, QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobResult
import Arbiter.Core.Listen qualified as Listen
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.PoolConfig (PoolConfig (..), defaultPoolConfig)
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Arbiter.Core.RateLimit.Spec (registryRateLimitPolicies)
import Arbiter.Core.Threads (labelArbiterThread)
import Arbiter.Core.Trace
  ( ConsumeShape (..)
  , ConsumeSpan
  , consumeSpanFor
  , markSpanError
  , recordJobCancelled
  , recordJobFailure
  , resolveTracer
  , withConsumeSpan
  )
import Control.Exception (SomeException, displayException, fromException, toException)
import Control.Exception qualified as E
import Control.Monad (forever, replicateM, unless, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Aeson (FromJSON, ToJSON, Value)
import Data.Bool (bool)
import Data.Either (partitionEithers)
import Data.Foldable (fold, foldMap', toList, traverse_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int32, Int64)
import Data.List (partition, sortOn)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Ord (Down (..))
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Traversable (for)
import Data.UUID (UUID)
import System.Directory (removeFile)
import UnliftIO
  ( MonadUnliftIO
  , atomically
  , bracket
  , catchSyncOrAsync
  , checkSTM
  , finally
  , isEmptyTBQueue
  , lengthTBQueue
  , mask_
  , modifyTVar'
  , newTBQueueIO
  , newTVarIO
  , readTBQueue
  , readTVar
  , tryAny
  , waitAnyCatch
  , writeTVar
  )
import UnliftIO.Async qualified as Async
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.STM (STM, TBQueue, TVar)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.BackoffStrategy
import Arbiter.Worker.ChannelHandlers
  ( RunningJobs
  , handleCancelNotif
  , handleCronRunNotif
  , handlePauseNotif
  , withRegisteredJobs
  )
import Arbiter.Worker.Config
import Arbiter.Worker.Cron
  ( BackfillPolicy (..)
  , CronJob (..)
  , OverlapPolicy (..)
  , cronJob
  , initCronSchedules
  , overlapPolicyFromText
  , overlapPolicyToText
  , runCronScheduler
  , updateCronScheduleChecked
  , validateCronScheduleUpdate
  )
import Arbiter.Worker.Dispatcher
import Arbiter.Worker.EnabledQueues (enabledQueuesForMonad, getEnabledQueues)
import Arbiter.Worker.Heartbeat (withJobsHeartbeat)
import Arbiter.Worker.Logger
import Arbiter.Worker.Logger.Internal
  ( runHook
  , withJobContext
  , withJobContextList
  , withJobContextOne
  )
import Arbiter.Worker.Retry (spawnRetried)
import Arbiter.Worker.WorkerState

-- ---------------------------------------------------------------------------
-- Multi-Queue Workers
-- ---------------------------------------------------------------------------

-- | A worker pool paired with its queue name (derived from the registry).
--
-- @
-- allWorkers =
--   [ namedWorkerPool emailConfig      -- "email_jobs"
--   , namedWorkerPool imageConfig      -- "image_jobs"
--   ]
--
-- main = runWorkerPools allWorkers
-- @
data NamedWorkerPool m
  = forall payload.
  ( Arb.RegistryAdmissionPolicies (RegistryOf m)
  , EncodeJobResult (ResultOf m payload)
  , QueueOperation m payload
  , RegistryTables (RegistryOf m)
  ) =>
  NamedWorkerPool
  { workerPoolName :: Text
  -- ^ Queue name from the type-level registry
  , workerPoolConfig :: WorkerConfig m payload
  -- ^ The worker configuration
  }

-- | Create a named worker pool, deriving the name from the type-level registry.
namedWorkerPool
  :: forall payload m
   . ( Arb.RegistryAdmissionPolicies (RegistryOf m)
     , EncodeJobResult (ResultOf m payload)
     , QueueOperation m payload
     , RegistryTables (RegistryOf m)
     )
  => WorkerConfig m payload
  -> NamedWorkerPool m
namedWorkerPool cfg =
  NamedWorkerPool
    { workerPoolName = Arb.queueTable @payload @m
    , workerPoolConfig = cfg
    }

-- | Run worker pools, each on its own 'workerStateVar'. Filters to queues
-- listed in @ARBITER_ENABLED_QUEUES@ (all if unset). Stop them with
-- 'shutdownPools', or one at a time with
-- 'Arbiter.Worker.Config.shutdownWorker'.
runWorkerPools
  :: forall m
   . (MonadUnliftIO m, RegistryTables (RegistryOf m))
  => [NamedWorkerPool m]
  -> m ()
runWorkerPools pools = do
  enabled <- liftIO $ enabledQueuesForMonad @m
  runSelectedWorkerPools enabled pools

-- | Signal graceful shutdown to every pool in one transaction, so none of them
-- claims another job after any of them has stopped.
shutdownPools :: (MonadIO m) => [NamedWorkerPool m'] -> m ()
shutdownPools pools =
  liftIO . STM.atomically $
    traverse_
      (\var -> STM.writeTVar var ShuttingDown)
      [workerStateVar cfg | NamedWorkerPool _ cfg <- pools]

-- | Run only the worker pools whose names appear in the enabled list. The first
-- pool to exit winds the others down, so the group stops together.
runSelectedWorkerPools
  :: forall m
   . (MonadUnliftIO m)
  => [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPools enabled pools =
  case filter (\(NamedWorkerPool name _) -> name `elem` enabled) pools of
    [] -> pure ()
    selected -> evalContT $ do
      asyncs <- for selected withPoolAsync
      lift $ do
        void $ waitAnyCatch asyncs
        shutdownPools selected
        traverse_ Async.waitCatch asyncs
  where
    withPoolAsync :: NamedWorkerPool m -> ContT () m (Async.Async ())
    withPoolAsync (NamedWorkerPool name cfg) =
      let cfg' = cfg {logConfig = withPoolContext name (logConfig cfg)}
       in ContT $ Async.withAsync (labelArbiterThread "pool" (Just name) >> runWorkerPool cfg')

-- | Inject the pool name into log context. User-supplied pairs come after
-- so they win on key collision.
withPoolContext :: Text -> LogConfig -> LogConfig
withPoolContext poolName lc =
  lc {additionalContext = (("pool" .= poolName) :) <$> additionalContext lc}

-- | A single-stripe pool sized for the pools enabled by @ARBITER_ENABLED_QUEUES@:
-- twice their combined worker count plus one for the shared listener (floor of 3).
--
-- A single stripe is intentional. @Data.Pool.withResource@ pins each thread to one
-- stripe by its capability and does not search other stripes when its own is
-- exhausted, so multiple stripes let one pool starve a stripe while others sit idle.
poolConfigForWorkers
  :: forall m
   . (RegistryTables (RegistryOf m))
  => [NamedWorkerPool m]
  -> IO PoolConfig
poolConfigForWorkers pools = do
  enabled <- enabledQueuesForMonad @m
  let n = sum [workerCount cfg | NamedWorkerPool nm cfg <- pools, nm `elem` enabled]
  pure defaultPoolConfig {poolSize = max 2 (2 * n) + 1}

-- ---------------------------------------------------------------------------
-- Worker Pool
-- ---------------------------------------------------------------------------

-- | Starts a worker pool with a dispatcher and N worker threads.
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
  let workerCap = workerCount config
      queueName = Arb.queueTable @payload @m
      -- Constant for the pool, so a claim never rebuilds the queue-wide attributes.
      consumeSpan = consumeSpanFor queueName (poolSpanShape config)

  schemaName <- getSchema
  workQueue <- newTBQueueIO (fromIntegral workerCap)
  busyWorkerCount <- newTVarIO 0
  workerFinishedVar <- newTVarIO False
  runningJobs <- STM.newTVarIO Map.empty

  registerResult <- tryAny (registerSelf config schemaName queueName)
  case registerResult of
    Left e -> warnEx (logConfig config) "Worker registry insert failed" e
    Right mPaused ->
      traverse_ (atomically . writeTVar (pauseVar config)) mPaused

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
          Listen.withChannels
            listener
            (Listen.HubLog (tryLog (logConfig config) Warning) (tryLog (logConfig config) Error))
            handlers
    void . ContT $ Async.withAsync (publishListenerReady config listenerReady)
    heartbeat <-
      spawnRetried (workerStateVar config) (logConfig config) queueName "Worker heartbeat" $
        heartbeatLoop config schemaName queueName
    dispatcher <-
      spawnRetried (workerStateVar config) (logConfig config) queueName "Dispatcher" $
        runDispatcher config workerCap workQueue busyWorkerCount workerFinishedVar dispatcherNotifVar
    workers <-
      replicateM workerCap $
        spawnRetried (workerStateVar config) (logConfig config) queueName "Worker thread" $
          workerLoop config consumeSpan runningJobs workQueue busyWorkerCount workerFinishedVar
    crons <-
      unlessNull (cronJobs config) $
        spawnRetried (workerStateVar config) (logConfig config) queueName "Cron scheduler" $
          runCronScheduler (workerStateVar config) cronRunVar (logConfig config) schemaName queueName (cronJobs config)
    reaper <-
      spawnRetried (workerStateVar config) (logConfig config) queueName "Reaper" $
        reaperLoop (logConfig config) (onMaintenance config) (reaperInterval config) (reaperTimeout config)

    (_, res) <- waitAnyCatch (dispatcher : reaper : heartbeat : crons <> workers)
    case res of
      Left e ->
        lift $ tryLog (logConfig config) Error $ "Thread pool exception: " <> T.pack (displayException e)
      Right _ -> pure ()

    lift $ shutdownPool config schemaName workQueue busyWorkerCount

-- | Flip 'listenerReadyVar' once the pool's channels are subscribed. Runs
-- alongside the pool and never gates startup.
publishListenerReady :: (MonadUnliftIO m) => WorkerConfig n payload -> STM Bool -> m ()
publishListenerReady config ready =
  atomically $ do
    ready >>= checkSTM
    writeTVar (listenerReadyVar config) True

-- | Remove the liveness file when the pool exits, after the drain.
withLivenessFile :: (MonadUnliftIO m) => WorkerConfig n payload -> ContT r m ()
withLivenessFile config = case livenessFile config of
  Nothing -> pure ()
  Just path -> ContT $ \k ->
    bracket
      (pure ())
      (\_ -> void . tryAny . liftIO . removeFile $ path)
      (\_ -> k ())

-- | Re-insert the worker's registry row from the config + schema/queue.
-- Returns the effective paused state so the caller can seed 'pauseVar'.
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

-- | Mark shutting-down, drain, then deregister. All DB
-- writes are best-effort and logged on failure.
shutdownPool
  :: (MonadArbiter m)
  => WorkerConfig n payload
  -> SchemaName
  -> TBQueue a
  -> TVar Int
  -> m ()
shutdownPool config schemaName workQueue busyCount = do
  shutdownWorker config
  let wid = workerId config
      logCfg = logConfig config
  tryWarn logCfg "Failed to mark worker shutting down" (Ops.markWorkerShuttingDown schemaName wid)
  drainPool logCfg (gracefulShutdownTimeout config) workQueue busyCount
  tryWarn logCfg "Failed to deregister worker" (Ops.deregisterWorker schemaName wid)

-- | Wait for the work queue to drain and all worker threads to go idle,
-- optionally bounded by a timeout. Logs the entry, periodic progress (every
-- 10s) when no timeout is set, and the result.
drainPool
  :: (MonadUnliftIO m)
  => LogConfig
  -> Maybe NominalDiffTime
  -> TBQueue a
  -> TVar Int
  -> m ()
drainPool logCfg mTimeout workQueue busyCount = do
  tryLog logCfg Info "Starting graceful shutdown. Draining in-flight jobs..."
  result <- case mTimeout of
    Nothing -> Right () <$ drainLoop
    Just timeoutSecs ->
      Async.race (threadDelay $ ceiling (timeoutSecs * 1_000_000)) waitForDrain
  case result of
    Right () -> tryLog logCfg Info "All workers are now idle. Graceful shutdown complete."
    Left () -> tryLog logCfg Warning "Graceful shutdown timed out. Some jobs may still be in-flight."
  where
    waitForDrain = atomically $ do
      qEmpty <- isEmptyTBQueue workQueue
      checkSTM qEmpty
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
                <*> (fromIntegral <$> lengthTBQueue workQueue)
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
  tick
  forever $ throttledWait *> tick
  where
    logCfg = logConfig config
    sig = heartbeatSignal config
    readShuttingDown = (== ShuttingDown) <$> STM.readTVar (workerStateVar config)
    cadenceMicros = ceiling (workerHeartbeatInterval config * 1_000_000)
    throttledWait = do
      delayVar <- STM.registerDelay cadenceMicros
      atomically $ do
        STM.readTVar delayVar >>= checkSTM
        paused <- STM.readTVar (pauseVar config)
        unless paused $ STM.takeTMVar sig
    tick = do
      traverse_
        (\path -> tryWarn logCfg "Liveness probe write failed" (liftIO $ writeFile path ""))
        (livenessFile config)
      result <- tryAny $ Ops.heartbeatWorker schemaName (workerId config)
      case result of
        Left e -> warnEx logCfg "Worker registry heartbeat failed" e
        Right Nothing -> reregister
        Right (Just rp) -> reconcile rp
    reregister = do
      shutting <- STM.atomically readShuttingDown
      unless shutting $ do
        tryLog logCfg Warning "Worker registry row missing, re-registering"
        tryWarn logCfg "Worker re-registration failed" (registerSelf config schemaName queueName)
    reconcile rp =
      STM.atomically $ do
        shutting <- readShuttingDown
        unless shutting $ STM.writeTVar (pauseVar config) rp

-- | @[]@ when the list is empty, else a singleton holding @act@'s result.
unlessNull :: (Applicative f) => [a] -> f b -> f [b]
unlessNull xs act = if null xs then pure [] else (: []) <$> act

-- | Run @act@. On exception, log a warning under @label@ and continue.
tryWarn :: (MonadUnliftIO m) => LogConfig -> Text -> m a -> m ()
tryWarn logCfg label act = tryWarnWith logCfg label () (void act)

-- | 'tryWarn' keeping @act@'s result, falling back to @fallback@ when it threw.
tryWarnWith :: (MonadUnliftIO m) => LogConfig -> Text -> a -> m a -> m a
tryWarnWith logCfg label fallback act =
  tryAny act >>= either (\e -> fallback <$ warnEx logCfg label e) pure

-- | The pool's log context narrowed to one job.
jobLog :: WorkerConfig m payload -> Job.JobRead payload -> LogConfig
jobLog config = withJobContextOne (logConfig config)

-- | Main loop for a single worker thread.
workerLoop
  :: forall payload m
   . ( EncodeJobResult (ResultOf m payload)
     , JobOperation m payload
     )
  => WorkerConfig m payload
  -> ConsumeSpan
  -- ^ The pool's consumer-span shape, built once for its queue.
  -> RunningJobs
  -- ^ Pool-shared map from job id to running handler async.
  -> TBQueue (NonEmpty (Job.JobRead payload))
  -> TVar Int
  -- ^ Busy worker count
  -> TVar Bool
  -- ^ Worker finished signal
  -> m ()
workerLoop config consumeSpan runningJobs workQueue busyCount workerFinishedVar = forever $ mask_ $ do
  -- Mask covers the window between the atomic claim (which increments
  -- busyCount) and entering the finally block that decrements it.
  jobBatch <- atomically $ do
    batch <- readTBQueue workQueue
    modifyTVar' busyCount (+ 1)
    pure batch

  let jobIds = map Job.primaryKey (toList jobBatch)
      batchLog = withJobContext (logConfig config) jobBatch
      claimHook now job =
        runHook (jobLog config job) "onJobClaimed" $
          Job.onJobClaimed (observabilityHooks config) job now

  flip
    finally
    ( atomically $ do
        modifyTVar' busyCount (subtract 1)
        writeTVar workerFinishedVar True
    )
    $ do
      currentTime <- liftIO getCurrentTime
      traverse_ (claimHook currentTime) jobBatch
      handoff <- newCancelHandoff
      result <-
        withRegisteredJobs runningJobs jobIds $
          processJobsWithRetry config consumeSpan handoff jobBatch
      case result of
        Right () -> pure ()
        Left e
          -- Finalized inside the job span, so the trace carries the cancel. One
          -- delivered before that catch, or interrupting it, arrives here undone.
          | Just (JobForceCancelled cancelledIds reclaimedIds) <- fromException e -> do
              finalized <- cancelFinalized handoff
              unless finalized $
                finalizeForceCancelled config jobBatch cancelledIds reclaimedIds handoff
          | Just Async.AsyncCancelled <- fromException e -> liftIO (E.throwIO e)
          | otherwise -> do
              tryLog batchLog Error $ "Worker exception: " <> T.pack (displayException e)
              threadDelay 2_000_000

-- | Read and decode child results for a rollup finalizer.
-- Decode failures appear as @Left decodeError@ - the child succeeded but
-- its result JSON doesn't match the expected type.
readChildResults
  :: (FromJSON a, MonadArbiter m)
  => Text
  -> Job.JobRead payload
  -> m (Map.Map Int64 (Either Text a), Map.Map Int64 T.Text)
readChildResults schemaName job = do
  (results, failures, mSnapshot, dlqFailures) <-
    Ops.readChildResultsRaw schemaName (Job.queueName job) (Job.primaryKey job)
  let raw = Ops.mergeRawChildResults results failures mSnapshot
      merged = Map.map (>>= decodeJobResult) raw
  pure (merged, dlqFailures)

processJobsWithRetry
  :: forall payload m
   . ( EncodeJobResult (ResultOf m payload)
     , JobOperation m payload
     )
  => WorkerConfig m payload
  -> ConsumeSpan
  -- ^ The pool's consumer-span shape, built once for its queue.
  -> CancelHandoff
  -> NonEmpty (Job.JobRead payload)
  -> m ()
processJobsWithRetry config consumeSpan handoff jobs = do
  startTime <- liftIO getCurrentTime
  schemaName <- Arb.getSchema
  tracer <- resolveTracer
  let (firstJob :| _) = jobs
      -- Rethrown with base throwIO, UnliftIO's wrapping it as synchronous. The flag
      -- is set last, so an interrupted finalizer leaves the rest to 'workerLoop'.
      onForceCancel exc@(JobForceCancelled cancelledIds goneIds) = do
        finalizeForceCancelled config jobs cancelledIds goneIds handoff
        markCancelFinalized handoff
        liftIO $ E.throwIO exc
  -- The span covers the outcome report and the force-cancel finalizer too, so every
  -- terminal hook fires while it is open.
  withConsumeSpan tracer consumeSpan jobs $ flip catchSyncOrAsync onForceCancel $ do
    result <-
      tryAny
        $ withJobsHeartbeat
          (observabilityHooks config)
          (jobHeartbeatInterval config)
          (visibilityTimeout config)
          startTime
          jobs
          (pendingJobs handoff jobs)
          (logConfig config)
          (heartbeatSignal config)
        $ case handlerMode config of
          SingleJobMode handler -> do
            withDbTransaction $ do
              handlerResult <- runHandlerWithConnection handler firstJob
              ackOrGone firstJob
              storeJobResult schemaName firstJob handlerResult
            finalizeJob config handoff startTime firstJob
          BatchedJobsMode _ handler -> handler jobs (batchCallbacks config handoff jobs startTime schemaName)
    endTime <- liftIO getCurrentTime
    reportBatchOutcome config startTime endTime jobs handoff result

-- | Ack a job, throwing if another worker reclaimed it mid-flight.
ackOrGone :: (JobOperation m payload) => Job.JobRead payload -> m ()
ackOrGone job = do
  rowsAffected <- Arb.ackJob job
  when (rowsAffected == 0) $
    throwJobGoneIds "reclaimed by another worker during processing" [Job.primaryKey job]

-- | Record a completed job, then report it.
finalizeJob
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> UTCTime
  -> Job.JobRead payload
  -> m ()
finalizeJob config handoff startTime j = markJobHandled handoff j >> reportSuccess config startTime j

-- | Report a completed job, once the handoff already records it.
reportSuccess
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> UTCTime
  -> Job.JobRead payload
  -> m ()
reportSuccess config startTime j = do
  endT <- liftIO getCurrentTime
  runHook (jobLog config j) "onJobSuccess" $ Job.onJobSuccess (observabilityHooks config) j startTime endT

-- | The settle operations a batch handler drives its jobs through.
batchCallbacks
  :: forall payload m
   . ( EncodeJobResult (ResultOf m payload)
     , JobOperation m payload
     )
  => WorkerConfig m payload
  -> CancelHandoff
  -> NonEmpty (Job.JobRead payload)
  -> UTCTime
  -> Text
  -> BatchCallbacks m payload (ResultOf m payload)
batchCallbacks config handoff jobs startTime schemaName =
  BatchCallbacks
    { ack = \job -> ackOneStoring job Nothing
    , ackWith = \job r -> ackOneStoring job (encodeJobResult r)
    , ackAll = \js -> ackBatchStoring (map (\j -> (j, Nothing)) js)
    , ackAllWith = \prs -> ackBatchStoring (map (\(j, r) -> (j, encodeJobResult r)) prs)
    , failRetry = failAs (Retryable . JobRetryableException)
    , failPermanent = failAs (Permanent . JobPermanentException)
    , cancelBranch = failAs (BranchCancel . BranchCancelException)
    , cancelTree = failAs (TreeCancel . TreeCancelException)
    , nack = nackOne
    }
  where
    markHandled = markJobHandled handoff
    nackOne j = releaseJobs config handoff [j]
    failAs mkExc j msg = failWith j (toException (mkExc msg))
    failWith j exc = do
      endT <- liftIO getCurrentTime
      outcome <- mask_ $ do
        result <-
          withDbTransaction $
            handleJobFailure config Ops.TakeLocks (batchSpanShape jobs) (classifyException exc) (jobMaxAtts j) startTime endT j
        result <$ markHandled j
      reportWritten outcome
      settleUnwritten config handoff [(j, outcome)]
    ackOneStoring j mVal = do
      mask_ $ do
        withDbTransaction $ do
          ackOrGone j
          storeEncodedResult schemaName j mVal
        markHandled j
      reportSuccess config startTime j
    ackBatchStoring pairs = do
      let js = map fst pairs
      (done, reclaimed) <- mask_ $ do
        acked <- withDbTransaction $ do
          ackedSet <- Set.fromList <$> Arb.ackJobsBatch js
          storeEncodedResults schemaName (filter (hasIdIn ackedSet . fst) pairs)
          pure ackedSet
        let (settled, gone) = partition (hasIdIn acked) js
        (settled, gone) <$ (traverse_ markHandled settled >> traverse_ markUnowned gone)
      -- Before the settle, so a tick landing in its transaction cannot lose these.
      traverse_ (reportSuccess config startTime) done
      let reclaimedLog = withJobContextList (logConfig config) reclaimed
      unless (null reclaimed) $ do
        tryLog reclaimedLog Info "Jobs no longer claimed by this worker during bulk completion, skipped"
        void $ settleGoneJobs config handoff (const (pure ())) "no longer claimed by this worker" reclaimed
    markUnowned j = markJobUnowned handoff j >> markHandled j

-- | Delete the jobs a force-cancel flagged, report them, and hand back the attempt
-- the claim consumed for the batch siblings it interrupted.
finalizeForceCancelled
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> NonEmpty (Job.JobRead payload)
  -> [Int64]
  -- ^ The jobs the cancel named.
  -> [Int64]
  -- ^ Jobs the same signal found gone, which report rather than take a nack.
  -> CancelHandoff
  -> m ()
finalizeForceCancelled config jobs cancelledIds goneIds handoff = do
  tryLog batchLog Info "Job(s) force-cancelled"
  schemaName <- getSchema
  pending <- pendingJobs handoff jobs
  -- A nack keeps the claim, so the cancel can name a job the handler already finalized.
  let settling = byIdDesc (hasIdIn (Set.fromList (cancelledIds <> map Job.primaryKey pending))) jobs
  deleted <-
    deleteCancelledOrWarn batchLog (workerId config) schemaName (Job.queueName firstJob) (map Job.primaryKey settling)
  (fresh, cancelled) <- recordCancelled handoff (deleted <> Set.fromList cancelledIds)
  let (gone, interrupted) = partition (hasIdIn cancelled) settling
      (unreported, alreadyReported) = partition (hasIdIn fresh) gone
      (unavailable, siblings) = partition (hasIdIn (Set.fromList goneIds)) interrupted
      unavailableLog = withJobContextList (logConfig config) unavailable
  reportGoneJobs config (markJobHandled handoff) cancelled "force-cancelled" unreported
  traverse_ (markJobHandled handoff) alreadyReported
  unless (null unavailable) $ do
    tryLog unavailableLog Info "Job(s) no longer claimed by this worker, skipping retry"
    reportGoneJobs config (markJobHandled handoff) mempty "no longer claimed by this worker" unavailable
  releaseOrWarn config handoff "Releasing a force-cancel batch sibling failed" siblings
  where
    (firstJob :| _) = jobs
    batchLog = withJobContext (logConfig config) jobs

-- | Hand back the attempt the claim consumed for the jobs left unfinalized, in one
-- statement, and report whichever of them the nack found under another claim.
releaseJobs
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> [Job.JobRead payload]
  -> m ()
releaseJobs _ _ [] = pure ()
releaseJobs config handoff js = do
  released <- mask_ $ do
    nacked <- Set.fromList <$> Arb.nackJobsBatch js
    nacked <$ traverse_ (markJobHandled handoff) js
  settleUnwritten config handoff [(j, Left "no longer claimed by this worker") | j <- js, not (hasIdIn released j)]

-- | 'releaseJobs' on an unwinding path, which has nothing left to throw to.
releaseOrWarn
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> Text
  -> [Job.JobRead payload]
  -> m ()
releaseOrWarn config handoff warning js =
  tryWarn (withJobContextList (logConfig config) js) warning (releaseJobs config handoff js)

-- | Settle the jobs a failure could not be written for, each under its own reason.
settleUnwritten
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> [(Job.JobRead payload, FailureOutcome m)]
  -> m ()
settleUnwritten config handoff pairs =
  traverse_ settle (Map.toList (Map.fromListWith (<>) [(reason, [j]) | (j, Left reason) <- pairs]))
  where
    ctx = withJobContextList (logConfig config)
    settle (reason, js) = do
      cancelled <- settleGoneJobs config handoff (const (pure ())) reason js
      let (forced, unavailable) = partition (hasIdIn cancelled) js
      unless (null forced) $ tryLog (ctx forced) Info "Job(s) force-cancelled"
      unless (null unavailable) $ tryLog (ctx unavailable) Warning ("Job(s) " <> reason)

-- | Delete whichever of @jobs@ a force-cancel flagged, report them all, return those ids.
-- The delete is recorded, so a cancel signal arriving later does not report them again.
settleGoneJobs
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> (Job.JobRead payload -> m ())
  -> Text
  -> [Job.JobRead payload]
  -> m (Set.Set Int64)
settleGoneJobs config handoff mark reason = \case
  [] -> pure mempty
  jobs@(j : _) -> do
    schemaName <- getSchema
    let logCfg = withJobContextList (logConfig config) jobs
    cancelled <- deleteCancelledOrWarn logCfg (workerId config) schemaName (Job.queueName j) (map Job.primaryKey jobs)
    void $ recordCancelled handoff cancelled
    cancelled <$ reportGoneJobs config mark cancelled reason jobs

-- | Delete whichever of @jobIds@ a force-cancel flagged against this worker's lease,
-- or against none, returning the ids it deleted. One held elsewhere is that worker's.
deleteCancelledOrWarn
  :: (MonadArbiter m)
  => LogConfig
  -> UUID
  -> SchemaName
  -> Text
  -- ^ Queue the jobs belong to.
  -> [Int64]
  -> m (Set.Set Int64)
deleteCancelledOrWarn logCfg owner schemaName queue jobIds =
  tryWarnWith logCfg "Deleting force-cancelled jobs failed" mempty $
    Set.fromList <$> Ops.deleteCancelledJobs schemaName queue (Just owner) jobIds

-- | What a batch has settled so far.
data BatchProgress = BatchProgress
  { progressHandled :: !(Set.Set Int64)
  -- ^ Jobs whose outcome has been recorded.
  , progressUnowned :: !(Set.Set Int64)
  -- ^ Jobs a batch ack found under another claim.
  , progressCancelled :: !(Set.Set Int64)
  -- ^ Jobs a force-cancel accounted for.
  , progressFinalized :: !Bool
  -- ^ Whether the force-cancel finalizer ran to completion.
  }

-- | What a force-cancel finalizer needs from the handler's scope, whichever side of
-- the job span runs it.
newtype CancelHandoff = CancelHandoff (IORef BatchProgress)

newCancelHandoff :: (MonadIO m) => m CancelHandoff
newCancelHandoff = liftIO (CancelHandoff <$> newIORef (BatchProgress mempty mempty mempty False))

readProgress :: (MonadIO m) => CancelHandoff -> m BatchProgress
readProgress (CancelHandoff ref) = liftIO (readIORef ref)

onProgress :: (MonadIO m) => CancelHandoff -> (BatchProgress -> (BatchProgress, a)) -> m a
onProgress (CancelHandoff ref) = liftIO . atomicModifyIORef' ref

-- | The batch's jobs a predicate selects, children-first, so the row locks taken for
-- them follow the same order as ack and force-cancel. The heartbeat needs this too:
-- its batch update joins the rows in the order they are handed to it.
byIdDesc :: (Job.JobRead payload -> Bool) -> NonEmpty (Job.JobRead payload) -> [Job.JobRead payload]
byIdDesc p = sortOn (Down . Job.primaryKey) . filter p . toList

-- | The batch's jobs still awaiting an outcome, which keeps the force-cancel finalizer
-- and the outcome report from both reporting the same job.
pendingJobs :: (MonadIO m) => CancelHandoff -> NonEmpty (Job.JobRead payload) -> m [Job.JobRead payload]
pendingJobs handoff jobs = do
  progress <- readProgress handoff
  pure (byIdDesc (not . hasIdIn (progressHandled progress)) jobs)

markJobHandled :: (MonadIO m) => CancelHandoff -> Job.JobRead payload -> m ()
markJobHandled handoff job =
  onProgress handoff $ \p -> (p {progressHandled = Set.insert (Job.primaryKey job) (progressHandled p)}, ())

markJobUnowned :: (MonadIO m) => CancelHandoff -> Job.JobRead payload -> m ()
markJobUnowned handoff job =
  onProgress handoff $ \p -> (p {progressUnowned = Set.insert (Job.primaryKey job) (progressUnowned p)}, ())

-- | Whether a set of job ids names this job.
hasIdIn :: Set.Set Int64 -> Job.JobRead payload -> Bool
hasIdIn ids job = Set.member (Job.primaryKey job) ids

-- | Add to the jobs a force-cancel accounted for, returning the ids new to this call
-- and every id recorded so far.
recordCancelled :: (MonadIO m) => CancelHandoff -> Set.Set Int64 -> m (Set.Set Int64, Set.Set Int64)
recordCancelled handoff ids =
  onProgress handoff $ \p ->
    let s = progressCancelled p <> ids in (p {progressCancelled = s}, (ids Set.\\ progressCancelled p, s))

-- | Whether a force-cancel was finalized in full, which only the finalizer records.
cancelFinalized :: (MonadIO m) => CancelHandoff -> m Bool
cancelFinalized = fmap progressFinalized . readProgress

markCancelFinalized :: (MonadIO m) => CancelHandoff -> m ()
markCancelFinalized handoff = onProgress handoff $ \p -> (p {progressFinalized = True}, ())

-- | Interpret a finished batch: warn when the handler left jobs unfinalized,
-- skip retry for gone or nacked jobs, otherwise fail whatever was not finalized.
reportBatchOutcome
  :: forall payload m
   . (JobOperation m payload)
  => WorkerConfig m payload
  -> UTCTime
  -> UTCTime
  -> NonEmpty (Job.JobRead payload)
  -> CancelHandoff
  -> Either SomeException ()
  -> m ()
reportBatchOutcome config startTime endTime jobs handoff outcome = do
  unhandled <- pendingJobs handoff jobs
  let splitNamed ids = partition (hasIdIn (Set.fromList ids)) unhandled
      reportUnavailable ids reason = do
        -- An exception naming no job speaks for none of them, so the remainder keeps
        -- its attempt and reports when it runs.
        let (jobsGone, siblings) = splitNamed ids
        void $ settleGoneJobs config handoff (markJobHandled handoff) reason jobsGone
        unless (null ids) $
          releaseOrWarn config handoff "Releasing an interrupted batch sibling failed" siblings
      unownedOf = do
        unowned <- progressUnowned <$> readProgress handoff
        pure (byIdDesc (hasIdIn unowned) jobs)
  case outcome of
    Right () ->
      unless (null unhandled) $
        tryLog (withJobContextList (logConfig config) unhandled) Warning "Handler left jobs unfinalized, will reprocess"
    Left exc
      | Just (JobGoneException reason gone) <- fromException exc -> do
          tryLog batchLog Info $ "Job(s) " <> reason <> ", skipping retry" <> namedJobIds gone
          reportUnavailable gone reason
      | Just JobNackException <- fromException exc -> do
          -- Hand back the attempt the claim consumed for every job the handler
          -- left unfinalized, so the nacked reprocess does not record a failure.
          releaseJobs config handoff unhandled
          tryLog batchLog Info "Job(s) nacked, will be reprocessed"
      | otherwise -> do
          -- Fail the jobs the handler did not finalize, in a separate transaction.
          let failure@(_, kind) = classifyException exc
              queue = Job.queueName firstJob
          schemaName <- getSchema
          -- A tree or branch cancel acts on the tree, not on this worker's claim.
          unowned <- if cancelsTree kind then unownedOf else pure []
          -- Every row the settles below touch, in one pass. A cancel reaches the whole
          -- tree, so its pass has to as well.
          let lockTrees
                | cancelsTree kind = Ops.lockJobTreesFromRoot
                | otherwise = Ops.lockJobTrees
          outcomes <- mask_ $ do
            outcomes <- withDbTransaction $ do
              Ops.lockJobParents schemaName queue (map Job.parentId unhandled)
              lockTrees schemaName queue (map Job.primaryKey (unhandled <> unowned))
              outcomes <- traverse (\j -> (j,) <$> failJob failure j) unhandled
              traverse_ (void . cancelJobFor kind) unowned
              pure outcomes
            outcomes <$ traverse_ (markJobHandled handoff) unhandled
          traverse_ report outcomes
          settleUnwritten config handoff outcomes
  where
    (firstJob :| _) = jobs
    batchLog = withJobContext (logConfig config) jobs
    report (job, written) =
      tryWarn (jobLog config job) "Reporting a job's failure failed" (reportWritten written)
    failJob failure job =
      handleJobFailure config Ops.LocksHeld (batchSpanShape jobs) failure (jobMaxAtts job) startTime endTime job

-- | A rollup parent's immediate child results, keyed by child id, with @Left@
-- for results that failed to decode, plus a map of DLQ'd immediate children.
-- Both are empty for a job with no children.
childResults
  :: (FromJSON (ResultOf m payload), MonadArbiter m)
  => Job.JobRead payload
  -> m (Map.Map Int64 (Either Text (ResultOf m payload)), Map.Map Int64 T.Text)
childResults job = do
  schemaName <- getSchema
  readChildResults schemaName job

-- | 'childResults' with the child results 'Monoid'-merged (decode failures
-- contribute 'mempty').
mergedChildResults
  :: ( FromJSON (ResultOf m payload)
     , MonadArbiter m
     , Monoid (ResultOf m payload)
     )
  => Job.JobRead payload
  -> m (ResultOf m payload, Map.Map Int64 T.Text)
mergedChildResults job = do
  (results, dlqFailures) <- childResults job
  pure (mergeChildResults results, dlqFailures)

-- | Fold child results via 'Monoid', treating failures as 'mempty'.
mergeChildResults :: (Monoid a) => Map Int64 (Either Text a) -> a
mergeChildResults = foldMap' fold

-- | Store a job's result for its parent rollup, if it has one.
storeJobResult
  :: (EncodeJobResult result, MonadArbiter m)
  => Text
  -> Job.JobRead payload
  -> result
  -> m ()
storeJobResult schemaName job = storeEncodedResult schemaName job . encodeJobResult

-- | 'storeJobResult' on an already-encoded result. 'Nothing' stores nothing.
storeEncodedResult
  :: (MonadArbiter m)
  => Text
  -> Job.JobRead payload
  -> Maybe Value
  -> m ()
storeEncodedResult schemaName job mVal =
  case (Job.parentId job, mVal) of
    (Just pid, Just val) ->
      void $ Ops.insertResult schemaName (Job.queueName job) pid (Job.primaryKey job) val
    (Nothing, Just val)
      | Ops.archivesOnAck job ->
          void $ Ops.updateArchiveResult schemaName (Job.queueName job) (Job.primaryKey job) val
    _ -> pure ()

-- | 'storeEncodedResult' over a batch from one queue: one statement for the
-- child results, one for the archived roots.
storeEncodedResults
  :: (MonadArbiter m)
  => Text
  -> [(Job.JobRead payload, Maybe Value)]
  -> m ()
storeEncodedResults _ [] = pure ()
storeEncodedResults schemaName pairs@((firstJob, _) : _) = do
  let (childRows, rootRows) = partitionEithers (mapMaybe resultRow pairs)
      queue = Job.queueName firstJob
  void $ Ops.insertResultsBatch schemaName queue childRows
  void $ Ops.updateArchiveResultsBatch schemaName queue rootRows
  where
    resultRow (job, mVal) = do
      val <- mVal
      case Job.parentId job of
        Just pid -> Just (Left (pid, Job.primaryKey job, val))
        Nothing
          | Ops.archivesOnAck job -> Just (Right (Job.primaryKey job, val))
          | otherwise -> Nothing

data FailureKind = RetryFailure | PermanentFailure | TreeCancelFailure | BranchCancelFailure
  deriving stock (Eq)

-- | The job's own attempt budget, or the default.
jobMaxAtts :: Job.JobRead payload -> Int32
jobMaxAtts job = fromMaybe Job.defaultMaxAttempts (Job.maxAttempts job)

-- | Classify a handler exception into an error message and failure disposition.
-- 'reportBatchOutcome' intercepts 'JobGoneException' first, so it never arrives here.
classifyException :: SomeException -> (T.Text, FailureKind)
classifyException e
  | Just (Retryable (JobRetryableException msg)) <- fromException e = (msg, RetryFailure)
  | Just (Permanent (JobPermanentException msg)) <- fromException e = (msg, PermanentFailure)
  | Just (TreeCancel (TreeCancelException msg)) <- fromException e = (msg, TreeCancelFailure)
  | Just (BranchCancel (BranchCancelException msg)) <- fromException e = (msg, BranchCancelFailure)
  | Just (ParsingException msg) <- fromException e = (msg, PermanentFailure)
  | otherwise = (T.pack $ show e, RetryFailure) -- Unknown exception, treat as retryable

-- | Whether a failure deletes a job tree rather than acting on the job's own claim.
cancelsTree :: FailureKind -> Bool
cancelsTree kind = kind `elem` [TreeCancelFailure, BranchCancelFailure]

-- | Report jobs this worker can no longer act on, as cancelled where a force-cancel
-- deleted them, marking each with @mark@.
reportGoneJobs
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> (Job.JobRead payload -> m ())
  -> Set.Set Int64
  -- ^ Ids a force-cancel accounted for.
  -> Text
  -> [Job.JobRead payload]
  -> m ()
reportGoneJobs config mark cancelled reason = traverse_ (\j -> mark j >> report j)
  where
    hooks = observabilityHooks config
    report job
      | hasIdIn cancelled job = fireCancelled logCfg hooks job "force-cancelled"
      | otherwise = fireUnavailable logCfg hooks job reason
      where
        logCfg = jobLog config job

-- | Report a claimed job the worker can no longer act on.
fireUnavailable
  :: (Job.JobPayload payload, MonadUnliftIO m)
  => LogConfig
  -> Job.ObservabilityHooks m payload
  -> Job.JobRead payload
  -> Text
  -> m ()
fireUnavailable logCfg hooks job reason =
  runHook logCfg "onJobUnavailable" $ Job.onJobUnavailable hooks job reason

-- | What this pool's consumer spans cover.
poolSpanShape :: WorkerConfig m payload -> ConsumeShape
poolSpanShape = bool PerJob PerBatch . (> 1) . handlerBatchSize

-- | What the consumer span over this batch covers, which a pool claiming fewer jobs
-- than it asked for narrows to the one job in hand.
batchSpanShape :: NonEmpty (Job.JobRead payload) -> ConsumeShape
batchSpanShape = bool PerJob PerBatch . (> 1) . length

-- | Report a failed job to its hooks and to the consumer span it ran under. A span
-- covering more than this job also covers the ones that succeeded, so only a
-- single-job span takes the error status.
fireFailure
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> ConsumeShape
  -- ^ What the span this job ran under covers.
  -> Job.JobRead payload
  -> Text
  -> UTCTime
  -> UTCTime
  -> m ()
fireFailure config shape job errorMsg startTime endTime = do
  recordJobFailure job errorMsg
  when (shape == PerJob) (markSpanError errorMsg)
  runHook (jobLog config job) "onJobFailure" $
    Job.onJobFailure (observabilityHooks config) job errorMsg startTime endTime

-- | Report a cancelled job.
fireCancelled
  :: (Job.JobPayload payload, MonadUnliftIO m)
  => LogConfig
  -> Job.ObservabilityHooks m payload
  -> Job.JobRead payload
  -> Text
  -> m ()
fireCancelled logCfg hooks job errorMsg = do
  recordJobCancelled job errorMsg
  runHook logCfg "onJobCancelled" $ Job.onJobCancelled hooks job errorMsg

-- | Delete what a tree or branch cancel names, returning the rows deleted.
cancelJobFor
  :: (JobOperation m payload)
  => FailureKind
  -> Job.JobRead payload
  -> m Int64
cancelJobFor kind job = do
  schemaName <- getSchema
  case kind of
    BranchCancelFailure ->
      Ops.cancelJobCascade schemaName (Job.queueName job) (fromMaybe (Job.primaryKey job) (Job.parentId job))
    _ -> Ops.cancelJobTree schemaName (Job.queueName job) (Job.primaryKey job)

-- | 'Left' why a failure write found no row, 'Right' how to report the one it wrote.
type FailureOutcome m = Either Text (m ())

-- | Report a failure the write landed.
reportWritten :: (Monad m) => FailureOutcome m -> m ()
reportWritten = either (const (pure ())) id

-- | Handle failure for a single job (retry or move to DLQ), for the caller to report
-- once it commits.
handleJobFailure
  :: forall payload m
   . (JobOperation m payload)
  => WorkerConfig m payload
  -> Ops.TreeLocks
  -- ^ Whether the caller already holds the parent and tree locks.
  -> ConsumeShape
  -- ^ What the span this job ran under covers.
  -> (Text, FailureKind)
  -- ^ The handler exception, classified once for the whole batch.
  -> Int32
  -> UTCTime
  -> UTCTime
  -> Job.JobRead payload
  -> m (FailureOutcome m)
handleJobFailure config locks shape (errorMsg, failureKind) maxAtts startTime endTime job = do
  let cfg = jobLog config job
      hooks = observabilityHooks config
      -- A batch sibling's cancel takes out the whole tree, so no rows still means gone.
      cancel = Right (fireCancelled cfg hooks job errorMsg) <$ cancelJobFor failureKind job
  let
    -- Nothing written means the job went elsewhere, so the failure is not ours to report.
    wrote reason after rowsAffected
      | rowsAffected == 0 = pure (Left reason)
      | otherwise = pure (Right (fireFailure config shape job errorMsg startTime endTime >> after))
    deadLetter = do
      schemaName <- getSchema
      Ops.moveToDLQ locks schemaName (Job.queueName job) errorMsg job
        >>= wrote
          "no longer available for the dead-letter queue"
          (runHook cfg "onJobFailedAndMovedToDLQ" $ Job.onJobFailedAndMovedToDLQ hooks errorMsg job)
    retryLater = do
      let baseDelay = calculateBackoff (backoffStrategy config) (Job.attempts job)
      backoffSecs <- liftIO $ applyJitter (jitter config) baseDelay
      Arb.updateJobForRetry backoffSecs errorMsg job
        >>= wrote
          "no longer available for retry"
          (runHook cfg "onJobRetry" $ Job.onJobRetry hooks job backoffSecs)
    dispatch
      | cancelsTree failureKind = cancel
      | failureKind == PermanentFailure || Job.attempts job >= maxAtts = deadLetter
      | otherwise = retryLater
  dispatch

-- | Refreshes the groups tables, sweeps stale worker registry rows, moves
-- exhausted jobs to the DLQ, and purges expired archived jobs (all schema-wide).
-- Each gated so only one pool runs it per interval.
reaperLoop
  :: forall m
   . ( Arb.RegistryAdmissionPolicies (RegistryOf m)
     , MonadArbiter m
     , RegistryTables (RegistryOf m)
     )
  => LogConfig
  -> (MaintenanceOp -> Int64 -> m ())
  -- ^ Reports the rows each op touched.
  -> NominalDiffTime
  -- ^ How often this loop runs.
  -> NominalDiffTime
  -- ^ Abort any single statement that exceeds this.
  -> m ()
reaperLoop logCfg report interval stmtTimeout = do
  let reaped op n = runHook logCfg "onMaintenance" $ report op n
      intervalSecs = ceiling interval
      queues = registryTableNames (Proxy @(RegistryOf m))
      pruneInterval = interval * 12
      hasConcurrency = not (Set.null (registryConcurrencyPolicies @(RegistryOf m)))
      hasRateLimit = not (Set.null (registryRateLimitPolicies @(RegistryOf m)))
  schemaName <- Arb.getSchema
  let
    -- Reports the rows the op touched, for the ops whose result counts any.
    gatedRows :: forall a. MaintenanceOp -> NominalDiffTime -> (a -> Int64) -> m a -> m (Maybe a)
    gatedRows op every rowsOf work = do
      mr <- runReaperOp logCfg schemaName stmtTimeout (maintenanceOpName op) every work
      mr <$ traverse_ (reaped op . rowsOf) mr
    gatedCount :: MaintenanceOp -> NominalDiffTime -> m Int64 -> m ()
    gatedCount op every = void . gatedRows op every id
    reportFailed :: MaintenanceOp -> [Text] -> m ()
    reportFailed op =
      traverse_ (\queue -> tryLog logCfg Warning $ maintenanceOpName op <> " failed for queue: " <> queue)
    sweep
      :: MaintenanceOp
      -> NominalDiffTime
      -> (Int64 -> m ())
      -> m (Int64, [Text])
      -> m ()
    sweep op every done work =
      gatedRows op every fst work
        >>= traverse_ (\(n, failed) -> reportFailed op failed >> when (n > 0) (done n))
    -- The cursors ride in the gate row, so every pool resumes from one set of them.
    refreshGroups = do
      swept <-
        runReaperStateOp logCfg schemaName stmtTimeout (maintenanceOpName RefreshGroups) interval $
          Ops.refreshAllGroups schemaName queues . fromMaybe mempty
      traverse_ (\(n, failed) -> reaped RefreshGroups n >> reportFailed RefreshGroups failed) swept
  forever $ do
    refreshGroups
    gatedCount SweepStaleWorkers interval $ Ops.sweepStaleWorkers schemaName
    sweep
      SweepExhaustedJobs
      interval
      (\n -> tryLog logCfg Warning $ "Reaper moved " <> T.pack (show n) <> " exhausted job(s) to the DLQ")
      $ Ops.sweepExhaustedJobs schemaName queues
    sweep
      SweepCancelledJobs
      interval
      (\n -> tryLog logCfg Info $ "Reaper deleted " <> T.pack (show n) <> " orphaned cancelled job(s)")
      $ Ops.sweepCancelledJobs schemaName queues
    when hasRateLimit $
      gatedCount PruneRateLimitBuckets pruneInterval $
        Arb.pruneRateLimitBuckets @m interval
    when hasConcurrency $ do
      gatedCount ReconcileConcurrencyStale interval $ Arb.reconcileConcurrencyCountsIfStale @m
      gatedCount ReconcilePruneConcurrency pruneInterval $ Arb.reconcileAndPruneConcurrency @m
    sweep
      PurgeArchives
      interval
      (\n -> tryLog logCfg Info $ "Reaper purged " <> T.pack (show n) <> " archived job(s)")
      $ Ops.purgeArchives schemaName queues
    threadDelay (intervalSecs * 1_000_000)

-- | Run one gated reaper op, logging and swallowing failures so the loop survives.
-- statement_timeout bounds each statement (aborting a stuck one at the DB), while a
-- legitimately long multi-statement op still runs to completion.
runReaperOp
  :: (MonadArbiter m)
  => LogConfig
  -> SchemaName
  -> NominalDiffTime
  -> Text
  -> NominalDiffTime
  -> m a
  -> m (Maybe a)
runReaperOp logCfg schemaName stmtTimeout task every work =
  reaperGate logCfg task $
    Ops.runGatedBounded schemaName task every stmtTimeout work

-- | 'runReaperOp' for an op that resumes from where its last run left off.
runReaperStateOp
  :: (FromJSON s, MonadArbiter m, ToJSON s)
  => LogConfig
  -> SchemaName
  -> NominalDiffTime
  -> Text
  -> NominalDiffTime
  -> (Maybe s -> m (a, s))
  -> m (Maybe a)
runReaperStateOp logCfg schemaName stmtTimeout task every work =
  reaperGate logCfg task $
    Ops.runGatedState schemaName task every $ \state ->
      Ops.setLocalStatementTimeout stmtTimeout >> work state

-- | Swallow a reaper op's failure, so one bad tick does not end the loop.
reaperGate :: (MonadArbiter m) => LogConfig -> Text -> m (Maybe a) -> m (Maybe a)
reaperGate logCfg task = tryWarnWith logCfg ("Reaper op failed: " <> task) Nothing

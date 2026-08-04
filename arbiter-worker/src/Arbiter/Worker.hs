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

    -- * Logging
  , tryLog
  , warnEx

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
  , JobNackException (..)
  , JobNotFoundException (..)
  , JobPermanentException (..)
  , JobRetryableException (..)
  , JobStolenException (..)
  , ParsingException (..)
  , TreeCancelException (..)
  , throwJobNotFoundIds
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
  ( ConsumeSpan
  , consumeSpanFor
  , markSpanError
  , recordJobCancelled
  , recordJobFailure
  , resolveTracer
  , withConsumeSpan
  , withConsumeSpanBatch
  )
import Control.Exception (SomeException, displayException, fromException, toException)
import Control.Exception qualified as E
import Control.Monad (forever, replicateM, unless, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Aeson (FromJSON, Value, toJSON)
import Data.Either (partitionEithers)
import Data.Foldable (fold, foldMap', toList, traverse_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int32, Int64)
import Data.List (partition)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Traversable (for)
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
  , throwIO
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
  , tryLog
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
     , MonadUnliftIO m
     , QueueOperation m payload
     , RegistryTables (RegistryOf m)
     )
  => WorkerConfig m payload
  -> m ()
runWorkerPool config = do
  let workerCap = workerCount config
      queueName = Arb.queueTable @payload @m
      -- Constant for the pool, so a claim never rebuilds the queue-wide attributes.
      consumeSpan = consumeSpanFor queueName []

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
  :: (MonadArbiter m, MonadUnliftIO m)
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
  :: (MonadArbiter m, MonadUnliftIO m)
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

-- | The pool's config with its log context narrowed to one job.
withJobLog :: WorkerConfig m payload -> Job.JobRead payload -> WorkerConfig m payload
withJobLog config job = config {logConfig = jobLog config job}

-- | The pool's log context narrowed to one job.
jobLog :: WorkerConfig m payload -> Job.JobRead payload -> LogConfig
jobLog config = withJobContextOne (logConfig config)

warnEx :: (MonadUnliftIO m) => LogConfig -> Text -> SomeException -> m ()
warnEx logCfg label e = tryLog logCfg Warning $ label <> ": " <> T.pack (displayException e)

-- | Main loop for a single worker thread.
workerLoop
  :: forall payload m
   . ( EncodeJobResult (ResultOf m payload)
     , JobOperation m payload
     , MonadUnliftIO m
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
        modifyTVar' runningJobs $ \m -> foldl' (flip Map.delete) m jobIds
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
          | Just (JobForceCancelled cancelledIds) <- fromException e -> do
              finalized <- cancelFinalized handoff
              unless finalized $
                finalizeForceCancelled config jobBatch cancelledIds handoff
          | Just Async.AsyncCancelled <- fromException e -> throwIO e
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
     , MonadUnliftIO m
     )
  => WorkerConfig m payload
  -> ConsumeSpan
  -- ^ The pool's consumer-span shape, built once for its queue.
  -> CancelHandoff
  -> NonEmpty (Job.JobRead payload)
  -> m ()
processJobsWithRetry config consumeSpan handoff jobs = do
  let hooks = observabilityHooks config
      jobMaxAtts job = fromMaybe Job.defaultMaxAttempts (Job.maxAttempts job)
      -- Ack a job, throwing if it was reclaimed by another worker mid-flight.
      ackJobOrSkip job = do
        rowsAffected <- Arb.ackJob job
        when (rowsAffected == 0) $
          throwJobNotFoundIds "reclaimed by another worker during processing" [Job.primaryKey job]
  startTime <- liftIO getCurrentTime
  schemaName <- Arb.getSchema
  let (firstJob :| _) = jobs
      markHandled = markJobHandled handoff
      fireSuccess j = do
        endT <- liftIO getCurrentTime
        runHook (jobLog config j) "onJobSuccess" $ Job.onJobSuccess hooks j startTime endT
      -- Rethrown with base throwIO, UnliftIO's wrapping it as synchronous. The flag
      -- is set last, so an interrupted finalizer leaves the rest to 'workerLoop'.
      onForceCancel exc@(JobForceCancelled cancelledIds) = do
        finalizeForceCancelled config jobs cancelledIds handoff
        markCancelFinalized handoff
        liftIO $ E.throwIO exc
      finalize j = fireSuccess j >> markHandled j
      nackOne j = Arb.nackJob j >> markHandled j
      failWith j exc = do
        endT <- liftIO getCurrentTime
        withDbTransaction $ handleJobFailure (withJobLog config j) exc (jobMaxAtts j) startTime endT j
        markHandled j
      failAs mkExc j msg = failWith j (toException (mkExc msg))
      ackOneStoring j mVal = do
        withDbTransaction $ do
          ackJobOrSkip j
          storeEncodedResult schemaName j mVal
        finalize j
      ackBatchStoring pairs = do
        let js = map fst pairs
            isAcked acked j = Job.primaryKey j `Set.member` acked
        acked <- withDbTransaction $ do
          ackedSet <- Set.fromList <$> Arb.ackJobsBatch js
          storeEncodedResults schemaName (filter (isAcked ackedSet . fst) pairs)
          pure ackedSet
        let (done, reclaimed) = partition (isAcked acked) js
        unless (null reclaimed) $ do
          tryLog
            (withJobContextList (logConfig config) reclaimed)
            Info
            "Jobs no longer claimed here during bulk completion, skipped"
          -- A force-cancel voids the claim the same way a reclaim does.
          cancelled <-
            deleteCancelledOrWarn
              (withJobContextList (logConfig config) reclaimed)
              schemaName
              (Job.queueName firstJob)
              (map Job.primaryKey reclaimed)
          traverse_ (\j -> reportReclaimed cancelled j >> markUnowned j) reclaimed
        traverse_ finalize done
      reportReclaimed cancelled j
        | Set.member (Job.primaryKey j) cancelled = fireForceCancelled (withJobLog config j) j
        | otherwise = fireUnavailable (withJobLog config j) j "no longer claimed by this worker"
      markUnowned j = markJobUnowned handoff j >> markHandled j
      callbacks =
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
  tracer <- resolveTracer
  let inJobSpan
        | batchedPool config = withConsumeSpanBatch tracer consumeSpan jobs
        | otherwise = withConsumeSpan tracer consumeSpan firstJob
  -- The span covers the outcome report and the force-cancel finalizer too, so every
  -- terminal hook fires while it is open.
  inJobSpan $ flip catchSyncOrAsync onForceCancel $ do
    result <-
      tryAny
        $ withJobsHeartbeat
          tracer
          hooks
          (jobHeartbeatInterval config)
          (visibilityTimeout config)
          startTime
          jobs
          (logConfig config)
          (heartbeatSignal config)
        $ case handlerMode config of
          SingleJobMode handler -> do
            withDbTransaction $ do
              handlerResult <- runHandlerWithConnection handler firstJob
              ackJobOrSkip firstJob
              storeJobResult schemaName firstJob handlerResult
            finalize firstJob
          BatchedJobsMode _ handler -> handler jobs callbacks
    endTime <- liftIO getCurrentTime
    reportBatchOutcome config startTime endTime jobs handoff result

-- | Delete the jobs a force-cancel flagged, report them, and hand back the attempt
-- the claim consumed for the batch siblings it interrupted.
finalizeForceCancelled
  :: (JobOperation m payload, MonadUnliftIO m)
  => WorkerConfig m payload
  -> NonEmpty (Job.JobRead payload)
  -> [Int64]
  -- ^ The jobs the cancel named.
  -> CancelHandoff
  -> m ()
finalizeForceCancelled config jobs cancelledIds handoff = do
  tryLog batchLog Info "Job(s) force-cancelled"
  schemaName <- getSchema
  deleted <- deleteCancelledOrWarn batchLog schemaName (Job.queueName firstJob) jobIds
  pending <- pendingJobs handoff jobs
  cancelled <- recordCancelled handoff (deleted <> Set.fromList cancelledIds)
  let (gone, siblings) = partition (\j -> Set.member (Job.primaryKey j) cancelled) pending
  traverse_ (\j -> cancelHook j >> markJobHandled handoff j) gone
  traverse_
    ( \j ->
        tryWarn batchLog "Releasing a force-cancel batch sibling failed" $
          Arb.nackJob j >> markJobHandled handoff j
    )
    siblings
  where
    (firstJob :| _) = jobs
    jobIds = map Job.primaryKey (toList jobs)
    batchLog = withJobContext (logConfig config) jobs
    cancelHook job = fireForceCancelled (withJobLog config job) job

-- | Delete whichever of @jobIds@ a force-cancel flagged, returning the ids it deleted.
deleteCancelledOrWarn
  :: (MonadArbiter m, MonadUnliftIO m)
  => LogConfig
  -> SchemaName
  -> Text
  -- ^ Queue the jobs belong to.
  -> [Int64]
  -> m (Set.Set Int64)
deleteCancelledOrWarn logCfg schemaName queue jobIds =
  tryWarnWith logCfg "Deleting force-cancelled jobs failed" mempty $
    Set.fromList <$> Ops.deleteCancelledJobsReturning schemaName queue jobIds

-- | What a force-cancel finalizer needs from the handler's scope, whichever side of
-- the job span runs it.
data CancelHandoff = CancelHandoff
  { handledRef :: IORef (Set.Set Int64)
  , unownedRef :: IORef (Set.Set Int64)
  , cancelledRef :: IORef (Set.Set Int64)
  , finalizedRef :: IORef Bool
  }

newCancelHandoff :: (MonadIO m) => m CancelHandoff
newCancelHandoff =
  liftIO (CancelHandoff <$> newIORef Set.empty <*> newIORef Set.empty <*> newIORef Set.empty <*> newIORef False)

-- | The batch's jobs still awaiting an outcome, which keeps the force-cancel finalizer
-- and the outcome report from both reporting the same job.
pendingJobs :: (MonadIO m) => CancelHandoff -> NonEmpty (Job.JobRead payload) -> m [Job.JobRead payload]
pendingJobs handoff jobs = do
  handled <- liftIO (readIORef (handledRef handoff))
  pure (filter (\j -> not (Set.member (Job.primaryKey j) handled)) (toList jobs))

markJobHandled :: (MonadIO m) => CancelHandoff -> Job.JobRead payload -> m ()
markJobHandled = insertJob . handledRef

markJobUnowned :: (MonadIO m) => CancelHandoff -> Job.JobRead payload -> m ()
markJobUnowned = insertJob . unownedRef

insertJob :: (MonadIO m) => IORef (Set.Set Int64) -> Job.JobRead payload -> m ()
insertJob ref job = liftIO $ atomicModifyIORef' ref $ \s -> (Set.insert (Job.primaryKey job) s, ())

-- | Add to the jobs a force-cancel accounted for, returning every id recorded so far.
recordCancelled :: (MonadIO m) => CancelHandoff -> Set.Set Int64 -> m (Set.Set Int64)
recordCancelled handoff ids =
  liftIO $ atomicModifyIORef' (cancelledRef handoff) $ \s -> let s' = s <> ids in (s', s')

-- | Whether a force-cancel was finalized in full, which only the finalizer records.
cancelFinalized :: (MonadIO m) => CancelHandoff -> m Bool
cancelFinalized = liftIO . readIORef . finalizedRef

markCancelFinalized :: (MonadIO m) => CancelHandoff -> m ()
markCancelFinalized = liftIO . flip writeIORef True . finalizedRef

-- | Interpret a finished batch: warn when the handler left jobs unfinalized,
-- skip retry for gone or nacked jobs, otherwise fail whatever was not finalized.
reportBatchOutcome
  :: forall payload m
   . (JobOperation m payload, MonadUnliftIO m)
  => WorkerConfig m payload
  -> UTCTime
  -> UTCTime
  -> NonEmpty (Job.JobRead payload)
  -> CancelHandoff
  -> Either SomeException ()
  -> m ()
reportBatchOutcome config startTime endTime jobs handoff outcome = do
  unhandled <- pendingJobs handoff jobs
  unowned <- liftIO (readIORef (unownedRef handoff))
  let splitNamed ids = let idSet = Set.fromList ids in partition (\j -> Set.member (Job.primaryKey j) idSet) unhandled
      reportUnavailable ids reason = do
        -- An exception naming no job speaks for none of them, so the remainder keeps
        -- its attempt and reports when it runs.
        let (jobsGone, siblings) = splitNamed ids
        traverse_ (\job -> fireUnavailable (withJobLog config job) job reason >> markJobHandled handoff job) jobsGone
        unless (null ids || null siblings) $
          tryWarn batchLog "Releasing an interrupted batch sibling failed" $
            withDbTransaction (traverse_ (void . Arb.nackJob) siblings)
      unownedOf = filter (\j -> Set.member (Job.primaryKey j) unowned) (toList jobs)
  case outcome of
    Right () ->
      unless (null unhandled) $
        tryLog (withJobContextList (logConfig config) unhandled) Warning "Handler left jobs unfinalized, will reprocess"
    Left exc
      | Just (JobStolenException ids stolen) <- fromException exc -> do
          tryLog batchLog Info $ "Job(s) reclaimed by another worker, skipping retry: " <> ids
          reportUnavailable stolen "reclaimed by another worker"
      | Just (JobNotFoundException _ gone) <- fromException exc -> do
          tryLog batchLog Info "Job(s) no longer available, skipping retry"
          reportUnavailable gone "no longer available"
      | Just JobNackException <- fromException exc -> do
          -- Hand back the attempt the claim consumed for every job the handler
          -- left unfinalized, so the nacked reprocess does not record a failure.
          withDbTransaction $ traverse_ (void . Arb.nackJob) unhandled
          tryLog batchLog Info "Job(s) nacked, will be reprocessed"
      | otherwise -> do
          -- Fail the jobs the handler did not finalize, in a separate transaction.
          let kind = snd (classifyException exc)
          withDbTransaction $ do
            traverse_ (failJob exc) unhandled
            -- A tree or branch cancel acts on the tree, not on this worker's claim.
            when (cancelsTree kind) $ traverse_ (void . cancelJobFor kind) unownedOf
          traverse_ (markJobHandled handoff) unhandled
  where
    batchLog = withJobContext (logConfig config) jobs
    jobMaxAtts job = fromMaybe Job.defaultMaxAttempts (Job.maxAttempts job)
    failJob exc job =
      handleJobFailure (withJobLog config job) exc (jobMaxAtts job) startTime endTime job

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

-- | Classify a handler exception into an error message and failure disposition.
--
-- Note: 'JobNotFoundException' and 'JobStolenException' are intercepted by
-- 'reportBatchOutcome' before reaching 'handleJobFailure', so they never
-- arrive here.
data FailureKind = RetryFailure | PermanentFailure | TreeCancelFailure | BranchCancelFailure
  deriving stock (Eq)

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

-- | Report a claimed job the worker can no longer act on.
fireUnavailable
  :: (JobOperation m payload, MonadUnliftIO m)
  => WorkerConfig m payload
  -> Job.JobRead payload
  -> Text
  -> m ()
fireUnavailable config job reason =
  runHook (logConfig config) "onJobUnavailable" $
    Job.onJobUnavailable (observabilityHooks config) job reason

-- | Report a job a force-cancel took out from under its handler.
fireForceCancelled
  :: (JobOperation m payload, MonadUnliftIO m)
  => WorkerConfig m payload
  -> Job.JobRead payload
  -> m ()
fireForceCancelled config job = fireCancelled config job "force-cancelled"

-- | Whether this pool's consumer spans cover a batch rather than a single job.
batchedPool :: WorkerConfig m payload -> Bool
batchedPool = (> 1) . handlerBatchSize

-- | Report a failed job to its hooks and to the consumer span it ran under. A batch
-- span also covers the jobs that succeeded, so only a single-job span takes the error
-- status.
fireFailure
  :: (JobOperation m payload, MonadUnliftIO m)
  => WorkerConfig m payload
  -> Job.JobRead payload
  -> Text
  -> UTCTime
  -> UTCTime
  -> m ()
fireFailure config job errorMsg startTime endTime = do
  recordJobFailure job errorMsg
  unless (batchedPool config) (markSpanError errorMsg)
  runHook (logConfig config) "onJobFailure" $
    Job.onJobFailure (observabilityHooks config) job errorMsg startTime endTime

-- | Report a cancelled job.
fireCancelled
  :: (JobOperation m payload, MonadUnliftIO m)
  => WorkerConfig m payload
  -> Job.JobRead payload
  -> Text
  -> m ()
fireCancelled config job errorMsg = do
  recordJobCancelled job errorMsg
  runHook (logConfig config) "onJobCancelled" $
    Job.onJobCancelled (observabilityHooks config) job errorMsg

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

-- | Handle failure for a single job (retry or move to DLQ).
handleJobFailure
  :: forall payload m
   . ( JobOperation m payload
     , MonadUnliftIO m
     )
  => WorkerConfig m payload
  -> SomeException
  -> Int32
  -> UTCTime
  -> UTCTime
  -> Job.JobRead payload
  -> m ()
handleJobFailure config e maxAtts startTime endTime job = do
  let (errorMsg, failureKind) = classifyException e
      cfg = logConfig config
      hooks = observabilityHooks config
      unavailable = fireUnavailable config job
      -- A batch sibling's cancel takes out the whole tree, so no rows still means gone.
      cancel = void (cancelJobFor failureKind job) >> fireCancelled config job errorMsg
  schemaName <- getSchema
  case failureKind of
    TreeCancelFailure -> cancel
    BranchCancelFailure -> cancel
    _
      | failureKind == PermanentFailure || Job.attempts job >= maxAtts -> do
          -- Snapshot into parent_state before DLQ move (survives CASCADE delete).
          -- Merges old snapshot so repeated DLQ round-trips don't lose data.
          when (Job.isRollup job) $ do
            (results, failures, mSnapshot, _dlqFailures) <-
              Ops.readChildResultsRaw schemaName (Job.queueName job) (Job.primaryKey job)
            let merged = Ops.mergeRawChildResults results failures mSnapshot
            unless (Map.null merged) $
              void $
                Ops.persistParentState schemaName (Job.queueName job) (Job.primaryKey job) (toJSON merged)
          -- Permanent failure or max attempts reached - move to DLQ
          rowsAffected <- Arb.moveToDLQ errorMsg job
          if rowsAffected == 0
            then do
              tryLog cfg Warning "Job not available for moving to DLQ"
              unavailable "no longer available for the dead-letter queue"
            else do
              -- Successfully moved to DLQ
              fireFailure config job errorMsg startTime endTime
              runHook cfg "onJobFailedAndMovedToDLQ" $ Job.onJobFailedAndMovedToDLQ hooks errorMsg job
      | otherwise -> do
          -- Retry with configured backoff strategy and jitter
          let baseDelay = calculateBackoff (backoffStrategy config) (Job.attempts job)
          backoffSecs <- liftIO $ applyJitter (jitter config) baseDelay
          rowsAffected <- Arb.updateJobForRetry backoffSecs errorMsg job
          if rowsAffected == 0
            then do
              tryLog cfg Warning $
                "Job " <> T.pack (show (Job.primaryKey job)) <> " not available for retry"
              unavailable "no longer available for retry"
            else do
              -- Successfully updated for retry
              fireFailure config job errorMsg startTime endTime
              runHook cfg "onJobRetry" $ Job.onJobRetry hooks job backoffSecs

-- | Refreshes the groups tables, sweeps stale worker registry rows, moves
-- exhausted jobs to the DLQ, and purges expired archived jobs (all schema-wide).
-- Each gated so only one pool runs it per interval.
reaperLoop
  :: forall m
   . ( Arb.RegistryAdmissionPolicies (RegistryOf m)
     , MonadArbiter m
     , MonadUnliftIO m
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
  let reaped op n = when (n > 0) $ runHook logCfg "onMaintenance" $ report op n
      intervalSecs = ceiling interval
      queues = registryTableNames (Proxy @(RegistryOf m))
      pruneInterval = interval * 12
      hasConcurrency = not (Set.null (registryConcurrencyPolicies @(RegistryOf m)))
      hasRateLimit = not (Set.null (registryRateLimitPolicies @(RegistryOf m)))
  schemaName <- Arb.getSchema
  let gated :: forall a. MaintenanceOp -> NominalDiffTime -> m a -> m (Maybe a)
      gated = runReaperOp logCfg schemaName stmtTimeout . maintenanceOpName
      -- Reports the rows the op touched, for the ops whose result counts any.
      gatedRows :: forall a. MaintenanceOp -> NominalDiffTime -> (a -> Int64) -> m a -> m (Maybe a)
      gatedRows op every rowsOf work = do
        mr <- gated op every work
        mr <$ traverse_ (reaped op . rowsOf) mr
      sweep
        :: MaintenanceOp
        -> NominalDiffTime
        -> Text
        -> (Int64 -> m ())
        -> m (Int64, [Text])
        -> m ()
      sweep op every failMsg done work =
        gatedRows op every fst work
          >>= traverse_
            ( \(n, failed) -> do
                traverse_ (\queue -> tryLog logCfg Warning $ failMsg <> queue) failed
                when (n > 0) $ done n
            )
  forever $ do
    sweep RefreshGroups interval "Groups refresh failed for queue: " (const (pure ())) $
      Ops.refreshAllGroups schemaName queues
    void $ gatedRows SweepStaleWorkers interval id $ Ops.sweepStaleWorkers schemaName
    sweep
      SweepExhaustedJobs
      interval
      "Exhausted-job sweep failed for queue: "
      (\n -> tryLog logCfg Warning $ "Reaper moved " <> T.pack (show n) <> " exhausted job(s) to the DLQ")
      $ Ops.sweepExhaustedJobs schemaName queues
    sweep
      SweepCancelledJobs
      interval
      "Cancelled-job sweep failed for queue: "
      (\n -> tryLog logCfg Info $ "Reaper deleted " <> T.pack (show n) <> " orphaned cancelled job(s)")
      $ Ops.sweepCancelledJobs schemaName queues
    when hasRateLimit $
      void $
        gatedRows PruneRateLimitBuckets pruneInterval id $
          Arb.pruneRateLimitBuckets @m interval
    when hasConcurrency $ do
      void $ gatedRows ReconcileConcurrencyStale interval id $ Arb.reconcileConcurrencyCountsIfStale @m
      void $ gatedRows ReconcilePruneConcurrency pruneInterval id $ Arb.reconcileAndPruneConcurrency @m
    sweep
      PurgeArchives
      interval
      "Archive purge failed for queue: "
      (\n -> tryLog logCfg Info $ "Reaper purged " <> T.pack (show n) <> " archived job(s)")
      $ Ops.purgeArchives schemaName queues
    threadDelay (intervalSecs * 1_000_000)

-- | Run one gated reaper op, logging and swallowing failures so the loop survives.
-- statement_timeout bounds each statement (aborting a stuck one at the DB), while a
-- legitimately long multi-statement op still runs to completion.
runReaperOp
  :: (MonadArbiter m, MonadUnliftIO m)
  => LogConfig
  -> SchemaName
  -> NominalDiffTime
  -> Text
  -> NominalDiffTime
  -> m a
  -> m (Maybe a)
runReaperOp logCfg schemaName stmtTimeout task every work =
  tryWarnWith logCfg ("Reaper op failed: " <> task) Nothing $
    Ops.runGatedBounded schemaName task every stmtTimeout work

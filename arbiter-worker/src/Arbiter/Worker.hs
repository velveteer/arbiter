{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeAbstractions #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Entry point for running a worker pool that fetches and executes jobs.
module Arbiter.Worker
  ( -- * Running Workers
    runWorkerPool

    -- * Multi-Queue Workers
  , NamedWorkerPool (..)
  , namedWorkerPool
  , queueOf
  , runWorkerPools
  , runSelectedWorkerPools
  , getEnabledQueues
  , getEnabledQueuesFrom
  , retryPolicyOf
  , withPoolContext

    -- * Job Result
  , JobResult (..)

    -- * Rollup Child Results
  , childResults
  , mergedChildResults
  , mergeChildResults

    -- * Re-exports
  , module Arbiter.Worker.Config
  , module Arbiter.Worker.BackoffStrategy
  , module Arbiter.Worker.Logger
  , module Arbiter.Worker.WorkerState
  , FailureOutcome (..)
  , FailureReport (..)
  , RetryPolicy (..)
  , defaultRetryPolicy
  , tryLog

    -- * Reaper
  , runQueueMaintenance
  , runReaperOp

    -- * Cron
  , CronJob (..)
  , OverlapPolicy (..)
  , BackfillPolicy (..)
  , cronJob
  , initCronSchedules
  , overlapPolicyToText
  , overlapPolicyFromText
  ) where

import Arbiter.Core.Concurrency.Spec (registryConcurrencyPolicies)
import Arbiter.Core.Exceptions
  ( BranchCancelException (..)
  , JobException (..)
  , JobNackException (..)
  , JobNotFoundException (..)
  , JobPermanentException (..)
  , JobRetryableException (..)
  , JobStolenException (..)
  , ParsingException (..)
  , TreeCancelException (..)
  , throwJobNotFound
  )
import Arbiter.Core.HasArbiterSchema (HasArbiterSchema (..))
import Arbiter.Core.HighLevel (JobOperation, QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables (..), TableForPayload)
import Arbiter.Core.RateLimit.Spec (registryRateLimitPolicies)
import Control.Exception (SomeException, fromException, toException)
import Control.Monad (forever, replicateM, unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Aeson (FromJSON, ToJSON, Value, toJSON)
import Data.Aeson qualified as Aeson
import Data.Bifunctor (second)
import Data.Bits (xor)
import Data.Foldable (fold, foldMap', for_, toList, traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int32, Int64)
import Data.List (partition)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Traversable (for)
import Data.Word (Word64)
import GHC.TypeLits (symbolVal)
import Numeric (showHex)
import System.Directory (removeFile)
import System.Environment (lookupEnv)
import UnliftIO
  ( MonadUnliftIO
  , atomically
  , bracket
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
  , writeTVar
  )
import UnliftIO.Async qualified as Async
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.STM (TBQueue, TVar)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.BackoffStrategy
import Arbiter.Worker.ChannelHandlers
  ( JobForceCancelled (..)
  , RunningJobs
  , handleCancelNotif
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
  )
import Arbiter.Worker.Dispatcher
import Arbiter.Worker.Heartbeat (withJobsHeartbeat)
import Arbiter.Worker.Logger
import Arbiter.Worker.Logger.Internal
  ( runHook
  , tryLog
  , withJobContext
  , withJobContextList
  , withJobContextOne
  )
import Arbiter.Worker.NotificationListener (runMultiChannelListener)
import Arbiter.Worker.Outcome (FailureOutcome (..), FailureReport (..), RetryPolicy (..), defaultRetryPolicy)
import Arbiter.Worker.Outcome qualified as Outcome
import Arbiter.Worker.Retry (spawnRetried)
import Arbiter.Worker.Trace qualified as Trace
import Arbiter.Worker.WorkerState

-- ---------------------------------------------------------------------------
-- Job Result
-- ---------------------------------------------------------------------------

-- | Handler result types. @()@ is fire-and-forget. any @(ToJSON a, FromJSON a)@
-- is stored in the results table when the job has a parent and decoded when
-- read by a rollup finalizer.
class JobResult a where
  encodeJobResult :: a -> Maybe Value
  decodeJobResult :: Value -> Either Text a

instance JobResult () where
  encodeJobResult _ = Nothing
  decodeJobResult _ = Right ()

instance {-# OVERLAPPABLE #-} (FromJSON a, ToJSON a) => JobResult a where
  encodeJobResult = Just . toJSON
  decodeJobResult v = case Aeson.fromJSON v of
    Aeson.Success a -> Right a
    Aeson.Error err -> Left (T.pack err)

-- ---------------------------------------------------------------------------
-- Multi-Queue Workers
-- ---------------------------------------------------------------------------

-- | A worker pool paired with its name.
--
-- @
-- allWorkers =
--   [ namedWorkerPool emailConfig      -- claims "email_jobs"
--   , namedWorkerPool imageConfig      -- claims "image_jobs"
--   ]
--
-- main = runSimpleDb env $ runWorkerPools (Proxy \@MyRegistry) allWorkers (\\_ -> pure ())
-- @
data NamedWorkerPool m
  = forall registry payload result.
  ( Arb.RegistryAdmissionPolicies registry
  , JobResult result
  , QueueOperation m registry payload
  , RegistryTables registry
  ) =>
  NamedWorkerPool
  { workerPoolName :: Text
  -- ^ What this pool is called, in its logs. Two pools may claim one queue. Metrics stay
  -- keyed by the queue.
  , workerPoolConfig :: WorkerConfig m payload result
  -- ^ The worker configuration
  }

-- | The queue a pool claims from, as its payload type names it in the registry.
queueOf :: NamedWorkerPool m -> Text
queueOf (NamedWorkerPool @_ @registry @payload _ _) =
  T.pack $ symbolVal (Proxy @(TableForPayload payload registry))

-- | Create a named worker pool, deriving the name from the type-level registry.
namedWorkerPool
  :: forall m registry payload result
   . ( Arb.RegistryAdmissionPolicies registry
     , JobResult result
     , QueueOperation m registry payload
     , RegistryTables registry
     )
  => WorkerConfig m payload result
  -> NamedWorkerPool m
namedWorkerPool cfg =
  NamedWorkerPool
    { workerPoolName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
    , workerPoolConfig = cfg
    }

-- | Run worker pools with shared shutdown state. Filters to queues listed
-- in @ARBITER_ENABLED_QUEUES@ (all if unset). The setup action receives the
-- shared 'TVar' for installing signal handlers.
runWorkerPools
  :: forall m registry
   . (MonadUnliftIO m, RegistryTables registry)
  => Proxy registry
  -> [NamedWorkerPool m]
  -> (TVar WorkerState -> IO ())
  -> m ()
runWorkerPools registry pools setup = do
  sharedState <- liftIO newWorkerState
  liftIO $ setup sharedState
  enabled <- liftIO $ getEnabledQueues "ARBITER_ENABLED_QUEUES" registry
  runSelectedWorkerPools sharedState enabled pools

-- | Run only the worker pools claiming from a queue in the enabled list.
runSelectedWorkerPools
  :: (MonadUnliftIO m)
  => TVar WorkerState
  -> [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPools sharedState enabled pools =
  case filter ((`elem` enabled) . queueOf) pools of
    [] -> pure ()
    selected -> evalContT $ do
      asyncs <- for selected $ \(NamedWorkerPool name cfg) ->
        ContT $ \k ->
          Async.withAsync
            (runWorkerPool cfg {workerStateVar = sharedState, logConfig = withPoolContext name (logConfig cfg)})
            k
      lift $ traverse_ Async.waitCatch asyncs

-- | Inject the pool name into log context, leaving any already there. A caller naming its
-- pool wins over the queue name 'runWorkerPool' falls back to.
withPoolContext :: Text -> LogConfig -> LogConfig
withPoolContext poolName lc =
  lc {additionalContext = named <$> additionalContext lc}
  where
    named ctx
      | any ((== "pool") . fst) ctx = ctx
      | otherwise = ("pool" .= poolName) : ctx

-- | Get enabled queues from an environment variable.
--
-- If the environment variable is set and non-empty, parses it as a
-- comma-separated list of queue names. Each name is validated against the
-- registry - invalid names cause an error. If not set or empty, returns all
-- queue names from the registry.
--
-- Example:
--
-- @
-- -- With ENABLED_QUEUES="email_jobs,notifications"
-- queues <- getEnabledQueues "ENABLED_QUEUES" (Proxy \@MyRegistry)
-- -- Returns: ["email_jobs", "notifications"]
--
-- -- With ENABLED_QUEUES unset or empty
-- queues <- getEnabledQueues "ENABLED_QUEUES" (Proxy \@MyRegistry)
-- -- Returns: all queues from registry
--
-- -- With ENABLED_QUEUES="email_jobs,invalid_queue"
-- -- Throws error: "Unknown queue names: invalid_queue"
-- @
getEnabledQueues
  :: (RegistryTables registry)
  => String
  -- ^ Environment variable name
  -> Proxy registry
  -- ^ Registry proxy
  -> IO [Text]
getEnabledQueues envVar registry = getEnabledQueuesFrom envVar (registryTableNames registry)

-- | 'getEnabledQueues' against an explicit candidate list rather than a registry.
-- Unset or empty enables all. Any name not in @allQueues@ is rejected.
getEnabledQueuesFrom :: String -> [Text] -> IO [Text]
getEnabledQueuesFrom envVar allQueues = do
  mVal <- lookupEnv envVar
  case mVal of
    Nothing -> pure allQueues
    Just val -> do
      let tval = T.pack val
      if T.null (T.strip tval)
        then pure allQueues
        else do
          let requested = map T.strip $ T.splitOn "," tval
              invalid = filter (`notElem` allQueues) requested
          if null invalid
            then pure requested
            else throwIO . userError $ "Unknown queue names: " <> T.unpack (T.intercalate ", " invalid)

-- ---------------------------------------------------------------------------
-- Worker Pool
-- ---------------------------------------------------------------------------

-- | Starts a worker pool with a dispatcher and N worker threads. With no 'handlerMode'
-- it consumes nothing and runs only the queue's background loops.
runWorkerPool
  :: forall m registry payload result
   . ( Arb.RegistryAdmissionPolicies registry
     , JobResult result
     , MonadUnliftIO m
     , QueueOperation m registry payload
     , RegistryTables registry
     )
  => WorkerConfig m payload result
  -> m ()
runWorkerPool config0 = do
  let queueName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  resolvedTracer <- Trace.resolveTracer
  let config = config0 {logConfig = withPoolContext queueName (logConfig config0), tracer = resolvedTracer}
  schemaName <- getSchema
  case handlerMode config of
    Nothing -> maintainOnly config schemaName queueName
    Just mode -> consume config mode schemaName queueName
  where
    maintainOnly config schemaName queueName = evalContT $ do
      loops <- backgroundLoops config schemaName queueName
      lift $ Async.race_ (liftIO (awaitShutdown (workerStateVar config))) (awaitPool (logConfig config) loops)

    consume config mode schemaName queueName = do
      let workerCap = workerCount config
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
      let createChannel = T.unpack (Schema.notificationChannelForTable queueName)
          pauseChannel = T.unpack (Schema.pauseNotifyChannel schemaName queueName)
          cancelChannel = T.unpack (Schema.cancelNotifyChannel schemaName queueName)
          handlers =
            [ (createChannel, atomically . STM.writeTVar dispatcherNotifVar . Just)
            , (pauseChannel, handlePauseNotif config)
            , (cancelChannel, handleCancelNotif config runningJobs)
            ]

      listenerReady <- STM.newTVarIO False

      evalContT $ do
        withLivenessFile config
        listener <-
          spawnRetried (workerStateVar config) (logConfig config) "Multi-channel listener" $
            runMultiChannelListener (connStr config) handlers (logConfig config) listenerReady
        lift . atomically $
          (readTVar listenerReady >>= checkSTM)
            `STM.orElse` void (Async.waitCatchSTM listener)
        heartbeat <-
          spawnRetried (workerStateVar config) (logConfig config) "Worker heartbeat" $
            heartbeatLoop config schemaName queueName
        dispatcher <-
          spawnRetried (workerStateVar config) (logConfig config) "Dispatcher" $
            runDispatcher config mode workerCap workQueue busyWorkerCount workerFinishedVar dispatcherNotifVar
        workers <-
          replicateM workerCap $
            spawnRetried (workerStateVar config) (logConfig config) "Worker thread" $
              workerLoop config mode runningJobs workQueue busyWorkerCount workerFinishedVar
        loops <- backgroundLoops config schemaName queueName

        lift $ awaitPool (logConfig config) (dispatcher : heartbeat : listener : loops <> workers)
        lift $ shutdownPool config schemaName workQueue busyWorkerCount

-- | The loops a pool runs whether or not it consumes: the reaper, and any cron schedules.
backgroundLoops
  :: ( Arb.RegistryAdmissionPolicies registry
     , MonadUnliftIO m
     , QueueOperation m registry payload
     , RegistryTables registry
     )
  => WorkerConfig m payload result
  -> SchemaName
  -> Text
  -> ContT r m [Async.Async ()]
backgroundLoops config schemaName queueName = do
  crons <-
    unlessNull (cronJobs config) $
      spawnRetried (workerStateVar config) (logConfig config) "Cron scheduler" $
        runCronScheduler (workerStateVar config) (logConfig config) schemaName queueName (cronJobs config)
  reaper <-
    spawnRetried (workerStateVar config) (logConfig config) "Reaper" $
      runQueueMaintenance
        (logConfig config)
        (claimAdmissionOverride config)
        (maintenanceQueues config)
        (reaperInterval config)
        (reaperTimeout config)
  pure (reaper : crons)

-- | Wait for the pool's first thread to finish, logging whatever brought it down.
awaitPool :: (MonadUnliftIO m) => LogConfig -> [Async.Async ()] -> m ()
awaitPool logCfg asyncs =
  Async.waitAnyCatch asyncs >>= \case
    (_, Left e) -> tryLog logCfg Error $ "Thread pool exception: " <> T.pack (show e)
    (_, Right ()) -> pure ()

-- | Remove the liveness file when the pool exits, after the drain.
withLivenessFile :: (MonadUnliftIO m) => WorkerConfig n payload result -> ContT r m ()
withLivenessFile config = case livenessFile config of
  Nothing -> pure ()
  Just path -> ContT $ \k ->
    bracket
      (pure ())
      (\_ -> void . tryAny . liftIO . removeFile $ path)
      (\_ -> k ())

-- | Re-insert the worker's registry row from the config + schema/queue.
-- Returns the effective paused state so the caller can seed 'pauseVar'.
registerSelf :: (MonadArbiter m) => WorkerConfig n payload result -> SchemaName -> Text -> m (Maybe Bool)
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
  => WorkerConfig n payload result
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
  => WorkerConfig n payload result
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
tryWarn logCfg label act = tryAny act >>= either (warnEx logCfg label) (const (pure ()))

warnEx :: (MonadUnliftIO m) => LogConfig -> Text -> SomeException -> m ()
warnEx logCfg label e = tryLog logCfg Warning $ label <> ": " <> T.pack (show e)

-- | Main loop for a single worker thread.
workerLoop
  :: forall m registry payload result
   . ( JobOperation m registry payload
     , JobResult result
     , MonadUnliftIO m
     )
  => WorkerConfig m payload result
  -> HandlerMode m payload result
  -> RunningJobs
  -- ^ Pool-shared map from job id to running handler async.
  -> TBQueue (NonEmpty (Job.JobRead payload))
  -> TVar Int
  -- ^ Busy worker count
  -> TVar Bool
  -- ^ Worker finished signal
  -> m ()
workerLoop config mode runningJobs workQueue busyCount workerFinishedVar = forever $ mask_ $ do
  -- Mask covers the window between the atomic claim (which increments
  -- busyCount) and entering the finally block that decrements it.
  jobBatch <- atomically $ do
    batch <- readTBQueue workQueue
    modifyTVar' busyCount (+ 1)
    pure batch

  let jobIds = map Job.primaryKey (toList jobBatch)
      jobLog job = withJobContextOne (logConfig config) job
      batchLog = withJobContext (logConfig config) jobBatch
      claimHook now job =
        runHook (jobLog job) "onJobClaimed" $
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
      result <-
        withRegisteredJobs runningJobs jobIds $
          processJobsWithRetry config mode jobBatch
      case result of
        Right () -> pure ()
        Left e
          | Just JobForceCancelled <- fromException e ->
              tryLog batchLog Info "Job(s) force-cancelled"
          | Just Async.AsyncCancelled <- fromException e -> throwIO e
          | otherwise -> do
              tryLog batchLog Error $ "Worker exception: " <> T.pack (show e)
              threadDelay 2_000_000

-- | Read and decode child results for a rollup finalizer.
-- Decode failures appear as @Left decodeError@ - the child succeeded but
-- its result JSON doesn't match the expected type.
readChildResults
  :: (JobResult a, MonadArbiter m)
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
  :: forall m registry payload result
   . ( JobOperation m registry payload
     , JobResult result
     , MonadUnliftIO m
     )
  => WorkerConfig m payload result
  -> HandlerMode m payload result
  -> NonEmpty (Job.JobRead payload)
  -> m ()
processJobsWithRetry config mode jobs = do
  let hooks = observabilityHooks config
      jobMaxAtts job = fromMaybe Job.defaultMaxAttempts (Job.maxAttempts job)
  startTime <- liftIO getCurrentTime
  schemaName <- Arb.getSchema
  let ackJobOrSkip mResult job = do
        rowsAffected <- Outcome.completeJob schemaName mResult job
        when (rowsAffected == 0) $
          throwJobNotFound "reclaimed by another worker during processing"
  handledRef <- liftIO $ newIORef Set.empty
  let jobLog j = withJobContextOne (logConfig config) j
      markHandled j = liftIO $ atomicModifyIORef' handledRef $ \s -> (Set.insert (Job.primaryKey j) s, ())
      fireSuccess j = do
        endT <- liftIO getCurrentTime
        runHook (jobLog j) "onJobSuccess" $ Job.onJobSuccess hooks j startTime endT
      finalize j = fireSuccess j >> markHandled j
      nackOne j = Arb.nackJob j >> markHandled j
      failWith j exc = do
        endT <- liftIO getCurrentTime
        event <- withDbTransaction $ handleJobFailure config {logConfig = jobLog j} exc (jobMaxAtts j) j
        traverse_ (fireFailureHooks (jobLog j) hooks startTime endT j) event
        markHandled j
      failAs mkExc j msg = failWith j (toException (mkExc msg))
      ackOne j = do
        withDbTransaction $ ackJobOrSkip Nothing j
        finalize j
      ackOneWith j r = do
        withDbTransaction $ ackJobOrSkip (encodeJobResult r) j
        finalize j
      ackBatch pairs = do
        let js = map fst pairs
            isAcked acked j = Job.primaryKey j `Set.member` acked
        acked <- withDbTransaction $ do
          ackedSet <- Set.fromList <$> Arb.ackJobsBatch js
          for_ pairs $ \(j, mr) ->
            when (isAcked ackedSet j) $
              traverse_ (\r -> Outcome.storeResult schemaName (encodeJobResult r) j) mr
          pure ackedSet
        let (done, reclaimed) = partition (isAcked acked) js
        unless (null reclaimed) $
          tryLog
            (withJobContextList (logConfig config) reclaimed)
            Info
            "Jobs reclaimed during bulk completion, skipped"
        traverse_ finalize done
      callbacks =
        BatchCallbacks
          { ack = ackOne
          , ackWith = ackOneWith
          , ackAll = \js -> ackBatch (map (\j -> (j, Nothing)) js)
          , ackAllWith = \pairs -> ackBatch (map (second Just) pairs)
          , failRetry = failAs (Retryable . JobRetryableException)
          , failPermanent = failAs (Permanent . JobPermanentException)
          , cancelBranch = failAs (BranchCancel . BranchCancelException)
          , cancelTree = failAs (TreeCancel . TreeCancelException)
          , nack = nackOne
          }
  result <-
    tryAny
      $ withJobsHeartbeat
        hooks
        (jobHeartbeatInterval config)
        (visibilityTimeout config)
        startTime
        jobs
        (logConfig config)
        (heartbeatSignal config)
      $ case mode of
        SingleJobMode handler -> do
          let (job :| _) = jobs
          withDbTransaction $ do
            handlerResult <-
              Trace.withConsumeSpan (tracer config) (consumeSpanAttributes config (pure job)) job $
                runHandlerWithConnection @_ @_ @result handler job
            ackJobOrSkip (encodeJobResult handlerResult) job
          finalize job
        BatchedJobsMode _ handler ->
          Trace.withConsumeSpanBatch (tracer config) (consumeSpanAttributes config jobs) jobs $ handler jobs callbacks
  endTime <- liftIO getCurrentTime
  handled <- liftIO $ readIORef handledRef
  reportBatchOutcome config hooks startTime endTime jobs handled result

-- | Interpret a finished batch: warn when the handler left jobs unfinalized,
-- skip retry for gone or nacked jobs, otherwise fail whatever was not finalized.
reportBatchOutcome
  :: forall m registry payload result
   . (JobOperation m registry payload, MonadUnliftIO m)
  => WorkerConfig m payload result
  -> Job.ObservabilityHooks m payload
  -> UTCTime
  -> UTCTime
  -> NonEmpty (Job.JobRead payload)
  -> Set.Set Int64
  -> Either SomeException ()
  -> m ()
reportBatchOutcome config hooks startTime endTime jobs handled = \case
  Right () ->
    unless (null unhandled) $
      tryLog (withJobContextList (logConfig config) unhandled) Warning "Handler left jobs unfinalized, will reprocess"
  Left e
    | Just (JobStolenException ids) <- fromException e ->
        tryLog batchLog Info $ "Job(s) reclaimed by another worker, skipping retry: " <> ids
    | isJobGoneException e ->
        tryLog batchLog Info "Job(s) no longer available, skipping retry"
    | Just JobNackException <- fromException e -> do
        -- Hand back the attempt the claim consumed for every job the handler
        -- left unfinalized, so the nacked reprocess does not record a failure.
        withDbTransaction $ traverse_ (void . Arb.nackJob) unhandled
        tryLog batchLog Info "Job(s) nacked, will be reprocessed"
    | otherwise -> do
        -- Fail the jobs the handler did not finalize, in a separate transaction.
        events <-
          withDbTransaction $
            traverse
              (\job -> (,) job <$> handleJobFailure config {logConfig = jobLog job} e (jobMaxAtts job) job)
              unhandled
        for_ events $ \(job, event) ->
          traverse_ (fireFailureHooks (jobLog job) hooks startTime endTime job) event
  where
    unhandled = filter (\j -> not (Set.member (Job.primaryKey j) handled)) (toList jobs)
    batchLog = withJobContext (logConfig config) jobs
    jobLog job = withJobContextOne (logConfig config) job
    jobMaxAtts job = fromMaybe Job.defaultMaxAttempts (Job.maxAttempts job)

-- | A rollup parent's immediate child results, keyed by child id, with @Left@
-- for results that failed to decode, plus a map of DLQ'd immediate children.
-- Both are empty for a job with no children.
childResults
  :: (HasArbiterSchema m registry, JobResult result, MonadArbiter m)
  => Job.JobRead payload
  -> m (Map.Map Int64 (Either Text result), Map.Map Int64 T.Text)
childResults job = do
  schemaName <- getSchema
  readChildResults schemaName job

-- | 'childResults' with the child results 'Monoid'-merged (decode failures
-- contribute 'mempty').
mergedChildResults
  :: (HasArbiterSchema m registry, JobResult result, MonadArbiter m, Monoid result)
  => Job.JobRead payload
  -> m (result, Map.Map Int64 T.Text)
mergedChildResults job = do
  (results, dlqFailures) <- childResults job
  pure (mergeChildResults results, dlqFailures)

-- | Fold child results via 'Monoid', treating failures as 'mempty'.
mergeChildResults :: (Monoid a) => Map Int64 (Either Text a) -> a
mergeChildResults = foldMap' fold

-- | Check if an exception indicates the job is gone (stolen or not found).
-- These jobs should not be retried or moved to DLQ.
isJobGoneException :: SomeException -> Bool
isJobGoneException e =
  case (fromException e :: Maybe JobNotFoundException, fromException e :: Maybe JobStolenException) of
    (Just _, _) -> True
    (_, Just _) -> True
    _ -> False

-- | Classify a handler exception into an error message and failure disposition.
--
-- Note: 'JobNotFoundException' and 'JobStolenException' are intercepted by
-- 'isJobGoneException' before reaching 'handleJobFailure', so they never
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

-- | A committed failure and what to tell its hooks.
data FailureEvent
  = -- | Deleted, rather than retried or dead-lettered.
    Cancelled Text
  | Failed Text Outcome.FailureOutcome

-- | Write a job's failure (retry or move to DLQ), yielding what its hooks report.
-- Hooks fire outside the failure transaction.
handleJobFailure
  :: forall m registry payload result
   . ( JobOperation m registry payload
     , MonadUnliftIO m
     )
  => WorkerConfig m payload result
  -> SomeException
  -> Int32
  -> Job.JobRead payload
  -> m (Maybe FailureEvent)
handleJobFailure config e maxAtts job = do
  let (errorMsg, failureKind) = classifyException e
      cfg = logConfig config
      cancelled deleted = if deleted > 0 then Just (Cancelled errorMsg) else Nothing
  schemaName <- getSchema
  case failureKind of
    TreeCancelFailure ->
      -- TreeCancel: delete the entire tree from root down (including this job)
      cancelled <$> Ops.cancelJobTree schemaName (Job.queueName job) (Job.primaryKey job)
    BranchCancelFailure -> do
      -- BranchCancel: cascade-delete the parent + all siblings (including this job).
      -- If no parent, just delete this job.
      let target = fromMaybe (Job.primaryKey job) (Job.parentId job)
      cancelled <$> Ops.cancelJobCascade schemaName (Job.queueName job) target
    _ -> do
      report <-
        Outcome.failJob
          schemaName
          (retryPolicyOf config)
          Nothing
          (failureKind == PermanentFailure)
          maxAtts
          errorMsg
          job
      if Outcome.reportRows report == 0
        then do
          tryLog cfg Warning $ "Job " <> T.pack (show (Job.primaryKey job)) <> " not available to fail"
          pure Nothing
        else pure (Just (Failed errorMsg (Outcome.reportOutcome report)))

-- | Report a committed failure to the job's hooks.
fireFailureHooks
  :: (Job.JobPayload payload, MonadUnliftIO m)
  => LogConfig
  -> Job.ObservabilityHooks m payload
  -> UTCTime
  -> UTCTime
  -> Job.JobRead payload
  -> FailureEvent
  -> m ()
fireFailureHooks cfg hooks startTime endTime job event =
  traverse_ (uncurry (runHook cfg)) $ case event of
    Cancelled msg ->
      [ ("onJobFailure", Job.onJobFailure hooks job msg startTime endTime)
      , ("onJobCancelled", Job.onJobCancelled hooks job msg)
      ]
    Failed msg outcome -> Outcome.failureHookCalls hooks job msg startTime endTime outcome

-- | The retry policy a pool's config runs with.
retryPolicyOf :: WorkerConfig m payload result -> Outcome.RetryPolicy
retryPolicyOf config =
  Outcome.RetryPolicy {Outcome.retryBackoff = backoffStrategy config, Outcome.retryJitter = jitter config}

-- | Refreshes the groups tables, sweeps stale worker registry rows, and moves
-- exhausted jobs to the DLQ. Each op is gated so only one pool runs it per interval.
-- Run this directly for a queue served over HTTP with no handler of its own.
runQueueMaintenance
  :: forall m registry
   . ( Arb.RegistryAdmissionPolicies registry
     , HasArbiterSchema m registry
     , MonadArbiter m
     , MonadUnliftIO m
     , RegistryTables registry
     )
  => LogConfig
  -> Maybe ClaimAdmission
  -- ^ Forces the rate-limit and concurrency maintenance on for a runtime queue,
  -- whose type-level registry declares no policies.
  -> Maybe [Text]
  -- ^ Queues to maintain. See 'maintenanceQueues'.
  -> NominalDiffTime
  -- ^ How often this loop runs.
  -> NominalDiffTime
  -- ^ Abort any single statement that exceeds this.
  -> m ()
runQueueMaintenance logCfg mAdmission mMaintenanceQueues interval stmtTimeout = do
  let intervalSecs = ceiling interval
      registryQueues = registryTableNames (Proxy @registry)
      -- A runtime pool's registry names only its own queue, so the deployment's list wins.
      queues = Set.toList (Set.fromList (fromMaybe registryQueues mMaintenanceQueues))
      pruneInterval = interval * 12
      hasConcurrency = maybe False admitConcurrent mAdmission || not (Set.null (registryConcurrencyPolicies @registry))
      hasRateLimit = maybe False admitRateLimited mAdmission || not (Set.null (registryRateLimitPolicies @registry))
      qtag = gateTag queues
  schemaName <- Arb.getSchema
  let gated :: forall a. Text -> NominalDiffTime -> m a -> m (Maybe a)
      gated = runReaperOp logCfg schemaName stmtTimeout
  forever $ do
    mFailed <- gated ("refresh-all-groups" <> qtag) interval $ Ops.refreshAllGroups schemaName queues
    traverse_ (traverse_ (\queue -> tryLog logCfg Warning $ "Groups refresh failed for queue: " <> queue)) mFailed
    void $ gated "sweep-stale-workers" interval $ Ops.sweepStaleWorkers schemaName
    mSwept <- gated ("sweep-exhausted-jobs" <> qtag) interval $ Ops.sweepExhaustedJobs schemaName queues
    traverse_
      ( \(n, failed) -> do
          traverse_ (\queue -> tryLog logCfg Warning $ "Exhausted-job sweep failed for queue: " <> queue) failed
          when (n > 0) $ tryLog logCfg Warning $ "Reaper moved " <> T.pack (show n) <> " exhausted job(s) to the DLQ"
      )
      mSwept
    when hasRateLimit $
      void $
        gated "prune-rate-limit-buckets" pruneInterval $
          Arb.pruneRateLimitBuckets @m @registry interval
    when hasConcurrency $
      tryAny (Ops.discoverQueues schemaName) >>= \case
        Left e -> warnEx logCfg "Queue registry read failed, skipping the concurrency reconcile" e
        Right discovered -> do
          -- A concurrency key spans queues, so the recount must cover every queue in the schema.
          -- A recount over this process's subset would zero the in-flight the unscanned queues hold.
          void $
            gated "reconcile-concurrency-stale" interval $
              Ops.reconcileConcurrencyCountsIfStale schemaName discovered
          void $
            gated "reconcile-prune-concurrency" pruneInterval $
              Ops.reconcileAndPruneConcurrency schemaName discovered
    threadDelay (intervalSecs * 1_000_000)

-- | A queue set's gate key suffix, order-independent so pools over the same queues share
-- one gate row. Digested past a bound the primary-key index could not hold.
gateTag :: [Text] -> Text
gateTag qs
  | T.length joined <= 128 = "@" <> joined
  | otherwise = "@" <> T.pack (showHex (fnv1a joined) "")
  where
    joined = T.intercalate "," (Set.toList (Set.fromList qs))

-- | FNV-1a over a queue set, for a gate key that is bounded rather than readable.
fnv1a :: Text -> Word64
fnv1a = T.foldl' step 0xcbf29ce484222325
  where
    step h c = (h `xor` fromIntegral (fromEnum c)) * 0x100000001b3

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
runReaperOp logCfg schemaName stmtTimeout task every work = do
  r <- tryAny $ Ops.runGatedBounded schemaName task every stmtTimeout work
  case r of
    Left e -> Nothing <$ warnEx logCfg ("Reaper op failed: " <> task) e
    Right mr -> pure mr

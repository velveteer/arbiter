{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Entry point for running a worker pool that fetches and executes jobs.
module Arbiter.Worker
  ( -- * Running Workers
    runWorkerPool

    -- * Multi-Queue Workers
  , NamedWorkerPool (..)
  , namedWorkerPool
  , runWorkerPools
  , runSelectedWorkerPools
  , getEnabledQueues

    -- * Job Result
  , JobResult (..)

    -- * Re-exports
  , module Arbiter.Worker.Config
  , module Arbiter.Worker.BackoffStrategy
  , module Arbiter.Worker.Logger
  , module Arbiter.Worker.WorkerState

    -- * Cron
  , CronJob (..)
  , OverlapPolicy (..)
  , BackfillPolicy (..)
  , cronJob
  , initCronSchedules
  , overlapPolicyToText
  , overlapPolicyFromText
  ) where

import Arbiter.Core.Exceptions
  ( BranchCancelException (..)
  , JobException (..)
  , JobNotFoundException (..)
  , JobPermanentException (..)
  , JobRetryableException (..)
  , JobStolenException
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
import Control.Exception (SomeException, fromException)
import Control.Monad (forever, replicateM, unless, void, when)
import Control.Monad.Catch (MonadMask)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Aeson (FromJSON, ToJSON, Value, toJSON)
import Data.Aeson qualified as Aeson
import Data.Foldable (toList, traverse_)
import Data.Int (Int32, Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Traversable (for)
import GHC.TypeLits (symbolVal)
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
  , waitAnyCatch
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
import Arbiter.Worker.Logger.Internal (runHook, tryLog, withJobContext)
import Arbiter.Worker.NotificationListener (runMultiChannelListener)
import Arbiter.Worker.Retry (spawnRetried)
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

-- | A worker pool paired with its queue name (derived from the registry).
--
-- @
-- allWorkers =
--   [ namedWorkerPool emailConfig      -- "email_jobs"
--   , namedWorkerPool imageConfig      -- "image_jobs"
--   ]
--
-- main = runWorkerPools (Proxy \@MyRegistry) allWorkers (\\_ -> pure ())
-- @
data NamedWorkerPool m
  = forall registry payload result.
  (JobResult result, QueueOperation m registry payload, RegistryTables registry) =>
  NamedWorkerPool
  { workerPoolName :: Text
  -- ^ Queue name from the type-level registry
  , workerPoolConfig :: WorkerConfig m payload result
  -- ^ The worker configuration
  }

-- | Create a named worker pool, deriving the name from the type-level registry.
namedWorkerPool
  :: forall m registry payload result
   . (JobResult result, QueueOperation m registry payload, RegistryTables registry)
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
   . (MonadMask m, MonadUnliftIO m, RegistryTables registry)
  => Proxy registry
  -> [NamedWorkerPool m]
  -> (TVar WorkerState -> IO ())
  -> m ()
runWorkerPools registry pools setup = do
  sharedState <- liftIO newWorkerState
  liftIO $ setup sharedState
  enabled <- liftIO $ getEnabledQueues "ARBITER_ENABLED_QUEUES" registry
  runSelectedWorkerPools sharedState enabled pools

-- | Run only the worker pools whose names appear in the enabled list.
runSelectedWorkerPools
  :: (MonadMask m, MonadUnliftIO m)
  => TVar WorkerState
  -> [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPools sharedState enabled pools =
  case filter (\(NamedWorkerPool name _) -> name `elem` enabled) pools of
    [] -> pure ()
    selected -> evalContT $ do
      asyncs <- for selected $ \(NamedWorkerPool name cfg) ->
        let cfg' =
              cfg
                { workerStateVar = sharedState
                , logConfig = withPoolContext name (logConfig cfg)
                }
         in ContT $ \k -> Async.withAsync (runWorkerPool cfg') k
      lift $ mapM_ Async.waitCatch asyncs

-- | Inject the pool name into log context. User-supplied pairs come after
-- so they win on key collision.
withPoolContext :: Text -> LogConfig -> LogConfig
withPoolContext poolName lc =
  lc {additionalContext = (("pool" .= poolName) :) <$> additionalContext lc}

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
getEnabledQueues envVar registry = do
  let allQueues = registryTableNames registry
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

-- | Starts a worker pool with a dispatcher and N worker threads.
runWorkerPool
  :: forall m registry payload result
   . ( JobResult result
     , MonadMask m
     , MonadUnliftIO m
     , QueueOperation m registry payload
     , RegistryTables registry
     )
  => WorkerConfig m payload result
  -> m ()
runWorkerPool config = do
  let workerCap = workerCount config
      queueName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))

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
        runDispatcher config workerCap workQueue busyWorkerCount workerFinishedVar dispatcherNotifVar
    workers <-
      replicateM workerCap $
        spawnRetried (workerStateVar config) (logConfig config) "Worker thread" $
          workerLoop config runningJobs workQueue busyWorkerCount workerFinishedVar
    cron <-
      spawnRetried (workerStateVar config) (logConfig config) "Cron scheduler" $
        runCronScheduler (workerStateVar config) (logConfig config) schemaName queueName (cronJobs config)
    reaper <-
      spawnRetried (workerStateVar config) (logConfig config) "Reaper" $
        reaperLoop (logConfig config) (reaperInterval config)

    (_, res) <- waitAnyCatch (dispatcher : reaper : heartbeat : listener : cron : workers)
    case res of
      Left e ->
        lift $ tryLog (logConfig config) Error $ "Thread pool exception: " <> T.pack (show e)
      Right _ -> pure ()

    lift $ shutdownPool config schemaName workQueue busyWorkerCount

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
     , MonadMask m
     , MonadUnliftIO m
     )
  => WorkerConfig m payload result
  -> RunningJobs
  -- ^ Pool-shared map from job id to running handler async.
  -> TBQueue (NonEmpty (Job.JobRead payload))
  -> TVar Int
  -- ^ Busy worker count
  -> TVar Bool
  -- ^ Worker finished signal
  -> m ()
workerLoop config runningJobs workQueue busyCount workerFinishedVar = forever $ mask_ $ do
  -- Mask covers the window between the atomic claim (which increments
  -- busyCount) and entering the finally block that decrements it.
  jobBatch <- atomically $ do
    batch <- readTBQueue workQueue
    modifyTVar' busyCount (+ 1)
    pure batch

  let jobIds = map Job.primaryKey (toList jobBatch)
      claimHook now job =
        runHook (logConfig config) "onJobClaimed" $
          Job.onJobClaimed (observabilityHooks config) job now

  flip
    finally
    ( atomically $ do
        modifyTVar' busyCount (subtract 1)
        modifyTVar' runningJobs $ \m -> foldl' (flip Map.delete) m jobIds
        writeTVar workerFinishedVar True
    )
    $ withJobContext jobBatch
    $ do
      currentTime <- liftIO getCurrentTime
      mapM_ (claimHook currentTime) jobBatch
      result <- withRegisteredJobs runningJobs jobIds (processJobsWithRetry config jobBatch)
      case result of
        Right () -> pure ()
        Left e
          | Just JobForceCancelled <- fromException e ->
              tryLog (logConfig config) Info $
                "Job(s) force-cancelled: " <> T.pack (show jobIds)
          | Just Async.AsyncCancelled <- fromException e -> throwIO e
          | otherwise -> do
              tryLog (logConfig config) Error $ "Worker exception: " <> T.pack (show e)
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
  -> NonEmpty (Job.JobRead payload)
  -> m ()
processJobsWithRetry config jobs = do
  let hooks = observabilityHooks config
      -- Use minimum maxAttempts across all jobs in the batch
      maxAtts = minimum $ map (\job -> fromMaybe (maxAttempts config) (Job.maxAttempts job)) (toList jobs)
  startTime <- liftIO getCurrentTime
  schemaName <- Arb.getSchema
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
      $ case handlerMode config of
        SingleJobMode handler -> withDbTransaction $ do
          let (job :| _) = jobs
          (childResults, dlqFailures) <- readRollupChildResults schemaName job
          handlerResult <- runHandlerWithConnection @_ @_ @result (handler childResults dlqFailures) job
          storeJobResult schemaName job handlerResult
          rowsAffected <- Arb.ackJob job
          when (rowsAffected == 0) $
            throwJobNotFound $
              "Job "
                <> T.pack (show (Job.primaryKey job))
                <> " was reclaimed during processing - rolling back handler transaction"
        BatchedJobsMode _ handler -> withDbTransaction $ do
          void $ runHandlerWithConnection @_ @_ @result handler jobs
          rowsAffected <- Arb.ackJobsBulk (toList jobs)
          when (rowsAffected /= fromIntegral (length jobs)) $
            throwJobNotFound $
              "Expected to ack "
                <> T.pack (show (length jobs))
                <> " jobs but only "
                <> T.pack (show rowsAffected)
                <> " were deleted - rolling back handler transaction"
        ManualJobMode handler -> do
          let (job :| _) = jobs
          (childResults, dlqFailures) <- readRollupChildResults schemaName job
          let completion handlerResult = do
                withDbTransaction $ do
                  storeJobResult schemaName job handlerResult
                  void $ Arb.ackJob job
                fireJobSuccess config hooks job startTime
          handler childResults dlqFailures job completion
        ManualBatchedJobsMode _ handler -> do
          let completion completedJob = do
                void $ Arb.ackJob completedJob
                fireJobSuccess config hooks completedJob startTime
          handler jobs completion
  endTime <- liftIO getCurrentTime
  case result of
    Right () ->
      -- Automatic modes fire onJobSuccess here, after the commit. Manual modes
      -- fire it inside the completion callback the handler invokes.
      case handlerMode config of
        ManualJobMode {} -> pure ()
        ManualBatchedJobsMode {} -> pure ()
        _ ->
          mapM_ (\job -> runHook (logConfig config) "onJobSuccess" $ Job.onJobSuccess hooks job startTime endTime) jobs
    Left e
      | isJobGoneException e ->
          tryLog (logConfig config) Info $
            "Job(s) no longer available, skipping retry: " <> T.pack (show e)
      | otherwise ->
          -- Update all jobs for retry or move to DLQ in a separate transaction
          withDbTransaction $
            mapM_ (handleJobFailure config hooks e maxAtts startTime endTime) jobs

-- | Read child results for a rollup job, empty otherwise.
readRollupChildResults
  :: (JobResult result, MonadArbiter m)
  => Text
  -> Job.JobRead payload
  -> m (Map.Map Int64 (Either Text result), Map.Map Int64 T.Text)
readRollupChildResults schemaName job
  | Job.isRollup job = readChildResults schemaName job
  | otherwise = pure (Map.empty, Map.empty)

-- | Store a job's result for its parent rollup, if it has one.
storeJobResult
  :: (JobResult result, MonadArbiter m)
  => Text
  -> Job.JobRead payload
  -> result
  -> m ()
storeJobResult schemaName job result =
  case (Job.parentId job, encodeJobResult result) of
    (Just pid, Just val) ->
      void $ Ops.insertResult schemaName (Job.queueName job) pid (Job.primaryKey job) val
    _ -> pure ()

-- | Fire onJobSuccess for a job that just completed, stamping the end time now.
fireJobSuccess
  :: (Job.JobPayload payload, MonadUnliftIO m)
  => WorkerConfig m payload result
  -> Job.ObservabilityHooks m payload
  -> Job.JobRead payload
  -> UTCTime
  -> m ()
fireJobSuccess config hooks job startTime = do
  endTime <- liftIO getCurrentTime
  runHook (logConfig config) "onJobSuccess" $ Job.onJobSuccess hooks job startTime endTime

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

-- | Handle failure for a single job (retry or move to DLQ).
handleJobFailure
  :: forall m registry payload result
   . ( JobOperation m registry payload
     , MonadUnliftIO m
     )
  => WorkerConfig m payload result
  -> Job.ObservabilityHooks m payload
  -> SomeException
  -> Int32
  -> UTCTime
  -> UTCTime
  -> Job.JobRead payload
  -> m ()
handleJobFailure config hooks e maxAtts startTime endTime job = do
  let (errorMsg, failureKind) = classifyException e
      cfg = logConfig config
  schemaName <- getSchema
  case failureKind of
    TreeCancelFailure -> do
      -- TreeCancel: delete the entire tree from root down (including this job)
      deleted <- Ops.cancelJobTree schemaName (Job.queueName job) (Job.primaryKey job)
      when (deleted > 0) $
        runHook cfg "onJobFailure" $
          Job.onJobFailure hooks job errorMsg startTime endTime
    BranchCancelFailure -> do
      -- BranchCancel: cascade-delete the parent + all siblings (including this job).
      -- If no parent, just delete this job.
      let target = fromMaybe (Job.primaryKey job) (Job.parentId job)
      deleted <- Ops.cancelJobCascade schemaName (Job.queueName job) target
      when (deleted > 0) $
        runHook cfg "onJobFailure" $
          Job.onJobFailure hooks job errorMsg startTime endTime
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
            then
              tryLog cfg Warning $
                "Job " <> T.pack (show (Job.primaryKey job)) <> " not available for moving to DLQ"
            else do
              -- Successfully moved to DLQ
              runHook cfg "onJobFailure" $ Job.onJobFailure hooks job errorMsg startTime endTime
              runHook cfg "onJobFailedAndMovedToDLQ" $ Job.onJobFailedAndMovedToDLQ hooks errorMsg job
      | otherwise -> do
          -- Retry with configured backoff strategy and jitter
          let baseDelay = calculateBackoff (backoffStrategy config) (Job.attempts job)
          backoffSecs <- liftIO $ applyJitter (jitter config) baseDelay
          rowsAffected <- Arb.updateJobForRetry backoffSecs errorMsg job
          if rowsAffected == 0
            then
              tryLog cfg Warning $
                "Job " <> T.pack (show (Job.primaryKey job)) <> " not available for retry"
            else do
              -- Successfully updated for retry
              runHook cfg "onJobFailure" $ Job.onJobFailure hooks job errorMsg startTime endTime
              runHook cfg "onJobRetry" $ Job.onJobRetry hooks job backoffSecs

-- | Refreshes the groups tables (schema-wide) and sweeps stale worker
-- registry rows (schema-wide). Both gated via 'Ops.runGated' so only one
-- pool runs each task per interval.
reaperLoop
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m, MonadUnliftIO m, RegistryTables registry)
  => LogConfig
  -> NominalDiffTime
  -- ^ How often this loop runs.
  -> m ()
reaperLoop logCfg interval = do
  let intervalSecs = ceiling interval
      queues = registryTableNames (Proxy @registry)
  schemaName <- Arb.getSchema
  forever $ do
    mFailed <- Ops.runGated schemaName "refresh-all-groups" interval $ Ops.refreshAllGroups schemaName queues
    traverse_ (traverse_ (\queue -> tryLog logCfg Warning $ "Groups refresh failed for queue: " <> queue)) mFailed
    void $ Ops.runGated schemaName "sweep-stale-workers" interval $ Ops.sweepStaleWorkers schemaName
    threadDelay (intervalSecs * 1_000_000)

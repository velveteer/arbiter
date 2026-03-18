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
  , cronJob
  , initCronSchedules
  , overlapPolicyToText
  , overlapPolicyFromText
  ) where

import Arbiter.Core.Codec (Col (..), pval)
import Arbiter.Core.Exceptions
  ( BranchCancelException (..)
  , JobException (..)
  , JobPermanentException (..)
  , JobRetryableException (..)
  , ParsingException (..)
  , TreeCancelException (..)
  , throwJobNotFound
  )
import Arbiter.Core.HasArbiterSchema (HasArbiterSchema (..))
import Arbiter.Core.HighLevel (JobOperation, QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables (..), TableForPayload)
import Control.Exception (SomeException, fromException)
import Control.Monad (forever, replicateM, void, when)
import Control.Monad.Catch (MonadMask)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Aeson (FromJSON, ToJSON, Value, toJSON)
import Data.Aeson qualified as Aeson
import Data.Foldable (toList)
import Data.Int (Int32, Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Traversable (for)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import GHC.TypeLits (symbolVal)
import System.Directory (removeFile)
import System.Environment (lookupEnv)
import UnliftIO
  ( MonadUnliftIO
  , atomically
  , bracket
  , checkSTM
  , finally
  , isAsyncException
  , isEmptyTBQueue
  , lengthTBQueue
  , mask
  , modifyTVar'
  , newTBQueueIO
  , newTVarIO
  , readTBQueue
  , readTVar
  , throwIO
  , tryAny
  , trySyncOrAsync
  , waitAnyCatch
  , withAsync
  , writeTVar
  )
import UnliftIO.Async (race)
import UnliftIO.Async qualified as Async
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.Concurrent qualified as Conc
import UnliftIO.MVar qualified as MVar
import UnliftIO.STM (TBQueue, TVar)

import Arbiter.Worker.BackoffStrategy
import Arbiter.Worker.Config
import Arbiter.Worker.Cron
  ( CronJob (..)
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
import Arbiter.Worker.Logger.Internal (logMessage, runHook, withJobContext)
import Arbiter.Worker.Retry (retryOnException)
import Arbiter.Worker.WorkerState

-- ---------------------------------------------------------------------------
-- Job Result
-- ---------------------------------------------------------------------------

-- | Handler result types. @()@ is fire-and-forget; any @(ToJSON a, FromJSON a)@
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

-- | A worker pool bundled with its queue name from the type-level registry.
--
-- The queue name is derived from the registry at compile time, ensuring it
-- stays in sync with the type-level definition.
--
-- Use 'namedWorkerPool' to construct these, then 'runSelectedWorkerPools'
-- to run only the ones matching a runtime configuration.
--
-- Example:
--
-- @
-- allWorkers :: [NamedWorkerPool (SimpleDb MyRegistry IO)]
-- allWorkers =
--   [ namedWorkerPool emailConfig      -- name derived from registry: "email_jobs"
--   , namedWorkerPool imageConfig      -- name derived from registry: "image_jobs"
--   , namedWorkerPool notifConfig      -- name derived from registry: "notifications"
--   ]
--
-- main = runWorkerPools (Proxy \@MyRegistry) allWorkers (\\_ -> pure ())
-- @
data NamedWorkerPool m
  = forall registry payload result.
  (JobResult result, QueueOperation m registry payload) =>
  NamedWorkerPool
  { workerPoolName :: Text
  -- ^ Queue name from the type-level registry
  , workerPoolConfig :: WorkerConfig m payload result
  -- ^ The worker configuration
  }

-- | Create a named worker pool, deriving the name from the type-level registry.
namedWorkerPool
  :: forall m registry payload result
   . (JobResult result, QueueOperation m registry payload)
  => WorkerConfig m payload result
  -> NamedWorkerPool m
namedWorkerPool cfg =
  NamedWorkerPool
    { workerPoolName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
    , workerPoolConfig = cfg
    }

-- | Run worker pools with shared state for coordinated shutdown.
--
-- Creates shared state, passes it to setup action (for signal handlers),
-- then runs pools. Reads enabled queues from ARBITER_ENABLED_QUEUES.
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
      asyncs <- for selected $ \(NamedWorkerPool _ cfg) ->
        let cfg' = cfg {workerStateVar = sharedState}
         in ContT $ \k -> Async.withAsync (runWorkerPool cfg') k
      lift $ mapM_ Async.waitCatch asyncs

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
    Just val
      | T.null (T.strip $ T.pack val) -> pure allQueues
      | otherwise -> do
          let requested = map T.strip $ T.splitOn "," $ T.pack val
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
     )
  => WorkerConfig m payload result
  -> m ()
runWorkerPool config = do
  let workerCap = workerCount config
      mLiveness = livenessConfig config
      mLivenessSignal = fmap livenessSignal mLiveness

  -- Create shared state
  workQueue <- liftIO $ newTBQueueIO (fromIntegral workerCap)
  busyWorkerCount <- liftIO $ newTVarIO 0
  workerFinishedVar <- liftIO $ newTVarIO False

  evalContT $ do
    -- Spawn liveness probe
    liveness <-
      case mLiveness of
        Just lc ->
          fmap pure <$> ContT . withAsync $
            refreshLiveness (logConfig config) (livenessPath lc) (livenessSignal lc) (livenessInterval lc)
              `finally` void (tryAny . liftIO $ removeFile (livenessPath lc))
        Nothing -> pure []

    -- Spawn dispatcher
    dispatcher <-
      ContT . withAsync $
        runDispatcher config workerCap workQueue busyWorkerCount mLivenessSignal workerFinishedVar

    -- Spawn workers
    workers <-
      replicateM workerCap . ContT . withAsync $
        workerLoop config workQueue busyWorkerCount workerFinishedVar

    -- Spawn cron scheduler (only when cronJobs is non-empty)
    cron <-
      case cronJobs config of
        [] -> pure []
        jobs -> do
          sch <- lift getSchema
          pure
            <$> ( ContT . withAsync $
                    retryOnException (workerStateVar config) (logConfig config) "Cron scheduler" $
                      bracket
                        (liftIO $ connectPostgreSQL (connStr config))
                        (liftIO . close)
                        (\conn -> runCronScheduler conn (logConfig config) sch jobs)
                )

    -- Spawn groups table reaper (corrects drift in job_count, min_priority, min_id)
    reaper <-
      pure
        <$> ( ContT . withAsync $
                retryOnException (workerStateVar config) (logConfig config) "Group reaper" $
                  groupReaperLoop config (groupReaperInterval config)
            )

    -- Wait for any thread to exit (normal or exceptional)
    (_, res) <- waitAnyCatch (dispatcher : cron ++ reaper ++ liveness ++ workers)

    case res of
      Left e ->
        -- A thread crashed
        lift . void . tryAny . liftIO $ logMessage (logConfig config) Error $ T.pack $ "Thread pool exception: " <> show e
      Right _ ->
        -- A thread exited normally
        pure ()

    -- Set shutdown state if not already set
    shutdownWorker config

    -- Graceful shutdown: drain the queue with optional timeout
    lift . void . tryAny . liftIO $
      logMessage (logConfig config) Info (T.pack "Starting graceful shutdown. Draining in-flight jobs...")

    -- Wait for work queue to be empty, with optional timeout
    let waitForDrain = liftIO . atomically $ do
          qEmpty <- isEmptyTBQueue workQueue
          checkSTM qEmpty
          busy <- readTVar busyWorkerCount
          checkSTM (busy == 0)

    drainResult <- case gracefulShutdownTimeout config of
      Nothing -> do
        -- No timeout: wait indefinitely, log progress every 10 seconds
        let drainLoop = do
              drainOrTick <- lift $ race (liftIO $ threadDelay 10_000_000) waitForDrain
              case drainOrTick of
                Right () -> pure ()
                Left () -> do
                  (busy, qLen) <-
                    liftIO . atomically $
                      (,)
                        <$> readTVar busyWorkerCount
                        <*> (fromIntegral <$> lengthTBQueue workQueue)
                  lift . void . tryAny . liftIO $
                    logMessage (logConfig config) Info $
                      T.pack $
                        "Graceful shutdown: waiting for "
                          <> show (busy :: Int)
                          <> " busy worker(s), "
                          <> show (qLen :: Int)
                          <> " job(s) in queue..."
                  drainLoop
        drainLoop
        pure (Right ())
      Just timeoutSecs -> do
        -- Race between draining and timeout
        let timeoutMicros = ceiling (timeoutSecs * 1_000_000)
        lift $ race (liftIO $ threadDelay timeoutMicros) waitForDrain

    case drainResult of
      Right () ->
        lift . void . tryAny . liftIO $
          logMessage (logConfig config) Info (T.pack "All workers are now idle. Graceful shutdown complete.")
      Left () ->
        lift . void . tryAny . liftIO $
          logMessage (logConfig config) Warning (T.pack "Graceful shutdown timed out. Some jobs may still be in-flight.")

-- | Periodically writes to a file to signal liveness.
--
-- Requires both a signal (from dispatcher or heartbeat) and a minimum delay
-- before writing — proof of work, rate-limited. If neither the dispatcher
-- nor heartbeat signals (e.g., broken DB connection, deadlocked dispatcher),
-- the file goes stale and the process should be restarted.
refreshLiveness :: (MonadUnliftIO m) => LogConfig -> FilePath -> MVar.MVar () -> Int -> m ()
refreshLiveness logCfg healthcheckPath livenessMVar n = forever $ do
  result <- tryAny $ liftIO $ writeFile healthcheckPath ""
  case result of
    Left e ->
      void . tryAny . liftIO $
        logMessage logCfg Error $
          "Liveness probe write failed: " <> T.pack (show e)
    Right () -> pure ()
  Async.concurrently_ (MVar.takeMVar livenessMVar) (Conc.threadDelay $ n * 1_000_000)

-- | Main loop for a single worker thread.
workerLoop
  :: forall m registry payload result
   . ( JobOperation m registry payload
     , JobResult result
     , MonadMask m
     , MonadUnliftIO m
     )
  => WorkerConfig m payload result
  -> TBQueue (NonEmpty (Job.JobRead payload))
  -> TVar Int
  -- ^ Busy worker count
  -> TVar Bool
  -- ^ Worker finished signal
  -> m ()
workerLoop config workQueue busyCount workerFinishedVar = forever $ mask $ \unmask -> do
  -- Read batch from queue (singleton in single mode, multiple in batched mode)
  jobBatch <- atomically $ do
    batch <- readTBQueue workQueue
    modifyTVar' busyCount (+ 1)
    pure batch

  flip
    finally
    ( atomically $ do
        modifyTVar' busyCount (subtract 1)
        writeTVar workerFinishedVar True
    )
    $ withJobContext jobBatch
    $ do
      currentTime <- liftIO getCurrentTime
      mapM_
        (\job -> runHook (logConfig config) "onJobClaimed" $ Job.onJobClaimed (observabilityHooks config) job currentTime)
        jobBatch
      -- Unmask for actual job processing (allow cancellation during work)
      result <- trySyncOrAsync $ unmask $ processJobsWithRetry config jobBatch
      liftIO $ handleWorkerException (logConfig config) result

-- | Common exception handling for worker loops
handleWorkerException :: LogConfig -> Either SomeException () -> IO ()
handleWorkerException cfg result =
  case result of
    Right () -> pure ()
    Left (e :: SomeException) -> do
      let msg = T.pack $ "Worker exception: " <> show e
      void . tryAny $ logMessage cfg Error msg
      when (isAsyncException e) $ throwIO e
      threadDelay 2_000_000

-- | Read and decode child results for a rollup finalizer.
--
-- Decode failures are represented as @Left@ entries in the result map,
-- allowing the handler to process valid results while seeing failures.
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
  result <-
    tryAny
      $ withJobsHeartbeat
        hooks
        (heartbeatInterval config)
        (visibilityTimeout config)
        startTime
        jobs
        (logConfig config)
        (fmap livenessSignal (livenessConfig config))
      $ if useWorkerTransaction config
        then withDbTransaction $ do
          schemaName <- Arb.getSchema
          case transactionTimeout config of
            Just timeout -> do
              let timeoutMs = ceiling (timeout * 1000) :: Int64
              void $ executeStatement "SET LOCAL statement_timeout = ?" [pval CInt8 timeoutMs]
            Nothing -> pure ()
          case handlerMode config of
            SingleJobMode handler -> do
              let (job :| _) = jobs
              (childResults, dlqFailures) <-
                if Job.isRollup job
                  then readChildResults schemaName job
                  else pure (Map.empty, Map.empty)
              handlerResult <- (runHandlerWithConnection (handler childResults dlqFailures) job :: m result)
              case (Job.parentId job, encodeJobResult handlerResult) of
                (Just pid, Just val) ->
                  void $ Ops.insertResult schemaName (Job.queueName job) pid (Job.primaryKey job) val
                _ -> pure ()
              rowsAffected <- Arb.ackJob job
              when (rowsAffected == 0) $
                throwJobNotFound $
                  "Job "
                    <> T.pack (show (Job.primaryKey job))
                    <> " was reclaimed during processing - rolling back handler transaction"
            BatchedJobsMode _ handler -> do
              void (runHandlerWithConnection handler jobs :: m result)
              rowsAffected <- Arb.ackJobsBulk (toList jobs)
              when (rowsAffected /= fromIntegral (length jobs)) $
                throwJobNotFound $
                  "Expected to ack "
                    <> T.pack (show (length jobs))
                    <> " jobs but only "
                    <> T.pack (show rowsAffected)
                    <> " were deleted - rolling back handler transaction"
        else do
          -- Manual mode: no transaction, no automatic acking
          schemaName' <- Arb.getSchema
          case handlerMode config of
            SingleJobMode handler -> do
              let (job :| _) = jobs
              (childResults, dlqFailures) <-
                if Job.isRollup job
                  then readChildResults schemaName' job
                  else pure (Map.empty, Map.empty)
              void (runHandlerWithConnection (handler childResults dlqFailures) job :: m result)
            BatchedJobsMode _ handler ->
              void (runHandlerWithConnection handler jobs :: m result)
  endTime <- liftIO getCurrentTime
  case result of
    Right () ->
      -- Success: only call onJobSuccess in automatic transaction mode
      -- In manual mode, users are responsible for acking and success observability
      when (useWorkerTransaction config) $
        mapM_ (\job -> runHook (logConfig config) "onJobSuccess" $ Job.onJobSuccess hooks job startTime endTime) jobs
    Left e
      | isJobGoneException e ->
          void . tryAny . liftIO $
            logMessage (logConfig config) Info $
              "Job(s) no longer available, skipping retry: " <> T.pack (show e)
      | otherwise ->
          -- Update all jobs for retry or move to DLQ in a separate transaction
          withDbTransaction $
            mapM_ (handleJobFailure config hooks e maxAtts startTime endTime) jobs

-- | Check if an exception indicates the job is gone (stolen or not found).
-- These jobs should not be retried or moved to DLQ.
isJobGoneException :: SomeException -> Bool
isJobGoneException e = case fromException e of
  Just (JobNotFound _) -> True
  Just (JobStolen _) -> True
  _ -> False

-- | Classify a handler exception into an error message and failure disposition.
--
-- Note: 'JobNotFound' and 'JobStolen' are intercepted by 'isJobGoneException'
-- before reaching 'handleJobFailure', so they never arrive here.
data FailureKind = RetryFailure | PermanentFailure | TreeCancelFailure | BranchCancelFailure
  deriving stock (Eq)

classifyException :: SomeException -> (T.Text, FailureKind)
classifyException e = case fromException e of
  Just (Retryable (JobRetryableException msg)) -> (msg, RetryFailure)
  Just (Permanent (JobPermanentException msg)) -> (msg, PermanentFailure)
  Just (TreeCancel (TreeCancelException msg)) -> (msg, TreeCancelFailure)
  Just (BranchCancel (BranchCancelException msg)) -> (msg, BranchCancelFailure)
  Just (ParseFailure (ParsingException msg)) -> (msg, PermanentFailure)
  _ -> (T.pack $ show e, RetryFailure) -- Unknown or non-JobException: treat as retryable

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
  case failureKind of
    TreeCancelFailure -> do
      -- TreeCancel: delete the entire tree from root down (including this job)
      schemaName <- getSchema
      deleted <- Ops.cancelJobTree schemaName (Job.queueName job) (Job.primaryKey job)
      when (deleted > 0) $
        runHook cfg "onJobFailure" $
          Job.onJobFailure hooks job errorMsg startTime endTime
    BranchCancelFailure -> do
      -- BranchCancel: cascade-delete the parent + all siblings (including this job).
      -- If no parent, just delete this job.
      schemaName <- getSchema
      let target = maybe (Job.primaryKey job) id (Job.parentId job)
      deleted <- Ops.cancelJobCascade schemaName (Job.queueName job) target
      when (deleted > 0) $
        runHook cfg "onJobFailure" $
          Job.onJobFailure hooks job errorMsg startTime endTime
    _
      | failureKind == PermanentFailure || Job.attempts job >= maxAtts -> do
          -- Snapshot into parent_state before DLQ move (survives CASCADE delete).
          -- Merges old snapshot so repeated DLQ round-trips don't lose data.
          when (Job.isRollup job) $ do
            schemaName <- getSchema
            (results, failures, mSnapshot, _dlqFailures) <-
              Ops.readChildResultsRaw schemaName (Job.queueName job) (Job.primaryKey job)
            let merged = Ops.mergeRawChildResults results failures mSnapshot
            when (not (Map.null merged)) $
              void $
                Ops.persistParentState schemaName (Job.queueName job) (Job.primaryKey job) (toJSON merged)
          -- Permanent failure or max attempts reached - move to DLQ
          rowsAffected <- Arb.moveToDLQ errorMsg job
          if rowsAffected == 0
            then
              void . tryAny . liftIO $
                logMessage cfg Warning $
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
              void . tryAny . liftIO $
                logMessage cfg Warning $
                  "Job " <> T.pack (show (Job.primaryKey job)) <> " not available for retry"
            else do
              -- Successfully updated for retry
              runHook cfg "onJobFailure" $ Job.onJobFailure hooks job errorMsg startTime endTime
              runHook cfg "onJobRetry" $ Job.onJobRetry hooks job backoffSecs

groupReaperLoop
  :: forall m registry payload result
   . ( MonadUnliftIO m
     , QueueOperation m registry payload
     )
  => WorkerConfig m payload result
  -> NominalDiffTime
  -> m ()
groupReaperLoop _config interval = do
  let intervalSecs = ceiling interval
  Arb.refreshGroups @m @registry @payload intervalSecs
  forever $ do
    liftIO $ threadDelay (intervalSecs * 1_000_000)
    Arb.refreshGroups @m @registry @payload intervalSecs

{-# LANGUAGE OverloadedStrings #-}

-- | Configuration types for the arbiter worker pool.
module Arbiter.Worker.Config
  ( -- * Worker Configuration
    WorkerConfig (..)
  , defaultWorkerConfig
  , defaultBatchedWorkerConfig
  , defaultRollupWorkerConfig
  , defaultManualWorkerConfig
  , defaultManualRollupWorkerConfig
  , defaultManualBatchedWorkerConfig
  , singleJobMode
  , mergedRollupHandler
  , manualJobMode
  , manualRollupHandler
  , mergeChildResults
  , HandlerMode (..)

    -- * Manual Completion
  , JobCompletion
  , BatchCompletion

    -- * Worker State
  , WorkerState (..)
  , shutdownWorker
  , getWorkerState
  , readEffectiveState
  ) where

import Arbiter.Core.Job.Types (JobRead, ObservabilityHooks, defaultObservabilityHooks)
import Arbiter.Core.MonadArbiter (BatchedJobHandler, JobHandler, MonadArbiter)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (Value, (.=))
import Data.ByteString (ByteString)
import Data.Foldable (fold)
import Data.Int (Int32, Int64)
import Data.List.NonEmpty (NonEmpty)
import Data.Map.Strict (Map)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Data.UUID (UUID, toString)
import Data.UUID.V4 qualified as UUID
import Network.HostName (getHostName)
import System.Directory (getTemporaryDirectory)
import UnliftIO.STM (TMVar, TVar, newEmptyTMVarIO, newTVarIO)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.BackoffStrategy (BackoffStrategy, Jitter (..), exponentialBackoff)
import Arbiter.Worker.Cron (CronJob)
import Arbiter.Worker.Logger (LogConfig (..), defaultLogConfig)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | Completion callback handed to a manual single-job handler. Apply it to the
-- job's result to store the result, ack the job, and fire onJobSuccess. Call it
-- once. Leaving it uncalled retries the job.
type JobCompletion m result = result -> m ()

-- | Completion callback handed to a manual batched handler. Apply it to a job
-- in the batch to ack it and fire onJobSuccess. Call once per job you finish.
-- Jobs left uncompleted are retried.
type BatchCompletion m payload = JobRead payload -> m ()

-- | Handler type and claiming strategy.
--
-- __Single vs Batched Mode__
--
-- * __Single__: Processes one job at a time. Failures are independent. Handler receives @JobRead payload@.
--
-- * __Batched__: Claims and processes multiple jobs from the same group together.
--   All-or-nothing: if any job in batch fails, entire batch rolls back (automatic mode)
--   or is retried together. Uses minimum maxAttempts across batch.
--   Handler receives @NonEmpty (JobRead payload)@.
--
-- __Automatic vs Manual Mode__
--
-- * __Automatic__ ('SingleJobMode', 'BatchedJobsMode'): the worker runs the
--   handler in a transaction, stores the result, acks, and fires onJobSuccess.
--
-- * __Manual__ ('ManualJobMode', 'ManualBatchedJobsMode'): the handler runs
--   without a worker transaction and is handed a completion callback. Calling
--   it stores the result (single only), acks, and fires onJobSuccess. Jobs left
--   uncompleted are retried.
data HandlerMode m payload result
  = -- | Claim 1 job per group. Handler receives a map of immediate child
    -- results (with @Left@ for decode failures) and a map of immediate
    -- DLQ'd children. Both are empty for jobs with no children.
    SingleJobMode (Map Int64 (Either Text result) -> Map Int64 Text -> JobHandler m payload result)
  | -- | Batched mode: claim up to N jobs per group, handler receives batch.
    -- Has no rollup awareness. Use 'SingleJobMode' for rollup parents.
    BatchedJobsMode Int (BatchedJobHandler m payload result)
  | -- | Manual single-job mode. Like 'SingleJobMode' but the handler decides
    -- when to finalize via the 'JobCompletion' callback. No worker transaction.
    ManualJobMode
      (Map Int64 (Either Text result) -> Map Int64 Text -> JobRead payload -> JobCompletion m result -> m ())
  | -- | Manual batched mode. The handler acks each job individually via the
    -- 'BatchCompletion' callback. No rollup awareness. No worker transaction.
    ManualBatchedJobsMode Int (NonEmpty (JobRead payload) -> BatchCompletion m payload -> m ())

-- | Configuration for a worker pool.
data WorkerConfig m payload result = WorkerConfig
  { connStr :: ByteString
  -- ^ PostgreSQL connection string (used for LISTEN/NOTIFY).
  , workerCount :: Int
  -- ^ Number of concurrent worker threads.
  , handlerMode :: HandlerMode m payload result
  -- ^ Job handler and claiming strategy. Set by the @default*WorkerConfig@ helpers.
  , pollInterval :: NominalDiffTime
  -- ^ Cadence floor in seconds for the dispatcher poll.
  -- Default: 5.
  , visibilityTimeout :: NominalDiffTime
  -- ^ How long a claimed job stays invisible to other workers.
  -- Must be greater than 'jobHeartbeatInterval'. Default: 60.
  , jobHeartbeatInterval :: NominalDiffTime
  -- ^ Interval for extending a job's visibility timeout during processing.
  -- Must be less than 'visibilityTimeout' to prevent job reclaim. Default: 30.
  , workerHeartbeatInterval :: NominalDiffTime
  -- ^ Cadence for bumping @arbiter_workers.last_heartbeat@, the optional
  -- liveness file, and reconciling pause state from the DB. Must be well below
  -- 'workerStaleThreshold'. Default: 10.
  , maxAttempts :: Int32
  -- ^ Max retries before moving to DLQ (used when job's maxAttempts is Nothing).
  -- Default: 10.
  , backoffStrategy :: BackoffStrategy
  -- ^ Retry backoff strategy. Default: exponential with base 2, max 1048576 seconds.
  , jitter :: Jitter
  -- ^ Jitter strategy for retry delays. Default: 'EqualJitter'.
  , observabilityHooks :: ObservabilityHooks m payload
  -- ^ Callbacks for metrics or tracing. Default: no-op hooks.
  , workerStateVar :: TVar WorkerState
  -- ^ Run/shutdown lifecycle. Pause is tracked separately in 'pauseVar'.
  -- Shared across pools in multi-pool setups.
  , pauseVar :: TVar Bool
  -- ^ Per-pool pause flag.
  , livenessFile :: Maybe FilePath
  -- ^ When set, the heartbeat loop touches this file at the
  -- 'workerHeartbeatInterval` cadence. Useful for file-based liveness probes.
  -- Default: @\/tmp\/arbiter-worker-\<workerId\>@.
  , gracefulShutdownTimeout :: Maybe NominalDiffTime
  -- ^ Maximum time in __seconds__ to wait for in-flight jobs during graceful
  -- shutdown. If @Nothing@, waits indefinitely. If @Just n@, force-exits after
  -- n seconds. Default: @Just 30@.
  , logConfig :: LogConfig
  -- ^ Logging configuration. Arbiter outputs structured JSON logs with job
  -- context automatically included. Use this to control log level, destination,
  -- and inject additional context (e.g., trace IDs). Default: Info level to stdout.
  , cronJobs :: [CronJob payload]
  -- ^ Cron schedules. The worker pool spawns a scheduler
  -- thread that inserts jobs on cron expressions. The @cron_schedules@
  -- table is consulted for runtime overrides.
  -- Default: @[]@.
  , reaperInterval :: NominalDiffTime
  -- ^ How often the reaper runs. Default: @300@ (5 minutes).
  , workerId :: UUID
  -- ^ Identity for this pool. Auto-minted by 'defaultWorkerConfig'.
  -- Note: this is not a stable identifier by default,
  -- i.e. it will not persist across worker restarts.
  , workerHost :: Maybe Text
  -- ^ Hostname recorded in the worker registry. Default: auto-generated.
  , workerMetadata :: Maybe Value
  -- ^ Arbitrary JSONB metadata for the worker registry row (image tag,
  -- git SHA, deploy id, etc.). Default: 'Nothing'.
  , workerStaleThreshold :: NominalDiffTime
  -- ^ Workers whose @last_heartbeat@ is older than this are swept from the
  -- registry by 'reaperInterval'. Must be well above the heartbeat cadence
  -- ('workerHeartbeatInterval', or 'jobHeartbeatInterval' while busy).
  -- Default: @300@ (5 minutes).
  , heartbeatSignal :: TMVar ()
  -- ^ Worker-level proof-of-work signal pulsed by the dispatcher and per-job heartbeats.
  }

-- | Create a t'WorkerConfig' with default settings.
defaultWorkerConfig
  :: (MonadArbiter n, MonadIO m)
  => ByteString
  -- ^ Connection string
  -> Int
  -- ^ Worker count
  -> JobHandler n payload result
  -> m (WorkerConfig n payload result)
defaultWorkerConfig connStrVal workerCnt handler =
  mkDefaultConfig connStrVal workerCnt (singleJobMode handler)

-- | Create a t'WorkerConfig' for batched job processing.
--
-- Like 'defaultWorkerConfig' but for handlers that process multiple jobs at once.
defaultBatchedWorkerConfig
  :: (MonadArbiter n, MonadIO m)
  => ByteString
  -- ^ Connection string
  -> Int
  -- ^ Worker count
  -> Int
  -- ^ Batch size (max jobs per group to claim together)
  -> BatchedJobHandler n payload result
  -> m (WorkerConfig n payload result)
defaultBatchedWorkerConfig connStrVal workerCnt batchSize handler =
  mkDefaultConfig connStrVal workerCnt (BatchedJobsMode batchSize handler)

-- | Create a t'WorkerConfig' for rollup parents (intermediate or root). See 'mergedRollupHandler'.
defaultRollupWorkerConfig
  :: (MonadArbiter n, MonadIO m, Monoid result)
  => ByteString
  -- ^ Connection string
  -> Int
  -- ^ Worker count
  -> (result -> Map Int64 Text -> JobHandler n payload result)
  -> m (WorkerConfig n payload result)
defaultRollupWorkerConfig connStrVal workerCnt handler =
  mkDefaultConfig connStrVal workerCnt (mergedRollupHandler handler)

-- | Create a t'WorkerConfig' whose handler finalizes its job manually. The
-- handler is handed a 'JobCompletion' callback and runs without a worker
-- transaction. See 'manualJobMode'.
defaultManualWorkerConfig
  :: (MonadArbiter n, MonadIO m)
  => ByteString
  -- ^ Connection string
  -> Int
  -- ^ Worker count
  -> (JobRead payload -> JobCompletion n result -> n ())
  -> m (WorkerConfig n payload result)
defaultManualWorkerConfig connStrVal workerCnt handler =
  mkDefaultConfig connStrVal workerCnt (manualJobMode handler)

-- | Create a t'WorkerConfig' for manual rollup parents. Child results are
-- 'Monoid'-merged, like 'defaultRollupWorkerConfig'. See 'manualRollupHandler'.
defaultManualRollupWorkerConfig
  :: (MonadArbiter n, MonadIO m, Monoid result)
  => ByteString
  -- ^ Connection string
  -> Int
  -- ^ Worker count
  -> (result -> Map Int64 Text -> JobRead payload -> JobCompletion n result -> n ())
  -> m (WorkerConfig n payload result)
defaultManualRollupWorkerConfig connStrVal workerCnt handler =
  mkDefaultConfig connStrVal workerCnt (manualRollupHandler handler)

-- | Create a t'WorkerConfig' for manual batched processing. The handler acks
-- each job via the 'BatchCompletion' callback. See 'ManualBatchedJobsMode'.
defaultManualBatchedWorkerConfig
  :: (MonadArbiter n, MonadIO m)
  => ByteString
  -- ^ Connection string
  -> Int
  -- ^ Worker count
  -> Int
  -- ^ Batch size (max jobs per group to claim together)
  -> (NonEmpty (JobRead payload) -> BatchCompletion n payload -> n ())
  -> m (WorkerConfig n payload result)
defaultManualBatchedWorkerConfig connStrVal workerCnt batchSize handler =
  mkDefaultConfig connStrVal workerCnt (ManualBatchedJobsMode batchSize handler)

-- | Handler that ignores child results. Use for regular jobs and leaf children.
singleJobMode :: JobHandler m payload result -> HandlerMode m payload result
singleJobMode handler = SingleJobMode (\_ _ -> handler)

-- | Handler for rollup parents (intermediate or root). Child results are
-- 'Monoid'-merged (decode failures contribute 'mempty'). Both args are empty
-- for jobs with no children. Use 'SingleJobMode' to inspect per-child decode
-- failures.
mergedRollupHandler
  :: (Monoid result) => (result -> Map Int64 Text -> JobHandler m payload result) -> HandlerMode m payload result
mergedRollupHandler handler = SingleJobMode $ \results dlqFailures -> handler (mergeChildResults results) dlqFailures

-- | Manual handler that ignores child results. The completion callback acks the
-- job and fires onJobSuccess. Use for regular jobs and leaf children.
manualJobMode :: (JobRead payload -> JobCompletion m result -> m ()) -> HandlerMode m payload result
manualJobMode handler = ManualJobMode (\_ _ -> handler)

-- | Manual handler for rollup parents. Child results are 'Monoid'-merged, like
-- 'mergedRollupHandler'. The completion callback stores the parent's result,
-- acks it, and fires onJobSuccess.
manualRollupHandler
  :: (Monoid result)
  => (result -> Map Int64 Text -> JobRead payload -> JobCompletion m result -> m ())
  -> HandlerMode m payload result
manualRollupHandler handler =
  ManualJobMode $ \results dlqFailures -> handler (mergeChildResults results) dlqFailures

-- | Fold child results via 'Monoid', treating failures as 'mempty'.
mergeChildResults :: (Monoid a) => Map Int64 (Either Text a) -> a
mergeChildResults = foldMap fold

-- | Internal helper to create a config with the given handler mode.
mkDefaultConfig
  :: (Applicative n, MonadIO m)
  => ByteString
  -> Int
  -> HandlerMode n payload result
  -> m (WorkerConfig n payload result)
mkDefaultConfig connStrVal workerCnt mode = do
  heartbeatTMVar <- liftIO newEmptyTMVarIO
  shutdownTVar <- newTVarIO Running
  pauseTVar <- newTVarIO False
  uuid <- liftIO UUID.nextRandom
  tmpDir <- liftIO getTemporaryDirectory
  host <- liftIO getHostName
  let livenessPath = tmpDir <> "/arbiter-worker-" <> toString uuid
  pure
    WorkerConfig
      { connStr = connStrVal
      , workerCount = workerCnt
      , handlerMode = mode
      , pollInterval = 5
      , visibilityTimeout = 60
      , jobHeartbeatInterval = 30
      , workerHeartbeatInterval = 10
      , maxAttempts = 10
      , backoffStrategy = exponentialBackoff 2.0 1_048_576
      , jitter = EqualJitter
      , observabilityHooks = defaultObservabilityHooks
      , workerStateVar = shutdownTVar
      , pauseVar = pauseTVar
      , livenessFile = Just livenessPath
      , gracefulShutdownTimeout = Just 30
      , logConfig = withWorkerIdContext uuid defaultLogConfig
      , cronJobs = []
      , reaperInterval = 300
      , workerId = uuid
      , workerHost = Just (T.pack host)
      , workerMetadata = Nothing
      , workerStaleThreshold = 300
      , heartbeatSignal = heartbeatTMVar
      }

withWorkerIdContext :: UUID -> LogConfig -> LogConfig
withWorkerIdContext workerId lc =
  lc {additionalContext = (("workerId" .= workerId) :) <$> additionalContext lc}

-- | Initiate graceful shutdown of the worker pool
--
-- Stops claiming new jobs. In-flight jobs will complete, then the pool exits.
shutdownWorker :: (MonadIO m) => WorkerConfig n payload result -> m ()
shutdownWorker config = liftIO . STM.atomically $ STM.writeTVar (workerStateVar config) ShuttingDown

getWorkerState :: (MonadIO m) => WorkerConfig n payload result -> m WorkerState
getWorkerState config = liftIO . STM.atomically $ readEffectiveState config

readEffectiveState :: WorkerConfig n payload result -> STM.STM WorkerState
readEffectiveState config = do
  st <- STM.readTVar (workerStateVar config)
  case st of
    ShuttingDown -> pure ShuttingDown
    _ -> do
      paused <- STM.readTVar (pauseVar config)
      pure $ if paused then Paused else st

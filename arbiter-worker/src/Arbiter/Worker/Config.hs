{-# LANGUAGE OverloadedStrings #-}

-- | Configuration types for the arbiter worker pool.
module Arbiter.Worker.Config
  ( -- * Worker Configuration
    WorkerConfig (..)
  , defaultWorkerConfig
  , defaultBatchedWorkerConfig
  , defaultRollupWorkerConfig
  , singleJobMode
  , mergedRollupHandler
  , mergeChildResults
  , HandlerMode (..)
  , LivenessConfig (..)

    -- * Worker State
  , WorkerState (..)
  , pauseWorker
  , resumeWorker
  , shutdownWorker
  , getWorkerState
  ) where

import Arbiter.Core.Job.Types (ObservabilityHooks, defaultObservabilityHooks)
import Arbiter.Core.MonadArbiter (BatchedJobHandler, JobHandler, MonadArbiter)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.Foldable (fold)
import Data.Int (Int32, Int64)
import Data.Map.Strict (Map)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Data.UUID (toString)
import Data.UUID.V4 qualified as UUID
import System.Directory (getTemporaryDirectory)
import UnliftIO.MVar (MVar, newEmptyMVar)
import UnliftIO.STM (TVar, newTVarIO)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.BackoffStrategy (BackoffStrategy, Jitter (..), exponentialBackoff)
import Arbiter.Worker.Cron (CronJob)
import Arbiter.Worker.Logger (LogConfig (..), defaultLogConfig)
import Arbiter.Worker.WorkerState (WorkerState (..))

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
data HandlerMode m payload result
  = -- | Claim 1 job per group. The handler receives:
    --
    -- 1. @Map childJobId (Either decodeError result)@ - child results for
    --    rollup finalizers. @Left@ means the child succeeded but its result
    --    couldn't be decoded into @result@. Empty for non-rollup jobs.
    -- 2. @Map dlqPrimaryKey errorMsg@ - children that failed and were DLQ'd.
    --    Empty for non-rollup jobs.
    SingleJobMode (Map Int64 (Either Text result) -> Map Int64 Text -> JobHandler m payload result)
  | -- | Batched mode: claim up to N jobs per group, handler receives batch.
    --
    -- __Rollup interaction:__ Batched mode has no rollup awareness - child results
    -- are not passed to the handler. Use 'SingleJobMode' for rollup finalizers.
    BatchedJobsMode Int (BatchedJobHandler m payload result)

-- | File-based liveness probe configuration.
--
-- The probe file is touched when the dispatcher claims jobs or the heartbeat
-- extends visibility, proving the worker is actively functioning. A timeout
-- fallback ensures the probe still updates during idle periods (no jobs to
-- process). If the file goes stale, the worker should be restarted.
data LivenessConfig = LivenessConfig
  { livenessPath :: FilePath
  -- ^ Path to the health check file
  , livenessSignal :: MVar ()
  -- ^ MVar pulsed by the dispatcher after each claim cycle and by the
  -- heartbeat after each successful visibility extension
  , livenessInterval :: Int
  -- ^ Seconds between health file writes
  }

-- | Configuration for a worker pool.
--
-- __Time units:__ All time-related fields use 'NominalDiffTime' (seconds).
-- Sub-second precision is supported (e.g., @0.1@ for 100ms).
data WorkerConfig m payload result = WorkerConfig
  { connStr :: ByteString
  -- ^ PostgreSQL connection string (used for LISTEN/NOTIFY).
  , workerCount :: Int
  -- ^ Number of concurrent worker threads.
  , handlerMode :: HandlerMode m payload result
  -- ^ Job handler and claiming strategy. Set by the @default*WorkerConfig@ helpers.
  , pollInterval :: NominalDiffTime
  -- ^ Polling interval in __seconds__ (fallback when no NOTIFY received).
  -- Also serves as the liveness heartbeat - the dispatcher signals the
  -- liveness probe after each poll cycle. Default: 5 seconds.
  , visibilityTimeout :: NominalDiffTime
  -- ^ Duration in __seconds__ a claimed job stays invisible to other workers.
  -- Must be greater than 'heartbeatInterval'. Sub-second values are
  -- rounded up to the nearest second. Default: 60.
  , heartbeatInterval :: Int
  -- ^ Interval in __seconds__ for extending visibility timeout during processing.
  -- Must be less than 'visibilityTimeout' to prevent job reclaim. Default: 30.
  , maxAttempts :: Int32
  -- ^ Max retries before moving to DLQ (used when job's maxAttempts is Nothing).
  -- Default: 10.
  , backoffStrategy :: BackoffStrategy
  -- ^ Retry backoff strategy. Default: exponential with base 2, max 1048576 seconds.
  , jitter :: Jitter
  -- ^ Jitter strategy for retry delays. Default: 'EqualJitter'.
  , useWorkerTransaction :: Bool
  -- ^ __Transaction Mode:__
  --
  -- * __True (Automatic)__: Handler runs in a transaction with automatic
  -- acking and rollback. Heartbeat prevents job reclaim during processing.
  -- Cannot control commit timing. If the ack fails then all database work is
  -- rolled back - this means 'BatchedJobsMode' can have a large blast radius
  -- because the batch is acked atomically. Default: True.
  --
  -- * __False (Manual)__: Full transaction control. Handler must explicitly ack jobs.
  -- Child results are __not__ automatically stored in the results table -
  -- the handler must call 'Arbiter.Core.HighLevel.insertResult' to make
  -- results visible to the rollup finalizer.
  , transactionTimeout :: Maybe NominalDiffTime
  -- ^ Transaction statement timeout in __seconds__. Only applies when
  -- useWorkerTransaction is True. Default: Nothing (no timeout).
  , observabilityHooks :: ObservabilityHooks m payload
  -- ^ Callbacks for metrics or tracing. Default: no-op hooks.
  , workerStateVar :: TVar WorkerState
  -- ^ Worker state (Running, Paused, ShuttingDown). Managed internally.
  , livenessConfig :: Maybe LivenessConfig
  -- ^ File-based liveness probe configuration. Default: writes to
  -- @\/tmp\/arbiter-worker-\<uuid\>@ every 60 seconds.
  , gracefulShutdownTimeout :: Maybe NominalDiffTime
  -- ^ Maximum time in __seconds__ to wait for in-flight jobs during graceful
  -- shutdown. If @Nothing@, waits indefinitely. If @Just n@, force-exits after
  -- n seconds. Default: @Just 30@.
  , logConfig :: LogConfig
  -- ^ Logging configuration. Arbiter outputs structured JSON logs with job
  -- context automatically included. Use this to control log level, destination,
  -- and inject additional context (e.g., trace IDs). Default: Info level to stdout.
  , claimThrottle :: Maybe (IO (Int, NominalDiffTime))
  -- ^ Optional claim rate limiter. The @IO@ action returns
  -- @(maxClaims, window)@: at most @maxClaims@ jobs will be claimed
  -- per @window@ duration. The action is called each claim cycle, so
  -- limits can be adjusted dynamically.
  -- Default: @Nothing@ (no throttling).
  , cronJobs :: [CronJob payload]
  -- ^ Cron schedules. When non-empty, the worker pool spawns a scheduler
  -- thread that inserts jobs on cron expressions. The @cron_schedules@
  -- table is consulted for runtime overrides (expression, overlap, enabled).
  -- Default: @[]@.
  , groupReaperInterval :: NominalDiffTime
  -- ^ How often to recompute the groups table (correct drift in
  -- job_count, min_priority, min_id, in_flight_until).
  -- Default: @300@ (5 minutes).
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

-- | Create a t'WorkerConfig' for rollup finalizers. See 'mergedRollupHandler'.
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

-- | Handler that ignores child results. Use for regular jobs and leaf children.
singleJobMode :: JobHandler m payload result -> HandlerMode m payload result
singleJobMode handler = SingleJobMode (\_ _ -> handler)

-- | Handler for rollup finalizers. Child results are merged via 'Monoid'.
--
-- The handler receives two arguments before the job:
--
-- 1. The @mappend@-fold of all child results. Children whose results
--    couldn't be decoded into the expected type contribute 'mempty'
--    (the raw JSON is still in the results table). Use 'SingleJobMode'
--    directly to inspect per-child decode failures via the @Left@ entries.
-- 2. A map of DLQ failures (@dlqPrimaryKey -> errorMessage@) for children
--    that failed and were moved to the DLQ.
--
-- For non-rollup jobs both arguments are empty.
mergedRollupHandler
  :: (Monoid result) => (result -> Map Int64 Text -> JobHandler m payload result) -> HandlerMode m payload result
mergedRollupHandler handler = SingleJobMode $ \results dlqFailures -> handler (mergeChildResults results) dlqFailures

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
  livenessMVar <- newEmptyMVar
  shutdownTVar <- newTVarIO Running
  uuid <- liftIO UUID.nextRandom
  tmpDir <- liftIO getTemporaryDirectory
  let livenessFile = tmpDir <> "/arbiter-worker-" <> toString uuid
  pure
    WorkerConfig
      { connStr = connStrVal
      , workerCount = workerCnt
      , handlerMode = mode
      , pollInterval = 5
      , visibilityTimeout = 60
      , heartbeatInterval = 30
      , maxAttempts = 10
      , backoffStrategy = exponentialBackoff 2.0 1_048_576
      , jitter = EqualJitter
      , useWorkerTransaction = True
      , transactionTimeout = Nothing
      , observabilityHooks = defaultObservabilityHooks
      , workerStateVar = shutdownTVar
      , livenessConfig = Just (LivenessConfig livenessFile livenessMVar 60)
      , gracefulShutdownTimeout = Just 30
      , logConfig = defaultLogConfig
      , claimThrottle = Nothing
      , cronJobs = []
      , groupReaperInterval = 300
      }

-- | Pause the worker pool
--
-- Stops claiming new jobs. In-flight jobs will complete normally.
-- Workers will wait in paused state until resumed or shut down.
pauseWorker :: (MonadIO m) => WorkerConfig n payload result -> m ()
pauseWorker config = liftIO . STM.atomically $ do
  st <- STM.readTVar (workerStateVar config)
  case st of
    ShuttingDown -> pure ()
    _ -> STM.writeTVar (workerStateVar config) Paused

-- | Resume the worker pool from paused state
--
-- Workers will start claiming new jobs again.
resumeWorker :: (MonadIO m) => WorkerConfig n payload result -> m ()
resumeWorker config = liftIO . STM.atomically $ do
  st <- STM.readTVar (workerStateVar config)
  case st of
    ShuttingDown -> pure ()
    _ -> STM.writeTVar (workerStateVar config) Running

-- | Initiate graceful shutdown of the worker pool
--
-- Stops claiming new jobs. In-flight jobs will complete, then the pool exits.
shutdownWorker :: (MonadIO m) => WorkerConfig n payload result -> m ()
shutdownWorker config = liftIO . STM.atomically $ STM.writeTVar (workerStateVar config) ShuttingDown

-- | Get the current worker state
getWorkerState :: (MonadIO m) => WorkerConfig n payload result -> m WorkerState
getWorkerState config = liftIO . STM.atomically $ STM.readTVar (workerStateVar config)

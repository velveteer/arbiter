{-# LANGUAGE OverloadedStrings #-}

-- | Configuration types for the arbiter worker pool.
module Arbiter.Worker.Config
  ( -- * Worker Configuration
    WorkerConfig (..)
  , defaultWorkerConfig
  , defaultBatchedWorkerConfig
  , defaultBatchedRollupWorkerConfig
  , defaultMaintenanceConfig
  , singleJobMode
  , HandlerMode (..)
  , ClaimAdmission (..)

    -- * Timings
  , WorkerTimings (..)
  , defaultWorkerTimings
  , applyWorkerTimings
  , applyRetryPolicy

    -- * Batch Callbacks
  , BatchCallbacks (..)

    -- * Worker State
  , WorkerState (..)
  , shutdownWorker
  , getWorkerState
  , readEffectiveState
  ) where

import Arbiter.Core.Job.Types (JobRead, ObservabilityHooks, defaultObservabilityHooks)
import Arbiter.Core.MonadArbiter (JobHandler, MonadArbiter)
import Arbiter.Core.Sql.Claim (ClaimAdmission (..))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (Value, (.=))
import Data.ByteString (ByteString)
import Data.List.NonEmpty (NonEmpty)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Data.UUID (UUID, toString)
import Data.UUID.V4 qualified as UUID
import Network.HostName (getHostName)
import OpenTelemetry.Attributes (Attribute)
import OpenTelemetry.Trace.Core (Tracer)
import System.Directory (getTemporaryDirectory)
import UnliftIO.STM (TMVar, TVar, newEmptyTMVarIO, newTVarIO)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.BackoffStrategy (BackoffStrategy, Jitter)
import Arbiter.Worker.Cron (CronJob)
import Arbiter.Worker.Logger (LogConfig (..), defaultLogConfig)
import Arbiter.Worker.Outcome (RetryPolicy (..), defaultRetryPolicy)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | The pool's cadences, in seconds.
data WorkerTimings = WorkerTimings
  { timingPollInterval :: NominalDiffTime
  , timingVisibilityTimeout :: NominalDiffTime
  , timingJobHeartbeatInterval :: NominalDiffTime
  -- ^ Must stay below 'timingVisibilityTimeout', or a running job is reclaimed.
  , timingWorkerHeartbeatInterval :: NominalDiffTime
  -- ^ Must stay below 'timingWorkerStaleThreshold', or a live worker is swept.
  , timingWorkerStaleThreshold :: NominalDiffTime
  , timingReaperInterval :: NominalDiffTime
  , timingReaperTimeout :: NominalDiffTime
  , timingGracefulShutdownTimeout :: Maybe NominalDiffTime
  -- ^ 'Nothing' waits for in-flight jobs indefinitely.
  }
  deriving stock (Eq, Show)

defaultWorkerTimings :: WorkerTimings
defaultWorkerTimings =
  WorkerTimings
    { timingPollInterval = 5
    , timingVisibilityTimeout = 60
    , timingJobHeartbeatInterval = 30
    , timingWorkerHeartbeatInterval = 10
    , timingWorkerStaleThreshold = 300
    , timingReaperInterval = 300
    , timingReaperTimeout = 300
    , timingGracefulShutdownTimeout = Just 30
    }

applyWorkerTimings :: WorkerTimings -> WorkerConfig m payload result -> WorkerConfig m payload result
applyWorkerTimings t config =
  config
    { pollInterval = timingPollInterval t
    , visibilityTimeout = timingVisibilityTimeout t
    , jobHeartbeatInterval = timingJobHeartbeatInterval t
    , workerHeartbeatInterval = timingWorkerHeartbeatInterval t
    , workerStaleThreshold = timingWorkerStaleThreshold t
    , reaperInterval = timingReaperInterval t
    , reaperTimeout = timingReaperTimeout t
    , gracefulShutdownTimeout = timingGracefulShutdownTimeout t
    }

applyRetryPolicy :: RetryPolicy -> WorkerConfig m payload result -> WorkerConfig m payload result
applyRetryPolicy p config =
  config {backoffStrategy = retryBackoff p, jitter = retryJitter p}

-- | Configuration for a worker pool.
data WorkerConfig m payload result = WorkerConfig
  { connStr :: ByteString
  -- ^ PostgreSQL connection string (used for LISTEN/NOTIFY).
  , workerCount :: Int
  -- ^ Number of concurrent worker threads.
  , handlerMode :: Maybe (HandlerMode m payload result)
  -- ^ Job handler and claiming strategy. Set by the @default*WorkerConfig@ helpers.
  -- 'Nothing' consumes nothing: the pool runs only the queue's background loops.
  -- See 'defaultMaintenanceConfig'.
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
  , backoffStrategy :: BackoffStrategy
  -- ^ Retry backoff strategy. Default: exponential with base 2, max 1048576 seconds.
  , jitter :: Jitter
  -- ^ Jitter strategy for retry delays. Default: 'EqualJitter'.
  , observabilityHooks :: ObservabilityHooks m payload
  -- ^ Callbacks for metrics or tracing. Default: no-op hooks.
  , consumeSpanAttributes :: NonEmpty (JobRead payload) -> [(Text, Attribute)]
  -- ^ Attributes set on the consumer span at creation, so a sampler can read them. Default: none.
  , tracer :: Maybe Tracer
  -- ^ Tracer resolved once at pool start. 'runWorkerPool' sets it. Default: none.
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
  , reaperTimeout :: NominalDiffTime
  -- ^ Abort any single reaper statement that runs longer than this. Default: @300@ (5 minutes).
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
  , claimAdmissionOverride :: Maybe ClaimAdmission
  -- ^ Force the claim's admission rather than deriving it from the payload type, for a
  -- runtime (untyped) worker. Default: 'Nothing' (type-derived).
  , maintenanceQueues :: Maybe [Text]
  -- ^ Queues this pool's reaper maintains: group refresh, exhausted-job sweep, and
  -- concurrency recount. A single-queue registry must name every queue the deployment
  -- runs. Default: this pool's registry.
  }

-- | Per-job finalizers handed to a batched handler. Untouched jobs are
-- reprocessed. The @With@ variants store a result for the job's parent rollup.
--
-- Each callback runs in its own transaction and commits on return. Call them at
-- the top level of the handler. Wrapping one in your own 'withDbTransaction'
-- enlists the ack into that transaction as a savepoint, committing atomically
-- with your writes. The success hook then fires at savepoint release, not at
-- your outer commit, so an outer rollback reprocesses the job after the
-- visibility timeout.
data BatchCallbacks m payload result = BatchCallbacks
  { ack :: JobRead payload -> m ()
  -- ^ Ack and fire onJobSuccess.
  , ackWith :: JobRead payload -> result -> m ()
  -- ^ Ack, store the result for the parent rollup, fire onJobSuccess.
  , ackAll :: [JobRead payload] -> m ()
  -- ^ Bulk-ack in one parent-aware transaction. Fires onJobSuccess per acked job.
  , ackAllWith :: [(JobRead payload, result)] -> m ()
  -- ^ 'ackAll' storing each job's result for its parent rollup.
  , failRetry :: JobRead payload -> Text -> m ()
  -- ^ Retry with backoff, then DLQ at the job's maxAttempts.
  , failPermanent :: JobRead payload -> Text -> m ()
  -- ^ Straight to the DLQ.
  , cancelBranch :: JobRead payload -> Text -> m ()
  -- ^ Cancel this job's branch (its parent and all siblings).
  , cancelTree :: JobRead payload -> Text -> m ()
  -- ^ Cancel the whole tree from the root down.
  , nack :: JobRead payload -> m ()
  -- ^ Reprocess after the visibility timeout, no failure recorded and no
  -- attempt consumed.
  }

-- | How the worker claims and runs jobs. Set by the @default*WorkerConfig@ helpers.
data HandlerMode m payload result
  = -- | Automatic single-job mode: claim one job per group and run the handler
    -- in a worker transaction, storing its result and acking atomically.
    SingleJobMode (JobHandler m payload result)
  | -- | Batched callback mode: claim up to N jobs per group and hand the batch to
    -- the handler with a 'BatchCallbacks' record to finalize each job. No worker
    -- transaction. Batch size 1 is the manual single-job case.
    BatchedJobsMode
      Int
      (NonEmpty (JobRead payload) -> BatchCallbacks m payload result -> m ())

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
  mkDefaultConfig connStrVal workerCnt (Just (singleJobMode handler))

-- | Create a t'WorkerConfig' for batched job processing. The handler receives
-- the batch and a 'BatchCallbacks' record to finalize each job (ack, fail,
-- cancel, or nack). Jobs left untouched are reprocessed. For rollup parents that
-- store a result per job, see 'defaultBatchedRollupWorkerConfig'.
defaultBatchedWorkerConfig
  :: (MonadArbiter n, MonadIO m)
  => ByteString
  -- ^ Connection string
  -> Int
  -- ^ Worker count
  -> Int
  -- ^ Batch size (max jobs per group to claim together)
  -> (NonEmpty (JobRead payload) -> BatchCallbacks n payload () -> n ())
  -> m (WorkerConfig n payload ())
defaultBatchedWorkerConfig connStrVal workerCnt batchSize handler =
  mkDefaultConfig connStrVal workerCnt (Just (BatchedJobsMode batchSize handler))

-- | Create a t'WorkerConfig' for batched rollup parents. Store each job's result
-- for its parent with 'ackWith' or 'ackAllWith'. Fetch a parent's child
-- results with 'Arbiter.Worker.childResults' or 'Arbiter.Worker.mergedChildResults'.
defaultBatchedRollupWorkerConfig
  :: (MonadArbiter n, MonadIO m)
  => ByteString
  -- ^ Connection string
  -> Int
  -- ^ Worker count
  -> Int
  -- ^ Batch size (max jobs per group to claim together)
  -> (NonEmpty (JobRead payload) -> BatchCallbacks n payload result -> n ())
  -> m (WorkerConfig n payload result)
defaultBatchedRollupWorkerConfig connStrVal workerCnt batchSize handler =
  mkDefaultConfig connStrVal workerCnt (Just (BatchedJobsMode batchSize handler))

-- | A t'WorkerConfig' for a queue this process serves but does not consume: background
-- loops only, with no dispatcher, worker threads, or worker-registry row.
defaultMaintenanceConfig :: (Applicative n, MonadIO m) => ByteString -> m (WorkerConfig n payload ())
defaultMaintenanceConfig connStrVal =
  (\c -> c {livenessFile = Nothing}) <$> mkDefaultConfig connStrVal 0 Nothing

-- | Handler that runs a single job. Use for regular jobs, leaf children, and
-- rollup parents (fetch results with 'Arbiter.Worker.mergedChildResults').
singleJobMode :: JobHandler m payload result -> HandlerMode m payload result
singleJobMode = SingleJobMode

-- | Internal helper to create a config with the given handler mode.
mkDefaultConfig
  :: (Applicative n, MonadIO m)
  => ByteString
  -> Int
  -> Maybe (HandlerMode n payload result)
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
      , pollInterval = timingPollInterval t
      , visibilityTimeout = timingVisibilityTimeout t
      , jobHeartbeatInterval = timingJobHeartbeatInterval t
      , workerHeartbeatInterval = timingWorkerHeartbeatInterval t
      , workerStaleThreshold = timingWorkerStaleThreshold t
      , reaperInterval = timingReaperInterval t
      , reaperTimeout = timingReaperTimeout t
      , gracefulShutdownTimeout = timingGracefulShutdownTimeout t
      , backoffStrategy = retryBackoff defaultRetryPolicy
      , jitter = retryJitter defaultRetryPolicy
      , observabilityHooks = defaultObservabilityHooks
      , consumeSpanAttributes = const []
      , tracer = Nothing
      , workerStateVar = shutdownTVar
      , pauseVar = pauseTVar
      , livenessFile = Just livenessPath
      , logConfig = withWorkerIdContext uuid defaultLogConfig
      , cronJobs = []
      , workerId = uuid
      , workerHost = Just (T.pack host)
      , workerMetadata = Nothing
      , heartbeatSignal = heartbeatTMVar
      , claimAdmissionOverride = Nothing
      , maintenanceQueues = Nothing
      }
  where
    t = defaultWorkerTimings

withWorkerIdContext :: UUID -> LogConfig -> LogConfig
withWorkerIdContext workerId lc =
  lc {additionalContext = (("worker_id" .= workerId) :) <$> additionalContext lc}

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

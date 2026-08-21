{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Configuration types for the arbiter worker pool.
module Arbiter.Worker.Config
  ( -- * Worker Configuration
    WorkerConfig (..)
  , transactionalWorkerConfig
  , manualWorkerConfig
  , defaultBatchedWorkerConfig
  , withHooks
  , withMaintenance
  , HandlerMode (..)
  , handlerBatchSize
  , MaintenanceOp (..)
  , maintenanceOpName
  , ResultOf
  , WorkerConfigException (..)
  , validateWorkerConfig

    -- * Batch Callbacks
  , BatchCallbacks (..)

    -- * Worker State
  , WorkerState (..)
  , shutdownWorker
  , getWorkerState
  , getListenerReady
  , readEffectiveState
  , writePause
  , writePauseIfCurrent
  ) where

import Arbiter.Core.Job.Types (JobRead, ObservabilityHooks, andThen, defaultObservabilityHooks)
import Arbiter.Core.MonadArbiter (JobHandler, MonadArbiter, ResultOf)
import Control.Exception (Exception)
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (Value, (.=))
import Data.Foldable (toList)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Data.UUID (UUID, toString)
import Data.UUID.V4 qualified as UUID
import Data.Word (Word64)
import Network.HostName (getHostName)
import System.Directory (getTemporaryDirectory)
import UnliftIO (MonadUnliftIO)
import UnliftIO.STM (TMVar, TVar, newEmptyTMVarIO, newTVarIO)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.BackoffStrategy (BackoffStrategy, Jitter (..), exponentialBackoff)
import Arbiter.Worker.Cron (CronJob)
import Arbiter.Worker.Logger (LogConfig (..), defaultLogConfig)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | Which reaper op a maintenance report came from.
data MaintenanceOp
  = RefreshGroups
  | SweepStaleWorkers
  | SweepExhaustedJobs
  | SweepCancelledJobs
  | PruneRateLimitBuckets
  | ReconcileConcurrencyStale
  | ReconcilePruneConcurrency
  | PurgeArchives
  deriving stock (Bounded, Enum, Eq, Ord, Show)

-- | The op's stable name, used to coordinate replicas and to label its metrics.
maintenanceOpName :: MaintenanceOp -> Text
maintenanceOpName op = case op of
  RefreshGroups -> "refresh-all-groups"
  SweepStaleWorkers -> "sweep-stale-workers"
  SweepExhaustedJobs -> "sweep-exhausted-jobs"
  SweepCancelledJobs -> "sweep-cancelled-jobs"
  PruneRateLimitBuckets -> "prune-rate-limit-buckets"
  ReconcileConcurrencyStale -> "reconcile-concurrency-stale"
  ReconcilePruneConcurrency -> "reconcile-prune-concurrency"
  PurgeArchives -> "purge-archives"

-- | Invalid worker configuration detected before a pool starts.
newtype WorkerConfigException = WorkerConfigException Text
  deriving stock (Eq, Show)
  deriving anyclass (Exception)

-- | Configuration for a worker pool.
data WorkerConfig m payload = WorkerConfig
  { workerCount :: Int
  -- ^ Number of concurrent worker threads.
  , handlerMode :: HandlerMode m payload
  -- ^ Job handler and claiming strategy. Set by this module's config constructors.
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
  , onMaintenance :: MaintenanceOp -> Int64 -> m ()
  -- ^ Called after a reaper op this pool won the gate for, with the rows it touched.
  -- Reaper work is schema-wide, so it carries no queue. Default: no-op.
  , workerStateVar :: TVar WorkerState
  -- ^ Run/shutdown lifecycle. Pause is tracked separately in 'pauseVar'.
  , pauseVar :: TVar Bool
  -- ^ Per-pool pause flag. Write it through 'writePause'.
  , pauseEpoch :: TVar Word64
  -- ^ Bumped by every 'pauseVar' write, so a reading taken before one lands is
  -- discarded rather than applied on top of it.
  , livenessFile :: Maybe FilePath
  -- ^ When set, the heartbeat loop touches this file at the
  -- 'workerHeartbeatInterval' cadence, for a file-based liveness probe.
  -- Default: @arbiter-worker-\<workerId\>@ in the system temporary directory.
  , gracefulShutdownTimeout :: Maybe NominalDiffTime
  -- ^ Seconds a graceful shutdown waits on in-flight jobs before force-exiting.
  -- 'Nothing' waits indefinitely. Default: @Just 30@.
  , logConfig :: LogConfig
  -- ^ Where the pool's structured JSON logs go, at what level, and what context they
  -- carry beyond the job's own. Default: Info to stdout.
  , cronJobs :: [CronJob payload]
  -- ^ Cron schedules. A non-empty list gives the pool a scheduler thread, which reads
  -- the @cron_schedules@ table each tick for runtime overrides. Default: @[]@.
  , reaperInterval :: NominalDiffTime
  -- ^ How often the reaper runs. Default: @300@ (5 minutes).
  , reaperTimeout :: NominalDiffTime
  -- ^ Abort any single reaper statement that runs longer than this. Default: @300@ (5 minutes).
  , workerId :: UUID
  -- ^ Identity for this pool. Freshly generated by default, so it does not survive a
  -- restart.
  , workerHost :: Maybe Text
  -- ^ Hostname recorded in the worker registry. Default: auto-generated.
  , workerMetadata :: Maybe Value
  -- ^ Arbitrary JSONB metadata for the worker registry row (image tag,
  -- git SHA, deploy id, etc.). Default: 'Nothing'.
  , workerStaleThreshold :: NominalDiffTime
  -- ^ Workers whose @last_heartbeat@ is older than this are swept from the
  -- runtime registry by 'reaperInterval'. Must be well above the heartbeat cadence
  -- ('workerHeartbeatInterval', or 'jobHeartbeatInterval' while busy).
  -- Default: @300@ (5 minutes).
  , heartbeatSignal :: TMVar ()
  -- ^ Worker-level proof-of-work signal pulsed by the dispatcher and per-job heartbeats.
  , listenerReadyVar :: TVar Bool
  -- ^ True once this pool's LISTEN channels are subscribed, or immediately when
  -- there is no listener. Observability only, startup never blocks on it.
  }

-- | Per-job finalizers handed to a batched handler. Untouched jobs are
-- reprocessed. The @With@ variants store a result for the job's parent rollup
-- or its archive entry.
--
-- Each callback runs in its own transaction and commits on return. Call them at
-- the top level of the handler. Wrapping one in your own 'Arbiter.Core.MonadArbiter.withDbTransaction'
-- enlists the ack into that transaction as a savepoint, committing atomically
-- with your writes. The success hook then fires at savepoint release, not at
-- your outer commit, so an outer rollback reprocesses the job after the
-- visibility timeout.
data BatchCallbacks m payload result = BatchCallbacks
  { ack :: JobRead payload -> m ()
  -- ^ Ack and fire onJobSuccess, storing no result. Available on any queue: a
  -- job acked this way is absent from its parent rollup's child results and
  -- leaves its archive entry's result @NULL@. Aborts the handler if another
  -- worker holds the job, which nacks the siblings still unfinalized.
  , ackWith :: JobRead payload -> result -> m ()
  -- ^ Ack, store the result for the parent rollup or the job's archive entry,
  -- fire onJobSuccess.
  , ackAll :: [JobRead payload] -> m ()
  -- ^ Bulk-'ack' in one parent-aware transaction, storing no results. Fires
  -- onJobSuccess per acked job. Unlike 'ack', a job another worker holds is
  -- reported and skipped, and the handler continues.
  , ackAllWith :: [(JobRead payload, result)] -> m ()
  -- ^ 'ackAll' storing each job's result for its parent rollup or archive entry.
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

-- | How the worker claims and runs jobs. Set by this module's config constructors.
data HandlerMode m payload
  = -- | Automatic single-job mode: claim one job per group and run the handler
    -- in a worker transaction, storing its result and acking atomically.
    SingleJobMode (JobHandler m payload (ResultOf m payload))
  | -- | Batched callback mode: claim up to N jobs per group and hand the batch to
    -- the handler with a 'BatchCallbacks' record to finalize each job. No worker
    -- transaction. Batch size 1 is the manual single-job case.
    BatchedJobsMode
      Int
      (NonEmpty (JobRead payload) -> BatchCallbacks m payload (ResultOf m payload) -> m ())

-- | How many jobs a pool claims per group.
handlerBatchSize :: WorkerConfig m payload -> Int
handlerBatchSize config = case handlerMode config of
  SingleJobMode _ -> 1
  BatchedJobsMode n _ -> n

-- | Validate invariants required for safe worker execution. Reports every
-- violation, not just the first.
validateWorkerConfig :: WorkerConfig m payload -> Either Text ()
validateWorkerConfig config =
  case fieldInvariants config *> crossFieldInvariants config of
    Validation (Left messages) -> Left (T.intercalate "; " (toList messages))
    Validation (Right ()) -> Right ()

-- | Rebuilds the config so every field is either checked or waived. A field added
-- to 'WorkerConfig' is a type error here until it is classified.
fieldInvariants :: WorkerConfig m payload -> Validation (WorkerConfig m payload)
fieldInvariants config =
  WorkerConfig
    <$> positive "workerCount" (workerCount config)
    <*> (handlerMode config <$ positive "handler batch size" (handlerBatchSize config))
    <*> positive "pollInterval" (pollInterval config)
    <*> positive "visibilityTimeout" (visibilityTimeout config)
    <*> positive "jobHeartbeatInterval" (jobHeartbeatInterval config)
    <*> positive "workerHeartbeatInterval" (workerHeartbeatInterval config)
    <*> waived (backoffStrategy config)
    <*> waived (jitter config)
    <*> waived (observabilityHooks config)
    <*> waived (onMaintenance config)
    <*> waived (workerStateVar config)
    <*> waived (pauseVar config)
    <*> waived (pauseEpoch config)
    <*> waived (livenessFile config)
    <*> traverse (nonNegative "gracefulShutdownTimeout") (gracefulShutdownTimeout config)
    <*> waived (logConfig config)
    <*> waived (cronJobs config)
    <*> positive "reaperInterval" (reaperInterval config)
    <*> positive "reaperTimeout" (reaperTimeout config)
    <*> waived (workerId config)
    <*> waived (workerHost config)
    <*> waived (workerMetadata config)
    <*> positive "workerStaleThreshold" (workerStaleThreshold config)
    <*> waived (heartbeatSignal config)
    <*> waived (listenerReadyVar config)

-- | Invariants spanning more than one field.
crossFieldInvariants :: WorkerConfig m payload -> Validation ()
crossFieldInvariants config =
  require
    (jobHeartbeatInterval config < visibilityTimeout config)
    "jobHeartbeatInterval must be less than visibilityTimeout"
    *> require
      (workerHeartbeatInterval config < workerStaleThreshold config)
      "workerHeartbeatInterval must be less than workerStaleThreshold"

newtype Validation a = Validation (Either (NonEmpty Text) a)

instance Functor Validation where
  fmap f (Validation result) = Validation (fmap f result)

instance Applicative Validation where
  pure = Validation . Right
  Validation (Left left) <*> Validation (Left right) = Validation (Left (left <> right))
  Validation (Left left) <*> Validation (Right _) = Validation (Left left)
  Validation (Right _) <*> Validation (Left right) = Validation (Left right)
  Validation (Right f) <*> Validation (Right a) = Validation (Right (f a))

-- | A field carrying no invariant of its own.
waived :: a -> Validation a
waived = pure

-- | Reject a non-positive field, passing it through unchanged.
positive :: (Num a, Ord a) => Text -> a -> Validation a
positive label value = value <$ require (value > 0) (label <> " must be greater than zero")

-- | Reject a negative field, passing it through unchanged. Zero opts out of a
-- wait rather than misconfiguring one.
nonNegative :: (Num a, Ord a) => Text -> a -> Validation a
nonNegative label value = value <$ require (value >= 0) (label <> " must not be negative")

require :: Bool -> Text -> Validation ()
require condition message = Validation (if condition then Right () else Left (message :| []))

-- | Create a t'WorkerConfig' running one job per group in a worker transaction
-- held for the duration of the handler.
--
-- The handler returns the result type @payload@'s registry entry declares.
transactionalWorkerConfig
  :: (MonadArbiter n, MonadIO m)
  => Int
  -- ^ Worker count
  -> JobHandler n payload (ResultOf n payload)
  -> m (WorkerConfig n payload)
transactionalWorkerConfig workerCnt handler =
  mkDefaultConfig workerCnt (SingleJobMode handler)

-- | Create a t'WorkerConfig' for batched job processing, no worker transaction.
-- The handler receives the batch and a 'BatchCallbacks' record to finalize each
-- job (ack, fail, cancel, or nack). Jobs left untouched are reprocessed. To store
-- a result per job, ack with 'ackWith' or 'ackAllWith'.
defaultBatchedWorkerConfig
  :: (MonadArbiter n, MonadIO m)
  => Int
  -- ^ Worker count
  -> Int
  -- ^ Batch size (max jobs per group to claim together)
  -> (NonEmpty (JobRead payload) -> BatchCallbacks n payload (ResultOf n payload) -> n ())
  -> m (WorkerConfig n payload)
defaultBatchedWorkerConfig workerCnt batchSize handler =
  mkDefaultConfig workerCnt (BatchedJobsMode batchSize handler)

-- | Create a t'WorkerConfig' running one job at a time, no worker transaction.
-- The handler finalizes the job through 'BatchCallbacks'. An unfinalized job is
-- reprocessed.
manualWorkerConfig
  :: (MonadArbiter n, MonadIO m)
  => Int
  -- ^ Worker count
  -> (JobRead payload -> BatchCallbacks n payload (ResultOf n payload) -> n ())
  -> m (WorkerConfig n payload)
manualWorkerConfig workerCnt handler =
  defaultBatchedWorkerConfig workerCnt 1 (\(job :| _) -> handler job)

-- | Rework a pool's observability hooks.
withHooks
  :: (ObservabilityHooks m payload -> ObservabilityHooks m payload)
  -> WorkerConfig m payload
  -> WorkerConfig m payload
withHooks f cfg = cfg {observabilityHooks = f (observabilityHooks cfg)}

-- | Run @report@ before the pool's own maintenance callback, both regardless of failure.
withMaintenance
  :: (MonadUnliftIO m)
  => (MaintenanceOp -> Int64 -> m ())
  -> WorkerConfig m payload
  -> WorkerConfig m payload
withMaintenance report cfg =
  cfg {onMaintenance = \op n -> report op n `andThen` onMaintenance cfg op n}

-- | Internal helper to create a config with the given handler mode.
mkDefaultConfig
  :: (Applicative n, MonadIO m)
  => Int
  -> HandlerMode n payload
  -> m (WorkerConfig n payload)
mkDefaultConfig workerCnt mode = do
  heartbeatTMVar <- liftIO newEmptyTMVarIO
  shutdownTVar <- newTVarIO Running
  pauseTVar <- newTVarIO False
  pauseEpochTVar <- newTVarIO 0
  listenerReadyTVar <- newTVarIO False
  uuid <- liftIO UUID.nextRandom
  tmpDir <- liftIO getTemporaryDirectory
  host <- liftIO getHostName
  let livenessPath = tmpDir <> "/arbiter-worker-" <> toString uuid
  pure
    WorkerConfig
      { workerCount = workerCnt
      , handlerMode = mode
      , pollInterval = 5
      , visibilityTimeout = 60
      , jobHeartbeatInterval = 30
      , workerHeartbeatInterval = 10
      , backoffStrategy = exponentialBackoff 2.0 1_048_576
      , jitter = EqualJitter
      , observabilityHooks = defaultObservabilityHooks
      , onMaintenance = \_ _ -> pure ()
      , workerStateVar = shutdownTVar
      , pauseVar = pauseTVar
      , pauseEpoch = pauseEpochTVar
      , livenessFile = Just livenessPath
      , gracefulShutdownTimeout = Just 30
      , logConfig = withWorkerIdContext uuid defaultLogConfig
      , cronJobs = []
      , reaperInterval = 300
      , reaperTimeout = 300
      , workerId = uuid
      , workerHost = Just (T.pack host)
      , workerMetadata = Nothing
      , workerStaleThreshold = 300
      , heartbeatSignal = heartbeatTMVar
      , listenerReadyVar = listenerReadyTVar
      }

withWorkerIdContext :: UUID -> LogConfig -> LogConfig
withWorkerIdContext workerId lc =
  lc {additionalContext = (("worker_id" .= workerId) :) <$> additionalContext lc}

-- | Initiate graceful shutdown of the worker pool
--
-- Stops claiming new jobs. In-flight jobs will complete, then the pool exits.
shutdownWorker :: (MonadIO m) => WorkerConfig n payload -> m ()
shutdownWorker config = liftIO . STM.atomically $ STM.writeTVar (workerStateVar config) ShuttingDown

-- | The pool's state, with a pause reported as paused.
getWorkerState :: (MonadIO m) => WorkerConfig n payload -> m WorkerState
getWorkerState config = liftIO . STM.atomically $ readEffectiveState config

-- | Whether this pool's LISTEN channels are subscribed (or there is no listener).
getListenerReady :: (MonadIO m) => WorkerConfig n payload -> m Bool
getListenerReady config = liftIO . STM.atomically $ STM.readTVar (listenerReadyVar config)

-- | Set the pause flag, superseding any reading a caller has in flight.
writePause :: WorkerConfig n payload -> Bool -> STM.STM ()
writePause config p = do
  STM.writeTVar (pauseVar config) p
  STM.modifyTVar' (pauseEpoch config) (+ 1)

-- | Apply a pause reading taken at @epoch@, unless a newer write has landed since.
-- A queue pause arrives by notification while a heartbeat's reading is in flight,
-- and that reading predates it.
writePauseIfCurrent :: WorkerConfig n payload -> Word64 -> Bool -> STM.STM ()
writePauseIfCurrent config epoch p = do
  current <- STM.readTVar (pauseEpoch config)
  when (current == epoch) $ writePause config p

-- | 'getWorkerState' inside 'STM.STM'.
readEffectiveState :: WorkerConfig n payload -> STM.STM WorkerState
readEffectiveState config = do
  st <- STM.readTVar (workerStateVar config)
  case st of
    ShuttingDown -> pure ShuttingDown
    _ -> do
      paused <- STM.readTVar (pauseVar config)
      pure $ if paused then Paused else st

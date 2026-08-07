{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Core.Job.Types
  ( -- * Core Job Type
    Job (..)
  , AdmissionKeys (..)
  , AdmissionColumns (..)
  , JobRead
  , JobWrite
  , defaultJob
  , defaultGroupedJob
  , defaultMaxAttempts
  , dayRetention
  , isRollup

    -- * Derived status
  , JobStatus (..)
  , jobStatusToText
  , jobStatusFromText

    -- * Type Constraints
  , JobPayload
  , RegistryAdmissionPolicies

    -- * Deduplication
  , DedupKey (..)
  , dedupParts

    -- * Trace context
  , TraceContext (..)
  , toTraceContext

    -- * Observability
  , ObservabilityHooks (..)
  , defaultObservabilityHooks
  , andThen
  , ClaimTime
  , CurrentTime
  , StartTime
  , EndTime
  , ErrorMsg
  , BackoffDelay
  ) where

import Control.Exception qualified as E
import Data.Aeson (FromJSON (..), ToJSON (..), Value, object, withObject, withText, (.:), (.=))
import Data.Aeson.Types (Parser)
import Data.Int (Int32, Int64)
import Data.Maybe (fromMaybe, isJust)
import Data.Text (Text)
import Data.Time (NominalDiffTime, UTCTime)
import Data.UUID.Types (UUID)
import GHC.Generics (Generic)
import UnliftIO (MonadUnliftIO, withRunInIO)

import Arbiter.Core.Concurrency.Spec (ConcurrencyKey, HasConcurrency, RegistryConcurrencyPolicies)
import Arbiter.Core.RateLimit.Spec (HasRateLimit, RateLimitKey, RegistryRateLimitPolicies)

-- | A job in the queue. Parametrized over payload, primary key, queue name,
-- and inserted-at timestamp. See 'JobWrite' (for insertion) and 'JobRead'
-- (returned from claims/queries).
data Job payload key q insertedAt adm = Job
  { primaryKey :: key
  -- ^ @()@ in 'JobWrite' (assigned by DB), @Int64@ in 'JobRead'.
  , payload :: payload
  -- ^ User-defined payload, stored as JSONB.
  , queueName :: q
  -- ^ @()@ in 'JobWrite', table name ('Text') in 'JobRead'. Not serialized.
  , groupKey :: Maybe Text
  -- ^ Jobs with the same group key are processed serially, one at a time per group.
  -- @Nothing@ for ungrouped jobs that can run in parallel.
  , insertedAt :: insertedAt
  -- ^ @()@ in 'JobWrite' (set by DB), @UTCTime@ in 'JobRead'.
  , updatedAt :: Maybe UTCTime
  -- ^ The time the job was last updated.
  , attempts :: Int32
  -- ^ The number of times this job has been attempted.
  , lastError :: Maybe Text
  -- ^ The error message from the last failed attempt.
  , priority :: Int32
  -- ^ The job's priority. Lower numbers are higher priority.
  , lastAttemptedAt :: Maybe UTCTime
  -- ^ The time this job was last claimed by a worker.
  , notVisibleUntil :: Maybe UTCTime
  -- ^ When this job becomes visible for claiming.
  , dedupKey :: Maybe DedupKey
  -- ^ The deduplication strategy for this job.
  , maxAttempts :: Maybe Int32
  -- ^ Attempt limit before the job moves to the DLQ. @Nothing@ defaults to
  -- 'defaultMaxAttempts' at insert time.
  , parentId :: Maybe Int64
  -- ^ Parent job ID. Set by 'insertJobTree', not manually. When this child
  -- is the last to complete, the parent (if a rollup finalizer) is resumed.
  , parentState :: Maybe Value
  -- ^ Snapshot of accumulated child results for rollup finalizers. The
  -- engine sets this to an empty object on insert when the job is a
  -- rollup finalizer, and overwrites it with the final results map before
  -- a DLQ move so the snapshot survives the @ON DELETE CASCADE@ on the
  -- results table. 'isRollup' is derived from whether this is non-null.
  , traceContext :: Maybe TraceContext
  -- ^ W3C trace context captured at enqueue.
  , suspended :: Bool
  -- ^ Whether this job is suspended (not claimable).
  -- @TRUE@ for: finalizers waiting for children to complete,
  -- or operator-paused jobs.
  , claimedBy :: Maybe UUID
  -- ^ Worker pool UUID that last claimed this job.
  , archiveFor :: Maybe Int32
  -- ^ Retention in seconds for this job's completed-job archive entry.
  -- @Just n@ archives the job on ack and keeps the entry for @n@ seconds.
  -- @Nothing@ (the default) deletes on ack with no archive write.
  , admission :: adm
  -- ^ @()@ in 'JobWrite'. 'AdmissionKeys' in 'JobRead', stamped at enqueue from
  -- the payload's admission selectors.
  }
  deriving stock (Eq, Generic, Show)

-- | The admission keys gating a stored job's claim, one field per kind.
data AdmissionKeys = AdmissionKeys
  { jobRateLimitKey :: Maybe RateLimitKey
  -- ^ From the payload's 'Arbiter.Core.RateLimit.Spec.HasRateLimit' instance.
  , jobConcurrencyKey :: Maybe ConcurrencyKey
  -- ^ From the payload's 'Arbiter.Core.Concurrency.Spec.HasConcurrency' instance.
  }
  deriving stock (Eq, Generic, Show)

-- | The writable admission columns, resolved from a payload at enqueue. The @key@
-- and @prefix@ columns round-trip via 'AdmissionKeys'. @cost@ is write-only.
data AdmissionColumns = AdmissionColumns
  { acRateLimitKey :: Maybe Text
  , acRateLimitPrefix :: Maybe Text
  , acRateLimitCost :: Double
  , acConcurrencyKey :: Maybe Text
  , acConcurrencyPrefix :: Maybe Text
  }
  deriving stock (Eq, Generic, Show)

-- | Default attempt limit stamped onto jobs whose 'maxAttempts' is unset.
defaultMaxAttempts :: Int32
defaultMaxAttempts = 10

-- | 24h in seconds, a convenience value for 'archiveFor'.
dayRetention :: Int32
dayRetention = 86400

-- | A rollup finalizer is any job whose 'parentState' snapshot is present
-- (an empty object on insert, the merged child results before a DLQ move).
isRollup :: Job p k q t adm -> Bool
isRollup = isJust . parentState

-- | Effective job status, derived (never stored) by the status SQL @CASE@ in the
-- templates module, which is its sole definition.
data JobStatus = Ready | InFlight | Backoff | Scheduled | Suspended | Throttled | Cancelled
  deriving stock (Bounded, Enum, Eq, Generic, Show)

jobStatusToText :: JobStatus -> Text
jobStatusToText Ready = "ready"
jobStatusToText InFlight = "in_flight"
jobStatusToText Backoff = "backoff"
jobStatusToText Scheduled = "scheduled"
jobStatusToText Suspended = "suspended"
jobStatusToText Throttled = "throttled"
jobStatusToText Cancelled = "cancelled"

-- | Reverse of 'jobStatusToText' over all constructors.
jobStatusFromTextMaybe :: Text -> Maybe JobStatus
jobStatusFromTextMaybe t = lookup t [(jobStatusToText s, s) | s <- [minBound .. maxBound]]

-- | Total reverse mapping, defaulting to 'Ready' for trusted SQL row decoding.
jobStatusFromText :: Text -> JobStatus
jobStatusFromText = fromMaybe Ready . jobStatusFromTextMaybe

instance ToJSON JobStatus where
  toJSON = toJSON . jobStatusToText

instance FromJSON JobStatus where
  parseJSON = withText "JobStatus" $ \t ->
    maybe (fail ("unknown job status: " <> show t)) pure (jobStatusFromTextMaybe t)

-- | Ungrouped 'JobWrite' with default values. For serial processing within a
-- group, use 'defaultGroupedJob'.
defaultJob :: payload -> JobWrite payload
defaultJob p =
  Job
    { primaryKey = ()
    , payload = p
    , queueName = ()
    , groupKey = Nothing
    , insertedAt = ()
    , updatedAt = Nothing
    , attempts = 0
    , lastError = Nothing
    , priority = 0
    , lastAttemptedAt = Nothing
    , notVisibleUntil = Nothing
    , dedupKey = Nothing
    , maxAttempts = Nothing
    , parentId = Nothing
    , parentState = Nothing
    , traceContext = Nothing
    , suspended = False
    , claimedBy = Nothing
    , archiveFor = Nothing
    , admission = ()
    }

-- | Grouped 'JobWrite'. Jobs sharing a group key are processed serially.
--
-- @
-- defaultGroupedJob "user-123" (ProcessEvent eventData)
-- @
defaultGroupedJob :: Text -> payload -> JobWrite payload
defaultGroupedJob gk p =
  Job
    { primaryKey = ()
    , payload = p
    , queueName = ()
    , groupKey = Just gk
    , insertedAt = ()
    , updatedAt = Nothing
    , attempts = 0
    , lastError = Nothing
    , priority = 0
    , lastAttemptedAt = Nothing
    , notVisibleUntil = Nothing
    , dedupKey = Nothing
    , maxAttempts = Nothing
    , parentId = Nothing
    , parentState = Nothing
    , traceContext = Nothing
    , suspended = False
    , claimedBy = Nothing
    , archiveFor = Nothing
    , admission = ()
    }

-- | A type alias for a job that has been read from the database.
type JobRead payload = Job payload Int64 Text UTCTime AdmissionKeys

-- | A type alias for a job that is ready to be written to the database.
-- It does not yet have an ID or insertion timestamp.
type JobWrite payload = Job payload () () () ()

-- | The full payload contract: JSON round-trip for JSONB storage plus the rate-limit and concurrency declarations (both default to unlimited).
type JobPayload payload =
  (FromJSON payload, ToJSON payload, HasRateLimit payload, HasConcurrency payload)

-- | The registry declares both admission policy kinds.
type RegistryAdmissionPolicies registry =
  (RegistryConcurrencyPolicies registry, RegistryRateLimitPolicies registry)

-- | Deduplication strategy, checked on INSERT via @ON CONFLICT@ on the dedup key.
-- | A job's W3C trace context.
data TraceContext = TraceContext
  { traceparent :: Text
  , tracestate :: Maybe Text
  }
  deriving stock (Eq, Generic, Show)

-- | A trace context from its two stored halves.
toTraceContext :: Maybe Text -> Maybe Text -> Maybe TraceContext
toTraceContext tp ts = flip TraceContext ts <$> tp

data DedupKey
  = -- | Skip if a job with this key exists (@DO NOTHING@).
    IgnoreDuplicate Text
  | -- | Replace the existing job with this key (@DO UPDATE@), unless it's
    -- actively in-flight on its first attempt.
    ReplaceDuplicate Text
  deriving stock (Eq, Generic, Show)

instance ToJSON DedupKey where
  toJSON (IgnoreDuplicate k) = object ["key" .= k, "strategy" .= ("ignore" :: Text)]
  toJSON (ReplaceDuplicate k) = object ["key" .= k, "strategy" .= ("replace" :: Text)]

instance FromJSON DedupKey where
  parseJSON = withObject "DedupKey" $ \v -> do
    key <- v .: "key"
    strategy <- v .: "strategy" :: Parser Text
    case strategy of
      "ignore" -> pure $ IgnoreDuplicate key
      "replace" -> pure $ ReplaceDuplicate key
      _ -> fail $ "Unknown dedup strategy: " <> show strategy

-- | The @dedup_key@ and @dedup_strategy@ column values for a 'DedupKey'.
dedupParts :: Maybe DedupKey -> (Maybe Text, Maybe Text)
dedupParts Nothing = (Nothing, Nothing)
dedupParts (Just (IgnoreDuplicate k)) = (Just k, Just "ignore")
dedupParts (Just (ReplaceDuplicate k)) = (Just k, Just "replace")

type ClaimTime = UTCTime
type CurrentTime = UTCTime
type StartTime = UTCTime
type EndTime = UTCTime
type ErrorMsg = Text
type BackoffDelay = NominalDiffTime

-- | A set of callbacks invoked at key points in the job lifecycle.
--
-- Use these hooks to integrate with metrics, logging, or tracing systems.
-- Hooks are exception-safe. Any exception thrown within a hook is caught
-- and ignored to prevent crashing the worker.
data ObservabilityHooks m payload = ObservabilityHooks
  { onJobClaimed
      :: (JobPayload payload)
      => JobRead payload
      -> ClaimTime
      -> m ()
  -- ^ Called immediately after a job is claimed by a worker.
  , onJobSuccess
      :: (JobPayload payload)
      => JobRead payload
      -> StartTime
      -> EndTime
      -> m ()
  -- ^ Called after a job handler succeeds. Use @diffUTCTime@ on the timestamps
  -- to calculate job duration.
  , onJobFailure
      :: (JobPayload payload)
      => JobRead payload
      -> ErrorMsg
      -> StartTime
      -> EndTime
      -> m ()
  -- ^ Called after a job handler fails and the job was retried or dead-lettered.
  -- A deliberate cancel reports through 'onJobCancelled' instead. Use @diffUTCTime@
  -- on the timestamps to calculate job duration.
  , onJobRetry
      :: (JobPayload payload)
      => JobRead payload
      -> BackoffDelay
      -> m ()
  -- ^ Called when a failed job is successfully scheduled for retry.
  , onJobFailedAndMovedToDLQ
      :: (JobPayload payload)
      => ErrorMsg
      -> JobRead payload
      -> m ()
  -- ^ Called when a job is successfully moved to the dead-letter queue.
  , onJobCancelled
      :: (JobPayload payload)
      => JobRead payload
      -> ErrorMsg
      -> m ()
  -- ^ Called when a handler cancelled the job's tree or branch and the rows were deleted.
  , onJobUnavailable
      :: (JobPayload payload)
      => JobRead payload
      -> ErrorMsg
      -> m ()
  -- ^ Called when a claimed job went away mid-flight and will not be retried here.
  , onJobHeartbeat
      :: (JobPayload payload)
      => JobRead payload
      -> CurrentTime
      -> StartTime
      -> m ()
  -- ^ Called periodically for a running job.
  }

-- | No-op hooks. Override fields to add observability:
--
-- @
-- myHooks = defaultObservabilityHooks
--   { onJobSuccess = \\job startTime endTime -> do
--       let duration = diffUTCTime endTime startTime
--       logInfo $ "Job " <> show (primaryKey job) <> " took " <> show duration
--   }
-- @
defaultObservabilityHooks :: (Applicative m) => ObservabilityHooks m payload
defaultObservabilityHooks =
  ObservabilityHooks
    { onJobClaimed = \_ _ -> pure ()
    , onJobSuccess = \_ _ _ -> pure ()
    , onJobFailure = \_ _ _ _ -> pure ()
    , onJobRetry = \_ _ -> pure ()
    , onJobFailedAndMovedToDLQ = \_ _ -> pure ()
    , onJobCancelled = \_ _ -> pure ()
    , onJobUnavailable = \_ _ -> pure ()
    , onJobHeartbeat = \_ _ _ -> pure ()
    }

-- | Runs both hooks at each lifecycle point, left before right. The right one runs
-- however the left ended, and the left's failure is the one that propagates.
instance (MonadUnliftIO m) => Semigroup (ObservabilityHooks m payload) where
  a <> b =
    ObservabilityHooks
      { onJobClaimed = \j t -> onJobClaimed a j t `andThen` onJobClaimed b j t
      , onJobSuccess = \j s e -> onJobSuccess a j s e `andThen` onJobSuccess b j s e
      , onJobFailure = \j msg s e -> onJobFailure a j msg s e `andThen` onJobFailure b j msg s e
      , onJobRetry = \j d -> onJobRetry a j d `andThen` onJobRetry b j d
      , onJobFailedAndMovedToDLQ = \msg j -> onJobFailedAndMovedToDLQ a msg j `andThen` onJobFailedAndMovedToDLQ b msg j
      , onJobCancelled = \j msg -> onJobCancelled a j msg `andThen` onJobCancelled b j msg
      , onJobUnavailable = \j msg -> onJobUnavailable a j msg `andThen` onJobUnavailable b j msg
      , onJobHeartbeat = \j c s -> onJobHeartbeat a j c s `andThen` onJobHeartbeat b j c s
      }

instance (MonadUnliftIO m) => Monoid (ObservabilityHooks m payload) where
  mempty = defaultObservabilityHooks

-- | base's @finally@, so the second action stays interruptible unlike UnliftIO's.
andThen :: (MonadUnliftIO m) => m () -> m () -> m ()
andThen first second = withRunInIO $ \run -> run first `E.finally` run second

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Job records, the enqueue setters, and the lifecycle hook types.
module Arbiter.Core.Job.Types
  ( -- * Core Job Type
    Job
  , AdmissionKeys (..)
  , AdmissionColumns (..)
  , JobRead
  , JobWrite
  , primaryKey
  , payload
  , queueName
  , groupKey
  , insertedAt
  , updatedAt
  , attempts
  , lastError
  , priority
  , lastAttemptedAt
  , notVisibleUntil
  , dedupKey
  , maxAttempts
  , parentId
  , parentState
  , traceContext
  , suspended
  , claimedBy
  , claimSeq
  , archiveFor
  , admission
  , defaultJob
  , defaultGroupedJob
  , setPayload
  , setGroupKey
  , setPriority
  , setNotVisibleUntil
  , setDedupKey
  , setMaxAttempts
  , setTraceContext
  , setArchiveFor
  , mapPayload
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
  , JobId
  , ClaimSeq
  , ClaimTime
  , CurrentTime
  , StartTime
  , EndTime
  , ErrorMsg
  , BackoffDelay
  ) where

import Control.Exception qualified as E
import Data.Aeson (FromJSON (..), ToJSON (..), withObject, (.!=), (.:), (.:?))
import Data.Int (Int32, Int64)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Time (NominalDiffTime, UTCTime)
import GHC.Generics (Generic)
import UnliftIO (MonadUnliftIO, withRunInIO)

import Arbiter.Core.Concurrency.Spec (ConcurrencyKey, HasConcurrency, RegistryConcurrencyPolicies)
import Arbiter.Core.Job.Dedup (DedupKey (..), dedupParts)
import Arbiter.Core.Job.Status (JobStatus (..), jobStatusFromText, jobStatusToText)
import Arbiter.Core.Job.TraceContext (TraceContext (..), toTraceContext)
import Arbiter.Core.Job.Types.Internal
  ( JobRecord (..)
  , admission
  , archiveFor
  , attempts
  , claimSeq
  , claimedBy
  , dedupKey
  , groupKey
  , insertedAt
  , lastAttemptedAt
  , lastError
  , maxAttempts
  , notVisibleUntil
  , parentId
  , parentState
  , payload
  , primaryKey
  , priority
  , queueName
  , suspended
  , traceContext
  , updatedAt
  )
import Arbiter.Core.RateLimit.Spec (HasRateLimit, RateLimitKey, RegistryRateLimitPolicies)

-- | A job parametrized over payload, primary key, queue name, insertion
-- timestamp, and admission metadata. The constructor is internal.
type Job payload key q insertedAt adm =
  JobRecord payload key q insertedAt adm

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
isRollup :: Job p Int64 q t adm -> Bool
isRollup = isJust . parentState

-- | A job read from the database.
type JobRead payload = Job payload Int64 Text UTCTime AdmissionKeys

-- | A job ready to enqueue. Arbiter owns claim, retry, parent, rollup, and
-- suspension state. Use the exported setters to configure enqueue fields.
type JobWrite payload = Job payload () () () ()

-- | Decode the complete persisted representation of a job.
instance (FromJSON payload) => FromJSON (JobRead payload) where
  parseJSON = withObject "Job" $ \v ->
    Job
      <$> v .: "primaryKey"
      <*> v .: "payload"
      <*> v .: "queueName"
      <*> v .: "groupKey"
      <*> v .: "insertedAt"
      <*> v .: "updatedAt"
      <*> v .: "attempts"
      <*> v .: "lastError"
      <*> v .: "priority"
      <*> v .: "lastAttemptedAt"
      <*> v .: "notVisibleUntil"
      <*> v .: "dedupKey"
      <*> v .: "maxAttempts"
      <*> v .:? "parentId"
      <*> v .:? "parentState"
      <*> (toTraceContext <$> v .:? "traceparent" <*> v .:? "tracestate")
      <*> v .:? "suspended" .!= False
      <*> v .:? "claimedBy"
      <*> v .:? "claimSeq" .!= 0
      <*> v .:? "archiveFor"
      <*> (AdmissionKeys <$> v .:? "rateLimit" <*> v .:? "concurrency")

-- | Ungrouped 'JobWrite' with default values. For serial processing within a
-- group, use 'defaultGroupedJob'.
defaultJob :: payload -> JobWrite payload
defaultJob value =
  Job
    { primaryKey = ()
    , payload = value
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
    , claimSeq = 0
    , archiveFor = Nothing
    , admission = ()
    }

-- | 'defaultJob' with a group key. Jobs sharing a group key are processed serially.
defaultGroupedJob :: Text -> payload -> JobWrite payload
defaultGroupedJob key = setGroupKey (Just key) . defaultJob

-- | Replace the payload.
setPayload :: payload' -> JobWrite payload -> JobWrite payload'
setPayload value job = job {payload = value}

-- | Set the group key.
setGroupKey :: Maybe Text -> JobWrite payload -> JobWrite payload
setGroupKey value job = job {groupKey = value}

-- | Set the claim priority. Lower numbers claim first.
setPriority :: Int32 -> JobWrite payload -> JobWrite payload
setPriority value job = job {priority = value}

-- | Delay the job's first visibility.
setNotVisibleUntil :: Maybe UTCTime -> JobWrite payload -> JobWrite payload
setNotVisibleUntil value job = job {notVisibleUntil = value}

-- | Set the dedup key.
setDedupKey :: Maybe DedupKey -> JobWrite payload -> JobWrite payload
setDedupKey value job = job {dedupKey = value}

-- | Override the queue's attempt limit.
setMaxAttempts :: Maybe Int32 -> JobWrite payload -> JobWrite payload
setMaxAttempts value job = job {maxAttempts = value}

-- | Attach a trace context.
setTraceContext :: Maybe TraceContext -> JobWrite payload -> JobWrite payload
setTraceContext value job = job {traceContext = value}

-- | Set the archive retention, in seconds.
setArchiveFor :: Maybe Int32 -> JobWrite payload -> JobWrite payload
setArchiveFor value job = job {archiveFor = value}

-- | Transform a job's payload without changing its stored metadata.
mapPayload
  :: (payload -> payload')
  -> Job payload key q insertedAt adm
  -> Job payload' key q insertedAt adm
mapPayload f job = job {payload = f (payload job)}

-- | The full payload contract: JSON round-trip for JSONB storage plus the rate-limit and concurrency declarations (both default to unlimited).
type JobPayload payload =
  (FromJSON payload, ToJSON payload, HasRateLimit payload, HasConcurrency payload)

-- | The registry declares both admission policy kinds.
type RegistryAdmissionPolicies registry =
  (RegistryConcurrencyPolicies registry, RegistryRateLimitPolicies registry)

-- | A job's primary key.
type JobId = Int64

-- | The token identifying one claim of a job.
type ClaimSeq = Int64

-- | When a claim was taken.
type ClaimTime = UTCTime

-- | The transaction's clock reading.
type CurrentTime = UTCTime

-- | When a handler began.
type StartTime = UTCTime

-- | When a handler finished.
type EndTime = UTCTime

-- | A failure message.
type ErrorMsg = Text

-- | How long to wait before the next attempt.
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
-- however the left ended, and when both throw the right's failure propagates.
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

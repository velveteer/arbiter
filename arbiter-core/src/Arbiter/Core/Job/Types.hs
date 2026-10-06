{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Job records, the enqueue setters, and the lifecycle hook types.
module Arbiter.Core.Job.Types
  ( -- * Core job type
    Job
  , HasKind (..)
  , constructorKind
  , constructorKinds
  , PayloadKeys (..)
  , JobRead
  , jobReadPairs
  , jobReadSeries
  , Stored
  , storedBytes
  , toStored
  , decodeStored
  , JobWrite
  , JobId
  , ClaimSeq
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
  , payloadKeys
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
  , minMaxAttempts
  , dayRetention
  , isRollup

    -- * Derived status
  , JobStatus (..)
  , jobStatusToText
  , jobStatusFromText

    -- * Type constraints
  , JobPayload
  , RegistryAdmissionPolicies

    -- * Deduplication
  , DedupKey (..)

    -- * Trace context
  , TraceContext (..)
  , toTraceContext

    -- * Observability
  , ObservabilityHooks (..)
  , defaultObservabilityHooks
  , hoistObservabilityHooks
  , andThen
  , ClaimTime
  , CurrentTime
  , StartTime
  , EndTime
  , ErrorMsg
  , BackoffDelay

    -- * Internal

    -- | Internal to the arbiter packages. Not covered by the PVP.
  , JobRecord
  , PayloadColumns (..)
  , dedupParts
  ) where

import Control.Exception qualified as E
import Data.Aeson
  ( FromJSON (..)
  , KeyValue (..)
  , ToJSON (..)
  , eitherDecodeStrict
  , encode
  , object
  , pairs
  , withObject
  , (.!=)
  , (.:)
  , (.:?)
  , (.=)
  )
import Data.Aeson.Encoding (Series)
import Data.Aeson.Types (Pair)
import Data.ByteString.Lazy qualified as BL
import Data.Int (Int32, Int64)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime)
import GHC.Generics (Generic)
import UnliftIO (MonadUnliftIO, withRunInIO)

import Arbiter.Core.Concurrency.Spec (ConcurrencyKey, HasConcurrency, RegistryConcurrencyPolicies)
import Arbiter.Core.Job.Dedup (DedupKey (..), dedupParts)
import Arbiter.Core.Job.Kind (HasKind (..), constructorKind, constructorKinds)
import Arbiter.Core.Job.Status (JobStatus (..), jobStatusFromText, jobStatusToText)
import Arbiter.Core.Job.TraceContext (TraceContext (..), toTraceContext)
import Arbiter.Core.Job.Types.Internal
  ( Job
  , JobRecord (..)
  , Stored (..)
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
  , payloadKeys
  , primaryKey
  , priority
  , queueName
  , storedBytes
  , suspended
  , traceContext
  , updatedAt
  )
import Arbiter.Core.RateLimit.Spec (HasRateLimit, RateLimitKey, RegistryRateLimitPolicies)

-- | The labels and keys a stored job carries from its payload, one field each.
data PayloadKeys = PayloadKeys
  { jobKind :: Maybe Text
  -- ^ From the payload's t'Arbiter.Core.Job.Kind.HasKind' instance.
  , jobRateLimitKey :: Maybe RateLimitKey
  -- ^ From the payload's t'Arbiter.RateLimit.HasRateLimit' instance.
  , jobConcurrencyKey :: Maybe ConcurrencyKey
  -- ^ From the payload's t'Arbiter.Concurrency.HasConcurrency' instance.
  }
  deriving stock (Eq, Generic, Show)

-- | The writable columns resolved from a payload at enqueue. The @kind@, @key@
-- and @prefix@ columns round-trip via 'PayloadKeys'. @cost@ is write-only.
data PayloadColumns = PayloadColumns
  { pcKind :: Maybe Text
  -- ^ The job's kind label.
  , pcRateLimitKey :: Maybe Text
  -- ^ The rate-limit bucket key.
  , pcRateLimitPrefix :: Maybe Text
  -- ^ The rate-limit policy prefix.
  , pcRateLimitCost :: Double
  -- ^ Tokens the job spends.
  , pcConcurrencyKey :: Maybe Text
  -- ^ The concurrency key.
  , pcConcurrencyPrefix :: Maybe Text
  -- ^ The concurrency policy prefix.
  }
  deriving stock (Eq, Generic, Show)

-- | Default attempt limit stamped onto jobs whose 'maxAttempts' is unset.
defaultMaxAttempts :: Int32
defaultMaxAttempts = 10

-- | Lowest attempt limit a job is stamped with. Every job gets at least one attempt.
minMaxAttempts :: Int32
minMaxAttempts = 1

-- | 24h in seconds, a convenience value for 'setArchiveFor'.
dayRetention :: Int32
dayRetention = 86400

-- | A rollup finalizer is any job whose 'parentState' snapshot is present
-- (an empty object from its insert or spawn, the merged child results before a DLQ move).
isRollup :: Job p Int64 q t adm -> Bool
isRollup = isJust . parentState

-- | A job read from the database.
type JobRead payload = Job payload Int64 Text UTCTime PayloadKeys

-- | A job ready to enqueue. Arbiter owns claim, retry, parent, rollup, and
-- suspension state. Use the exported setters to configure enqueue fields.
type JobWrite payload = Job payload () () () ()

-- | A payload's stored form.
toStored :: (ToJSON payload) => payload -> Stored payload
toStored = Stored . BL.toStrict . encode

-- | Decode a stored payload, or the reason the type rejects it.
decodeStored :: (FromJSON payload) => Stored payload -> Either Text payload
decodeStored = either (Left . ("Failed to decode job payload: " <>) . T.pack) Right . eitherDecodeStrict . storedBytes

-- | Decode the complete persisted representation of a job.
instance (FromJSON payload) => FromJSON (JobRead payload) where
  parseJSON = withObject "Job" $ \obj ->
    Job
      <$> obj .: "primaryKey"
      <*> obj .: "payload"
      <*> obj .: "queueName"
      <*> obj .: "groupKey"
      <*> obj .: "insertedAt"
      <*> obj .: "updatedAt"
      <*> obj .: "attempts"
      <*> obj .: "lastError"
      <*> obj .: "priority"
      <*> obj .: "lastAttemptedAt"
      <*> obj .: "notVisibleUntil"
      <*> obj .: "dedupKey"
      <*> obj .: "maxAttempts"
      <*> obj .:? "parentId"
      <*> obj .:? "parentState"
      <*> (toTraceContext <$> obj .:? "traceparent" <*> obj .:? "tracestate")
      <*> obj .:? "suspended" .!= False
      <*> obj .:? "claimedBy"
      <*> obj .:? "claimSeq" .!= 0
      <*> obj .:? "archiveFor"
      <*> (PayloadKeys <$> obj .:? "kind" <*> obj .:? "rateLimit" <*> obj .:? "concurrency")

instance (ToJSON payload) => ToJSON (JobRead payload) where
  toJSON = object . jobReadPairs
  toEncoding = pairs . jobReadSeries

-- | A job's JSON fields as key-value pairs.
jobReadPairs :: (ToJSON payload) => JobRead payload -> [Pair]
jobReadPairs = jobReadFields

-- | A job's JSON fields as an encoding series.
jobReadSeries :: (ToJSON payload) => JobRead payload -> Series
jobReadSeries = mconcat . jobReadFields

jobReadFields :: (KeyValue e kv, ToJSON payload) => JobRead payload -> [kv]
jobReadFields job =
  [ "primaryKey" .= primaryKey job
  , "payload" .= payload job
  , "queueName" .= queueName job
  , "groupKey" .= groupKey job
  , "insertedAt" .= insertedAt job
  , "updatedAt" .= updatedAt job
  , "attempts" .= attempts job
  , "lastError" .= lastError job
  , "priority" .= priority job
  , "lastAttemptedAt" .= lastAttemptedAt job
  , "notVisibleUntil" .= notVisibleUntil job
  , "dedupKey" .= dedupKey job
  , "maxAttempts" .= maxAttempts job
  , "parentId" .= parentId job
  , "parentState" .= parentState job
  , "isRollup" .= isRollup job
  , "traceparent" .= (traceparent <$> traceContext job)
  , "tracestate" .= (tracestate =<< traceContext job)
  , "suspended" .= suspended job
  , "claimedBy" .= claimedBy job
  , "claimSeq" .= claimSeq job
  , "archiveFor" .= archiveFor job
  , "kind" .= jobKind (payloadKeys job)
  , "rateLimit" .= jobRateLimitKey (payloadKeys job)
  , "concurrency" .= jobConcurrencyKey (payloadKeys job)
  ]

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
    , payloadKeys = ()
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

-- | Set the claim priority. Lower numbers claim first. In a group, a retried job goes first.
setPriority :: Int32 -> JobWrite payload -> JobWrite payload
setPriority value job = job {priority = value}

-- | Delay the job's first visibility.
setNotVisibleUntil :: Maybe UTCTime -> JobWrite payload -> JobWrite payload
setNotVisibleUntil value job = job {notVisibleUntil = value}

-- | Set the dedup key.
setDedupKey :: Maybe DedupKey -> JobWrite payload -> JobWrite payload
setDedupKey value job = job {dedupKey = value}

-- | Set the attempt limit. 'Nothing' takes 'defaultMaxAttempts'. Values below
-- 'minMaxAttempts' are raised to it.
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
mapPayload transform job = job {payload = transform (payload job)}

-- | The full payload contract. JSON round-trip for JSONB storage plus the kind,
-- rate-limit and concurrency declarations. All three default to none.
type JobPayload payload =
  (FromJSON payload, ToJSON payload, HasKind payload, HasRateLimit payload, HasConcurrency payload)

-- | The constraint that lets the migration collect a registry's declared policies of both kinds.
type RegistryAdmissionPolicies registry =
  (RegistryConcurrencyPolicies registry, RegistryRateLimitPolicies registry)

-- | A job's primary key.
type JobId = Int64

-- | The token identifying one claim of a job.
type ClaimSeq = Int64

-- | When a worker thread took the claimed job off its queue.
type ClaimTime = UTCTime

-- | The worker's clock reading after the extend landed.
type CurrentTime = UTCTime

-- | When the worker thread received the job's batch.
type StartTime = UTCTime

-- | For a success, when the ack committed. For a failure, when the handler failed the job.
type EndTime = UTCTime

-- | A failure message.
type ErrorMsg = Text

-- | How long to wait before the next attempt.
type BackoffDelay = NominalDiffTime

-- | Callbacks fired at each point of a job's lifecycle, for metrics, logging or tracing.
-- The worker catches an exception thrown inside one and logs it at Warning.
data ObservabilityHooks m payload = ObservabilityHooks
  { onJobClaimed
      :: (JobPayload payload)
      => JobRead payload
      -> ClaimTime
      -> m ()
  -- ^ Called when a worker thread starts a claimed job.
  , onJobSuccess
      :: (JobPayload payload)
      => JobRead payload
      -> StartTime
      -> EndTime
      -> m ()
  -- ^ Called when the job's ack or spawn commits. In batched mode this can occur while the
  -- handler runs.
  , onJobFailure
      :: (JobPayload payload)
      => JobRead payload
      -> ErrorMsg
      -> StartTime
      -> EndTime
      -> m ()
  -- ^ Called after a job handler fails and the job was retried or dead-lettered.
  -- A deliberate cancel reports through 'onJobCancelled'.
  , onJobRetry
      :: (JobPayload payload)
      => JobRead payload
      -> BackoffDelay
      -> m ()
  -- ^ Called when a failed job is successfully scheduled for retry.
  , onJobFailedAndMovedToDLQ
      :: (JobPayload payload)
      => JobRead payload
      -> ErrorMsg
      -> m ()
  -- ^ Called when a worker moves a job to the dead-letter queue after a handler failure.
  -- A reaper sweep or an undecodable row fires no hook.
  , onJobCancelled
      :: (JobPayload payload)
      => JobRead payload
      -> ErrorMsg
      -> m ()
  -- ^ Called when a handler or an operator force-cancel cancelled the job. It also fires
  -- when no rows were deleted.
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
  -- ^ Called after each landed extend, on its own thread. It can overlap an earlier
  -- call for the same job and run after the handler has finished.
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

-- | Hooks written in @m@, run in @n@ through the given natural transformation.
-- It must be a monad morphism, such as @lift@.
hoistObservabilityHooks :: (forall a. m a -> n a) -> ObservabilityHooks m payload -> ObservabilityHooks n payload
hoistObservabilityHooks nat hooks =
  ObservabilityHooks
    { onJobClaimed = \job claimTime -> nat (onJobClaimed hooks job claimTime)
    , onJobSuccess = \job start end -> nat (onJobSuccess hooks job start end)
    , onJobFailure = \job msg start end -> nat (onJobFailure hooks job msg start end)
    , onJobRetry = \job delay -> nat (onJobRetry hooks job delay)
    , onJobFailedAndMovedToDLQ = \job msg -> nat (onJobFailedAndMovedToDLQ hooks job msg)
    , onJobCancelled = \job msg -> nat (onJobCancelled hooks job msg)
    , onJobUnavailable = \job msg -> nat (onJobUnavailable hooks job msg)
    , onJobHeartbeat = \job now start -> nat (onJobHeartbeat hooks job now start)
    }

-- | Run both hooks at each lifecycle point, left before right. The right one runs
-- however the left ended. When both throw, the right's failure propagates.
instance (MonadUnliftIO m) => Semigroup (ObservabilityHooks m payload) where
  left <> right =
    ObservabilityHooks
      { onJobClaimed = \job claimTime -> onJobClaimed left job claimTime `andThen` onJobClaimed right job claimTime
      , onJobSuccess = \job start end -> onJobSuccess left job start end `andThen` onJobSuccess right job start end
      , onJobFailure = \job msg start end -> onJobFailure left job msg start end `andThen` onJobFailure right job msg start end
      , onJobRetry = \job delay -> onJobRetry left job delay `andThen` onJobRetry right job delay
      , onJobFailedAndMovedToDLQ = \job msg -> onJobFailedAndMovedToDLQ left job msg `andThen` onJobFailedAndMovedToDLQ right job msg
      , onJobCancelled = \job msg -> onJobCancelled left job msg `andThen` onJobCancelled right job msg
      , onJobUnavailable = \job msg -> onJobUnavailable left job msg `andThen` onJobUnavailable right job msg
      , onJobHeartbeat = \job now start -> onJobHeartbeat left job now start `andThen` onJobHeartbeat right job now start
      }

instance (MonadUnliftIO m) => Monoid (ObservabilityHooks m payload) where
  mempty = defaultObservabilityHooks

-- | base's @finally@. The second action stays interruptible.
andThen :: (MonadUnliftIO m) => m () -> m () -> m ()
andThen first second = withRunInIO $ \run -> run first `E.finally` run second

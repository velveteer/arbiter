{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Response types for the Arbiter REST API
module Arbiter.Servant.Types
  ( module Arbiter.Servant.Types
  , CronScheduleRow (..)
  , CronScheduleUpdate (..)
  , QueueRow (..)
  , WorkerRow (..)
  , RateLimitPolicyView (..)
  , RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)
  , ConcurrencyPolicyView (..)
  , ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)
  ) where

import Arbiter.Core.Admission (prefixedKeyPairs)
import Arbiter.Core.Concurrency.Spec (ConcurrencyKey)
import Arbiter.Core.Concurrency.Stats
  ( ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)
  , ConcurrencyPolicyView (..)
  )
import Arbiter.Core.CronSchedule (CronScheduleRow (..), CronScheduleUpdate (..))
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Types (Job (..), JobRead, JobStatus, JobWrite, isRollup)
import Arbiter.Core.Job.Types qualified as Arb
import Arbiter.Core.Operations (QueueStats)
import Arbiter.Core.Queues (QueueRow (..))
import Arbiter.Core.RateLimit.Spec (RateLimitKey (..))
import Arbiter.Core.RateLimit.Stats
  ( RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)
  , RateLimitPolicyView (..)
  )
import Arbiter.Core.Worker (WorkerRow (..))
import Data.Aeson (FromJSON (..), ToJSON (..), Value (..), object, withObject, (.!=), (.:), (.:?), (.=))
import Data.Aeson.Types (Pair)
import Data.Int (Int32, Int64)
import Data.Map.Strict (Map)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.UUID.Types (UUID)
import GHC.Generics (Generic)

newtype ApiJob payload = ApiJob {unApiJob :: JobRead payload}
  deriving stock (Eq, Show)

-- | A job row plus its SQL-derived status, for the list endpoint.
data ApiJobWithStatus payload = ApiJobWithStatus
  { ajwsJob :: JobRead payload
  , ajwsStatus :: JobStatus
  }
  deriving stock (Eq, Show)

-- | Write-side job type for REST API insertion.
--
-- Accepts @payload@, @groupKey@, @priority@, @notVisibleUntil@, @dedupKey@,
-- and @maxAttempts@. Fields like @parentId@, @parentState@, and @suspended@
-- are managed internally and cannot be set through the REST API.
data ApiJobWrite payload = ApiJobWrite
  { unApiJobWrite :: JobWrite payload
  , writeAdmission :: AdmissionRefs
  -- ^ Keys a raw-JSON writer attaches. A queue with a payload type rejects these.
  }
  deriving stock (Eq, Show)

-- | The admission keys a raw-JSON enqueue attaches, one per kind.
data AdmissionRefs = AdmissionRefs
  { refRateLimit :: Maybe RateLimitRef
  , refConcurrency :: Maybe ConcurrencyKey
  }
  deriving stock (Eq, Show)

-- | No keys attached: what a typed queue's payload-derived insert carries.
noAdmissionRefs :: AdmissionRefs
noAdmissionRefs = AdmissionRefs Nothing Nothing

-- | Did the caller attach any admission key at all?
hasAdmissionRefs :: AdmissionRefs -> Bool
hasAdmissionRefs (AdmissionRefs rl cc) = isJust rl || isJust cc

-- | Shared JSON field list for a job row. 'ApiJobWithStatus' appends @status@.
apiJobPairs :: (ToJSON payload) => JobRead payload -> [Pair]
apiJobPairs job =
  [ "primaryKey" .= primaryKey job
  , "payload" .= payload job
  , "queueName" .= Arb.queueName job
  , "groupKey" .= groupKey job
  , "insertedAt" .= insertedAt job
  , "updatedAt" .= Arb.updatedAt job
  , "attempts" .= attempts job
  , "lastError" .= lastError job
  , "priority" .= priority job
  , "lastAttemptedAt" .= lastAttemptedAt job
  , "notVisibleUntil" .= notVisibleUntil job
  , "dedupKey" .= dedupKey job
  , "maxAttempts" .= maxAttempts job
  , "parentId" .= parentId job
  , "parentState" .= parentState job
  , "traceparent" .= traceparent job
  , "tracestate" .= tracestate job
  , "isRollup" .= isRollup job
  , "suspended" .= suspended job
  , "claimedBy" .= Arb.claimedBy job
  , "rateLimit" .= Arb.jobRateLimitKey (Arb.admission job)
  , "concurrency" .= Arb.jobConcurrencyKey (Arb.admission job)
  ]

instance (ToJSON payload) => ToJSON (ApiJob payload) where
  toJSON (ApiJob job) = object (apiJobPairs job)

instance (ToJSON payload) => ToJSON (ApiJobWithStatus payload) where
  toJSON (ApiJobWithStatus job status) = object (apiJobPairs job <> ["status" .= status])

instance (FromJSON payload) => FromJSON (ApiJob payload) where
  parseJSON = withObject "Job" $ \v -> do
    job <-
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
        <*> v .:? "traceparent"
        <*> v .:? "tracestate"
        <*> v .:? "suspended" .!= False
        <*> v .:? "claimedBy"
        <*> (Arb.AdmissionKeys <$> v .:? "rateLimit" <*> v .:? "concurrency")
    pure $ ApiJob job

instance (FromJSON payload) => FromJSON (ApiJobWithStatus payload) where
  parseJSON v = do
    ApiJob job <- parseJSON v
    status <- withObject "JobWithStatus" (.: "status") v
    pure $ ApiJobWithStatus job status

instance (ToJSON payload) => ToJSON (ApiJobWrite payload) where
  toJSON (ApiJobWrite job adm) =
    object $
      [ "payload" .= payload job
      , "groupKey" .= groupKey job
      , "priority" .= priority job
      , "notVisibleUntil" .= notVisibleUntil job
      , "dedupKey" .= dedupKey job
      , "maxAttempts" .= maxAttempts job
      ]
        <> foldMap (\r -> ["rateLimit" .= r]) (refRateLimit adm)
        <> foldMap (\r -> ["concurrency" .= r]) (refConcurrency adm)

-- | Every field the client cannot set keeps its 'Arb.defaultJob' value: the server
-- resolves admission, the request headers carry the trace context.
instance (FromJSON payload) => FromJSON (ApiJobWrite payload) where
  parseJSON = withObject "JobWrite" $ \v -> do
    base <- Arb.defaultJob <$> v .: "payload"
    gk <- v .:? "groupKey"
    pr <- v .:? "priority" .!= 0
    nvu <- v .:? "notVisibleUntil"
    dk <- v .:? "dedupKey"
    ma <- v .:? "maxAttempts"
    let job =
          base
            { groupKey = gk
            , priority = pr
            , notVisibleUntil = nvu
            , dedupKey = dk
            , maxAttempts = ma
            }
    ApiJobWrite job <$> (AdmissionRefs <$> v .:? "rateLimit" <*> v .:? "concurrency")

newtype ApiDLQJob payload = ApiDLQJob {unApiDLQJob :: DLQ.DLQJob payload}
  deriving stock (Eq, Show)

instance (ToJSON payload) => ToJSON (ApiDLQJob payload) where
  toJSON (ApiDLQJob dlq) =
    object
      [ "dlqPrimaryKey" .= DLQ.dlqPrimaryKey dlq
      , "failedAt" .= DLQ.failedAt dlq
      , "jobSnapshot" .= ApiJob (DLQ.jobSnapshot dlq)
      ]

instance (FromJSON payload) => FromJSON (ApiDLQJob payload) where
  parseJSON = withObject "DLQJob" $ \v -> do
    apiJob <- v .: "jobSnapshot"
    dlq <-
      DLQ.DLQJob
        <$> v .: "dlqPrimaryKey"
        <*> v .: "failedAt"
        <*> pure (unApiJob apiJob)
    pure $ ApiDLQJob dlq

-- ---------------------------------------------------------------------------
-- Consumer protocol (claim / ack / nack / extend)
-- ---------------------------------------------------------------------------

-- | Claim up to @maxJobs@ visible jobs, leasing each for @visibilitySecs@
-- (default 60). @maxJobs@ defaults to 1.
data ClaimRequest = ClaimRequest
  { crMaxJobs :: Int
  , crVisibilitySecs :: Maybe Double
  }
  deriving stock (Eq, Show)

instance FromJSON ClaimRequest where
  parseJSON = withObject "ClaimRequest" $ \v ->
    ClaimRequest <$> v .:? "maxJobs" .!= 1 <*> v .:? "visibilitySecs"

-- | The jobs a claim leased. Each 'ApiJob' carries its @primaryKey@ and
-- @attempts@ - the lease proof a client echoes back to ack, nack, or extend.
newtype ClaimResponse = ClaimResponse {claimedJobs :: [ApiJob Value]}
  deriving stock (Generic)

instance ToJSON ClaimResponse where
  toJSON (ClaimResponse js) = object ["jobs" .= js]

instance FromJSON ClaimResponse where
  parseJSON = withObject "ClaimResponse" $ \v -> ClaimResponse <$> v .: "jobs"

-- | Complete a claimed job. @claimedBy@ (the unforgeable claimant id from the
-- claim) and @attempts@ together are the lease proof. @result@ is stored for a
-- parent rollup when present.
data AckRequest = AckRequest
  { arAttempts :: Int32
  , arClaimedBy :: UUID
  , arResult :: Maybe Value
  }
  deriving stock (Eq, Show)

instance FromJSON AckRequest where
  parseJSON = withObject "AckRequest" $ \v ->
    AckRequest <$> v .: "attempts" <*> v .: "claimedBy" <*> v .:? "result"

-- | Soft-return a claimed job for reprocessing, no attempt consumed.
data NackRequest = NackRequest
  { nrAttempts :: Int32
  , nrClaimedBy :: UUID
  }
  deriving stock (Eq, Show)

instance FromJSON NackRequest where
  parseJSON = withObject "NackRequest" $ \v ->
    NackRequest <$> v .: "attempts" <*> v .: "claimedBy"

-- | Extend a claimed job's lease (a client-driven heartbeat). @visibilitySecs@
-- defaults to 60.
data ExtendRequest = ExtendRequest
  { erAttempts :: Int32
  , erClaimedBy :: UUID
  , erVisibilitySecs :: Double
  }
  deriving stock (Eq, Show)

instance FromJSON ExtendRequest where
  parseJSON = withObject "ExtendRequest" $ \v ->
    ExtendRequest <$> v .: "attempts" <*> v .: "claimedBy" <*> v .:? "visibilitySecs" .!= 60

-- | Report a claimed job as failed. @permanent@ dead-letters it regardless of the
-- attempt budget, otherwise it retries with backoff until the budget runs out.
-- @retryDelaySecs@ overrides the computed backoff.
data FailRequest = FailRequest
  { frAttempts :: Int32
  , frClaimedBy :: UUID
  , frError :: Text
  , frPermanent :: Bool
  , frRetryDelaySecs :: Maybe Double
  }
  deriving stock (Eq, Show)

instance FromJSON FailRequest where
  parseJSON = withObject "FailRequest" $ \v ->
    FailRequest
      <$> v .: "attempts"
      <*> v .: "claimedBy"
      <*> v .: "error"
      <*> v .:? "permanent" .!= False
      <*> v .:? "retryDelaySecs"

-- | What a reported failure did to the job.
data FailOutcome = Retried | DeadLettered
  deriving stock (Eq, Show)

instance ToJSON FailOutcome where
  toJSON Retried = String "retry"
  toJSON DeadLettered = String "dlq"

-- | The outcome of a reported failure, and when a retried job comes back.
data FailResponse = FailResponse
  { failOutcome :: FailOutcome
  , failRetryInSecs :: Maybe Double
  }
  deriving stock (Eq, Show)

instance ToJSON FailResponse where
  toJSON r = object ["outcome" .= failOutcome r, "retryInSecs" .= failRetryInSecs r]

-- | A rate-limit key attached to a runtime enqueue, plus the token @cost@ it
-- debits (default 1).
data RateLimitRef = RateLimitRef
  { rlKey :: RateLimitKey
  , rlCost :: Double
  }
  deriving stock (Eq, Show)

instance FromJSON RateLimitRef where
  parseJSON = withObject "rateLimit" $ \o ->
    RateLimitRef <$> parseJSON (Object o) <*> o .:? "cost" .!= 1

instance ToJSON RateLimitRef where
  toJSON r = object (prefixedKeyPairs (rlkPrefix (rlKey r)) (rlkSuffix (rlKey r)) <> ["cost" .= rlCost r])

-- | Single-job response envelope, parameterized over the job representation
-- ('ApiJob' for insert, 'ApiJobWithStatus' for the detail endpoint).
newtype JobResponse a = JobResponse
  { job :: a
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Response wrapper for multiple jobs
data JobsResponse payload = JobsResponse
  { jobs :: [ApiJobWithStatus payload]
  , jobsTotal :: Int
  , jobsOffset :: Int
  , jobsLimit :: Int
  , childCounts :: Map Int64 Int64
  , pausedParents :: [Int64]
  , dlqChildCounts :: Map Int64 Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Response wrapper for DLQ jobs
data DLQResponse payload = DLQResponse
  { dlqJobs :: [ApiDLQJob payload]
  , dlqTotal :: Int
  , dlqOffset :: Int
  , dlqLimit :: Int
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Queue statistics response
data StatsResponse = StatsResponse
  { stats :: QueueStats
  , timestamp :: Text
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | One queue's stats in the bulk landing response, with pause state: the queue's
-- own paused flag, plus how many of its live workers are paused.
data QueueStatsEntry = QueueStatsEntry
  { queue :: Text
  , stats :: QueueStats
  , paused :: Bool
  , workersLive :: Int
  , workersPaused :: Int
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Every queue's stats, for the landing overview.
data AllStatsResponse = AllStatsResponse
  { queues :: [QueueStatsEntry]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Queues list response
data QueuesResponse = QueuesResponse
  { queues :: [Text]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Request body for batch job insert
newtype BatchInsertRequest payload = BatchInsertRequest
  { jobWrites :: [ApiJobWrite payload]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Response body for batch job insert
data BatchInsertResponse payload = BatchInsertResponse
  { inserted :: [ApiJob payload]
  , insertedCount :: Int
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Request body for batch DLQ delete
data BatchDeleteRequest = BatchDeleteRequest
  { ids :: [Int64]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Response body for batch DLQ delete
data BatchDeleteResponse = BatchDeleteResponse
  { deleted :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Cron schedules response
data CronSchedulesResponse = CronSchedulesResponse
  { cronSchedules :: [CronScheduleRow]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Worker registry response
data WorkersResponse = WorkersResponse
  { workers :: [WorkerRow]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Rate-limit policies response
data RateLimitPoliciesResponse = RateLimitPoliciesResponse
  { policies :: [RateLimitPolicyView]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Rate-limit buckets response (one prefix's keys)
data RateLimitBucketsResponse = RateLimitBucketsResponse
  { buckets :: [RateLimitBucketView]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Number of buckets cleared by a reset.
data RateLimitResetResponse = RateLimitResetResponse
  { reset :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Concurrency pools response
data ConcurrencyPoliciesResponse = ConcurrencyPoliciesResponse
  { policies :: [ConcurrencyPolicyView]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Concurrency keys response (one prefix's keys)
data ConcurrencyKeysResponse = ConcurrencyKeysResponse
  { keys :: [ConcurrencyKeyView]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Number of count rows repaired from live jobs.
data ConcurrencyReconcileResponse = ConcurrencyReconcileResponse
  { reconciled :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

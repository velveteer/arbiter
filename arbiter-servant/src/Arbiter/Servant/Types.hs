{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Response types for the Arbiter REST API.
module Arbiter.Servant.Types
  ( module Arbiter.Servant.Types
  , CronScheduleRow (..)
  , CronScheduleUpdate (..)
  , QueueOverview (..)
  , QueueRow (..)
  , WorkerRow (..)
  , RateLimitPolicyView (..)
  , RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)
  , ConcurrencyPolicyView (..)
  , ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)
  , PgDbHealth (..)
  ) where

import Arbiter.Core.Concurrency.Stats
  ( ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)
  , ConcurrencyPolicyView (..)
  )
import Arbiter.Core.CronSchedule (CronScheduleRow (..), CronScheduleUpdate (..))
import Arbiter.Core.Health (PgDbHealth (..))
import Arbiter.Core.Job.Archive qualified as Archive
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Types
  ( JobRead
  , JobStatus
  , JobWrite
  , isRollup
  , traceparent
  , tracestate
  )
import Arbiter.Core.Job.Types qualified as Arb
import Arbiter.Core.Operations (QueueOverview (..), QueueStats)
import Arbiter.Core.Queues (QueueRow (..))
import Arbiter.Core.RateLimit.Stats
  ( RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)
  , RateLimitPolicyView (..)
  )
import Arbiter.Core.Worker (WorkerRow (..))
import Data.Aeson
  ( FromJSON (..)
  , ToJSON (..)
  , Value (Object)
  , object
  , withObject
  , withText
  , (.!=)
  , (.:)
  , (.:!)
  , (.:?)
  , (.=)
  )
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Pair)
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Text (Text)
import Data.Time.Clock (UTCTime)
import Data.UUID.Types (UUID)
import GHC.Generics (Generic)

-- | A job in its JSON wire shape.
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
newtype ApiJobWrite payload = ApiJobWrite {unApiJobWrite :: JobWrite payload}
  deriving newtype (Eq, Show)

-- | Shared JSON field list for a job row. 'ApiJobWithStatus' appends @status@.
apiJobPairs :: (ToJSON payload) => JobRead payload -> [Pair]
apiJobPairs job =
  [ "primaryKey" .= Arb.primaryKey job
  , "payload" .= Arb.payload job
  , "queueName" .= Arb.queueName job
  , "groupKey" .= Arb.groupKey job
  , "insertedAt" .= Arb.insertedAt job
  , "updatedAt" .= Arb.updatedAt job
  , "attempts" .= Arb.attempts job
  , "lastError" .= Arb.lastError job
  , "priority" .= Arb.priority job
  , "lastAttemptedAt" .= Arb.lastAttemptedAt job
  , "notVisibleUntil" .= Arb.notVisibleUntil job
  , "dedupKey" .= Arb.dedupKey job
  , "maxAttempts" .= Arb.maxAttempts job
  , "parentId" .= Arb.parentId job
  , "parentState" .= Arb.parentState job
  , "isRollup" .= isRollup job
  , "traceparent" .= (traceparent <$> Arb.traceContext job)
  , "tracestate" .= (tracestate =<< Arb.traceContext job)
  , "suspended" .= Arb.suspended job
  , "claimedBy" .= Arb.claimedBy job
  , "claimSeq" .= Arb.claimSeq job
  , "archiveFor" .= Arb.archiveFor job
  , "kind" .= Arb.jobKind (Arb.payloadKeys job)
  , "rateLimit" .= Arb.jobRateLimitKey (Arb.payloadKeys job)
  , "concurrency" .= Arb.jobConcurrencyKey (Arb.payloadKeys job)
  ]

instance (ToJSON payload) => ToJSON (ApiJob payload) where
  toJSON (ApiJob job) = object (apiJobPairs job)

instance (ToJSON payload) => ToJSON (ApiJobWithStatus payload) where
  toJSON (ApiJobWithStatus job status) = object (apiJobPairs job <> ["status" .= status])

instance (FromJSON payload) => FromJSON (ApiJob payload) where
  parseJSON value = ApiJob <$> parseJSON value

instance (FromJSON payload) => FromJSON (ApiJobWithStatus payload) where
  parseJSON value = do
    ApiJob job <- parseJSON value
    status <- withObject "JobWithStatus" (.: "status") value
    pure $ ApiJobWithStatus job status

instance (ToJSON payload) => ToJSON (ApiJobWrite payload) where
  toJSON (ApiJobWrite job) =
    object
      [ "payload" .= Arb.payload job
      , "groupKey" .= Arb.groupKey job
      , "priority" .= Arb.priority job
      , "notVisibleUntil" .= Arb.notVisibleUntil job
      , "dedupKey" .= Arb.dedupKey job
      , "maxAttempts" .= Arb.maxAttempts job
      , "archiveFor" .= Arb.archiveFor job
      ]

instance (FromJSON payload) => FromJSON (ApiJobWrite payload) where
  parseJSON = withObject "JobWrite" $ \obj -> do
    payload <- obj .: "payload"
    group <- obj .:? "groupKey"
    priority <- obj .:? "priority" .!= 0
    visibleAt <- obj .:? "notVisibleUntil"
    dedup <- obj .:? "dedupKey"
    attempts <- obj .:? "maxAttempts"
    retention <- obj .:! "archiveFor" .!= Nothing
    pure . ApiJobWrite
      $ Arb.setArchiveFor retention
      $ Arb.setMaxAttempts attempts
      $ Arb.setDedupKey dedup
      $ Arb.setNotVisibleUntil visibleAt
      $ Arb.setPriority priority
      $ Arb.setGroupKey group
      $ Arb.defaultJob payload

-- | A DLQ entry in its JSON wire shape.
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
  parseJSON = withObject "DLQJob" $ \obj -> do
    apiJob <- obj .: "jobSnapshot"
    dlq <-
      DLQ.DLQJob
        <$> obj .: "dlqPrimaryKey"
        <*> obj .: "failedAt"
        <*> pure (unApiJob apiJob)
    pure $ ApiDLQJob dlq

-- | An archived job in its JSON wire shape.
newtype ApiArchiveJob payload = ApiArchiveJob {unApiArchiveJob :: Archive.ArchiveJob payload}
  deriving stock (Eq, Show)

instance (ToJSON payload) => ToJSON (ApiArchiveJob payload) where
  toJSON (ApiArchiveJob archived) =
    object
      [ "archivePrimaryKey" .= Archive.archivePrimaryKey archived
      , "completedAt" .= Archive.completedAt archived
      , "jobSnapshot" .= ApiJob (Archive.jobSnapshot archived)
      , "result" .= Archive.archivedResult archived
      ]

instance (FromJSON payload) => FromJSON (ApiArchiveJob payload) where
  parseJSON = withObject "ArchiveJob" $ \obj -> do
    apiJob <- obj .: "jobSnapshot"
    archived <-
      Archive.ArchiveJob
        <$> obj .: "archivePrimaryKey"
        <*> obj .: "completedAt"
        <*> pure (unApiJob apiJob)
        <*> obj .:? "result"
    pure $ ApiArchiveJob archived

-- | Response wrapper for archived jobs.
data ArchiveResponse payload = ArchiveResponse
  { archiveJobs :: [ApiArchiveJob payload]
  , archiveTotal :: Int
  , archiveOffset :: Int
  , archiveLimit :: Int
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Single-job response envelope, parameterized over the job representation
-- ('ApiJob' for insert, 'ApiJobWithStatus' for the detail endpoint).
newtype JobResponse a = JobResponse
  { job :: a
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Response wrapper for multiple jobs.
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

-- | A consumer's request to lease visible jobs.
data ClaimRequest = ClaimRequest
  { crMaxJobs :: Maybe Int
  , crLeaseSeconds :: Maybe Double
  }
  deriving stock (Eq, Show)

instance FromJSON ClaimRequest where
  parseJSON = withObject "ClaimRequest" $ \obj ->
    ClaimRequest <$> obj .:? "maxJobs" <*> obj .:? "leaseSeconds"

instance ToJSON ClaimRequest where
  toJSON req = object ["maxJobs" .= crMaxJobs req, "leaseSeconds" .= crLeaseSeconds req]

-- | Jobs returned by one claim. Each job contains the lease fields required for
-- finalization.
newtype ClaimResponse payload = ClaimResponse {claimedJobs :: [ApiJob payload]}
  deriving stock (Eq, Show)

instance (ToJSON payload) => ToJSON (ClaimResponse payload) where
  toJSON (ClaimResponse claimed) = object ["jobs" .= claimed]

instance (FromJSON payload) => FromJSON (ClaimResponse payload) where
  parseJSON = withObject "ClaimResponse" $ \obj -> ClaimResponse <$> obj .: "jobs"

-- | Proof that the caller still holds a claimed job.
data JobLease = JobLease
  { jlClaimSeq :: Int64
  , jlClaimedBy :: UUID
  }
  deriving stock (Eq, Show)

instance FromJSON JobLease where
  parseJSON = withObject "JobLease" $ \obj ->
    JobLease <$> obj .: "claimSeq" <*> obj .: "claimedBy"

-- | Shared JSON fields for a lease. Request encoders append request-specific fields.
jobLeasePairs :: JobLease -> [Pair]
jobLeasePairs lease = ["claimSeq" .= jlClaimSeq lease, "claimedBy" .= jlClaimedBy lease]

instance ToJSON JobLease where
  toJSON = object . jobLeasePairs

-- | A lease and an optional stored result. An absent result performs a plain ack.
data AckRequest result = AckRequest
  { arLease :: JobLease
  , arResult :: Maybe result
  }
  deriving stock (Eq, Show)

instance (FromJSON result) => FromJSON (AckRequest result) where
  parseJSON = withObject "AckRequest" $ \obj ->
    AckRequest <$> parseJSON (Object obj) <*> obj .:? "result"

instance (ToJSON result) => ToJSON (AckRequest result) where
  toJSON req = object (jobLeasePairs (arLease req) <> foldMap (\stored -> ["result" .= stored]) (arResult req))

-- | A lease plus the window to hide the job for, counted from now.
data ExtendRequest = ExtendRequest
  { erLease :: JobLease
  , erSeconds :: Double
  }
  deriving stock (Eq, Show)

instance FromJSON ExtendRequest where
  parseJSON = withObject "ExtendRequest" $ \obj ->
    ExtendRequest <$> parseJSON (Object obj) <*> obj .: "seconds"

instance ToJSON ExtendRequest where
  toJSON req = object (jobLeasePairs (erLease req) <> ["seconds" .= erSeconds req])

-- | Rows each maintenance operation touched, and the operations that raised. An
-- operation in neither was skipped.
data MaintenanceResponse = MaintenanceResponse
  { maintenanceOps :: Map Text Int64
  , maintenanceFailed :: [Text]
  }
  deriving stock (Eq, Show)

instance ToJSON MaintenanceResponse where
  toJSON (MaintenanceResponse ops failed) = object ["ops" .= ops, "failed" .= failed]

instance FromJSON MaintenanceResponse where
  parseJSON = withObject "MaintenanceResponse" $ \obj ->
    MaintenanceResponse <$> obj .: "ops" <*> obj .:? "failed" .!= []

-- | Response wrapper for DLQ jobs.
data DLQResponse payload = DLQResponse
  { dlqJobs :: [ApiDLQJob payload]
  , dlqTotal :: Int
  , dlqOffset :: Int
  , dlqLimit :: Int
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Queue statistics response.
data StatsResponse = StatsResponse
  { stats :: QueueStats
  , timestamp :: Text
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Every queue's stats, for the landing overview.
data AllStatsResponse = AllStatsResponse
  { queues :: [QueueOverview]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Queues list response.
data QueuesResponse = QueuesResponse
  { queues :: [Text]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Request body for batch job insert.
newtype BatchInsertRequest payload = BatchInsertRequest
  { jobWrites :: [ApiJobWrite payload]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Response body for batch job insert.
data BatchInsertResponse payload = BatchInsertResponse
  { inserted :: [ApiJob payload]
  , insertedCount :: Int
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Request body for batch DLQ delete.
data BatchDeleteRequest = BatchDeleteRequest
  { ids :: [Int64]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Response body for batch DLQ delete.
data BatchDeleteResponse = BatchDeleteResponse
  { deleted :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | A cron schedule row plus the next tick its effective expression fires at.
-- An expression that never fires again, or one the server cannot parse, has none.
data CronScheduleView = CronScheduleView
  { schedule :: CronScheduleRow
  , nextRunAt :: Maybe UTCTime
  }
  deriving stock (Eq, Generic, Show)

-- | The row's own fields, with @nextRunAt@ alongside them.
instance ToJSON CronScheduleView where
  toJSON view = case toJSON (schedule view) of
    Object obj -> Object (KM.insert "nextRunAt" (toJSON (nextRunAt view)) obj)
    other -> other

instance FromJSON CronScheduleView where
  parseJSON = withObject "CronScheduleView" $ \obj ->
    CronScheduleView <$> parseJSON (Object obj) <*> obj .:? "nextRunAt"

-- | Cron schedules response.
data CronSchedulesResponse = CronSchedulesResponse
  { cronSchedules :: [CronScheduleView]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Worker registry response.
data WorkersResponse = WorkersResponse
  { workers :: [WorkerRow]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Rate-limit policies response.
data RateLimitPoliciesResponse = RateLimitPoliciesResponse
  { policies :: [RateLimitPolicyView]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Rate-limit buckets response (one prefix's keys).
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

-- | Concurrency pools response.
data ConcurrencyPoliciesResponse = ConcurrencyPoliciesResponse
  { policies :: [ConcurrencyPolicyView]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Concurrency keys response (one prefix's keys).
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

-- | Whether the API can reach its database.
data HealthStatus = Ok | Down
  deriving stock (Eq, Generic, Show)

instance ToJSON HealthStatus where
  toJSON = toJSON . healthStatusToText

instance FromJSON HealthStatus where
  parseJSON = withText "HealthStatus" $ \txt -> case txt of
    "ok" -> pure Ok
    "down" -> pure Down
    _ -> fail "expected ok or down"

healthStatusToText :: HealthStatus -> Text
healthStatusToText = \case
  Ok -> "ok"
  Down -> "down"

-- | Readiness of the API and the database behind it.
data HealthResponse = HealthResponse
  { status :: HealthStatus
  , schemaName :: Text
  , checkedAt :: UTCTime
  , dbLatencyMs :: Maybe Double
  -- ^ Nothing when the database could not be reached.
  , db :: Maybe PgDbHealth
  -- ^ Connection and age counters, absent when the database is unreachable.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | The process is running. Answered without touching the database.
data LivenessResponse = LivenessResponse
  { alive :: Bool
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

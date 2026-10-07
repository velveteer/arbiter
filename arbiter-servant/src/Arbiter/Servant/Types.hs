{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Request and response types for the Arbiter REST API.
module Arbiter.Servant.Types
  ( -- * Jobs
    ApiJobWithStatus (..)
  , ApiJobWrite (..)
  , JobResponse (..)
  , JobsResponse (..)
  , RescheduleRequest (..)
  , BatchInsertRequest (..)
  , BatchInsertResponse (..)

    -- * DLQ and archive
  , PayloadEdit (..)
  , OptionalJSON
  , BatchDeleteRequest (..)
  , BatchDeleteResponse (..)

    -- * Pages
  , Page (..)
  , PageSize (..)
  , PageLimit
  , KeyPageLimit
  , pageSizeDefault
  , pageLimitRange
  , Items (..)
  , ArchiveResponse
  , DLQResponse
  , GroupsResponse

    -- * Leases
  , ClaimRequest (..)
  , claimJobsRange
  , defaultClaimJobs
  , leaseSecondsRange
  , defaultLeaseSeconds
  , ClaimResponse (..)
  , JobLease (..)
  , AckRequest (..)
  , ExtendRequest (..)

    -- * Queues and stats
  , StatsResponse (..)
  , AllStatsResponse (..)
  , QueuesResponse (..)

    -- * Maintenance
  , MaintenanceResponse (..)

    -- * Cron and workers
  , CronScheduleView (..)
  , CronSchedulesResponse (..)
  , WorkersResponse (..)

    -- * Admission policies
  , RateLimitPoliciesResponse (..)
  , RateLimitBucketsResponse
  , RateLimitResetResponse (..)
  , AddTokensRequest (..)
  , AddTokensResponse (..)
  , PruneResponse (..)
  , ConcurrencyPoliciesResponse (..)
  , ConcurrencyKeysResponse
  , ConcurrencyReconcileResponse (..)

    -- * Health
  , HealthStatus (..)
  , healthStatusToText
  , HealthResponse (..)
  , LivenessResponse (..)

    -- * Re-exported core types
  , CronScheduleRow (..)
  , CronScheduleUpdate (..)
  , QueueOverview (..)
  , QueueStats (..)
  , QueueRow (..)
  , WorkerRow (..)
  , RateLimitPolicyView (..)
  , RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)
  , ConcurrencyPolicyView (..)
  , ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)
  , PgDbHealth (..)
  , GroupSummary (..)

    -- * Internal

    -- | Internal to the arbiter packages. Not covered by the PVP.
  , bodylessContentType
  , jobLeasePairs
  ) where

import Arbiter.Core.Concurrency.Stats
  ( ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)
  , ConcurrencyPolicyView (..)
  )
import Arbiter.Core.CronSchedule (CronScheduleRow (..), CronScheduleUpdate (..))
import Arbiter.Core.Enum (enumFromText)
import Arbiter.Core.Health (PgDbHealth (..))
import Arbiter.Core.Job.Archive qualified as Archive
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Types (JobRead, JobStatus, JobWrite, Stored, jobReadPairs, jobReadSeries)
import Arbiter.Core.Job.Types qualified as Arb
import Arbiter.Core.Operations (GroupSummary (..), QueueOverview (..), QueueStats (..))
import Arbiter.Core.Queues (QueueRow (..))
import Arbiter.Core.RateLimit.Stats
  ( RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)
  , RateLimitPolicyView (..)
  )
import Arbiter.Core.Worker (WorkerRow (..))
import Data.Aeson
  ( FromJSON (..)
  , KeyValue
  , ToJSON (..)
  , Value (Object)
  , eitherDecode
  , encode
  , object
  , pairs
  , withObject
  , withText
  , (.!=)
  , (.:)
  , (.:!)
  , (.:?)
  , (.=)
  )
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Object, Pair, Parser)
import Data.ByteString.Lazy.Char8 qualified as LBS8
import Data.Char (isSpace)
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Proxy (Proxy (..))
import Data.String (IsString)
import Data.Text (Text, unpack)
import Data.Time.Clock (UTCTime)
import Data.UUID.Types (UUID)
import GHC.Generics (Generic, Generically (..))
import GHC.TypeLits (KnownNat, Nat, natVal)
import Servant.API (Accept (..), FromHttpApiData, JSON, MimeRender (..), MimeUnrender (..), ToHttpApiData)

-- | A job row plus its SQL-derived status, for the list and detail endpoints.
data ApiJobWithStatus payload = ApiJobWithStatus
  { ajwsJob :: JobRead payload
  -- ^ The job. Its fields are flattened into the object.
  , ajwsStatus :: JobStatus
  }
  deriving stock (Eq, Show)

-- | Write-side job for REST insertion. Lineage, suspension and trace fields are not settable.
newtype ApiJobWrite payload = ApiJobWrite
  { unApiJobWrite :: JobWrite payload
  -- ^ The job to insert. Its settable fields are flattened into the object.
  }
  deriving newtype (Eq, Show)

instance (ToJSON payload) => ToJSON (ApiJobWithStatus payload) where
  toJSON (ApiJobWithStatus job status) = object (jobReadPairs job <> ["status" .= status])
  toEncoding (ApiJobWithStatus job status) = pairs (jobReadSeries job <> "status" .= status)

instance (FromJSON payload) => FromJSON (ApiJobWithStatus payload) where
  parseJSON value = ApiJobWithStatus <$> parseJSON value <*> withObject "JobWithStatus" (.: "status") value

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

-- | Page size of a listing, with the size it takes when the request omits it.
newtype PageSize (def :: Nat) = PageSize {unPageSize :: Int}
  deriving newtype (Eq, FromHttpApiData, Show, ToHttpApiData)

-- | Page size of a job, DLQ, archive or group listing.
type PageLimit = PageSize 50

-- | Page size of a bucket or key listing.
type KeyPageLimit = PageSize 100

-- | Bounds on page size.
pageLimitRange :: (Int, Int)
pageLimitRange = (1, 1000)

-- | The page size a listing takes when the request omits it.
pageSizeDefault :: (KnownNat def) => proxy def -> Int
pageSizeDefault = fromInteger . natVal

-- | One page of a list, with the size of the full list.
data Page a = Page
  { pageItems :: [a]
  , pageTotal :: Int
  -- ^ Rows that match the filters, across all pages.
  , pageOffset :: Int
  , pageLimit :: Int
  -- ^ Page size, clamped to 'pageLimitRange'.
  }
  deriving stock (Eq, Show)

instance (ToJSON a) => ToJSON (Page a) where
  toJSON = object . pageKeys
  toEncoding = pairs . mconcat . pageKeys

pageKeys :: (KeyValue e kv, ToJSON a) => Page a -> [kv]
pageKeys p = ["items" .= pageItems p, "total" .= pageTotal p, "offset" .= pageOffset p, "limit" .= pageLimit p]

instance (FromJSON a) => FromJSON (Page a) where
  parseJSON = withObject "Page" parsePage

parsePage :: (FromJSON a) => Object -> Parser (Page a)
parsePage obj = Page <$> obj .: "items" <*> obj .: "total" <*> obj .: "offset" <*> obj .: "limit"

-- | A list of rows without a total.
newtype Items a = Items
  { items :: [a]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | A page of archived jobs.
type ArchiveResponse payload = Page (Archive.ArchiveJob (Stored payload))

-- | Single-job response envelope, parameterized over the job representation
-- ('JobRead' for insert, 'ApiJobWithStatus' for the detail endpoint).
newtype JobResponse a = JobResponse
  { job :: a
  }
  deriving stock (Eq, Generic, Show)
  deriving (FromJSON, ToJSON) via Generically (JobResponse a)

-- | A page of jobs with their tree counts.
data JobsResponse payload = JobsResponse
  { jobsPage :: Page (ApiJobWithStatus (Stored payload))
  -- ^ The page. Its fields are flattened into the object.
  , childCounts :: Map Int64 Int64
  -- ^ Child count, keyed by the id of each parent on the page.
  , pausedParents :: [Int64]
  -- ^ Ids of the parents on the page whose children are all paused.
  , dlqChildCounts :: Map Int64 Int64
  -- ^ DLQ child count, keyed by the id of each parent on the page.
  }
  deriving stock (Eq, Show)

instance ToJSON (JobsResponse payload) where
  toJSON = object . jobsResponseKeys
  toEncoding = pairs . mconcat . jobsResponseKeys

jobsResponseKeys :: (KeyValue e kv) => JobsResponse payload -> [kv]
jobsResponseKeys r =
  pageKeys (jobsPage r)
    <> ["childCounts" .= childCounts r, "pausedParents" .= pausedParents r, "dlqChildCounts" .= dlqChildCounts r]

instance FromJSON (JobsResponse payload) where
  parseJSON = withObject "JobsResponse" $ \obj ->
    JobsResponse <$> parsePage obj <*> obj .: "childCounts" <*> obj .: "pausedParents" <*> obj .: "dlqChildCounts"

-- | Bounds on jobs per claim.
claimJobsRange :: (Int, Int)
claimJobsRange = (1, 1000)

-- | Jobs per claim when the request omits it.
defaultClaimJobs :: Int
defaultClaimJobs = 1

-- | Bounds on lease seconds.
leaseSecondsRange :: (Double, Double)
leaseSecondsRange = (1, 3600)

-- | Lease seconds when the request omits it.
defaultLeaseSeconds :: Double
defaultLeaseSeconds = 60

-- | A consumer's request to lease visible jobs.
data ClaimRequest = ClaimRequest
  { maxJobs :: Maybe Int
  -- ^ Jobs to claim. See 'defaultClaimJobs' and 'claimJobsRange'.
  , leaseSeconds :: Maybe Double
  -- ^ Lease length in seconds. See 'defaultLeaseSeconds' and 'leaseSecondsRange'.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Jobs returned by one claim. Each job contains the lease fields required for
-- finalization.
newtype ClaimResponse payload = ClaimResponse
  { jobs :: [JobRead payload]
  }
  deriving stock (Eq, Generic, Show)
  deriving (FromJSON, ToJSON) via Generically (ClaimResponse payload)

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
  -- ^ The lease. Its fields are flattened into the object.
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
  -- ^ The lease. Its fields are flattened into the object.
  , erSeconds :: Double
  -- ^ Clamped to 'leaseSecondsRange'.
  }
  deriving stock (Eq, Show)

instance FromJSON ExtendRequest where
  parseJSON = withObject "ExtendRequest" $ \obj ->
    ExtendRequest <$> parseJSON (Object obj) <*> obj .: "leaseSeconds"

instance ToJSON ExtendRequest where
  toJSON req = object (jobLeasePairs (erLease req) <> ["leaseSeconds" .= erSeconds req])

-- | Rows each maintenance operation touched, and the operations that raised. An
-- operation in neither was skipped.
data MaintenanceResponse = MaintenanceResponse
  { maintenanceOps :: Map Text Int64
  -- ^ Rows touched, keyed by operation name.
  , maintenanceFailed :: [Text]
  -- ^ Names of the operations that raised.
  }
  deriving stock (Eq, Show)

instance ToJSON MaintenanceResponse where
  toJSON (MaintenanceResponse ops failed) = object ["ops" .= ops, "failed" .= failed]

instance FromJSON MaintenanceResponse where
  parseJSON = withObject "MaintenanceResponse" $ \obj ->
    MaintenanceResponse <$> obj .: "ops" <*> obj .:? "failed" .!= []

-- | JSON that a request can leave out. An empty body decodes as 'Nothing'. A request
-- with no content type is read as this type and must have an empty body.
data OptionalJSON

-- | The type Servant gives a request that has no content type.
bodylessContentType :: (IsString s) => s
bodylessContentType = "application/octet-stream"

instance Accept OptionalJSON where
  contentTypes _ = contentTypes (Proxy @JSON) <> pure bodylessContentType

instance (FromJSON a) => MimeUnrender OptionalJSON (Maybe a) where
  mimeUnrender proxy = mimeUnrenderWithType proxy bodylessContentType
  mimeUnrenderWithType _ mediaType body
    | LBS8.all isSpace body = Right Nothing
    | mediaType == bodylessContentType = Left "a request body needs a JSON content type"
    | otherwise = Just <$> eitherDecode body

-- | 'Nothing' renders an empty body.
instance (ToJSON a) => MimeRender OptionalJSON (Maybe a) where
  mimeRender _ = maybe mempty encode

-- | A replacement payload for a retry or a re-enqueue.
newtype PayloadEdit payload = PayloadEdit
  { editPayload :: payload
  }
  deriving stock (Eq, Show)

instance (FromJSON payload) => FromJSON (PayloadEdit payload) where
  parseJSON = withObject "PayloadEdit" $ \obj -> PayloadEdit <$> obj .: "payload"

instance (ToJSON payload) => ToJSON (PayloadEdit payload) where
  toJSON edit = object ["payload" .= editPayload edit]

-- | When a rescheduled job becomes visible.
data RescheduleRequest = RescheduleRequest
  { runAt :: UTCTime
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Tokens to add to one rate-limit bucket.
newtype AddTokensRequest = AddTokensRequest
  { addedTokens :: Double
  }
  deriving stock (Eq, Show)

instance FromJSON AddTokensRequest where
  parseJSON = withObject "AddTokensRequest" $ \obj -> AddTokensRequest <$> obj .: "tokens"

instance ToJSON AddTokensRequest where
  toJSON request = object ["tokens" .= addedTokens request]

-- | Jobs a token grant made claimable again.
data AddTokensResponse = AddTokensResponse
  { woken :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Rows a prune deleted.
data PruneResponse = PruneResponse
  { pruned :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | A page of a queue's open groups.
type GroupsResponse = Page GroupSummary

-- | A page of DLQ jobs.
type DLQResponse payload = Page (DLQ.DLQJob (Stored payload))

-- | Queue statistics response.
data StatsResponse = StatsResponse
  { stats :: QueueStats
  , timestamp :: UTCTime
  -- ^ When the stats were read.
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
  -- ^ Registry queue names.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Request body for batch job insert.
newtype BatchInsertRequest payload = BatchInsertRequest
  { jobs :: [ApiJobWrite payload]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Response body for batch job insert.
data BatchInsertResponse payload = BatchInsertResponse
  { inserted :: [JobRead payload]
  , insertedCount :: Int
  }
  deriving stock (Eq, Generic, Show)
  deriving (FromJSON, ToJSON) via Generically (BatchInsertResponse payload)

-- | Request body for a batch DLQ or archive delete.
data BatchDeleteRequest = BatchDeleteRequest
  { ids :: [Int64]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Response body for a batch DLQ or archive delete.
data BatchDeleteResponse = BatchDeleteResponse
  { deleted :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | A cron schedule row plus the next tick its effective expression fires at.
-- A disabled schedule, an expression that never fires again, or one the server
-- cannot parse has none.
data CronScheduleView = CronScheduleView
  { schedule :: CronScheduleRow
  -- ^ The row. Its fields are flattened into the object.
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
  { schedules :: [CronScheduleView]
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
type RateLimitBucketsResponse = Items RateLimitBucketView

-- | Number of buckets a reset refilled to full.
data RateLimitResetResponse = RateLimitResetResponse
  { reset :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Concurrency policies response.
data ConcurrencyPoliciesResponse = ConcurrencyPoliciesResponse
  { policies :: [ConcurrencyPolicyView]
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Concurrency keys response (one prefix's keys).
type ConcurrencyKeysResponse = Items ConcurrencyKeyView

-- | Number of count rows repaired from live jobs.
data ConcurrencyReconcileResponse = ConcurrencyReconcileResponse
  { reconciled :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Whether the API can reach its database.
data HealthStatus
  = -- | The database answered.
    Ok
  | -- | The database did not answer.
    Down
  deriving stock (Bounded, Enum, Eq, Generic, Show)

instance ToJSON HealthStatus where
  toJSON = toJSON . healthStatusToText

instance FromJSON HealthStatus where
  parseJSON = withText "HealthStatus" $ either (fail . unpack) pure . enumFromText "health status" healthStatusToText

-- | The JSON text of a status: @ok@ or @down@.
healthStatusToText :: HealthStatus -> Text
healthStatusToText = \case
  Ok -> "ok"
  Down -> "down"

-- | Readiness of the API and the database behind it.
data HealthResponse = HealthResponse
  { status :: HealthStatus
  , schemaName :: Text
  -- ^ The schema the API serves.
  , checkedAt :: UTCTime
  -- ^ When the probe finished.
  , dbLatencyMs :: Maybe Double
  -- ^ Null when the database could not be reached.
  , db :: Maybe PgDbHealth
  -- ^ Connection and age counters. Null when the database is unreachable or reports no row.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | The process is running. Answered without touching the database.
data LivenessResponse = LivenessResponse
  { alive :: Bool
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

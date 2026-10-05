{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- | The admin API's route types, generated from the registry.
module Arbiter.Servant.API
  ( -- * Route tree
    ArbiterAPI
  , RegistryToAPI
  , SharedAPI

    -- * Per-queue routes
  , TableAPI (..)
  , JobsAPI (..)
  , DLQAPI (..)
  , ArchiveAPI (..)
  , StatsAPI (..)

    -- * Shared routes
  , QueuesAPI (..)
  , MaintenanceAPI (..)
  , EventsAPI
  , CronAPI (..)
  , WorkersAPI (..)
  , HealthAPI (..)
  , RateLimitsAPI (..)
  , ConcurrencyAPI (..)
  ) where

import Arbiter.Core.Enum (enumFromTextCI)
import Arbiter.Core.Job.Types (JobRead, JobStatus, Stored, jobStatusToText)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry, SpecName, SpecPayload, SpecResult)
import Arbiter.Core.Sql.Jobs
  ( ArchiveSortColumn
  , DLQSortColumn
  , JobSortColumn
  , SortDir
  , archiveSortColumnName
  , dlqSortColumnName
  , jobSortColumnName
  , sortDirName
  )
import Data.Int (Int64)
import Data.Kind (Type)
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)
import GHC.Generics (Generic)
import Servant.API

import Arbiter.Servant.Types

instance FromHttpApiData JobSortColumn where
  parseQueryParam = enumFromTextCI "job sort column" jobSortColumnName

instance ToHttpApiData JobSortColumn where
  toUrlPiece = jobSortColumnName

instance FromHttpApiData DLQSortColumn where
  parseQueryParam = enumFromTextCI "DLQ sort column" dlqSortColumnName

instance ToHttpApiData DLQSortColumn where
  toUrlPiece = dlqSortColumnName

instance FromHttpApiData ArchiveSortColumn where
  parseQueryParam = enumFromTextCI "archive sort column" archiveSortColumnName

instance ToHttpApiData ArchiveSortColumn where
  toUrlPiece = archiveSortColumnName

instance FromHttpApiData SortDir where
  parseQueryParam = enumFromTextCI "sort direction" sortDirName

instance ToHttpApiData SortDir where
  toUrlPiece = sortDirName

instance FromHttpApiData JobStatus where
  parseQueryParam = enumFromTextCI "job status" jobStatusToText

instance ToHttpApiData JobStatus where
  toUrlPiece = jobStatusToText

-- | How the optional retry and re-enqueue body reads.
type PayloadEditNote =
  "Optional. A payload replaces the stored payload, and the kind, rate-limit and concurrency columns come from it again. An empty body keeps the stored payload."

-- | One queue's job routes.
data JobsAPI payload result mode = JobsAPI
  { -- | @GET \/:queue\/jobs?limit=N&offset=N&group_key=X&parent_id=N&job_id=N&roots_only&status=S&claimed_by=UUID&kind=X&payload=text&rate_limit_prefix=X&concurrency_prefix=X&sort_by=...&sort_dir=...@
    listJobs
      :: mode
        :- QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> QueryParam "group_key" Text
          :> QueryParam "parent_id" Int64
          :> QueryParam "job_id" Int64
          :> QueryFlag "roots_only"
          :> QueryParam "status" JobStatus
          :> QueryParam "claimed_by" UUID
          :> QueryParam "kind" Text
          :> QueryParam "payload" Text
          :> QueryParam "rate_limit_prefix" Text
          :> QueryParam "concurrency_prefix" Text
          :> QueryParam "sort_by" JobSortColumn
          :> QueryParam "sort_dir" SortDir
          :> Get '[JSON] (JobsResponse payload)
  , -- | @POST \/:queue\/jobs@ Insert a new job.
    insertJob
      :: mode
        :- ReqBody '[JSON] (ApiJobWrite payload)
          :> Post '[JSON] (JobResponse (JobRead payload))
  , -- | @POST \/:queue\/jobs\/batch@ Insert many jobs.
    insertJobsBatch
      :: mode
        :- "batch"
          :> ReqBody '[JSON] (BatchInsertRequest payload)
          :> Post '[JSON] (BatchInsertResponse payload)
  , -- | @GET \/:queue\/jobs\/:id@
    getJob
      :: mode
        :- Capture "id" Int64
          :> Get '[JSON] (JobResponse (ApiJobWithStatus (Stored payload)))
  , -- | @DELETE \/:queue\/jobs\/:id@ Cancel the job and its children.
    cancelJob
      :: mode
        :- Capture "id" Int64
          :> DeleteNoContent
  , -- | @POST \/:queue\/jobs\/:id\/force-cancel@ Cancel the job and its children, and interrupt running handlers.
    forceCancelJob
      :: mode
        :- Capture "id" Int64
          :> "force-cancel"
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/ack@ Complete a job this caller holds.
    ackClaimedJob
      :: mode
        :- Capture "id" Int64
          :> "ack"
          :> ReqBody '[JSON] (AckRequest result)
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/nack@ Hand back a job this caller holds.
    nackClaimedJob
      :: mode
        :- Capture "id" Int64
          :> "nack"
          :> ReqBody '[JSON] JobLease
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/extend@ Push out the lease this caller holds.
    extendClaimedJob
      :: mode
        :- Capture "id" Int64
          :> "extend"
          :> ReqBody '[JSON] ExtendRequest
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/promote@
    promoteJob
      :: mode
        :- Capture "id" Int64
          :> "promote"
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/reschedule@
    rescheduleJob
      :: mode
        :- Capture "id" Int64
          :> "reschedule"
          :> ReqBody '[JSON] RescheduleRequest
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/move-to-dlq@
    moveToDLQ
      :: mode
        :- Capture "id" Int64
          :> "move-to-dlq"
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/pause-children@
    pauseChildren
      :: mode
        :- Capture "id" Int64
          :> "pause-children"
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/resume-children@
    resumeChildren
      :: mode
        :- Capture "id" Int64
          :> "resume-children"
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/suspend@
    suspendJob
      :: mode
        :- Capture "id" Int64
          :> "suspend"
          :> PostNoContent
  , -- | @POST \/:queue\/jobs\/:id\/resume@
    resumeJob
      :: mode
        :- Capture "id" Int64
          :> "resume"
          :> PostNoContent
  }
  deriving stock (Generic)

-- | One queue's DLQ routes.
data DLQAPI payload mode = DLQAPI
  { -- | @GET \/:queue\/dlq?limit=N&offset=N&parent_id=N&job_id=N&group_key=X&kind=X&payload=text&error=text&sort_by=...&sort_dir=...@
    listDLQ
      :: mode
        :- QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> QueryParam "parent_id" Int64
          :> QueryParam "job_id" Int64
          :> QueryParam "group_key" Text
          :> QueryParam "kind" Text
          :> QueryParam "payload" Text
          :> QueryParam "error" Text
          :> QueryParam "sort_by" DLQSortColumn
          :> QueryParam "sort_dir" SortDir
          :> Get '[JSON] (DLQResponse payload)
  , -- | @POST \/:queue\/dlq\/:id\/retry@ Move the job back to the main queue, optionally with a new payload.
    retryFromDLQ
      :: mode
        :- Capture "id" Int64
          :> "retry"
          :> ReqBody' '[Description PayloadEditNote] '[OptionalJSON] (Maybe (PayloadEdit payload))
          :> PostNoContent
  , -- | @DELETE \/:queue\/dlq\/:id@ Delete the job permanently.
    deleteDLQ
      :: mode
        :- Capture "id" Int64
          :> DeleteNoContent
  , -- | @POST \/:queue\/dlq\/batch-delete@ Delete many jobs permanently.
    deleteDLQBatch
      :: mode
        :- "batch-delete"
          :> ReqBody '[JSON] BatchDeleteRequest
          :> Post '[JSON] BatchDeleteResponse
  }
  deriving stock (Generic)

-- | One queue's archive routes.
data ArchiveAPI payload mode = ArchiveAPI
  { -- | @GET \/:queue\/archive?limit=N&offset=N&parent_id=N&job_id=N&group_key=X&kind=X&payload=text&completed_after=T&completed_before=T&sort_by=...&sort_dir=...@
    listArchive
      :: mode
        :- QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> QueryParam "parent_id" Int64
          :> QueryParam "job_id" Int64
          :> QueryParam "group_key" Text
          :> QueryParam "kind" Text
          :> QueryParam "payload" Text
          :> QueryParam "completed_after" UTCTime
          :> QueryParam "completed_before" UTCTime
          :> QueryParam "sort_by" ArchiveSortColumn
          :> QueryParam "sort_dir" SortDir
          :> Get '[JSON] (ArchiveResponse payload)
  , -- | @POST \/:queue\/archive\/:id\/reenqueue@ Run the job again as a fresh job, optionally with a new payload.
    reEnqueueArchive
      :: mode
        :- Capture "id" Int64
          :> "reenqueue"
          :> ReqBody' '[Description PayloadEditNote] '[OptionalJSON] (Maybe (PayloadEdit payload))
          :> PostNoContent
  , -- | @DELETE \/:queue\/archive\/:id@ Purge one entry.
    deleteArchive
      :: mode
        :- Capture "id" Int64
          :> DeleteNoContent
  , -- | @POST \/:queue\/archive\/batch-delete@ Purge many entries.
    deleteArchiveBatch
      :: mode
        :- "batch-delete"
          :> ReqBody '[JSON] BatchDeleteRequest
          :> Post '[JSON] BatchDeleteResponse
  }
  deriving stock (Generic)

-- | One queue's stats route.
data StatsAPI mode = StatsAPI
  { -- | @GET \/:queue\/stats@
    getStats
      :: mode
        :- Get '[JSON] StatsResponse
  }
  deriving stock (Generic)

-- | Schema-wide maintenance. A running worker pool does the same work.
newtype MaintenanceAPI mode = MaintenanceAPI
  { -- | @POST \/maintenance@ Run one gated maintenance pass.
    runMaintenance
      :: mode
        :- Post '[JSON] MaintenanceResponse
  }
  deriving stock (Generic)

-- | One queue's routes.
data TableAPI payload result mode = TableAPI
  { -- | @\/:queue\/jobs@ The job routes.
    jobs :: mode :- "jobs" :> NamedRoutes (JobsAPI payload result)
  , -- | @POST \/:queue\/claim@ Lease visible jobs.
    claimJobs
      :: mode
        :- "claim"
          :> ReqBody '[JSON] ClaimRequest
          :> Post '[JSON] (ClaimResponse payload)
  , -- | @\/:queue\/dlq@ The DLQ routes.
    dlq :: mode :- "dlq" :> NamedRoutes (DLQAPI payload)
  , -- | @\/:queue\/archive@ The archive routes.
    archive :: mode :- "archive" :> NamedRoutes (ArchiveAPI payload)
  , -- | @\/:queue\/stats@ The stats route.
    stats :: mode :- "stats" :> NamedRoutes StatsAPI
  , -- | @GET \/:queue\/kinds@ The payload kind labels.
    listKinds :: mode :- "kinds" :> Get '[JSON] [Text]
  , -- | @GET \/:queue\/groups?limit=N&offset=N&group_key=X@
    listGroups
      :: mode
        :- "groups"
          :> QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> QueryParam "group_key" Text
          :> Get '[JSON] GroupsResponse
  }
  deriving stock (Generic)

-- | The queue registry routes.
data QueuesAPI mode = QueuesAPI
  { -- | @GET \/queues@
    listQueues
      :: mode
        :- Get '[JSON] QueuesResponse
  , -- | @GET \/queues\/stats@
    getAllStats
      :: mode
        :- "stats"
          :> Get '[JSON] AllStatsResponse
  , -- | @GET \/queues\/:queue\/details@
    getDetails
      :: mode
        :- Capture "queue" Text
          :> "details"
          :> Get '[JSON] (Maybe QueueRow)
  , -- | @POST \/queues\/:queue\/pause@
    pauseQueue
      :: mode
        :- Capture "queue" Text
          :> "pause"
          :> PostNoContent
  , -- | @POST \/queues\/:queue\/resume@
    resumeQueue
      :: mode
        :- Capture "queue" Text
          :> "resume"
          :> PostNoContent
  }
  deriving stock (Generic)

-- | The SSE stream, served by a raw WAI handler.
type EventsAPI = "stream" :> Raw

-- | The cron schedule routes.
data CronAPI mode = CronAPI
  { -- | @GET \/cron\/schedules@
    --
    -- Optional @?queue=name@ scopes the result to a single queue.
    listSchedules
      :: mode
        :- "schedules"
          :> QueryParam "queue" Text
          :> Get '[JSON] CronSchedulesResponse
  , -- | @PATCH \/cron\/schedules\/:name@
    updateSchedule
      :: mode
        :- "schedules"
          :> Capture "name" Text
          :> ReqBody '[JSON] CronScheduleUpdate
          :> Patch '[JSON] CronScheduleView
  , -- | @POST \/cron\/schedules\/:name\/run@
    runSchedule
      :: mode
        :- "schedules"
          :> Capture "name" Text
          :> "run"
          :> PostNoContent
  }
  deriving stock (Generic)

-- | The worker registry routes.
data WorkersAPI mode = WorkersAPI
  { -- | @GET \/workers@
    --
    -- Optional @?queue=name@ scopes the result to a single queue.
    -- Optional @?live=seconds@ filters to workers with a heartbeat inside the
    -- threshold. Without it all rows are returned.
    listWorkers
      :: mode
        :- QueryParam "queue" Text
          :> QueryParam "live" Double
          :> Get '[JSON] WorkersResponse
  , -- | @POST \/workers\/:id\/pause@
    --
    -- Sets the worker's @paused@ flag.
    pauseWorker
      :: mode
        :- Capture "id" UUID
          :> "pause"
          :> PostNoContent
  , -- | @POST \/workers\/:id\/resume@
    resumeWorker
      :: mode
        :- Capture "id" UUID
          :> "resume"
          :> PostNoContent
  }
  deriving stock (Generic)

-- | Rate limits API routes, schema-wide.
data RateLimitsAPI mode = RateLimitsAPI
  { -- | @GET \/rate-limits@
    listRateLimits
      :: mode
        :- Get '[JSON] RateLimitPoliciesResponse
  , -- | @GET \/rate-limits\/:prefix\/buckets?limit=N&offset=N@
    listRateLimitBuckets
      :: mode
        :- Capture "prefix" Text
          :> "buckets"
          :> QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> Get '[JSON] RateLimitBucketsResponse
  , -- | @PATCH \/rate-limits\/:prefix@
    updateRateLimitPolicy
      :: mode
        :- Capture "prefix" Text
          :> ReqBody '[JSON] RateLimitPolicyUpdate
          :> Patch '[JSON] RateLimitPolicyView
  , -- | @POST \/rate-limits\/:prefix\/reset@
    resetRateLimitBuckets
      :: mode
        :- Capture "prefix" Text
          :> "reset"
          :> Post '[JSON] RateLimitResetResponse
  , -- | @POST \/rate-limits\/:prefix\/buckets\/:key\/tokens@
    addRateLimitTokens
      :: mode
        :- Capture "prefix" Text
          :> "buckets"
          :> Capture "key" Text
          :> "tokens"
          :> ReqBody '[JSON] AddTokensRequest
          :> Post '[JSON] AddTokensResponse
  , -- | @POST \/rate-limits\/prune?idle=seconds@
    pruneRateLimitBuckets
      :: mode
        :- "prune"
          :> QueryParam "idle" Double
          :> Post '[JSON] PruneResponse
  }
  deriving stock (Generic)

-- | Concurrency API routes, schema-wide.
data ConcurrencyAPI mode = ConcurrencyAPI
  { -- | @GET \/concurrency@
    listConcurrency
      :: mode
        :- Get '[JSON] ConcurrencyPoliciesResponse
  , -- | @GET \/concurrency\/:prefix\/keys?limit=N&offset=N@
    listConcurrencyKeys
      :: mode
        :- Capture "prefix" Text
          :> "keys"
          :> QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> Get '[JSON] ConcurrencyKeysResponse
  , -- | @PATCH \/concurrency\/:prefix@
    updateConcurrencyPolicy
      :: mode
        :- Capture "prefix" Text
          :> ReqBody '[JSON] ConcurrencyPolicyUpdate
          :> Patch '[JSON] ConcurrencyPolicyView
  , -- | @POST \/concurrency\/reconcile@
    reconcileConcurrency
      :: mode
        :- "reconcile"
          :> Post '[JSON] ConcurrencyReconcileResponse
  , -- | @POST \/concurrency\/prune@
    pruneConcurrencyKeys
      :: mode
        :- "prune"
          :> Post '[JSON] PruneResponse
  }
  deriving stock (Generic)

-- | Liveness and readiness.
data HealthAPI mode = HealthAPI
  { -- | @GET \/health@
    --
    -- Readiness. Reaches the database and reports its connection counters.
    -- Returns 200 when the database is reachable and 503 when it is down.
    -- Both carry the same body.
    getHealth
      :: mode
        :- Get '[JSON] HealthResponse
  , -- | @GET \/health\/live@
    --
    -- Liveness. Does not touch the database.
    getLiveness
      :: mode
        :- "live"
          :> Get '[JSON] LivenessResponse
  }
  deriving stock (Generic)

-- | Shared top-level routes appended after the per-table routes.
type SharedAPI =
  "queues" :> NamedRoutes QueuesAPI
    :<|> "maintenance" :> NamedRoutes MaintenanceAPI
    :<|> "events" :> EventsAPI
    :<|> "cron" :> NamedRoutes CronAPI
    :<|> "workers" :> NamedRoutes WorkersAPI
    :<|> "rate-limits" :> NamedRoutes RateLimitsAPI
    :<|> "concurrency" :> NamedRoutes ConcurrencyAPI
    :<|> "health" :> NamedRoutes HealthAPI

-- | A 'TableAPI' route per registry entry, followed by the shared
-- top-level routes.
type family RegistryToAPI (registry :: JobPayloadRegistry) :: Type where
  RegistryToAPI '[] = SharedAPI
  RegistryToAPI (spec ': rest) =
    (SpecName spec :> NamedRoutes (TableAPI (SpecPayload spec) (SpecResult spec)))
      :<|> RegistryToAPI rest

-- | Top-level Arbiter API, mounted at @\/api\/v1@. The route tree under that
-- prefix is generated from the registry. See 'RegistryToAPI' for the shape.
type ArbiterAPI :: JobPayloadRegistry -> Type
type ArbiterAPI registry = "api" :> "v1" :> RegistryToAPI registry

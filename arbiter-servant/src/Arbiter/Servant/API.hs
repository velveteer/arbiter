{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- | The API's route types for queue operations and remote job consumers, generated from the registry.
module Arbiter.Servant.API
  ( -- * Route tree
    ArbiterAPI
  , RegistryToAPI
  , SharedAPI
  , PayloadEditNote

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

    -- * Sort parameters
  , JobSortColumn (..)
  , DLQSortColumn (..)
  , ArchiveSortColumn (..)
  , SortDir (..)
  ) where

import Arbiter.Core.Enum (enumFromTextCI)
import Arbiter.Core.Job.Types (JobRead, JobStatus, Stored, jobStatusToText)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry, SpecName, SpecPayload, SpecResult)
import Arbiter.Core.Sql.Jobs
  ( ArchiveSortColumn (..)
  , DLQSortColumn (..)
  , JobSortColumn (..)
  , SortDir (..)
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
  { listJobs
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
  -- ^ @GET \/:queue\/jobs?limit=N&offset=N&group_key=X&parent_id=N&job_id=N&roots_only&status=S&claimed_by=UUID&kind=X&payload=text&rate_limit_prefix=X&concurrency_prefix=X&sort_by=...&sort_dir=...@
  , insertJob
      :: mode
        :- ReqBody '[JSON] (ApiJobWrite payload)
          :> Post '[JSON] (JobResponse (JobRead payload))
  -- ^ @POST \/:queue\/jobs@ Insert a new job.
  --
  -- A dedup-ignore collision returns the existing job (200). A blocked replace is 409.
  , insertJobsBatch
      :: mode
        :- "batch"
          :> ReqBody '[JSON] (BatchInsertRequest payload)
          :> Post '[JSON] (BatchInsertResponse payload)
  -- ^ @POST \/:queue\/jobs\/batch@ Insert many jobs.
  , getJob
      :: mode
        :- Capture "id" Int64
          :> Get '[JSON] (JobResponse (ApiJobWithStatus (Stored payload)))
  -- ^ @GET \/:queue\/jobs\/:id@
  , cancelJob
      :: mode
        :- Capture "id" Int64
          :> DeleteNoContent
  -- ^ @DELETE \/:queue\/jobs\/:id@ Cancel the job and its children.
  , forceCancelJob
      :: mode
        :- Capture "id" Int64
          :> "force-cancel"
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/force-cancel@ Cancel the job and its children, and interrupt running handlers.
  , ackClaimedJob
      :: mode
        :- Capture "id" Int64
          :> "ack"
          :> ReqBody '[JSON] (AckRequest result)
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/ack@ Complete a job this caller holds.
  --
  -- 404 for a missing job. 409 when this lease does not hold the job, a registered
  -- worker holds it, or the job is suspended.
  , nackClaimedJob
      :: mode
        :- Capture "id" Int64
          :> "nack"
          :> ReqBody '[JSON] JobLease
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/nack@ Hand back a job this caller holds.
  --
  -- 404 for a missing job. 409 when this lease does not hold the job, a registered
  -- worker holds it, or the job is suspended.
  , extendClaimedJob
      :: mode
        :- Capture "id" Int64
          :> "extend"
          :> ReqBody '[JSON] ExtendRequest
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/extend@ Push out the lease this caller holds.
  --
  -- 404 for a missing job. 409 when this lease does not hold the job, a registered
  -- worker holds it, or the job is suspended.
  , promoteJob
      :: mode
        :- Capture "id" Int64
          :> "promote"
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/promote@
  , rescheduleJob
      :: mode
        :- Capture "id" Int64
          :> "reschedule"
          :> ReqBody '[JSON] RescheduleRequest
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/reschedule@
  , moveToDLQ
      :: mode
        :- Capture "id" Int64
          :> "move-to-dlq"
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/move-to-dlq@
  , pauseChildren
      :: mode
        :- Capture "id" Int64
          :> "pause-children"
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/pause-children@
  , resumeChildren
      :: mode
        :- Capture "id" Int64
          :> "resume-children"
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/resume-children@
  , suspendJob
      :: mode
        :- Capture "id" Int64
          :> "suspend"
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/suspend@
  , resumeJob
      :: mode
        :- Capture "id" Int64
          :> "resume"
          :> PostNoContent
  -- ^ @POST \/:queue\/jobs\/:id\/resume@
  }
  deriving stock (Generic)

-- | One queue's DLQ routes.
data DLQAPI payload mode = DLQAPI
  { listDLQ
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
  -- ^ @GET \/:queue\/dlq?limit=N&offset=N&parent_id=N&job_id=N&group_key=X&kind=X&payload=text&error=text&sort_by=...&sort_dir=...@
  , retryFromDLQ
      :: mode
        :- Capture "id" Int64
          :> "retry"
          :> ReqBody' '[Description PayloadEditNote] '[OptionalJSON] (Maybe (PayloadEdit payload))
          :> PostNoContent
  -- ^ @POST \/:queue\/dlq\/:id\/retry@ Move the job back to the main queue, optionally with a new payload.
  , deleteDLQ
      :: mode
        :- Capture "id" Int64
          :> DeleteNoContent
  -- ^ @DELETE \/:queue\/dlq\/:id@ Delete the job permanently.
  , deleteDLQBatch
      :: mode
        :- "batch-delete"
          :> ReqBody '[JSON] BatchDeleteRequest
          :> Post '[JSON] BatchDeleteResponse
  -- ^ @POST \/:queue\/dlq\/batch-delete@ Delete many jobs permanently.
  }
  deriving stock (Generic)

-- | One queue's archive routes.
data ArchiveAPI payload mode = ArchiveAPI
  { listArchive
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
  -- ^ @GET \/:queue\/archive?limit=N&offset=N&parent_id=N&job_id=N&group_key=X&kind=X&payload=text&completed_after=T&completed_before=T&sort_by=...&sort_dir=...@
  , reEnqueueArchive
      :: mode
        :- Capture "id" Int64
          :> "reenqueue"
          :> ReqBody' '[Description PayloadEditNote] '[OptionalJSON] (Maybe (PayloadEdit payload))
          :> PostNoContent
  -- ^ @POST \/:queue\/archive\/:id\/reenqueue@ Run the job again as a fresh job, optionally with a new payload.
  , deleteArchive
      :: mode
        :- Capture "id" Int64
          :> DeleteNoContent
  -- ^ @DELETE \/:queue\/archive\/:id@ Purge one entry.
  , deleteArchiveBatch
      :: mode
        :- "batch-delete"
          :> ReqBody '[JSON] BatchDeleteRequest
          :> Post '[JSON] BatchDeleteResponse
  -- ^ @POST \/:queue\/archive\/batch-delete@ Purge many entries.
  }
  deriving stock (Generic)

-- | One queue's stats route.
data StatsAPI mode = StatsAPI
  { getStats
      :: mode
        :- Get '[JSON] StatsResponse
  -- ^ @GET \/:queue\/stats@
  }
  deriving stock (Generic)

-- | Schema-wide maintenance. A running worker pool does the same work.
newtype MaintenanceAPI mode = MaintenanceAPI
  { runMaintenance
      :: mode
        :- Post '[JSON] MaintenanceResponse
  -- ^ @POST \/maintenance@ Run one gated maintenance pass.
  }
  deriving stock (Generic)

-- | One queue's routes.
data TableAPI payload result mode = TableAPI
  { jobs :: mode :- "jobs" :> NamedRoutes (JobsAPI payload result)
  -- ^ @\/:queue\/jobs@ The job routes.
  , claimJobs
      :: mode
        :- "claim"
          :> ReqBody '[JSON] ClaimRequest
          :> Post '[JSON] (ClaimResponse payload)
  -- ^ @POST \/:queue\/claim@ Lease visible jobs. A paused queue returns no jobs.
  , dlq :: mode :- "dlq" :> NamedRoutes (DLQAPI payload)
  -- ^ @\/:queue\/dlq@ The DLQ routes.
  , archive :: mode :- "archive" :> NamedRoutes (ArchiveAPI payload)
  -- ^ @\/:queue\/archive@ The archive routes.
  , stats :: mode :- "stats" :> NamedRoutes StatsAPI
  -- ^ @\/:queue\/stats@ The stats route.
  , listKinds :: mode :- "kinds" :> Get '[JSON] [Text]
  -- ^ @GET \/:queue\/kinds@ The payload kind labels.
  , listGroups
      :: mode
        :- "groups"
          :> QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> QueryParam "group_key" Text
          :> Get '[JSON] GroupsResponse
  -- ^ @GET \/:queue\/groups?limit=N&offset=N&group_key=X@
  }
  deriving stock (Generic)

-- | The queue registry routes.
data QueuesAPI mode = QueuesAPI
  { listQueues
      :: mode
        :- Get '[JSON] QueuesResponse
  -- ^ @GET \/queues@
  , getAllStats
      :: mode
        :- "stats"
          :> Get '[JSON] AllStatsResponse
  -- ^ @GET \/queues\/stats@
  , getDetails
      :: mode
        :- Capture "queue" Text
          :> "details"
          :> Get '[JSON] (Maybe QueueRow)
  -- ^ @GET \/queues\/:queue\/details@
  --
  -- Null when the queue has no pause-state row. A pause or a resume creates it.
  , pauseQueue
      :: mode
        :- Capture "queue" Text
          :> "pause"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/pause@ 404 for a queue not in the registry.
  , resumeQueue
      :: mode
        :- Capture "queue" Text
          :> "resume"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/resume@ 404 for a queue not in the registry.
  }
  deriving stock (Generic)

-- | The SSE stream, served by a raw WAI handler.
type EventsAPI = "stream" :> Raw

-- | The cron schedule routes.
data CronAPI mode = CronAPI
  { listSchedules
      :: mode
        :- "schedules"
          :> QueryParam "queue" Text
          :> Get '[JSON] CronSchedulesResponse
  -- ^ @GET \/cron\/schedules@
  --
  -- Optional @?queue=name@ scopes the result to a single queue.
  , updateSchedule
      :: mode
        :- "schedules"
          :> Capture "name" Text
          :> ReqBody '[JSON] CronScheduleUpdate
          :> Patch '[JSON] CronScheduleView
  -- ^ @PATCH \/cron\/schedules\/:name@
  , runSchedule
      :: mode
        :- "schedules"
          :> Capture "name" Text
          :> "run"
          :> PostNoContent
  -- ^ @POST \/cron\/schedules\/:name\/run@
  }
  deriving stock (Generic)

-- | The worker registry routes.
data WorkersAPI mode = WorkersAPI
  { listWorkers
      :: mode
        :- QueryParam "queue" Text
          :> QueryParam "live" Double
          :> Get '[JSON] WorkersResponse
  -- ^ @GET \/workers@
  --
  -- Optional @?queue=name@ scopes the result to a single queue.
  -- Optional @?live=seconds@ filters to workers with a heartbeat inside the
  -- threshold. Without it all rows are returned.
  , pauseWorker
      :: mode
        :- Capture "id" UUID
          :> "pause"
          :> PostNoContent
  -- ^ @POST \/workers\/:id\/pause@
  --
  -- Sets the worker's @paused@ flag.
  , resumeWorker
      :: mode
        :- Capture "id" UUID
          :> "resume"
          :> PostNoContent
  -- ^ @POST \/workers\/:id\/resume@
  }
  deriving stock (Generic)

-- | Rate limits API routes, schema-wide.
data RateLimitsAPI mode = RateLimitsAPI
  { listRateLimits
      :: mode
        :- Get '[JSON] RateLimitPoliciesResponse
  -- ^ @GET \/rate-limits@
  , listRateLimitBuckets
      :: mode
        :- Capture "prefix" Text
          :> "buckets"
          :> QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> Get '[JSON] RateLimitBucketsResponse
  -- ^ @GET \/rate-limits\/:prefix\/buckets?limit=N&offset=N@
  --
  -- Default limit 100, range 1 to 1000.
  , updateRateLimitPolicy
      :: mode
        :- Capture "prefix" Text
          :> ReqBody '[JSON] RateLimitPolicyUpdate
          :> Patch '[JSON] RateLimitPolicyView
  -- ^ @PATCH \/rate-limits\/:prefix@
  , resetRateLimitBuckets
      :: mode
        :- Capture "prefix" Text
          :> "reset"
          :> Post '[JSON] RateLimitResetResponse
  -- ^ @POST \/rate-limits\/:prefix\/reset@
  , addRateLimitTokens
      :: mode
        :- Capture "prefix" Text
          :> "buckets"
          :> Capture "key" Text
          :> "tokens"
          :> ReqBody '[JSON] AddTokensRequest
          :> Post '[JSON] AddTokensResponse
  -- ^ @POST \/rate-limits\/:prefix\/buckets\/:key\/tokens@
  --
  -- @:key@ is the full bucket key with its prefix, as the bucket listing shows it.
  -- A key outside the prefix or a token count of 0 or less is 400. An unknown prefix is 404.
  , pruneRateLimitBuckets
      :: mode
        :- "prune"
          :> QueryParam "idle" Double
          :> Post '[JSON] PruneResponse
  -- ^ @POST \/rate-limits\/prune?idle=seconds@
  --
  -- The default idle is the server's maintenance bucket idle age. A negative value is 400.
  }
  deriving stock (Generic)

-- | Concurrency API routes, schema-wide.
data ConcurrencyAPI mode = ConcurrencyAPI
  { listConcurrency
      :: mode
        :- Get '[JSON] ConcurrencyPoliciesResponse
  -- ^ @GET \/concurrency@
  , listConcurrencyKeys
      :: mode
        :- Capture "prefix" Text
          :> "keys"
          :> QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> Get '[JSON] ConcurrencyKeysResponse
  -- ^ @GET \/concurrency\/:prefix\/keys?limit=N&offset=N@
  --
  -- Default limit 100, range 1 to 1000.
  , updateConcurrencyPolicy
      :: mode
        :- Capture "prefix" Text
          :> ReqBody '[JSON] ConcurrencyPolicyUpdate
          :> Patch '[JSON] ConcurrencyPolicyView
  -- ^ @PATCH \/concurrency\/:prefix@
  , reconcileConcurrency
      :: mode
        :- "reconcile"
          :> Post '[JSON] ConcurrencyReconcileResponse
  -- ^ @POST \/concurrency\/reconcile@
  , pruneConcurrencyKeys
      :: mode
        :- "prune"
          :> Post '[JSON] PruneResponse
  -- ^ @POST \/concurrency\/prune@
  }
  deriving stock (Generic)

-- | Liveness and readiness.
data HealthAPI mode = HealthAPI
  { getHealth
      :: mode
        :- Get '[JSON] HealthResponse
  -- ^ @GET \/health@
  --
  -- Readiness. Reaches the database and reports its connection counters.
  -- Returns 200 when the database is reachable and 503 when it is down.
  -- Both carry the same body.
  , getLiveness
      :: mode
        :- "live"
          :> Get '[JSON] LivenessResponse
  -- ^ @GET \/health\/live@
  --
  -- Liveness. Does not touch the database.
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

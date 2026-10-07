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

    -- * Route descriptions
  , PayloadEditNote
  , LeaseRefusal
  , Throws'
  , Throws
  , ThrowsBody

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
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)
import GHC.Generics (Generic)
import GHC.TypeLits (Nat, Symbol)
import Servant.API
import Servant.Client.Core (HasClient (..))
import Servant.Server (HasServer (..))

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

-- | Why a lease route refuses the lease it was sent.
type LeaseRefusal =
  "This lease does not hold the job, a registered worker holds it, the job is suspended, or the job was force-cancelled."

-- | A documented error response with an optional JSON body. The server, client and links ignore it.
data Throws' (status :: Nat) (reason :: Symbol) (body :: Maybe Type)

-- | An error response with no declared content type.
type Throws status reason = Throws' status reason 'Nothing

-- | An error response with a JSON body of type @body@.
type ThrowsBody status reason body = Throws' status reason ('Just body)

instance (HasServer api context) => HasServer (Throws' status reason body :> api) context where
  type ServerT (Throws' status reason body :> api) m = ServerT api m
  route _ = route (Proxy @api)
  hoistServerWithContext _ = hoistServerWithContext (Proxy @api)

instance (HasClient m api) => HasClient m (Throws' status reason body :> api) where
  type Client m (Throws' status reason body :> api) = Client m api
  clientWithRoute pm _ = clientWithRoute pm (Proxy @api)
  hoistClientMonad pm _ = hoistClientMonad pm (Proxy @api)

instance (HasLink api) => HasLink (Throws' status reason body :> api) where
  type MkLink (Throws' status reason body :> api) link = MkLink api link
  toLink toA _ = toLink toA (Proxy @api)

-- | One queue's job routes.
data JobsAPI payload result mode = JobsAPI
  { listJobs
      :: mode
        :- Summary "List jobs"
          :> Description
               "Filters combine. The payload filter searches the payload text."
          :> QueryParam "limit" PageLimit
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
  -- ^ @GET \/queues\/:queue\/jobs?limit=N&offset=N&group_key=X&parent_id=N&job_id=N&roots_only&status=S&claimed_by=UUID&kind=X&payload=text&rate_limit_prefix=X&concurrency_prefix=X&sort_by=...&sort_dir=...@
  , insertJob
      :: mode
        :- Summary "Enqueue a job"
          :> Description
               "A duplicate of an ignore dedup key returns the existing job. A duplicate of a replace dedup key replaces the job in the queue."
          :> Throws 409 "A replace dedup key matched a job that is in flight, flagged for cancel, or has children."
          :> Throws 409 "An ignore dedup key matched a job that was deleted before the server could read it."
          :> ReqBody '[JSON] (ApiJobWrite payload)
          :> Post '[JSON] (JobResponse (JobRead payload))
  -- ^ @POST \/queues\/:queue\/jobs@
  , insertJobsBatch
      :: mode
        :- Summary "Enqueue many jobs"
          :> Description
               "Returns the jobs inserted or replaced. The response omits a job that an ignore dedup key skips, and a job that a replace dedup key cannot replace."
          :> "batch"
          :> ReqBody '[JSON] (BatchInsertRequest payload)
          :> Post '[JSON] (BatchInsertResponse payload)
  -- ^ @POST \/queues\/:queue\/jobs\/batch@
  , getJob
      :: mode
        :- Summary "Show a job"
          :> Description "The response includes the derived status."
          :> Capture "id" Int64
          :> Get '[JSON] (JobResponse (ApiJobWithStatus (Stored payload)))
  -- ^ @GET \/queues\/:queue\/jobs\/:id@
  , cancelJob
      :: mode
        :- Summary "Cancel a job and its descendants"
          :> Description
               "Deletes the job and every descendant, also the jobs in flight. The handler of a job in flight continues, and its ack finds no job. Use force-cancel to stop the handler."
          :> Capture "id" Int64
          :> DeleteNoContent
  -- ^ @DELETE \/queues\/:queue\/jobs\/:id@
  , forceCancelJob
      :: mode
        :- Summary "Cancel a job and interrupt its handlers"
          :> Description
               "Deletes the jobs in the tree that are not in flight. A job in flight receives a cancel flag and cannot be claimed again. A worker pool stops its handler. An HTTP claimant with a live lease receives 409 on its next ack, nack or extend, until the reaper deletes the job. Then it receives 404. A claimant whose lease expired before the force-cancel receives 404."
          :> Capture "id" Int64
          :> "force-cancel"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/force-cancel@
  , ackClaimedJob
      :: mode
        :- Summary "Complete a held job"
          :> Description
               "Send the claimSeq and claimedBy from the claim. The server stores the optional result for the parent rollup or the archive."
          :> Throws 409 LeaseRefusal
          :> Capture "id" Int64
          :> "ack"
          :> ReqBody '[JSON] (AckRequest result)
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/ack@
  , nackClaimedJob
      :: mode
        :- Summary "Return a held job"
          :> Description "Restores the attempt that the claim used. The job becomes claimable when its lease expires."
          :> Throws 409 LeaseRefusal
          :> Capture "id" Int64
          :> "nack"
          :> ReqBody '[JSON] JobLease
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/nack@
  , extendClaimedJob
      :: mode
        :- Summary "Extend a held lease"
          :> Description "Sets the lease to expire leaseSeconds from now."
          :> Throws 409 LeaseRefusal
          :> Capture "id" Int64
          :> "extend"
          :> ReqBody '[JSON] ExtendRequest
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/extend@
  , promoteJob
      :: mode
        :- Summary "Make a job visible now"
          :> Throws 409 "The job is already visible, suspended, in flight, or flagged for cancel."
          :> Capture "id" Int64
          :> "promote"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/promote@
  , rescheduleJob
      :: mode
        :- Summary "Set when a job becomes visible"
          :> Throws 409 "The job is in flight, suspended, flagged for cancel, or out of attempts, or another operation changed it."
          :> Capture "id" Int64
          :> "reschedule"
          :> ReqBody '[JSON] RescheduleRequest
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/reschedule@
  , moveToDLQ
      :: mode
        :- Summary "Move a job to the DLQ"
          :> Description
               "A rollup takes its descendants with it. A job in flight is moved too. Its handler continues, and its ack finds no job."
          :> Throws 409 "Another operation changed or deleted the job between the read and the move."
          :> Capture "id" Int64
          :> "move-to-dlq"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/move-to-dlq@
  , pauseChildren
      :: mode
        :- Summary "Suspend the visible descendants of a job"
          :> Description
               "The operation skips descendants that are in flight, delayed or throttled. Succeeds when nothing is suspended. 404 when the job is gone."
          :> Capture "id" Int64
          :> "pause-children"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/pause-children@
  , resumeChildren
      :: mode
        :- Summary "Resume the suspended descendants of a job"
          :> Description
               "A finalizer with children in the queue stays suspended. Succeeds when nothing is resumed. 404 when the job is gone."
          :> Capture "id" Int64
          :> "resume-children"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/resume-children@
  , suspendJob
      :: mode
        :- Summary "Suspend a job"
          :> Description "A suspended job cannot be claimed."
          :> Throws 409 "The job is already suspended, or it is in flight."
          :> Capture "id" Int64
          :> "suspend"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/suspend@
  , resumeJob
      :: mode
        :- Summary "Resume a suspended job"
          :> Throws 409 "The job is not suspended, it is a finalizer with children in the queue, or another operation changed it."
          :> Capture "id" Int64
          :> "resume"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/jobs\/:id\/resume@
  }
  deriving stock (Generic)

-- | One queue's DLQ routes.
data DLQAPI payload mode = DLQAPI
  { listDLQ
      :: mode
        :- Summary "List dead-lettered jobs"
          :> Description
               "The payload and error filters search the payload and the last error."
          :> QueryParam "limit" PageLimit
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
  -- ^ @GET \/queues\/:queue\/dlq?limit=N&offset=N&parent_id=N&job_id=N&group_key=X&kind=X&payload=text&error=text&sort_by=...&sort_dir=...@
  , retryFromDLQ
      :: mode
        :- Summary "Retry a dead-lettered job"
          :> Description "Moves the job back to the queue."
          :> Throws 409 "The parent of the job is gone from the queue and the DLQ."
          :> Capture "id" Int64
          :> "retry"
          :> ReqBody' '[Description PayloadEditNote] '[OptionalJSON] (Maybe (PayloadEdit payload))
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/dlq\/:id\/retry@
  , deleteDLQ
      :: mode
        :- Summary "Delete a dead-lettered job"
          :> Description "Resumes its parent when no child of the parent is left in the queue."
          :> Capture "id" Int64
          :> DeleteNoContent
  -- ^ @DELETE \/queues\/:queue\/dlq\/:id@
  , deleteDLQBatch
      :: mode
        :- Summary "Delete dead-lettered jobs"
          :> Description "Returns the number deleted. The server skips unknown ids."
          :> "batch-delete"
          :> ReqBody '[JSON] BatchDeleteRequest
          :> Post '[JSON] BatchDeleteResponse
  -- ^ @POST \/queues\/:queue\/dlq\/batch-delete@
  }
  deriving stock (Generic)

-- | One queue's archive routes.
data ArchiveAPI payload mode = ArchiveAPI
  { listArchive
      :: mode
        :- Summary "List archived jobs"
          :> Description "The payload filter searches the payload text."
          :> QueryParam "limit" PageLimit
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
  -- ^ @GET \/queues\/:queue\/archive?limit=N&offset=N&parent_id=N&job_id=N&group_key=X&kind=X&payload=text&completed_after=T&completed_before=T&sort_by=...&sort_dir=...@
  , reEnqueueArchive
      :: mode
        :- Summary "Run an archived job again"
          :> Description "Inserts a new job. The archive entry stays."
          :> Capture "id" Int64
          :> "reenqueue"
          :> ReqBody' '[Description PayloadEditNote] '[OptionalJSON] (Maybe (PayloadEdit payload))
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/archive\/:id\/reenqueue@
  , deleteArchive
      :: mode
        :- Summary "Delete an archived job"
          :> Capture "id" Int64
          :> DeleteNoContent
  -- ^ @DELETE \/queues\/:queue\/archive\/:id@
  , deleteArchiveBatch
      :: mode
        :- Summary "Delete archived jobs"
          :> Description "Returns the number deleted. The server skips unknown ids."
          :> "batch-delete"
          :> ReqBody '[JSON] BatchDeleteRequest
          :> Post '[JSON] BatchDeleteResponse
  -- ^ @POST \/queues\/:queue\/archive\/batch-delete@
  }
  deriving stock (Generic)

-- | One queue's stats route.
data StatsAPI mode = StatsAPI
  { getStats
      :: mode
        :- Summary "Show queue stats"
          :> Get '[JSON] StatsResponse
  -- ^ @GET \/queues\/:queue\/stats@
  }
  deriving stock (Generic)

-- | Schema-wide maintenance. A running worker pool does the same work.
newtype MaintenanceAPI mode = MaintenanceAPI
  { runMaintenance
      :: mode
        :- Summary "Run a maintenance pass"
          :> Description
               "Runs the work of the reaper once. An operation that another caller runs at the same time is skipped and is not in the response."
          :> Post '[JSON] MaintenanceResponse
  -- ^ @POST \/maintenance@
  }
  deriving stock (Generic)

-- | One queue's routes.
data TableAPI payload result mode = TableAPI
  { jobs :: mode :- "jobs" :> NamedRoutes (JobsAPI payload result)
  -- ^ @\/queues\/:queue\/jobs@ The job routes.
  , claimJobs
      :: mode
        :- Summary "Claim jobs"
          :> Description
               "Leases up to maxJobs visible jobs for leaseSeconds. Each job carries the claimSeq and claimedBy that ack, nack and extend need. The server does not renew the lease. A paused queue returns no jobs."
          :> "claim"
          :> ReqBody '[JSON] ClaimRequest
          :> Post '[JSON] (ClaimResponse payload)
  -- ^ @POST \/queues\/:queue\/claim@
  , dlq :: mode :- "dlq" :> NamedRoutes (DLQAPI payload)
  -- ^ @\/queues\/:queue\/dlq@ The DLQ routes.
  , archive :: mode :- "archive" :> NamedRoutes (ArchiveAPI payload)
  -- ^ @\/queues\/:queue\/archive@ The archive routes.
  , stats :: mode :- "stats" :> NamedRoutes StatsAPI
  -- ^ @\/queues\/:queue\/stats@ The stats route.
  , listKinds
      :: mode
        :- Summary "List payload kinds"
          :> "kinds"
          :> Get '[JSON] [Text]
  -- ^ @GET \/queues\/:queue\/kinds@
  , listGroups
      :: mode
        :- Summary "List open groups"
          :> Description "The largest groups come first."
          :> "groups"
          :> QueryParam "limit" PageLimit
          :> QueryParam "offset" Int
          :> QueryParam "group_key" Text
          :> Get '[JSON] GroupsResponse
  -- ^ @GET \/queues\/:queue\/groups?limit=N&offset=N&group_key=X@
  , getDetails
      :: mode
        :- Summary "Show the pause state of a queue"
          :> Description "Null when the queue was never paused or resumed."
          :> "details"
          :> Get '[JSON] (Maybe QueueRow)
  -- ^ @GET \/queues\/:queue\/details@
  , pauseQueue
      :: mode
        :- Summary "Pause a queue"
          :> Description "Workers do not claim from the queue. Claims through this API return no jobs."
          :> "pause"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/pause@
  , resumeQueue
      :: mode
        :- Summary "Resume a queue"
          :> "resume"
          :> PostNoContent
  -- ^ @POST \/queues\/:queue\/resume@
  }
  deriving stock (Generic)

-- | The queue registry routes.
data QueuesAPI mode = QueuesAPI
  { listQueues
      :: mode
        :- Summary "List queues"
          :> Get '[JSON] QueuesResponse
  -- ^ @GET \/queues@
  , getAllStats
      :: mode
        :- Summary "Show stats for every queue"
          :> "stats"
          :> Get '[JSON] AllStatsResponse
  -- ^ @GET \/queues\/stats@
  }
  deriving stock (Generic)

-- | The SSE stream, served by a raw WAI handler.
type EventsAPI = "stream" :> Raw

-- | The cron schedule routes.
data CronAPI mode = CronAPI
  { listSchedules
      :: mode
        :- Summary "List cron schedules"
          :> "schedules"
          :> QueryParam "queue" Text
          :> Get '[JSON] CronSchedulesResponse
  -- ^ @GET \/cron\/schedules@
  --
  -- Optional @?queue=name@ scopes the result to a single queue.
  , updateSchedule
      :: mode
        :- Summary "Override a cron schedule"
          :> Description "A null field clears its override. An absent field keeps it."
          :> Throws 400 "An override is not valid."
          :> "schedules"
          :> Capture "name" Text
          :> ReqBody '[JSON] CronScheduleUpdate
          :> Patch '[JSON] CronScheduleView
  -- ^ @PATCH \/cron\/schedules\/:name@
  , runSchedule
      :: mode
        :- Summary "Run a cron schedule now"
          :> Throws 409 "The schedule is disabled, or it already has a run that waits to start."
          :> "schedules"
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
        :- Summary "List workers"
          :> QueryParam "queue" Text
          :> QueryParam "live" Double
          :> Get '[JSON] WorkersResponse
  -- ^ @GET \/workers@
  --
  -- Optional @?queue=name@ scopes the result to a single queue.
  -- Optional @?live=seconds@ filters to workers with a heartbeat inside the
  -- threshold. Without it all rows are returned.
  , pauseWorker
      :: mode
        :- Summary "Pause a worker"
          :> Description "The worker does not claim new jobs."
          :> Capture "id" UUID
          :> "pause"
          :> PostNoContent
  -- ^ @POST \/workers\/:id\/pause@
  --
  -- Sets the worker's @paused@ flag.
  , resumeWorker
      :: mode
        :- Summary "Resume a worker"
          :> Capture "id" UUID
          :> "resume"
          :> PostNoContent
  -- ^ @POST \/workers\/:id\/resume@
  }
  deriving stock (Generic)

-- | Rate limits API routes, schema-wide.
data RateLimitsAPI mode = RateLimitsAPI
  { listRateLimits
      :: mode
        :- Summary "List rate-limit policies"
          :> Get '[JSON] RateLimitPoliciesResponse
  -- ^ @GET \/rate-limits@
  , listRateLimitBuckets
      :: mode
        :- Summary "List the buckets of a rate-limit policy"
          :> Capture "prefix" Text
          :> "buckets"
          :> QueryParam "limit" KeyPageLimit
          :> QueryParam "offset" Int
          :> Get '[JSON] RateLimitBucketsResponse
  -- ^ @GET \/rate-limits\/:prefix\/buckets?limit=N&offset=N@
  , updateRateLimitPolicy
      :: mode
        :- Summary "Override a rate-limit policy"
          :> Description "A null field clears its override. An absent field keeps it."
          :> Throws 400 "An override value is not valid."
          :> Capture "prefix" Text
          :> ReqBody '[JSON] RateLimitPolicyUpdate
          :> Patch '[JSON] RateLimitPolicyView
  -- ^ @PATCH \/rate-limits\/:prefix@
  , resetRateLimitBuckets
      :: mode
        :- Summary "Refill the buckets of a rate-limit policy"
          :> Description "Refills every bucket under the prefix to full. Returns the number refilled."
          :> Capture "prefix" Text
          :> "reset"
          :> Post '[JSON] RateLimitResetResponse
  -- ^ @POST \/rate-limits\/:prefix\/reset@
  , addRateLimitTokens
      :: mode
        :- Summary "Add tokens to a rate-limit bucket"
          :> Description "Wakes the throttled jobs of the key."
          :> Throws 400 "The token count is 0 or less, or the key is not under the prefix."
          :> Capture "prefix" Text
          :> "buckets"
          :> Capture "key" Text
          :> "tokens"
          :> ReqBody '[JSON] AddTokensRequest
          :> Post '[JSON] AddTokensResponse
  -- ^ @POST \/rate-limits\/:prefix\/buckets\/:key\/tokens@
  --
  -- @:key@ is the full bucket key with its prefix, as the bucket listing shows it.
  , pruneRateLimitBuckets
      :: mode
        :- Summary "Prune idle rate-limit buckets"
          :> Throws 400 "The idle value is negative."
          :> "prune"
          :> QueryParam "idle" Double
          :> Post '[JSON] PruneResponse
  -- ^ @POST \/rate-limits\/prune?idle=seconds@
  --
  -- The default idle is the server's maintenance bucket idle age.
  }
  deriving stock (Generic)

-- | Concurrency API routes, schema-wide.
data ConcurrencyAPI mode = ConcurrencyAPI
  { listConcurrency
      :: mode
        :- Summary "List concurrency policies"
          :> Get '[JSON] ConcurrencyPoliciesResponse
  -- ^ @GET \/concurrency@
  , listConcurrencyKeys
      :: mode
        :- Summary "List the keys of a concurrency policy"
          :> Capture "prefix" Text
          :> "keys"
          :> QueryParam "limit" KeyPageLimit
          :> QueryParam "offset" Int
          :> Get '[JSON] ConcurrencyKeysResponse
  -- ^ @GET \/concurrency\/:prefix\/keys?limit=N&offset=N@
  , updateConcurrencyPolicy
      :: mode
        :- Summary "Override a concurrency limit"
          :> Description "A null overrideLimit clears the override. An absent one keeps it."
          :> Throws 400 "The override limit is negative."
          :> Capture "prefix" Text
          :> ReqBody '[JSON] ConcurrencyPolicyUpdate
          :> Patch '[JSON] ConcurrencyPolicyView
  -- ^ @PATCH \/concurrency\/:prefix@
  , reconcileConcurrency
      :: mode
        :- Summary "Recount in-flight jobs per concurrency key"
          :> Description "Repairs each in-flight count from the live jobs. Returns the rows repaired."
          :> "reconcile"
          :> Post '[JSON] ConcurrencyReconcileResponse
  -- ^ @POST \/concurrency\/reconcile@
  , pruneConcurrencyKeys
      :: mode
        :- Summary "Prune drained concurrency keys"
          :> Description "Deletes keys with no live job."
          :> "prune"
          :> Post '[JSON] PruneResponse
  -- ^ @POST \/concurrency\/prune@
  }
  deriving stock (Generic)

-- | Liveness and readiness.
data HealthAPI mode = HealthAPI
  { getHealth
      :: mode
        :- Summary "Check readiness"
          :> ThrowsBody 503 "The database is not reachable." HealthResponse
          :> Get '[JSON] HealthResponse
  -- ^ @GET \/health@
  --
  -- Readiness. Reaches the database and reports its connection counters.
  , getLiveness
      :: mode
        :- Summary "Check liveness"
          :> "live"
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
    ("queues" :> SpecName spec :> NamedRoutes (TableAPI (SpecPayload spec) (SpecResult spec)))
      :<|> RegistryToAPI rest

-- | Top-level Arbiter API, mounted at @\/api@. The route tree under that
-- prefix is generated from the registry. See 'RegistryToAPI' for the shape.
type ArbiterAPI :: JobPayloadRegistry -> Type
type ArbiterAPI registry = "api" :> RegistryToAPI registry

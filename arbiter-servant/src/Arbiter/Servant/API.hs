{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-orphans #-}

-- | Servant API type definitions for Arbiter job queue admin interface.
--
-- The API is generated from the registry, creating separate endpoints for each table.
module Arbiter.Servant.API
  ( ArbiterAPI
  , RegistryToAPI
  , SharedAPI
  , TableAPI (..)
  , JobsAPI (..)
  , DLQAPI (..)
  , StatsAPI (..)
  , QueuesAPI (..)
  , EventsAPI
  , CronAPI (..)
  , WorkersAPI (..)
  , RateLimitsAPI (..)
  , ConcurrencyAPI (..)
  ) where

import Arbiter.Core.Job.Types (JobStatus, jobStatusToText)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Arbiter.Core.Sql.Jobs
  ( DLQSortColumn
  , JobSortColumn
  , SortDir
  , dlqSortColumnName
  , jobSortColumnName
  , sortDirSql
  )
import Data.Int (Int64)
import Data.Kind (Type)
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID.Types (UUID)
import GHC.Generics (Generic)
import GHC.TypeLits (Symbol)
import Servant.API

import Arbiter.Servant.Types

-- | Case-insensitive lookup of an enum value by its canonical name.
parseEnum :: (Bounded a, Enum a) => (a -> Text) -> Text -> Either Text a
parseEnum toName t =
  let lower = T.toLower t
      table = [(T.toLower (toName x), x) | x <- [minBound .. maxBound]]
   in case lookup lower table of
        Just x -> Right x
        Nothing -> Left $ "unknown value: " <> t

instance FromHttpApiData JobSortColumn where
  parseQueryParam = parseEnum jobSortColumnName

instance ToHttpApiData JobSortColumn where
  toUrlPiece = jobSortColumnName

instance FromHttpApiData DLQSortColumn where
  parseQueryParam = parseEnum dlqSortColumnName

instance ToHttpApiData DLQSortColumn where
  toUrlPiece = dlqSortColumnName

instance FromHttpApiData SortDir where
  parseQueryParam = parseEnum sortDirSql

instance ToHttpApiData SortDir where
  toUrlPiece = sortDirSql

instance FromHttpApiData JobStatus where
  parseQueryParam = parseEnum jobStatusToText

instance ToHttpApiData JobStatus where
  toUrlPiece = jobStatusToText

-- | Jobs API routes - manage jobs in a specific table
data JobsAPI payload mode = JobsAPI
  { -- GET /:table/jobs?limit=N&offset=N&group_key=X&parent_id=N&roots_only&status=S&sort_by=...&sort_dir=...
    listJobs
      :: mode
        :- QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> QueryParam "group_key" Text
          :> QueryParam "parent_id" Int64
          :> QueryFlag "roots_only"
          :> QueryParam "status" JobStatus
          :> QueryParam "sort_by" JobSortColumn
          :> QueryParam "sort_dir" SortDir
          :> Get '[JSON] (JobsResponse payload)
  , -- POST /:table/jobs (insert new job)
    insertJob
      :: mode
        :- ReqBody '[JSON] (ApiJobWrite payload)
          :> Post '[JSON] (JobResponse (ApiJob payload))
  , -- POST /:table/jobs/batch (insert multiple jobs)
    insertJobsBatch
      :: mode
        :- "batch"
          :> ReqBody '[JSON] (BatchInsertRequest payload)
          :> Post '[JSON] (BatchInsertResponse payload)
  , -- GET /:table/jobs/:id
    getJob
      :: mode
        :- Capture "id" Int64
          :> Get '[JSON] (JobResponse (ApiJobWithStatus payload))
  , -- DELETE /:table/jobs/:id (cancel job)
    cancelJob
      :: mode
        :- Capture "id" Int64
          :> DeleteNoContent
  , -- POST /:table/jobs/:id/force-cancel (cascade-delete + interrupt running handler)
    forceCancelJob
      :: mode
        :- Capture "id" Int64
          :> "force-cancel"
          :> PostNoContent
  , -- POST /:table/jobs/:id/promote
    promoteJob
      :: mode
        :- Capture "id" Int64
          :> "promote"
          :> PostNoContent
  , -- POST /:table/jobs/:id/move-to-dlq
    moveToDLQ
      :: mode
        :- Capture "id" Int64
          :> "move-to-dlq"
          :> PostNoContent
  , -- POST /:table/jobs/:id/pause-children
    pauseChildren
      :: mode
        :- Capture "id" Int64
          :> "pause-children"
          :> PostNoContent
  , -- POST /:table/jobs/:id/resume-children
    resumeChildren
      :: mode
        :- Capture "id" Int64
          :> "resume-children"
          :> PostNoContent
  , -- POST /:table/jobs/:id/suspend
    suspendJob
      :: mode
        :- Capture "id" Int64
          :> "suspend"
          :> PostNoContent
  , -- POST /:table/jobs/:id/resume
    resumeJob
      :: mode
        :- Capture "id" Int64
          :> "resume"
          :> PostNoContent
  }
  deriving stock (Generic)

-- | DLQ API routes - manage failed jobs in a specific table
data DLQAPI payload mode = DLQAPI
  { -- GET /:table/dlq?limit=N&offset=N&parent_id=N&group_key=X&sort_by=...&sort_dir=...
    listDLQ
      :: mode
        :- QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> QueryParam "parent_id" Int64
          :> QueryParam "group_key" Text
          :> QueryParam "sort_by" DLQSortColumn
          :> QueryParam "sort_dir" SortDir
          :> Get '[JSON] (DLQResponse payload)
  , -- POST /:table/dlq/:id/retry (move back to main queue)
    retryFromDLQ
      :: mode
        :- Capture "id" Int64
          :> "retry"
          :> PostNoContent
  , -- DELETE /:table/dlq/:id (permanently delete)
    deleteDLQ
      :: mode
        :- Capture "id" Int64
          :> DeleteNoContent
  , -- POST /:table/dlq/batch-delete
    deleteDLQBatch
      :: mode
        :- "batch-delete"
          :> ReqBody '[JSON] BatchDeleteRequest
          :> Post '[JSON] BatchDeleteResponse
  }
  deriving stock (Generic)

-- | Stats API routes - queue statistics for a specific table
data StatsAPI mode = StatsAPI
  { -- GET /:table/stats
    getStats
      :: mode
        :- Get '[JSON] StatsResponse
  }
  deriving stock (Generic)

-- | API routes for a specific table
data TableAPI payload mode = TableAPI
  { jobs :: mode :- "jobs" :> NamedRoutes (JobsAPI payload)
  , dlq :: mode :- "dlq" :> NamedRoutes (DLQAPI payload)
  , stats :: mode :- "stats" :> NamedRoutes StatsAPI
  }
  deriving stock (Generic)

-- | Queues API routes - list available queues
data QueuesAPI mode = QueuesAPI
  { -- GET /queues
    listQueues
      :: mode
        :- Get '[JSON] QueuesResponse
  , -- GET /queues/stats
    getAllStats
      :: mode
        :- "stats"
          :> Get '[JSON] AllStatsResponse
  , -- GET /queues/:queue/details
    getDetails
      :: mode
        :- Capture "queue" Text
          :> "details"
          :> Get '[JSON] (Maybe QueueRow)
  , -- POST /queues/:queue/pause
    pauseQueue
      :: mode
        :- Capture "queue" Text
          :> "pause"
          :> PostNoContent
  , -- POST /queues/:queue/resume
    resumeQueue
      :: mode
        :- Capture "queue" Text
          :> "resume"
          :> PostNoContent
  }
  deriving stock (Generic)

-- | Events API type - raw WAI handler for SSE streaming
type EventsAPI = "stream" :> Raw

-- | Cron API routes - manage cron schedules
data CronAPI mode = CronAPI
  { -- GET /cron/schedules
    --
    -- Optional @?queue=name@ scopes the result to a single queue.
    listSchedules
      :: mode
        :- "schedules"
          :> QueryParam "queue" Text
          :> Get '[JSON] CronSchedulesResponse
  , -- PATCH /cron/schedules/:name
    updateSchedule
      :: mode
        :- "schedules"
          :> Capture "name" Text
          :> ReqBody '[JSON] CronScheduleUpdate
          :> Patch '[JSON] CronScheduleRow
  , -- POST /cron/schedules/:name/run
    runSchedule
      :: mode
        :- "schedules"
          :> Capture "name" Text
          :> "run"
          :> PostNoContent
  }
  deriving stock (Generic)

-- | Workers API routes - view the registry and pause/resume individual workers
data WorkersAPI mode = WorkersAPI
  { -- GET /workers
    --
    -- Optional @?queue=name@ scopes the result to a single queue.
    -- Optional @?live=seconds@ filters to workers whose last_heartbeat is
    -- within the given threshold (otherwise all rows are returned).
    listWorkers
      :: mode
        :- QueryParam "queue" Text
          :> QueryParam "live" Double
          :> Get '[JSON] WorkersResponse
  , -- POST /workers/:id/pause
    --
    -- Sets the worker's @paused@ flag.
    pauseWorker
      :: mode
        :- Capture "id" UUID
          :> "pause"
          :> PostNoContent
  , -- POST /workers/:id/resume
    resumeWorker
      :: mode
        :- Capture "id" UUID
          :> "resume"
          :> PostNoContent
  }
  deriving stock (Generic)

-- | Rate limits API routes. Global (not queue-scoped).
data RateLimitsAPI mode = RateLimitsAPI
  { listRateLimits
      :: mode
        :- Get '[JSON] RateLimitPoliciesResponse
  , listRateLimitBuckets
      :: mode
        :- Capture "prefix" Text
          :> "buckets"
          :> QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> Get '[JSON] RateLimitBucketsResponse
  , updateRateLimitPolicy
      :: mode
        :- Capture "prefix" Text
          :> ReqBody '[JSON] RateLimitPolicyUpdate
          :> Patch '[JSON] RateLimitPolicyView
  , resetRateLimitBuckets
      :: mode
        :- Capture "prefix" Text
          :> "reset"
          :> Post '[JSON] RateLimitResetResponse
  }
  deriving stock (Generic)

-- | Concurrency API routes. Global (not queue-scoped).
data ConcurrencyAPI mode = ConcurrencyAPI
  { listConcurrency
      :: mode
        :- Get '[JSON] ConcurrencyPoliciesResponse
  , listConcurrencyKeys
      :: mode
        :- Capture "prefix" Text
          :> "keys"
          :> QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> Get '[JSON] ConcurrencyKeysResponse
  , updateConcurrencyPolicy
      :: mode
        :- Capture "prefix" Text
          :> ReqBody '[JSON] ConcurrencyPolicyUpdate
          :> Patch '[JSON] ConcurrencyPolicyView
  , reconcileConcurrency
      :: mode
        :- "reconcile"
          :> Post '[JSON] ConcurrencyReconcileResponse
  }
  deriving stock (Generic)

-- | Shared top-level routes appended after the per-table routes.
type SharedAPI =
  "queues" :> NamedRoutes QueuesAPI
    :<|> "events" :> EventsAPI
    :<|> "cron" :> NamedRoutes CronAPI
    :<|> "workers" :> NamedRoutes WorkersAPI
    :<|> "rate-limits" :> NamedRoutes RateLimitsAPI
    :<|> "concurrency" :> NamedRoutes ConcurrencyAPI

-- | Generates a 'TableAPI' route per registry entry, followed by the shared
-- top-level routes.
type family RegistryToAPI (registry :: [(Symbol, Type)]) :: Type where
  RegistryToAPI '[] = SharedAPI
  RegistryToAPI ('(tableName, payload) ': '[]) =
    tableName :> NamedRoutes (TableAPI payload) :<|> SharedAPI
  RegistryToAPI ('(tableName, payload) ': rest) =
    (tableName :> NamedRoutes (TableAPI payload)) :<|> RegistryToAPI rest

-- | Top-level Arbiter API, mounted at @\/api\/v1@. The route tree under that
-- prefix is generated from the registry; see 'RegistryToAPI' for the shape.
type ArbiterAPI :: JobPayloadRegistry -> Type
type ArbiterAPI registry = "api" :> "v1" :> RegistryToAPI registry

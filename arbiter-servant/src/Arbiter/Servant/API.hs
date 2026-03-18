{-# LANGUAGE DataKinds #-}
{-# LANGUAGE TypeFamilies #-}

-- | Servant API type definitions for Arbiter job queue admin interface.
--
-- The API is generated from the registry, creating separate endpoints for each table.
module Arbiter.Servant.API
  ( ArbiterAPI
  , RegistryToAPI
  , TableAPI (..)
  , JobsAPI (..)
  , DLQAPI (..)
  , StatsAPI (..)
  , QueuesAPI (..)
  , EventsAPI
  , CronAPI (..)
  ) where

import Data.Int (Int64)
import Data.Kind (Type)
import Data.Text (Text)
import GHC.Generics (Generic)
import GHC.TypeLits (Symbol)
import Servant.API

import Arbiter.Servant.Types

-- | Jobs API routes - manage jobs in a specific table
data JobsAPI payload mode = JobsAPI
  { -- GET /:table/jobs?limit=N&offset=N&group_key=X&parent_id=N&suspended=B
    listJobs
      :: mode
        :- QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> QueryParam "group_key" Text
          :> QueryParam "parent_id" Int64
          :> QueryParam "suspended" Bool
          :> Get '[JSON] (JobsResponse payload)
  , -- POST /:table/jobs (insert new job)
    insertJob
      :: mode
        :- ReqBody '[JSON] (ApiJobWrite payload)
          :> Post '[JSON] (JobResponse payload)
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
          :> Get '[JSON] (JobResponse payload)
  , -- GET /:table/jobs/in-flight?limit=N&offset=N
    getInFlightJobs
      :: mode
        :- "in-flight"
          :> QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> Get '[JSON] (JobsResponse payload)
  , -- DELETE /:table/jobs/:id (cancel job)
    cancelJob
      :: mode
        :- Capture "id" Int64
          :> DeleteNoContent
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
  { -- GET /:table/dlq?limit=N&offset=N&parent_id=N&group_key=X
    listDLQ
      :: mode
        :- QueryParam "limit" Int
          :> QueryParam "offset" Int
          :> QueryParam "parent_id" Int64
          :> QueryParam "group_key" Text
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
  }
  deriving stock (Generic)

-- | Events API type - raw WAI handler for SSE streaming
type EventsAPI = "stream" :> Raw

-- | Cron API routes - manage cron schedules
data CronAPI mode = CronAPI
  { -- GET /cron/schedules
    listSchedules
      :: mode
        :- "schedules"
          :> Get '[JSON] CronSchedulesResponse
  , -- PATCH /cron/schedules/:name
    updateSchedule
      :: mode
        :- "schedules"
          :> Capture "name" Text
          :> ReqBody '[JSON] CronScheduleUpdate
          :> Patch '[JSON] CronScheduleRow
  }
  deriving stock (Generic)

-- | Type family to build API routes from registry
-- For registry '[ '("table1", Payload1), '("table2", Payload2) ]
-- Generates: Capture "table1" :> NamedRoutes (TableAPI Payload1) :<|> Capture "table2" :> NamedRoutes (TableAPI Payload2) :<|> queues :<|> events
type family RegistryToAPI (registry :: [(Symbol, Type)]) :: Type where
  RegistryToAPI '[] =
    "queues" :> NamedRoutes QueuesAPI
      :<|> "events" :> EventsAPI
      :<|> "cron" :> NamedRoutes CronAPI
  RegistryToAPI ('(tableName, payload) ': '[]) =
    tableName :> NamedRoutes (TableAPI payload)
      :<|> "queues" :> NamedRoutes QueuesAPI
      :<|> "events" :> EventsAPI
      :<|> "cron" :> NamedRoutes CronAPI
  RegistryToAPI ('(tableName, payload) ': rest) =
    (tableName :> NamedRoutes (TableAPI payload)) :<|> RegistryToAPI rest

-- | Top-level Arbiter API
--
-- Routes are generated from the registry:
--
--   * @\/api\/v1\/:tableName\/jobs\/...@ for each table in registry
--   * @\/api\/v1\/queues@ - list all available queues
--   * @\/api\/v1\/events\/stream@ - real-time job updates (SSE)
type ArbiterAPI registry = "api" :> "v1" :> RegistryToAPI registry

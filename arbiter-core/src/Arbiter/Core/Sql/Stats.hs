{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Stats SQL templates.
module Arbiter.Core.Sql.Stats
  ( getQueueStatsSQL
  , allQueueStatsSQL
  , countChildrenBatchSQL
  ) where

import Data.Text (Text)
import NeatInterpolation (text)

import Arbiter.Core.Job.Schema (jobQueueTable)
import Arbiter.Core.Queues (arbiterQueuesTable)
import Arbiter.Core.Sql.Jobs (jobStatusCaseSQL, unionAllOverQueueTables)
import Arbiter.Core.Worker (arbiterWorkersTable)

-- | Per-status queue counts plus the age of the oldest @ready@ job.
--
-- Counts are broken down by the canonical 'jobStatusCaseSQL' taxonomy so the
-- UI can distinguish actively-leased ('in_flight') jobs from merely delayed
-- ('scheduled'/'backoff'/'throttled') or 'suspended' ones. The six status counts
-- sum to @total_jobs@. @oldest_ready_age_seconds@ measures only @ready@ rows so a
-- far-future scheduled job no longer skews the queue's backlog latency.
getQueueStatsSQL :: Text -> Text -> Text
getQueueStatsSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      statusCase = jobStatusCaseSQL
   in [text|
        WITH classified AS (SELECT inserted_at, ${statusCase} AS status FROM ${tbl})
        SELECT ${statsAggColumns} FROM classified
      |]

-- | Per-status count columns, shared by the single- and all-queue stats queries.
statsAggColumns :: Text
statsAggColumns =
  [text|
    COUNT(*) AS total_jobs,
    COUNT(*) FILTER (WHERE status = 'ready') AS ready_jobs,
    COUNT(*) FILTER (WHERE status = 'in_flight') AS in_flight_jobs,
    COUNT(*) FILTER (WHERE status = 'scheduled') AS scheduled_jobs,
    COUNT(*) FILTER (WHERE status = 'backoff') AS backoff_jobs,
    COUNT(*) FILTER (WHERE status = 'throttled') AS throttled_jobs,
    COUNT(*) FILTER (WHERE status = 'suspended') AS suspended_jobs,
    EXTRACT(EPOCH FROM (NOW() - MIN(inserted_at) FILTER (WHERE status = 'ready')))::float8 AS oldest_ready_age_seconds
  |]

-- | Every queue's stats in one query, tagged by name. Caller guards the empty list.
allQueueStatsSQL :: Text -> [Text] -> Text
allQueueStatsSQL schema tableNames =
  let statusCase = jobStatusCaseSQL
      qTbl = arbiterQueuesTable schema
      wTbl = arbiterWorkersTable schema
      -- A worker counts as live while its heartbeat is within its own stale threshold.
      liveWorker = "last_heartbeat >= NOW() - stale_threshold_secs * interval '1 second'" :: Text
   in unionAllOverQueueTables schema tableNames $ \tableName tbl ->
        [text|
          SELECT '${tableName}' AS queue, ${statsAggColumns},
                 COALESCE((SELECT paused FROM ${qTbl} WHERE queue_name = '${tableName}'), FALSE) AS queue_paused,
                 (SELECT COUNT(*) FROM ${wTbl} WHERE queue_name = '${tableName}' AND ${liveWorker})::int8 AS workers_live,
                 (SELECT COUNT(*) FILTER (WHERE paused) FROM ${wTbl} WHERE queue_name = '${tableName}' AND ${liveWorker})::int8 AS workers_paused
          FROM (SELECT inserted_at, ${statusCase} AS status FROM ${tbl}) classified
        |]

-- ---------------------------------------------------------------------------
-- Parent-Child Operations
-- ---------------------------------------------------------------------------

-- | Batch child count: returns (parent_id, total_count, suspended_count) for a set of job IDs
--
-- Parameters: array of job IDs
countChildrenBatchSQL :: Text -> Text -> Text
countChildrenBatchSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        SELECT parent_id, COUNT(*),
               COUNT(*) FILTER (WHERE suspended)
        FROM ${tbl}
        WHERE parent_id = ANY(?)
        GROUP BY parent_id
      |]

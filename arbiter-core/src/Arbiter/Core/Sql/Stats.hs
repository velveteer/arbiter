{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Stats SQL templates.
module Arbiter.Core.Sql.Stats
  ( getQueueStatsSQL
  , allQueueStatsSQL
  , countChildrenBatchSQL
  ) where

import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Codec (RowCodec)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.Queues (arbiterQueuesTable)
import Arbiter.Core.Sql.Jobs (jobStatusCaseSQL, unionAllOverQueueTables)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rawRows, rows)
import Arbiter.Core.SqlLiterals (textLiteral)
import Arbiter.Core.Worker (arbiterWorkersTable)

-- | Per-status queue counts plus the age of the oldest @ready@ and @in_flight@ job.
-- Counts follow the 'jobStatusCaseSQL' taxonomy and sum to @total_jobs@.
getQueueStatsSQL :: RowCodec a -> SchemaName -> TableName -> [Text] -> Query a
getQueueStatsSQL codec schema tableName kinds =
  let stats = queueStatsSelect schema tableName kinds
   in rows codec [sql|${stats}|]

-- | One queue's stats row. The classified rows are aggregated once per kind and once
-- over the whole table in one pass. @total_row@ marks the whole-table row.
queueStatsSelect :: SchemaName -> TableName -> [Text] -> Text
queueStatsSelect schema tableName kinds =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      kindExpr = declaredKindSQL kinds
   in [text|
        SELECT MAX(total_jobs) FILTER (WHERE total_row = 1) AS total_jobs,
               MAX(ready_jobs) FILTER (WHERE total_row = 1) AS ready_jobs,
               MAX(in_flight_jobs) FILTER (WHERE total_row = 1) AS in_flight_jobs,
               MAX(scheduled_jobs) FILTER (WHERE total_row = 1) AS scheduled_jobs,
               MAX(backoff_jobs) FILTER (WHERE total_row = 1) AS backoff_jobs,
               MAX(throttled_jobs) FILTER (WHERE total_row = 1) AS throttled_jobs,
               MAX(suspended_jobs) FILTER (WHERE total_row = 1) AS suspended_jobs,
               MAX(cancelled_jobs) FILTER (WHERE total_row = 1) AS cancelled_jobs,
               MAX(oldest_ready_age_seconds) FILTER (WHERE total_row = 1) AS oldest_ready_age_seconds,
               MAX(oldest_in_flight_age_seconds) FILTER (WHERE total_row = 1) AS oldest_in_flight_age_seconds,
               (SELECT COUNT(*)::int8 FROM ${dlqTbl}) AS dlq_jobs,
               jsonb_object_agg(kind, total_jobs) FILTER (WHERE total_row = 0 AND kind IS NOT NULL) AS kind_counts
        FROM (
          SELECT kind, GROUPING(kind) AS total_row,
                 COUNT(*)::int8 AS total_jobs,
                 COUNT(*) FILTER (WHERE status = 'ready') AS ready_jobs,
                 COUNT(*) FILTER (WHERE status = 'in_flight') AS in_flight_jobs,
                 COUNT(*) FILTER (WHERE status = 'scheduled') AS scheduled_jobs,
                 COUNT(*) FILTER (WHERE status = 'backoff') AS backoff_jobs,
                 COUNT(*) FILTER (WHERE status = 'throttled') AS throttled_jobs,
                 COUNT(*) FILTER (WHERE status = 'suspended') AS suspended_jobs,
                 COUNT(*) FILTER (WHERE status = 'cancelled') AS cancelled_jobs,
                 EXTRACT(EPOCH FROM (
                   clock_timestamp() - MIN(inserted_at) FILTER (WHERE status = 'ready')
                 ))::float8 AS oldest_ready_age_seconds,
                 EXTRACT(EPOCH FROM (
                   clock_timestamp() - MIN(last_attempted_at) FILTER (WHERE status = 'in_flight')
                 ))::float8 AS oldest_in_flight_age_seconds
          FROM (
            SELECT inserted_at, last_attempted_at, ${kindExpr} AS kind, ${jobStatusCaseSQL} AS status
            FROM ${tbl}
          ) classified
          GROUP BY GROUPING SETS ((), (kind))
        ) rollup
      |]

-- | A stored label the payload declares, and NULL for anything else.
declaredKindSQL :: [Text] -> Text
declaredKindSQL [] = "NULL::text"
declaredKindSQL kinds =
  let literals = T.intercalate ", " (map textLiteral kinds)
   in [text|CASE WHEN kind IN (${literals}) THEN kind END|]

-- | Every queue's stats in one query, tagged by name. Caller guards the empty list.
allQueueStatsSQL :: RowCodec a -> SchemaName -> [(TableName, [Text])] -> Query a
allQueueStatsSQL codec schema queueKinds =
  let queuesTbl = arbiterQueuesTable schema
      workersTbl = arbiterWorkersTable schema
   in rawRows codec $ unionAllOverQueueTables schema (map fst queueKinds) $ \tableName _ ->
        let stats = queueStatsSelect schema tableName (fromMaybe [] (lookup tableName queueKinds))
         in [text|
          SELECT '${tableName}' AS queue, stats.*,
                 COALESCE((SELECT paused FROM ${queuesTbl} WHERE queue_name = '${tableName}'), FALSE) AS queue_paused,
                 worker_counts.workers_live, worker_counts.workers_paused
          FROM (${stats}) stats
          CROSS JOIN (
            -- Live matches the worker health CASE: a fresh heartbeat and not draining.
            SELECT COUNT(*)::int8 AS workers_live, COUNT(*) FILTER (WHERE paused)::int8 AS workers_paused
            FROM ${workersTbl}
            WHERE queue_name = '${tableName}'
              AND last_heartbeat >= NOW() - stale_threshold_secs * interval '1 second'
              AND NOT shutting_down
          ) worker_counts
        |]

-- ---------------------------------------------------------------------------
-- Parent-Child Operations
-- ---------------------------------------------------------------------------

-- | Child counts as @(parent_id, total, suspended)@ per parent, over a set of job ids.
-- The caller attaches the row decoder.
countChildrenBatchSQL :: Text -> Text -> [Int64] -> Query ()
countChildrenBatchSQL schema tableName jobIds =
  let tbl = jobQueueTable schema tableName
   in [sql|
        SELECT parent_id, COUNT(*),
               COUNT(*) FILTER (WHERE suspended)
        FROM ${tbl}
        WHERE parent_id = ANY(#{jobIds :: [CInt8]})
        GROUP BY parent_id
      |]

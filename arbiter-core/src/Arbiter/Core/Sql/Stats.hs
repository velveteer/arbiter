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

import Arbiter.Core.Codec (RowCodec, codecColumns)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.Queues (arbiterQueuesTable)
import Arbiter.Core.Sql.Jobs (jobStatusCaseSQL, unionAllOverQueueTables)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rawRows, rows)
import Arbiter.Core.SqlLiterals (textLiteral)
import Arbiter.Core.Worker (arbiterWorkersTable)

-- | Per-status queue counts plus the age of the oldest @ready@ and @in_flight@ job.
--
-- Counts are broken down by the canonical 'jobStatusCaseSQL' taxonomy so the
-- UI can distinguish actively-leased (@in_flight@) jobs from merely delayed
-- (@scheduled@/@backoff@/@throttled@) or @suspended@ ones. The status counts
-- sum to @total_jobs@. @oldest_ready_age_seconds@ measures only @ready@ rows so a
-- far-future scheduled job does not skew the queue's backlog latency.
-- @oldest_in_flight_age_seconds@ measures from the claim that leased each row, so a
-- an active handler continues to increase until it returns.
--
-- The select list is rendered from the decoder's own columns.
getQueueStatsSQL :: RowCodec a -> SchemaName -> TableName -> [Text] -> Query a
getQueueStatsSQL codec schema tableName kinds =
  let cols = codecColumns codec
      rollup = kindRollupSQL (jobQueueTable schema tableName) kinds cols
      selected = statsColumnList schema tableName cols
   in rows codec [sql|SELECT ${selected} FROM ${rollup} rollup|]

-- | The classified rows aggregated once per kind and once over the whole table, in
-- one pass. @total_row@ marks the whole-table row.
kindRollupSQL :: Text -> [Text] -> [Text] -> Text
kindRollupSQL tbl kinds cols =
  "(SELECT kind, GROUPING(kind) AS total_row, "
    <> T.intercalate ", " (map aggregate (filter rolledUp cols))
    <> " FROM (SELECT inserted_at, last_attempted_at, "
    <> declaredKindSQL kinds
    <> " AS kind, "
    <> jobStatusCaseSQL
    <> " AS status FROM "
    <> tbl
    <> ") classified GROUP BY GROUPING SETS ((), (kind)))"
  where
    rolledUp c = c /= "dlq_jobs" && c /= "kind_counts"
    aggregate "total_jobs" = "COUNT(*)::int8 AS total_jobs"
    aggregate "oldest_ready_age_seconds" = age "inserted_at" "ready" <> " AS oldest_ready_age_seconds"
    aggregate "oldest_in_flight_age_seconds" = age "last_attempted_at" "in_flight" <> " AS oldest_in_flight_age_seconds"
    aggregate c = "COUNT(*) FILTER (WHERE status = '" <> T.dropEnd (T.length "_jobs") c <> "') AS " <> c
    age column status =
      "EXTRACT(EPOCH FROM (clock_timestamp() - MIN(" <> column <> ") FILTER (WHERE status = '" <> status <> "')))::float8"

-- | A stored label the payload declares, and NULL for anything else.
declaredKindSQL :: [Text] -> Text
declaredKindSQL [] = "NULL::text"
declaredKindSQL kinds = "CASE WHEN kind IN (" <> T.intercalate ", " (map textLiteral kinds) <> ") THEN kind END"

-- | The stats select list: each decoder column read off the rollup rows.
statsColumnList :: SchemaName -> TableName -> [Text] -> Text
statsColumnList schema tableName = T.intercalate ", " . map render
  where
    dlqTbl = jobQueueDLQTable schema tableName
    render "dlq_jobs" = "(SELECT COUNT(*)::int8 FROM " <> dlqTbl <> ") AS dlq_jobs"
    render "kind_counts" = kindCounts <> " AS kind_counts"
    render c = "MAX(" <> c <> ") FILTER (WHERE total_row = 1) AS " <> c
    kindCounts = "jsonb_object_agg(kind, total_jobs) FILTER (WHERE total_row = 0 AND kind IS NOT NULL)"

-- | Every queue's stats in one query, tagged by name. Caller guards the empty list.
-- @statsCols@ are the per-queue stats columns, which sit inside the overview row.
allQueueStatsSQL :: RowCodec a -> [Text] -> SchemaName -> [(TableName, [Text])] -> Query a
allQueueStatsSQL codec statsCols schema queueKinds =
  let qTbl = arbiterQueuesTable schema
      wTbl = arbiterWorkersTable schema
      -- Live = heartbeat within the stale threshold and not draining, matching the worker health CASE.
      liveWorker = "last_heartbeat >= NOW() - stale_threshold_secs * interval '1 second' AND NOT shutting_down" :: Text
   in rawRows codec $ unionAllOverQueueTables schema (map fst queueKinds) $ \tableName tbl ->
        let selected = statsColumnList schema tableName statsCols
            rollup = kindRollupSQL tbl (fromMaybe [] (lookup tableName queueKinds)) statsCols
         in [text|
          SELECT '${tableName}' AS queue, s.*,
                 COALESCE((SELECT paused FROM ${qTbl} WHERE queue_name = '${tableName}'), FALSE) AS queue_paused,
                 w.workers_live, w.workers_paused
          FROM (SELECT ${selected} FROM ${rollup} rollup) s
          CROSS JOIN (
            SELECT COUNT(*)::int8 AS workers_live, COUNT(*) FILTER (WHERE paused)::int8 AS workers_paused
            FROM ${wTbl} WHERE queue_name = '${tableName}' AND ${liveWorker}
          ) w
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

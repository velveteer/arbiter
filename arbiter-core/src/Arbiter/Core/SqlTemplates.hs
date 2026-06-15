{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | SQL query templates with parameter markers
module Arbiter.Core.SqlTemplates
  ( -- * Job Queue Operations
    insertJobSQL
  , insertJobReplaceSQL
  , insertJobsBatchSQL
  , insertJobsBatchSQL_
  , claimJobsBatchedSQL
  , smartAckJobSQL
  , smartAckJobsBatchSQL
  , setVisibilityTimeoutSQL
  , setVisibilityTimeoutBatchSQL
  , updateJobForRetrySQL
  , moveToDLQSQL

    -- * Dead Letter Queue Operations
  , retryFromDLQSQL
  , dlqJobExistsSQL
  , deleteDLQJobSQL

    -- * Admin Operations
  , getJobByIdSQL
  , getJobByIdWithStatusSQL
  , getJobByDedupKeySQL
  , cancelJobSQL
  , promoteJobSQL
  , getQueueStatsSQL

    -- * Batch Operations
  , moveToDLQBatchSQL
  , deleteDLQJobsBatchSQL

    -- * Job Dependency Operations
  , pauseChildrenSQL
  , resumeChildrenSQL
  , cancelJobCascadeSQL
  , cancelJobTreeSQL
  , forceCancelJobSQL
  , tryWakeAncestorSQL
  , suspendJobSQL
  , resumeJobSQL
  , parentExistsSQL
  , getParentIdSQL

    -- * Parent-Child Operations
  , countChildrenBatchSQL
  , countDLQChildrenBatchSQL
  , cascadeChildrenToDLQSQL
  , descendantRollupIdsSQL

    -- * Results Table Operations
  , insertResultSQL
  , getResultsByParentSQL
  , getDLQChildErrorsByParentSQL
  , persistParentStateSQL
  , getParentStateSnapshotSQL
  , readChildResultsSQL

    -- * Groups Table Operations
  , lockGroupsSQL
  , refreshGroupsSQL

    -- * Filtered Query Operations
  , JobFilter (..)
  , JobSortColumn (..)
  , DLQSortColumn (..)
  , SortDir (..)
  , jobStatusCaseSQL
  , listJobsFilteredSQL
  , countJobsFilteredSQL
  , listJobsWithStatusSQL
  , listDLQFilteredSQL
  , countDLQFilteredSQL
  , buildJobsOrderBy
  , buildDLQOrderBy
  , jobSortColumnName
  , dlqSortColumnName
  , sortDirSql

    -- * Cron Schedule Operations
  , upsertCronDefaultSQL
  , listCronSchedulesSQL
  , getCronScheduleByNameSQL
  , touchCronLastFiredSQL
  , touchCronCheckedSQL
  , tryFireCronGateSQL
  , tryAcquireCronLeaderSQL

    -- * Worker Registry Operations
  , upsertWorkerSQL
  , heartbeatWorkerSQL
  , setWorkerPausedSQL
  , markWorkerShuttingDownSQL
  , deleteWorkerSQL
  , listWorkersSQL
  , deleteStaleWorkersSQL
  , workerColumnList

    -- * Queue Operations
  , ensureQueueSQL
  , setQueuePausedSQL
  , getQueueSQL
  , listQueuesSQL
  , queueColumnList

    -- * Global Gate Operations
  , ensureGateRowSQL
  , checkGateSQL
  , tryClaimGateSQL
  , bumpGateSQL
  ) where

import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Data.UUID.Types (UUID)
import Data.UUID.Types qualified as UUID
import NeatInterpolation (text)

import Arbiter.Core.Codec
  ( codecColumns
  , cronScheduleRowCodec
  , dlqRowCodec
  , jobRowCodec
  , queueRowCodec
  , workerRowCodec
  )
import Arbiter.Core.CronSchedule (cronSchedulesTable)
import Arbiter.Core.Gates (arbiterGatesTable)
import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , cancelNotifyChannel
  , jobQueueDLQTable
  , jobQueueGroupsTable
  , jobQueueResultsTable
  , jobQueueTable
  , pauseNotifyChannelPrefix
  )
import Arbiter.Core.Job.Types (JobStatus)
import Arbiter.Core.Queues (arbiterQueuesTable)
import Arbiter.Core.Worker (arbiterWorkersTable)

-- ---------------------------------------------------------------------------
-- Filtered Query Operations
-- ---------------------------------------------------------------------------

-- | Filter predicates for job listing queries.
data JobFilter
  = FilterGroupKey Text
  | FilterParentId Int64
  | FilterRootsOnly
  | FilterStatus JobStatus
  deriving stock (Eq, Show)

-- | Sortable columns on the main jobs table. Closed enum so the SQL builder
-- only ever sees a value that round-trips with 'jobSortColumnName'.
data JobSortColumn
  = JsId
  | JsPriority
  | JsAttempts
  | JsInsertedAt
  | JsNotVisibleUntil
  | JsGroupKey
  | JsParentId
  | JsLastAttemptedAt
  deriving stock (Bounded, Enum, Eq, Show)

-- | Underlying SQL column name for a 'JobSortColumn'.
jobSortColumnName :: JobSortColumn -> Text
jobSortColumnName = \case
  JsId -> "id"
  JsPriority -> "priority"
  JsAttempts -> "attempts"
  JsInsertedAt -> "inserted_at"
  JsNotVisibleUntil -> "not_visible_until"
  JsGroupKey -> "group_key"
  JsParentId -> "parent_id"
  JsLastAttemptedAt -> "last_attempted_at"

-- | Sortable columns on the DLQ table. @DlqId@ is the DLQ primary key
-- (distinct from the original job id, which is @DlqJobId@).
data DLQSortColumn
  = DlqId
  | DlqFailedAt
  | DlqJobId
  | DlqPriority
  | DlqAttempts
  | DlqInsertedAt
  | DlqGroupKey
  | DlqParentId
  | DlqLastAttemptedAt
  deriving stock (Bounded, Enum, Eq, Show)

dlqSortColumnName :: DLQSortColumn -> Text
dlqSortColumnName = \case
  DlqId -> "id"
  DlqFailedAt -> "failed_at"
  DlqJobId -> "job_id"
  DlqPriority -> "priority"
  DlqAttempts -> "attempts"
  DlqInsertedAt -> "inserted_at"
  DlqGroupKey -> "group_key"
  DlqParentId -> "parent_id"
  DlqLastAttemptedAt -> "last_attempted_at"

-- | Sort direction.
data SortDir = SortAsc | SortDesc
  deriving stock (Bounded, Enum, Eq, Show)

sortDirSql :: SortDir -> Text
sortDirSql SortAsc = "ASC"
sortDirSql SortDesc = "DESC"

-- | Sole definition of the derived job status. Its string values must match 'jobStatusToText'.
jobStatusCaseSQL :: Text
jobStatusCaseSQL =
  [text|
    CASE
      WHEN suspended THEN 'suspended'
      WHEN claimed_by IS NULL AND last_error IS NOT NULL AND not_visible_until IS NOT NULL AND not_visible_until > NOW() THEN 'backoff'
      WHEN attempts > 0 AND not_visible_until IS NOT NULL AND not_visible_until > NOW() THEN 'in_flight'
      WHEN not_visible_until IS NOT NULL AND not_visible_until > NOW() THEN 'scheduled'
      ELSE 'ready'
    END
  |]

-- | All job columns plus the derived @status@ column, aliased @j@, for filtering.
jobsWithStatusSubquery :: Text -> Text -> Text
jobsWithStatusSubquery schema tableName =
  let tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
      statusCase = jobStatusCaseSQL
   in [text|(SELECT ${columns}, ${statusCase} AS status FROM ${tbl}) j|]

-- | List filtered jobs without the derived status, for callers that don't need it. Params: limit, offset.
listJobsFilteredSQL :: Text -> Text -> Text -> Text -> Text
listJobsFilteredSQL schema tableName whereClause orderBy =
  let tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in [text|
        SELECT ${columns}
        FROM ${tbl}
        ${whereClause}
        ORDER BY ${orderBy} LIMIT ? OFFSET ?
      |]

-- | List filtered jobs with @status@ as a trailing column. Params: limit, offset.
listJobsWithStatusSQL :: Text -> Text -> Text -> Text -> Text
listJobsWithStatusSQL schema tableName whereClause orderBy =
  let sub = jobsWithStatusSubquery schema tableName
   in [text|
        SELECT * FROM ${sub}
        ${whereClause}
        ORDER BY ${orderBy} LIMIT ? OFFSET ?
      |]

-- | Count filtered jobs, through the status subquery so status filters work.
countJobsFilteredSQL :: Text -> Text -> Text -> Text
countJobsFilteredSQL schema tableName whereClause =
  let sub = jobsWithStatusSubquery schema tableName
   in [text|SELECT COUNT(*) FROM ${sub} ${whereClause}|]

-- | Fetch a single job by id with its derived @status@ trailing column. Param: id.
getJobByIdWithStatusSQL :: Text -> Text -> Text
getJobByIdWithStatusSQL schema tableName =
  let sub = jobsWithStatusSubquery schema tableName
   in [text|SELECT * FROM ${sub} WHERE id = ?|]

-- | Generic SQL for listing DLQ jobs with dynamic WHERE and ORDER BY clauses.
--
-- @orderBy@ must be produced by 'buildDLQOrderBy'. Caller params (appended
-- after filter params): limit, offset.
listDLQFilteredSQL :: Text -> Text -> Text -> Text -> Text
listDLQFilteredSQL schema tableName whereClause orderBy =
  let dlqTbl = jobQueueDLQTable schema tableName
      columns = T.intercalate ", " allDLQColumns
   in [text|
        SELECT ${columns}
        FROM ${dlqTbl}
        ${whereClause}
        ORDER BY ${orderBy}
        LIMIT ? OFFSET ?
      |]

-- | How NULLs in a sort column should order relative to non-NULL values.
data NullsBehavior
  = -- | Column is NOT NULL; emit no NULLS clause.
    NullsNotApplicable
  | -- | NULL is unmodelled / "absent"; always sort last.
    NullsAsAbsent
  | -- | NULL semantically represents "minimum" for the column's domain
    -- (e.g. @not_visible_until IS NULL@ means visible now, @last_attempted_at IS NULL@
    -- means never attempted = earliest). NULLS FIRST when ASC, NULLS LAST when DESC.
    NullsAsMinimum

nullsClause :: NullsBehavior -> SortDir -> Text
nullsClause NullsNotApplicable _ = ""
nullsClause NullsAsAbsent _ = " NULLS LAST"
nullsClause NullsAsMinimum SortAsc = " NULLS FIRST"
nullsClause NullsAsMinimum SortDesc = " NULLS LAST"

jobColumnNulls :: JobSortColumn -> NullsBehavior
jobColumnNulls = \case
  JsId -> NullsNotApplicable
  JsPriority -> NullsNotApplicable
  JsAttempts -> NullsNotApplicable
  JsInsertedAt -> NullsNotApplicable
  JsNotVisibleUntil -> NullsAsMinimum
  JsGroupKey -> NullsAsAbsent
  JsParentId -> NullsAsAbsent
  JsLastAttemptedAt -> NullsAsMinimum

dlqColumnNulls :: DLQSortColumn -> NullsBehavior
dlqColumnNulls = \case
  DlqId -> NullsNotApplicable
  DlqFailedAt -> NullsNotApplicable
  DlqJobId -> NullsNotApplicable
  DlqInsertedAt -> NullsNotApplicable
  DlqPriority -> NullsNotApplicable
  DlqAttempts -> NullsNotApplicable
  DlqGroupKey -> NullsAsAbsent
  DlqParentId -> NullsAsAbsent
  DlqLastAttemptedAt -> NullsAsMinimum

-- | Build the ORDER BY clause for the jobs table from a typed sort spec.
-- Defaults are @id DESC@. NULL placement is column-specific (see
-- 'jobColumnNulls'). A stable @id@ tie-breaker in the same direction as the
-- primary sort is appended when sorting by anything other than @id@.
buildJobsOrderBy :: Maybe JobSortColumn -> Maybe SortDir -> Text
buildJobsOrderBy mCol mDir =
  let col = fromMaybe JsId mCol
      dir = fromMaybe SortDesc mDir
      dirText = sortDirSql dir
      primaryNulls = nullsClause (jobColumnNulls col) dir
      tieBreaker = if col == JsId then "" else ", id " <> dirText
   in jobSortColumnName col <> " " <> dirText <> primaryNulls <> tieBreaker

-- | Build the ORDER BY clause for the DLQ table. Defaults are
-- @failed_at DESC@. NULL semantics as in 'buildJobsOrderBy' but using
-- 'dlqColumnNulls'.
buildDLQOrderBy :: Maybe DLQSortColumn -> Maybe SortDir -> Text
buildDLQOrderBy mCol mDir =
  let col = fromMaybe DlqFailedAt mCol
      dir = fromMaybe SortDesc mDir
      dirText = sortDirSql dir
      primaryNulls = nullsClause (dlqColumnNulls col) dir
      tieBreaker = if col == DlqId then "" else ", id " <> dirText
   in dlqSortColumnName col <> " " <> dirText <> primaryNulls <> tieBreaker

-- | Generic SQL for counting DLQ jobs with dynamic WHERE clause.
countDLQFilteredSQL :: Text -> Text -> Text -> Text
countDLQFilteredSQL schema tableName whereClause =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|SELECT COUNT(*) FROM ${dlqTbl} ${whereClause}|]

allJobColumns :: [Text]
allJobColumns = codecColumns (jobRowCodec "")

-- | DLQ columns: DLQ-specific fields + all Job fields (with job_id instead of id).
-- @drop 1 allJobColumns@ drops the @id@ column, replaced by @job_id@ in the DLQ table.
allDLQColumns :: [Text]
allDLQColumns = codecColumns (dlqRowCodec "")

-- | All job columns except @id@ and @last_error@, comma-separated.
-- Used for DLQ INSERT operations where @id@ becomes @job_id@ and @last_error@ is overridden.
jobColsExceptError :: Text
jobColsExceptError = T.intercalate ", " $ filter (/= "last_error") (drop 1 allJobColumns)

-- | Standard job column list (for SELECT and RETURNING)
jobColumns :: Maybe Text -> Text
jobColumns mAlias = T.intercalate ", " $ map withAlias allJobColumns
  where
    withAlias name = maybe name (\alias -> alias <> "." <> name) mAlias

-- | SQL template for inserting a job
--
-- Parameters (in order):
--   payload, group_key, attempts, last_error, priority, dedup_key, dedup_strategy,
--   max_attempts, parent_id, parent_state, suspended, not_visible_until
--
-- Returns: The inserted job row
insertJobSQL :: Text -> Text -> Text
insertJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in [text|
        INSERT INTO ${tbl} (payload, group_key, attempts, last_error, priority, dedup_key, dedup_strategy, max_attempts, parent_id, parent_state, suspended, not_visible_until)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
        RETURNING ${columns}
      |]

-- | SQL template for replace deduplication strategy
--
-- Replaces existing job unless actively in-flight or has children (in either
-- the main queue or the DLQ). A parent with no children yet can be replaced.
--
-- The groups table is maintained by triggers on the main job table.
-- @ON CONFLICT DO UPDATE@ fires the UPDATE trigger, whose transition tables
-- contain the old and new rows -- handling cross-group moves automatically.
--
-- Parameters: same 12 as insertJobSQL
insertJobReplaceSQL :: Text -> Text -> Text
insertJobReplaceSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      columns = jobColumns Nothing
   in [text|
        INSERT INTO ${tbl} (payload, group_key, attempts, last_error, priority, dedup_key, dedup_strategy, max_attempts, parent_id, parent_state, suspended, not_visible_until)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO UPDATE SET
          payload = EXCLUDED.payload,
          group_key = EXCLUDED.group_key,
          attempts = 0,
          last_error = NULL,
          priority = EXCLUDED.priority,
          dedup_strategy = EXCLUDED.dedup_strategy,
          max_attempts = EXCLUDED.max_attempts,
          updated_at = NOW(),
          parent_id = EXCLUDED.parent_id,
          parent_state = EXCLUDED.parent_state,
          suspended = EXCLUDED.suspended,
          not_visible_until = EXCLUDED.not_visible_until,
          last_attempted_at = NULL
        WHERE (${tbl}.attempts = 0
          OR ${tbl}.not_visible_until IS NULL
          OR ${tbl}.not_visible_until <= NOW()
          OR ${tbl}.last_error IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ${tbl}.id)
          AND NOT EXISTS (SELECT 1 FROM ${dlqTbl} d WHERE d.parent_id = ${tbl}.id)
        RETURNING ${columns}
      |]

-- | SQL template for batch inserting jobs using array parameters
--
-- Uses unnest to expand parallel arrays into rows. Supports dedup keys:
-- jobs with @IgnoreDuplicate@ are silently skipped on conflict, jobs with
-- @ReplaceDuplicate@ update the existing row (unless actively in-flight).
--
-- Parameters (10 arrays):
--   1. payloads           - jsonb[]
--   2. group_keys         - text[] (with NULLs)
--   3. priorities         - int[]
--   4. dedup_keys         - text[] (with NULLs)
--   5. dedup_strategies   - text[] (with NULLs)
--   6. max_attempts       - int[] (with NULLs)
--   7. parent_ids         - bigint[] (with NULLs)
--   8. parent_states      - jsonb[] (with NULLs)
--   9. suspended          - boolean[]
--  10. not_visible_untils - timestamptz[] (with NULLs)
insertJobsBatchSQL :: Text -> Text -> Text
insertJobsBatchSQL schema tableName =
  let columns = jobColumns Nothing
      returning = "RETURNING " <> columns
   in insertJobsBatchBase schema tableName returning

insertJobsBatchSQL_ :: Text -> Text -> Text
insertJobsBatchSQL_ schema tableName =
  insertJobsBatchBase schema tableName ""

insertJobsBatchBase :: Text -> Text -> Text -> Text
insertJobsBatchBase schema tableName returning =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
   in [text|
        INSERT INTO ${tbl} (payload, group_key, attempts, last_error, priority, dedup_key, dedup_strategy, max_attempts, parent_id, parent_state, suspended, not_visible_until)
        SELECT
          payload, group_key, 0, NULL, priority, dedup_key, dedup_strategy, max_attempts, parent_id,
          parent_state, suspended, not_visible_until
        FROM (
          SELECT
            unnest(?::jsonb[]) AS payload,
            unnest(?::text[]) AS group_key,
            unnest(?::int[]) AS priority,
            unnest(?::text[]) AS dedup_key,
            unnest(?::text[]) AS dedup_strategy,
            unnest(?::int[]) AS max_attempts,
            unnest(?::bigint[]) AS parent_id,
            unnest(?::jsonb[]) AS parent_state,
            unnest(?::boolean[]) AS suspended,
            unnest(?::timestamptz[]) AS not_visible_until
        ) src
        WHERE (src.parent_id IS NULL
            OR EXISTS (SELECT 1 FROM ${tbl} p WHERE p.id = src.parent_id))
        ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO UPDATE SET
          payload = EXCLUDED.payload,
          group_key = EXCLUDED.group_key,
          attempts = 0,
          last_error = NULL,
          priority = EXCLUDED.priority,
          dedup_strategy = EXCLUDED.dedup_strategy,
          max_attempts = EXCLUDED.max_attempts,
          updated_at = NOW(),
          parent_id = EXCLUDED.parent_id,
          parent_state = EXCLUDED.parent_state,
          suspended = EXCLUDED.suspended,
          not_visible_until = EXCLUDED.not_visible_until,
          last_attempted_at = NULL
        WHERE EXCLUDED.dedup_strategy = 'replace'
          AND (${tbl}.attempts = 0
            OR ${tbl}.not_visible_until IS NULL
            OR ${tbl}.not_visible_until <= NOW()
            OR ${tbl}.last_error IS NOT NULL)
          AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ${tbl}.id)
          AND NOT EXISTS (SELECT 1 FROM ${dlqTbl} d WHERE d.parent_id = ${tbl}.id)
        ${returning}
      |]

-- | Batched single-CTE claim. Also serves the single-job claim at batch size 1.

--- Claims any unsuspended visible job, including rollup children and woken
--- rollup parents.
claimJobsBatchedSQL :: SchemaName -> TableName -> Int -> Int -> NominalDiffTime -> Maybe UUID -> Text
claimJobsBatchedSQL schema tableName batchSize maxBatches timeoutSeconds mWorkerId =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      columns = jobColumns Nothing
      bs = T.pack (show batchSize)
      mb = T.pack (show maxBatches)
      timeout = T.pack (show (realToFrac timeoutSeconds :: Double))
      ungroupedLimit = T.pack (show (maxBatches * batchSize))
      overfetch = T.pack (show (maxBatches * 10))
      claimedBy = uuidLiteral mWorkerId
   in [text|
  WITH
    group_candidates AS (
      (
        SELECT group_key FROM ${groupsTbl}
        WHERE ready_count > 0 AND in_flight_until IS NULL
        ORDER BY min_priority ASC, min_id ASC
        LIMIT ${overfetch}
      )
      UNION
      (
        SELECT group_key FROM ${groupsTbl}
        WHERE next_due <= NOW()
        ORDER BY next_due ASC
        LIMIT ${overfetch}
      )
    ),
    eligible_groups AS (
      SELECT g.group_key FROM ${groupsTbl} g
      WHERE g.group_key IN (SELECT group_key FROM group_candidates)
        AND g.job_count > 0
        AND (g.in_flight_until IS NULL OR g.in_flight_until <= NOW())
      ORDER BY g.min_priority ASC, g.min_id ASC
      LIMIT ${overfetch}
      FOR UPDATE SKIP LOCKED
    ),
    eligible_heads AS (
      SELECT el.group_key, h.min_priority, h.min_id
      FROM eligible_groups el
      CROSS JOIN LATERAL (
        SELECT t.priority AS min_priority, t.id AS min_id
        FROM ${tbl} t
        WHERE t.group_key = el.group_key
          AND NOT t.suspended
          AND (t.not_visible_until IS NULL OR t.not_visible_until <= NOW())
        ORDER BY t.priority ASC, t.id ASC
        LIMIT 1
      ) h
    ),
    ungrouped_pool AS (
      (
        SELECT id, priority
        FROM ${tbl}
        WHERE group_key IS NULL
          AND NOT suspended
          AND not_visible_until IS NULL
        ORDER BY priority ASC, id ASC
        LIMIT ${ungroupedLimit}
      )
      UNION ALL
      (
        SELECT id, priority
        FROM ${tbl}
        WHERE group_key IS NULL
          AND NOT suspended
          AND not_visible_until IS NOT NULL
          AND not_visible_until <= NOW()
        ORDER BY not_visible_until ASC
        LIMIT ${ungroupedLimit}
      )
    ),
    ungrouped_numbered AS (
      SELECT id, priority,
        ((ROW_NUMBER() OVER (ORDER BY priority ASC, id ASC) - 1)
          / ${bs}) + 1 AS batch_num
      FROM ungrouped_pool
      ORDER BY priority ASC, id ASC
      LIMIT ${ungroupedLimit}
    ),
    ungrouped_batch_info AS (
      SELECT batch_num, MIN(priority) AS min_priority, MIN(id) AS min_id
      FROM ungrouped_numbered
      GROUP BY batch_num
    ),
    allocated_slots AS (
      SELECT s.group_key, s.ungrouped_batch
      FROM (
        SELECT group_key, NULL::bigint AS ungrouped_batch, min_priority, min_id
        FROM eligible_heads
        UNION ALL
        SELECT NULL::text, batch_num, min_priority, min_id
        FROM ungrouped_batch_info
        ORDER BY min_priority ASC, min_id ASC
      ) s
      LIMIT ${mb}
    ),
    final_locked_groups AS (
      SELECT group_key FROM allocated_slots WHERE group_key IS NOT NULL
    ),
    grouped_candidates AS (
      SELECT j.id, flg.group_key AS expected_group
      FROM final_locked_groups flg
      CROSS JOIN LATERAL (
        SELECT id
        FROM ${tbl}
        WHERE group_key = flg.group_key
          AND NOT suspended
          AND (not_visible_until IS NULL OR not_visible_until <= NOW())
        ORDER BY attempts DESC, priority ASC, id ASC
        LIMIT ${bs}
      ) j
    ),
    ungrouped_candidates AS (
      SELECT id, NULL::text AS expected_group
      FROM ungrouped_numbered
      WHERE batch_num IN (
        SELECT ungrouped_batch
        FROM allocated_slots
        WHERE ungrouped_batch IS NOT NULL
      )
    ),
    locked AS (
      SELECT j.id
      FROM (
        SELECT id, expected_group FROM grouped_candidates
        UNION ALL
        SELECT id, expected_group FROM ungrouped_candidates
      ) i
      INNER JOIN ${tbl} j ON j.id = i.id
      WHERE NOT j.suspended
        AND (j.not_visible_until IS NULL OR j.not_visible_until <= NOW())
        AND j.group_key IS NOT DISTINCT FROM i.expected_group
      ORDER BY j.priority ASC, j.id ASC
      FOR UPDATE OF j SKIP LOCKED
      LIMIT ${ungroupedLimit}
    ),
    claimed AS (
      UPDATE ${tbl} j
      SET not_visible_until = NOW() + (${timeout} * interval '1 second'),
          attempts = j.attempts + 1,
          last_attempted_at = NOW(),
          updated_at = NOW(),
          claimed_by = ${claimedBy}
      FROM locked l
      WHERE j.id = l.id
      RETURNING j.*
    )
    SELECT ${columns} FROM claimed ORDER BY priority ASC, id ASC
    |]

-- | Smart ack CTE for job dependencies.
--
-- 1. ack: DELETE the job only if it has no children. Returns deleted row.
-- 2. suspend: If ack returned nothing AND children exist, suspend the job
--    (it becomes a finalizer waiting for children to complete).
-- 3. wake_parent: If ack deleted a child whose parent is suspended with no
--    remaining siblings in the queue, resume the parent for its
--    completion round.
--
-- Returns @rows_affected@ (1 on success, 0 if stolen/gone).
-- Parameters: job_id, attempts, job_id, job_id, attempts, job_id
smartAckJobSQL :: Text -> Text -> Text
smartAckJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH ack AS (
          DELETE FROM ${tbl}
          WHERE id = ? AND attempts = ?
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = ?)
          RETURNING id, parent_id
        ),
        suspend AS (
          UPDATE ${tbl}
          SET suspended = TRUE, not_visible_until = NULL, updated_at = NOW()
          WHERE id = ? AND attempts = ?
            AND NOT EXISTS (SELECT 1 FROM ack)
            AND EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = ?)
          RETURNING id
        ),
        wake_parent AS (
          UPDATE ${tbl}
          SET suspended = FALSE, updated_at = NOW()
          WHERE id = (SELECT parent_id FROM ack WHERE parent_id IS NOT NULL)
            AND suspended = TRUE
            AND NOT EXISTS (
              SELECT 1 FROM ${tbl} c
              WHERE c.parent_id = (SELECT parent_id FROM ack WHERE parent_id IS NOT NULL)
                AND c.id NOT IN (SELECT id FROM ack)
            )
          RETURNING id
        )
        SELECT
          (SELECT count(*) FROM ack) + (SELECT count(*) FROM suspend) AS result
      |]

-- | Set-based smart ack over @unnest@ed @(id, attempts)@ arrays: deletes leaves,
-- suspends finalizers that still have children, and wakes parents whose last
-- child completed. The wake check excludes acked children explicitly, since a
-- sibling CTE's deletes are not visible within the same statement. Returns the
-- acked ids. Reclaimed jobs (attempts no longer match) are absent. The caller
-- holds the parent locks.
smartAckJobsBatchSQL :: Text -> Text -> Text
smartAckJobsBatchSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH input AS (
          SELECT unnest(?::bigint[]) AS id, unnest(?::int[]) AS att
        ),
        ack AS (
          DELETE FROM ${tbl} j
          USING input i
          WHERE j.id = i.id AND j.attempts = i.att
            AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = j.id)
          RETURNING j.id, j.parent_id
        ),
        suspend AS (
          UPDATE ${tbl} j
          SET suspended = TRUE, not_visible_until = NULL, updated_at = NOW()
          FROM input i
          WHERE j.id = i.id AND j.attempts = i.att
            AND NOT EXISTS (SELECT 1 FROM ack a WHERE a.id = j.id)
            AND EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = j.id)
          RETURNING j.id
        ),
        wake_parent AS (
          UPDATE ${tbl} p
          SET suspended = FALSE, updated_at = NOW()
          WHERE p.id IN (SELECT DISTINCT parent_id FROM ack WHERE parent_id IS NOT NULL)
            AND p.suspended = TRUE
            AND NOT EXISTS (
              SELECT 1 FROM ${tbl} c
              WHERE c.parent_id = p.id
                AND NOT EXISTS (SELECT 1 FROM ack a WHERE a.id = c.id)
            )
          RETURNING p.id
        )
        SELECT id FROM ack
        UNION
        SELECT id FROM suspend
      |]

-- | SQL template for setting visibility timeout
--
-- Parameters: timeout, job_id, attempts
--
-- Uses optimistic locking (attempts check) to prevent race conditions when
-- another worker has reclaimed the job after visibility timeout expired.
setVisibilityTimeoutSQL :: Text -> Text -> Text
setVisibilityTimeoutSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET not_visible_until = CASE WHEN ?::double precision <= 0 THEN NULL ELSE NOW() + (?::double precision * interval '1 second') END,
            updated_at = NOW()
        WHERE id = ? AND attempts = ?
      |]

-- | Atomically updates the visibility timeout for a batch of jobs and returns
-- the detailed status of each job in a single query.
--
-- This is used for heartbeating. The query attempts to update all jobs, and
-- then reports on which ones succeeded, which were missing (acked), and which
-- had a different attempts count (stolen).
setVisibilityTimeoutBatchSQL :: Text -> Text -> Text -> Text
setVisibilityTimeoutBatchSQL schema tableName valuesPlaceholder =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH input_jobs AS (
          SELECT v.id::bigint AS id, v.expected_attempts::int AS expected_attempts
          FROM (VALUES ${valuesPlaceholder}) AS v(id, expected_attempts)
        ),
        updated AS (
          UPDATE ${tbl} j
          SET not_visible_until = CASE WHEN ?::double precision <= 0 THEN NULL ELSE NOW() + (?::double precision * interval '1 second') END,
              updated_at = NOW()
          FROM input_jobs ij
          WHERE j.id = ij.id AND j.attempts = ij.expected_attempts
          RETURNING j.id
        )
        SELECT
          ij.id,
          (u.id IS NOT NULL) as was_heartbeated,
          j.attempts as current_db_attempts
        FROM input_jobs ij
        LEFT JOIN updated u ON ij.id = u.id
        LEFT JOIN ${tbl} j ON j.id = ij.id
      |]

-- | SQL template for updating job for retry
--
-- Parameters: backoff, error, job_id, attempts
--
-- Uses optimistic locking (attempts check) to prevent race conditions when
-- a job's visibility timeout expires and another worker claims it before
-- the retry update completes.
updateJobForRetrySQL :: Text -> Text -> Text
updateJobForRetrySQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET not_visible_until = NOW() + (? * interval '1 second'),
            last_error = ?,
            updated_at = NOW(),
            claimed_by = NULL
        WHERE id = ? AND attempts = ? AND NOT suspended
      |]

-- | Promote a delayed or retrying job to be immediately visible.
--
-- Refuses in-flight jobs (attempts > 0 with no last_error).
-- Returns 0 if job doesn't exist, is already visible, or is in-flight.
promoteJobSQL :: Text -> Text -> Text
promoteJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET not_visible_until = NULL,
            updated_at = NOW()
        WHERE id = ?
          AND not_visible_until IS NOT NULL
          AND not_visible_until > NOW()
          AND (attempts = 0 OR last_error IS NOT NULL)
      |]

-- | SQL template for moving job to DLQ atomically
--
-- This preserves ALL job fields (complete snapshot) plus DLQ metadata.
-- The operation is atomic: the job is deleted from the main queue and
-- inserted into the DLQ in a single statement. The final error message
-- is passed as a parameter to capture the error that caused the DLQ move.
--
-- Parameters: job_id, attempts, last_error
moveToDLQSQL :: Text -> Text -> Text
moveToDLQSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      cols = jobColsExceptError
   in [text|
        WITH deleted_job AS (
          DELETE FROM ${tbl}
          WHERE id = ? AND attempts = ?
          RETURNING *
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (
            job_id, ${cols}, last_error
          )
          SELECT
            id, ${cols}, ?
          FROM deleted_job
        )
        SELECT count(*) FROM deleted_job
      |]

-- | SQL template for retrying a job from DLQ (tree-aware)
--
-- Tree-aware retry behavior - retrying any member of a DLQ'd tree recovers
-- the entire tree in a single operation:
--
-- 1. If the target is a child whose parent is in the DLQ (not in main queue),
--    the parent is auto-retried as @suspended = TRUE@, and ALL DLQ'd siblings
--    are auto-retried too. The parent waits for children to complete.
--
-- 2. If the target is a rollup finalizer with DLQ'd children, ALL children
--    are auto-retried and the finalizer comes back as @suspended = TRUE@
--    (waits for children to complete). If no DLQ'd children exist, it comes
--    back as @suspended = FALSE@ (runs immediately with snapshot data).
--
-- 3. Refuses to retry if the tree root's parent_id references a job that no
--    longer exists in the main queue - prevents creating orphaned children.
--
-- Retried rollup finalizers get @suspended = TRUE@ when they have children
-- being retried alongside them. @dedup_key@ and @dedup_strategy@ are
-- intentionally dropped on retry (columns omitted → NULL defaults).
--
-- Parameters: id (the DLQ primary key)
retryFromDLQSQL :: Text -> Text -> Text
retryFromDLQSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
      tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in [text|
        WITH RECURSIVE
        target AS (
          SELECT * FROM ${dlqTbl} WHERE id = ?
        ),
        -- Walk up through DLQ ancestors to find the root of the tree.
        -- Stops when parent_id IS NULL, parent is in main queue, or
        -- parent is not found in DLQ (orphaned).
        ancestors AS (
          SELECT d.job_id, d.parent_id, 0 AS depth
          FROM ${dlqTbl} d
          WHERE d.job_id = (SELECT parent_id FROM target)
            AND (SELECT parent_id FROM target) IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE id = (SELECT parent_id FROM target))
          UNION ALL
          SELECT d.job_id, d.parent_id, a.depth + 1
          FROM ${dlqTbl} d
          JOIN ancestors a ON d.job_id = a.parent_id
          WHERE a.parent_id IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE id = a.parent_id)
        ),
        -- Root is the topmost DLQ ancestor, or the target itself
        root_job_id AS (
          SELECT COALESCE(
            (SELECT job_id FROM ancestors ORDER BY depth DESC LIMIT 1),
            (SELECT job_id FROM target)
          ) AS job_id
        ),
        -- Guard: root's parent must be NULL or exist in main queue
        can_retry AS (
          SELECT EXISTS (
            SELECT 1
            FROM root_job_id r
            JOIN ${dlqTbl} d ON d.job_id = r.job_id
            WHERE d.parent_id IS NULL
               OR EXISTS (SELECT 1 FROM ${tbl} WHERE id = d.parent_id)
          ) AS val
        ),
        -- Walk down from root to collect all DLQ tree members
        tree AS (
          SELECT d.id AS dlq_id, d.job_id, d.payload, d.group_key, d.priority,
                 d.max_attempts, d.parent_id, d.parent_state
          FROM ${dlqTbl} d
          WHERE d.job_id = (SELECT job_id FROM root_job_id)
          UNION ALL
          SELECT d.id AS dlq_id, d.job_id, d.payload, d.group_key, d.priority,
                 d.max_attempts, d.parent_id, d.parent_state
          FROM ${dlqTbl} d
          JOIN tree t ON d.parent_id = t.job_id
        ),
        -- Delete all tree members from DLQ (guarded by can_retry)
        deleted AS (
          DELETE FROM ${dlqTbl}
          WHERE id IN (SELECT dlq_id FROM tree)
            AND (SELECT val FROM can_retry)
          RETURNING job_id, payload, group_key, priority, max_attempts, parent_id, parent_state
        ),
        -- Re-insert into main queue with computed suspended state:
        -- rollup finalizers are suspended if they have children (in this
        -- retry batch OR already in the main queue).
        inserted AS (
          INSERT INTO ${tbl} (id, payload, group_key, attempts, priority, max_attempts,
                              parent_id, parent_state, suspended)
          SELECT d.job_id, d.payload, d.group_key, 0, d.priority, d.max_attempts,
                 d.parent_id, d.parent_state,
                 CASE WHEN d.parent_state IS NOT NULL
                   THEN EXISTS (SELECT 1 FROM deleted c WHERE c.parent_id = d.job_id)
                     OR EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = d.job_id)
                   ELSE FALSE
                 END
          FROM deleted d
          RETURNING *
        ),
        -- Re-suspend any rollup parents already in the main queue that just
        -- got fresh children re-inserted under them.
        parent_resuspend AS (
          UPDATE ${tbl}
          SET suspended = TRUE, updated_at = NOW()
          WHERE parent_state IS NOT NULL
            AND id IN (SELECT DISTINCT parent_id FROM inserted WHERE parent_id IS NOT NULL)
            AND NOT suspended
            AND NOT (attempts > 0 AND not_visible_until IS NOT NULL AND not_visible_until > NOW())
        )
        SELECT ${columns} FROM inserted WHERE id = (SELECT job_id FROM target)
      |]

-- | Check whether a DLQ job exists by ID.
--
-- Parameters: dlq_id
dlqJobExistsSQL :: Text -> Text -> Text
dlqJobExistsSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|SELECT EXISTS (SELECT 1 FROM ${dlqTbl} WHERE id = ?) AS result|]

-- | SQL template for deleting a DLQ job
--
-- Parameters: id (the DLQ primary key)
-- Returns: parent_id of the deleted job (NULL if no parent)
deleteDLQJobSQL :: Text -> Text -> Text
deleteDLQJobSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|DELETE FROM ${dlqTbl} WHERE id = ? RETURNING parent_id|]

-- * Admin Operations

-- | SQL template for getting a job by ID
--
-- Parameters: job_id
--
-- Returns: Single job row if found
getJobByIdSQL :: Text -> Text -> Text
getJobByIdSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in [text|
        SELECT ${columns}
        FROM ${tbl}
        WHERE id = ?
      |]

-- | SQL template for fetching a job by its dedup_key.
--
-- The partial unique index on @dedup_key@ guarantees at most one row.
--
-- Parameters: dedup_key
--
-- Returns: Single job row if found
getJobByDedupKeySQL :: Text -> Text -> Text
getJobByDedupKeySQL schema tableName =
  let tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in [text|
        SELECT ${columns}
        FROM ${tbl}
        WHERE dedup_key = ?
      |]

-- | SQL template for canceling (deleting) a job by ID.
--
-- Refuses to delete a job that has children - use 'cancelJobCascadeSQL' instead.
--
-- If the deleted job was a child and no siblings remain in the queue,
-- resumes the parent for its completion round.
--
-- Returns @rows_affected@.
--
-- Parameters: job_id (for DELETE), job_id (for children guard)
cancelJobSQL :: Text -> Text -> Text
cancelJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH cancel AS (
          DELETE FROM ${tbl}
          WHERE id = ?
            AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ?)
          RETURNING id, parent_id
        ),
        wake_parent AS (
          UPDATE ${tbl}
          SET suspended = FALSE, updated_at = NOW()
          WHERE id = (SELECT parent_id FROM cancel WHERE parent_id IS NOT NULL)
            AND suspended = TRUE
            AND NOT EXISTS (
              SELECT 1 FROM ${tbl} c
              WHERE c.parent_id = (SELECT parent_id FROM cancel WHERE parent_id IS NOT NULL)
                AND c.id NOT IN (SELECT id FROM cancel)
            )
          RETURNING id
        )
        SELECT (SELECT count(*) FROM cancel) AS result
      |]

-- | SQL template for getting queue statistics
--
-- Returns: total_jobs, visible_jobs, oldest_job_age_seconds
getQueueStatsSQL :: Text -> Text -> Text
getQueueStatsSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        SELECT
          COUNT(*) as total_jobs,
          COUNT(*) FILTER (WHERE NOT suspended AND (not_visible_until IS NULL OR not_visible_until <= NOW())) as visible_jobs,
          EXTRACT(EPOCH FROM (NOW() - MIN(inserted_at)))::float8 as oldest_job_age_seconds
        FROM ${tbl}
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

-- | Batch DLQ child count: returns (parent_id, count) for a set of job IDs
--
-- Parameters: array of job IDs
countDLQChildrenBatchSQL :: Text -> Text -> Text
countDLQChildrenBatchSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|
        SELECT parent_id, COUNT(*)
        FROM ${dlqTbl}
        WHERE parent_id = ANY(?)
        GROUP BY parent_id
      |]

-- ---------------------------------------------------------------------------
-- Batch Operations
-- ---------------------------------------------------------------------------

-- | SQL template for moving multiple jobs to DLQ in a single operation
--
-- Uses unnest to process multiple (id, attempts, error_msg) tuples.
-- Returns the number of jobs moved.
--
-- Parameters: Array of job IDs, array of attempts, array of error messages
moveToDLQBatchSQL :: Text -> Text -> Text
moveToDLQBatchSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      cols = jobColsExceptError
   in [text|
        WITH input_jobs AS (
          SELECT unnest(?::bigint[]) AS id,
                 unnest(?::int[]) AS expected_attempts,
                 unnest(?::text[]) AS error_msg
        ),
        deleted_jobs AS (
          DELETE FROM ${tbl} j
          USING input_jobs ij
          WHERE j.id = ij.id AND j.attempts = ij.expected_attempts
          RETURNING j.*, ij.error_msg AS new_error
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (job_id, failed_at, ${cols}, last_error)
          SELECT id, NOW(), ${cols}, new_error
          FROM deleted_jobs
        )
        SELECT count(*) FROM deleted_jobs
      |]

-- | SQL template for deleting multiple DLQ jobs by ID
--
-- Parameters: Array of DLQ job IDs
-- Returns: parent_id of each deleted job (NULL if no parent)
deleteDLQJobsBatchSQL :: Text -> Text -> Text
deleteDLQJobsBatchSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|DELETE FROM ${dlqTbl} WHERE id = ANY(?) RETURNING parent_id|]

-- ---------------------------------------------------------------------------
-- Job Dependency Operations
-- ---------------------------------------------------------------------------

-- | Pause all claimable jobs in a parent's subtree (set suspended = TRUE).
--
-- Walks the descendant tree and pauses every job that is currently
-- claimable (not in-flight, not already suspended). Naturally-suspended
-- rollup finalizers in the tree are left alone (they're already suspended
-- waiting for their own children). In-flight children are left alone so
-- their visibility timeout can expire normally if the worker crashes -
-- pausing them would break crash recovery.
--
-- Parameters: parent_id
pauseChildrenSQL :: Text -> Text -> Text
pauseChildrenSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH RECURSIVE descendants AS (
          SELECT id FROM ${tbl} WHERE parent_id = ?
          UNION ALL
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        )
        UPDATE ${tbl}
        SET suspended = TRUE, updated_at = NOW()
        WHERE id IN (SELECT id FROM descendants)
          AND NOT suspended
          AND (not_visible_until IS NULL OR not_visible_until <= NOW())
      |]

-- | Resume user-paused jobs in a parent's subtree (set suspended = FALSE).
--
-- Walks the descendant tree and resumes every suspended job that is NOT a
-- naturally-suspended rollup finalizer. A rollup is "naturally suspended"
-- when it has children still in the main queue - it should stay suspended
-- until those children complete, otherwise its handler would run with an
-- incomplete view of child results. Resuming the rollup's children directly
-- is correct: the rollup will wake itself when they finish.
--
-- Parameters: parent_id
resumeChildrenSQL :: Text -> Text -> Text
resumeChildrenSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH RECURSIVE descendants AS (
          SELECT id FROM ${tbl} WHERE parent_id = ?
          UNION ALL
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        )
        UPDATE ${tbl} t
        SET suspended = FALSE, updated_at = NOW()
        WHERE t.id IN (SELECT id FROM descendants)
          AND t.suspended = TRUE
          AND NOT (
            t.parent_state IS NOT NULL
            AND EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = t.id)
          )
      |]

-- | Recursive CTE binding @descendants@ to a job id and all its descendants.
-- Shared by the cascade-delete templates. Binds one @?@ (the root job id).
descendantsCte :: Text -> Text
descendantsCte tbl =
  [text|
    WITH RECURSIVE descendants AS (
      SELECT id FROM ${tbl} WHERE id = ?
      UNION ALL
      SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
    )
  |]

-- | Cancel a job and all its descendants recursively.
--
-- Parameters: job_id
cancelJobCascadeSQL :: Text -> Text -> Text
cancelJobCascadeSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      cte = descendantsCte tbl
   in [text|
        ${cte},
        deleted AS (
          DELETE FROM ${tbl} WHERE id IN (SELECT id FROM descendants)
          RETURNING id
        )
        SELECT count(*) FROM deleted
      |]

-- | Cascade-delete like 'cancelJobCascadeSQL', plus a per-row cancel NOTIFY for each deleted claimed job.
-- Parameters: job_id
forceCancelJobSQL :: SchemaName -> TableName -> Text
forceCancelJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      cte = descendantsCte tbl
      chan = T.replace "'" "''" (cancelNotifyChannel schema tableName)
   in [text|
        ${cte},
        deleted AS (
          DELETE FROM ${tbl} WHERE id IN (SELECT id FROM descendants)
          RETURNING id, claimed_by
        ),
        notif AS (
          SELECT pg_notify(
            '${chan}',
            json_build_object('worker_id', d.claimed_by, 'job_id', d.id)::text
          )
          FROM deleted d
          WHERE d.claimed_by IS NOT NULL
        )
        SELECT count(*)::int8 FROM deleted
        WHERE (SELECT count(*) FROM notif) >= 0
      |]

-- | Cancel an entire job tree by walking up from any node to the root,
-- then cascade-deleting everything from the root down.
--
-- Parameters: job_id
cancelJobTreeSQL :: Text -> Text -> Text
cancelJobTreeSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH RECURSIVE
        ancestors AS (
          SELECT id, parent_id FROM ${tbl} WHERE id = ?
          UNION ALL
          SELECT j.id, j.parent_id FROM ${tbl} j JOIN ancestors a ON j.id = a.parent_id
        ),
        root AS (
          SELECT id FROM ancestors WHERE parent_id IS NULL
        ),
        descendants AS (
          SELECT id FROM ${tbl} WHERE id = (SELECT id FROM root)
          UNION ALL
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        ),
        deleted AS (
          DELETE FROM ${tbl} WHERE id IN (SELECT id FROM descendants)
          RETURNING id
        )
        SELECT count(*) FROM deleted
      |]

-- | Try to wake a suspended ancestor when all its children are gone.
--
-- Resumes the parent for a completion round (sets suspended = FALSE).
-- Only wakes if the parent is suspended and has no remaining children
-- in the main queue.
--
-- Parameters: ancestor_id (repeated 2 times)
tryWakeAncestorSQL :: Text -> Text -> Text
tryWakeAncestorSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET suspended = FALSE, updated_at = NOW()
        WHERE id = ?
          AND suspended = TRUE
          AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ?)
      |]

-- | Cascade all descendants of a rollup parent to the DLQ.
--
-- Recursively finds all descendants and moves them from the main queue
-- to the DLQ in a single operation. Used when a rollup parent is moved
-- to DLQ to prevent orphaned children from hitting FK violations on
-- the results table.
--
-- Parameters: parent_job_id, error_message
cascadeChildrenToDLQSQL :: Text -> Text -> Text
cascadeChildrenToDLQSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      cols = jobColsExceptError
   in [text|
        WITH RECURSIVE descendants AS (
          SELECT id FROM ${tbl} WHERE parent_id = ?
          UNION ALL
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        ),
        deleted AS (
          DELETE FROM ${tbl}
          WHERE id IN (SELECT id FROM descendants)
          RETURNING id, ${cols}
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (job_id, ${cols}, last_error)
          SELECT id, ${cols}, ?
          FROM deleted
        )
        SELECT count(*) FROM deleted
      |]

-- | Find descendant rollup finalizer IDs for snapshot preservation.
--
-- Used before cascade-DLQ to identify intermediate rollup nodes that
-- need their results persisted into @parent_state@ before deletion.
--
-- Parameters: parent_job_id
descendantRollupIdsSQL :: Text -> Text -> Text
descendantRollupIdsSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH RECURSIVE descendants AS (
          SELECT id, parent_state FROM ${tbl} WHERE parent_id = ?
          UNION ALL
          SELECT j.id, j.parent_state FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        )
        SELECT id AS result FROM descendants WHERE parent_state IS NOT NULL
      |]

-- | Suspend a job (make it unclaimable).
--
-- Only suspends non-in-flight jobs (not currently being processed by workers).
--
-- Parameters: job_id
-- Returns: number of rows updated (0 if job doesn't exist, is in-flight, or already suspended)
suspendJobSQL :: Text -> Text -> Text
suspendJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET suspended = TRUE, updated_at = NOW()
        WHERE id = ?
          AND NOT suspended
          AND NOT (attempts > 0 AND not_visible_until IS NOT NULL AND not_visible_until > NOW())
      |]

-- | Resume a suspended job (make it claimable again).
--
-- Refuses to resume a rollup finalizer that still has children in the main
-- queue, preventing premature handler execution. Children in the DLQ are
-- considered terminal - the finalizer's handler receives DLQ errors via
-- 'readChildResultsSQL' and can decide how to handle them.
--
-- Parameters: job_id
-- Returns: number of rows updated (0 if job doesn't exist, isn't suspended,
--          or is a finalizer with remaining children in the main queue)
resumeJobSQL :: Text -> Text -> Text
resumeJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET suspended = FALSE, updated_at = NOW()
        WHERE id = ? AND suspended = TRUE
          AND NOT (
            parent_state IS NOT NULL
            AND EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ${tbl}.id)
          )
      |]

-- | Check whether a parent job exists.
--
-- Parameters: parent_id
-- Returns: single row with a boolean
parentExistsSQL :: Text -> Text -> Text
parentExistsSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|SELECT EXISTS (SELECT 1 FROM ${tbl} WHERE id = ?) AS result|]

-- | Fetch just the parent_id for a given job.
--
-- Parameters: job_id
-- Returns: single row with parent_id (NULL if no parent or job doesn't exist)
getParentIdSQL :: Text -> Text -> Text
getParentIdSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|SELECT parent_id FROM ${tbl} WHERE id = ?|]

-- ---------------------------------------------------------------------------
-- Results Table Operations
-- ---------------------------------------------------------------------------

-- | Insert a child's result into the results table.
--
-- Parameters: parent_id (bigint), child_id (bigint), result (jsonb)
insertResultSQL :: Text -> Text -> Text
insertResultSQL schema tableName =
  let resultsTbl = jobQueueResultsTable schema tableName
   in [text|
        INSERT INTO ${resultsTbl} (parent_id, child_id, result)
        VALUES (?, ?, ?)
        ON CONFLICT (parent_id, child_id) DO UPDATE SET result = EXCLUDED.result
      |]

-- | Get all child results for a parent from the results table.
--
-- Parameters: parent_id (bigint)
-- Returns: rows of (child_id bigint, result jsonb)
getResultsByParentSQL :: Text -> Text -> Text
getResultsByParentSQL schema tableName =
  let resultsTbl = jobQueueResultsTable schema tableName
   in [text|
        SELECT child_id, result FROM ${resultsTbl} WHERE parent_id = ?
      |]

-- | Get DLQ child errors for a parent.
--
-- Returns rows of (job_id bigint, last_error text) for each DLQ'd child.
--
-- Parameters: parent_id (bigint)
getDLQChildErrorsByParentSQL :: Text -> Text -> Text
getDLQChildErrorsByParentSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|
        SELECT job_id, last_error FROM ${dlqTbl} WHERE parent_id = ?
      |]

-- | Snapshot results into parent_state before DLQ move.
--
-- Parameters: parent_state (jsonb), job_id (bigint)
persistParentStateSQL :: Text -> Text -> Text
persistParentStateSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl} SET parent_state = ?, updated_at = NOW() WHERE id = ?
      |]

-- | Read the raw parent_state snapshot from the DB.
--
-- Parameters: job_id (bigint)
-- Returns: single row with parent_state (jsonb, may be NULL)
getParentStateSnapshotSQL :: Text -> Text -> Text
getParentStateSnapshotSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|SELECT parent_state FROM ${tbl} WHERE id = ?|]

-- | Read all child result data for a rollup finalizer in a single query.
--
-- Combines results table, DLQ child errors, and parent_state snapshot
-- into a tagged UNION ALL. Tags: @'r'@ = result, @'e'@ = DLQ error,
-- @'s'@ = parent_state snapshot.
--
-- Parameters: parent_id (bigint) × 3
-- Returns: tagged rows (source, child_id, result_jsonb, error_text, dlq_pk)
readChildResultsSQL :: Text -> Text -> Text
readChildResultsSQL schema tableName =
  let resultsTbl = jobQueueResultsTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      tbl = jobQueueTable schema tableName
   in [text|
        SELECT 'r'::text AS source, child_id, result, NULL::text AS error, NULL::bigint AS dlq_pk FROM ${resultsTbl} WHERE parent_id = ?
        UNION ALL
        SELECT 'e' AS source, job_id AS child_id, NULL::jsonb AS result, last_error AS error, id AS dlq_pk FROM ${dlqTbl} WHERE parent_id = ?
        UNION ALL
        SELECT 's' AS source, NULL::bigint AS child_id, parent_state AS result, NULL::text AS error, NULL::bigint AS dlq_pk FROM ${tbl} WHERE id = ? AND parent_state IS NOT NULL
      |]

-- ---------------------------------------------------------------------------
-- Groups Table Operations
-- ---------------------------------------------------------------------------

-- | @FOR UPDATE SKIP LOCKED@ over the groups rows, returning the locked keys for
-- the reaper to recompute. Claim-locked groups are skipped.
lockGroupsSQL :: Text -> Text -> Text
lockGroupsSQL schema tableName =
  let groupsTbl = jobQueueGroupsTable schema tableName
   in [text|SELECT group_key FROM ${groupsTbl} FOR UPDATE SKIP LOCKED|]

-- | Recompute the groups table, scoped to the locked keys from 'lockGroupsSQL'.
-- A separate statement, so its snapshot post-dates the lock and cannot clobber a
-- concurrent claim's @in_flight_until@. The INSERT only repairs missing rows.
refreshGroupsSQL :: Text -> Text -> Text
refreshGroupsSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
   in [text|
        WITH params AS (
          SELECT unnest(?::text[]) AS group_key
        ),
        current AS (
          SELECT group_key,
                 MIN(priority) AS min_priority,
                 MIN(id) AS min_id,
                 COUNT(*) AS job_count,
                 COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended) AS ready_count,
                 MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended) AS next_due,
                 MAX(not_visible_until) FILTER (WHERE not_visible_until > NOW() AND NOT suspended AND attempts > 0) AS in_flight_until
          FROM ${tbl}
          WHERE group_key IN (SELECT group_key FROM params)
          GROUP BY group_key
        ),
        deleted AS (
          DELETE FROM ${groupsTbl} g
          WHERE g.group_key IN (SELECT group_key FROM params)
            AND NOT EXISTS (SELECT 1 FROM current c WHERE c.group_key = g.group_key)
        ),
        updated AS (
          UPDATE ${groupsTbl} g
          SET min_priority = c.min_priority,
              min_id = c.min_id,
              job_count = c.job_count,
              ready_count = c.ready_count,
              next_due = c.next_due,
              in_flight_until = c.in_flight_until
          FROM current c
          WHERE g.group_key = c.group_key
            AND (g.min_priority <> c.min_priority OR g.min_id <> c.min_id
                 OR g.job_count <> c.job_count
                 OR g.ready_count <> c.ready_count
                 OR g.next_due IS DISTINCT FROM c.next_due
                 OR g.in_flight_until IS DISTINCT FROM c.in_flight_until)
        )
        INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count, ready_count, next_due, in_flight_until)
        SELECT group_key,
               MIN(priority), MIN(id), COUNT(*),
               COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended),
               MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended),
               MAX(not_visible_until) FILTER (WHERE not_visible_until > NOW() AND NOT suspended AND attempts > 0)
        FROM ${tbl}
        WHERE group_key IS NOT NULL
          AND group_key NOT IN (SELECT group_key FROM ${groupsTbl})
        GROUP BY group_key
        ON CONFLICT (group_key) DO NOTHING
      |]

-- ---------------------------------------------------------------------------
-- Cron Schedule Operations
-- ---------------------------------------------------------------------------

allCronColumns :: Text
allCronColumns = T.intercalate ", " (codecColumns cronScheduleRowCodec)

-- | Upsert a cron schedule's default values, preserving @override_*@ columns on conflict.
-- Parameters: name, queue_name, default_expression, default_overlap, default_timezone
upsertCronDefaultSQL :: Text -> Text
upsertCronDefaultSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "INSERT INTO "
        <> tbl
        <> " (name, queue_name, default_expression, default_overlap, default_timezone) VALUES (?, ?, ?, ?, ?)"
        <> " ON CONFLICT (name) DO UPDATE SET"
        <> " queue_name = EXCLUDED.queue_name,"
        <> " default_expression = EXCLUDED.default_expression,"
        <> " default_overlap = EXCLUDED.default_overlap,"
        <> " default_timezone = EXCLUDED.default_timezone,"
        <> " updated_at = NOW()"

-- | List cron schedules ordered by name, optionally filtered by queue.
-- Parameters: queue_name (NULL = all queues)
listCronSchedulesSQL :: Text -> Text
listCronSchedulesSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in [text|
        SELECT ${allCronColumns} FROM ${tbl}
        WHERE ?::text IS NULL OR queue_name = ?::text
        ORDER BY name
      |]

-- | Get a single cron schedule by name.
--
-- Parameters: schedule name
getCronScheduleByNameSQL :: Text -> Text
getCronScheduleByNameSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "SELECT " <> allCronColumns <> " FROM " <> tbl <> " WHERE name = ?"

-- | Set @last_fired_at@ to NOW() for a schedule.
touchCronLastFiredSQL :: Text -> Text
touchCronLastFiredSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "UPDATE " <> tbl <> " SET last_fired_at = NOW() WHERE name = ?"

-- | Set @last_checked_at@ to the caller-supplied watermark for the given
-- schedule names. The watermark must be the minute boundary the scheduler
-- finished evaluating (not DB @NOW()@) so a slow iteration cannot leapfrog
-- @last_checked_at@ past minutes it never tested. 'GREATEST' guards against
-- backward motion under concurrent worker pools with skewed clocks.
--
-- Parameters: watermark timestamp, schedule names (text array)
touchCronCheckedSQL :: Text -> Text
touchCronCheckedSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "UPDATE "
        <> tbl
        <> " SET last_checked_at = GREATEST(last_checked_at, ?) WHERE name = ANY(?)"

-- | Fire-once-per-minute gate. Advances @last_fired_at@ to the minute floor
-- only if the existing value is strictly less. 0 rows = another pool won.
--
-- Parameters: minute floor, schedule name, minute floor
tryFireCronGateSQL :: Text -> Text
tryFireCronGateSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "UPDATE "
        <> tbl
        <> " SET last_fired_at = GREATEST(last_fired_at, ?)"
        <> " WHERE name = ?"
        <> " AND (last_fired_at IS NULL OR last_fired_at < ?)"

-- | Per-(schema, queue, name) transaction-scoped advisory lock for cron.
-- Parameters: schema, queue, name.
tryAcquireCronLeaderSQL :: Text
tryAcquireCronLeaderSQL =
  "SELECT pg_try_advisory_xact_lock(hashtextextended(? || ':' || ? || ':' || ?, 0)) AS result"

-- ---------------------------------------------------------------------------
-- Worker Registry Operations
-- ---------------------------------------------------------------------------

-- | Worker SELECT columns from the codec, with @health@ rendered as a computed expression.
workerColumnList :: Text
workerColumnList = T.intercalate ", " (map render (codecColumns workerRowCodec))
  where
    render "health" = workerHealthCaseSQL <> " AS health"
    render c = c

-- | Heartbeat-derived health, computed against the DB clock.
workerHealthCaseSQL :: Text
workerHealthCaseSQL =
  "CASE WHEN last_heartbeat < NOW() - stale_threshold_secs * interval '1 second' THEN 'stale' WHEN shutting_down THEN 'draining' ELSE 'live' END"

-- | Render a 'Maybe UUID' as a SQL literal: NULL or 'uuid-text'::uuid.
uuidLiteral :: Maybe UUID -> Text
uuidLiteral Nothing = "NULL"
uuidLiteral (Just u) = "'" <> UUID.toText u <> "'::uuid"

-- | Upsert a worker registration and return its effective paused state (worker OR queue).
-- Parameters: worker_id, queue_name, host_name, worker_count, stale_threshold_secs, metadata
upsertWorkerSQL :: SchemaName -> Text
upsertWorkerSQL schemaName =
  let tbl = arbiterWorkersTable schemaName
      qmTbl = arbiterQueuesTable schemaName
   in [text|
        WITH upserted AS (
          INSERT INTO ${tbl}
            (worker_id, queue_name, host_name, worker_count, stale_threshold_secs, metadata, started_at, last_heartbeat, shutting_down)
          VALUES (?, ?, ?, ?, ?, ?, NOW(), NOW(), FALSE)
          ON CONFLICT (worker_id) DO UPDATE
            SET queue_name = EXCLUDED.queue_name,
                host_name = EXCLUDED.host_name,
                worker_count = EXCLUDED.worker_count,
                stale_threshold_secs = EXCLUDED.stale_threshold_secs,
                metadata = EXCLUDED.metadata,
                last_heartbeat = NOW(),
                shutting_down = FALSE
          RETURNING queue_name, paused
        )
        SELECT u.paused OR COALESCE(qm.paused, FALSE) AS effective_paused
        FROM upserted u
        LEFT JOIN ${qmTbl} qm ON qm.queue_name = u.queue_name
      |]

-- | Bump last_heartbeat and return the worker's effective paused state (worker OR queue).
-- Parameters: worker_id
heartbeatWorkerSQL :: SchemaName -> Text
heartbeatWorkerSQL schemaName =
  let tbl = arbiterWorkersTable schemaName
      qmTbl = arbiterQueuesTable schemaName
   in [text|
        WITH updated AS (
          UPDATE ${tbl} SET last_heartbeat = NOW()
          WHERE worker_id = ?
          RETURNING queue_name, paused
        )
        SELECT updated.paused OR COALESCE(qm.paused, FALSE) AS effective_paused
        FROM updated
        LEFT JOIN ${qmTbl} qm ON qm.queue_name = updated.queue_name
      |]

-- | Set @paused@ for a worker and NOTIFY its effective pause state.
-- Parameters: paused, worker_id
setWorkerPausedSQL :: SchemaName -> Text
setWorkerPausedSQL schemaName =
  let tbl = arbiterWorkersTable schemaName
      qmTbl = arbiterQueuesTable schemaName
      chanPrefix = pauseNotifyChannelPrefix schemaName
   in [text|
        WITH updated AS (
          UPDATE ${tbl} SET paused = ? WHERE worker_id = ?
          RETURNING worker_id, queue_name, paused
        ),
        notif AS (
          SELECT pg_notify(
            LEFT('${chanPrefix}' || u.queue_name, 63),
            json_build_object(
              'worker_id', u.worker_id,
              'paused', u.paused OR COALESCE(qm.paused, FALSE)
            )::text
          )
          FROM updated u
          LEFT JOIN ${qmTbl} qm ON qm.queue_name = u.queue_name
        )
        SELECT count(*)::int8 AS count FROM updated
        WHERE (SELECT count(*) FROM notif) >= 0
      |]

-- | Mark a worker as gracefully draining.
-- Parameters: worker_id
markWorkerShuttingDownSQL :: SchemaName -> Text
markWorkerShuttingDownSQL schemaName =
  let tbl = arbiterWorkersTable schemaName
   in "UPDATE " <> tbl <> " SET shutting_down = TRUE, last_heartbeat = NOW() WHERE worker_id = ?"

-- | Remove a worker row outright (clean shutdown, rather than waiting for the stale-sweeper).
-- Parameters: worker_id
deleteWorkerSQL :: SchemaName -> Text
deleteWorkerSQL schemaName =
  let tbl = arbiterWorkersTable schemaName
   in "DELETE FROM " <> tbl <> " WHERE worker_id = ?"

-- | List workers, with NULL parameters short-circuiting the corresponding filter.
-- Parameters: queue_name (NULL = any queue), live_threshold_seconds (NULL = no heartbeat filter)
listWorkersSQL :: SchemaName -> Text
listWorkersSQL schemaName =
  let tbl = arbiterWorkersTable schemaName
      cols = workerColumnList
   in [text|
        WITH filt AS (SELECT ?::text AS queue, ?::float8 AS live_secs)
        SELECT ${cols} FROM ${tbl}, filt
        WHERE (filt.queue IS NULL OR queue_name = filt.queue)
          AND (filt.live_secs IS NULL
               OR last_heartbeat > NOW() - filt.live_secs * interval '1 second')
        ORDER BY queue_name, started_at DESC
      |]

-- | Delete worker rows older than their own @stale_threshold_secs@.
deleteStaleWorkersSQL :: SchemaName -> Text
deleteStaleWorkersSQL schemaName =
  let tbl = arbiterWorkersTable schemaName
   in [text|
        DELETE FROM ${tbl}
        WHERE last_heartbeat < NOW() - (stale_threshold_secs * interval '1 second')
      |]

-- ---------------------------------------------------------------------------
-- Queue Operations
-- ---------------------------------------------------------------------------

queueColumnList :: Text
queueColumnList = T.intercalate ", " (codecColumns queueRowCodec)

-- | Insert an arbiter_queues row with defaults if one doesn't already exist.
--
-- Parameters: queue_name
ensureQueueSQL :: SchemaName -> Text
ensureQueueSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
   in [text|
        INSERT INTO ${tbl} (queue_name) VALUES (?)
        ON CONFLICT (queue_name) DO NOTHING
      |]

-- | Upsert the queue's paused flag and fan one NOTIFY per worker in the queue.
-- @paused_at@ is set on first pause, cleared on resume, untouched on re-pause.
-- Parameters: queue_name, paused, paused (repeated for the CASE expression)
setQueuePausedSQL :: SchemaName -> Text
setQueuePausedSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
      wTbl = arbiterWorkersTable schemaName
      chanPrefix = pauseNotifyChannelPrefix schemaName
   in [text|
        WITH upsert AS (
          INSERT INTO ${tbl} (queue_name, paused, paused_at)
            VALUES (?, ?, CASE WHEN ?::boolean THEN NOW() ELSE NULL END)
          ON CONFLICT (queue_name) DO UPDATE
            SET paused = EXCLUDED.paused,
                paused_at = CASE
                  WHEN EXCLUDED.paused AND NOT arbiter_queues.paused THEN NOW()
                  WHEN NOT EXCLUDED.paused THEN NULL
                  ELSE arbiter_queues.paused_at
                END,
                updated_at = NOW()
          RETURNING queue_name, paused
        ),
        notif AS (
          SELECT pg_notify(
            LEFT('${chanPrefix}' || w.queue_name, 63),
            json_build_object(
              'worker_id', w.worker_id,
              'paused', w.paused OR u.paused
            )::text
          )
          FROM upsert u
          JOIN ${wTbl} w ON w.queue_name = u.queue_name
        )
        SELECT count(*)::int8 AS count FROM upsert
        WHERE (SELECT count(*) FROM notif) >= 0
      |]

-- | Get the arbiter_queues row for a single queue.
--
-- Parameters: queue_name
getQueueSQL :: SchemaName -> Text
getQueueSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
      cols = queueColumnList
   in [text|SELECT ${cols} FROM ${tbl} WHERE queue_name = ?|]

-- | List all arbiter_queues rows.
listQueuesSQL :: SchemaName -> Text
listQueuesSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
      cols = queueColumnList
   in [text|SELECT ${cols} FROM ${tbl} ORDER BY queue_name|]

-- ---------------------------------------------------------------------------
-- Global Gate Operations
-- ---------------------------------------------------------------------------

-- | Idempotently create the gate row for a task. Parameters: task_name.
ensureGateRowSQL :: SchemaName -> Text
ensureGateRowSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|
        INSERT INTO ${tbl} (task_name) VALUES (?)
        ON CONFLICT (task_name) DO NOTHING
      |]

-- | Cheap read-only pre-transaction check: TRUE if last_run_at is older than the interval.
-- Parameters: interval seconds, task_name.
checkGateSQL :: SchemaName -> Text
checkGateSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|
        SELECT (last_run_at < NOW() - (?::double precision * interval '1 second'))
          AS result
        FROM ${tbl}
        WHERE task_name = ?
      |]

-- | Atomically claim the gate row iff the interval elapsed and no concurrent tx holds it.
-- Parameters: task_name, interval seconds.
tryClaimGateSQL :: SchemaName -> Text
tryClaimGateSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|
        SELECT 1::bigint AS result FROM ${tbl}
        WHERE task_name = ?
          AND last_run_at < NOW() - (?::double precision * interval '1 second')
        FOR UPDATE SKIP LOCKED
      |]

-- | Bump last_run_at to NOW(), inside the claim transaction so it commits with the task's work.
-- Parameters: task_name.
bumpGateSQL :: SchemaName -> Text
bumpGateSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|UPDATE ${tbl} SET last_run_at = NOW() WHERE task_name = ?|]

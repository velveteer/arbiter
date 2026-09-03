{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Jobs SQL templates.
module Arbiter.Core.Sql.Jobs
  ( JobFilter (..)
  , JobSortColumn (..)
  , jobSortColumnName
  , DLQSortColumn (..)
  , dlqSortColumnName
  , ArchiveSortColumn (..)
  , archiveSortColumnName
  , SortDir (..)
  , sortDirSql
  , throttledPredicateSQL
  , jobStatusCaseSQL
  , jobsWithStatusSubquery
  , listJobsFilteredSQL
  , listJobsWithStatusSQL
  , countJobsFilteredSQL
  , getJobByIdWithStatusSQL
  , listDLQFilteredSQL
  , NullsBehavior (..)
  , nullsClause
  , jobColumnNulls
  , dlqColumnNulls
  , buildJobsOrderBy
  , buildDLQOrderBy
  , buildArchiveOrderBy
  , countDLQFilteredSQL
  , allDLQColumns
  , jobColsExceptId
  , dlqCarriedCols
  , requeuedCols
  , enqueuedAgainCols
  , jobColumns
  , dedupUpdateSet
  , insertJobSQL
  , insertJobReplaceSQL
  , insertJobsBatchSQL
  , insertJobsBatchSQL_
  , insertJobsBatchBase
  , getJobByIdSQL
  , getJobByDedupKeySQL
  , cancelJobSQL
  , unionAllOverQueueTables
  ) where

import Data.Aeson (Value)
import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)
import NeatInterpolation (text)

import Arbiter.Core.Codec (dlqRowCodec, jobRowCodec)
import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , jobQueueDLQTable
  , jobQueueTable
  )
import Arbiter.Core.Job.Types (JobRead, JobStatus)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, mwhen, rows)

-- | A narrowing predicate on a job listing. @FilterJobId@ and the @completed_at@
-- range name DLQ and archive columns.
data JobFilter
  = FilterGroupKey Text
  | FilterParentId Int64
  | FilterRootsOnly
  | FilterStatus JobStatus
  | FilterId Int64
  | FilterJobId Int64
  | FilterClaimedBy UUID
  | FilterKind Text
  | FilterPayloadText Text
  | FilterRateLimitPrefix Text
  | FilterConcurrencyPrefix Text
  | FilterInsertedAfter UTCTime
  | FilterInsertedBefore UTCTime
  | FilterCompletedAfter UTCTime
  | FilterCompletedBefore UTCTime
  deriving stock (Eq, Show)

-- | Sortable columns on the main jobs table.
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

-- | Sortable columns on the DLQ table. @DlqId@ is the DLQ primary key and
-- @DlqJobId@ the original job id.
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

-- | The DLQ column a sort key names.
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

-- | Sortable columns on the archive table. @ArchiveId@ is the archive primary
-- key and @ArchiveJobId@ the original job id.
data ArchiveSortColumn
  = ArchiveId
  | ArchiveCompletedAt
  | ArchiveInsertedAt
  | ArchiveJobId
  | ArchiveAttempts
  | ArchiveGroupKey
  | ArchiveParentId
  deriving stock (Bounded, Enum, Eq, Show)

-- | The archive column a sort key names.
archiveSortColumnName :: ArchiveSortColumn -> Text
archiveSortColumnName = \case
  ArchiveId -> "id"
  ArchiveCompletedAt -> "completed_at"
  ArchiveInsertedAt -> "inserted_at"
  ArchiveJobId -> "job_id"
  ArchiveAttempts -> "attempts"
  ArchiveGroupKey -> "group_key"
  ArchiveParentId -> "parent_id"

-- | Sort direction.
data SortDir = SortAsc | SortDesc
  deriving stock (Bounded, Enum, Eq, Show)

-- | A sort direction as SQL.
sortDirSql :: SortDir -> Text
sortDirSql SortAsc = "ASC"
sortDirSql SortDesc = "DESC"

-- | A throttle-deferred job with a live marker, still parked. Shared by status,
-- count, and wake. Claiming clears the marker.
throttledPredicateSQL :: Text
throttledPredicateSQL =
  "throttled_until > NOW() AND not_visible_until > NOW()"

-- | The derived job status. Its string values match 'Arbiter.Core.Job.Status.jobStatusToText'.
jobStatusCaseSQL :: Text
jobStatusCaseSQL =
  [text|
    CASE
      WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
      WHEN suspended THEN 'suspended'
      WHEN ${throttledPredicateSQL} THEN 'throttled'
      WHEN claimed_by IS NULL AND attempts > 0
           AND not_visible_until IS NOT NULL AND not_visible_until > NOW() THEN 'backoff'
      WHEN attempts > 0 AND not_visible_until IS NOT NULL AND not_visible_until > NOW() THEN 'in_flight'
      WHEN not_visible_until IS NOT NULL AND not_visible_until > NOW() THEN 'scheduled'
      ELSE 'ready'
    END
  |]

-- | All job columns plus the derived @status@ column, aliased @job@, for filtering.
jobsWithStatusSubquery :: Text -> Text -> Text
jobsWithStatusSubquery schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|(SELECT ${jobColumns}, ${jobStatusCaseSQL} AS status FROM ${tbl}) job|]

-- | List filtered jobs without the derived status.
listJobsFilteredSQL :: Text -> Text -> Query () -> Text -> Int64 -> Int64 -> Query (JobRead Value)
listJobsFilteredSQL schema tableName whereFrag orderBy limit offset =
  let tbl = jobQueueTable schema tableName
   in rows
        (jobRowCodec tableName)
        [sql|
          SELECT ${jobColumns}
          FROM ${tbl}
          ${whereFrag}
          ORDER BY ${orderBy} LIMIT #{limit :: CInt8} OFFSET #{offset :: CInt8}
        |]

-- | List filtered jobs with @status@ as a trailing column. The caller attaches the row decoder.
listJobsWithStatusSQL :: Text -> Text -> Query () -> Text -> Int64 -> Int64 -> Query ()
listJobsWithStatusSQL schema tableName whereFrag orderBy limit offset =
  let sub = jobsWithStatusSubquery schema tableName
   in [sql|
        SELECT * FROM ${sub}
        ${whereFrag}
        ORDER BY ${orderBy} LIMIT #{limit :: CInt8} OFFSET #{offset :: CInt8}
      |]

-- | Count filtered jobs through the status subquery.
countJobsFilteredSQL :: Text -> Text -> Query () -> Query Int64
countJobsFilteredSQL schema tableName whereFrag =
  let sub = jobsWithStatusSubquery schema tableName
   in [sql|SELECT COUNT(*) AS @{count :: CInt8} FROM ${sub} ${whereFrag}|]

-- | Fetch a single job by id with its derived @status@ trailing column. The caller attaches the row decoder.
getJobByIdWithStatusSQL :: Text -> Text -> Int64 -> Query ()
getJobByIdWithStatusSQL schema tableName jobId =
  let sub = jobsWithStatusSubquery schema tableName
   in [sql|SELECT * FROM ${sub} WHERE id = #{jobId :: CInt8}|]

-- | List DLQ jobs under a dynamic WHERE and an @orderBy@ from 'buildDLQOrderBy'.
listDLQFilteredSQL :: Text -> Text -> Query () -> Text -> Int64 -> Int64 -> Query (Int64, UTCTime, JobRead Value)
listDLQFilteredSQL schema tableName whereFrag orderBy limit offset =
  let dlqTbl = jobQueueDLQTable schema tableName
   in rows
        (dlqRowCodec tableName)
        [sql|
          SELECT ${allDLQColumns}
          FROM ${dlqTbl}
          ${whereFrag}
          ORDER BY ${orderBy}
          LIMIT #{limit :: CInt8} OFFSET #{offset :: CInt8}
        |]

-- | How NULLs in a sort column should order relative to non-NULL values.
data NullsBehavior
  = -- | Column is NOT NULL. Emit no NULLS clause.
    NullsNotApplicable
  | -- | NULL means absent. Always sorts last.
    NullsAsAbsent
  | -- | NULL is the column's minimum (e.g. @not_visible_until IS NULL@ means visible now).
    -- NULLS FIRST when ASC, NULLS LAST when DESC.
    NullsAsMinimum

-- | The @NULLS@ placement for a column's null semantics and direction.
nullsClause :: NullsBehavior -> SortDir -> Text
nullsClause NullsNotApplicable _ = ""
nullsClause NullsAsAbsent _ = " NULLS LAST"
nullsClause NullsAsMinimum SortAsc = " NULLS FIRST"
nullsClause NullsAsMinimum SortDesc = " NULLS LAST"

-- | How nulls sort for a job column.
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

-- | How nulls sort for a DLQ column.
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

archiveColumnNulls :: ArchiveSortColumn -> NullsBehavior
archiveColumnNulls = \case
  ArchiveId -> NullsNotApplicable
  ArchiveCompletedAt -> NullsNotApplicable
  ArchiveInsertedAt -> NullsNotApplicable
  ArchiveJobId -> NullsNotApplicable
  ArchiveAttempts -> NullsNotApplicable
  ArchiveGroupKey -> NullsAsAbsent
  ArchiveParentId -> NullsAsAbsent

-- | Build an ORDER BY clause from a typed sort spec. @nullsFn@ places NULLs per
-- column. A stable @id@ tie-breaker in the primary sort's direction follows any
-- sort column other than @idCol@.
buildOrderBy :: (Eq a) => (a -> Text) -> (a -> NullsBehavior) -> a -> a -> Maybe a -> Maybe SortDir -> Text
buildOrderBy nameFn nullsFn defCol idCol mCol mDir =
  let sortCol = fromMaybe defCol mCol
      dir = fromMaybe SortDesc mDir
      columnName = nameFn sortCol
      dirText = sortDirSql dir
      nulls = nullsClause (nullsFn sortCol) dir
      tieBreaker = mwhen (sortCol /= idCol) [text|, id ${dirText}|]
   in [text|${columnName} ${dirText}${nulls}${tieBreaker}|]

-- | ORDER BY for the jobs table. Defaults to @id DESC@.
buildJobsOrderBy :: Maybe JobSortColumn -> Maybe SortDir -> Text
buildJobsOrderBy = buildOrderBy jobSortColumnName jobColumnNulls JsId JsId

-- | ORDER BY for the DLQ table. Defaults to @failed_at DESC@.
buildDLQOrderBy :: Maybe DLQSortColumn -> Maybe SortDir -> Text
buildDLQOrderBy = buildOrderBy dlqSortColumnName dlqColumnNulls DlqFailedAt DlqId

-- | ORDER BY for the archive table. Defaults to @completed_at DESC@.
buildArchiveOrderBy :: Maybe ArchiveSortColumn -> Maybe SortDir -> Text
buildArchiveOrderBy = buildOrderBy archiveSortColumnName archiveColumnNulls ArchiveCompletedAt ArchiveId

-- | Count DLQ jobs under a dynamic WHERE.
countDLQFilteredSQL :: Text -> Text -> Query () -> Query Int64
countDLQFilteredSQL schema tableName whereFrag =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|SELECT COUNT(*) AS @{count :: CInt8} FROM ${dlqTbl} ${whereFrag}|]

-- | The job read columns, in codec order, for SELECT and RETURNING.
jobColumns :: Text
jobColumns =
  [text|
    id, payload, group_key, inserted_at, updated_at, attempts, last_error, priority,
    last_attempted_at, not_visible_until, dedup_key, dedup_strategy, max_attempts,
    parent_id, parent_state, traceparent, tracestate, suspended, claimed_by, claim_seq,
    archive_for, kind, rate_limit_key, rate_limit_prefix, concurrency_key, concurrency_prefix
  |]

-- | The DLQ read columns, in codec order. The DLQ uses @job_id@ for the main-table @id@.
allDLQColumns :: Text
allDLQColumns =
  [text|
    id, failed_at, job_id, payload, group_key, inserted_at, updated_at, attempts, last_error, priority,
    last_attempted_at, not_visible_until, dedup_key, dedup_strategy, max_attempts,
    parent_id, parent_state, traceparent, tracestate, suspended, claimed_by, claim_seq,
    archive_for, kind, rate_limit_key, rate_limit_prefix, concurrency_key, concurrency_prefix
  |]

-- | The job read columns except @id@. The archive INSERT copies them with the main
-- table's @id@ as @job_id@.
jobColsExceptId :: Text
jobColsExceptId =
  [text|
    payload, group_key, inserted_at, updated_at, attempts, last_error, priority,
    last_attempted_at, not_visible_until, dedup_key, dedup_strategy, max_attempts,
    parent_id, parent_state, traceparent, tracestate, suspended, claimed_by, claim_seq,
    archive_for, kind, rate_limit_key, rate_limit_prefix, concurrency_key, concurrency_prefix
  |]

-- | Job columns carried through a DLQ round-trip. The read columns except @id@ and
-- @last_error@, plus write-only @rate_limit_cost@.
dlqCarriedCols :: Text
dlqCarriedCols =
  [text|
    payload, group_key, inserted_at, updated_at, attempts, priority,
    last_attempted_at, not_visible_until, dedup_key, dedup_strategy, max_attempts,
    parent_id, parent_state, traceparent, tracestate, suspended, claimed_by, claim_seq,
    archive_for, kind, rate_limit_key, rate_limit_prefix, concurrency_key, concurrency_prefix,
    rate_limit_cost
  |]

-- | Job columns a DLQ retry carries back to the main table. The retry re-arms the rest.
requeuedCols :: Text
requeuedCols =
  [text|
    payload, group_key, priority, max_attempts, parent_id, parent_state, traceparent, tracestate,
    archive_for, kind, rate_limit_key, rate_limit_prefix, concurrency_key, concurrency_prefix,
    rate_limit_cost
  |]

-- | 'requeuedCols' for an archive re-enqueue, without the parent link.
enqueuedAgainCols :: Text
enqueuedAgainCols =
  [text|
    payload, group_key, priority, max_attempts, traceparent, tracestate,
    archive_for, kind, rate_limit_key, rate_limit_prefix, concurrency_key, concurrency_prefix,
    rate_limit_cost
  |]

-- | @DO UPDATE SET@ body for a replace-dedup upsert. Copies each writable column
-- from the excluded row, then re-arms the replaced job for a fresh run.
dedupUpdateSet :: Text -> Text
dedupUpdateSet tbl =
  [text|
    payload = EXCLUDED.payload,
    group_key = EXCLUDED.group_key,
    priority = EXCLUDED.priority,
    not_visible_until = EXCLUDED.not_visible_until,
    dedup_strategy = EXCLUDED.dedup_strategy,
    max_attempts = EXCLUDED.max_attempts,
    parent_id = EXCLUDED.parent_id,
    parent_state = EXCLUDED.parent_state,
    traceparent = EXCLUDED.traceparent,
    tracestate = EXCLUDED.tracestate,
    suspended = EXCLUDED.suspended,
    archive_for = EXCLUDED.archive_for,
    kind = EXCLUDED.kind,
    rate_limit_key = EXCLUDED.rate_limit_key,
    rate_limit_prefix = EXCLUDED.rate_limit_prefix,
    concurrency_key = EXCLUDED.concurrency_key,
    concurrency_prefix = EXCLUDED.concurrency_prefix,
    rate_limit_cost = EXCLUDED.rate_limit_cost,
    attempts = 0,
    claim_seq = ${tbl}.claim_seq + 1,
    last_error = NULL,
    updated_at = NOW(),
    throttled_until = NULL,
    last_attempted_at = NULL,
    claimed_by = NULL
  |]

-- | An existing row is replaceable when idle, unflagged and childless.
replaceableGuard :: Text -> Text -> Text
replaceableGuard tbl dlqTbl =
  [text|
    (${tbl}.attempts = 0
      OR ${tbl}.claimed_by IS NULL
      OR ${tbl}.not_visible_until IS NULL
      OR ${tbl}.not_visible_until <= NOW())
      AND ${tbl}.cancel_requested_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM ${tbl} child WHERE child.parent_id = ${tbl}.id)
      AND NOT EXISTS (SELECT 1 FROM ${dlqTbl} dlq_child WHERE dlq_child.parent_id = ${tbl}.id)
  |]

-- | Insert a job. The write fragment carries the column list and parameters.
insertJobSQL :: SchemaName -> TableName -> Query () -> Query (JobRead Value)
insertJobSQL schema tableName valuesFrag =
  let tbl = jobQueueTable schema tableName
   in rows
        (jobRowCodec tableName)
        [sql|
          INSERT INTO ${tbl} ${valuesFrag}
          ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
          RETURNING ${jobColumns}
        |]

-- | Insert under the replace dedup strategy. Replaces an existing job that is idle
-- and childless in the main queue and the DLQ. @ON CONFLICT DO UPDATE@ fires the
-- groups UPDATE trigger, which maintains a cross-group move.
insertJobReplaceSQL :: SchemaName -> TableName -> Query () -> Query (JobRead Value)
insertJobReplaceSQL schema tableName valuesFrag =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      guard = replaceableGuard tbl dlqTbl
      dedupSet = dedupUpdateSet tbl
   in rows
        (jobRowCodec tableName)
        [sql|
          INSERT INTO ${tbl} ${valuesFrag}
          ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO UPDATE SET
            ${dedupSet}
          WHERE ${guard}
          RETURNING ${jobColumns}
        |]

-- | Batch insert over @unnest@ed parallel arrays. An ignore-dedup job is skipped on
-- conflict. A replace-dedup job updates an idle existing row.
insertJobsBatchSQL :: SchemaName -> TableName -> Query () -> Query (JobRead Value)
insertJobsBatchSQL schema tableName batchSrc =
  rows (jobRowCodec tableName) (insertJobsBatchBase schema tableName batchSrc [text|RETURNING ${jobColumns}|])

-- | 'insertJobsBatchBase' with no @RETURNING@.
insertJobsBatchSQL_ :: SchemaName -> TableName -> Query () -> Query ()
insertJobsBatchSQL_ schema tableName batchSrc =
  insertJobsBatchBase schema tableName batchSrc ""

-- | Batch insert with dedup and replaceable-job handling, plus a caller's @RETURNING@.
insertJobsBatchBase :: SchemaName -> TableName -> Query () -> Text -> Query ()
insertJobsBatchBase schema tableName batchSrc returning =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      guard = replaceableGuard tbl dlqTbl
      dedupSet = dedupUpdateSet tbl
   in [sql|
        INSERT INTO ${tbl} ${batchSrc}
        WHERE (src.parent_id IS NULL
            OR EXISTS (SELECT 1 FROM ${tbl} parent WHERE parent.id = src.parent_id))
        ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO UPDATE SET
          ${dedupSet}
        WHERE EXCLUDED.dedup_strategy = 'replace'
          AND ${guard}
        ${returning}
      |]

-- ---------------------------------------------------------------------------
-- Admin Operations
-- ---------------------------------------------------------------------------

-- | Fetch a job by id.
getJobByIdSQL :: Text -> Text -> Int64 -> Query (JobRead Value)
getJobByIdSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in rows
        (jobRowCodec tableName)
        [sql|
          SELECT ${jobColumns}
          FROM ${tbl}
          WHERE id = #{jobId :: CInt8}
        |]

-- | Fetch a job by its dedup key. The partial unique index guarantees at most one row.
getJobByDedupKeySQL :: Text -> Text -> Text -> Query (JobRead Value)
getJobByDedupKeySQL schema tableName key =
  let tbl = jobQueueTable schema tableName
   in rows
        (jobRowCodec tableName)
        [sql|
          SELECT ${jobColumns}
          FROM ${tbl}
          WHERE dedup_key = #{key :: CText}
        |]

-- | Delete a job by id. Refuses one with children, which 'cancelJobCascadeSQL' takes.
-- A deleted child with no siblings left resumes its parent for a completion round.
cancelJobSQL :: Text -> Text -> Int64 -> Query Int64
cancelJobSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        WITH cancel AS (
          DELETE FROM ${tbl}
          WHERE id = #{jobId :: CInt8}
            AND NOT EXISTS (SELECT 1 FROM ${tbl} child WHERE child.parent_id = #{jobId :: CInt8})
          RETURNING id, parent_id
        ),
        wake_parent AS (
          UPDATE ${tbl}
          SET suspended = FALSE, updated_at = NOW()
          WHERE id = (SELECT parent_id FROM cancel WHERE parent_id IS NOT NULL)
            AND suspended = TRUE
            AND NOT EXISTS (
              SELECT 1 FROM ${tbl} child
              WHERE child.parent_id = (SELECT parent_id FROM cancel WHERE parent_id IS NOT NULL)
                AND child.id NOT IN (SELECT id FROM cancel)
            )
          RETURNING id
        )
        SELECT (SELECT count(*) FROM cancel) AS @{result :: CInt8}
      |]

-- | @UNION ALL@ of @body@ over each job table, passing its raw name and schema-qualified reference.
unionAllOverQueueTables :: SchemaName -> [TableName] -> (TableName -> Text -> Text) -> Text
unionAllOverQueueTables schema tableNames body =
  T.intercalate " UNION ALL " (map (\tableName -> body tableName (jobQueueTable schema tableName)) tableNames)

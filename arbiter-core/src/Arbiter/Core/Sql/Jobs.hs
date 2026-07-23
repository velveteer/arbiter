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
  , allJobColumns
  , allDLQColumns
  , jobColsExceptError
  , jobColsExceptId
  , dlqCarriedCols
  , jobColumns
  , insertJobSQL
  , insertJobReplaceSQL
  , insertJobsBatchSQL
  , insertJobsBatchSQL_
  , insertJobsBatchBase
  , getJobByIdSQL
  , getJobByDedupKeySQL
  , cancelJobSQL
  , uuidLiteral
  , unionAllOverQueueTables
  ) where

import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID.Types (UUID)
import Data.UUID.Types qualified as UUID
import NeatInterpolation (text)

import Arbiter.Core.Admission (excludedAssignment)
import Arbiter.Core.Codec
  ( batchUnnest
  , codecColumns
  , dlqRowCodec
  , insertColumns
  , insertValues
  , jobRowCodec
  , writeColumnNames
  )
import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , jobQueueDLQTable
  , jobQueueTable
  )
import Arbiter.Core.Job.Types (JobStatus)

data JobFilter
  = FilterGroupKey Text
  | FilterParentId Int64
  | FilterRootsOnly
  | FilterStatus JobStatus
  | FilterId Int64
  | FilterJobId Int64
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

-- | Sortable columns on the archive table. @ArchiveId@ is the archive primary
-- key (distinct from the original job id, which is @ArchiveJobId@).
data ArchiveSortColumn
  = ArchiveId
  | ArchiveCompletedAt
  | ArchiveInsertedAt
  | ArchiveJobId
  | ArchiveAttempts
  | ArchiveGroupKey
  | ArchiveParentId
  deriving stock (Bounded, Enum, Eq, Show)

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

sortDirSql :: SortDir -> Text
sortDirSql SortAsc = "ASC"
sortDirSql SortDesc = "DESC"

-- | A throttle-deferred job: live marker, still parked. Shared by status, count,
-- and wake so they cannot drift. Claiming clears the marker, excluding running jobs.
throttledPredicateSQL :: Text
throttledPredicateSQL =
  "throttled_until > NOW() AND not_visible_until > NOW()"

-- | Sole definition of the derived job status. Its string values must match 'jobStatusToText'.
jobStatusCaseSQL :: Text
jobStatusCaseSQL =
  [text|
    CASE
      WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
      WHEN suspended THEN 'suspended'
      WHEN ${throttledPredicateSQL} THEN 'throttled'
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
  = -- | Column is NOT NULL. Emit no NULLS clause.
    NullsNotApplicable
  | -- | NULL is unmodelled / "absent". Always sort last.
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

archiveColumnNulls :: ArchiveSortColumn -> NullsBehavior
archiveColumnNulls = \case
  ArchiveId -> NullsNotApplicable
  ArchiveCompletedAt -> NullsNotApplicable
  ArchiveInsertedAt -> NullsNotApplicable
  ArchiveJobId -> NullsNotApplicable
  ArchiveAttempts -> NullsNotApplicable
  ArchiveGroupKey -> NullsAsAbsent
  ArchiveParentId -> NullsAsAbsent

-- | Build an ORDER BY clause from a typed sort spec. NULL placement is
-- column-specific via @nullsFn@. A stable @id@ tie-breaker in the primary
-- sort's direction is appended unless the sort column is @idCol@ itself.
buildOrderBy :: (Eq a) => (a -> Text) -> (a -> NullsBehavior) -> a -> a -> Maybe a -> Maybe SortDir -> Text
buildOrderBy nameFn nullsFn defCol idCol mCol mDir =
  let col = fromMaybe defCol mCol
      dir = fromMaybe SortDesc mDir
      dirText = sortDirSql dir
      tieBreaker = if col == idCol then "" else ", id " <> dirText
   in nameFn col <> " " <> dirText <> nullsClause (nullsFn col) dir <> tieBreaker

-- | ORDER BY for the jobs table. Defaults to @id DESC@.
buildJobsOrderBy :: Maybe JobSortColumn -> Maybe SortDir -> Text
buildJobsOrderBy = buildOrderBy jobSortColumnName jobColumnNulls JsId JsId

-- | ORDER BY for the DLQ table. Defaults to @failed_at DESC@.
buildDLQOrderBy :: Maybe DLQSortColumn -> Maybe SortDir -> Text
buildDLQOrderBy = buildOrderBy dlqSortColumnName dlqColumnNulls DlqFailedAt DlqId

-- | ORDER BY for the archive table. Defaults to @completed_at DESC@.
buildArchiveOrderBy :: Maybe ArchiveSortColumn -> Maybe SortDir -> Text
buildArchiveOrderBy = buildOrderBy archiveSortColumnName archiveColumnNulls ArchiveCompletedAt ArchiveId

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

-- | All job read columns except @id@, comma-separated. Used for the archive
-- INSERT, where the main table's @id@ becomes the archive's @job_id@ and every
-- other read column is copied verbatim.
jobColsExceptId :: Text
jobColsExceptId = T.intercalate ", " (drop 1 allJobColumns)

-- | Job columns carried through a DLQ round-trip: the read columns plus write-only rate_limit_cost.
dlqCarriedCols :: Text
dlqCarriedCols = jobColsExceptError <> ", rate_limit_cost"

-- | Standard job column list (for SELECT and RETURNING)
jobColumns :: Maybe Text -> Text
jobColumns mAlias = T.intercalate ", " $ map withAlias allJobColumns
  where
    withAlias name = maybe name (\alias -> alias <> "." <> name) mAlias

-- | @DO UPDATE SET@ body for a replace-dedup upsert. Copies each writable column
-- from the excluded row, then re-arms the replaced job for a fresh run.
dedupUpdateSet :: Text
dedupUpdateSet = T.intercalate ", " (copied <> rearm)
  where
    -- dedup_key is the conflict key. attempts and last_error are re-armed below.
    copied = map excludedAssignment (filter (`notElem` ["dedup_key", "attempts", "last_error"]) writeColumnNames)
    rearm =
      [ "attempts = 0"
      , "last_error = NULL"
      , "updated_at = NOW()"
      , "throttled_until = NULL"
      , "last_attempted_at = NULL"
      , "claimed_by = NULL"
      ]

-- | Guard: an existing row may be replaced only if idle and childless.
replaceableGuard :: Text -> Text -> Text
replaceableGuard tbl dlqTbl =
  [text|
    (${tbl}.attempts = 0
      OR ${tbl}.not_visible_until IS NULL
      OR ${tbl}.not_visible_until <= NOW()
      OR ${tbl}.last_error IS NOT NULL)
      AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ${tbl}.id)
      AND NOT EXISTS (SELECT 1 FROM ${dlqTbl} d WHERE d.parent_id = ${tbl}.id)
  |]

-- | Insert a job. Parameters bind the INSERT column list in order.
insertJobSQL :: Text -> Text -> Text
insertJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in [text|
        INSERT INTO ${tbl} (${insertColumns})
        VALUES (${insertValues})
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
-- Parameters bind the INSERT column list in order, as in 'insertJobSQL'.
insertJobReplaceSQL :: Text -> Text -> Text
insertJobReplaceSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      columns = jobColumns Nothing
      guard = replaceableGuard tbl dlqTbl
   in [text|
        INSERT INTO ${tbl} (${insertColumns})
        VALUES (${insertValues})
        ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO UPDATE SET
          ${dedupUpdateSet}
        WHERE ${guard}
        RETURNING ${columns}
      |]

-- | SQL template for batch inserting jobs using array parameters
--
-- Uses unnest to expand parallel arrays into rows. Supports dedup keys:
-- jobs with @IgnoreDuplicate@ are silently skipped on conflict, jobs with
-- @ReplaceDuplicate@ update the existing row (unless actively in-flight).
--
-- Parameters are one array per @unnest@ column, in that order.
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
      guard = replaceableGuard tbl dlqTbl
   in [text|
        INSERT INTO ${tbl} (${insertColumns})
        SELECT ${insertColumns}
        FROM (
          SELECT ${batchUnnest}
        ) src
        WHERE (src.parent_id IS NULL
            OR EXISTS (SELECT 1 FROM ${tbl} p WHERE p.id = src.parent_id))
        ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO UPDATE SET
          ${dedupUpdateSet}
        WHERE EXCLUDED.dedup_strategy = 'replace'
          AND ${guard}
        ${returning}
      |]

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

-- | Render a 'Maybe UUID' as a SQL literal: NULL or 'uuid-text'::uuid.
uuidLiteral :: Maybe UUID -> Text
uuidLiteral Nothing = "NULL"
uuidLiteral (Just u) = "'" <> UUID.toText u <> "'::uuid"

-- | @UNION ALL@ of @body@ over each job table, passing its raw name and schema-qualified reference.
unionAllOverQueueTables :: SchemaName -> [TableName] -> (TableName -> Text -> Text) -> Text
unionAllOverQueueTables schema tableNames body =
  T.intercalate " UNION ALL " (map (\t -> body t (jobQueueTable schema t)) tableNames)

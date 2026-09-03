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
  , jobColsExceptId
  , dlqCarriedCols
  , requeuedCols
  , enqueuedAgainCols
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

import Data.Aeson (Value)
import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)
import Data.UUID.Types qualified as UUID
import NeatInterpolation (text)

import Arbiter.Core.Admission (excludedAssignment)
import Arbiter.Core.Codec
  ( codecColumns
  , dlqRowCodec
  , jobRowCodec
  , writeColumnNames
  )
import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , jobQueueDLQTable
  , jobQueueTable
  )
import Arbiter.Core.Job.Types (JobRead, JobStatus)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, mwhen, rows)

-- | A narrowing predicate on a job listing. A predicate names a column its table
-- has, so @FilterJobId@ and the @completed_at@ range only suit the DLQ and archive.
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

-- | A throttle-deferred job: live marker, still parked. Shared by status, count,
-- and wake so they cannot drift. Claiming clears the marker, excluding running jobs.
throttledPredicateSQL :: Text
throttledPredicateSQL =
  "throttled_until > NOW() AND not_visible_until > NOW()"

-- | Sole definition of the derived job status. Its string values must match 'Arbiter.Core.Job.Status.jobStatusToText'.
jobStatusCaseSQL :: Text
jobStatusCaseSQL =
  [text|
    CASE
      WHEN cancel_requested_at IS NOT NULL THEN 'cancelled'
      WHEN suspended THEN 'suspended'
      WHEN ${throttledPredicateSQL} THEN 'throttled'
      WHEN claimed_by IS NULL AND attempts > 0 AND not_visible_until IS NOT NULL AND not_visible_until > NOW() THEN 'backoff'
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

-- | List filtered jobs without the derived status, for callers that don't need it.
listJobsFilteredSQL :: Text -> Text -> Query () -> Text -> Int64 -> Int64 -> Query (JobRead Value)
listJobsFilteredSQL schema tableName whereFrag orderBy limit offset =
  let tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in rows
        (jobRowCodec tableName)
        [sql|
          SELECT ${columns}
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

-- | Count filtered jobs, through the status subquery so status filters work.
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
      columns = T.intercalate ", " allDLQColumns
   in rows
        (dlqRowCodec tableName)
        [sql|
          SELECT ${columns}
          FROM ${dlqTbl}
          ${whereFrag}
          ORDER BY ${orderBy}
          LIMIT #{limit :: CInt8} OFFSET #{offset :: CInt8}
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

-- | Build an ORDER BY clause from a typed sort spec. NULL placement is
-- column-specific via @nullsFn@. A stable @id@ tie-breaker in the primary
-- sort's direction is appended unless the sort column is @idCol@ itself.
buildOrderBy :: (Eq a) => (a -> Text) -> (a -> NullsBehavior) -> a -> a -> Maybe a -> Maybe SortDir -> Text
buildOrderBy nameFn nullsFn defCol idCol mCol mDir =
  let col = fromMaybe defCol mCol
      dir = fromMaybe SortDesc mDir
      dirText = sortDirSql dir
      tieBreaker = mwhen (col /= idCol) (", id " <> dirText)
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

-- | Count DLQ jobs under a dynamic WHERE.
countDLQFilteredSQL :: Text -> Text -> Query () -> Query Int64
countDLQFilteredSQL schema tableName whereFrag =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|SELECT COUNT(*) AS @{count :: CInt8} FROM ${dlqTbl} ${whereFrag}|]

-- | Every job read column, in codec order.
allJobColumns :: [Text]
allJobColumns = codecColumns (jobRowCodec "")

-- | DLQ-specific fields followed by all job fields. The DLQ uses @job_id@ for
-- the main-table @id@.
-- @drop 1 allJobColumns@ drops the @id@ column, replaced by @job_id@ in the DLQ table.
allDLQColumns :: [Text]
allDLQColumns = codecColumns (dlqRowCodec "")

-- | All job columns except @id@ and @last_error@.
jobColsExceptErrorColumns :: [Text]
jobColsExceptErrorColumns = filter (/= "last_error") (drop 1 allJobColumns)

-- | All job read columns except @id@, comma-separated. Used for the archive
-- INSERT, where the main table's @id@ becomes the archive's @job_id@ and every
-- other read column is copied verbatim.
jobColsExceptId :: Text
jobColsExceptId = aliasedCols Nothing (drop 1 allJobColumns)

-- | Job columns carried through a DLQ round-trip: the read columns plus write-only rate_limit_cost.
dlqCarriedCols :: Text
dlqCarriedCols = aliasedCols Nothing dlqCarriedColumns

dlqCarriedColumns :: [Text]
dlqCarriedColumns = jobColsExceptErrorColumns <> ["rate_limit_cost"]

-- | Job columns a DLQ retry carries back to the main table, optionally aliased. The
-- rest are re-armed by the retry itself: a fresh attempt count, a bumped claim token,
-- a recomputed suspended flag, no claim, no dedup key, and no timestamps of the failed run.
requeuedCols :: Maybe Text -> Text
requeuedCols mAlias = aliasedCols mAlias requeuedColumns

-- | 'requeuedCols' for an archive re-enqueue, which starts a standalone job and so
-- leaves the parent link behind too.
enqueuedAgainCols :: Text
enqueuedAgainCols = aliasedCols Nothing (filter (`notElem` ["parent_id", "parent_state"]) requeuedColumns)

-- | The columns a DLQ retry carries back to the main table: every read column it does
-- not re-arm, plus write-only rate_limit_cost.
requeuedColumns :: [Text]
requeuedColumns = filter (`notElem` reArmedColumns) allJobColumns <> ["rate_limit_cost"]

-- | Columns that a requeue sets to new values.
reArmedColumns :: [Text]
reArmedColumns =
  [ "id"
  , "inserted_at"
  , "updated_at"
  , "attempts"
  , "last_error"
  , "last_attempted_at"
  , "not_visible_until"
  , "dedup_key"
  , "dedup_strategy"
  , "suspended"
  , "claimed_by"
  , "claim_seq"
  ]

-- | The job column list, for SELECT and RETURNING.
jobColumns :: Maybe Text -> Text
jobColumns mAlias = aliasedCols mAlias allJobColumns

-- | Comma-separated column list, each name qualified by @alias@ when given.
aliasedCols :: Maybe Text -> [Text] -> Text
aliasedCols mAlias = T.intercalate ", " . map withAlias
  where
    withAlias name = maybe name (\alias -> alias <> "." <> name) mAlias

-- | @DO UPDATE SET@ body for a replace-dedup upsert. Copies each writable column
-- from the excluded row, then re-arms the replaced job for a fresh run.
dedupUpdateSet :: Text -> Text
dedupUpdateSet tbl = T.intercalate ", " (copied <> rearm)
  where
    -- dedup_key is the conflict key. attempts, claim_seq and last_error are re-armed below.
    copied = map excludedAssignment (filter (`notElem` ["dedup_key", "attempts", "claim_seq", "last_error"]) writeColumnNames)
    rearm =
      [ "attempts = 0"
      , "claim_seq = " <> tbl <> ".claim_seq + 1"
      , "last_error = NULL"
      , "updated_at = NOW()"
      , "throttled_until = NULL"
      , "last_attempted_at = NULL"
      , "claimed_by = NULL"
      ]

-- | Guard: an existing row may be replaced only if idle, unflagged and childless.
replaceableGuard :: Text -> Text -> Text
replaceableGuard tbl dlqTbl =
  [text|
    (${tbl}.attempts = 0
      OR ${tbl}.claimed_by IS NULL
      OR ${tbl}.not_visible_until IS NULL
      OR ${tbl}.not_visible_until <= NOW())
      AND ${tbl}.cancel_requested_at IS NULL
      AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ${tbl}.id)
      AND NOT EXISTS (SELECT 1 FROM ${dlqTbl} d WHERE d.parent_id = ${tbl}.id)
  |]

-- | Insert a job. The write fragment carries the column list and parameters.
insertJobSQL :: SchemaName -> TableName -> Query () -> Query (JobRead Value)
insertJobSQL schema tableName valuesFrag =
  let tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in rows
        (jobRowCodec tableName)
        [sql|
          INSERT INTO ${tbl} ${valuesFrag}
          ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO NOTHING
          RETURNING ${columns}
        |]

-- | Insert under the replace dedup strategy, replacing an existing job unless it is
-- in-flight or has children in the main queue or the DLQ. @ON CONFLICT DO UPDATE@ fires
-- the groups UPDATE trigger, whose transition tables carry the old and new rows, so a
-- cross-group move is maintained too.
insertJobReplaceSQL :: SchemaName -> TableName -> Query () -> Query (JobRead Value)
insertJobReplaceSQL schema tableName valuesFrag =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      columns = jobColumns Nothing
      guard = replaceableGuard tbl dlqTbl
      dedupSet = dedupUpdateSet tbl
   in rows
        (jobRowCodec tableName)
        [sql|
          INSERT INTO ${tbl} ${valuesFrag}
          ON CONFLICT (dedup_key) WHERE dedup_key IS NOT NULL DO UPDATE SET
            ${dedupSet}
          WHERE ${guard}
          RETURNING ${columns}
        |]

-- | Batch insert over @unnest@ed parallel arrays. An ignore-dedup job is skipped on
-- conflict, a replace-dedup job updates the existing row unless it is in-flight.
insertJobsBatchSQL :: SchemaName -> TableName -> Query () -> Query (JobRead Value)
insertJobsBatchSQL schema tableName batchSrc =
  rows (jobRowCodec tableName) $
    insertJobsBatchBase schema tableName batchSrc ("RETURNING " <> jobColumns Nothing)

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
            OR EXISTS (SELECT 1 FROM ${tbl} p WHERE p.id = src.parent_id))
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
      columns = jobColumns Nothing
   in rows
        (jobRowCodec tableName)
        [sql|
          SELECT ${columns}
          FROM ${tbl}
          WHERE id = #{jobId :: CInt8}
        |]

-- | Fetch a job by its dedup key. The partial unique index guarantees at most one row.
getJobByDedupKeySQL :: Text -> Text -> Text -> Query (JobRead Value)
getJobByDedupKeySQL schema tableName key =
  let tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in rows
        (jobRowCodec tableName)
        [sql|
          SELECT ${columns}
          FROM ${tbl}
          WHERE dedup_key = #{key :: CText}
        |]

-- | Delete a job by id, refusing one with children (@cancelJobCascadeSQL@ takes those).
-- A deleted child with no siblings left resumes its parent for a completion round.
cancelJobSQL :: Text -> Text -> Int64 -> Query Int64
cancelJobSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        WITH cancel AS (
          DELETE FROM ${tbl}
          WHERE id = #{jobId :: CInt8}
            AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = #{jobId :: CInt8})
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
        SELECT (SELECT count(*) FROM cancel) AS @{result :: CInt8}
      |]

-- | Render a 'Maybe UUID' as a SQL literal: NULL or 'uuid-text'::uuid.
uuidLiteral :: Maybe UUID -> Text
uuidLiteral Nothing = "NULL"
uuidLiteral (Just u) = "'" <> UUID.toText u <> "'::uuid"

-- | @UNION ALL@ of @body@ over each job table, passing its raw name and schema-qualified reference.
unionAllOverQueueTables :: SchemaName -> [TableName] -> (TableName -> Text -> Text) -> Text
unionAllOverQueueTables schema tableNames body =
  T.intercalate " UNION ALL " (map (\t -> body t (jobQueueTable schema t)) tableNames)

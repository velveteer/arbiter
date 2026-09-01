{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | SQL generation functions for job queue schemas. No database execution happens here.
module Arbiter.Core.Job.Schema
  ( -- * Name Types
    SchemaName
  , TableName

    -- * Schema Creation
  , createSchemaSQL
  , defaultSchemaName

    -- * Table Creation SQL
  , createJobQueueTableSQL
  , createJobQueueDLQTableSQL
  , createJobQueueArchiveTableSQL
  , queueTableNames
  , addTraceContextColumnSQL
  , addClaimSeqColumnSQL
  , setMaxAttemptsDefaultSQL

    -- * Index Creation SQL
  , createJobQueueGroupKeyIndexSQL
  , createJobQueueGroupRetriedIndexSQL
  , createJobQueueUngroupedReadyRankingIndexSQL
  , createJobQueueUngroupedDueIndexSQL
  , migrateUngroupedReadySplitIndexesSQL
  , createDLQGroupKeyIndexSQL
  , createDLQFailedAtIndexSQL
  , createDLQParentIdIndexSQL
  , createArchiveCompletedAtIndexSQL
  , createArchiveExpiresAtIndexSQL
  , createArchiveJobIdIndexSQL
  , createArchiveParentIdIndexSQL
  , createArchiveGroupKeyIndexSQL
  , createDedupKeyIndexSQL
  , createParentIdIndexSQL

    -- * NOTIFY Trigger SQL
  , createNotifyFunctionSQL
  , createNotifyTriggerSQL
  , dropNotifyTriggerSQL
  , dropNotifyFunctionSQL

    -- * Event Streaming Trigger SQL
  , createEventStreamingFunctionSQL
  , createEventStreamingTriggersSQL
  , dropEventStreamingFunctionSQL
  , dropEventStreamingTriggersSQL

    -- * Notification Channel Helpers
  , notificationChannelForTable
  , eventStreamingChannel
  , pauseNotifyChannel
  , pauseNotifyChannelPrefix
  , cancelNotifyChannel
  , cancelNotifyChannelPrefix
  , cronRunNotifyChannel

    -- * Trigger / Function Name Helpers
  , notifyFunctionName
  , notifyTriggerName
  , eventStreamingFunctionName
  , eventStreamingTriggerName
  , eventStreamingDLQTriggerName
  , legacyEventStreamingTriggers
  , notifyObjectComment
  , notifyObjectCommentPrefix
  , notifyAdoptedObjectComment
  , eventStreamingObjectComment
  , eventStreamingObjectCommentPrefix
  , eventStreamingAdoptedObjectComment

    -- * Table Name Helpers
  , qualifiedTable
  , jobQueueTable
  , jobQueueDLQTable
  , jobQueueArchiveTable
  , jobQueueResultsTable
  , jobQueueGroupsTable

    -- * Results Table
  , createResultsTableSQL

    -- * Groups Table
  , createGroupsTableSQL
  , migrateGroupsReadyRankingSQL
  , createGroupsEmptiedIndexSQL

    -- * Groups Trigger SQL
  , createGroupsTriggerFunctionsSQL
  , maintenanceFunctionNames
  , createGroupsTriggersSQL
  , createMaintenanceTriggersSQL
  , statementTriggerSQL
  , inFlightPredicate
  , groupAggregates
  ) where

import Data.Bool (bool)
import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Job.Types (defaultMaxAttempts)
import Arbiter.Core.SqlLiterals (quoteIdentifier)

-- | PostgreSQL schema name, e.g. @"arbiter"@.
type SchemaName = Text

-- | Unqualified table name within a schema, e.g. @"email_jobs"@.
type TableName = Text

-- | The schema arbiter's tables live in by default, keeping them out of @public@.
defaultSchemaName :: SchemaName
defaultSchemaName = "arbiter"

-- | A table's own job-arrival NOTIFY channel: @\"email_jobs\"@ -> @\"email_jobs_created\"@.
notificationChannelForTable :: TableName -> Text
notificationChannelForTable tableName = tableName <> "_created"

-- | Channel name used by the event streaming (SSE) system.
eventStreamingChannel :: Text
eventStreamingChannel = "arbiter_job_events"

-- | Prefix for per-queue pause NOTIFY channels. The full channel name appends
-- the queue. Exported so SQL templates can build the channel dynamically from
-- @queue_name@ returned by a CTE.
pauseNotifyChannelPrefix :: SchemaName -> Text
pauseNotifyChannelPrefix schemaName = "arbiter_pause_" <> schemaName <> "_"

-- | Per-queue NOTIFY channel for pause/resume changes. Workers LISTEN on the
-- channel for their own queue to reconcile faster than the heartbeat poll
-- cadence.
pauseNotifyChannel :: SchemaName -> Text -> Text
pauseNotifyChannel schemaName queueName =
  T.take 63 $ pauseNotifyChannelPrefix schemaName <> queueName

-- | Prefix for per-queue cancel NOTIFY channels. See 'cancelNotifyChannel'.
cancelNotifyChannelPrefix :: SchemaName -> Text
cancelNotifyChannelPrefix schemaName = "arbiter_cancel_" <> schemaName <> "_"

-- | Per-queue NOTIFY channel for force-cancel signals. The payload identifies
-- the target worker and job. Only the matching worker reacts.
cancelNotifyChannel :: SchemaName -> Text -> Text
cancelNotifyChannel schemaName queueName =
  T.take 63 $ cancelNotifyChannelPrefix schemaName <> queueName

-- | Per-schema NOTIFY channel for manual cron run-now requests.
cronRunNotifyChannel :: SchemaName -> Text
cronRunNotifyChannel schemaName = T.take 63 $ "arbiter_cron_run_" <> schemaName

-- | Per-table NOTIFY trigger function name.
notifyFunctionName :: TableName -> Text
notifyFunctionName tableName = "notify_" <> tableName <> "_created"

-- | Per-table NOTIFY trigger name.
notifyTriggerName :: TableName -> Text
notifyTriggerName tableName = tableName <> "_notify_trigger"

-- | Shared event streaming trigger function name (one per schema).
eventStreamingFunctionName :: Text
eventStreamingFunctionName = "notify_job_event"

-- | Per-table event streaming trigger name.
eventStreamingTriggerName :: TableName -> Text
eventStreamingTriggerName tableName = "notify_job_event_" <> tableName

-- | Per-table DLQ event streaming trigger name.
eventStreamingDLQTriggerName :: TableName -> Text
eventStreamingDLQTriggerName tableName = "notify_job_event_" <> tableName <> "_dlq"

-- | Event-streaming trigger names arbiter generated before the per-queue names, each
-- paired with whether it sits on the DLQ table.
legacyEventStreamingTriggers :: [(Text, Bool)]
legacyEventStreamingTriggers =
  [ ("notify_job_insert", False)
  , ("notify_job_update", False)
  , ("notify_job_delete", False)
  , ("notify_dlq_insert", True)
  ]

-- | Ownership marker stamped on every notify function and trigger arbiter installs.
-- Sweeps match 'notifyObjectCommentPrefix', so an unmarked object is never dropped.
-- A trigger is current when its comment equals this exact value, so bump the version
-- whenever 'createNotifyTriggerSQL' changes.
notifyObjectComment :: Text
notifyObjectComment = notifyObjectCommentPrefix <> "v1"

-- | The marker prefix identifying a notify object as arbiter's, across versions.
notifyObjectCommentPrefix :: Text
notifyObjectCommentPrefix = "arbiter:notify:"

-- | Marker stamped on notify objects installed before arbiter marked them. Sweeps
-- match it and no trigger body is ever built with it, so an adopted trigger is rebuilt.
notifyAdoptedObjectComment :: Text
notifyAdoptedObjectComment = notifyObjectCommentPrefix <> "adopted"

-- | Ownership marker stamped on every event-streaming function and trigger arbiter
-- installs. Bump the version whenever 'createEventStreamingTriggersSQL' changes.
eventStreamingObjectComment :: Text
eventStreamingObjectComment = eventStreamingObjectCommentPrefix <> "v1"

-- | The marker prefix identifying an event-streaming object as arbiter's, across versions.
eventStreamingObjectCommentPrefix :: Text
eventStreamingObjectCommentPrefix = "arbiter:event-stream:"

-- | Marker stamped on event-streaming objects installed before arbiter marked them.
-- See 'notifyAdoptedObjectComment'.
eventStreamingAdoptedObjectComment :: Text
eventStreamingAdoptedObjectComment = eventStreamingObjectCommentPrefix <> "adopted"

-- | Any schema-qualified table: @qualifiedTable "arbiter" "arbiter_workers"@ -> @"arbiter"."arbiter_workers"@
qualifiedTable :: SchemaName -> TableName -> Text
qualifiedTable schemaName tableName = quoteIdentifier schemaName <> "." <> quoteIdentifier tableName

-- | Qualified table name: @jobQueueTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs"@
jobQueueTable :: SchemaName -> TableName -> Text
jobQueueTable = qualifiedTable

-- | Qualified DLQ table name: @jobQueueDLQTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs_dlq"@
jobQueueDLQTable :: SchemaName -> TableName -> Text
jobQueueDLQTable schemaName tableName = qualifiedTable schemaName (tableName <> dlqSuffix)

-- | Qualified archive table name: @jobQueueArchiveTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs_archive"@
jobQueueArchiveTable :: SchemaName -> TableName -> Text
jobQueueArchiveTable schemaName tableName = qualifiedTable schemaName (tableName <> archiveSuffix)

-- | Backfill NULL @max_attempts@ to the default and set the column default.
-- Left nullable for rolling-deploy safety (old code may still insert NULL).
setMaxAttemptsDefaultSQL :: SchemaName -> TableName -> Text
setMaxAttemptsDefaultSQL schemaName tableName =
  let tbl = jobQueueTable schemaName tableName
      dma = T.pack (show defaultMaxAttempts)
   in [text|
        UPDATE ${tbl} SET max_attempts = ${dma} WHERE max_attempts IS NULL;
        ALTER TABLE ${tbl} ALTER COLUMN max_attempts SET DEFAULT ${dma};
      |]

-- | Qualified results table name: @jobQueueResultsTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs_results"@
jobQueueResultsTable :: Text -> Text -> Text
jobQueueResultsTable schemaName tableName = qualifiedTable schemaName (tableName <> resultsSuffix)

-- | Qualified groups table name: @jobQueueGroupsTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs_groups"@
jobQueueGroupsTable :: Text -> Text -> Text
jobQueueGroupsTable schemaName tableName = qualifiedTable schemaName (tableName <> groupsSuffix)

dlqSuffix, archiveSuffix, resultsSuffix, groupsSuffix :: Text
dlqSuffix = "_dlq"
archiveSuffix = "_archive"
resultsSuffix = "_results"
groupsSuffix = "_groups"

-- | A queue's own table and its companions, unqualified and unquoted, for callers that
-- match on @pg_catalog@ relnames.
queueTableNames :: TableName -> [TableName]
queueTableNames tableName = tableName : map (tableName <>) [dlqSuffix, archiveSuffix, resultsSuffix, groupsSuffix]

-- | Create the schema arbiter's tables live in.
createSchemaSQL :: SchemaName -> Text
createSchemaSQL schemaName =
  "CREATE SCHEMA IF NOT EXISTS " <> quoteIdentifier schemaName <> ";"

-- | The job columns the queue and DLQ tables share. Checksummed by the create-table
-- migration, so a new column ships as its own ALTER script.
jobColumns :: [Text]
jobColumns =
  [ "  id BIGSERIAL PRIMARY KEY,"
  , "  payload JSONB NOT NULL,"
  , "  group_key TEXT,"
  , "  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
  , "  updated_at TIMESTAMPTZ,"
  , "  last_attempted_at TIMESTAMPTZ,"
  , "  not_visible_until TIMESTAMPTZ,"
  , "  attempts INT NOT NULL DEFAULT 0,"
  , "  last_error TEXT,"
  , "  priority INT NOT NULL DEFAULT 0,"
  , "  dedup_key TEXT,"
  , "  dedup_strategy TEXT,"
  , "  max_attempts INT,"
  , "  parent_id BIGINT,"
  , "  parent_state JSONB,"
  , "  suspended BOOLEAN NOT NULL DEFAULT FALSE"
  ]

-- | @ADD COLUMN IF NOT EXISTS@ over a queue's three job tables, one statement each.
addJobColumnsSQL :: Text -> Text -> [Text] -> Text
addJobColumnsSQL schemaName tableName columns =
  T.unlines [alter (tbl schemaName tableName) | tbl <- [jobQueueTable, jobQueueDLQTable, jobQueueArchiveTable]]
  where
    alter table = "ALTER TABLE " <> table <> " " <> T.intercalate ", " (map addColumn columns) <> ";"
    addColumn column = "ADD COLUMN IF NOT EXISTS " <> column

-- | Add the W3C trace-context columns to a queue's three job tables.
addTraceContextColumnSQL :: Text -> Text -> Text
addTraceContextColumnSQL schemaName tableName =
  addJobColumnsSQL schemaName tableName ["traceparent TEXT", "tracestate TEXT"]

-- | Add the per-claim token column to a queue's three job tables.
addClaimSeqColumnSQL :: Text -> Text -> Text
addClaimSeqColumnSQL schemaName tableName =
  addJobColumnsSQL schemaName tableName ["claim_seq BIGINT NOT NULL DEFAULT 0"]

-- | 'jobColumns' for the DLQ table, with @job_id@ in place of @id@.
jobColumnsForDLQ :: Text
jobColumnsForDLQ =
  T.unlines
    [ "  id BIGSERIAL PRIMARY KEY,"
    , "  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  job_id BIGINT NOT NULL,"
    ]
    <> T.unlines (drop 1 jobColumns)

-- | Create a queue's main job table, holding its pending and in-progress jobs.
createJobQueueTableSQL :: Text -> Text -> Text
createJobQueueTableSQL schemaName tableName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> jobQueueTable schemaName tableName <> " ("
    , T.unlines jobColumns
    , ") WITH (fillfactor = 70);"
    ]

-- | Create a queue's DLQ table, where a job that runs out of attempts lands as a full
-- snapshot plus its failure metadata.
createJobQueueDLQTableSQL :: Text -> Text -> Text
createJobQueueDLQTableSQL schemaName tableName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> jobQueueDLQTable schemaName tableName <> " ("
    , jobColumnsForDLQ
    , ");"
    ]

-- | Archive table columns: every Job read column (@job_id@ for @id@) plus the write-only @rate_limit_cost@, the completed root job's @result@, and the @completed_at@/@archive_expires_at@ metadata.
jobColumnsForArchive :: Text
jobColumnsForArchive =
  T.unlines
    [ "  id BIGSERIAL PRIMARY KEY,"
    , "  completed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  archive_expires_at TIMESTAMPTZ NOT NULL,"
    , "  job_id BIGINT NOT NULL,"
    , "  claimed_by UUID,"
    , "  archive_for INT,"
    , "  rate_limit_key TEXT,"
    , "  rate_limit_prefix TEXT,"
    , "  rate_limit_cost DOUBLE PRECISION,"
    , "  concurrency_key TEXT,"
    , "  concurrency_prefix TEXT,"
    , "  result JSONB,"
    ]
    <> T.unlines (drop 1 jobColumns)

-- | Create the completed-job archive table. Logged, since it is the only copy of
-- completed-job history and must survive an unclean shutdown and reach replicas.
createJobQueueArchiveTableSQL :: Text -> Text -> Text
createJobQueueArchiveTableSQL schemaName tableName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> jobQueueArchiveTable schemaName tableName <> " ("
    , jobColumnsForArchive
    , ");"
    ]

-- | Index on archive @completed_at@ for the most-recent-first history listing.
createArchiveCompletedAtIndexSQL :: Text -> Text -> Text
createArchiveCompletedAtIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_archive_completed_at")
    , "ON " <> jobQueueArchiveTable schemaName tableName <> " (completed_at DESC);"
    ]

-- | Index on archive @archive_expires_at@. Drives the retention purge sweep.
createArchiveExpiresAtIndexSQL :: Text -> Text -> Text
createArchiveExpiresAtIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_archive_expires_at")
    , "ON " <> jobQueueArchiveTable schemaName tableName <> " (archive_expires_at);"
    ]

-- | Index on archive @job_id@ for by-id lookups (@getArchivedJobById@).
createArchiveJobIdIndexSQL :: Text -> Text -> Text
createArchiveJobIdIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_archive_job_id")
    , "ON " <> jobQueueArchiveTable schemaName tableName <> " (job_id);"
    ]

-- | Index on archive @parent_id@ for per-tree history lookups.
createArchiveParentIdIndexSQL :: Text -> Text -> Text
createArchiveParentIdIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_archive_parent_id")
    , "ON " <> jobQueueArchiveTable schemaName tableName <> " (parent_id)"
    , "WHERE parent_id IS NOT NULL;"
    ]

-- | Index on archive @group_key@ for per-group history lookups.
createArchiveGroupKeyIndexSQL :: Text -> Text -> Text
createArchiveGroupKeyIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_archive_group_key")
    , "ON " <> jobQueueArchiveTable schemaName tableName <> " (group_key);"
    ]

-- | Partial index over @(group_key, priority, id)@, read by the claim's LATERAL
-- subqueries and by the maintenance triggers recomputing a group's minima and
-- @in_flight_until@ without leaving the index.
createJobQueueGroupKeyIndexSQL :: Text -> Text -> Text
createJobQueueGroupKeyIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_group_key")
    , "ON " <> jobQueueTable schemaName tableName <> " (group_key, priority ASC, id ASC)"
    , "WHERE group_key IS NOT NULL;"
    ]

-- | Partial index over @(group_key, attempts DESC, priority, id)@ for retried rows,
-- read by the claim's group head gate. Retried rows rank ahead of the rest, so the
-- gate merges this run with the @(group_key, priority, id)@ scan and avoids sorting
-- the whole group.
createJobQueueGroupRetriedIndexSQL :: Text -> Text -> Text
createJobQueueGroupRetriedIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_group_retried")
    , "ON " <> jobQueueTable schemaName tableName <> " (group_key, attempts DESC, priority ASC, id ASC)"
    , "WHERE group_key IS NOT NULL AND attempts > 0;"
    ]

-- | Ranking index over ready ungrouped jobs only (@not_visible_until IS NULL AND
-- NOT suspended@). Scheduled/backoff/in-flight rows have @not_visible_until@ set
-- and suspended rows are excluded, so they are absent, and the claim's ordered
-- @LIMIT@ stops the scan at the first ready rows.
createJobQueueUngroupedReadyRankingIndexSQL :: Text -> Text -> Text
createJobQueueUngroupedReadyRankingIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_ungrouped_ready_ranking")
    , "ON " <> jobQueueTable schemaName tableName <> " (priority ASC, id ASC)"
    , "WHERE group_key IS NULL AND not_visible_until IS NULL AND NOT suspended;"
    ]

-- | Due-finder for ungrouped parked rows: range-scanned by @not_visible_until <=
-- NOW()@ to pick up scheduled/backoff jobs that have come due and expired leases,
-- without touching the future-dated tail.
createJobQueueUngroupedDueIndexSQL :: Text -> Text -> Text
createJobQueueUngroupedDueIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_ungrouped_due")
    , "ON " <> jobQueueTable schemaName tableName <> " (not_visible_until ASC)"
    , "WHERE group_key IS NULL AND not_visible_until IS NOT NULL AND NOT suspended;"
    ]

-- | Migration: replace the full ungrouped ranking index with the ready-only
-- ranking index plus the due-finder, so the claim splits ready and due instead
-- of walking the backlog through one @(priority, id)@ index.
migrateUngroupedReadySplitIndexesSQL :: Text -> Text -> Text
migrateUngroupedReadySplitIndexesSQL schemaName tableName =
  T.unlines
    [ "DROP INDEX IF EXISTS "
        <> quoteIdentifier schemaName
        <> "."
        <> quoteIdentifier ("idx_" <> tableName <> "_ungrouped_ranking")
        <> ";"
    , createJobQueueUngroupedReadyRankingIndexSQL schemaName tableName
    , createJobQueueUngroupedDueIndexSQL schemaName tableName
    ]

-- | Index on DLQ @group_key@, for per-group failure listings.
createDLQGroupKeyIndexSQL :: Text -> Text -> Text
createDLQGroupKeyIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_dlq_group_key")
    , "ON " <> jobQueueDLQTable schemaName tableName <> " (group_key);"
    ]

-- | Index on DLQ @failed_at@, for the most-recent-first listing.
createDLQFailedAtIndexSQL :: Text -> Text -> Text
createDLQFailedAtIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_dlq_failed_at")
    , "ON " <> jobQueueDLQTable schemaName tableName <> " (failed_at DESC);"
    ]

-- | Index on DLQ @parent_id@, for per-parent child lookups and counts.
createDLQParentIdIndexSQL :: Text -> Text -> Text
createDLQParentIdIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_dlq_parent_id")
    , "ON " <> jobQueueDLQTable schemaName tableName <> " (parent_id)"
    , "WHERE parent_id IS NOT NULL;"
    ]

-- | Unique index on @dedup_key@, which the dedup @ON CONFLICT@ resolves against.
createDedupKeyIndexSQL :: Text -> Text -> Text
createDedupKeyIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE UNIQUE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_dedup_key")
    , "ON " <> jobQueueTable schemaName tableName <> " (dedup_key)"
    , "WHERE dedup_key IS NOT NULL;"
    ]

-- | Partial index on @parent_id@, for per-parent child lookups.
createParentIdIndexSQL :: Text -> Text -> Text
createParentIdIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_parent_id")
    , "ON " <> jobQueueTable schemaName tableName <> " (parent_id)"
    , "WHERE parent_id IS NOT NULL;"
    ]

-- | Create a queue's results table, one row per child keyed by @(parent_id, child_id)@.
-- Its foreign key cascades, so acking the parent clears them.
createResultsTableSQL :: Text -> Text -> Text
createResultsTableSQL schemaName tableName =
  let resultsTbl = jobQueueResultsTable schemaName tableName
      mainTbl = jobQueueTable schemaName tableName
   in T.unlines
        [ "CREATE TABLE IF NOT EXISTS " <> resultsTbl <> " ("
        , "  parent_id BIGINT NOT NULL REFERENCES " <> mainTbl <> "(id) ON DELETE CASCADE,"
        , "  child_id BIGINT NOT NULL,"
        , "  result JSONB NOT NULL,"
        , "  PRIMARY KEY (parent_id, child_id)"
        , ");"
        ]

-- | Create a queue's groups table, one summary row per @group_key@ carrying the group's
-- precomputed minima, counts and @in_flight_until@. Maintained by the statement-level
-- AFTER triggers in 'createGroupsTriggerFunctionsSQL'.
createGroupsTableSQL :: Text -> Text -> Text
createGroupsTableSQL schemaName tableName =
  let groupsTbl = jobQueueGroupsTable schemaName tableName
   in T.unlines
        [ "CREATE TABLE IF NOT EXISTS " <> groupsTbl <> " ("
        , "  group_key TEXT PRIMARY KEY,"
        , "  min_priority INT NOT NULL DEFAULT 0,"
        , "  min_id BIGINT NOT NULL DEFAULT 0,"
        , "  job_count INT NOT NULL DEFAULT 0,"
        , "  in_flight_until TIMESTAMPTZ DEFAULT NULL"
        , ");"
        ]

-- | Add @ready_count@ and @next_due@ to the groups summary, make the ranking
-- index partial on ready rows, and add the @next_due@ due-finder.
migrateGroupsReadyRankingSQL :: Text -> Text -> Text
migrateGroupsReadyRankingSQL schemaName tableName =
  let groupsTbl = jobQueueGroupsTable schemaName tableName
      tbl = jobQueueTable schemaName tableName
      qidx suffix = quoteIdentifier ("idx_" <> tableName <> suffix)
   in T.unlines
        [ "ALTER TABLE " <> groupsTbl <> " ADD COLUMN IF NOT EXISTS ready_count INT NOT NULL DEFAULT 0;"
        , "ALTER TABLE " <> groupsTbl <> " ADD COLUMN IF NOT EXISTS next_due TIMESTAMPTZ;"
        , "UPDATE " <> groupsTbl <> " g SET"
        , "  min_priority = sub.mp, min_id = sub.mi, ready_count = COALESCE(sub.rc, 0), next_due = sub.nd"
        , "FROM ("
        , "  SELECT group_key,"
        , "    MIN(priority) AS mp,"
        , "    MIN(id) AS mi,"
        , "    COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended) AS rc,"
        , "    MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended) AS nd"
        , "  FROM " <> tbl <> " WHERE group_key IS NOT NULL GROUP BY group_key"
        , ") sub WHERE g.group_key = sub.group_key;"
        , "DROP INDEX IF EXISTS " <> quoteIdentifier schemaName <> "." <> qidx "_groups_ranking" <> ";"
        , "CREATE INDEX IF NOT EXISTS "
            <> qidx "_groups_ranking"
            <> " ON "
            <> groupsTbl
            <> " (min_priority ASC, min_id ASC) WHERE ready_count > 0 AND in_flight_until IS NULL;"
        , "CREATE INDEX IF NOT EXISTS "
            <> qidx "_groups_next_due"
            <> " ON "
            <> groupsTbl
            <> " (next_due ASC) WHERE next_due IS NOT NULL;"
        ]

-- | Index over the summary rows the maintenance triggers emptied in place.
createGroupsEmptiedIndexSQL :: Text -> Text -> Text
createGroupsEmptiedIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_groups_emptied")
    , "ON " <> jobQueueGroupsTable schemaName tableName <> " (group_key)"
    , "WHERE job_count = 0;"
    ]

-- ---------------------------------------------------------------------------
-- Groups Maintenance Triggers
-- ---------------------------------------------------------------------------

-- | The three trigger functions maintaining a queue's groups table, incrementally where
-- the transition tables allow it.
createGroupsTriggerFunctionsSQL :: Text -> Text -> Text
createGroupsTriggerFunctionsSQL schemaName tableName =
  let groupsTbl = jobQueueGroupsTable schemaName tableName
      tbl = jobQueueTable schemaName tableName
      baseName = "maintain_" <> tableName <> "_groups"
      (funcInsert, funcDelete, funcUpdate) = maintenanceFunctionNames schemaName baseName
      dd = "$$"
   in T.unlines
        [ groupsInsertFunction funcInsert groupsTbl dd
        , groupsDeleteFunction funcDelete groupsTbl tbl dd
        , groupsUpdateFunction funcUpdate groupsTbl tbl dd
        ]

-- | The qualified @<baseName>_{insert,delete,update}@ maintenance-function names.
maintenanceFunctionNames :: Text -> Text -> (Text, Text, Text)
maintenanceFunctionNames schemaName baseName =
  (func "_insert", func "_delete", func "_update")
  where
    func suffix = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> suffix)

-- | The @ON CONFLICT@ merge of newly grouped rows into an existing group summary.
groupsMergeSet :: Text -> Text
groupsMergeSet groupsTbl =
  [text|
    min_priority = CASE WHEN ${groupsTbl}.job_count = 0 THEN EXCLUDED.min_priority
      ELSE LEAST(${groupsTbl}.min_priority, EXCLUDED.min_priority) END,
    min_id = CASE WHEN ${groupsTbl}.job_count = 0 THEN EXCLUDED.min_id
      ELSE LEAST(${groupsTbl}.min_id, EXCLUDED.min_id) END,
    job_count = ${groupsTbl}.job_count + EXCLUDED.job_count,
    ready_count = ${groupsTbl}.ready_count + EXCLUDED.ready_count,
    next_due = LEAST(${groupsTbl}.next_due, EXCLUDED.next_due)
  |]

groupsInsertFunction :: Text -> Text -> Text -> Text
groupsInsertFunction funcName groupsTbl dd =
  let mergeSet = groupsMergeSet groupsTbl
      aggs = groupAggregates ""
   in [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM new_table WHERE group_key IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      -- Lock group rows in group_key order to avoid deadlock with concurrent triggers.
      PERFORM 1 FROM ${groupsTbl} g
      WHERE g.group_key IN (SELECT group_key FROM new_table WHERE group_key IS NOT NULL)
      ORDER BY g.group_key
      FOR UPDATE;

      INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count, ready_count, next_due)
      SELECT group_key, ${aggs}
      FROM new_table
      WHERE group_key IS NOT NULL
      GROUP BY group_key
      ORDER BY group_key
      ON CONFLICT (group_key) DO UPDATE SET
        ${mergeSet},
        in_flight_until = CASE WHEN ${groupsTbl}.in_flight_until <= NOW()
          THEN NULL ELSE ${groupsTbl}.in_flight_until END;

      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

-- | The group summary aggregates over job rows grouped by @group_key@. @col@ prefixes
-- each column.
groupAggregates :: Text -> Text
groupAggregates col =
  [text|MIN(${col}priority) AS min_priority, MIN(${col}id) AS min_id, COUNT(*) AS job_count, COUNT(*) FILTER (WHERE ${col}not_visible_until IS NULL AND NOT ${col}suspended) AS ready_count, MIN(${col}not_visible_until) FILTER (WHERE ${col}not_visible_until IS NOT NULL AND NOT ${col}suspended) AS next_due|]

-- | Whether a job still holds its group's in-flight slot. @col@ prefixes each column.
inFlightPredicate :: Text -> Text
inFlightPredicate col =
  col
    <> "not_visible_until > NOW() AND NOT "
    <> col
    <> "suspended AND ("
    <> col
    <> "attempts > 0 OR "
    <> col
    <> "throttled_until > NOW())"

-- | Ordering and visibility recomputed from the rows a group has left, aliased @t@.
survivingAggregates :: Text
survivingAggregates =
  [text|
    MIN(t.priority) AS new_min_priority,
    MIN(t.id) AS new_min_id,
    MIN(t.not_visible_until) FILTER (WHERE t.not_visible_until IS NOT NULL AND NOT t.suspended) AS new_next_due
  |]

-- | Decrement a group summary for removed rows, resetting an emptied group in place.
-- @removedRows@ aggregates them as @group_key@, @removed_count@, @removed_ready_count@
-- and @had_inflight@.
groupsRemoveUpdate :: Text -> Text -> Text -> Text
groupsRemoveUpdate groupsTbl tbl removedRows =
  let ifSurv = inFlightPredicate "t."
   in [text|
    UPDATE ${groupsTbl} g
    SET job_count = CASE WHEN sub.new_min_id IS NULL THEN 0
          ELSE GREATEST(0, g.job_count - sub.removed_count) END,
        min_priority = CASE WHEN sub.new_min_id IS NULL THEN 0
          ELSE sub.new_min_priority END,
        min_id = CASE WHEN sub.new_min_id IS NULL THEN 0
          ELSE sub.new_min_id END,
        ready_count = CASE WHEN sub.new_min_id IS NULL THEN 0
          ELSE GREATEST(0, g.ready_count - sub.removed_ready_count) END,
        next_due = CASE WHEN sub.new_min_id IS NULL THEN NULL
          ELSE sub.new_next_due END,
        in_flight_until = CASE
          WHEN sub.new_min_id IS NULL THEN NULL
          WHEN sub.had_inflight THEN sub.surviving_ift
          ELSE g.in_flight_until
        END
    FROM (
      SELECT d.group_key, d.removed_count, d.removed_ready_count, d.had_inflight,
        ${survivingAggregates},
        MAX(t.not_visible_until) FILTER (WHERE ${ifSurv}) AS surviving_ift
      FROM (
        ${removedRows}
      ) d
      LEFT JOIN ${tbl} t ON t.group_key = d.group_key
      GROUP BY d.group_key, d.removed_count, d.removed_ready_count, d.had_inflight
    ) sub
    WHERE g.group_key = sub.group_key;
  |]

groupsDeleteFunction :: Text -> Text -> Text -> Text -> Text
groupsDeleteFunction funcName groupsTbl tbl dd =
  let ifOld = inFlightPredicate ""
      removeUpdate =
        groupsRemoveUpdate
          groupsTbl
          tbl
          [text|
            SELECT group_key, COUNT(*) AS removed_count,
              COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended) AS removed_ready_count,
              bool_or(${ifOld}) AS had_inflight
            FROM old_table
            WHERE group_key IS NOT NULL
            GROUP BY group_key
          |]
   in [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM old_table WHERE group_key IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      -- Lock group rows in group_key order to avoid deadlock with concurrent triggers.
      PERFORM 1 FROM ${groupsTbl} g
      WHERE g.group_key IN (SELECT group_key FROM old_table WHERE group_key IS NOT NULL)
      ORDER BY g.group_key
      FOR UPDATE;

      ${removeUpdate}

      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

groupsUpdateFunction :: Text -> Text -> Text -> Text -> Text
groupsUpdateFunction funcName groupsTbl tbl dd =
  let ifSurv = inFlightPredicate "t."
      ifOld = inFlightPredicate "o."
      aggsN = groupAggregates "n."
      mergeSet = groupsMergeSet groupsTbl
      removeUpdate =
        groupsRemoveUpdate
          groupsTbl
          tbl
          [text|
            SELECT o.group_key, COUNT(*) AS removed_count,
              COUNT(*) FILTER (WHERE o.not_visible_until IS NULL AND NOT o.suspended) AS removed_ready_count,
              bool_or(${ifOld}) AS had_inflight
            FROM old_table o
            JOIN new_table n ON o.id = n.id
            WHERE o.group_key IS NOT NULL
              AND o.group_key IS DISTINCT FROM n.group_key
            GROUP BY o.group_key
          |]
   in [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM new_table WHERE group_key IS NOT NULL LIMIT 1
      ) AND NOT EXISTS (
        SELECT 1 FROM old_table WHERE group_key IS NOT NULL LIMIT 1
      ) THEN
        RETURN NULL;
      END IF;

      -- Lock group rows (old and new) in group_key order to avoid deadlock with concurrent triggers.
      PERFORM 1 FROM ${groupsTbl} g
      WHERE g.group_key IN (
        SELECT group_key FROM new_table WHERE group_key IS NOT NULL
        UNION
        SELECT group_key FROM old_table WHERE group_key IS NOT NULL
      )
      ORDER BY g.group_key
      FOR UPDATE;

      -- Full rescan: recompute in_flight_until when not_visible_until moves back or suspended changes.
      UPDATE ${groupsTbl} g
      SET in_flight_until = sub.new_ift
      FROM (
        SELECT t.group_key,
          MAX(t.not_visible_until) FILTER (
            WHERE ${ifSurv}
          ) AS new_ift
        FROM ${tbl} t
        WHERE t.group_key IN (
          SELECT n.group_key FROM new_table n
          JOIN old_table o ON o.id = n.id
          WHERE n.group_key IS NOT NULL
            AND (o.not_visible_until IS DISTINCT FROM n.not_visible_until
                 OR o.suspended IS DISTINCT FROM n.suspended
                 OR o.attempts IS DISTINCT FROM n.attempts)
            AND (
              n.not_visible_until > NOW() AND NOT n.suspended AND n.attempts > 0
              AND (o.not_visible_until IS NULL OR o.not_visible_until <= NOW()
                   OR n.not_visible_until > o.not_visible_until)
            ) IS NOT TRUE
        )
        GROUP BY t.group_key
      ) sub
      WHERE g.group_key = sub.group_key
        AND g.in_flight_until IS DISTINCT FROM sub.new_ift;

      IF EXISTS (
        SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE o.group_key IS DISTINCT FROM n.group_key
        LIMIT 1
      ) THEN
        -- A dedup replace moved the key, so drop the rows from their old group.
        ${removeUpdate}

        -- And add them to the new one.
        INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count, ready_count, next_due)
        SELECT n.group_key, ${aggsN}
        FROM new_table n
        JOIN old_table o ON o.id = n.id
        WHERE n.group_key IS NOT NULL
          AND o.group_key IS DISTINCT FROM n.group_key
        GROUP BY n.group_key
        ORDER BY n.group_key
        ON CONFLICT (group_key) DO UPDATE SET
          ${mergeSet};
      END IF;

      -- Same-group ordering and visibility recompute, in-flight extend and ready delta in one write.
      UPDATE ${groupsTbl} g
      SET min_priority = CASE WHEN m.recompute THEN m.new_min_priority ELSE g.min_priority END,
          min_id = CASE WHEN m.recompute THEN m.new_min_id ELSE g.min_id END,
          next_due = CASE WHEN m.recompute THEN m.new_next_due ELSE g.next_due END,
          in_flight_until = GREATEST(g.in_flight_until, m.new_ift),
          ready_count = GREATEST(0, g.ready_count + COALESCE(m.delta, 0))
      FROM (
        SELECT COALESCE(sub.group_key, s.group_key) AS group_key,
          sub.group_key IS NOT NULL AS recompute,
          sub.new_min_priority, sub.new_min_id, sub.new_next_due,
          s.new_ift, s.delta
        FROM (
          SELECT d.group_key,
            ${survivingAggregates}
          FROM (
            SELECT DISTINCT n.group_key
            FROM new_table n
            JOIN old_table o ON o.id = n.id
            WHERE n.group_key IS NOT NULL
              AND n.group_key IS NOT DISTINCT FROM o.group_key
              AND (n.priority IS DISTINCT FROM o.priority
                   OR o.not_visible_until IS DISTINCT FROM n.not_visible_until
                   OR o.suspended IS DISTINCT FROM n.suspended)
          ) d
          LEFT JOIN ${tbl} t ON t.group_key = d.group_key
          GROUP BY d.group_key
        ) sub
        FULL OUTER JOIN (
          SELECT COALESCE(ift.group_key, rc.group_key) AS group_key, ift.new_ift, rc.delta
          FROM (
            SELECT n.group_key, MAX(n.not_visible_until) AS new_ift
            FROM new_table n
            JOIN old_table o ON o.id = n.id
            WHERE n.group_key IS NOT NULL
              AND n.not_visible_until > NOW()
              AND NOT n.suspended
              AND n.attempts > 0
              AND (o.not_visible_until IS NULL OR o.not_visible_until <= NOW()
                   OR n.not_visible_until > o.not_visible_until)
            GROUP BY n.group_key
          ) ift
          FULL OUTER JOIN (
            SELECT group_key, delta FROM (
              SELECT n.group_key,
                SUM(
                  (CASE WHEN n.not_visible_until IS NULL AND NOT n.suspended THEN 1 ELSE 0 END)
                  - (CASE WHEN o.not_visible_until IS NULL AND NOT o.suspended THEN 1 ELSE 0 END)
                )::int AS delta
              FROM new_table n
              JOIN old_table o ON o.id = n.id
              WHERE n.group_key IS NOT NULL
                AND n.group_key IS NOT DISTINCT FROM o.group_key
              GROUP BY n.group_key
            ) z
            WHERE delta <> 0
          ) rc ON ift.group_key = rc.group_key
        ) s ON s.group_key = sub.group_key
      ) m
      WHERE g.group_key = m.group_key
        AND ((m.recompute
              AND (g.min_priority IS DISTINCT FROM m.new_min_priority
                   OR g.min_id IS DISTINCT FROM m.new_min_id
                   OR g.next_due IS DISTINCT FROM m.new_next_due))
             OR g.in_flight_until IS DISTINCT FROM GREATEST(g.in_flight_until, m.new_ift)
             OR COALESCE(m.delta, 0) <> 0);

      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

-- | The three statement-level AFTER triggers calling a queue's groups maintenance
-- functions, each handed its affected rows through a transition table.
createGroupsTriggersSQL :: Text -> Text -> Text
createGroupsTriggersSQL schemaName tableName =
  createMaintenanceTriggersSQL schemaName (jobQueueTable schemaName tableName) ("maintain_" <> tableName <> "_groups")

-- | One statement-level AFTER trigger: drop then recreate, wiring the
-- @<baseName><suffix>@ function over @tbl@ with the given event and REFERENCING clause.
statementTriggerSQL :: Text -> Text -> Text -> Text -> Text -> Text -> Text
statementTriggerSQL schemaName tbl baseName suffix event referencing =
  let func = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> suffix)
      trig = quoteIdentifier (baseName <> suffix)
   in T.intercalate
        "\n"
        [ "DROP TRIGGER IF EXISTS " <> trig <> " ON " <> tbl <> ";"
        , "CREATE TRIGGER " <> trig
        , "AFTER " <> event <> " ON " <> tbl
        , "REFERENCING " <> referencing
        , "FOR EACH STATEMENT EXECUTE FUNCTION " <> func <> "();"
        ]

-- | The 3 statement-level AFTER triggers (insert/delete/update) wiring a table's
-- maintenance functions, named @<baseName>_{insert,delete,update}@.
createMaintenanceTriggersSQL :: Text -> Text -> Text -> Text
createMaintenanceTriggersSQL schemaName tbl baseName =
  T.intercalate
    "\n\n"
    [ statementTriggerSQL schemaName tbl baseName "_insert" "INSERT" "NEW TABLE AS new_table"
    , statementTriggerSQL schemaName tbl baseName "_delete" "DELETE" "OLD TABLE AS old_table"
    , statementTriggerSQL schemaName tbl baseName "_update" "UPDATE" "OLD TABLE AS old_table NEW TABLE AS new_table"
    ]
    <> "\n"

-- | SQL for the per-table NOTIFY function, fired once per insert statement.
-- A statement that inserted nothing notifies nothing. Channel name is quoted as
-- a string literal, not an identifier.
createNotifyFunctionSQL :: Text -> Text -> Text
createNotifyFunctionSQL schemaName tableName =
  let functionName = notifyFunctionName tableName
      channelName = notificationChannelForTable tableName
      quotedChannel = T.replace "'" "''" channelName -- Escape single quotes for string literal
   in T.unlines
        [ "CREATE OR REPLACE FUNCTION " <> quoteIdentifier schemaName <> "." <> quoteIdentifier functionName <> "()"
        , "RETURNS TRIGGER AS $$"
        , "BEGIN"
        , "  IF EXISTS (SELECT 1 FROM new_table) THEN"
        , "    PERFORM pg_notify('" <> quotedChannel <> "', '');"
        , "  END IF;"
        , "  RETURN NULL;"
        , "END;"
        , "$$ LANGUAGE plpgsql;"
        , "COMMENT ON FUNCTION "
            <> quoteIdentifier schemaName
            <> "."
            <> quoteIdentifier functionName
            <> "() IS '"
            <> notifyObjectComment
            <> "';"
        ]

-- | A table's job-arrival NOTIFY trigger. Statement-level, so a batch insert notifies
-- one time for the statement.
createNotifyTriggerSQL :: Text -> Text -> Text
createNotifyTriggerSQL schemaName tableName =
  let functionName = notifyFunctionName tableName
      trigName = quoteIdentifier (notifyTriggerName tableName)
      tbl = jobQueueTable schemaName tableName
   in T.unlines
        [ "DROP TRIGGER IF EXISTS " <> trigName <> " ON " <> tbl <> ";"
        , "CREATE TRIGGER " <> trigName
        , "AFTER INSERT ON " <> tbl
        , "REFERENCING NEW TABLE AS new_table"
        , "FOR EACH STATEMENT"
        , "EXECUTE FUNCTION " <> quoteIdentifier schemaName <> "." <> quoteIdentifier functionName <> "();"
        , "COMMENT ON TRIGGER " <> trigName <> " ON " <> tbl <> " IS '" <> notifyObjectComment <> "';"
        ]

-- | Drop a table's job-arrival NOTIFY trigger.
dropNotifyTriggerSQL :: Text -> Text -> Text
dropNotifyTriggerSQL schemaName tableName =
  "DROP TRIGGER IF EXISTS "
    <> quoteIdentifier (notifyTriggerName tableName)
    <> " ON "
    <> jobQueueTable schemaName tableName
    <> ";"

-- | Drop a table's job-arrival NOTIFY function.
dropNotifyFunctionSQL :: Text -> Text -> Text
dropNotifyFunctionSQL schemaName tableName =
  let functionName = notifyFunctionName tableName
   in "DROP FUNCTION IF EXISTS " <> quoteIdentifier schemaName <> "." <> quoteIdentifier functionName <> "();"

-- ---------------------------------------------------------------------------
-- Event Streaming Triggers (for admin UI / SSE)
-- ---------------------------------------------------------------------------

-- | Event-streaming function that receives the logical queue name and DLQ flag
-- from each trigger. Trigger arguments avoid inferring either value from table
-- suffixes, so queue names ending in @_dlq@ remain unambiguous.
createEventStreamingFunctionSQL :: SchemaName -> Text
createEventStreamingFunctionSQL schemaName =
  let funcName = quoteIdentifier schemaName <> "." <> quoteIdentifier eventStreamingFunctionName
   in T.unlines
        [ "CREATE OR REPLACE FUNCTION " <> funcName <> "() RETURNS trigger AS $$"
        , "DECLARE"
        , "  event_type text;"
        , "  job_id bigint;"
        , "  queue_name text := TG_ARGV[0];"
        , "  is_dlq boolean := TG_ARGV[1]::boolean;"
        , "BEGIN"
        , "  CASE TG_OP"
        , "    WHEN 'INSERT' THEN"
        , "      event_type := CASE WHEN is_dlq THEN 'job_dlq' ELSE 'job_inserted' END;"
        , "      job_id := NEW.id;"
        , "    WHEN 'UPDATE' THEN"
        , "      event_type := 'job_updated';"
        , "      job_id := NEW.id;"
        , "    WHEN 'DELETE' THEN"
        , "      event_type := 'job_deleted';"
        , "      job_id := OLD.id;"
        , "  END CASE;"
        , ""
        , "  PERFORM pg_notify('" <> eventStreamingChannel <> "',"
        , "    json_build_object("
        , "      'event', event_type,"
        , "      'table', queue_name,"
        , "      'job_id', job_id"
        , "    )::text);"
        , "  RETURN NULL;"
        , "END;"
        , "$$ LANGUAGE plpgsql;"
        , "COMMENT ON FUNCTION " <> funcName <> "() IS '" <> eventStreamingObjectComment <> "';"
        ]

-- | Install event-streaming triggers with explicit logical queue metadata.
createEventStreamingTriggersSQL :: SchemaName -> TableName -> Text
createEventStreamingTriggersSQL schemaName tableName =
  let tbl = jobQueueTable schemaName tableName
      dlqTbl = jobQueueDLQTable schemaName tableName
      funcName = quoteIdentifier schemaName <> "." <> quoteIdentifier eventStreamingFunctionName
      triggerCall isDLQ =
        "FOR EACH ROW EXECUTE FUNCTION "
          <> funcName
          <> "("
          <> quoteLiteral tableName
          <> ", "
          <> quoteLiteral isDLQ
          <> ");"
      triggerComment trigger tableRef =
        "COMMENT ON TRIGGER "
          <> quoteIdentifier trigger
          <> " ON "
          <> tableRef
          <> " IS "
          <> quoteLiteral eventStreamingObjectComment
          <> ";"
   in dropEventStreamingTriggersSQL schemaName tableName
        <> T.unlines
          [ "CREATE TRIGGER " <> quoteIdentifier (eventStreamingTriggerName tableName)
          , "AFTER INSERT OR UPDATE OR DELETE ON " <> tbl
          , triggerCall "false"
          , triggerComment (eventStreamingTriggerName tableName) tbl
          , ""
          , "CREATE TRIGGER " <> quoteIdentifier (eventStreamingDLQTriggerName tableName)
          , "AFTER INSERT ON " <> dlqTbl
          , triggerCall "true"
          , triggerComment (eventStreamingDLQTriggerName tableName) dlqTbl
          ]
  where
    quoteLiteral = ("'" <>) . (<> "'") . T.replace "'" "''"

-- | Drop current and legacy event-streaming triggers for a queue and its DLQ.
-- The shared function is dropped separately after every queue is detached.
dropEventStreamingTriggersSQL :: Text -> Text -> Text
dropEventStreamingTriggersSQL schemaName tableName =
  let tbl = jobQueueTable schemaName tableName
      dlqTbl = jobQueueDLQTable schemaName tableName
      dropTrigger name tableRef = "DROP TRIGGER IF EXISTS " <> quoteIdentifier name <> " ON " <> tableRef <> ";"
   in T.unlines $
        map (\(name, isDLQ) -> dropTrigger name (bool tbl dlqTbl isDLQ)) legacyEventStreamingTriggers
          <> [ dropTrigger (eventStreamingTriggerName tableName) tbl
             , dropTrigger (eventStreamingDLQTriggerName tableName) dlqTbl
             ]

-- | Drop the schema-wide event-streaming function after its triggers are detached.
dropEventStreamingFunctionSQL :: SchemaName -> Text
dropEventStreamingFunctionSQL schemaName =
  "DROP FUNCTION IF EXISTS "
    <> quoteIdentifier schemaName
    <> "."
    <> quoteIdentifier eventStreamingFunctionName
    <> "();"

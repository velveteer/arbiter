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

    -- * Groups Trigger SQL
  , createGroupsTriggerFunctionsSQL
  , maintenanceFunctionNames
  , createGroupsTriggersSQL
  , createMaintenanceTriggersSQL
  , statementTriggerSQL
  , inFlightPredicate
  , groupAggregates
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Job.Types (defaultMaxAttempts)
import Arbiter.Core.SqlLiterals (quoteIdentifier)

-- | PostgreSQL schema name, e.g. @"arbiter"@.
type SchemaName = Text

-- | Unqualified table name within a schema, e.g. @"email_jobs"@.
type TableName = Text

-- | Default PostgreSQL schema name for Arbiter tables
--
-- Using a dedicated schema prevents namespace pollution in the user's public schema.
defaultSchemaName :: SchemaName
defaultSchemaName = "arbiter"

-- | Generate notification channel name for a table
--
-- Each table gets its own NOTIFY channel for job insertions.
-- Example: notificationChannelForTable "email_jobs" -> "email_jobs_created"
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

-- | SQL to create the schema for Arbiter tables
createSchemaSQL :: SchemaName -> Text
createSchemaSQL schemaName =
  "CREATE SCHEMA IF NOT EXISTS " <> quoteIdentifier schemaName <> ";"

-- | Common job column definitions (matches the Job type structure)
--
-- These columns are shared between job_queue and dead_letter_queue tables.
--
-- Checksummed by the create-table migration: a new column ships as its own ALTER script.
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

-- | Add the W3C trace-context columns to a queue's three job tables.
addTraceContextColumnSQL :: Text -> Text -> Text
addTraceContextColumnSQL schemaName tableName =
  T.unlines [alter (tbl schemaName tableName) | tbl <- [jobQueueTable, jobQueueDLQTable, jobQueueArchiveTable]]
  where
    alter table = "ALTER TABLE " <> table <> " " <> T.intercalate ", " (map addColumn ["traceparent", "tracestate"]) <> ";"
    addColumn column = "ADD COLUMN IF NOT EXISTS " <> column <> " TEXT"

-- | Add the per-claim token column to a queue's three job tables.
--
-- Bumped by every claim and never decremented, so it identifies one claim where
-- @attempts@ cannot: a nack gives an attempt back, so that counter repeats values.
addClaimSeqColumnSQL :: Text -> Text -> Text
addClaimSeqColumnSQL schemaName tableName =
  T.unlines [alter (tbl schemaName tableName) | tbl <- [jobQueueTable, jobQueueDLQTable, jobQueueArchiveTable]]
  where
    alter table = "ALTER TABLE " <> table <> " ADD COLUMN IF NOT EXISTS claim_seq BIGINT NOT NULL DEFAULT 0;"

-- | Job column definitions for DLQ table (with job_id instead of id)
jobColumnsForDLQ :: Text
jobColumnsForDLQ =
  T.unlines
    [ "  id BIGSERIAL PRIMARY KEY,"
    , "  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  job_id BIGINT NOT NULL,"
    ]
    <> T.unlines (drop 1 jobColumns)

-- | SQL to create the main job queue table within a schema
--
-- This table stores pending and in-progress jobs.
createJobQueueTableSQL :: Text -> Text -> Text
createJobQueueTableSQL schemaName tableName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> jobQueueTable schemaName tableName <> " ("
    , T.unlines jobColumns
    , ") WITH (fillfactor = 70);"
    ]

-- | SQL to create the dead letter queue table within a schema
--
-- Jobs that fail repeatedly (exceed max attempts) are moved here for inspection.
-- This table contains ALL the Job fields (complete snapshot) plus DLQ-specific metadata.
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

-- | Index on archive @job_id@ for by-id lookups ('getArchivedJobById').
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

-- | SQL to create a partial index on group_key for efficient per-group lookups.
--
-- Used by claim queries' LATERAL subqueries and by the DELETE/UPDATE triggers
-- when recomputing @in_flight_until@. Composite @(group_key, priority, id)@
-- so the DELETE trigger can recompute min values via index-only lookup.
createJobQueueGroupKeyIndexSQL :: Text -> Text -> Text
createJobQueueGroupKeyIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_group_key")
    , "ON " <> jobQueueTable schemaName tableName <> " (group_key, priority ASC, id ASC)"
    , "WHERE group_key IS NOT NULL;"
    ]

-- | Ranking index over ready ungrouped jobs only (@not_visible_until IS NULL AND
-- NOT suspended@). Scheduled/backoff/in-flight rows have @not_visible_until@ set
-- and suspended rows are excluded, so they are absent, and the claim's ordered
-- @LIMIT@ short-circuits at the head over ready rows instead of walking them.
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

-- | SQL to create index on DLQ group_key for querying failed jobs by group
createDLQGroupKeyIndexSQL :: Text -> Text -> Text
createDLQGroupKeyIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_dlq_group_key")
    , "ON " <> jobQueueDLQTable schemaName tableName <> " (group_key);"
    ]

-- | SQL to create index on DLQ failed_at for time-based queries
createDLQFailedAtIndexSQL :: Text -> Text -> Text
createDLQFailedAtIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_dlq_failed_at")
    , "ON " <> jobQueueDLQTable schemaName tableName <> " (failed_at DESC);"
    ]

-- | SQL to create index on DLQ parent_id for efficient child lookups (DLQ child counts)
createDLQParentIdIndexSQL :: Text -> Text -> Text
createDLQParentIdIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_dlq_parent_id")
    , "ON " <> jobQueueDLQTable schemaName tableName <> " (parent_id)"
    , "WHERE parent_id IS NOT NULL;"
    ]

-- | SQL to create unique index on dedup_key for job deduplication
createDedupKeyIndexSQL :: Text -> Text -> Text
createDedupKeyIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE UNIQUE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_dedup_key")
    , "ON " <> jobQueueTable schemaName tableName <> " (dedup_key)"
    , "WHERE dedup_key IS NOT NULL;"
    ]

-- | SQL to create a partial index on parent_id for efficient child lookups.
createParentIdIndexSQL :: Text -> Text -> Text
createParentIdIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_parent_id")
    , "ON " <> jobQueueTable schemaName tableName <> " (parent_id)"
    , "WHERE parent_id IS NOT NULL;"
    ]

-- | SQL to create the results table for storing child job results.
--
-- Child results are stored as individual rows (one per child), keyed by
-- @(parent_id, child_id)@. The @ON DELETE CASCADE@ foreign key ensures
-- cleanup when the parent is acked (deleted).
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

-- | SQL to create the groups table for fast grouped claims.
--
-- Stores one row per distinct @group_key@ with pre-computed @min_priority@,
-- @min_id@, @job_count@, and @in_flight_until@. Maintained by statement-level
-- AFTER triggers on the main job table (see 'createGroupsTriggerFunctionsSQL').
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

-- ---------------------------------------------------------------------------
-- Groups Maintenance Triggers
-- ---------------------------------------------------------------------------

-- | Three trigger functions maintaining the groups table via statement-level
-- AFTER triggers. Uses incremental operations where possible.
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

      -- Step 1: Full rescan - recompute in_flight_until when not_visible_until decreases or suspended changes
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
        -- Step 2: group_key change (dedup replace) - remove from old group
        ${removeUpdate}

        -- Step 3: group_key change - add to new group
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

      -- Step 4: same-group ordering and visibility recompute, in-flight extend and ready delta in one write.
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

-- | SQL to create 3 statement-level AFTER triggers on the main job table
-- that call the groups maintenance functions.
--
-- Uses @REFERENCING NEW\/OLD TABLE AS@ for efficient batch access to
-- affected rows via transition tables.
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

-- | SQL for the per-table NOTIFY function (fires after INSERT).
-- Channel name is quoted as a string literal, not an identifier.
createNotifyFunctionSQL :: Text -> Text -> Text
createNotifyFunctionSQL schemaName tableName =
  let functionName = notifyFunctionName tableName
      channelName = notificationChannelForTable tableName
      quotedChannel = T.replace "'" "''" channelName -- Escape single quotes for string literal
   in T.unlines
        [ "CREATE OR REPLACE FUNCTION " <> quoteIdentifier schemaName <> "." <> quoteIdentifier functionName <> "()"
        , "RETURNS TRIGGER AS $$"
        , "BEGIN"
        , "  PERFORM pg_notify('" <> quotedChannel <> "', '');"
        , "  RETURN NEW;"
        , "END;"
        , "$$ LANGUAGE plpgsql;"
        ]

-- | SQL to create the NOTIFY trigger for a specific table
--
-- This trigger fires AFTER INSERT on the job queue table and calls the table-specific notify function.
createNotifyTriggerSQL :: Text -> Text -> Text
createNotifyTriggerSQL schemaName tableName =
  let functionName = notifyFunctionName tableName
      trigName = quoteIdentifier (notifyTriggerName tableName)
      tbl = jobQueueTable schemaName tableName
   in T.unlines
        [ "DROP TRIGGER IF EXISTS " <> trigName <> " ON " <> tbl <> ";"
        , "CREATE TRIGGER " <> trigName
        , "AFTER INSERT ON " <> tbl
        , "FOR EACH ROW"
        , "EXECUTE FUNCTION " <> quoteIdentifier schemaName <> "." <> quoteIdentifier functionName <> "();"
        ]

-- | SQL to drop the NOTIFY trigger
dropNotifyTriggerSQL :: Text -> Text -> Text
dropNotifyTriggerSQL schemaName tableName =
  "DROP TRIGGER IF EXISTS "
    <> quoteIdentifier (notifyTriggerName tableName)
    <> " ON "
    <> jobQueueTable schemaName tableName
    <> ";"

-- | SQL to drop the NOTIFY function for a specific table
dropNotifyFunctionSQL :: Text -> Text -> Text
dropNotifyFunctionSQL schemaName tableName =
  let functionName = notifyFunctionName tableName
   in "DROP FUNCTION IF EXISTS " <> quoteIdentifier schemaName <> "." <> quoteIdentifier functionName <> "();"

-- ---------------------------------------------------------------------------
-- Event Streaming Triggers (for admin UI / SSE)
-- ---------------------------------------------------------------------------

-- | Shared event streaming function (one per schema). Fires on
-- INSERT\/UPDATE\/DELETE of job tables and INSERT on DLQ tables. Sends a JSON
-- event via @pg_notify@ on the @arbiter_job_events@ channel. Uses
-- @TG_TABLE_NAME@ and @TG_OP@ so a single function covers all tables.
createEventStreamingFunctionSQL :: SchemaName -> Text
createEventStreamingFunctionSQL schemaName =
  let funcName = quoteIdentifier schemaName <> "." <> quoteIdentifier eventStreamingFunctionName
   in T.unlines
        [ "CREATE OR REPLACE FUNCTION " <> funcName <> "() RETURNS trigger AS $$"
        , "DECLARE"
        , "  event_type text;"
        , "  job_id bigint;"
        , "BEGIN"
        , "  CASE TG_OP"
        , "    WHEN 'INSERT' THEN"
        , "      event_type := CASE"
        , "        WHEN TG_TABLE_NAME LIKE '%_dlq' THEN 'job_dlq'"
        , "        ELSE 'job_inserted'"
        , "      END;"
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
        , "      'table', regexp_replace(TG_TABLE_NAME, '_dlq$', ''),"
        , "      'job_id', job_id"
        , "    )::text);"
        , "  RETURN NULL;"
        , "END;"
        , "$$ LANGUAGE plpgsql;"
        ]

-- | SQL to create event streaming triggers for a table and its DLQ
--
-- Creates a combined INSERT/UPDATE/DELETE trigger on the main table and an
-- INSERT trigger on the DLQ table, both calling the shared @notify_job_event@
-- function. Also drops any legacy per-operation triggers left by older versions
-- of @setupEventTriggers@.
createEventStreamingTriggersSQL :: Text -> Text -> Text
createEventStreamingTriggersSQL schemaName tableName =
  let tbl = jobQueueTable schemaName tableName
      dlqTbl = jobQueueDLQTable schemaName tableName
      funcName = quoteIdentifier schemaName <> "." <> quoteIdentifier eventStreamingFunctionName
      trigName = eventStreamingTriggerName tableName
      dlqTrigName = eventStreamingDLQTriggerName tableName
   in T.unlines
        [ -- Drop legacy per-operation triggers (from setupEventTriggers)
          "DROP TRIGGER IF EXISTS " <> quoteIdentifier "notify_job_insert" <> " ON " <> tbl <> ";"
        , "DROP TRIGGER IF EXISTS " <> quoteIdentifier "notify_job_update" <> " ON " <> tbl <> ";"
        , "DROP TRIGGER IF EXISTS " <> quoteIdentifier "notify_job_delete" <> " ON " <> tbl <> ";"
        , "DROP TRIGGER IF EXISTS " <> quoteIdentifier "notify_dlq_insert" <> " ON " <> dlqTbl <> ";"
        , ""
        , -- Create combined triggers (drop first for idempotency)
          "DROP TRIGGER IF EXISTS " <> quoteIdentifier trigName <> " ON " <> tbl <> ";"
        , "CREATE TRIGGER " <> quoteIdentifier trigName
        , "AFTER INSERT OR UPDATE OR DELETE ON " <> tbl
        , "FOR EACH ROW EXECUTE FUNCTION " <> funcName <> "();"
        , ""
        , "DROP TRIGGER IF EXISTS " <> quoteIdentifier dlqTrigName <> " ON " <> dlqTbl <> ";"
        , "CREATE TRIGGER " <> quoteIdentifier dlqTrigName
        , "AFTER INSERT ON " <> dlqTbl
        , "FOR EACH ROW EXECUTE FUNCTION " <> funcName <> "();"
        ]

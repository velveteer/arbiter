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
  , addKindColumnSQL
  , setMaxAttemptsDefaultSQL

    -- * Index Creation SQL
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

    -- * Maintenance Trigger SQL
  , maintenanceFunctionNames
  , createMaintenanceTriggersSQL
  , statementTriggerSQL
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

-- | The schema arbiter's tables live in by default.
defaultSchemaName :: SchemaName
defaultSchemaName = "arbiter"

-- | A table's own job-arrival NOTIFY channel: @\"email_jobs\"@ -> @\"email_jobs_created\"@.
notificationChannelForTable :: TableName -> Text
notificationChannelForTable tableName = tableName <> "_created"

-- | Channel name used by the event streaming (SSE) system.
eventStreamingChannel :: Text
eventStreamingChannel = "arbiter_job_events"

-- | Prefix for per-queue pause NOTIFY channels. The full channel name appends
-- the queue. SQL templates build the channel from @queue_name@ returned by a CTE.
pauseNotifyChannelPrefix :: SchemaName -> Text
pauseNotifyChannelPrefix schemaName = "arbiter_pause_" <> schemaName <> "_"

-- | Per-queue NOTIFY channel for pause/resume changes. Workers LISTEN on the
-- channel for their own queue.
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
-- Sweeps match 'notifyObjectCommentPrefix'. A trigger is current when its comment
-- equals this exact value. Bump the version whenever 'createNotifyTriggerSQL' changes.
notifyObjectComment :: Text
notifyObjectComment = notifyObjectCommentPrefix <> "v1"

-- | The marker prefix identifying a notify object as arbiter's, across versions.
notifyObjectCommentPrefix :: Text
notifyObjectCommentPrefix = "arbiter:notify:"

-- | Marker stamped on notify objects installed before arbiter marked them. Sweeps
-- match it. An adopted trigger is rebuilt.
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
-- The column stays nullable for rolling deploys.
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
-- migration. A new column ships as its own ALTER script.
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

-- | Add the payload variant label to a queue's three job tables.
addKindColumnSQL :: Text -> Text -> Text
addKindColumnSQL schemaName tableName =
  addJobColumnsSQL schemaName tableName ["kind TEXT"]

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

-- | Archive table columns: every Job read column (@job_id@ for @id@) plus the
-- write-only @rate_limit_cost@, the completed root job's @result@, and the
-- @completed_at@/@archive_expires_at@ metadata.
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

-- | Create the completed-job archive table. The table is logged.
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

-- | Ranking index over ready ungrouped jobs (@not_visible_until IS NULL AND NOT
-- suspended@). The claim's ordered @LIMIT@ stops the scan at the first ready rows.
createJobQueueUngroupedReadyRankingIndexSQL :: Text -> Text -> Text
createJobQueueUngroupedReadyRankingIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_ungrouped_ready_ranking")
    , "ON " <> jobQueueTable schemaName tableName <> " (priority ASC, id ASC)"
    , "WHERE group_key IS NULL AND not_visible_until IS NULL AND NOT suspended;"
    ]

-- | Due-finder for ungrouped parked rows. The claim range-scans it by
-- @not_visible_until <= NOW()@ for due scheduled, backoff and expired-lease jobs.
createJobQueueUngroupedDueIndexSQL :: Text -> Text -> Text
createJobQueueUngroupedDueIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_ungrouped_due")
    , "ON " <> jobQueueTable schemaName tableName <> " (not_visible_until ASC)"
    , "WHERE group_key IS NULL AND not_visible_until IS NOT NULL AND NOT suspended;"
    ]

-- | Replace the full ungrouped ranking index with the ready-only ranking index
-- plus the due-finder.
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

-- | Unique index on @dedup_key@. The dedup @ON CONFLICT@ resolves against it.
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
-- Its foreign key cascades. Acking the parent clears them.
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

-- ---------------------------------------------------------------------------
-- Groups Maintenance Triggers
-- ---------------------------------------------------------------------------

-- | The qualified @<baseName>_{insert,delete,update}@ maintenance-function names.
maintenanceFunctionNames :: Text -> Text -> (Text, Text, Text)
maintenanceFunctionNames schemaName baseName =
  (func "_insert", func "_delete", func "_update")
  where
    func suffix = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> suffix)

-- | One statement-level AFTER trigger. Drops then recreates, wiring the
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
-- a string literal.
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

-- | A table's job-arrival NOTIFY trigger. Statement-level. A batch insert notifies
-- one time.
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
-- from each trigger. Queue names ending in @_dlq@ stay unambiguous. A lease-extend
-- update emits no event.
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
        , "  IF TG_OP = 'UPDATE' THEN"
        , "    IF NEW.claimed_by IS NOT NULL AND NEW.claim_seq = OLD.claim_seq"
        , "       AND NEW.not_visible_until >= OLD.not_visible_until"
        , "       AND " <> leaseStripped "OLD" <> " = " <> leaseStripped "NEW" <> " THEN"
        , "      RETURN NULL;"
        , "    END IF;"
        , "  END IF;"
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
  where
    leaseStripped row = "(to_jsonb(" <> row <> ") - 'not_visible_until' - 'updated_at')"

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

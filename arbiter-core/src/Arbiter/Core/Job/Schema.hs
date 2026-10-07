{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- SQL generation functions for job queue schemas. No database execution happens here.
module Arbiter.Core.Job.Schema
  ( -- * Name Types
    SchemaName
  , TableName

    -- * Schema creation
  , createSchemaSQL

    -- * Table creation and column migrations
  , createJobQueueTableSQL
  , createJobQueueDLQTableSQL
  , createJobQueueArchiveTableSQL
  , addTraceContextColumnSQL
  , addClaimSeqColumnSQL
  , addKindColumnSQL
  , setMaxAttemptsDefaultSQL

    -- * Index creation SQL
  , indexSQL
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

    -- * NOTIFY trigger SQL
  , createNotifyFunctionSQL
  , createNotifyTriggerSQL

    -- * Event-streaming trigger SQL
  , createEventStreamingFunctionSQL
  , createEventStreamingTriggersSQL
  , dropEventStreamingFunctionSQL

    -- * Notification channel helpers
  , notificationChannelForTable
  , eventStreamingChannel
  , pauseNotifyChannel
  , pauseNotifyChannelPrefix
  , cancelNotifyChannel
  , cronRunNotifyChannel

    -- * Trigger and function name helpers
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

    -- * Table name helpers
  , queueTableNames
  , qualifiedTable
  , jobQueueTable
  , jobQueueDLQTable
  , jobQueueArchiveTable
  , jobQueueResultsTable
  , jobQueueGroupsTable

    -- * Results table
  , createResultsTableSQL

    -- * Maintenance trigger SQL
  , maintenanceFunctionNames
  , createMaintenanceTriggersSQL
  , statementTriggerSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.SqlLiterals (defaultMaxAttemptsSQL, quoteIdentifier, textLiteral)

-- | PostgreSQL schema name, e.g. @"arbiter"@.
type SchemaName = Text

-- | Unqualified table name within a schema, e.g. @"email_jobs"@.
type TableName = Text

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

-- | Event-streaming trigger names the reconcile adopts beside the per-queue names, each
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

-- | Marker the reconcile stamps on an unmarked notify object it adopts. Sweeps
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
   in [text|
        UPDATE ${tbl} SET max_attempts = ${defaultMaxAttemptsSQL} WHERE max_attempts IS NULL;
        ALTER TABLE ${tbl} ALTER COLUMN max_attempts SET DEFAULT ${defaultMaxAttemptsSQL};
      |]

-- | Qualified results table name: @jobQueueResultsTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs_results"@
jobQueueResultsTable :: SchemaName -> TableName -> Text
jobQueueResultsTable schemaName tableName = qualifiedTable schemaName (tableName <> resultsSuffix)

-- | Qualified groups table name: @jobQueueGroupsTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs_groups"@
jobQueueGroupsTable :: SchemaName -> TableName -> Text
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

-- | The job columns the queue, DLQ and archive tables share. Checksummed by the create-table
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
addJobColumnsSQL :: SchemaName -> TableName -> [Text] -> Text
addJobColumnsSQL schemaName tableName columns =
  T.unlines [alter (tbl schemaName tableName) | tbl <- [jobQueueTable, jobQueueDLQTable, jobQueueArchiveTable]]
  where
    alter table = "ALTER TABLE " <> table <> " " <> T.intercalate ", " (map addColumn columns) <> ";"
    addColumn column = "ADD COLUMN IF NOT EXISTS " <> column

-- | Add the W3C trace-context columns to a queue's three job tables.
addTraceContextColumnSQL :: SchemaName -> TableName -> Text
addTraceContextColumnSQL schemaName tableName =
  addJobColumnsSQL schemaName tableName ["traceparent TEXT", "tracestate TEXT"]

-- | Add the per-claim token column to a queue's three job tables.
addClaimSeqColumnSQL :: SchemaName -> TableName -> Text
addClaimSeqColumnSQL schemaName tableName =
  addJobColumnsSQL schemaName tableName ["claim_seq BIGINT NOT NULL DEFAULT 0"]

-- | Add the payload variant label to a queue's three job tables.
addKindColumnSQL :: SchemaName -> TableName -> Text
addKindColumnSQL schemaName tableName =
  addJobColumnsSQL schemaName tableName ["kind TEXT"]

-- | 'jobColumns' for the DLQ table. Its own @id@, @failed_at@ and the original @job_id@
-- come first.
jobColumnsForDLQ :: Text
jobColumnsForDLQ =
  T.unlines
    [ "  id BIGSERIAL PRIMARY KEY,"
    , "  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  job_id BIGINT NOT NULL,"
    ]
    <> T.unlines (drop 1 jobColumns)

-- | Create a queue's main job table, holding its pending and in-progress jobs.
createJobQueueTableSQL :: SchemaName -> TableName -> Text
createJobQueueTableSQL schemaName tableName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> jobQueueTable schemaName tableName <> " ("
    , T.unlines jobColumns
    , ") WITH (fillfactor = 70);"
    ]

-- | Create a queue's DLQ table, where failed jobs land as a full snapshot plus their
-- failure metadata.
createJobQueueDLQTableSQL :: SchemaName -> TableName -> Text
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
createJobQueueArchiveTableSQL :: SchemaName -> TableName -> Text
createJobQueueArchiveTableSQL schemaName tableName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> jobQueueArchiveTable schemaName tableName <> " ("
    , jobColumnsForArchive
    , ");"
    ]

-- | @CREATE INDEX IF NOT EXISTS@ on the named table, partial under a predicate.
indexSQL :: Text -> Text -> Text -> Maybe Text -> Text
indexSQL = indexSQLWith "CREATE INDEX IF NOT EXISTS "

-- | 'indexSQL' for a unique index.
uniqueIndexSQL :: Text -> Text -> Text -> Maybe Text -> Text
uniqueIndexSQL = indexSQLWith "CREATE UNIQUE INDEX IF NOT EXISTS "

indexSQLWith :: Text -> Text -> Text -> Text -> Maybe Text -> Text
indexSQLWith create name tbl columns predicate =
  T.unlines (create <> quoteIdentifier name : body)
  where
    body = case predicate of
      Nothing -> ["ON " <> tbl <> " (" <> columns <> ");"]
      Just filterText -> ["ON " <> tbl <> " (" <> columns <> ")", "WHERE " <> filterText <> ";"]

-- | Index on archive @completed_at@ for the most-recent-first history listing.
createArchiveCompletedAtIndexSQL :: SchemaName -> TableName -> Text
createArchiveCompletedAtIndexSQL schemaName tableName =
  indexSQL
    ("idx_" <> tableName <> "_archive_completed_at")
    (jobQueueArchiveTable schemaName tableName)
    "completed_at DESC"
    Nothing

-- | Index on archive @archive_expires_at@. Drives the retention purge sweep.
createArchiveExpiresAtIndexSQL :: SchemaName -> TableName -> Text
createArchiveExpiresAtIndexSQL schemaName tableName =
  indexSQL
    ("idx_" <> tableName <> "_archive_expires_at")
    (jobQueueArchiveTable schemaName tableName)
    "archive_expires_at"
    Nothing

-- | Index on archive @job_id@ for by-id lookups ('Arbiter.Core.HighLevel.getArchiveJobById').
createArchiveJobIdIndexSQL :: SchemaName -> TableName -> Text
createArchiveJobIdIndexSQL schemaName tableName =
  indexSQL ("idx_" <> tableName <> "_archive_job_id") (jobQueueArchiveTable schemaName tableName) "job_id" Nothing

-- | Index on archive @parent_id@ for per-tree history lookups.
createArchiveParentIdIndexSQL :: SchemaName -> TableName -> Text
createArchiveParentIdIndexSQL schemaName tableName =
  indexSQL
    ("idx_" <> tableName <> "_archive_parent_id")
    (jobQueueArchiveTable schemaName tableName)
    "parent_id"
    (Just "parent_id IS NOT NULL")

-- | Index on archive @group_key@ for per-group history lookups.
createArchiveGroupKeyIndexSQL :: SchemaName -> TableName -> Text
createArchiveGroupKeyIndexSQL schemaName tableName =
  indexSQL ("idx_" <> tableName <> "_archive_group_key") (jobQueueArchiveTable schemaName tableName) "group_key" Nothing

-- | Ranking index over ready ungrouped jobs (@not_visible_until IS NULL AND NOT
-- suspended@). The claim's ordered @LIMIT@ stops the scan at the first ready rows.
createJobQueueUngroupedReadyRankingIndexSQL :: SchemaName -> TableName -> Text
createJobQueueUngroupedReadyRankingIndexSQL schemaName tableName =
  indexSQL
    ("idx_" <> tableName <> "_ungrouped_ready_ranking")
    (jobQueueTable schemaName tableName)
    "priority ASC, id ASC"
    (Just "group_key IS NULL AND not_visible_until IS NULL AND NOT suspended")

-- | Due-finder for ungrouped parked rows. The claim range-scans it by
-- @not_visible_until <= NOW()@ for due scheduled, backoff and expired-lease jobs.
createJobQueueUngroupedDueIndexSQL :: SchemaName -> TableName -> Text
createJobQueueUngroupedDueIndexSQL schemaName tableName =
  indexSQL
    ("idx_" <> tableName <> "_ungrouped_due")
    (jobQueueTable schemaName tableName)
    "not_visible_until ASC"
    (Just "group_key IS NULL AND not_visible_until IS NOT NULL AND NOT suspended")

-- | Replace the full ungrouped ranking index with the ready-only ranking index
-- plus the due-finder.
migrateUngroupedReadySplitIndexesSQL :: SchemaName -> TableName -> Text
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
createDLQGroupKeyIndexSQL :: SchemaName -> TableName -> Text
createDLQGroupKeyIndexSQL schemaName tableName =
  indexSQL ("idx_" <> tableName <> "_dlq_group_key") (jobQueueDLQTable schemaName tableName) "group_key" Nothing

-- | Index on DLQ @failed_at@, for the most-recent-first listing.
createDLQFailedAtIndexSQL :: SchemaName -> TableName -> Text
createDLQFailedAtIndexSQL schemaName tableName =
  indexSQL ("idx_" <> tableName <> "_dlq_failed_at") (jobQueueDLQTable schemaName tableName) "failed_at DESC" Nothing

-- | Index on DLQ @parent_id@, for per-parent child lookups and counts.
createDLQParentIdIndexSQL :: SchemaName -> TableName -> Text
createDLQParentIdIndexSQL schemaName tableName =
  indexSQL
    ("idx_" <> tableName <> "_dlq_parent_id")
    (jobQueueDLQTable schemaName tableName)
    "parent_id"
    (Just "parent_id IS NOT NULL")

-- | Unique index on @dedup_key@. The dedup @ON CONFLICT@ resolves against it.
createDedupKeyIndexSQL :: SchemaName -> TableName -> Text
createDedupKeyIndexSQL schemaName tableName =
  uniqueIndexSQL
    ("idx_" <> tableName <> "_dedup_key")
    (jobQueueTable schemaName tableName)
    "dedup_key"
    (Just "dedup_key IS NOT NULL")

-- | Partial index on @parent_id@, for per-parent child lookups.
createParentIdIndexSQL :: SchemaName -> TableName -> Text
createParentIdIndexSQL schemaName tableName =
  indexSQL
    ("idx_" <> tableName <> "_parent_id")
    (jobQueueTable schemaName tableName)
    "parent_id"
    (Just "parent_id IS NOT NULL")

-- | Create a queue's results table, one row per child keyed by @(parent_id, child_id)@.
-- Its foreign key cascades. Acking the parent clears them.
createResultsTableSQL :: SchemaName -> TableName -> Text
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

-- | The qualified @\<baseName\>_{insert,delete,update}@ maintenance-function names.
maintenanceFunctionNames :: SchemaName -> Text -> (Text, Text, Text)
maintenanceFunctionNames schemaName baseName =
  (func "_insert", func "_delete", func "_update")
  where
    func suffix = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> suffix)

-- | One statement-level AFTER trigger. Drops then recreates, wiring the
-- @\<baseName\>\<suffix\>@ function over @tbl@ with the given event and REFERENCING clause.
statementTriggerSQL :: SchemaName -> Text -> Text -> Text -> Text -> Text -> Text
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

-- | The 3 statement-level AFTER triggers (insert\/delete\/update) wiring a table's
-- maintenance functions, named @\<baseName\>_{insert,delete,update}@.
createMaintenanceTriggersSQL :: SchemaName -> Text -> Text -> Text
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
createNotifyFunctionSQL :: SchemaName -> TableName -> Text
createNotifyFunctionSQL schemaName tableName =
  let functionName = notifyFunctionName tableName
      channel = textLiteral (notificationChannelForTable tableName)
   in T.unlines
        [ "CREATE OR REPLACE FUNCTION " <> quoteIdentifier schemaName <> "." <> quoteIdentifier functionName <> "()"
        , "RETURNS TRIGGER AS $$"
        , "BEGIN"
        , "  IF EXISTS (SELECT 1 FROM new_table) THEN"
        , "    PERFORM pg_notify(" <> channel <> ", '');"
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
createNotifyTriggerSQL :: SchemaName -> TableName -> Text
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

-- ---------------------------------------------------------------------------
-- Event Streaming Triggers (for admin UI / SSE)
-- ---------------------------------------------------------------------------

-- | Event-streaming function that receives the logical queue name and DLQ flag
-- from each trigger. Queue names ending in @_dlq@ stay unambiguous. A DLQ row
-- reports its original job id. A lease-extend update emits no event.
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
        , "      IF is_dlq THEN"
        , "        event_type := 'job_dlq';"
        , "        job_id := NEW.job_id;"
        , "      ELSE"
        , "        event_type := 'job_inserted';"
        , "        job_id := NEW.id;"
        , "      END IF;"
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
        , "      'job_id', job_id,"
        , "      'dlq', is_dlq"
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
          <> textLiteral tableName
          <> ", "
          <> textLiteral isDLQ
          <> ");"
      triggerComment trigger tableRef =
        "COMMENT ON TRIGGER "
          <> quoteIdentifier trigger
          <> " ON "
          <> tableRef
          <> " IS "
          <> textLiteral eventStreamingObjectComment
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

-- | Drop current and legacy event-streaming triggers for a queue and its DLQ.
-- The shared function is dropped separately after every queue is detached.
dropEventStreamingTriggersSQL :: SchemaName -> TableName -> Text
dropEventStreamingTriggersSQL schemaName tableName =
  let tbl = jobQueueTable schemaName tableName
      dlqTbl = jobQueueDLQTable schemaName tableName
      dropTrigger name tableRef = "DROP TRIGGER IF EXISTS " <> quoteIdentifier name <> " ON " <> tableRef <> ";"
   in T.unlines $
        map (\(name, isDLQ) -> dropTrigger name (if isDLQ then dlqTbl else tbl)) legacyEventStreamingTriggers
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

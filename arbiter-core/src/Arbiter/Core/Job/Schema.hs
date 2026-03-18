{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | SQL generation functions for job queue schemas. No database execution happens here.
module Arbiter.Core.Job.Schema
  ( -- * Schema Creation
    createSchemaSQL
  , defaultSchemaName

    -- * Table Creation SQL
  , createJobQueueTableSQL
  , createJobQueueDLQTableSQL

    -- * Index Creation SQL
  , createJobQueueGroupKeyIndexSQL
  , createJobQueueUngroupedRankingIndexSQL
  , createDLQGroupKeyIndexSQL
  , createDLQFailedAtIndexSQL
  , createDLQParentIdIndexSQL
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

    -- * Trigger / Function Name Helpers
  , notifyFunctionName
  , notifyTriggerName
  , eventStreamingFunctionName
  , eventStreamingTriggerName
  , eventStreamingDLQTriggerName

    -- * Table Name Helpers
  , jobQueueTable
  , jobQueueDLQTable
  , jobQueueResultsTable
  , jobQueueGroupsTable
  , jobQueueReaperSeq

    -- * Reaper Coordination
  , createReaperSeqSQL

    -- * Results Table
  , createResultsTableSQL

    -- * Groups Table
  , createGroupsTableSQL
  , createGroupsIndexSQL

    -- * Groups Trigger SQL
  , createGroupsTriggerFunctionsSQL
  , createGroupsTriggersSQL

    -- * Identifier Quoting
  , quoteIdentifier
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

-- | Quote a PostgreSQL identifier (schema name, table name, column name).
--
-- This escapes double quotes by doubling them and wraps the identifier in quotes.
-- This prevents SQL injection when using dynamic identifiers.
quoteIdentifier :: Text -> Text
quoteIdentifier ident =
  "\"" <> T.replace "\"" "\"\"" ident <> "\""

-- | Default PostgreSQL schema name for Arbiter tables
--
-- Using a dedicated schema prevents namespace pollution in the user's public schema.
defaultSchemaName :: Text
defaultSchemaName = "arbiter"

-- | Generate notification channel name for a table
--
-- Each table gets its own NOTIFY channel for job insertions.
-- Example: notificationChannelForTable "email_jobs" -> "email_jobs_created"
notificationChannelForTable :: Text -> Text
notificationChannelForTable tableName = tableName <> "_created"

-- | Channel name used by the event streaming (SSE) system.
eventStreamingChannel :: Text
eventStreamingChannel = "arbiter_job_events"

-- | Per-table NOTIFY trigger function name.
notifyFunctionName :: Text -> Text
notifyFunctionName tableName = "notify_" <> tableName <> "_created"

-- | Per-table NOTIFY trigger name.
notifyTriggerName :: Text -> Text
notifyTriggerName tableName = tableName <> "_notify_trigger"

-- | Shared event streaming trigger function name (one per schema).
eventStreamingFunctionName :: Text
eventStreamingFunctionName = "notify_job_event"

-- | Per-table event streaming trigger name.
eventStreamingTriggerName :: Text -> Text
eventStreamingTriggerName tableName = "notify_job_event_" <> tableName

-- | Per-table DLQ event streaming trigger name.
eventStreamingDLQTriggerName :: Text -> Text
eventStreamingDLQTriggerName tableName = "notify_job_event_" <> tableName <> "_dlq"

-- | Qualified table name: @jobQueueTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs"@
jobQueueTable :: Text -> Text -> Text
jobQueueTable schemaName tableName = quoteIdentifier schemaName <> "." <> quoteIdentifier tableName

-- | Qualified DLQ table name: @jobQueueDLQTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs_dlq"@
jobQueueDLQTable :: Text -> Text -> Text
jobQueueDLQTable schemaName tableName = quoteIdentifier schemaName <> "." <> quoteIdentifier (tableName <> "_dlq")

-- | Qualified results table name: @jobQueueResultsTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs_results"@
jobQueueResultsTable :: Text -> Text -> Text
jobQueueResultsTable schemaName tableName = quoteIdentifier schemaName <> "." <> quoteIdentifier (tableName <> "_results")

-- | Qualified groups table name: @jobQueueGroupsTable "arbiter" "email_jobs"@ -> @"arbiter"."email_jobs_groups"@
jobQueueGroupsTable :: Text -> Text -> Text
jobQueueGroupsTable schemaName tableName = quoteIdentifier schemaName <> "." <> quoteIdentifier (tableName <> "_groups")

-- | Qualified reaper sequence name.
jobQueueReaperSeq :: Text -> Text -> Text
jobQueueReaperSeq schemaName tableName = quoteIdentifier schemaName <> "." <> quoteIdentifier (tableName <> "_reaper_seq")

-- | SQL to create the reaper coordination sequence.
-- Stores the epoch (seconds) of the last reaper run.
createReaperSeqSQL :: Text -> Text -> Text
createReaperSeqSQL schemaName tableName =
  let seqName = jobQueueReaperSeq schemaName tableName
   in "CREATE SEQUENCE IF NOT EXISTS " <> seqName <> " START WITH 0 MINVALUE 0;"

-- | SQL to create the schema for Arbiter tables
createSchemaSQL :: Text -> Text
createSchemaSQL schemaName =
  "CREATE SCHEMA IF NOT EXISTS " <> quoteIdentifier schemaName <> ";"

-- | Common job column definitions (matches the Job type structure)
--
-- These columns are shared between job_queue and dead_letter_queue tables.
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
-- Completed jobs are deleted, failed jobs are moved to the DLQ.
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

createJobQueueUngroupedRankingIndexSQL :: Text -> Text -> Text
createJobQueueUngroupedRankingIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_ungrouped_ranking")
    , "ON " <> jobQueueTable schemaName tableName <> " (priority ASC, id ASC)"
    , "WHERE group_key IS NULL;"
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

-- | SQL to create a ranking index on the groups table.
--
-- Non-partial so that @UPDATE SET job_count@, @in_flight_until@ etc. never
-- change indexed columns — enabling HOT updates on every ack and claim.
createGroupsIndexSQL :: Text -> Text -> Text
createGroupsIndexSQL schemaName tableName =
  let groupsTbl = jobQueueGroupsTable schemaName tableName
   in T.unlines
        [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_groups_ranking")
        , "ON " <> groupsTbl <> " (min_priority ASC, min_id ASC);"
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
      funcInsert = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> "_insert")
      funcDelete = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> "_delete")
      funcUpdate = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> "_update")
      dd = "$$"
   in T.unlines
        [ groupsInsertFunction funcInsert groupsTbl dd
        , groupsDeleteFunction funcDelete groupsTbl tbl dd
        , groupsUpdateFunction funcUpdate groupsTbl tbl dd
        ]

groupsInsertFunction :: Text -> Text -> Text -> Text
groupsInsertFunction funcName groupsTbl dd =
  [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM new_table WHERE group_key IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count)
      SELECT group_key, MIN(priority), MIN(id), COUNT(*)
      FROM new_table
      WHERE group_key IS NOT NULL
      GROUP BY group_key
      ON CONFLICT (group_key) DO UPDATE SET
        min_priority = LEAST(${groupsTbl}.min_priority, EXCLUDED.min_priority),
        min_id = LEAST(${groupsTbl}.min_id, EXCLUDED.min_id),
        job_count = ${groupsTbl}.job_count + EXCLUDED.job_count,
        in_flight_until = CASE WHEN ${groupsTbl}.in_flight_until <= NOW()
          THEN NULL ELSE ${groupsTbl}.in_flight_until END;

      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

groupsDeleteFunction :: Text -> Text -> Text -> Text -> Text
groupsDeleteFunction funcName groupsTbl tbl dd =
  [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM old_table WHERE group_key IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      UPDATE ${groupsTbl} g
      SET job_count = g.job_count - sub.removed_count,
          min_priority = COALESCE(sub.new_min_priority, g.min_priority),
          min_id = COALESCE(sub.new_min_id, g.min_id),
          in_flight_until = CASE
            WHEN sub.had_inflight THEN NULL
            ELSE g.in_flight_until
          END
      FROM (
        SELECT d.group_key, d.removed_count, d.had_inflight,
          MIN(t.priority) AS new_min_priority, MIN(t.id) AS new_min_id
        FROM (
          SELECT group_key, COUNT(*) AS removed_count,
            bool_or(not_visible_until > NOW() AND NOT suspended) AS had_inflight
          FROM old_table
          WHERE group_key IS NOT NULL
          GROUP BY group_key
        ) d
        LEFT JOIN ${tbl} t ON t.group_key = d.group_key
        GROUP BY d.group_key, d.removed_count, d.had_inflight
      ) sub
      WHERE g.group_key = sub.group_key;

      DELETE FROM ${groupsTbl}
      WHERE job_count <= 0
        AND group_key IN (SELECT group_key FROM old_table WHERE group_key IS NOT NULL);

      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

groupsUpdateFunction :: Text -> Text -> Text -> Text -> Text
groupsUpdateFunction funcName groupsTbl tbl dd =
  [text|
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

      -- Step 1: Fast path — extend in_flight_until when not_visible_until increases (claim, retry)
      UPDATE ${groupsTbl} g
      SET in_flight_until = GREATEST(g.in_flight_until, sub.new_ift)
      FROM (
        SELECT n.group_key, MAX(n.not_visible_until) AS new_ift
        FROM new_table n
        JOIN old_table o ON o.id = n.id
        WHERE n.group_key IS NOT NULL
          AND n.not_visible_until > NOW()
          AND NOT n.suspended
          AND (o.not_visible_until IS NULL OR o.not_visible_until <= NOW()
               OR n.not_visible_until > o.not_visible_until)
        GROUP BY n.group_key
      ) sub
      WHERE g.group_key = sub.group_key;

      -- Step 2: Full rescan — recompute in_flight_until when not_visible_until decreases or suspended changes
      UPDATE ${groupsTbl} g
      SET in_flight_until = sub.new_ift
      FROM (
        SELECT t.group_key,
          MAX(t.not_visible_until) FILTER (
            WHERE t.not_visible_until > NOW() AND NOT t.suspended
          ) AS new_ift
        FROM ${tbl} t
        WHERE t.group_key IN (
          SELECT n.group_key FROM new_table n
          JOIN old_table o ON o.id = n.id
          WHERE n.group_key IS NOT NULL
            AND (o.not_visible_until IS DISTINCT FROM n.not_visible_until
                 OR o.suspended IS DISTINCT FROM n.suspended)
            AND (
              n.not_visible_until > NOW() AND NOT n.suspended
              AND (o.not_visible_until IS NULL OR o.not_visible_until <= NOW()
                   OR n.not_visible_until > o.not_visible_until)
            ) IS NOT TRUE
        )
        GROUP BY t.group_key
      ) sub
      WHERE g.group_key = sub.group_key
        AND g.in_flight_until IS DISTINCT FROM sub.new_ift;

      -- Step 3: Handle group_key changes (dedup replace) — remove from old group
      UPDATE ${groupsTbl} g
      SET job_count = g.job_count - sub.cnt,
          min_priority = COALESCE(sub.new_min_priority, g.min_priority),
          min_id = COALESCE(sub.new_min_id, g.min_id),
          in_flight_until = CASE
            WHEN sub.had_inflight THEN NULL
            ELSE g.in_flight_until
          END
      FROM (
        SELECT d.group_key, d.cnt, d.had_inflight,
          MIN(t.priority) AS new_min_priority, MIN(t.id) AS new_min_id
        FROM (
          SELECT o.group_key, COUNT(*) AS cnt,
            bool_or(o.not_visible_until > NOW() AND NOT o.suspended) AS had_inflight
          FROM old_table o
          JOIN new_table n ON o.id = n.id
          WHERE o.group_key IS NOT NULL
            AND o.group_key IS DISTINCT FROM n.group_key
          GROUP BY o.group_key
        ) d
        LEFT JOIN ${tbl} t ON t.group_key = d.group_key
        GROUP BY d.group_key, d.cnt, d.had_inflight
      ) sub
      WHERE g.group_key = sub.group_key;

      DELETE FROM ${groupsTbl}
      WHERE job_count <= 0
        AND group_key IN (
          SELECT o.group_key FROM old_table o
          JOIN new_table n ON o.id = n.id
          WHERE o.group_key IS NOT NULL
            AND o.group_key IS DISTINCT FROM n.group_key
        );

      -- Step 4: Handle group_key changes — add to new group
      INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count)
      SELECT n.group_key, MIN(n.priority), MIN(n.id), COUNT(*)
      FROM new_table n
      JOIN old_table o ON o.id = n.id
      WHERE n.group_key IS NOT NULL
        AND o.group_key IS DISTINCT FROM n.group_key
      GROUP BY n.group_key
      ON CONFLICT (group_key) DO UPDATE SET
        min_priority = LEAST(${groupsTbl}.min_priority, EXCLUDED.min_priority),
        min_id = LEAST(${groupsTbl}.min_id, EXCLUDED.min_id),
        job_count = ${groupsTbl}.job_count + EXCLUDED.job_count;

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
  let tbl = jobQueueTable schemaName tableName
      baseName = "maintain_" <> tableName <> "_groups"
      funcInsert = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> "_insert")
      funcDelete = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> "_delete")
      funcUpdate = quoteIdentifier schemaName <> "." <> quoteIdentifier (baseName <> "_update")
      trigInsert = quoteIdentifier (baseName <> "_insert")
      trigDelete = quoteIdentifier (baseName <> "_delete")
      trigUpdate = quoteIdentifier (baseName <> "_update")
   in T.unlines
        [ "DROP TRIGGER IF EXISTS " <> trigInsert <> " ON " <> tbl <> ";"
        , "CREATE TRIGGER " <> trigInsert
        , "AFTER INSERT ON " <> tbl
        , "REFERENCING NEW TABLE AS new_table"
        , "FOR EACH STATEMENT EXECUTE FUNCTION " <> funcInsert <> "();"
        , ""
        , "DROP TRIGGER IF EXISTS " <> trigDelete <> " ON " <> tbl <> ";"
        , "CREATE TRIGGER " <> trigDelete
        , "AFTER DELETE ON " <> tbl
        , "REFERENCING OLD TABLE AS old_table"
        , "FOR EACH STATEMENT EXECUTE FUNCTION " <> funcDelete <> "();"
        , ""
        , "DROP TRIGGER IF EXISTS " <> trigUpdate <> " ON " <> tbl <> ";"
        , "CREATE TRIGGER " <> trigUpdate
        , "AFTER UPDATE ON " <> tbl
        , "REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table"
        , "FOR EACH STATEMENT EXECUTE FUNCTION " <> funcUpdate <> "();"
        ]

-- | SQL to create the NOTIFY function for a specific table
--
-- This function is triggered after job insertion and sends a notification on a
-- table-specific channel. Each table gets its own function and channel for isolation.
--
-- Note: The channel name is quoted as a string literal (single quotes), not an identifier.
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

-- | SQL to create the event streaming notification function
--
-- This is a shared function (one per schema) that fires on INSERT, UPDATE,
-- DELETE of any job table and INSERT on any DLQ table. It sends a lightweight
-- JSON event via @pg_notify@ on the @arbiter_job_events@ channel with just
-- the event type, table name, and job ID.
--
-- The function uses @TG_TABLE_NAME@ and @TG_OP@ to determine context, so it
-- works for all tables without per-table copies.
createEventStreamingFunctionSQL :: Text -> Text
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

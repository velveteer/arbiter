{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Schema for the per-queue group summary table and the statement-level triggers
-- that maintain it. The summary carries each group's head, counts and visibility
-- deadlines so the claim ranks groups without scanning their jobs.
module Arbiter.Core.Job.Schema.Groups
  ( -- * Grouped Job Indexes
    createJobQueueGroupKeyIndexSQL
  , createJobQueueGroupRetriedIndexSQL
  , createJobQueueGroupedDueIndexSQL
  , createJobQueueGroupInFlightIndexSQL

    -- * Groups Table SQL
  , createGroupsTableSQL
  , migrateGroupsReadyRankingSQL
  , createGroupsEmptiedIndexSQL

    -- * Summary Column Definitions
  , groupAggregates
  , inFlightPredicate

    -- * Groups Trigger SQL
  , createGroupsTriggerFunctionsSQL
  , createGroupsTriggersSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Job.Schema
  ( createMaintenanceTriggersSQL
  , jobQueueGroupsTable
  , jobQueueTable
  , maintenanceFunctionNames
  )
import Arbiter.Core.SqlLiterals (quoteIdentifier)

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

-- | Scheduled grouped jobs by due time. Group maintenance uses this index to
-- replace @next_due@ with one point lookup.
createJobQueueGroupedDueIndexSQL :: Text -> Text -> Text
createJobQueueGroupedDueIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_grouped_due")
    , "ON " <> jobQueueTable schemaName tableName <> " (group_key, not_visible_until ASC)"
    , "WHERE group_key IS NOT NULL AND not_visible_until IS NOT NULL AND NOT suspended;"
    ]

-- | Possible in-flight grouped jobs by descending lease deadline. The query
-- applies the time-dependent part of the predicate at runtime.
createJobQueueGroupInFlightIndexSQL :: Text -> Text -> Text
createJobQueueGroupInFlightIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_group_in_flight")
    , "ON " <> jobQueueTable schemaName tableName <> " (group_key, not_visible_until DESC NULLS LAST)"
    , "WHERE group_key IS NOT NULL AND not_visible_until IS NOT NULL AND NOT suspended AND (attempts > 0 OR throttled_until IS NOT NULL);"
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

-- | The @ON CONFLICT@ merge of newly grouped rows into an existing group summary.
-- The head is whichever side ranks first as a whole @(min_priority, min_id)@ pair.
groupsMergeSet :: Text -> Text
groupsMergeSet groupsTbl =
  let takesHead =
        [text|${groupsTbl}.job_count = 0 OR (EXCLUDED.min_priority, EXCLUDED.min_id) < (${groupsTbl}.min_priority, ${groupsTbl}.min_id)|]
   in [text|
    min_priority = CASE WHEN ${takesHead} THEN EXCLUDED.min_priority
      ELSE ${groupsTbl}.min_priority END,
    min_id = CASE WHEN ${takesHead} THEN EXCLUDED.min_id
      ELSE ${groupsTbl}.min_id END,
    job_count = ${groupsTbl}.job_count + EXCLUDED.job_count,
    ready_count = ${groupsTbl}.ready_count + EXCLUDED.ready_count,
    next_due = LEAST(${groupsTbl}.next_due, EXCLUDED.next_due)
  |]

-- | Lock a queue's group summaries in @group_key@ order. Every maintenance function
-- takes them this way, so two concurrent statements cannot deadlock on them.
groupsLock :: Text -> Text -> Text
groupsLock groupsTbl keys =
  [text|
    PERFORM 1 FROM ${groupsTbl} g
    WHERE g.group_key IN (${keys})
    ORDER BY g.group_key FOR UPDATE;
  |]

groupsInsertFunction :: Text -> Text -> Text -> Text
groupsInsertFunction funcName groupsTbl dd =
  let mergeSet = groupsMergeSet groupsTbl
      aggs = groupAggregates ""
      lockRows = groupsLock groupsTbl "SELECT group_key FROM new_table WHERE group_key IS NOT NULL"
   in [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM new_table WHERE group_key IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      ${lockRows}

      INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count, ready_count, next_due)
      SELECT group_key,
        ${aggs}
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

-- | The group summary aggregates over job rows grouped by @group_key@. @min_id@ is the
-- id of the head row, the one the claim ranks first. @col@ prefixes each column.
groupAggregates :: Text -> Text
groupAggregates col =
  let ready = readyPredicate col
      scheduled = scheduledPredicate col
   in [text|
    MIN(${col}priority) AS min_priority,
    (MIN(ARRAY[${col}priority::bigint, ${col}id]))[2] AS min_id,
    COUNT(*) AS job_count,
    COUNT(*) FILTER (WHERE ${ready}) AS ready_count,
    MIN(${col}not_visible_until) FILTER (WHERE ${scheduled}) AS next_due
  |]

-- | Whether a job counts toward its group's ready total. @col@ prefixes each column.
readyPredicate :: Text -> Text
readyPredicate col = col <> "not_visible_until IS NULL AND NOT " <> col <> "suspended"

-- | Whether a job's deadline counts toward its group's next due time. @col@ prefixes
-- each column.
scheduledPredicate :: Text -> Text
scheduledPredicate col = col <> "not_visible_until IS NOT NULL AND NOT " <> col <> "suspended"

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

-- | Whether an updated row changes a value some group summary reads, its
-- @group_key@ aside.
summaryValueChanged :: Text
summaryValueChanged =
  "n.priority IS DISTINCT FROM o.priority"
    <> " OR n.not_visible_until IS DISTINCT FROM o.not_visible_until"
    <> " OR n.suspended IS DISTINCT FROM o.suspended"
    <> " OR n.attempts IS DISTINCT FROM o.attempts"
    <> " OR n.throttled_until IS DISTINCT FROM o.throttled_until"

-- | Apply a group's count deltas and replace all four extrema with indexed point
-- lookups. @deltaRows@ supplies @group_key@, @count_delta@ and @ready_delta@. Each
-- lookup reads one index entry and stops. A group whose summary comes out unchanged
-- keeps its row, which no update to this table can rewrite in place.
groupsSnapshotUpdate :: Text -> Text -> Text -> Text
groupsSnapshotUpdate groupsTbl tbl deltaRows =
  let scheduled = scheduledPredicate "q."
      inFlight = inFlightPredicate "q."
      emptied = "hp.priority IS NULL"
   in [text|
    WITH t AS (
      SELECT g0.group_key,
        CASE WHEN ${emptied} THEN 0 ELSE GREATEST(0, g0.job_count + d.count_delta) END AS job_count,
        CASE WHEN ${emptied} THEN 0 ELSE GREATEST(0, g0.ready_count + d.ready_delta) END AS ready_count,
        COALESCE(hp.priority, 0) AS min_priority,
        COALESCE(hp.id, 0) AS min_id,
        nd.not_visible_until AS next_due,
        fi.not_visible_until AS in_flight_until
      FROM (
        ${deltaRows}
      ) d
      JOIN ${groupsTbl} g0 ON g0.group_key = d.group_key
      LEFT JOIN LATERAL (
        SELECT q.priority, q.id FROM ${tbl} q
        WHERE q.group_key = d.group_key
        ORDER BY q.priority ASC, q.id ASC LIMIT 1
      ) hp ON TRUE
      LEFT JOIN LATERAL (
        SELECT q.not_visible_until FROM ${tbl} q
        WHERE q.group_key = d.group_key AND ${scheduled}
        ORDER BY q.not_visible_until ASC LIMIT 1
      ) nd ON TRUE
      LEFT JOIN LATERAL (
        SELECT q.not_visible_until FROM ${tbl} q
        WHERE q.group_key = d.group_key AND ${inFlight}
        ORDER BY q.not_visible_until DESC NULLS LAST LIMIT 1
      ) fi ON TRUE
    )
    UPDATE ${groupsTbl} g
    SET job_count = t.job_count,
      ready_count = t.ready_count,
      min_priority = t.min_priority,
      min_id = t.min_id,
      next_due = t.next_due,
      in_flight_until = t.in_flight_until
    FROM t
    WHERE g.group_key = t.group_key
      AND (g.job_count, g.ready_count, g.min_priority, g.min_id, g.next_due, g.in_flight_until)
          IS DISTINCT FROM (t.job_count, t.ready_count, t.min_priority, t.min_id, t.next_due, t.in_flight_until);
  |]

-- | Removed rows leave their group summary.
groupsDeleteFunction :: Text -> Text -> Text -> Text -> Text
groupsDeleteFunction funcName groupsTbl tbl dd =
  let ready = readyPredicate ""
      lockRows = groupsLock groupsTbl "SELECT group_key FROM old_table WHERE group_key IS NOT NULL"
      removeUpdate =
        groupsSnapshotUpdate
          groupsTbl
          tbl
          [text|
            SELECT group_key, (-COUNT(*))::int AS count_delta,
              (-COUNT(*) FILTER (WHERE ${ready}))::int AS ready_delta
            FROM old_table WHERE group_key IS NOT NULL GROUP BY group_key
          |]
   in [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM old_table WHERE group_key IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      ${lockRows}
      ${removeUpdate}
      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

-- | Updated rows keep their group unless a dedup replace moved them. A moved row
-- leaves the old summary and merges into the new one. Counts come from the
-- transition tables, everything else from the snapshot lookups.
groupsUpdateFunction :: Text -> Text -> Text -> Text -> Text
groupsUpdateFunction funcName groupsTbl tbl dd =
  let valueChanged = summaryValueChanged
      changed = "n.group_key IS DISTINCT FROM o.group_key OR " <> valueChanged
      readyOld = readyPredicate "o."
      readyNew = readyPredicate "n."
      aggsN = groupAggregates "n."
      mergeSet = groupsMergeSet groupsTbl
      lockRows =
        groupsLock
          groupsTbl
          [text|
            SELECT group_key
            FROM new_table
            WHERE group_key IS NOT NULL
            UNION
            SELECT group_key
            FROM old_table
            WHERE group_key IS NOT NULL
          |]
      departureUpdate =
        groupsSnapshotUpdate
          groupsTbl
          tbl
          [text|
            SELECT o.group_key, (-COUNT(*))::int AS count_delta,
              (-COUNT(*) FILTER (WHERE ${readyOld}))::int AS ready_delta
            FROM old_table o JOIN new_table n ON n.id = o.id
            WHERE o.group_key IS NOT NULL AND o.group_key IS DISTINCT FROM n.group_key
            GROUP BY o.group_key
          |]
      arrivalUpdate =
        groupsSnapshotUpdate
          groupsTbl
          tbl
          [text|
            SELECT n.group_key, 0 AS count_delta, 0 AS ready_delta
            FROM new_table n JOIN old_table o ON o.id = n.id
            WHERE n.group_key IS NOT NULL AND o.group_key IS DISTINCT FROM n.group_key
            GROUP BY n.group_key
          |]
      sameGroupUpdate =
        groupsSnapshotUpdate
          groupsTbl
          tbl
          [text|
            SELECT n.group_key, 0 AS count_delta,
              SUM((${readyNew})::int - (${readyOld})::int)::int AS ready_delta
            FROM new_table n JOIN old_table o ON o.id = n.id
            WHERE n.group_key IS NOT NULL
              AND n.group_key IS NOT DISTINCT FROM o.group_key
              AND (${valueChanged})
            GROUP BY n.group_key
          |]
   in [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM new_table WHERE group_key IS NOT NULL LIMIT 1)
         AND NOT EXISTS (SELECT 1 FROM old_table WHERE group_key IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      IF NOT EXISTS (
        SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE (n.group_key IS NOT NULL OR o.group_key IS NOT NULL)
          AND (${changed})
        LIMIT 1
      ) THEN
        RETURN NULL;
      END IF;

      ${lockRows}
      IF EXISTS (
        SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE o.group_key IS DISTINCT FROM n.group_key LIMIT 1
      ) THEN
        ${departureUpdate}

        INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count, ready_count, next_due)
        SELECT n.group_key,
          ${aggsN}
        FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE n.group_key IS NOT NULL AND o.group_key IS DISTINCT FROM n.group_key
        GROUP BY n.group_key ORDER BY n.group_key
        ON CONFLICT (group_key) DO UPDATE SET ${mergeSet};

        ${arrivalUpdate}
      END IF;
      ${sameGroupUpdate}
      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

-- | Group maintenance with indexed extremum replacement. Transition tables
-- supply count deltas. No update or delete scans all rows in an affected group.
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

-- | The three statement-level AFTER triggers calling a queue's groups maintenance
-- functions, each handed its affected rows through a transition table.
createGroupsTriggersSQL :: Text -> Text -> Text
createGroupsTriggersSQL schemaName tableName =
  createMaintenanceTriggersSQL schemaName (jobQueueTable schemaName tableName) ("maintain_" <> tableName <> "_groups")

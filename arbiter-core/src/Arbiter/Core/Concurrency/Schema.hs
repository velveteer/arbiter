{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | DDL for the concurrency-limit feature: the seeded per-prefix pool policies (with
-- the operator override), the global per-key count table, job columns, and the
-- per-queue delta triggers that maintain the count. No database execution here.
module Arbiter.Core.Concurrency.Schema
  ( -- * Table names
    arbiterConcurrencyTable
  , arbiterConcurrencyPoliciesTable

    -- * SQL fragments
  , concurrencyAdvisoryLockExpr

    -- * DDL
  , createConcurrencyPoliciesTableSQL
  , createConcurrencyTableSQL
  , addConcurrencyColumnsSQL
  , createConcurrencyIndexSQL
  , createConcurrencyTriggerFunctionsSQL
  , createConcurrencyTriggersSQL
  , upsertConcurrencyPolicyRowSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Admission (policyUpsertSQL)
import Arbiter.Core.Concurrency.Spec (ConcurrencyPolicy (..))
import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , createMaintenanceTriggersSQL
  , jobQueueDLQTable
  , jobQueueTable
  , maintenanceFunctionNames
  )
import Arbiter.Core.SqlLiterals (intLiteral, quoteIdentifier, textLiteral)

-- | Qualified name of the global per-key in-flight count table.
arbiterConcurrencyTable :: SchemaName -> Text
arbiterConcurrencyTable schemaName =
  quoteIdentifier schemaName <> ".arbiter_concurrency"

-- | Qualified name of the app-global pool policies table.
arbiterConcurrencyPoliciesTable :: SchemaName -> Text
arbiterConcurrencyPoliciesTable schemaName =
  quoteIdentifier schemaName <> ".arbiter_concurrency_policies"

-- | The per-key advisory lock id, identical for the enqueue trigger and the prune.
concurrencyAdvisoryLockExpr :: Text -> Text
concurrencyAdvisoryLockExpr key =
  "hashtextextended('arbiter_conc:' || " <> key <> ", 0)"

-- | DDL for the pool policies table. @default_limit@ is migration-owned,
-- @override_limit@ management-owned. Effective cap @COALESCE(override, default)@.
createConcurrencyPoliciesTableSQL :: SchemaName -> Text
createConcurrencyPoliciesTableSQL schemaName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> arbiterConcurrencyPoliciesTable schemaName <> " ("
    , "  prefix_id TEXT PRIMARY KEY,"
    , "  default_limit INTEGER NOT NULL CHECK (default_limit > 0),"
    , "  override_limit INTEGER CHECK (override_limit >= 0)"
    , ");"
    ]

-- | The global per-key @UNLOGGED@ count table, maintained by the delta triggers.
createConcurrencyTableSQL :: SchemaName -> Text
createConcurrencyTableSQL schemaName =
  T.unlines
    [ "CREATE UNLOGGED TABLE IF NOT EXISTS " <> arbiterConcurrencyTable schemaName <> " ("
    , "  concurrency_key TEXT PRIMARY KEY,"
    , "  concurrency_prefix TEXT NOT NULL,"
    , "  in_flight INTEGER NOT NULL DEFAULT 0"
    , ") WITH (fillfactor = 80);"
    ]

-- | Migration adding the concurrency columns to a queue's job and DLQ tables. The
-- key is @prefix:suffix@. The prefix is kept separate to join the pool policy.
addConcurrencyColumnsSQL :: SchemaName -> TableName -> Text
addConcurrencyColumnsSQL schemaName tableName =
  T.unlines
    [ "ALTER TABLE " <> jobQueueTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS concurrency_key TEXT;"
    , "ALTER TABLE " <> jobQueueTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS concurrency_prefix TEXT;"
    , "ALTER TABLE " <> jobQueueDLQTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS concurrency_key TEXT;"
    , "ALTER TABLE " <> jobQueueDLQTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS concurrency_prefix TEXT;"
    ]

-- | Index backing the per-key in-flight recount, over claimed jobs only.
createConcurrencyIndexSQL :: SchemaName -> TableName -> Text
createConcurrencyIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_concurrency")
    , "ON " <> jobQueueTable schemaName tableName <> " (concurrency_key)"
    , "WHERE concurrency_key IS NOT NULL;"
    ]

-- | Per-queue triggers maintaining each key's @in_flight@.
createConcurrencyTriggerFunctionsSQL :: SchemaName -> TableName -> Text
createConcurrencyTriggerFunctionsSQL schemaName tableName =
  let concTbl = arbiterConcurrencyTable schemaName
      baseName = "maintain_" <> tableName <> "_concurrency"
      (funcInsert, funcDelete, funcUpdate) = maintenanceFunctionNames schemaName baseName
      dd = "$$"
   in T.unlines
        [ concurrencyInsertFunction funcInsert concTbl dd
        , concurrencyDeleteFunction funcDelete concTbl dd
        , concurrencyUpdateFunction funcUpdate concTbl dd
        ]

-- | Seed a count row only for a fresh key. A per-key shared advisory lock makes a concurrent prune skip the key, touching no count tuple on a steady-state enqueue.
concurrencyInsertFunction :: Text -> Text -> Text -> Text
concurrencyInsertFunction funcName concTbl dd =
  let lockExpr = concurrencyAdvisoryLockExpr "t.k"
   in [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM new_table WHERE concurrency_key IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      PERFORM pg_advisory_xact_lock_shared(${lockExpr})
      FROM (SELECT DISTINCT concurrency_key AS k FROM new_table WHERE concurrency_key IS NOT NULL) t;

      INSERT INTO ${concTbl} (concurrency_key, concurrency_prefix)
      SELECT n.concurrency_key, MAX(n.concurrency_prefix)
      FROM new_table n
      WHERE n.concurrency_key IS NOT NULL
        AND NOT EXISTS (SELECT 1 FROM ${concTbl} c WHERE c.concurrency_key = n.concurrency_key)
      GROUP BY n.concurrency_key
      ORDER BY n.concurrency_key
      ON CONFLICT (concurrency_key) DO NOTHING;

      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

concurrencyDeleteFunction :: Text -> Text -> Text -> Text
concurrencyDeleteFunction funcName concTbl dd =
  [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM old_table WHERE concurrency_key IS NOT NULL AND claimed_by IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      -- Lock affected count rows in key order to avoid deadlock with concurrent triggers.
      -- Only claimed rows shift in_flight, so unclaimed deletions touch no count row.
      PERFORM 1 FROM ${concTbl} a
      WHERE a.concurrency_key IN (SELECT concurrency_key FROM old_table WHERE concurrency_key IS NOT NULL AND claimed_by IS NOT NULL)
      ORDER BY a.concurrency_key
      FOR UPDATE;

      WITH deltas AS (
        SELECT concurrency_key AS key,
               COUNT(*) AS inflight_delta
        FROM old_table
        WHERE concurrency_key IS NOT NULL AND claimed_by IS NOT NULL
        GROUP BY concurrency_key
      )
      UPDATE ${concTbl} a
      SET in_flight = GREATEST(0, a.in_flight - d.inflight_delta)
      FROM deltas d
      WHERE a.concurrency_key = d.key;

      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

concurrencyUpdateFunction :: Text -> Text -> Text -> Text
concurrencyUpdateFunction funcName concTbl dd =
  [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dd}
    BEGIN
      -- Only a claimed_by flip (shifts in_flight) or a concurrency_key move (a dedup
      -- replace, shifts in_flight between keys) touches in_flight. A heartbeat or other
      -- update leaves it unchanged, so skip it before locking.
      IF NOT EXISTS (
        SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE (n.concurrency_key IS NOT NULL OR o.concurrency_key IS NOT NULL)
          AND (n.claimed_by IS DISTINCT FROM o.claimed_by
               OR n.concurrency_key IS DISTINCT FROM o.concurrency_key)
        LIMIT 1
      ) THEN
        RETURN NULL;
      END IF;

      -- Lock old and new keys' count rows in key order to avoid deadlock, but only
      -- for rows that shift in_flight. An updated row with no claimed_by flip or key
      -- move (a claim's throttle deferral) may reference a key another claimer holds,
      -- and blocking on it here would invert the claim's lock order.
      PERFORM 1 FROM ${concTbl} a
      WHERE a.concurrency_key IN (
        SELECT o.concurrency_key FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE o.concurrency_key IS NOT NULL
          AND (n.claimed_by IS DISTINCT FROM o.claimed_by
               OR n.concurrency_key IS DISTINCT FROM o.concurrency_key)
        UNION
        SELECT n.concurrency_key FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE n.concurrency_key IS NOT NULL
          AND (n.claimed_by IS DISTINCT FROM o.claimed_by
               OR n.concurrency_key IS DISTINCT FROM o.concurrency_key)
      )
      ORDER BY a.concurrency_key
      FOR UPDATE;

      -- Same key: only in_flight shifts by the claimed_by delta.
      WITH deltas AS (
        SELECT n.concurrency_key AS key,
               SUM((n.claimed_by IS NOT NULL)::int - (o.claimed_by IS NOT NULL)::int) AS inflight_delta
        FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE n.concurrency_key IS NOT NULL
          AND n.concurrency_key IS NOT DISTINCT FROM o.concurrency_key
        GROUP BY n.concurrency_key
      )
      UPDATE ${concTbl} a
      SET in_flight = GREATEST(0, a.in_flight + d.inflight_delta)
      FROM deltas d
      WHERE a.concurrency_key = d.key AND d.inflight_delta <> 0;

      -- Skip both key-move branches when no key changed.
      IF EXISTS (
        SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE n.concurrency_key IS DISTINCT FROM o.concurrency_key
          AND (n.concurrency_key IS NOT NULL OR o.concurrency_key IS NOT NULL)
        LIMIT 1
      ) THEN
        -- Key move: remove the row's in_flight from the old key.
        WITH deltas AS (
          SELECT o.concurrency_key AS key,
                 SUM((o.claimed_by IS NOT NULL)::int) AS inflight_delta
          FROM old_table o JOIN new_table n ON o.id = n.id
          WHERE o.concurrency_key IS NOT NULL
            AND o.concurrency_key IS DISTINCT FROM n.concurrency_key
          GROUP BY o.concurrency_key
        )
        UPDATE ${concTbl} a
        SET in_flight = GREATEST(0, a.in_flight - d.inflight_delta)
        FROM deltas d
        WHERE a.concurrency_key = d.key;

        -- Key move: add the row's in_flight to the new key, creating its row if absent.
        INSERT INTO ${concTbl} (concurrency_key, concurrency_prefix, in_flight)
        SELECT n.concurrency_key,
               MAX(n.concurrency_prefix),
               SUM((n.claimed_by IS NOT NULL)::int)
        FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE n.concurrency_key IS NOT NULL
          AND n.concurrency_key IS DISTINCT FROM o.concurrency_key
        GROUP BY n.concurrency_key
        ORDER BY n.concurrency_key
        ON CONFLICT (concurrency_key) DO UPDATE SET
          in_flight = ${concTbl}.in_flight + EXCLUDED.in_flight;
      END IF;

      RETURN NULL;
    END;
    ${dd} LANGUAGE plpgsql;
  |]

-- | SQL to create the 3 statement-level AFTER triggers on a queue's job table.
createConcurrencyTriggersSQL :: SchemaName -> TableName -> Text
createConcurrencyTriggersSQL schemaName tableName =
  createMaintenanceTriggersSQL
    schemaName
    (jobQueueTable schemaName tableName)
    ("maintain_" <> tableName <> "_concurrency")

-- | Upsert a pool's @default_limit@, leaving any operator @override_limit@ untouched
-- so a hand-tuned override survives every deploy.
upsertConcurrencyPolicyRowSQL :: SchemaName -> ConcurrencyPolicy -> Text
upsertConcurrencyPolicyRowSQL schemaName policy =
  policyUpsertSQL
    (arbiterConcurrencyPoliciesTable schemaName)
    (textLiteral (cpPrefix policy))
    [("default_limit", intLiteral (cpLimit policy))]

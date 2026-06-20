{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Concurrency SQL templates.
module Arbiter.Core.Sql.Concurrency
  ( updateConcurrencyPolicyOverrideSQL
  , liveConcurrencyKeysUnion
  , lockDeadConcurrencyKeysSQL
  , pruneLockedConcurrencyKeysSQL
  , tryLockDeadConcurrencyAdvisorySQL
  , lockConcurrencyCountsSQL
  , reconcileConcurrencyCountsSQL
  , listConcurrencyPoliciesSQL
  , getConcurrencyPolicySQL
  , concurrencyPoliciesSQL
  , listConcurrencyKeysSQL
  , concurrencyHasAnyKeySQL
  , concurrencyCountsStaleSQL
  ) where

import Data.Text (Text)
import NeatInterpolation (text)

import Arbiter.Core.Admission (effectivePolicyCol, policyViewScope, touchColumn)
import Arbiter.Core.Concurrency.Schema
  ( arbiterConcurrencyPoliciesTable
  , arbiterConcurrencyTable
  , concurrencyAdvisoryLockExpr
  )
import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.Sql.Jobs (unionAllOverQueueTables)

-- | Set (or clear, with NULL) a pool's operator override limit when the guard flag is true. Affects every key under the prefix.
updateConcurrencyPolicyOverrideSQL :: SchemaName -> Text
updateConcurrencyPolicyOverrideSQL schema =
  let policies = arbiterConcurrencyPoliciesTable schema
      touch = touchColumn "override_limit" "int"
   in [text| UPDATE ${policies} SET ${touch} WHERE prefix_id = ? |]

-- | Every concurrency_key held by a live job, unioned across the queue tables.
liveConcurrencyKeysUnion :: SchemaName -> [TableName] -> Text
liveConcurrencyKeysUnion schema tableNames =
  unionAllOverQueueTables schema tableNames $ \t ->
    "SELECT concurrency_key FROM " <> t <> " WHERE concurrency_key IS NOT NULL"

-- | Lock count rows with no live job, in key order to match the triggers. Returns the locked keys.
lockDeadConcurrencyKeysSQL :: SchemaName -> [TableName] -> Text
lockDeadConcurrencyKeysSQL schema tableNames =
  let concTbl = arbiterConcurrencyTable schema
      live = liveConcurrencyKeysUnion schema tableNames
   in [text|
        SELECT c.concurrency_key
        FROM ${concTbl} c
        WHERE NOT EXISTS (
          SELECT 1 FROM ( ${live} ) live WHERE live.concurrency_key = c.concurrency_key
        )
        ORDER BY c.concurrency_key
        FOR UPDATE
      |]

-- | Delete the passed locked keys that are still dead under a fresh snapshot, so a key whose
-- job committed after the lock pass (and is held by a concurrent seed) is not stranded.
pruneLockedConcurrencyKeysSQL :: SchemaName -> [TableName] -> Text
pruneLockedConcurrencyKeysSQL schema tableNames =
  let concTbl = arbiterConcurrencyTable schema
      live = liveConcurrencyKeysUnion schema tableNames
   in [text|
        DELETE FROM ${concTbl} c
        WHERE c.concurrency_key = ANY(?)
          AND NOT EXISTS (
            SELECT 1 FROM ( ${live} ) live WHERE live.concurrency_key = c.concurrency_key
          )
      |]

-- | Try an exclusive per-key advisory lock over the dead keys, pairing the insert trigger's shared lock.
-- Never waits, so it cannot deadlock with or stall behind an open enqueue transaction. Returns the keys acquired.
tryLockDeadConcurrencyAdvisorySQL :: Text
tryLockDeadConcurrencyAdvisorySQL =
  let lockExpr = concurrencyAdvisoryLockExpr "k"
   in [text|
        SELECT k AS concurrency_key
        FROM unnest(?::text[]) AS t(k)
        WHERE pg_try_advisory_xact_lock(${lockExpr})
      |]

-- | Lock every count row in key order, so a reconcile recount after it sees committed claims.
lockConcurrencyCountsSQL :: SchemaName -> Text
lockConcurrencyCountsSQL schema =
  let concTbl = arbiterConcurrencyTable schema
   in [text|
        SELECT concurrency_key FROM ${concTbl} ORDER BY concurrency_key FOR UPDATE
      |]

-- | Recount in_flight for the passed locked keys and seed a row for a live key that
-- has none. Writes only rows the caller locked (or fresh inserts, never updated on
-- conflict), so a key seeded after the lock pass keeps its trigger-maintained count
-- and a concurrent claim is never overwritten. Run after 'lockConcurrencyCountsSQL'
-- in one transaction. Returns the number repaired. Parameter: locked keys.
reconcileConcurrencyCountsSQL :: SchemaName -> [TableName] -> Text
reconcileConcurrencyCountsSQL schema tableNames =
  let concTbl = arbiterConcurrencyTable schema
      union = unionAllOverQueueTables schema tableNames $ \t ->
        "SELECT concurrency_key, claimed_by, concurrency_prefix FROM " <> t <> " WHERE concurrency_key IS NOT NULL"
   in [text|
        WITH live AS (
          SELECT concurrency_key,
                 COUNT(*) FILTER (WHERE claimed_by IS NOT NULL) AS inflight,
                 MAX(concurrency_prefix) AS prefix
          FROM ( ${union} ) j
          GROUP BY concurrency_key
        ),
        held AS (
          SELECT k AS concurrency_key FROM unnest(?::text[]) AS t(k)
        ),
        fixed AS (
          UPDATE ${concTbl} c
          SET in_flight = COALESCE(l.inflight, 0)
          FROM held h
          LEFT JOIN live l ON l.concurrency_key = h.concurrency_key
          WHERE c.concurrency_key = h.concurrency_key
            AND c.in_flight IS DISTINCT FROM COALESCE(l.inflight, 0)
          RETURNING c.concurrency_key
        ),
        seeded AS (
          INSERT INTO ${concTbl} (concurrency_key, concurrency_prefix, in_flight)
          SELECT l.concurrency_key, l.prefix, l.inflight
          FROM live l
          WHERE l.concurrency_key NOT IN (SELECT concurrency_key FROM held)
            AND NOT EXISTS (SELECT 1 FROM ${concTbl} c WHERE c.concurrency_key = l.concurrency_key)
          ORDER BY l.concurrency_key
          ON CONFLICT (concurrency_key) DO NOTHING
          RETURNING concurrency_key
        )
        SELECT ((SELECT COUNT(*) FROM fixed) + (SELECT COUNT(*) FROM seeded))::int8 AS reconciled
      |]

-- | List every concurrency pool with its default/override limit and live key and
-- in-flight aggregates. No parameters.
listConcurrencyPoliciesSQL :: SchemaName -> Text
listConcurrencyPoliciesSQL schema = concurrencyPoliciesSQL schema False

-- | Single-prefix variant of 'listConcurrencyPoliciesSQL'. Parameter: prefix.
getConcurrencyPolicySQL :: SchemaName -> Text
getConcurrencyPolicySQL schema = concurrencyPoliciesSQL schema True

concurrencyPoliciesSQL :: SchemaName -> Bool -> Text
concurrencyPoliciesSQL schema single =
  let policies = arbiterConcurrencyPoliciesTable schema
      counts = arbiterConcurrencyTable schema
      (kCte, aggWhere, scope) = policyViewScope single "c.concurrency_prefix"
   in [text|
        ${kCte}
        SELECT p.prefix_id, p.default_limit, p.override_limit,
               COALESCE(agg.key_count, 0) AS key_count,
               COALESCE(agg.total_in_flight, 0) AS total_in_flight,
               agg.max_in_flight
        FROM ${policies} p
        LEFT JOIN (
          SELECT c.concurrency_prefix, COUNT(*) AS key_count,
                 SUM(c.in_flight) AS total_in_flight, MAX(c.in_flight) AS max_in_flight
          FROM ${counts} c
          ${aggWhere}
          GROUP BY c.concurrency_prefix
        ) agg ON agg.concurrency_prefix = p.prefix_id
        ${scope}
      |]

-- | List a prefix's keys with effective cap and in-flight fill fraction, paginated.
-- Parameters: concurrency_prefix, limit, offset.
listConcurrencyKeysSQL :: SchemaName -> Text
listConcurrencyKeysSQL schema =
  let policies = arbiterConcurrencyPoliciesTable schema
      counts = arbiterConcurrencyTable schema
      effLimit = effectivePolicyCol "p" "limit"
   in [text|
        SELECT c.concurrency_key, c.concurrency_prefix, c.in_flight,
               ${effLimit} AS effective_limit,
               c.in_flight::float8 / NULLIF(${effLimit}, 0) AS fill_fraction
        FROM ${counts} c
        JOIN ${policies} p ON p.prefix_id = c.concurrency_prefix
        WHERE c.concurrency_prefix = ?
        ORDER BY fill_fraction DESC NULLS LAST, c.concurrency_key
        LIMIT ? OFFSET ?
      |]

-- | Whether any concurrency key exists, to skip the full reconcile/prune scan otherwise.
concurrencyHasAnyKeySQL :: SchemaName -> Text
concurrencyHasAnyKeySQL schema =
  "SELECT EXISTS (SELECT 1 FROM " <> arbiterConcurrencyTable schema <> ") AS present"

-- | Whether a crash truncated the count table (a live keyed job has no count row).
-- Enqueues re-seed only their own keys, so any missing row means lost counts.
concurrencyCountsStaleSQL :: SchemaName -> [TableName] -> Text
concurrencyCountsStaleSQL schema tableNames =
  let concTbl = arbiterConcurrencyTable schema
      keyed = liveConcurrencyKeysUnion schema tableNames
   in [text|
        SELECT EXISTS (
          SELECT 1 FROM ( ${keyed} ) k
          WHERE NOT EXISTS (SELECT 1 FROM ${concTbl} c WHERE c.concurrency_key = k.concurrency_key)
        ) AS stale
      |]

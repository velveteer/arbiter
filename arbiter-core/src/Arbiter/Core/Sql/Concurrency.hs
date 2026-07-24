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
  , listConcurrencyKeysSQL
  , concurrencyHasAnyKeySQL
  , concurrencyCountsStaleSQL
  ) where

import Control.Monad (join)
import Data.Int (Int32, Int64)
import Data.Maybe (isJust)
import Data.Text (Text)

import Arbiter.Core.Admission (effectivePolicyCol, policyViewScope)
import Arbiter.Core.Codec (concurrencyKeyViewCodec, concurrencyPolicyViewCodec)
import Arbiter.Core.Concurrency.Schema
  ( arbiterConcurrencyPoliciesTable
  , arbiterConcurrencyTable
  , concurrencyAdvisoryLockExpr
  )
import Arbiter.Core.Concurrency.Stats (ConcurrencyKeyView, ConcurrencyPolicyView)
import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.Sql.Jobs (unionAllOverQueueTables)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)

-- | Set a pool's operator override limit. @Nothing@ leaves it untouched;
-- @Just v@ writes @v@ (a null clears the override back to the default).
updateConcurrencyPolicyOverrideSQL :: SchemaName -> Maybe (Maybe Int32) -> Text -> Query ()
updateConcurrencyPolicyOverrideSQL schema mLimit prefix =
  let policies = arbiterConcurrencyPoliciesTable schema
      touch = isJust mLimit
      limit = join mLimit
   in [sql|
        UPDATE ${policies}
        SET override_limit = CASE WHEN #{touch :: CBool}::boolean THEN #{limit :: Maybe CInt4}::int ELSE override_limit END
        WHERE prefix_id = #{prefix :: CText}
      |]

-- | Every concurrency_key held by a live job, unioned across the queue tables.
liveConcurrencyKeysUnion :: SchemaName -> [TableName] -> Text
liveConcurrencyKeysUnion schema tableNames =
  unionAllOverQueueTables schema tableNames $ \_ t ->
    "SELECT concurrency_key FROM " <> t <> " WHERE concurrency_key IS NOT NULL"

-- | Lock count rows with no live job, in key order to match the triggers. Returns the locked keys.
lockDeadConcurrencyKeysSQL :: SchemaName -> [TableName] -> Query Text
lockDeadConcurrencyKeysSQL schema tableNames =
  let concTbl = arbiterConcurrencyTable schema
      live = liveConcurrencyKeysUnion schema tableNames
   in [sql|
        SELECT @{concurrency_key :: CText}
        FROM ${concTbl} c
        WHERE NOT EXISTS (
          SELECT 1 FROM ( ${live} ) live WHERE live.concurrency_key = c.concurrency_key
        )
        ORDER BY c.concurrency_key
        FOR UPDATE
      |]

-- | Delete the passed locked keys that are still dead under a fresh snapshot, so a key whose
-- job committed after the lock pass (and is held by a concurrent seed) is not stranded.
pruneLockedConcurrencyKeysSQL :: SchemaName -> [TableName] -> [Text] -> Query ()
pruneLockedConcurrencyKeysSQL schema tableNames lockedKeys =
  let concTbl = arbiterConcurrencyTable schema
      live = liveConcurrencyKeysUnion schema tableNames
   in [sql|
        DELETE FROM ${concTbl} c
        WHERE c.concurrency_key = ANY(#{lockedKeys :: [CText]})
          AND NOT EXISTS (
            SELECT 1 FROM ( ${live} ) live WHERE live.concurrency_key = c.concurrency_key
          )
      |]

-- | Try an exclusive per-key advisory lock over the dead keys, pairing the insert trigger's shared lock.
-- Never waits, so it cannot deadlock with or stall behind an open enqueue transaction. Returns the keys acquired.
tryLockDeadConcurrencyAdvisorySQL :: [Text] -> Query Text
tryLockDeadConcurrencyAdvisorySQL deadKeys =
  let lockExpr = concurrencyAdvisoryLockExpr "k"
   in [sql|
        SELECT k AS @{concurrency_key :: CText}
        FROM unnest(#{deadKeys :: [CText]}::text[]) AS t(k)
        WHERE pg_try_advisory_xact_lock(${lockExpr})
      |]

-- | Lock every count row in key order, so a reconcile recount after it sees committed claims.
lockConcurrencyCountsSQL :: SchemaName -> Query Text
lockConcurrencyCountsSQL schema =
  let concTbl = arbiterConcurrencyTable schema
   in [sql|
        SELECT @{concurrency_key :: CText} FROM ${concTbl} ORDER BY concurrency_key FOR UPDATE
      |]

-- | Recount in_flight for the passed locked keys and seed a row for a live key that
-- has none. Writes only rows the caller locked (or fresh inserts, never updated on
-- conflict), so a key seeded after the lock pass keeps its trigger-maintained count
-- and a concurrent claim is never overwritten. Run after 'lockConcurrencyCountsSQL'
-- in one transaction. Returns the number repaired. Parameter: locked keys.
reconcileConcurrencyCountsSQL :: SchemaName -> [TableName] -> [Text] -> Query Int64
reconcileConcurrencyCountsSQL schema tableNames heldKeys =
  let concTbl = arbiterConcurrencyTable schema
      union = unionAllOverQueueTables schema tableNames $ \_ t ->
        "SELECT concurrency_key, claimed_by, concurrency_prefix FROM " <> t <> " WHERE concurrency_key IS NOT NULL"
   in [sql|
        WITH live AS (
          SELECT concurrency_key,
                 COUNT(*) FILTER (WHERE claimed_by IS NOT NULL) AS inflight,
                 MAX(concurrency_prefix) AS prefix
          FROM ( ${union} ) j
          GROUP BY concurrency_key
        ),
        held AS (
          SELECT k AS concurrency_key FROM unnest(#{heldKeys :: [CText]}::text[]) AS t(k)
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
        SELECT ((SELECT COUNT(*) FROM fixed) + (SELECT COUNT(*) FROM seeded))::int8 AS @{reconciled :: CInt8}
      |]

-- | List every concurrency pool with its default/override limit and live key and
-- in-flight aggregates.
listConcurrencyPoliciesSQL :: SchemaName -> Query ConcurrencyPolicyView
listConcurrencyPoliciesSQL schema = concurrencyPoliciesSQL schema Nothing

-- | Single-prefix variant of 'listConcurrencyPoliciesSQL'.
getConcurrencyPolicySQL :: SchemaName -> Text -> Query ConcurrencyPolicyView
getConcurrencyPolicySQL schema prefix = concurrencyPoliciesSQL schema (Just prefix)

concurrencyPoliciesSQL :: SchemaName -> Maybe Text -> Query ConcurrencyPolicyView
concurrencyPoliciesSQL schema mPrefix =
  let policies = arbiterConcurrencyPoliciesTable schema
      counts = arbiterConcurrencyTable schema
      kCte = foldMap (\p -> [sql|WITH k AS (SELECT #{p :: CText}::text AS prefix)|]) mPrefix
      (aggWhere, scope) = policyViewScope (isJust mPrefix) "c.concurrency_prefix"
   in rows
        concurrencyPolicyViewCodec
        [sql|
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
listConcurrencyKeysSQL :: SchemaName -> Text -> Int64 -> Int64 -> Query ConcurrencyKeyView
listConcurrencyKeysSQL schema prefix limit offset =
  let policies = arbiterConcurrencyPoliciesTable schema
      counts = arbiterConcurrencyTable schema
      effLimit = effectivePolicyCol "p" "limit"
   in rows
        concurrencyKeyViewCodec
        [sql|
          SELECT concurrency_key, concurrency_prefix, in_flight, effective_limit,
                 in_flight::float8 / NULLIF(effective_limit, 0) AS fill_fraction
          FROM (
            SELECT c.concurrency_key, c.concurrency_prefix, c.in_flight,
                   ${effLimit} AS effective_limit
            FROM ${counts} c
            JOIN ${policies} p ON p.prefix_id = c.concurrency_prefix
            WHERE c.concurrency_prefix = #{prefix :: CText}
          ) r
          ORDER BY fill_fraction DESC NULLS LAST, concurrency_key
          LIMIT #{limit :: CInt8} OFFSET #{offset :: CInt8}
        |]

-- | Whether any concurrency key exists, to skip the full reconcile/prune scan otherwise.
concurrencyHasAnyKeySQL :: SchemaName -> Query Bool
concurrencyHasAnyKeySQL schema =
  let concTbl = arbiterConcurrencyTable schema
   in [sql|SELECT EXISTS (SELECT 1 FROM ${concTbl}) AS @{present :: CBool}|]

-- | Whether a crash truncated the count table (a live keyed job has no count row).
-- Enqueues re-seed only their own keys, so any missing row means lost counts.
concurrencyCountsStaleSQL :: SchemaName -> [TableName] -> Query Bool
concurrencyCountsStaleSQL schema tableNames =
  let concTbl = arbiterConcurrencyTable schema
      keyed = liveConcurrencyKeysUnion schema tableNames
   in [sql|
        SELECT EXISTS (
          SELECT 1 FROM ( ${keyed} ) k
          WHERE NOT EXISTS (SELECT 1 FROM ${concTbl} c WHERE c.concurrency_key = k.concurrency_key)
        ) AS @{stale :: CBool}
      |]

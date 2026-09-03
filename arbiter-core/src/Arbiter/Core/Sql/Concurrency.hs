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
import NeatInterpolation (text)

import Arbiter.Core.Admission (effectivePolicyCol)
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

-- | Set a pool's operator override limit. @Nothing@ leaves it untouched.
-- @Just v@ writes @v@ (a null clears the override back to the default).
updateConcurrencyPolicyOverrideSQL :: SchemaName -> Maybe (Maybe Int32) -> Text -> Query ()
updateConcurrencyPolicyOverrideSQL schema mLimit prefix =
  let policies = arbiterConcurrencyPoliciesTable schema
      touch = isJust mLimit
      limit = join mLimit
   in [sql|
        UPDATE ${policies}
        SET override_limit = CASE WHEN #{touch :: CBool}::boolean
                               THEN #{limit :: Maybe CInt4}::int
                               ELSE override_limit END
        WHERE prefix_id = #{prefix :: CText}
      |]

-- | Every concurrency_key held by a live job, unioned across the queue tables.
liveConcurrencyKeysUnion :: SchemaName -> [TableName] -> Text
liveConcurrencyKeysUnion schema tableNames =
  unionAllOverQueueTables schema tableNames $ \_ table ->
    [text|SELECT concurrency_key FROM ${table} WHERE concurrency_key IS NOT NULL|]

-- | Lock count rows with no live job, in the triggers' key order. Returns the locked keys.
lockDeadConcurrencyKeysSQL :: SchemaName -> [TableName] -> Query Text
lockDeadConcurrencyKeysSQL schema tableNames =
  let concTbl = arbiterConcurrencyTable schema
      live = liveConcurrencyKeysUnion schema tableNames
   in [sql|
        SELECT @{concurrency_key :: CText}
        FROM ${concTbl} counts
        WHERE NOT EXISTS (
          SELECT 1 FROM ( ${live} ) live WHERE live.concurrency_key = counts.concurrency_key
        )
        ORDER BY counts.concurrency_key
        FOR UPDATE
      |]

-- | Delete the passed locked keys that are still dead under a fresh snapshot.
pruneLockedConcurrencyKeysSQL :: SchemaName -> [TableName] -> [Text] -> Query ()
pruneLockedConcurrencyKeysSQL schema tableNames lockedKeys =
  let concTbl = arbiterConcurrencyTable schema
      live = liveConcurrencyKeysUnion schema tableNames
   in [sql|
        DELETE FROM ${concTbl} counts
        WHERE counts.concurrency_key = ANY(#{lockedKeys :: [CText]})
          AND NOT EXISTS (
            SELECT 1 FROM ( ${live} ) live WHERE live.concurrency_key = counts.concurrency_key
          )
      |]

-- | Try an exclusive per-key advisory lock over the dead keys, pairing the insert trigger's shared lock.
-- Never waits. Returns the keys acquired.
tryLockDeadConcurrencyAdvisorySQL :: [Text] -> Query Text
tryLockDeadConcurrencyAdvisorySQL deadKeys =
  let lockExpr = concurrencyAdvisoryLockExpr "dead_key"
   in [sql|
        SELECT dead_key AS @{concurrency_key :: CText}
        FROM unnest(#{deadKeys :: [CText]}::text[]) AS dead(dead_key)
        WHERE pg_try_advisory_xact_lock(${lockExpr})
      |]

-- | Lock every count row in key order.
lockConcurrencyCountsSQL :: SchemaName -> Query Text
lockConcurrencyCountsSQL schema =
  let concTbl = arbiterConcurrencyTable schema
   in [sql|
        SELECT @{concurrency_key :: CText} FROM ${concTbl} ORDER BY concurrency_key FOR UPDATE
      |]

-- | Recount in_flight for the passed locked keys and seed a row for a live key that
-- has none. Writes the rows the caller locked and fresh inserts. Run after
-- 'lockConcurrencyCountsSQL' in one transaction. Returns the number repaired.
reconcileConcurrencyCountsSQL :: SchemaName -> [TableName] -> [Text] -> Query Int64
reconcileConcurrencyCountsSQL schema tableNames heldKeys =
  let concTbl = arbiterConcurrencyTable schema
      union = unionAllOverQueueTables schema tableNames $ \_ table ->
        [text|SELECT concurrency_key, claimed_by, concurrency_prefix FROM ${table} WHERE concurrency_key IS NOT NULL|]
   in [sql|
        WITH live AS (
          SELECT concurrency_key,
                 COUNT(*) FILTER (WHERE claimed_by IS NOT NULL) AS inflight,
                 MAX(concurrency_prefix) AS prefix
          FROM ( ${union} ) job
          GROUP BY concurrency_key
        ),
        held AS (
          SELECT held_key AS concurrency_key FROM unnest(#{heldKeys :: [CText]}::text[]) AS held_keys(held_key)
        ),
        fixed AS (
          UPDATE ${concTbl} counts
          SET in_flight = COALESCE(live_row.inflight, 0)
          FROM held held_row
          LEFT JOIN live live_row ON live_row.concurrency_key = held_row.concurrency_key
          WHERE counts.concurrency_key = held_row.concurrency_key
            AND counts.in_flight IS DISTINCT FROM COALESCE(live_row.inflight, 0)
          RETURNING counts.concurrency_key
        ),
        seeded AS (
          INSERT INTO ${concTbl} (concurrency_key, concurrency_prefix, in_flight)
          SELECT live_row.concurrency_key, live_row.prefix, live_row.inflight
          FROM live live_row
          WHERE live_row.concurrency_key NOT IN (SELECT concurrency_key FROM held)
            AND NOT EXISTS (SELECT 1 FROM ${concTbl} counts WHERE counts.concurrency_key = live_row.concurrency_key)
          ORDER BY live_row.concurrency_key
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

-- | The policy views, every pool or the one a prefix names.
concurrencyPoliciesSQL :: SchemaName -> Maybe Text -> Query ConcurrencyPolicyView
concurrencyPoliciesSQL schema mPrefix =
  let policies = arbiterConcurrencyPoliciesTable schema
      counts = arbiterConcurrencyTable schema
   in rows
        concurrencyPolicyViewCodec
        [sql|
        WITH target AS (SELECT #{mPrefix :: Maybe CText}::text AS prefix)
        SELECT policy.prefix_id, policy.default_limit, policy.override_limit,
               COALESCE(agg.key_count, 0) AS key_count,
               COALESCE(agg.total_in_flight, 0) AS total_in_flight,
               agg.max_in_flight
        FROM ${policies} policy
        LEFT JOIN (
          SELECT counts.concurrency_prefix, COUNT(*) AS key_count,
                 SUM(counts.in_flight) AS total_in_flight, MAX(counts.in_flight) AS max_in_flight
          FROM ${counts} counts
          WHERE (SELECT prefix FROM target) IS NULL OR counts.concurrency_prefix = (SELECT prefix FROM target)
          GROUP BY counts.concurrency_prefix
        ) agg ON agg.concurrency_prefix = policy.prefix_id
        WHERE (SELECT prefix FROM target) IS NULL OR policy.prefix_id = (SELECT prefix FROM target)
        ORDER BY policy.prefix_id
      |]

-- | List a prefix's keys with effective cap and in-flight fill fraction, paginated.
listConcurrencyKeysSQL :: SchemaName -> Text -> Int64 -> Int64 -> Query ConcurrencyKeyView
listConcurrencyKeysSQL schema prefix limit offset =
  let policies = arbiterConcurrencyPoliciesTable schema
      counts = arbiterConcurrencyTable schema
      effLimit = effectivePolicyCol "policy" "limit"
   in rows
        concurrencyKeyViewCodec
        [sql|
          SELECT concurrency_key, concurrency_prefix, in_flight, effective_limit,
                 in_flight::float8 / NULLIF(effective_limit, 0) AS fill_fraction
          FROM (
            SELECT counts.concurrency_key, counts.concurrency_prefix, counts.in_flight,
                   ${effLimit} AS effective_limit
            FROM ${counts} counts
            JOIN ${policies} policy ON policy.prefix_id = counts.concurrency_prefix
            WHERE counts.concurrency_prefix = #{prefix :: CText}
          ) keyed
          ORDER BY fill_fraction DESC NULLS LAST, concurrency_key
          LIMIT #{limit :: CInt8} OFFSET #{offset :: CInt8}
        |]

-- | Whether any concurrency key exists.
concurrencyHasAnyKeySQL :: SchemaName -> Query Bool
concurrencyHasAnyKeySQL schema =
  let concTbl = arbiterConcurrencyTable schema
   in [sql|SELECT EXISTS (SELECT 1 FROM ${concTbl}) AS @{present :: CBool}|]

-- | Whether a crash truncated the count table (a live keyed job has no count row).
concurrencyCountsStaleSQL :: SchemaName -> [TableName] -> Query Bool
concurrencyCountsStaleSQL schema tableNames =
  let concTbl = arbiterConcurrencyTable schema
      keyed = liveConcurrencyKeysUnion schema tableNames
   in [sql|
        SELECT EXISTS (
          SELECT 1 FROM ( ${keyed} ) live_key
          WHERE NOT EXISTS (SELECT 1 FROM ${concTbl} counts WHERE counts.concurrency_key = live_key.concurrency_key)
        ) AS @{stale :: CBool}
      |]

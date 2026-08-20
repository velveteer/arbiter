{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Postgres health SQL: reads over the stats and catalog views. The caller
-- attaches the row decoder.
module Arbiter.Core.Sql.Health
  ( pgDbHealthSQL
  , pgTableHealthSQL
  ) where

import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query)

-- | One row of connection state for the current database. Skips the reading backend.
-- Every connection reading comes from @pg_stat_activity@, so the states partition the
-- backend count at one visibility scope.
pgDbHealthSQL :: Query ()
pgDbHealthSQL =
  [sql|
    SELECT
      a.numbackends::int8,
      a.active::int8, a.idle::int8, a.idle_in_txn::int8, a.idle_in_txn_aborted::int8, a.blocked::int8, a.other::int8,
      a.oldest_txn_age::float8, a.oldest_query_age::float8
    FROM (
      SELECT
        count(*) FILTER (WHERE state = 'active' AND wait_event_type IS DISTINCT FROM 'Lock') AS active,
        count(*) FILTER (WHERE state = 'idle') AS idle,
        count(*) FILTER (WHERE state = 'idle in transaction') AS idle_in_txn,
        count(*) FILTER (WHERE state = 'idle in transaction (aborted)') AS idle_in_txn_aborted,
        count(*) FILTER (WHERE state = 'active' AND wait_event_type = 'Lock') AS blocked,
        count(*) FILTER (WHERE state IS NULL OR state NOT IN ('active', 'idle', 'idle in transaction', 'idle in transaction (aborted)')) AS other,
        count(*) AS numbackends,
        COALESCE(EXTRACT(EPOCH FROM max(clock_timestamp() - xact_start)), 0) AS oldest_txn_age,
        COALESCE(EXTRACT(EPOCH FROM max(clock_timestamp() - query_start) FILTER (WHERE state = 'active')), 0) AS oldest_query_age
      FROM pg_stat_activity WHERE datname = current_database() AND pid <> pg_backend_pid()
    ) a
  |]

-- | Per-table churn, block traffic, and freeze age for arbiter's own tables, one row per
-- table. Block counts total the heap, index, and toast reads.
pgTableHealthSQL :: SchemaName -> [TableName] -> Query ()
pgTableHealthSQL schemaName tableNames =
  [sql|
    SELECT
      t.relname::text, t.n_live_tup::int8, t.n_dead_tup::int8,
      EXTRACT(EPOCH FROM clock_timestamp() - GREATEST(t.last_autovacuum, t.last_vacuum))::float8 AS autovacuum_age,
      pg_total_relation_size(t.relid)::int8 AS total_bytes,
      t.seq_scan::float8, COALESCE(t.idx_scan, 0)::float8 AS idx_scan,
      (COALESCE(io.heap_blks_hit, 0) + COALESCE(io.idx_blks_hit, 0)
        + COALESCE(io.toast_blks_hit, 0) + COALESCE(io.tidx_blks_hit, 0))::float8 AS blks_hit,
      (COALESCE(io.heap_blks_read, 0) + COALESCE(io.idx_blks_read, 0)
        + COALESCE(io.toast_blks_read, 0) + COALESCE(io.tidx_blks_read, 0))::float8 AS blks_read,
      age(NULLIF(c.relfrozenxid, '0'::xid))::int8 AS xid_age
    FROM pg_stat_user_tables t
    JOIN pg_class c ON c.oid = t.relid
    LEFT JOIN pg_statio_user_tables io ON io.relid = t.relid
    WHERE t.schemaname = #{schemaName :: CText}
      AND t.relname = ANY(#{tableNames :: [CText]})
  |]

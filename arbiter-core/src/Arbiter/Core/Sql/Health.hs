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

-- | One row of database-wide health, counters cumulative. Skips the reading backend.
-- Every connection reading comes from @pg_stat_activity@, so the states partition the
-- backend count at one visibility scope.
pgDbHealthSQL :: Query ()
pgDbHealthSQL =
  [sql|
    SELECT
      d.blks_hit::float8, d.blks_read::float8,
      d.xact_commit::float8, d.xact_rollback::float8, d.deadlocks::float8,
      a.numbackends::int8,
      a.active::int8, a.idle::int8, a.idle_in_txn::int8, a.idle_in_txn_aborted::int8, a.blocked::int8, a.other::int8,
      a.oldest_txn_age::float8, a.oldest_query_age::float8,
      age(pd.datfrozenxid)::int8 AS xid_age
    FROM pg_stat_database d
    JOIN pg_database pd ON pd.datname = d.datname
    CROSS JOIN LATERAL (
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
    WHERE d.datname = current_database()
  |]

-- | Per-table churn for arbiter's own tables, one row per table.
pgTableHealthSQL :: SchemaName -> [TableName] -> Query ()
pgTableHealthSQL schemaName tableNames =
  [sql|
    SELECT
      relname::text, n_live_tup::int8, n_dead_tup::int8,
      COALESCE(EXTRACT(EPOCH FROM clock_timestamp() - GREATEST(last_autovacuum, last_vacuum)), -1)::float8 AS autovacuum_age,
      pg_total_relation_size(relid)::int8 AS total_bytes,
      seq_scan::float8, COALESCE(idx_scan, 0)::float8 AS idx_scan
    FROM pg_stat_user_tables
    WHERE schemaname = #{schemaName :: CText}
      AND relname = ANY(#{tableNames :: [CText]})
  |]

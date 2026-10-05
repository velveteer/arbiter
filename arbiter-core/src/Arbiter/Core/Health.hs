{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Postgres health snapshots from the stats and catalog views. The queries name Arbiter's
-- own tables and do not scan them.
module Arbiter.Core.Health
  ( PgDbHealth (..)
  , PgTableHealth (..)
  , getPgHealth
  , getPgDbHealth
  ) where

import Data.Aeson (FromJSON, ToJSON)
import Data.Int (Int64)
import Data.Maybe (listToMaybe)
import Data.Text (Text)
import GHC.Generics (Generic)

import Arbiter.Core.Codec (Col (..), RowCodec, col, ncol)
import Arbiter.Core.Job.Schema (TableName)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.SchemaTables (allSchemaTables)
import Arbiter.Core.Sql.Health qualified as Sql
import Arbiter.Core.Sql.Query (rows)

-- | Connection and age counters for the current database, shared with its other clients.
data PgDbHealth = PgDbHealth
  { numBackends :: Int64
  -- ^ Connections to the database, the reading one excluded.
  , connActive :: Int64
  -- ^ Connections running a query and not waiting on a lock.
  , connIdle :: Int64
  -- ^ Idle connections.
  , connIdleInTxn :: Int64
  -- ^ Connections idle inside an open transaction.
  , connIdleInTxnAborted :: Int64
  -- ^ Connections idle inside a failed transaction.
  , connBlocked :: Int64
  -- ^ Connections running a query that waits on a lock.
  , connOther :: Int64
  -- ^ Connections in any other state.
  , oldestTxnAge :: Double
  -- ^ Age of the oldest open transaction, in seconds. 0 when none is open.
  , oldestQueryAge :: Double
  -- ^ Age of the oldest running query, in seconds. 0 when none runs.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Per-table tuple counts, size, scan counters, block traffic, and freeze age.
data PgTableHealth = PgTableHealth
  { table :: Text
  -- ^ Table name.
  , liveTup :: Int64
  -- ^ Estimated live rows.
  , deadTup :: Int64
  -- ^ Estimated dead rows.
  , autovacuumAge :: Maybe Double
  -- ^ Seconds since the last vacuum, manual or auto. 'Nothing' when the table has never been vacuumed.
  , totalBytes :: Int64
  -- ^ Size with indexes and TOAST, in bytes.
  , seqScan :: Double
  -- ^ Sequential scans started.
  , idxScan :: Double
  -- ^ Index scans started.
  , blksHit :: Double
  -- ^ Blocks read from the buffer cache, for the table, its indexes, and its TOAST.
  , blksRead :: Double
  -- ^ Blocks read from disk, for the table, its indexes, and its TOAST.
  , xidAge :: Maybe Int64
  -- ^ Age of the table's frozen transaction id, in transactions. 'Nothing' when the table has no frozen transaction id.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Column order matches the SELECT lists. 'RowCodec' is positional.
pgDbHealthCodec :: RowCodec PgDbHealth
pgDbHealthCodec =
  PgDbHealth
    <$> col "numbackends" CInt8
    <*> col "active" CInt8
    <*> col "idle" CInt8
    <*> col "idle_in_txn" CInt8
    <*> col "idle_in_txn_aborted" CInt8
    <*> col "blocked" CInt8
    <*> col "other" CInt8
    <*> col "oldest_txn_age" CFloat8
    <*> col "oldest_query_age" CFloat8

pgTableHealthCodec :: RowCodec PgTableHealth
pgTableHealthCodec =
  PgTableHealth
    <$> col "relname" CText
    <*> col "n_live_tup" CInt8
    <*> col "n_dead_tup" CInt8
    <*> ncol "autovacuum_age" CFloat8
    <*> col "total_bytes" CInt8
    <*> col "seq_scan" CFloat8
    <*> col "idx_scan" CFloat8
    <*> col "blks_hit" CFloat8
    <*> col "blks_read" CFloat8
    <*> ncol "xid_age" CInt8

-- | Database-wide health and per-table churn for the given queues' tables and the schema's
-- shared arbiter tables. Backends owned by another role report their state as unknown,
-- absent @pg_read_all_stats@.
getPgHealth :: (MonadArbiter m) => [TableName] -> m (Maybe PgDbHealth, [PgTableHealth])
getPgHealth queueTables = do
  schemaName <- getSchema
  dbHealth <- getPgDbHealth
  tableRows <- executeQuery (rows pgTableHealthCodec (Sql.pgTableHealthSQL schemaName scanned))
  pure (dbHealth, tableRows)
  where
    scanned = allSchemaTables queueTables

-- | The database-wide half on its own.
getPgDbHealth :: (MonadArbiter m) => m (Maybe PgDbHealth)
getPgDbHealth = listToMaybe <$> executeQuery (rows pgDbHealthCodec Sql.pgDbHealthSQL)

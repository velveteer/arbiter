{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Postgres health snapshots from the stats and catalog views. Arbiter's own tables are
-- named but never scanned.
module Arbiter.Core.Health
  ( PgDbHealth (..)
  , PgTableHealth (..)
  , getPgHealth
  ) where

import Data.Aeson (FromJSON, ToJSON)
import Data.Int (Int64)
import Data.Maybe (listToMaybe)
import Data.Text (Text)
import GHC.Generics (Generic)

import Arbiter.Core.Codec (Col (..), RowCodec, col, ncol)
import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.SchemaTables (allSchemaTables)
import Arbiter.Core.Sql.Health qualified as Sql
import Arbiter.Core.Sql.Query (rows)

data PgDbHealth = PgDbHealth
  { blksHit :: Double
  , blksRead :: Double
  , xactCommit :: Double
  , xactRollback :: Double
  , deadlocks :: Double
  , numBackends :: Int64
  , connActive :: Int64
  , connIdle :: Int64
  , connIdleInTxn :: Int64
  , connIdleInTxnAborted :: Int64
  , connBlocked :: Int64
  , connOther :: Int64
  , oldestTxnAge :: Double
  , oldestQueryAge :: Double
  , xidAge :: Int64
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

data PgTableHealth = PgTableHealth
  { table :: Text
  , liveTup :: Int64
  , deadTup :: Int64
  , autovacuumAge :: Maybe Double
  -- ^ Nothing when the table has never been vacuumed.
  , totalBytes :: Int64
  , seqScan :: Double
  , idxScan :: Double
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Column order must match the SELECT lists ('RowCodec' is positional).
pgDbHealthCodec :: RowCodec PgDbHealth
pgDbHealthCodec =
  PgDbHealth
    <$> col "blks_hit" CFloat8
    <*> col "blks_read" CFloat8
    <*> col "xact_commit" CFloat8
    <*> col "xact_rollback" CFloat8
    <*> col "deadlocks" CFloat8
    <*> col "numbackends" CInt8
    <*> col "active" CInt8
    <*> col "idle" CInt8
    <*> col "idle_in_txn" CInt8
    <*> col "idle_in_txn_aborted" CInt8
    <*> col "blocked" CInt8
    <*> col "other" CInt8
    <*> col "oldest_txn_age" CFloat8
    <*> col "oldest_query_age" CFloat8
    <*> col "xid_age" CInt8

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

-- | Database-wide health and per-table churn for the given queues' tables and the schema's
-- shared arbiter tables. Backends owned by another role report their state as unknown,
-- absent @pg_read_all_stats@.
getPgHealth :: (MonadArbiter m) => SchemaName -> [TableName] -> m (Maybe PgDbHealth, [PgTableHealth])
getPgHealth schemaName queueTables = do
  dbRows <- executeQuery (rows pgDbHealthCodec Sql.pgDbHealthSQL)
  tableRows <- executeQuery (rows pgTableHealthCodec (Sql.pgTableHealthSQL schemaName scanned))
  pure (listToMaybe dbRows, tableRows)
  where
    scanned = allSchemaTables queueTables

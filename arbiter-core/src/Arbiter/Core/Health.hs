{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Postgres health snapshots: @pg_stat_*@ reads that never touch arbiter's own tables.
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

import Arbiter.Core.Codec (Col (..), RowCodec, col)
import Arbiter.Core.Job.Schema (SchemaName, TableName, queueTableNames)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
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
  , autovacuumAge :: Double
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
    <*> col "oldest_txn_age" CFloat8
    <*> col "oldest_query_age" CFloat8
    <*> col "xid_age" CInt8

pgTableHealthCodec :: RowCodec PgTableHealth
pgTableHealthCodec =
  PgTableHealth
    <$> col "relname" CText
    <*> col "n_live_tup" CInt8
    <*> col "n_dead_tup" CInt8
    <*> col "autovacuum_age" CFloat8
    <*> col "total_bytes" CInt8
    <*> col "seq_scan" CFloat8
    <*> col "idx_scan" CFloat8

-- | Database-wide health and per-table churn for the given queues' tables and the schema's
-- shared arbiter tables. Connection and age readings cover only arbiter's own role, absent
-- @pg_read_all_stats@.
getPgHealth :: (MonadArbiter m) => SchemaName -> [TableName] -> m (Maybe PgDbHealth, [PgTableHealth])
getPgHealth schemaName queueTables = do
  dbRows <- executeQuery (rows pgDbHealthCodec Sql.pgDbHealthSQL)
  tableRows <- executeQuery (rows pgTableHealthCodec (Sql.pgTableHealthSQL schemaName queueTables'))
  pure (listToMaybe dbRows, tableRows)
  where
    queueTables' = concatMap queueTableNames queueTables

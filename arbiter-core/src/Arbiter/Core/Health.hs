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

import Arbiter.Core.Codec (Col (..), RowCodec, col, pval)
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Sql.Health qualified as Sql

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

-- | The database snapshot (one row) plus per-table churn for the schema.
getPgHealth :: (MonadArbiter m) => SchemaName -> m (Maybe PgDbHealth, [PgTableHealth])
getPgHealth schemaName = do
  dbRows <- executeQuery Sql.pgDbHealthSQL [] pgDbHealthCodec
  tableRows <- executeQuery Sql.pgTableHealthSQL [pval CText schemaName] pgTableHealthCodec
  pure (listToMaybe dbRows, tableRows)

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Types and DDL for the @arbiter_workers@ table.
module Arbiter.Core.Worker
  ( WorkerRow (..)
  , WorkerHealth (..)
  , workerHealthFromText
  , arbiterWorkersTable
  , arbiterWorkersTableName
  , createWorkersTableSQL
  , addClaimedByColumnSQL
  , addCancelRequestedAtColumnSQL
  , addArchiveForColumnSQL
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), Value, withText)
import Data.Aeson qualified as Aeson
import Data.Int (Int32)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)
import GHC.Generics (Generic)

import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.SqlLiterals (quoteIdentifier)

-- | Heartbeat-derived health of a worker, orthogonal to its 'paused' flag.
data WorkerHealth
  = Live
  | Stale
  | Draining
  deriving stock (Eq, Generic, Show)

instance ToJSON WorkerHealth where
  toJSON = \case
    Live -> Aeson.String "live"
    Stale -> Aeson.String "stale"
    Draining -> Aeson.String "draining"

instance FromJSON WorkerHealth where
  parseJSON = withText "WorkerHealth" $ either (fail . T.unpack) pure . workerHealthFromText

-- | Decode the @health@ SQL token into a 'WorkerHealth'.
workerHealthFromText :: Text -> Either Text WorkerHealth
workerHealthFromText = \case
  "live" -> Right Live
  "stale" -> Right Stale
  "draining" -> Right Draining
  other -> Left ("unknown worker health: " <> other)

-- | A row in the worker registry. One row per running worker pool.
data WorkerRow = WorkerRow
  { workerId :: UUID
  , queueName :: Text
  , hostName :: Maybe Text
  , workerCount :: Maybe Int32
  , startedAt :: UTCTime
  , lastHeartbeat :: UTCTime
  , shuttingDown :: Bool
  , paused :: Bool
  , staleThresholdSecs :: Double
  , metadata :: Maybe Value
  , health :: WorkerHealth
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Qualified table name for the arbiter_workers table.
arbiterWorkersTable :: SchemaName -> Text
arbiterWorkersTable schemaName = quoteIdentifier schemaName <> "." <> arbiterWorkersTableName

arbiterWorkersTableName :: Text
arbiterWorkersTableName = "arbiter_workers"

-- | DDL for the @arbiter_workers@ table.
createWorkersTableSQL :: SchemaName -> Text
createWorkersTableSQL schemaName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> arbiterWorkersTable schemaName <> " ("
    , "  worker_id UUID PRIMARY KEY,"
    , "  queue_name TEXT NOT NULL,"
    , "  host_name TEXT,"
    , "  worker_count INT,"
    , "  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  shutting_down BOOLEAN NOT NULL DEFAULT FALSE,"
    , "  paused BOOLEAN NOT NULL DEFAULT FALSE,"
    , "  stale_threshold_secs DOUBLE PRECISION NOT NULL DEFAULT 300,"
    , "  metadata JSONB"
    , ");"
    ]

-- | Idempotent migration adding the @claimed_by@ column to a queue's job and DLQ tables.
addClaimedByColumnSQL :: SchemaName -> TableName -> Text
addClaimedByColumnSQL schemaName tableName =
  T.unlines
    [ "ALTER TABLE " <> jobQueueTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS claimed_by UUID;"
    , "ALTER TABLE " <> jobQueueDLQTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS claimed_by UUID;"
    ]

-- | Idempotent migration adding the @cancel_requested_at@ column to a queue's
-- job table, with a partial index backing the reaper's flagged-job sweep.
addCancelRequestedAtColumnSQL :: SchemaName -> TableName -> Text
addCancelRequestedAtColumnSQL schemaName tableName =
  T.unlines
    [ "ALTER TABLE " <> jobQueueTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ;"
    , "CREATE INDEX IF NOT EXISTS "
        <> quoteIdentifier ("idx_" <> tableName <> "_cancel_requested")
        <> " ON "
        <> jobQueueTable schemaName tableName
        <> " (id ASC) WHERE cancel_requested_at IS NOT NULL;"
    ]

-- | Idempotent migration adding the @archive_for@ column to a queue's job and DLQ tables.
addArchiveForColumnSQL :: SchemaName -> TableName -> Text
addArchiveForColumnSQL schemaName tableName =
  T.unlines
    [ "ALTER TABLE " <> jobQueueTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS archive_for INT;"
    , "ALTER TABLE " <> jobQueueDLQTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS archive_for INT;"
    ]

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Types and DDL for the @arbiter_workers@ table.
module Arbiter.Core.Worker
  ( WorkerRow (..)
  , WorkerHealth (..)
  , workerHealthToText
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

import Arbiter.Core.Enum (enumFromText)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.SqlLiterals (quoteIdentifier)

-- | Heartbeat-derived health of a worker. Independent of its 'paused' flag.
data WorkerHealth
  = -- | The heartbeat is fresh and the worker is not shutting down.
    Live
  | -- | The last heartbeat is older than the worker's stale threshold.
    Stale
  | -- | The heartbeat is fresh and the worker is shutting down.
    Draining
  deriving stock (Bounded, Enum, Eq, Generic, Show)

instance ToJSON WorkerHealth where
  toJSON = Aeson.String . workerHealthToText

-- | The @health@ SQL token of a 'WorkerHealth'.
workerHealthToText :: WorkerHealth -> Text
workerHealthToText = \case
  Live -> "live"
  Stale -> "stale"
  Draining -> "draining"

instance FromJSON WorkerHealth where
  parseJSON = withText "WorkerHealth" $ either (fail . T.unpack) pure . workerHealthFromText

-- | Decode the @health@ SQL token into a 'WorkerHealth'.
workerHealthFromText :: Text -> Either Text WorkerHealth
workerHealthFromText = enumFromText "worker health" workerHealthToText

-- | A row in the worker registry. One row per running worker pool.
data WorkerRow = WorkerRow
  { workerId :: UUID
  -- ^ The worker pool's id.
  , queueName :: Text
  -- ^ The queue the pool works.
  , hostName :: Maybe Text
  -- ^ The host the pool runs on.
  , workerCount :: Maybe Int32
  -- ^ The pool's worker thread count.
  , startedAt :: UTCTime
  -- ^ When the pool registered.
  , lastHeartbeat :: UTCTime
  -- ^ When the pool last sent a heartbeat.
  , shuttingDown :: Bool
  -- ^ Whether the pool is draining.
  , paused :: Bool
  -- ^ The worker's own pause flag. The queue's flag is separate.
  , staleThresholdSecs :: Double
  -- ^ Heartbeat age in seconds after which the pool counts as stale.
  , metadata :: Maybe Value
  -- ^ Free-form metadata the pool registered.
  , health :: WorkerHealth
  -- ^ Health derived from the heartbeat and the shutdown flag.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Qualified table name for the @arbiter_workers@ table.
arbiterWorkersTable :: SchemaName -> Text
arbiterWorkersTable schemaName = quoteIdentifier schemaName <> "." <> arbiterWorkersTableName

-- | Bare name of the workers table, for catalog lookups by relname.
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

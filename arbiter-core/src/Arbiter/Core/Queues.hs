{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Types and DDL for the @arbiter_queues@ table. One row per queue, scoped to
-- the schema. Holds operator-facing per-queue state (pause flag, DLQ retention,
-- metadata). Migrations add a row for every queue they create. Maintenance reads
-- the table to find the schema's queues.
module Arbiter.Core.Queues
  ( QueueRow (..)
  , arbiterQueuesTable
  , arbiterQueuesTableName
  , createQueuesTableSQL
  , addDLQRetentionColumnSQL
  , registerQueuesSQL
  ) where

import Data.Aeson (FromJSON, ToJSON, Value)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime)
import GHC.Generics (Generic)

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.SqlLiterals (sharedTable)

-- | A row of the @arbiter_queues@ table.
data QueueRow = QueueRow
  { queueName :: Text
  -- ^ The queue's table name.
  , paused :: Bool
  -- ^ Whether the queue is paused.
  , pausedAt :: Maybe UTCTime
  -- ^ When the queue was paused. 'Nothing' while it runs.
  , metadata :: Maybe Value
  -- ^ Free-form metadata.
  , createdAt :: UTCTime
  -- ^ When the row was created.
  , updatedAt :: UTCTime
  -- ^ When the row last changed.
  , dlqRetention :: Maybe NominalDiffTime
  -- ^ Age in seconds at which the reaper purges the queue's DLQ jobs. 'Nothing' keeps them.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Qualified table name for the @arbiter_queues@ table.
arbiterQueuesTable :: SchemaName -> Text
arbiterQueuesTable schemaName = sharedTable schemaName arbiterQueuesTableName

-- | Bare name of the queues table, for catalog lookups by relname.
arbiterQueuesTableName :: Text
arbiterQueuesTableName = "arbiter_queues"

-- | DDL for the @arbiter_queues@ table.
createQueuesTableSQL :: SchemaName -> Text
createQueuesTableSQL schemaName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> arbiterQueuesTable schemaName <> " ("
    , "  queue_name TEXT PRIMARY KEY,"
    , "  paused BOOLEAN NOT NULL DEFAULT FALSE,"
    , "  paused_at TIMESTAMPTZ,"
    , "  metadata JSONB,"
    , "  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
    , ");"
    ]

-- | Idempotent migration adding the @dlq_retention_secs@ column. A retention must be positive.
addDLQRetentionColumnSQL :: SchemaName -> Text
addDLQRetentionColumnSQL schemaName =
  "ALTER TABLE "
    <> arbiterQueuesTable schemaName
    <> " ADD COLUMN IF NOT EXISTS dlq_retention_secs BIGINT CHECK (dlq_retention_secs > 0);"

-- | Add a row with defaults for each queue in the one @text[]@ parameter. Rows that
-- exist are left alone.
registerQueuesSQL :: SchemaName -> Text
registerQueuesSQL schemaName =
  "INSERT INTO "
    <> arbiterQueuesTable schemaName
    <> " (queue_name) SELECT unnest(?::text[]) ON CONFLICT (queue_name) DO NOTHING"

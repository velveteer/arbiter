{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Types and DDL for the @arbiter_queues@ table. One row per queue, scoped to
-- the schema. Holds operator-facing per-queue state (pause flag, metadata).
module Arbiter.Core.Queues
  ( QueueRow (..)
  , arbiterQueuesTable
  , createQueuesTableSQL
  ) where

import Data.Aeson (FromJSON, ToJSON, Value)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import GHC.Generics (Generic)

import Arbiter.Core.Job.Schema (SchemaName, quoteIdentifier)

data QueueRow = QueueRow
  { queueName :: Text
  , paused :: Bool
  , pausedAt :: Maybe UTCTime
  , metadata :: Maybe Value
  , createdAt :: UTCTime
  , updatedAt :: UTCTime
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Qualified table name for the arbiter_queues table.
arbiterQueuesTable :: SchemaName -> Text
arbiterQueuesTable schemaName = quoteIdentifier schemaName <> ".arbiter_queues"

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

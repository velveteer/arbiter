{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Schema-scoped watermark gates for global tasks. One row per task holds @last_run_at@
-- and an optional @metadata@ value.
-- 'Arbiter.Core.Operations.runGated' claims the row with @SELECT ... FOR UPDATE SKIP LOCKED@
-- once the interval has elapsed. At most one worker pool runs the task per interval.
module Arbiter.Core.Gates
  ( arbiterGatesTable
  , arbiterGatesTableName
  , createGatesTableSQL
  , addGateMetadataColumnSQL
  , gateEpochSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.SqlLiterals (sharedTable)

-- | Qualified name of the gates table.
arbiterGatesTable :: SchemaName -> Text
arbiterGatesTable schemaName = sharedTable schemaName arbiterGatesTableName

-- | Bare name of the gates table, for catalog lookups by relname.
arbiterGatesTableName :: Text
arbiterGatesTableName = "arbiter_gates"

-- | The @last_run_at@ of a gate that has not run.
gateEpochSQL :: Text
gateEpochSQL = "'1970-01-01'::timestamptz"

-- | DDL for the @arbiter_gates@ table.
createGatesTableSQL :: SchemaName -> Text
createGatesTableSQL schemaName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> arbiterGatesTable schemaName <> " ("
    , "  task_name TEXT PRIMARY KEY,"
    , "  last_run_at TIMESTAMPTZ NOT NULL DEFAULT " <> gateEpochSQL
    , ");"
    ]

-- | Add the @metadata@ column a task stores its result or resume state in.
addGateMetadataColumnSQL :: SchemaName -> Text
addGateMetadataColumnSQL schemaName =
  "ALTER TABLE " <> arbiterGatesTable schemaName <> " ADD COLUMN IF NOT EXISTS metadata JSONB;"

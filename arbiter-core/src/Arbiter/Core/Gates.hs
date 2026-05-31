{-# LANGUAGE OverloadedStrings #-}

-- | Schema-scoped watermark gates for global tasks (refresh groups, sweep stale
-- workers). One row per task holding @last_run_at@. 'Arbiter.Core.Operations.runGated'
-- claims the row with @SELECT ... FOR UPDATE SKIP LOCKED@ once the interval has
-- elapsed, so at most one worker pool runs the task per interval.
module Arbiter.Core.Gates
  ( arbiterGatesTable
  , createGatesTableSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Core.Job.Schema (SchemaName, quoteIdentifier)

-- | Qualified name of the gates table.
arbiterGatesTable :: SchemaName -> Text
arbiterGatesTable schemaName = quoteIdentifier schemaName <> ".arbiter_gates"

-- | DDL for the @arbiter_gates@ table.
createGatesTableSQL :: SchemaName -> Text
createGatesTableSQL schemaName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> arbiterGatesTable schemaName <> " ("
    , "  task_name TEXT PRIMARY KEY,"
    , "  last_run_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01'::timestamptz"
    , ");"
    ]

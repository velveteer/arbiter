{-# LANGUAGE OverloadedStrings #-}

-- | DDL for the three run tables. They are schema-wide, one set per arbiter schema,
-- and they ship as one tracked migration through
-- @Arbiter.Migrations.MigrationConfig.extraMigrations@, so they take the same schema
-- lock and the same checksum discipline as the queue's own. The queue tables are
-- untouched.
module Arbiter.Workflow.Schema
  ( workflowRunsTable
  , workflowRunsTableName
  , workflowStepsTable
  , workflowStepsTableName
  , workflowEdgesTable
  , workflowEdgesTableName
  , workflowTables
  , workflowMigrations
  ) where

import Arbiter.Core.Job.Schema (SchemaName, qualifiedTable)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Database.PostgreSQL.Simple.Migration (MigrationCommand (..))

-- | Qualified name of the runs table.
workflowRunsTable :: SchemaName -> Text
workflowRunsTable schemaName = qualifiedTable schemaName workflowRunsTableName

-- | Bare name of the runs table, for catalog lookups by relname.
workflowRunsTableName :: Text
workflowRunsTableName = "arbiter_workflow_runs"

-- | Qualified name of the steps table.
workflowStepsTable :: SchemaName -> Text
workflowStepsTable schemaName = qualifiedTable schemaName workflowStepsTableName

-- | Bare name of the steps table.
workflowStepsTableName :: Text
workflowStepsTableName = "arbiter_workflow_steps"

-- | Qualified name of the edges table.
workflowEdgesTable :: SchemaName -> Text
workflowEdgesTable schemaName = qualifiedTable schemaName workflowEdgesTableName

-- | Bare name of the edges table.
workflowEdgesTableName :: Text
workflowEdgesTableName = "arbiter_workflow_edges"

-- | Every table this layer owns, qualified. Truncating the runs table cascades to
-- the other two.
workflowTables :: SchemaName -> [Text]
workflowTables schemaName = map (qualifiedTable schemaName) [workflowRunsTableName, workflowStepsTableName, workflowEdgesTableName]

-- | The layer's tracked migrations. Pass these to @extraMigrations@. Add-only: to
-- change a shipped body, ship a renamed script beside it.
workflowMigrations :: SchemaName -> [MigrationCommand]
workflowMigrations schemaName =
  [ MigrationScript "create-workflow-tables" (encodeUtf8 (createWorkflowTablesSQL schemaName))
  ]

-- | The runs, steps and edges tables with their indexes.
createWorkflowTablesSQL :: SchemaName -> Text
createWorkflowTablesSQL schemaName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> runs <> " ("
    , "  id BIGSERIAL PRIMARY KEY,"
    , "  workflow TEXT NOT NULL,"
    , "  version INTEGER NOT NULL,"
    , "  status TEXT NOT NULL,"
    , "  mode TEXT NOT NULL DEFAULT 'graph',"
    , "  input JSONB,"
    , "  output JSONB,"
    , "  traceparent TEXT,"
    , "  tracestate TEXT,"
    , "  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  finished_at TIMESTAMPTZ"
    , ");"
    , "CREATE INDEX IF NOT EXISTS arbiter_workflow_runs_status_idx ON " <> runs <> " (status, id DESC);"
    , "CREATE INDEX IF NOT EXISTS arbiter_workflow_runs_name_idx ON " <> runs <> " (workflow, version, id DESC);"
    , "CREATE INDEX IF NOT EXISTS arbiter_workflow_runs_finished_idx ON "
        <> runs
        <> " (finished_at) WHERE finished_at IS NOT NULL;"
    , "CREATE TABLE IF NOT EXISTS " <> steps <> " ("
    , "  id BIGSERIAL PRIMARY KEY,"
    , "  run_id BIGINT NOT NULL REFERENCES " <> runs <> " (id) ON DELETE CASCADE,"
    , "  name TEXT NOT NULL,"
    , "  kind TEXT NOT NULL,"
    , "  status TEXT NOT NULL,"
    , "  queue TEXT,"
    , "  job_id BIGINT,"
    , "  wait_count INTEGER NOT NULL DEFAULT 0,"
    , "  output JSONB,"
    , "  signal_key TEXT,"
    , "  deadline TIMESTAMPTZ,"
    , "  step_index INTEGER"
    , ");"
    , "CREATE UNIQUE INDEX IF NOT EXISTS arbiter_workflow_steps_name_idx ON " <> steps <> " (run_id, name);"
    , "CREATE UNIQUE INDEX IF NOT EXISTS arbiter_workflow_steps_job_idx ON "
        <> steps
        <> " (queue, job_id) WHERE queue IS NOT NULL AND job_id IS NOT NULL;"
    , "CREATE INDEX IF NOT EXISTS arbiter_workflow_steps_run_idx ON " <> steps <> " (run_id, status);"
    , "CREATE INDEX IF NOT EXISTS arbiter_workflow_steps_deadline_idx ON "
        <> steps
        <> " (deadline) WHERE deadline IS NOT NULL AND status IN ('waiting', 'ready');"
    , "CREATE TABLE IF NOT EXISTS " <> edges <> " ("
    , "  from_step BIGINT NOT NULL REFERENCES " <> steps <> " (id) ON DELETE CASCADE,"
    , "  to_step BIGINT NOT NULL REFERENCES " <> steps <> " (id) ON DELETE CASCADE,"
    , "  PRIMARY KEY (from_step, to_step)"
    , ");"
    , "CREATE INDEX IF NOT EXISTS arbiter_workflow_edges_to_idx ON " <> edges <> " (to_step);"
    ]
  where
    runs = workflowRunsTable schemaName
    steps = workflowStepsTable schemaName
    edges = workflowEdgesTable schemaName

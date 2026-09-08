-- | Workflow graphs over the Arbiter job queue.
module Arbiter.Workflow
  ( -- * Defining
    Workflow (Workflow)
  , workflowName
  , workflowVersion
  , Graph
  , Absent
  , Ref
  , Expr
  , use
  , both
  , step
  , stepWith
  , signal
  , branch
  , choose
  , expand
  , forEach
  , embed
  , under

    -- * Registering
  , WorkflowRegistry
  , SomeWorkflow
  , workflow
  , workflows
  , lookupWorkflow
  , latestVersion
  , registeredWorkflows

    -- * Checkpoints
  , checkpoint
  , startCheckpointRun
  , adoptCheckpointRun
  , withCheckpointRun
  , withCheckpointRuns
  , defaultCheckpointVersion
  , withRunRetention

    -- * Running
  , startWorkflow
  , startWorkflowWith
  , startWorkflowByName
  , withWorkflows
  , withWorkflowMaintenance
  , sweepWorkflows

    -- * Operations
  , RunId (..)
  , StepId (..)
  , StepName (..)
  , RunStatus (..)
  , RunMode (..)
  , StepStatus (..)
  , StepKind (..)
  , RunRow (..)
  , StepRow (..)
  , SettleOutcome (..)
  , Advance (..)
  , ReadyStep (..)
  , getRun
  , listSteps
  , runOfJob
  , cancelRun
  , sendWorkflowSignal
  , settleWorkflowJob
  , failStepForJob
  , failRun
  , retryRun
  , sweepSignalDeadlines
  , reviveRetriedRuns
  , purgeFinishedRuns

    -- * Rendering
  , Rendered (..)
  , render

    -- * Storage
  , workflowMigrations
  , workflowTables
  , workflowRunsTable
  , workflowStepsTable
  , workflowEdgesTable
  , runStatusText
  , runStatusFromText
  , runModeText
  , runModeFromText
  , stepStatusText
  , stepStatusFromText
  , stepKindText
  , stepKindFromText
  ) where

import Arbiter.Workflow.Checkpoint (adoptCheckpointRun, checkpoint, startCheckpointRun, withCheckpointRun)
import Arbiter.Workflow.Expr (Expr, Ref, both, use)
import Arbiter.Workflow.Graph hiding (fmap, join, pure, return, (<*>), (>>), (>>=))
import Arbiter.Workflow.Interpret (Rendered (..), render)
import Arbiter.Workflow.Ops
import Arbiter.Workflow.Registry
import Arbiter.Workflow.Schema
import Arbiter.Workflow.Settle
import Arbiter.Workflow.Types
import Arbiter.Workflow.Worker

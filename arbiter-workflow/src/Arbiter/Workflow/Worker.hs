{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Starting a run, and giving a worker pool the settle.
module Arbiter.Workflow.Worker
  ( startWorkflow
  , startWorkflowWith
  , startWorkflowByName
  , withWorkflows
  , withWorkflowMaintenance
  , withCheckpointRuns
  , defaultCheckpointVersion
  , withRunRetention
  , sweepWorkflows
  ) where

import Arbiter.Core.Job.TraceContext (TraceContext)
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Settled (SettledJob (..), SettledOutcome (..))
import Arbiter.Core.Trace (currentTraceContext)
import Arbiter.Worker.Config (WorkerConfig (..), withExtraMaintenance, withJobSettled, withPostCronInsert)
import Control.Applicative ((<|>))
import Control.Monad (void)
import Data.Aeson (ToJSON, Value, toJSON)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import UnliftIO (liftIO)

import Arbiter.Workflow.Checkpoint (adoptCheckpointRun)
import Arbiter.Workflow.Graph (Workflow (..))
import Arbiter.Workflow.Interpret (materialize)
import Arbiter.Workflow.Ops (advanceReady, startRun)
import Arbiter.Workflow.Registry (SomeWorkflow (..), WorkflowRegistry, latestVersion, lookupWorkflow)
import Arbiter.Workflow.Settle
  ( failRun
  , failStepForJob
  , purgeFinishedRuns
  , reviveRetriedRuns
  , settleWorkflowJob
  , sweepSignalDeadlines
  , workflowReady
  )
import Arbiter.Workflow.Types (RunId)

-- | Start a run of one definition, under the span the caller is in.
startWorkflow
  :: (MonadArbiter m, ToJSON input)
  => WorkflowRegistry registry
  -> Workflow registry input output
  -> input
  -> m (Either Text RunId)
startWorkflow registry definition input = do
  trace <- liftIO currentTraceContext
  startWorkflowWith trace registry definition input

-- | 'startWorkflow' under a span the caller names. Every job of the run inherits it.
startWorkflowWith
  :: (MonadArbiter m, ToJSON input)
  => Maybe TraceContext
  -> WorkflowRegistry registry
  -> Workflow registry input output
  -> input
  -> m (Either Text RunId)
startWorkflowWith trace registry definition input = startFrom trace registry definition (toJSON input)

-- | Start a run of the definition registered under this name, at the version given or
-- the highest one registered.
startWorkflowByName
  :: (MonadArbiter m)
  => WorkflowRegistry registry
  -> Text
  -> Maybe Int32
  -> Value
  -> m (Either Text RunId)
startWorkflowByName registry name version input =
  case version <|> latestVersion registry name of
    Nothing -> pure (Left ("no definition is registered under " <> name))
    Just resolved -> case lookupWorkflow registry name resolved of
      Nothing -> pure (Left ("no definition is registered under " <> name <> " at that version"))
      Just (SomeWorkflow definition) -> do
        trace <- liftIO currentTraceContext
        startFrom trace registry definition input

startFrom
  :: (MonadArbiter m)
  => Maybe TraceContext
  -> WorkflowRegistry registry
  -> Workflow registry input output
  -> Value
  -> m (Either Text RunId)
startFrom trace registry definition input =
  case materialize definition input Map.empty of
    Left err -> pure (Left err)
    Right built -> do
      runId <- startRun (workflowName definition) (workflowVersion definition) input trace built
      driven <- advanceReady (workflowReady registry runId) runId
      case driven of
        Left err -> Left err <$ failRun runId
        Right _ -> pure (Right runId)

-- | Give a pool everything the workflow layer needs: the settle, which runs in the
-- transaction that takes a step's job out of its queue, whether an ack deleted it or a
-- failure dead-lettered it; the maintenance pass; and the record that makes each job a
-- schedule fires a checkpoint run. Each composes with what the caller already set.
withWorkflows
  :: (MonadArbiter m, ToJSON payload)
  => WorkflowRegistry registry
  -> WorkerConfig m payload
  -> WorkerConfig m payload
withWorkflows registry =
  withWorkflowMaintenance
    . withCheckpointRuns defaultCheckpointVersion
    . withJobSettled (traverse_ (settleSettled registry))

-- | The version 'withWorkflows' records a schedule's runs under.
defaultCheckpointVersion :: Int32
defaultCheckpointVersion = 1

-- | Record the job each of this pool's schedules fires as a checkpoint run named for
-- the schedule, in the transaction that inserted it. 'withWorkflows' applies this.
withCheckpointRuns
  :: (MonadArbiter m, ToJSON payload)
  => Int32
  -> WorkerConfig m payload
  -> WorkerConfig m payload
withCheckpointRuns version =
  withPostCronInsert (\schedule _tick job -> void (adoptCheckpointRun schedule version job))

-- | Run the workflow maintenance pass on the reaper's cadence, behind a gate of its
-- own, so one pool in a deployment runs it per interval. 'withWorkflows' already
-- applies this; reach for it alone on a pool that runs no workflow queue of its own.
withWorkflowMaintenance :: (MonadArbiter m) => WorkerConfig m payload -> WorkerConfig m payload
withWorkflowMaintenance = withExtraMaintenance workflowSweepTask sweepWorkflows

-- | Delete the runs that finished longer than @keepFor@ ago, on the reaper's cadence
-- and behind a gate of its own. Steps and edges cascade with the run.
withRunRetention
  :: (MonadArbiter m)
  => NominalDiffTime
  -> WorkerConfig m payload
  -> WorkerConfig m payload
withRunRetention keepFor = withExtraMaintenance runPurgeTask (purgeFinishedRuns keepFor)

runPurgeTask :: Text
runPurgeTask = "workflow-purge"

-- | Fail the runs whose signal deadline has passed, and pick up the ones whose dead
-- step was retried. Returns how many rows the two touched.
sweepWorkflows :: (MonadArbiter m) => m Int64
sweepWorkflows = (+) <$> sweepSignalDeadlines <*> reviveRetriedRuns

workflowSweepTask :: Text
workflowSweepTask = "workflow-sweep"

-- | Settle one job that left its queue: an ack advances the run, a dead letter fails
-- it. A settle that cannot go on fails its run rather than rolling the ack back, which
-- would run the same handler again to the same end.
settleSettled :: (MonadArbiter m) => WorkflowRegistry registry -> SettledJob -> m ()
settleSettled registry settled = case settledOutcome settled of
  JobAcked -> void (settleWorkflowJob registry (settledQueue settled) (settledJobId settled) (settledResult settled))
  JobDeadLettered -> void (failStepForJob (settledQueue settled) (settledJobId settled))

{-# LANGUAGE GADTs #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The settle that interprets a run's definition: it materializes what a decision
-- revealed, settles the virtual steps inline, and gives each ready step its job.
module Arbiter.Workflow.Settle
  ( settleWorkflowJob
  , sendWorkflowSignal
  , workflowReady
  , failRun
  , failStepForJob
  , retryRun
  , sweepSignalDeadlines
  , reviveRetriedRuns
  , purgeFinishedRuns
  ) where

import Arbiter.Core.Job.TraceContext (TraceContext, toTraceContext)
import Arbiter.Core.Job.Types (primaryKey, setTraceContext)
import Arbiter.Core.MonadArbiter (MonadArbiter, executeQuery, getSchema, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (void)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT (..), runExceptT, throwE)
import Data.Aeson (Value (Null))
import Data.Bifunctor (first)
import Data.Either (fromRight)
import Data.Foldable (traverse_)
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, isJust, listToMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Time (NominalDiffTime, addUTCTime, getCurrentTime)
import UnliftIO (liftIO)

import Arbiter.Workflow.Graph (Node (..))
import Arbiter.Workflow.Interpret (Materialized (..), materialize)
import Arbiter.Workflow.Ops
  ( Advance (..)
  , ReadyHandler
  , ReadyStep (..)
  , SettleOutcome (..)
  , lockRun
  , sendSignalWith
  , settleStep
  , stepIds
  , toReadyStep
  , writeSteps
  )
import Arbiter.Workflow.Registry (SomeWorkflow (..), WorkflowRegistry, lookupWorkflow)
import Arbiter.Workflow.Sql
import Arbiter.Workflow.Types
  ( RunId (..)
  , RunMode (..)
  , RunStatus (..)
  , StepId (..)
  , StepKind (..)
  , StepName (..)
  , runModeFromText
  )

-- | Advance the run one job finished. Runs in the ack's transaction.
settleWorkflowJob
  :: (MonadArbiter m)
  => WorkflowRegistry registry
  -> Text
  -> Int64
  -> Maybe Value
  -> m (Either Text SettleOutcome)
settleWorkflowJob registry queue jobId result = do
  schemaName <- getSchema
  found <- executeQuery (stepForJobSQL schemaName queue jobId)
  case listToMaybe found of
    Nothing -> pure (Right NotAStep)
    Just (stepId, runId, _, _) -> do
      mode <- readMode schemaName (RunId runId)
      case mode of
        CheckpointRun -> Right <$> closeCheckpointRun schemaName (RunId runId) (StepId stepId) result
        GraphRun -> do
          let handler = workflowReady registry (RunId runId)
          outcome <- settleStep handler schemaName (RunId runId) (StepId stepId) result
          either (\err -> Left err <$ failRun (RunId runId)) (pure . Right) outcome

-- | The steps a settle made ready, given the run's definition: materialize what a
-- decision revealed, settle the virtual steps, and give each job step its job.
workflowReady :: (MonadArbiter m) => WorkflowRegistry registry -> RunId -> ReadyHandler m
workflowReady registry runId ready = runExceptT $ do
  schemaName <- lift getSchema
  (built, trace) <- ExceptT (readGraph registry schemaName runId)
  existing <- lift (stepIds schemaName runId)
  newly <- lift (writeSteps schemaName runId existing built)
  stored <- lift (readOutputs schemaName runId)
  let byName = Map.fromList [(nodeName node, node) | node <- materializedNodes built]
  (newly <>) . concat <$> traverse (ExceptT . advanceStep schemaName trace byName stored) ready

-- | Put a failed run back to work: each failed step goes back to ready, and a step
-- whose job died comes out of the dead-letter queue first. A run that is not failed,
-- or a failed step with no job to resume, is refused.
retryRun :: (MonadArbiter m) => RunId -> m (Either Text ())
retryRun runId = withDbTransaction $ do
  schemaName <- getSchema
  status <- lockRun schemaName runId
  case status of
    RunFailed -> runExceptT $ do
      failed <- lift (executeQuery (failedStepsSQL schemaName runId))
      traverse_ (ExceptT . reopen schemaName) failed
      lift (void (executeQuery (reopenRunSQL schemaName runId)))
    _ -> pure (Left "run is not failed")

-- | One failed step, back to ready.
reopen :: (MonadArbiter m) => Text -> (Int64, Text, Maybe Text, Maybe Int64) -> m (Either Text ())
reopen schemaName (stepId, name, queue, jobId) =
  case (queue, jobId) of
    (Just onQueue, Just held) -> do
      present <- Ops.jobExists schemaName onQueue held
      revived <- if present then pure True else retryDead schemaName onQueue held
      if revived
        then Right () <$ executeQuery (reopenStepSQL schemaName (StepId stepId))
        else pure (Left ("step " <> name <> " has no job left to retry"))
    _ -> pure (Left ("step " <> name <> " holds no job to retry"))

-- | Bring one job back from its queue's dead-letter queue.
retryDead :: (MonadArbiter m) => Text -> Text -> Int64 -> m Bool
retryDead schemaName queue jobId = do
  dead <- executeQuery (deadLetterOfSQL schemaName queue jobId)
  case listToMaybe dead of
    Nothing -> pure False
    Just dlqId -> isJust <$> Ops.retryFromDLQ @_ @Value schemaName queue dlqId

-- | Deliver a signal and advance the run from it, interpreting its definition.
sendWorkflowSignal
  :: (MonadArbiter m)
  => WorkflowRegistry registry
  -> RunId
  -> Text
  -> Value
  -> m (Either Text SettleOutcome)
sendWorkflowSignal registry runId key value =
  sendSignalWith (workflowReady registry runId) runId key value

-- | How a run's steps come to exist. A run whose row is gone reads as a graph run,
-- and the settle that follows finds nothing to advance.
readMode :: (MonadArbiter m) => Text -> RunId -> m RunMode
readMode schemaName runId = do
  found <- executeQuery (runModeSQL schemaName runId)
  pure (fromRight GraphRun (runModeFromText (fromMaybe "graph" (listToMaybe found))))

-- | A checkpoint run finishes when its job does. Its steps are already recorded.
closeCheckpointRun :: (MonadArbiter m) => Text -> RunId -> StepId -> Maybe Value -> m SettleOutcome
closeCheckpointRun schemaName runId stepId result = do
  status <- lockRun schemaName runId
  void (executeQuery (completeStepSQL schemaName stepId result))
  case status of
    RunRunning -> do
      finished <- executeQuery (finishRunWithSQL schemaName runId result)
      pure (Advanced (Advance runId stepId [] (not (null finished))))
    _ -> pure (RunClosed runId)

-- | Close a run as failed. Its other steps run to their own end.
failRun :: (MonadArbiter m) => RunId -> m ()
failRun runId = do
  schemaName <- getSchema
  void (executeQuery (closeRunSQL schemaName runId RunFailed))

-- | Fail the step one job belongs to, and the run behind it. Reports whether the job
-- was a step at all.
failStepForJob :: (MonadArbiter m) => Text -> Int64 -> m Bool
failStepForJob queue jobId = withDbTransaction $ do
  schemaName <- getSchema
  found <- executeQuery (stepForJobSQL schemaName queue jobId)
  case listToMaybe found of
    Nothing -> pure False
    Just (stepId, runId, _, _) -> True <$ failStepAndRun schemaName (RunId runId) (StepId stepId)

-- | Fail one step and the run behind it, taking the run's row first.
failStepAndRun :: (MonadArbiter m) => Text -> RunId -> StepId -> m ()
failStepAndRun schemaName runId stepId = do
  void (executeQuery (lockRunSQL schemaName runId))
  void (executeQuery (failStepSQL schemaName stepId))
  failRun runId

-- | Fail the run of every signal step whose deadline has passed, in one pass. Returns
-- the number of steps swept.
sweepSignalDeadlines :: (MonadArbiter m) => m Int64
sweepSignalDeadlines = withDbTransaction $ do
  schemaName <- getSchema
  sum <$> executeQuery (expireSignalStepsSQL schemaName maintenanceBatch)

-- | Put back to work every failed run whose failed step holds a job again, which is
-- what a retry from the dead-letter queue leaves behind. The jobs are looked for one
-- queue at a time, since each queue is a table of its own. Returns the number revived.
reviveRetriedRuns :: (MonadArbiter m) => m Int64
reviveRetriedRuns = withDbTransaction $ do
  schemaName <- getSchema
  failed <- executeQuery (failedRunStepsSQL schemaName maintenanceBatch)
  present <- traverse (queuePresent schemaName) (Map.toAscList (byQueue failed))
  let back = Set.unions present
      revivable = [(runId, stepId) | (runId, stepId, queue, jobId) <- failed, Set.member (queue, jobId) back]
  if null revivable
    then pure 0
    else sum <$> executeQuery (reviveStepsSQL schemaName (map fst revivable) (map snd revivable))
  where
    byQueue failed = Map.fromListWith (<>) [(queue, [jobId]) | (_, _, queue, jobId) <- failed]
    queuePresent schemaName (queue, jobIds) =
      Set.fromList . map (queue,) <$> executeQuery (presentJobsSQL schemaName queue jobIds)

-- | Delete the runs that finished longer than @keepFor@ ago, with their steps and
-- edges. Returns the number deleted.
purgeFinishedRuns :: (MonadArbiter m) => NominalDiffTime -> m Int64
purgeFinishedRuns keepFor = withDbTransaction $ do
  schemaName <- getSchema
  cutoff <- addUTCTime (negate keepFor) <$> liftIO getCurrentTime
  sum <$> executeQuery (purgeFinishedRunsSQL schemaName cutoff maintenanceBatch)

-- | How many rows one maintenance pass takes.
maintenanceBatch :: Int
maintenanceBatch = 100

-- | The graph a run has materialized, and the span its jobs inherit.
readGraph
  :: (MonadArbiter m)
  => WorkflowRegistry registry
  -> Text
  -> RunId
  -> m (Either Text (Materialized, Maybe TraceContext))
readGraph registry schemaName runId = do
  found <- executeQuery (runDefinitionSQL schemaName runId)
  case listToMaybe found of
    Nothing -> pure (Left "workflow: the run is gone")
    Just (name, version, input, traceparent, tracestate) ->
      case lookupWorkflow registry name version of
        Nothing -> pure (Left ("workflow: no definition for " <> name <> " at the version this run started under"))
        Just (SomeWorkflow definition) -> do
          stored <- readOutputs schemaName runId
          pure ((,toTraceContext traceparent tracestate) <$> materialize definition input stored)

-- | Every output a run's settled steps have stored. A step that stored nothing reads
-- back as @null@.
readOutputs :: (MonadArbiter m) => Text -> RunId -> m (Map StepName Value)
readOutputs schemaName runId =
  Map.fromList . map (first StepName . fmap (fromMaybe Null)) <$> executeQuery (runOutputsSQL schemaName runId)

-- | Take one ready step as far as it goes: a virtual step settles inline, a job step
-- gets its job, a signal step waits.
advanceStep
  :: (MonadArbiter m)
  => Text
  -> Maybe TraceContext
  -> Map StepName Node
  -> Map StepName Value
  -> ReadyStep
  -> m (Either Text [ReadyStep])
advanceStep schemaName trace byName stored step =
  case (readyStepKind step, Map.lookup (readyStepName step) byName) of
    (KindSignal, _) -> pure (Right [])
    (_, Nothing) -> pure (Left ("workflow: the definition has no step named " <> stepNameText (readyStepName step)))
    (KindJob, Just node) -> enqueue schemaName trace stored step node
    (_, Just node) -> settleVirtual schemaName stored step node

-- | Give a ready job step its job. A step that already holds one is left alone: its job
-- is claimable already, and taking its row here would close a cycle with the ack that
-- holds it and wants this run's row.
enqueue
  :: (MonadArbiter m)
  => Text
  -> Maybe TraceContext
  -> Map StepName Value
  -> ReadyStep
  -> Node
  -> m (Either Text [ReadyStep])
enqueue schemaName trace stored step node =
  runExceptT $ case (readyStepQueue step, readyStepJob step) of
    (Just _, Just _) -> pure []
    (Nothing, _) -> throwE (stepIs " names no queue")
    (Just queue, Nothing) -> do
      build <- maybe (throwE (stepIs " has no job to enqueue")) pure (nodeJob node)
      job <- either throwE pure (build stored)
      inserted <- lift (Ops.insertJob schemaName queue (setTraceContext trace job))
      row <- maybe (throwE ("workflow: the job of step " <> named <> " was not inserted")) pure inserted
      [] <$ lift (executeQuery (setStepJobSQL schemaName (readyStepId step) queue (primaryKey row)))
  where
    named = stepNameText (readyStepName step)
    stepIs what = "workflow: step " <> named <> what

-- | Settle a branch, a continuation or a merge inline, and report what it made ready.
settleVirtual
  :: (MonadArbiter m)
  => Text
  -> Map StepName Value
  -> ReadyStep
  -> Node
  -> m (Either Text [ReadyStep])
settleVirtual schemaName stored step node = runExceptT $ do
  compute <-
    maybe
      (throwE ("workflow: step " <> stepNameText (readyStepName step) <> " has nothing to settle to"))
      pure
      (nodeCompute node)
  output <- either throwE pure (compute stored)
  lift (void (executeQuery (completeStepSQL schemaName (readyStepId step) (Just output))))
  map toReadyStep <$> lift (executeQuery (advanceDependentsSQL schemaName (readyStepId step)))

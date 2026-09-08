{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}

-- | A run whose steps its handler records as it goes.
--
-- A checkpoint commits when it is reached, so a handler that fails part way runs
-- again and skips what it already did. That needs a handler mode with no worker
-- transaction around it: under
-- @Arbiter.Worker.Config.transactionalWorkerConfig@ the whole handler rolls back
-- together, checkpoints included. Use @manualWorkerConfig@ or a batched pool.
module Arbiter.Workflow.Checkpoint
  ( checkpoint
  , startCheckpointRun
  , adoptCheckpointRun
  , withCheckpointRun
  ) where

import Arbiter.Core.Exceptions (throwParsing)
import Arbiter.Core.Job.TraceContext (traceparent, tracestate)
import Arbiter.Core.Job.Types (JobPayload, JobRead, JobWrite, payload, primaryKey, queueName, traceContext)
import Arbiter.Core.MonadArbiter (MonadArbiter, executeQuery, getSchema, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (TableForPayload)
import Control.Monad (void)
import Data.Aeson (FromJSON, Result (..), ToJSON, Value, fromJSON, toJSON)
import Data.Int (Int32, Int64)
import Data.Maybe (listToMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import GHC.TypeLits (KnownSymbol, symbolVal)

import Arbiter.Workflow.Ops (one)
import Arbiter.Workflow.Sql
import Arbiter.Workflow.Types

-- | Run an action once for the run this job belongs to, under a name of its own. A
-- name already recorded gives back what it stored without running the action again.
-- A job no run owns runs its actions every time and records nothing.
checkpoint
  :: (FromJSON a, MonadArbiter m, ToJSON a)
  => JobRead payload
  -> Text
  -> m a
  -> m a
checkpoint job name action = do
  schemaName <- getSchema
  stored <- executeQuery (checkpointOutputSQL schemaName (queueName job) (primaryKey job) name)
  case listToMaybe stored of
    Just (Just value) -> case fromJSON value of
      Success done -> pure done
      Error err -> throwParsing ("checkpoint " <> name <> " does not decode: " <> T.pack err)
    _ -> do
      done <- action
      done <$ executeQuery (insertCheckpointSQL schemaName (queueName job) (primaryKey job) name (toJSON done))

-- | Start a run whose handler records its own steps. Its one job carries the work.
startCheckpointRun
  :: forall registry payload m
   . (JobPayload payload, KnownSymbol (TableForPayload payload registry), MonadArbiter m)
  => Text
  -- ^ Definition name, for grouping runs of one kind.
  -> Int32
  -- ^ Definition version.
  -> Value
  -- ^ Run input, for the record.
  -> JobWrite payload
  -> m RunId
startCheckpointRun name version input job = withDbTransaction $ do
  schemaName <- getSchema
  let queue = T.pack (symbolVal (Proxy @(TableForPayload payload registry)))
  runId <-
    one "startCheckpointRun: the run insert returned no row" $
      insertRunSQL schemaName name version CheckpointRun input Nothing Nothing
  inserted <- Ops.insertJob schemaName queue job
  case inserted of
    Nothing -> throwParsing "startCheckpointRun: the run's job was not inserted"
    Just row -> RunId runId <$ recordJobStep schemaName runId queue (primaryKey row)

-- | Record a job as a checkpoint run under a definition name and version. A job
-- already recorded gives back its run.
adoptCheckpointRun
  :: (MonadArbiter m, ToJSON payload)
  => Text
  -> Int32
  -> JobRead payload
  -> m RunId
adoptCheckpointRun name version job = withDbTransaction $ do
  schemaName <- getSchema
  found <- executeQuery (stepForJobSQL schemaName (queueName job) (primaryKey job))
  case listToMaybe found of
    Just (_, runId, _, _) -> pure (RunId runId)
    Nothing -> do
      let trace = traceContext job
      runId <-
        one "adoptCheckpointRun: the run insert returned no row" $
          insertRunSQL
            schemaName
            name
            version
            CheckpointRun
            (toJSON (payload job))
            (traceparent <$> trace)
            (tracestate =<< trace)
      RunId runId <$ recordJobStep schemaName runId (queueName job) (primaryKey job)

-- | Run a handler's work as a checkpoint run of the job it holds.
withCheckpointRun
  :: (MonadArbiter m, ToJSON payload)
  => Text
  -> Int32
  -> JobRead payload
  -> m a
  -> m a
withCheckpointRun name version job action = adoptCheckpointRun name version job *> action

recordJobStep :: (MonadArbiter m) => Text -> Int64 -> Text -> Int64 -> m ()
recordJobStep schemaName runId queue jobId =
  void . one "checkpoint run: the step insert returned no row" $
    insertStepsSQL schemaName runId [step]
  where
    step =
      NewStep
        { newStepName = "job"
        , newStepKind = stepKindText KindJob
        , newStepStatus = stepStatusText StepReady
        , newStepQueue = Just queue
        , newStepJobId = Just jobId
        , newStepOutput = Nothing
        , newStepSignalKey = Nothing
        , newStepDeadline = Nothing
        , newStepIndex = Nothing
        }

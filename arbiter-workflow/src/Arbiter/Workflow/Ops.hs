{-# LANGUAGE OverloadedStrings #-}

-- | Starting, advancing, cancelling and signalling a run.
--
-- 'settleStep' is the body of the post-ack hook, so it runs in the transaction of the
-- ack that finished a step and takes no transaction of its own. The others open one.
module Arbiter.Workflow.Ops
  ( SettleOutcome (..)
  , Advance (..)
  , ReadyStep (..)
  , ReadyHandler
  , toReadyStep
  , startRun
  , writeSteps
  , stepIds
  , settleStep
  , lockRun
  , sendSignalWith
  , advanceReady
  , cancelRun
  , getRun
  , runOfJob
  , listSteps
  , one
  , RunRow (..)
  , StepRow (..)
  ) where

import Arbiter.Core.Exceptions (throwParsing)
import Arbiter.Core.Job.TraceContext (TraceContext, traceparent, tracestate)
import Arbiter.Core.MonadArbiter (MonadArbiter, executeQuery, executeStatement, getSchema, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.Sql.Query (Query)
import Control.Monad (unless, void)
import Data.Aeson (Value)
import Data.Bifunctor (first)
import Data.Either (fromRight)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust, listToMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Time (UTCTime, getCurrentTime)
import UnliftIO (liftIO)

import Arbiter.Workflow.Graph (Node (..))
import Arbiter.Workflow.Interpret (Materialized (..), deadlineAt)
import Arbiter.Workflow.Sql
import Arbiter.Workflow.Types

-- | A step one settle made ready. A job step is given its job before it is reported.
-- A signal step waits for its value, and a virtual step settles inline.
data ReadyStep = ReadyStep
  { readyStepId :: StepId
  , readyStepName :: StepName
  , readyStepKind :: StepKind
  , readyStepQueue :: Maybe Text
  , readyStepJob :: Maybe Int64
  }
  deriving stock (Eq, Show)

-- | What one settle did to a run.
data Advance = Advance
  { advancedRun :: RunId
  , advancedStep :: StepId
  , advancedReady :: [ReadyStep]
  , advancedRunDone :: Bool
  }
  deriving stock (Eq, Show)

-- | What a settle found.
data SettleOutcome
  = -- | The job has no step row. Its handler is not part of a run.
    NotAStep
  | -- | The run is no longer running. The step keeps its output, and nothing is made ready.
    RunClosed RunId
  | Advanced Advance
  deriving stock (Eq, Show)

-- | Insert a run and the graph its definition materialized, in one transaction. Its
-- steps carry no job: the settle that finds a step ready gives it one.
startRun
  :: (MonadArbiter m)
  => Text
  -- ^ Definition name.
  -> Int32
  -- ^ Definition version.
  -> Value
  -- ^ Run input.
  -> Maybe TraceContext
  -- ^ The run span every step's job inherits.
  -> Materialized
  -> m RunId
startRun name version input trace built = withDbTransaction $ do
  schemaName <- getSchema
  runId <-
    one "startRun: the run insert returned no row" $
      insertRunSQL schemaName name version GraphRun input (traceparent <$> trace) (tracestate =<< trace)
  RunId runId <$ writeSteps schemaName (RunId runId) Map.empty built

-- | Write the nodes of a graph that hold no step row yet, with their edges, and report
-- the ones that came out ready. A run's first steps and the ones a decision later
-- reveals take this one path, so what a step waits on is derived in one place, from
-- the edges the write just laid down.
writeSteps :: (MonadArbiter m) => Text -> RunId -> Map StepName Int64 -> Materialized -> m [ReadyStep]
writeSteps schemaName (RunId rawRunId) existing built
  | null missing = pure []
  | otherwise = do
      now <- liftIO getCurrentTime
      inserted <- executeQuery (insertStepsSQL schemaName rawRunId (map (newStep now) missing))
      let ids = existing <> Map.fromList [(StepName name, stepId) | (stepId, name) <- inserted]
          edges =
            [ (from, to)
            | node <- missing
            , Just to <- [Map.lookup (nodeName node) ids]
            , predecessor <- nodeAfter node
            , Just from <- [Map.lookup predecessor ids]
            ]
      unless (null edges) $
        void (executeStatement (insertEdgesSQL schemaName (map fst edges) (map snd edges)))
      map toReadyStep <$> executeQuery (reconcileNewStepsSQL schemaName (map fst inserted))
  where
    missing = [node | node <- materializedNodes built, not (Map.member (nodeName node) existing)]

-- | One node as a step row. A node the builder knows the output of is written done.
newStep :: UTCTime -> Node -> NewStep
newStep now node =
  NewStep
    { newStepName = stepNameText (nodeName node)
    , newStepKind = stepKindText (nodeKind node)
    , newStepStatus = stepStatusText (if isJust (nodeOutput node) then StepDone else StepWaiting)
    , newStepQueue = nodeQueue node
    , newStepJobId = Nothing
    , newStepOutput = nodeOutput node
    , newStepSignalKey = nodeSignalKey node
    , newStepDeadline = deadlineAt now (nodeDeadline node)
    , newStepIndex = nodeIndex node
    }

-- | The step rows a run already holds, by name.
stepIds :: (MonadArbiter m) => Text -> RunId -> m (Map StepName Int64)
stepIds schemaName runId =
  Map.fromList . map (first StepName) <$> executeQuery (runStepIdsSQL schemaName runId)

-- | What a settle does with the steps it made ready. It may report more, which the
-- settle feeds back to it until it reports none.
type ReadyHandler m = [ReadyStep] -> m (Either Text [ReadyStep])

-- | Write a step's output, make the dependents that reached zero ready, hand them to
-- the handler, and finish the run when nothing is left. The runs row is the top lock.
settleStep
  :: (MonadArbiter m)
  => ReadyHandler m
  -> Text
  -> RunId
  -> StepId
  -> Maybe Value
  -> m (Either Text SettleOutcome)
settleStep handler schemaName runId stepId result = do
  status <- lockRun schemaName runId
  void (executeQuery (completeStepSQL schemaName stepId result))
  case status of
    RunRunning -> do
      ready <- map toReadyStep <$> executeQuery (advanceDependentsSQL schemaName stepId)
      fmap (Advanced . uncurry (Advance runId stepId)) <$> driveFrom handler schemaName runId ready
    _ -> pure (Right (RunClosed runId))

-- | Drive the ready steps to a standstill, then finish the run if nothing is left.
driveFrom
  :: (MonadArbiter m)
  => ReadyHandler m
  -> Text
  -> RunId
  -> [ReadyStep]
  -> m (Either Text ([ReadyStep], Bool))
driveFrom handler schemaName runId ready =
  drive handler ready >>= traverse finish
  where
    finish seen = do
      finished <- executeQuery (finishRunSQL schemaName runId)
      pure (seen, not (null finished))

-- | Hand the ready steps to the handler until a round takes nothing in and gives
-- nothing back. A step that settled inline can reveal more, so a productive round is
-- always followed by another.
--
-- A step reaches @ready@ once, so the rounds are bounded by the run's step count. A
-- step reported twice is this loop losing count, not a run to fail for.
drive :: (MonadArbiter m) => ReadyHandler m -> [ReadyStep] -> m (Either Text [ReadyStep])
drive handler = round' [] Set.empty
  where
    round' seen reported pending = handler pending >>= either (pure . Left) (next seen reported pending)
    next seen reported pending more
      | null more && null pending = pure (Right seen)
      | any ((`Set.member` reported') . readyStepId) more = pure (Left "workflow: the settle reported a step ready twice")
      | otherwise = round' (seen <> pending) reported' more
      where
        reported' = foldl' (\ids step -> Set.insert (readyStepId step) ids) reported pending

toReadyStep :: (Int64, Text, Text, Maybe Text, Maybe Int64) -> ReadyStep
toReadyStep (stepId, name, kind, queue, jobId) =
  ReadyStep
    { readyStepId = StepId stepId
    , readyStepName = StepName name
    , readyStepKind = fromRight KindJob (stepKindFromText kind)
    , readyStepQueue = queue
    , readyStepJob = jobId
    }

-- | Hand a run's ready steps to a handler, and finish the run if nothing is left.
-- This is what makes a run whose first step is a decision move at all.
advanceReady :: (MonadArbiter m) => ReadyHandler m -> RunId -> m (Either Text Bool)
advanceReady handler runId = withDbTransaction $ do
  schemaName <- getSchema
  status <- lockRun schemaName runId
  case status of
    RunRunning -> do
      ready <- map toReadyStep <$> executeQuery (readyStepsSQL schemaName runId)
      fmap snd <$> driveFrom handler schemaName runId ready
    _ -> pure (Right False)

-- | Cancel a run: its jobs in each queue, then its unsettled steps. Reports whether
-- the run was still running.
cancelRun :: (MonadArbiter m) => RunId -> m Bool
cancelRun runId = withDbTransaction $ do
  schemaName <- getSchema
  status <- lockRun schemaName runId
  case status of
    RunRunning -> do
      jobs <- executeQuery (runJobsSQL schemaName runId)
      -- Queues ascending, the lock order every statement over many queues' rows takes.
      traverse_
        (\(queue, jobIds) -> void (Ops.forceCancelJobs schemaName queue jobIds))
        (Map.toAscList (Map.fromListWith (<>) [(queue, [jobId]) | (queue, jobId) <- jobs]))
      void (executeQuery (cancelStepsSQL schemaName runId))
      void (executeQuery (closeRunSQL schemaName runId RunCancelled))
      pure True
    _ -> pure False

-- | Deliver a value to the step waiting on a signal key, and advance the run from it.
sendSignalWith
  :: (MonadArbiter m)
  => ReadyHandler m
  -> RunId
  -> Text
  -> Value
  -> m (Either Text SettleOutcome)
sendSignalWith handler runId key value = withDbTransaction $ do
  schemaName <- getSchema
  status <- lockRun schemaName runId
  case status of
    RunRunning -> do
      found <- executeQuery (waitingSignalStepSQL schemaName runId key)
      case listToMaybe found of
        Nothing -> pure (Left ("no step of this run is waiting on signal " <> key))
        Just stepId -> settleStep handler schemaName runId (StepId stepId) (Just value)
    _ -> pure (Left "run is not running")

-- | The run one job is a step of, if any.
runOfJob :: (MonadArbiter m) => Text -> Int64 -> m (Maybe RunId)
runOfJob queue jobId = do
  schemaName <- getSchema
  found <- executeQuery (stepForJobSQL schemaName queue jobId)
  pure (listToMaybe [RunId runId | (_, runId, _, _) <- found])

-- | One run.
getRun :: (MonadArbiter m) => RunId -> m (Maybe RunRow)
getRun runId = do
  schemaName <- getSchema
  found <- executeQuery (getRunSQL schemaName runId) >>= traverse decoded
  pure (listToMaybe found)

-- | A run's steps, in insertion order.
listSteps :: (MonadArbiter m) => RunId -> m [StepRow]
listSteps runId = do
  schemaName <- getSchema
  executeQuery (listStepsSQL schemaName runId) >>= traverse decoded

-- | Take the runs row and read its status.
lockRun :: (MonadArbiter m) => Text -> RunId -> m RunStatus
lockRun schemaName runId = do
  found <- executeQuery (lockRunSQL schemaName runId)
  case listToMaybe found of
    Nothing -> throwParsing "workflow: the run is gone"
    Just status -> decoded (runStatusFromText status)

-- | A decoded row, or a parse failure naming the stored value.
decoded :: (MonadArbiter m) => Either Text a -> m a
decoded = either throwParsing pure

-- | The single row a statement returns.
one :: (MonadArbiter m) => Text -> Query a -> m a
one missing statement = executeQuery statement >>= maybe (throwParsing missing) pure . listToMaybe

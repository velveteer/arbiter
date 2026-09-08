{-# LANGUAGE OverloadedStrings #-}

-- | The identifiers and states a run and its steps are stored under.
module Arbiter.Workflow.Types
  ( RunId (..)
  , RunMode (..)
  , runModeText
  , runModeFromText
  , StepId (..)
  , StepName (..)
  , RunStatus (..)
  , runStatusText
  , runStatusFromText
  , StepStatus (..)
  , stepStatusText
  , stepStatusFromText
  , StepKind (..)
  , stepKindText
  , stepKindFromText
  ) where

import Data.Int (Int64)
import Data.List (find)
import Data.Text (Text)

-- | One execution of a workflow definition.
newtype RunId = RunId Int64
  deriving newtype (Eq, Ord, Show)

-- | One step row. The lock order key within a run.
newtype StepId = StepId Int64
  deriving newtype (Eq, Ord, Show)

-- | A step's path in its definition. Unique per run, and the same on every replay.
newtype StepName = StepName {stepNameText :: Text}
  deriving newtype (Eq, Ord, Show)

-- | How a run's steps come to exist.
data RunMode
  = -- | A definition materializes them.
    GraphRun
  | -- | The handler records them as it runs.
    CheckpointRun
  deriving stock (Bounded, Enum, Eq, Ord, Show)

runModeText :: RunMode -> Text
runModeText GraphRun = "graph"
runModeText CheckpointRun = "checkpoint"

runModeFromText :: Text -> Either Text RunMode
runModeFromText = fromLabel "run mode" runModeText

-- | Where a run is. Only 'RunRunning' materializes new steps.
data RunStatus
  = RunRunning
  | RunDone
  | RunFailed
  | RunCancelled
  deriving stock (Bounded, Enum, Eq, Ord, Show)

runStatusText :: RunStatus -> Text
runStatusText RunRunning = "running"
runStatusText RunDone = "done"
runStatusText RunFailed = "failed"
runStatusText RunCancelled = "cancelled"

runStatusFromText :: Text -> Either Text RunStatus
runStatusFromText = fromLabel "run status" runStatusText

-- | Where a step is. The queue row stays authoritative for claimability.
data StepStatus
  = StepWaiting
  | StepReady
  | StepDone
  | StepFailed
  | StepCancelled
  deriving stock (Bounded, Enum, Eq, Ord, Show)

stepStatusText :: StepStatus -> Text
stepStatusText StepWaiting = "waiting"
stepStatusText StepReady = "ready"
stepStatusText StepDone = "done"
stepStatusText StepFailed = "failed"
stepStatusText StepCancelled = "cancelled"

stepStatusFromText :: Text -> Either Text StepStatus
stepStatusFromText = fromLabel "step status" stepStatusText

-- | What a step is. Only 'KindJob' and 'KindSignal' wait on anything outside the
-- transaction that made them ready. The rest settle inline.
data StepKind
  = KindInput
  | KindJob
  | KindSignal
  | KindBranch
  | KindExpand
  | KindMerge
  | KindCheckpoint
  deriving stock (Bounded, Enum, Eq, Ord, Show)

stepKindText :: StepKind -> Text
stepKindText KindInput = "input"
stepKindText KindJob = "job"
stepKindText KindSignal = "signal"
stepKindText KindBranch = "branch"
stepKindText KindExpand = "expand"
stepKindText KindMerge = "merge"
stepKindText KindCheckpoint = "checkpoint"

stepKindFromText :: Text -> Either Text StepKind
stepKindFromText = fromLabel "step kind" stepKindText

-- | Decode a stored label, naming what failed to decode.
fromLabel :: (Bounded a, Enum a) => Text -> (a -> Text) -> Text -> Either Text a
fromLabel label render stored =
  maybe (Left ("unknown " <> label <> ": " <> stored)) Right (find ((== stored) . render) [minBound .. maxBound])

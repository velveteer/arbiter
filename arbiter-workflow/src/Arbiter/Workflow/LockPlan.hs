-- | The order every workflow transaction takes its row locks in.
--
-- The runs row is the top lock for a run, and every path takes it exclusive, so the
-- settles, signals and cancels of one run run one at a time. A settle has to see
-- every other step's outcome to know the run has finished, and under @READ
-- COMMITTED@ two settles holding the row shared each miss the other's step and
-- neither finishes the run. A cancel skips a job row another transaction holds rather
-- than waiting on one, so a settle that holds its job row and wants the runs row
-- cannot close a cycle with it.
--
-- The statements the operations layer issues follow these plans. The lock-order
-- simulation runs them against a model of PostgreSQL row locks.
module Arbiter.Workflow.LockPlan
  ( LockMode (..)
  , HeldRows (..)
  , LockTarget (..)
  , LockStep (..)
  , settlePlan
  , signalPlan
  , advancePlan
  , cancelPlan
  , stepRowOrder
  , jobRowOrder
  ) where

import Arbiter.Core.Sql.Tree (HeldRows (..))
import Data.Bifunctor (second)
import Data.Int (Int64)
import Data.List (sortOn)
import Data.Ord (Down (..))
import Data.Set qualified as Set
import Data.Text (Text)

import Arbiter.Workflow.Types (RunId, StepId)

-- | How a lock pass takes a row.
data LockMode = Share | Exclusive
  deriving stock (Eq, Ord, Show)

-- | One row a workflow transaction locks.
data LockTarget
  = RunRow RunId
  | StepRow StepId
  | JobRow Text Int64
  deriving stock (Eq, Ord, Show)

-- | One lock a workflow transaction takes, in plan order.
data LockStep = LockStep
  { lockTarget :: LockTarget
  , lockMode :: LockMode
  , lockHeldRows :: HeldRows
  }
  deriving stock (Eq, Ord, Show)

-- | The settle path, run from the transaction of the ack that finished a step. The ack
-- already holds that step's job row, so the plan names it first. Past that it is an
-- advance: the steps it makes ready get their jobs by insert, not by lock.
settlePlan
  :: RunId
  -> (Text, Int64)
  -- ^ The settling step's queue and job, held by the ack.
  -> [StepId]
  -- ^ The step and its dependents.
  -> [LockStep]
settlePlan runId ackedJob steps =
  LockStep (uncurry JobRow ackedJob) Exclusive Wait : advancePlan runId steps

-- | The signal path. It holds no job row of its own, so it takes what an advance takes.
signalPlan :: RunId -> [StepId] -> [LockStep]
signalPlan = advancePlan

-- | The path that drives a run's ready steps, at a start and after a materialization.
-- It takes no job row at all: a step that already holds a job is left alone, and a step
-- that needs one gets it by insert. Taking a ready step's job row here closes a cycle
-- with the ack that holds it and wants this run's row.
advancePlan :: RunId -> [StepId] -> [LockStep]
advancePlan runId steps =
  LockStep (RunRow runId) Exclusive Wait
    : [LockStep (StepRow step) Exclusive Wait | step <- stepRowOrder steps]

-- | The cancel path. It takes the runs row exclusive, so it waits for the settles in
-- flight, then skips any job row one of them still holds. A handler whose row was
-- skipped settles later, finds the run is no longer running, and stops.
cancelPlan :: RunId -> [(Text, Int64)] -> [StepId] -> [LockStep]
cancelPlan runId jobs steps =
  LockStep (RunRow runId) Exclusive Wait
    : [LockStep (uncurry JobRow job) Exclusive Skip | job <- jobRowOrder jobs]
      <> [LockStep (StepRow step) Exclusive Wait | step <- stepRowOrder steps]

-- | Step rows ascending by id, deduplicated.
stepRowOrder :: [StepId] -> [StepId]
stepRowOrder = Set.toAscList . Set.fromList

-- | Job rows in the order the queue's own statements take them: queues ascending, and
-- within one queue descending by id, the children-first order ack, nack and
-- force-cancel share.
jobRowOrder :: [(Text, Int64)] -> [(Text, Int64)]
jobRowOrder = sortOn (second Down) . Set.toList . Set.fromList

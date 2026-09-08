{-# LANGUAGE OverloadedStrings #-}

-- | The workflow lock order under io-sim. Each scenario runs the plans
-- "Arbiter.Workflow.LockPlan" gives against a model of PostgreSQL row locks, and
-- IOSimPOR explores the schedules. A plan that could deadlock leaves every thread
-- blocked, and the explorer fails on it.
--
-- The model keeps the two invariants the queue gives the layer. A settling step's job
-- row is in flight, so it belongs to that ack alone. Every other job row of the run is
-- one a settle inserts, which no other transaction reaches before it commits, so two
-- settles of a run never contend for a job row. A cancel is under neither: it names any
-- row of its run and skips the ones it cannot take.
module Test.Arbiter.Workflow.LockSim (spec) where

import Control.Concurrent.Class.MonadSTM (atomically, check, newTVarIO, readTVar, writeTVar)
import Control.Monad (void, when)
import Control.Monad.Class.MonadAsync (concurrently, mapConcurrently)
import Control.Monad.Class.MonadTest (exploreRaces)
import Control.Monad.IOSim (IOSim)
import Data.Int (Int64)
import Data.Set qualified as Set
import Data.Text (Text)
import Test.Hspec (Spec, describe, it, shouldBe)
import Test.QuickCheck (Gen, Property, chooseInt, conjoin, counterexample, elements, property, sublistOf, vectorOf)

import Arbiter.Workflow.LockPlan
  ( HeldRows (..)
  , LockMode (..)
  , LockStep (..)
  , LockTarget (..)
  , advancePlan
  , cancelPlan
  , settlePlan
  , signalPlan
  )
import Arbiter.Workflow.Types (RunId (..), StepId (..))
import Test.Arbiter.Workflow.Sim
  ( TxId (..)
  , deadlocks
  , explorePlans
  , exploreScenario
  , newLockTable
  , runTransaction
  , runTransactionWith
  )

spec :: Spec
spec = describe "workflow lock order" $ do
  it "commits two settles that fan into one step of one run"
    $ exploreScenario tookEveryWait
    $ runPlans
      [ settlePlan aRun ("email", 10) [StepId 1, StepId 3]
      , settlePlan aRun ("email", 11) [StepId 2, StepId 3]
      ]

  it "commits a settle racing the cancel of its own run"
    $ exploreScenario tookEveryWait
    $ runPlans
      [ settlePlan aRun ("email", 10) [StepId 1, StepId 3]
      , cancelPlan aRun [("email", 10), ("report", 20), ("report", 21)] [StepId 1, StepId 2, StepId 3]
      ]

  it "commits a signal racing the cancel of its own run"
    $ exploreScenario tookEveryWait
    $ runPlans
      [ signalPlan aRun [StepId 2, StepId 4]
      , cancelPlan aRun [("email", 10), ("report", 20)] [StepId 2, StepId 4]
      ]

  it "commits a settle and the cancel of another run that share a queue"
    $ exploreScenario tookEveryWait
    $ runPlans
      [ settlePlan aRun ("email", 10) [StepId 1]
      , cancelPlan bRun [("email", 10), ("email", 12), ("report", 20)] [StepId 5, StepId 6]
      ]

  it "commits an advance racing a settle of its own run"
    $ exploreScenario tookEveryWait
    $ runPlans
      [ settlePlan aRun ("email", 10) [StepId 1, StepId 3]
      , advancePlan aRun [StepId 1, StepId 2, StepId 3]
      ]

  it "commits any mix of settles, signals and cancels" $
    explorePlans 50 tookEveryWait (runPlans <$> mixedPlans)

  it "deadlocks once the cancel waits on a job row instead of skipping it" $
    deadlocks waitingCancel `shouldBe` True

  it "deadlocks once the advance takes a job row an ack holds" $
    deadlocks advanceOverJob `shouldBe` True

aRun :: RunId
aRun = RunId 1

bRun :: RunId
bRun = RunId 2

-- | Run every plan concurrently, pairing each with what it locked.
runPlans :: [[LockStep]] -> IOSim s [([LockStep], [LockTarget])]
runPlans plans = do
  exploreRaces
  table <- newLockTable
  mapConcurrently (\(index, plan) -> (plan,) <$> runTransaction table (TxId index) plan) (zip [1 ..] plans)

-- | Every 'Wait' lock a plan names is one the transaction took. Only a 'Skip' pass
-- walks past a row, so a wait left unsatisfied is a deadlock.
tookEveryWait :: [([LockStep], [LockTarget])] -> Property
tookEveryWait outcomes = conjoin (map judge outcomes)
  where
    judge (plan, taken) =
      counterexample (show (plan, taken)) $
        property (all (`elem` taken) [lockTarget step | step <- plan, lockHeldRows step == Wait])

-- | A settle and a cancel of one run, with the cancel waiting on a held job row. The
-- barrier pins the interleaving the skip is there to break: the settle holds its job
-- row and wants the runs row, the cancel holds the runs row and wants that job row.
waitingCancel :: IOSim s ()
waitingCancel = do
  table <- newLockTable
  settleReady <- newTVarIO False
  cancelReady <- newTVarIO False
  let settle = settlePlan aRun ("email", 10) [StepId 1]
      cancel = [step {lockHeldRows = Wait} | step <- cancelPlan aRun [("email", 10)] [StepId 1]]
  void $
    concurrently
      (runTransactionWith table (TxId 1) settle (barrier settleReady cancelReady))
      (runTransactionWith table (TxId 2) cancel (barrier cancelReady settleReady))
  where
    barrier mine theirs index = when (index == 0) $ do
      atomically (writeTVar mine True)
      atomically (readTVar theirs >>= check)

-- | An advance that takes a ready step's job row, as it did before that row was left
-- alone. The barrier pins the interleaving: the ack holds the job row and wants the
-- runs row, the advance holds the runs row and wants that job row.
advanceOverJob :: IOSim s ()
advanceOverJob = do
  table <- newLockTable
  settleReady <- newTVarIO False
  advanceReady <- newTVarIO False
  let settle = settlePlan aRun ("email", 10) [StepId 1]
      advance = advancePlan aRun [StepId 1] <> [LockStep (JobRow "email" 10) Exclusive Wait]
  void $
    concurrently
      (runTransactionWith table (TxId 1) settle (barrier settleReady advanceReady))
      (runTransactionWith table (TxId 2) advance (barrier advanceReady settleReady))
  where
    barrier mine theirs index = when (index == 0) $ do
      atomically (writeTVar mine True)
      atomically (readTVar theirs >>= check)

-- | What one generated transaction is.
data Kind = Settle | Signal | Cancel
  deriving stock (Eq, Show)

-- | Two or three transactions over a small universe. Every row a transaction names
-- belongs to its own run, which is what the layer guarantees: a run's steps, and the
-- jobs behind them, are its own. Two settles in one run take no job row but the one
-- each acks, and those are rows of its own.
mixedPlans :: Gen [[LockStep]]
mixedPlans = do
  count <- chooseInt (2, 3)
  kinds <- vectorOf count (elements [Settle, Signal, Cancel])
  traverse somePlan (zip [0 ..] kinds)

somePlan :: (Int, Kind) -> Gen [LockStep]
somePlan (index, kind) = do
  runId <- elements [aRun, bRun]
  steps <- nonEmptySublist (stepsOf runId)
  jobs <- nonEmptySublist (jobsOf runId)
  pure $ case kind of
    Settle -> settlePlan runId (jobsOf runId !! index) steps
    Signal -> signalPlan runId steps
    Cancel -> cancelPlan runId jobs steps

-- | A run's step rows. Ids are unique across the schema, so two runs share none.
stepsOf :: RunId -> [StepId]
stepsOf runId = map (StepId . (+ base)) [1 .. 3]
  where
    base = if runId == aRun then 0 else 10

-- | A run's job rows. A settle acks one of them, and a cancel may name any.
jobsOf :: RunId -> [(Text, Int64)]
jobsOf runId = [(queue, jobBase runId + offset) | queue <- ["email", "report"], offset <- [0 .. 2]]

-- | Where a run's job ids start. The two runs share the queues, not the rows.
jobBase :: RunId -> Int64
jobBase runId = if runId == aRun then 1000 else 2000

-- | A sublist with at least one element, so every plan locks something.
nonEmptySublist :: (Ord a) => [a] -> Gen [a]
nonEmptySublist values = do
  chosen <- sublistOf values
  picked <- elements values
  pure (Set.toList (Set.fromList (picked : chosen)))

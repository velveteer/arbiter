-- | A model of PostgreSQL row locks under io-sim, and the explorers that run a
-- scenario against it. A transaction takes the locks its plan names and holds them
-- to commit. A cycle of waits leaves every thread blocked, which io-sim reports as a
-- deadlock and 'exploreScenario' fails on.
module Test.Arbiter.Workflow.Sim
  ( TxId (..)
  , LockTable
  , newLockTable
  , runTransaction
  , runTransactionWith
  , deadlocks
  , explorePlans
  , exploreScenario
  ) where

import Control.Concurrent.Class.MonadSTM
  ( TVar
  , atomically
  , modifyTVar'
  , newTVarIO
  , readTVar
  , retry
  , writeTVar
  )
import Control.Monad.Class.MonadThrow (bracket_)
import Control.Monad.IOSim (Failure (..), IOSim, SimTrace, exploreSimTrace, runIOSimPORGen, runSim, traceResult)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes)
import Data.Set (Set)
import Data.Set qualified as Set
import Test.QuickCheck (Gen, Property, counterexample, withNumTests)

import Arbiter.Workflow.LockPlan (HeldRows (..), LockMode (..), LockStep (..), LockTarget)

-- | One transaction in the model.
newtype TxId = TxId Int
  deriving newtype (Eq, Ord, Show)

-- | Who holds one row.
data Holders
  = SharedBy (Set TxId)
  | HeldBy TxId
  deriving stock (Eq, Show)

-- | The rows every transaction locks against.
type LockTable s = TVar (IOSim s) (Map LockTarget Holders)

newLockTable :: IOSim s (LockTable s)
newLockTable = newTVarIO Map.empty

-- | Run one transaction's plan and release its locks at commit. Returns the targets
-- it took, so a caller can assert what a 'Skip' pass walked past.
runTransaction :: LockTable s -> TxId -> [LockStep] -> IOSim s [LockTarget]
runTransaction table txid plan = runTransactionWith table txid plan (const (pure ()))

-- | 'runTransaction' running @between@ after the lock at each index, so a scenario can
-- pin an interleaving of its own.
runTransactionWith :: LockTable s -> TxId -> [LockStep] -> (Int -> IOSim s ()) -> IOSim s [LockTarget]
runTransactionWith table txid plan between =
  bracket_ (pure ()) (release table txid) (catMaybes <$> traverse step (zip [0 ..] plan))
  where
    step (index, lockStep) = acquire table txid lockStep <* between index

-- | Take one lock. A 'Wait' pass blocks until the row is free of conflicting
-- holders. A 'Skip' pass gives up on a held row and reports 'Nothing'.
acquire :: LockTable s -> TxId -> LockStep -> IOSim s (Maybe LockTarget)
acquire table txid (LockStep target mode heldRows) = atomically $ do
  rows <- readTVar table
  let taken = Map.lookup target rows
      grant = Just target <$ writeTVar table (Map.insert target (holdersFor txid mode taken) rows)
  case (compatible txid mode taken, heldRows) of
    (True, _) -> grant
    (False, Wait) -> retry
    (False, Skip) -> pure Nothing

-- | Whether this transaction may join a row's current holders.
compatible :: TxId -> LockMode -> Maybe Holders -> Bool
compatible _ _ Nothing = True
compatible txid _ (Just (HeldBy holder)) = holder == txid
compatible _ Share (Just (SharedBy _)) = True
compatible txid Exclusive (Just (SharedBy holders)) = Set.toList holders == [txid]

-- | The holders a row has once this transaction joins them.
holdersFor :: TxId -> LockMode -> Maybe Holders -> Holders
holdersFor txid Exclusive _ = HeldBy txid
holdersFor txid Share (Just (SharedBy holders)) = SharedBy (Set.insert txid holders)
holdersFor txid Share _ = SharedBy (Set.singleton txid)

-- | Drop every lock this transaction holds.
release :: LockTable s -> TxId -> IOSim s ()
release table txid = atomically (modifyTVar' table (Map.mapMaybe without))
  where
    without (HeldBy holder)
      | holder == txid = Nothing
    without (SharedBy holders)
      | Set.null remaining = Nothing
      | otherwise = Just (SharedBy remaining)
      where
        remaining = Set.delete txid holders
    without holders = Just holders

-- | Whether a scenario left its threads deadlocked. For the plans a correct order
-- rules out.
deadlocks :: (forall s. IOSim s a) -> Bool
deadlocks run = case runSim run of
  Left (FailureDeadlock _) -> True
  _ -> False

-- | Explore the schedules of @runs@ generated scenarios and judge each result.
explorePlans :: (Show a) => Int -> (a -> Property) -> (forall s. Gen (IOSim s a)) -> Property
explorePlans runs judge plans = withNumTests runs (runIOSimPORGen id (const (judgeTrace judge)) plans)

-- | Explore every schedule of one scenario and judge its result.
exploreScenario :: (Show a) => (a -> Property) -> (forall s. IOSim s a) -> Property
exploreScenario judge run = exploreSimTrace id run (const (judgeTrace judge))

-- | Judge a finished trace, failing on a deadlock or an escaped exception.
judgeTrace :: (Show a) => (a -> Property) -> SimTrace a -> Property
judgeTrace judge trace = case traceResult False trace of
  Left failure -> counterexample (show failure) False
  Right result -> counterexample (show result) (judge result)

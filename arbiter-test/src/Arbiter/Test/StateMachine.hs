{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Hedgehog properties for the job engine against any 'Arbiter.Core.MonadArbiter.MonadArbiter'
-- backend. A sequential property checks each operation's postconditions and the
-- engine invariants after each step. A concurrent property runs parallel branches
-- under the HOL detector.
module Arbiter.Test.StateMachine
  ( stateMachineSpec
  , ArbiterC
  , SMPayload (..)
  ) where

import Control.Exception (finally)
import Control.Monad (void)
import Data.Foldable (traverse_)
import Data.Text (Text)
import Hedgehog
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Hspec
import UnliftIO.Async (mapConcurrently_)

import Arbiter.Test.StateMachine.Actions
import Arbiter.Test.StateMachine.Guards (guardSpec)
import Arbiter.Test.StateMachine.LockOrder (lockOrderSpec)
import Arbiter.Test.StateMachine.Oracle
  ( WithConn
  , holViolations
  , installHolDetector
  , removeHolDetector
  , settledViolations
  , stepViolations
  , truncateHol
  )

-- | Random operation sequences. Each step's postconditions and the engine
-- invariants hold after it, and the settled invariants hold at the end.
prop_engine
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Env
  -> IO ()
  -> Property
prop_engine run env reset = withTests 300 $ property $ do
  acts <- forAll (Gen.list (Range.linear 1 40) (genAct SequentialMode))
  evalIO (resetSeeded reset schema withConn)
  traverse_ step (zip [1 :: Int ..] acts)
  settled <- evalIO (settledViolations schema table withConn)
  settled === []
  where
    schema = envSchema env
    table = envTable env
    withConn :: WithConn
    withConn = envWithConn env
    step (index, act) = do
      postconditions <- evalIO (run (interpret @sm env act))
      failures <- evalIO postconditions
      violations <- evalIO (stepViolations schema table withConn)
      map (\message -> "[step " <> show index <> " " <> show act <> "] " <> message) (failures <> violations) === []

-- | Concurrent property with up to eight independent action branches. The seed
-- reproduces the branches. Shrinking is off. Run them under the
-- caller-installed HOL detector. Then run one reaper tick and check the detector
-- log and settled summary.
prop_concurrent
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Env
  -> IO ()
  -> Property
-- A concurrent failure is nondeterministic and shrink candidates rarely
-- reproduce it. The seed-reproducible counterexample is kept.
prop_concurrent run env reset = withTests 100 $ withShrinks 0 $ property $ do
  branches <- forAll $ Gen.list (Range.linear 2 8) (Gen.list (Range.linear 5 25) (genAct ConcurrentMode))
  (hol, settled) <- evalIO $ do
    resetSeeded reset schema withConn
    truncateHol schema table withConn
    mapConcurrently_
      (traverse_ (\act -> withRetry (void (run (interpret @sm env act)))))
      branches
    void (run (runReaper @sm schema table))
    (,) <$> withConn (\conn -> holViolations conn schema table) <*> settledViolations schema table withConn
  (hol <> settled) === []
  where
    schema = envSchema env
    table = envTable env
    withConn :: WithConn
    withConn = envWithConn env

-- | Parameterized state-machine property suite. The runner executes a backend
-- action against the real database. @withConn@ exposes a raw 'PG.Connection' for
-- the oracle and HOL-detector SQL. @reset@ truncates the test tables.
-- The runner's registry must declare a queue for 'SMPayload' at the table name.
stateMachineSpec
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -- ^ Runner
  -> Text
  -> Text
  -> WithConn
  -- ^ Raw-connection accessor
  -> IO ()
  -- ^ Reset (truncate) action
  -> Spec
stateMachineSpec run schema table withConn reset = do
  it "core engine invariants hold over random operation sequences" $ do
    passed <- check (prop_engine @sm run env reset)
    passed `shouldBe` True
  it "no serialization or summary violation under N concurrent generated streams" $
    withinSecs 180 $ do
      withConn $ \conn -> installHolDetector conn schema table
      passed <-
        check (prop_concurrent @sm run env reset)
          `finally` withConn (\conn -> removeHolDetector conn schema table)
      passed `shouldBe` True
  lockOrderSpec @sm run schema table withConn reset
  guardSpec @sm run schema table withConn reset
  where
    env = Env schema table withConn

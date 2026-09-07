-- | Helpers shared by the io-sim suites: the event recorder, the guard under
-- test, and the plan explorer.
module Test.Arbiter.Worker.Sim
  ( Recorder
  , newRecorder
  , startGuard
  , refuseExtend
  , hangExtend
  , neverReturns
  , scripted
  , escaped
  , explorePlans
  , exploreScenario
  ) where

import Control.Concurrent.Class.MonadSTM (atomically, modifyTVar', newTVarIO, readTVarIO)
import Control.Exception (IOException, SomeAsyncException, SomeException, fromException)
import Control.Monad (void)
import Control.Monad.Class.MonadFork (forkIO)
import Control.Monad.Class.MonadThrow (throwIO)
import Control.Monad.Class.MonadTime.SI (DiffTime, Time, getMonotonicTime)
import Control.Monad.Class.MonadTimer.SI (threadDelay)
import Control.Monad.IOSim (IOSim, SimTrace, exploreSimTrace, runIOSimPORGen, traceResult)
import Test.QuickCheck (Gen, Property, counterexample, withNumTests)

import Arbiter.Worker.Heartbeat.Guard (GuardConfig, HeartbeatGuard, newHeartbeatGuard, runHeartbeatGuard)

-- | Records an event at the current monotonic time.
type Recorder s event = (Time -> event) -> IOSim s ()

-- | A recorder and the action that reads its log in order.
newRecorder :: IOSim s (Recorder s event, IOSim s [event])
newRecorder = do
  logVar <- newTVarIO []
  let recorder event = getMonotonicTime >>= \now -> atomically (modifyTVar' logVar (event now :))
  pure (recorder, reverse <$> readTVarIO logVar)

-- | A guard with its loop running on a thread of its own.
startGuard :: GuardConfig (IOSim s) job -> IOSim s (HeartbeatGuard (IOSim s) job)
startGuard config = do
  guard <- newHeartbeatGuard config
  guard <$ forkIO (void (runHeartbeatGuard guard))

-- | A scripted extend that throws.
refuseExtend :: IOSim s a
refuseExtend = throwIO (userError "extend refused" :: IOException)

-- | A scripted extend that never returns. The timeout interrupts it.
hangExtend :: IOSim s [a]
hangExtend = threadDelay neverReturns >> pure []

-- | Longer than any lease or horizon under test.
neverReturns :: DiffTime
neverReturns = 1_000_000

-- | The next scripted reply. The last one repeats, @fallback@ when there are none.
scripted :: a -> [a] -> (a, [a])
scripted fallback [] = (fallback, [])
scripted _ [reply] = (reply, [reply])
scripted _ (reply : rest) = (reply, rest)

-- | The message of an exception still asynchronous at a boundary.
escaped :: SomeException -> Maybe String
escaped exc = show exc <$ fromException @SomeAsyncException exc

-- | Explore the schedules of @runs@ generated plans and judge each result.
explorePlans :: (Show a) => Int -> (a -> Property) -> (forall s. Gen (IOSim s a)) -> Property
explorePlans runs judge plans = withNumTests runs (runIOSimPORGen id (const (judgeTrace judge)) plans)

-- | Explore every schedule of one scenario and judge its result.
exploreScenario :: (Show a) => (a -> Property) -> (forall s. IOSim s a) -> Property
exploreScenario judge run = exploreSimTrace id run (const (judgeTrace judge))

-- | Judge a finished trace's result, failing on a deadlock or an escaped exception.
judgeTrace :: (Show a) => (a -> Property) -> SimTrace a -> Property
judgeTrace judge trace = case traceResult False trace of
  Left failure -> counterexample (show failure) False
  Right result -> counterexample (show result) (judge result)

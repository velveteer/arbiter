-- | The worker pool's run state and its shutdown signal.
module Arbiter.Worker.WorkerState
  ( WorkerState (..)
  , newWorkerState
  , signalShutdown
  ) where

import Control.Concurrent.STM (TVar)
import Control.Concurrent.STM qualified as STM

-- | Create a new worker state initialized to 'Running'.
newWorkerState :: IO (TVar WorkerState)
newWorkerState = STM.newTVarIO Running

-- | Signal graceful shutdown on a worker state.
--
-- Workers will stop claiming new jobs, finish in-flight work, then exit.
signalShutdown :: TVar WorkerState -> IO ()
signalShutdown st = STM.atomically $ STM.writeTVar st ShuttingDown

-- | Effective state of a worker pool. Synthesized from two TVars on the
-- 'Arbiter.Worker.Config.WorkerConfig': 'ShuttingDown' if the (possibly
-- shared) shutdown TVar is set, else 'Paused' if the per-pool pause flag is
-- set, else 'Running'. Returned by 'Arbiter.Worker.Config.getWorkerState' /
-- 'Arbiter.Worker.Config.readEffectiveState'.
data WorkerState
  = Running
  | Paused
  | ShuttingDown
  deriving stock (Eq, Show)

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

-- | Signal graceful shutdown: the pool stops claiming, finishes what it holds, exits.
signalShutdown :: TVar WorkerState -> IO ()
signalShutdown st = STM.atomically $ STM.writeTVar st ShuttingDown

-- | A worker pool's effective state, read off the shutdown and pause flags on its
-- 'Arbiter.Worker.Config.WorkerConfig': shutdown wins, then pause, else running.
data WorkerState
  = Running
  | Paused
  | ShuttingDown
  deriving stock (Eq, Show)

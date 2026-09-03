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

-- | Signal graceful shutdown. The pool stops claiming, finishes what it holds, and exits.
signalShutdown :: TVar WorkerState -> IO ()
signalShutdown stateVar = STM.atomically $ STM.writeTVar stateVar ShuttingDown

-- | A worker pool's effective state, read off the shutdown and pause flags on its
-- 'Arbiter.Worker.Config.WorkerConfig'. Shutdown wins, then pause, then running.
data WorkerState
  = Running
  | Paused
  | ShuttingDown
  deriving stock (Eq, Show)

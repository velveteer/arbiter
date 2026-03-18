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

-- | State for worker pool coordination.
--
-- Controls whether workers claim new jobs:
--
--   * 'Running': Normal operation, claim jobs continuously
--   * 'Paused': Stop claiming new jobs, finish in-flight jobs, wait for resume
--   * 'ShuttingDown': Stop claiming new jobs, finish in-flight jobs, then exit
data WorkerState
  = -- | Normal operation
    Running
  | -- | Paused (stop claiming, finish in-flight, wait for resume)
    Paused
  | -- | Graceful shutdown in progress (drain and exit)
    ShuttingDown
  deriving stock (Eq, Show)

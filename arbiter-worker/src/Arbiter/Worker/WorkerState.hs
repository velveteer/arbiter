-- | The worker pool's run state.
module Arbiter.Worker.WorkerState
  ( WorkerState (..)

    -- * Internal
  , isShuttingDown
  ) where

import UnliftIO.STM (STM, TVar, readTVar)

-- | A worker pool's effective state, read off the shutdown and pause flags on its
-- 'Arbiter.Worker.Config.WorkerConfig'. Shutdown wins, then pause, then running.
data WorkerState
  = -- | Claims and runs jobs.
    Running
  | -- | Claims no new jobs.
    Paused
  | -- | Claims no new jobs and stops.
    ShuttingDown
  deriving stock (Eq, Show)

-- | Whether the pool state is 'ShuttingDown'.
isShuttingDown :: TVar WorkerState -> STM Bool
isShuttingDown stateVar = (== ShuttingDown) <$> readTVar stateVar

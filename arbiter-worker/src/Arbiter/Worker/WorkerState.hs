-- | The worker pool's run state.
module Arbiter.Worker.WorkerState
  ( WorkerState (..)
  ) where

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

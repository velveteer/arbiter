{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- The pool's heartbeat guard, run in IO. These are the IO wrappers over
-- "Arbiter.Worker.Heartbeat.Guard".
module Arbiter.Worker.Heartbeat
  ( PoolGuard
  , newHeartbeatGuard
  , runHeartbeatGuard
  , recheckJob
  ) where

import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (JobId, JobRead, ObservabilityHooks (..), notVisibleUntil, primaryKey)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Void (Void)
import UnliftIO (UnliftIO (..), askUnliftIO)
import UnliftIO.STM (atomically)

import Arbiter.Worker.Config (WorkerConfig (..), pulseHeartbeat)
import Arbiter.Worker.Heartbeat.Guard (GuardConfig (..), toDiffTime)
import Arbiter.Worker.Heartbeat.Guard qualified as Guard
import Arbiter.Worker.Logger.Internal (jobHook, poolLog)

-- | The pool's guard, run in IO and keyed on the job.
type PoolGuard payload = Guard.HeartbeatGuard IO (JobRead payload)

-- | Build the pool's guard from its config. IO wrapper over 'Arbiter.Worker.Heartbeat.Guard.newHeartbeatGuard'.
newHeartbeatGuard :: (JobOperation m payload) => WorkerConfig m payload -> m (PoolGuard payload)
newHeartbeatGuard config = do
  UnliftIO run <- askUnliftIO
  liftIO . Guard.newHeartbeatGuard $
    GuardConfig
      { configInterval = toDiffTime (jobHeartbeatInterval config)
      , configTimeout = toDiffTime (visibilityTimeout config)
      , configMaxDuration = toDiffTime <$> maxJobDuration config
      , configKey = primaryKey
      , configLease = notVisibleUntil
      , configExtend = run . Arb.setVisibilityTimeoutBatch (visibilityTimeout config)
      , configExtended = atomically (pulseHeartbeat config)
      , configLog = poolLog (logConfig config)
      , configHeartbeat = \job now start ->
          run (jobHook (logConfig config) job "onJobHeartbeat" (onJobHeartbeat (observabilityHooks config) job now start))
      }

-- | Run the guard loop. IO wrapper over 'Arbiter.Worker.Heartbeat.Guard.runHeartbeatGuard'.
runHeartbeatGuard :: (MonadIO m) => PoolGuard payload -> m Void
runHeartbeatGuard = liftIO . Guard.runHeartbeatGuard

-- | Extend the batch that holds the job now.
recheckJob :: (MonadIO m) => PoolGuard payload -> JobId -> m ()
recheckJob guard = liftIO . Guard.recheck guard

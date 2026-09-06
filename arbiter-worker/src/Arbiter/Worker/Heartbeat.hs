{-# LANGUAGE OverloadedStrings #-}

-- | The pool's heartbeat guard, run in IO. The guard itself is
-- "Arbiter.Worker.Heartbeat.Guard".
module Arbiter.Worker.Heartbeat
  ( HeartbeatGuard
  , newHeartbeatGuard
  , runHeartbeatGuard
  ) where

import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (JobRead, ObservabilityHooks (..), primaryKey)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Void (Void)
import UnliftIO (UnliftIO (..), askUnliftIO)
import UnliftIO.STM (atomically)

import Arbiter.Worker.Config (WorkerConfig (..), pulseHeartbeat)
import Arbiter.Worker.Heartbeat.Guard (GuardConfig (..), toDiffTime)
import Arbiter.Worker.Heartbeat.Guard qualified as Guard
import Arbiter.Worker.Logger.Internal (jobHook, poolLog)

type HeartbeatGuard payload = Guard.HeartbeatGuard IO (JobRead payload)

newHeartbeatGuard :: (JobOperation m payload) => WorkerConfig m payload -> m (HeartbeatGuard payload)
newHeartbeatGuard config = do
  UnliftIO run <- askUnliftIO
  liftIO . Guard.newHeartbeatGuard $
    GuardConfig
      { configInterval = toDiffTime (jobHeartbeatInterval config)
      , configTimeout = toDiffTime (visibilityTimeout config)
      , configMaxDuration = toDiffTime <$> maxJobDuration config
      , configKey = primaryKey
      , configExtend = run . Arb.setVisibilityTimeoutBatch (visibilityTimeout config)
      , configExtended = atomically (pulseHeartbeat config)
      , configLog = poolLog (logConfig config)
      , configHeartbeat = \job now start ->
          run (jobHook (logConfig config) job "onJobHeartbeat" (onJobHeartbeat (observabilityHooks config) job now start))
      }

runHeartbeatGuard :: (MonadIO m) => HeartbeatGuard payload -> m Void
runHeartbeatGuard = liftIO . Guard.runHeartbeatGuard

{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Heartbeat
  ( withJobsHeartbeat
  ) where

import Arbiter.Core.Exceptions (JobForceCancelled (..), throwJobGoneIds)
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (JobRead, ObservabilityHooks (..))
import Arbiter.Core.Trace (capturingContext)
import Control.Exception (throwIO)
import Control.Monad (forever, unless, void)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Set qualified as Set
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Void (absurd)
import UnliftIO.Async (race)
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.STM (TMVar, atomically)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Logger (LogConfig)
import Arbiter.Worker.Logger.Internal (runHook, withJobContext, withJobContextOne)
import Arbiter.Worker.Retry (retryOnExceptionForever)
import Arbiter.Worker.Settle (hasIdIn)

-- | Run an action under a heartbeat thread that extends its jobs' visibility timeout
-- every interval, so a long-running handler does not have its work claimed out from
-- under it. A job the handler already finalized is skipped, one that is simply gone is
-- not an error, and a reclaimed one throws: every job in a batch shares a deadline, so a
-- reclaim means the whole batch's lease has lapsed and the remaining work is wasted.
-- Fires the heartbeat hook each tick.
withJobsHeartbeat
  :: forall payload m a
   . (JobOperation m payload)
  => ObservabilityHooks m payload
  -- ^ Observability hooks (for heartbeat hook)
  -> NominalDiffTime
  -- ^ Heartbeat interval
  -> NominalDiffTime
  -- ^ Visibility timeout
  -> UTCTime
  -- ^ Start time (for calculating elapsed time in heartbeat hook)
  -> NonEmpty (JobRead payload)
  -- ^ The job(s) being processed
  -> m [JobRead payload]
  -- ^ Read each tick for the job(s) still awaiting an outcome.
  -> LogConfig
  -- ^ Log configuration
  -> TMVar ()
  -- ^ Proof-of-work signal pulsed after each successful heartbeat.
  -> m a
  -- ^ Action to run with heartbeat protection
  -> m a
withJobsHeartbeat hooks intervalSecs timeoutSecs startTime jobs pending logCfg signal action = do
  -- 'race' forks both sides, so the handler needs the job span reattached too.
  inherited <- capturingContext
  either id id <$> race (inherited (absurd <$> heartbeatThread)) (inherited action)
  where
    heartbeatThread =
      retryOnExceptionForever (withJobContext logCfg jobs) "Heartbeat" 3 $
        forever tick

    tick = do
      threadDelay (ceiling (intervalSecs * 1_000_000))
      live <- pending
      results <- Arb.setVisibilityTimeoutBatch timeoutSecs live
      atomically $ void $ STM.tryPutTMVar signal ()
      let cancelledJobs = [jobId | Arb.JobCancelled jobId <- results]
          stolenJobs = [jobId | Arb.JobReclaimed jobId _ _ <- results]
          goneJobs = [jobId | Arb.JobGone jobId <- results]
      unless (null cancelledJobs) $
        liftIO (throwIO (JobForceCancelled cancelledJobs (stolenJobs <> goneJobs)))
      unless (null stolenJobs) $
        throwJobGoneIds "reclaimed by another worker" stolenJobs
      let activeJobIds = Set.fromList [jobId | Arb.VisibilityExtended jobId <- results]
          activeJobs = filter (hasIdIn activeJobIds) live
      currentTime <- liftIO getCurrentTime
      traverse_
        ( \job ->
            runHook (withJobContextOne logCfg job) "onJobHeartbeat" $
              onJobHeartbeat hooks job currentTime startTime
        )
        activeJobs

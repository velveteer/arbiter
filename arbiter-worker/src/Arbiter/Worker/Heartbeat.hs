{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Heartbeat
  ( withJobsHeartbeat
  ) where

import Arbiter.Core.Exceptions (JobForceCancelled (..), throwJobGoneIds)
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (Job (..), JobRead, ObservabilityHooks (..))
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

-- | Run an action with a heartbeat that extends visibility timeout for all jobs.
--
-- The heartbeat runs in a separate thread spawned via 'race' and extends the
-- visibility timeout at regular intervals, preventing long-running jobs from
-- becoming visible and being claimed by another worker.
--
-- The heartbeat distinguishes between:
--
--   * Job successfully heartbeated - continue normally
--   * Job already completed (acked\/canceled by handler) - ignore, not an error
--   * Job stolen by another worker (claim token changed) - throw to abort
--
-- Every job in a batch shares one visibility deadline (each tick extends them
-- together), so a reclaim means the whole batch's lease has lapsed. The heartbeat
-- throws to abort the action and stop wasting work on jobs it no longer owns.
--
-- Only the jobs still awaiting an outcome are beaten. One the handler already
-- finalized has left this worker's lease, and its row no longer answers to the
-- claim token the claim recorded.
--
-- Calls onJobHeartbeat hook at each interval for monitoring long-running jobs.
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
          activeJobs = filter (\job -> Set.member (primaryKey job) activeJobIds) live
      currentTime <- liftIO getCurrentTime
      traverse_
        ( \job ->
            runHook (withJobContextOne logCfg job) "onJobHeartbeat" $
              onJobHeartbeat hooks job currentTime startTime
        )
        activeJobs

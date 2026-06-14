{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Heartbeat
  ( withJobsHeartbeat
  ) where

import Arbiter.Core.Exceptions (throwJobStolen)
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (Job (..), JobRead, ObservabilityHooks (..))
import Control.Monad (forever, unless, void)
import Control.Monad.Catch (MonadMask)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (toList, traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Void (absurd)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Async (race)
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.STM (TMVar, atomically)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Logger (LogConfig)
import Arbiter.Worker.Logger.Internal (runHook, showJobIds, withJobContext)
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
--   * Job stolen by another worker (attempts changed) - throw to abort
--
-- Every job in a batch shares one visibility deadline (each tick extends them
-- together), so a reclaim means the whole batch's lease has lapsed. The heartbeat
-- throws to abort the action and stop wasting work on jobs it no longer owns.
--
-- Calls onJobHeartbeat hook at each interval for monitoring long-running jobs.
withJobsHeartbeat
  :: forall registry m payload a
   . ( JobOperation m registry payload
     , MonadMask m
     , MonadUnliftIO m
     )
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
  -> LogConfig
  -- ^ Log configuration
  -> TMVar ()
  -- ^ Proof-of-work signal pulsed after each successful heartbeat.
  -> m a
  -- ^ Action to run with heartbeat protection
  -> m a
withJobsHeartbeat hooks intervalSecs timeoutSecs startTime jobs logCfg signal action =
  -- race forks both arms onto new threads; re-establish the job context on each
  -- so handler and heartbeat logs carry the job fields.
  either absurd id <$> race (withJobContext jobs heartbeatThread) (withJobContext jobs action)
  where
    heartbeatThread =
      retryOnExceptionForever logCfg "Heartbeat" 3 $
        forever tick

    tick = do
      threadDelay (ceiling (intervalSecs * 1_000_000))
      results <- Arb.setVisibilityTimeoutBatch timeoutSecs (toList jobs)
      atomically $ void $ STM.tryPutTMVar signal ()
      let stolenJobs = [jobId | Arb.JobReclaimed jobId _ _ <- results]
      unless (null stolenJobs) $
        throwJobStolen (showJobIds stolenJobs)
      let activeJobIds = [jobId | Arb.VisibilityExtended jobId <- results]
          activeJobs = filter (\job -> primaryKey job `elem` activeJobIds) (toList jobs)
      currentTime <- liftIO getCurrentTime
      traverse_
        ( \job ->
            runHook logCfg "onJobHeartbeat" $
              onJobHeartbeat hooks job currentTime startTime
        )
        activeJobs

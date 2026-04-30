{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Heartbeat
  ( withJobsHeartbeat
  ) where

import Arbiter.Core.Exceptions (throwJobStolen)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (Job (..), JobPayload, JobRead, ObservabilityHooks (..))
import Arbiter.Simple (SimpleEnv, runSimpleDb)
import Control.Concurrent.MVar qualified as MVar
import Control.Monad (forever, unless)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (toList, traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Void (absurd)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Async (race)
import UnliftIO.Concurrent (threadDelay)

import Arbiter.Worker.Logger (LogConfig)
import Arbiter.Worker.Logger.Internal (runHook)
import Arbiter.Worker.Retry (retryOnExceptionForever)

-- | Run an action with a heartbeat that extends visibility timeout for all jobs
--
-- The heartbeat runs in a separate thread and extends the visibility timeout at
-- regular intervals, preventing long-running jobs from becoming visible and being
-- claimed by another worker.
--
-- Uses 'race' to coordinate the heartbeat and action threads. If the heartbeat
-- detects a stolen job, its exception propagates out (cancelling the action).
-- If the action completes first, the heartbeat is cancelled cleanly.
--
-- The heartbeat distinguishes between:
--
--   * Job successfully heartbeated - continue normally
--   * Job already completed (acked\/canceled by handler) - ignore, not an error
--   * Job stolen by another worker (attempts changed) - throw to stop duplicate work
--
-- Calls onJobHeartbeat hook at each interval for monitoring long-running jobs.
withJobsHeartbeat
  :: forall registry m payload a
   . (JobPayload payload, MonadUnliftIO m)
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
  -> SimpleEnv registry
  -- ^ Dedicated heartbeat env (own connection pool, separate from worker pool)
  -> LogConfig
  -- ^ Log configuration
  -> Maybe (MVar.MVar ())
  -- ^ Liveness signal (pulsed after each successful heartbeat)
  -> m a
  -- ^ Action to run with heartbeat protection
  -> m a
withJobsHeartbeat hooks intervalSecs timeoutSecs startTime jobs heartbeatEnv logCfg mLivenessMVar action =
  either absurd id <$> race heartbeatThread action
  where
    heartbeatThread =
      retryOnExceptionForever logCfg "Heartbeat" 3 $
        forever tick

    tick = do
      liftIO $ threadDelay (ceiling (intervalSecs * 1_000_000))
      results <-
        runSimpleDb heartbeatEnv $
          Arb.setVisibilityTimeoutBatch timeoutSecs (toList jobs)
      traverse_ (\mv -> liftIO $ MVar.tryPutMVar mv ()) mLivenessMVar
      let stolenJobs = [jobId | Arb.JobReclaimed jobId _ _ <- results]
      unless (null stolenJobs) $
        throwJobStolen $
          "Heartbeat detected stolen jobs: "
            <> T.intercalate ", " (map (T.pack . show) stolenJobs)
            <> " (another worker reclaimed them, stopping to prevent duplicate processing)"
      let activeJobIds = [jobId | Arb.VisibilityExtended jobId <- results]
          activeJobs = filter (\job -> primaryKey job `elem` activeJobIds) (toList jobs)
      currentTime <- liftIO getCurrentTime
      traverse_
        ( \job ->
            runHook logCfg "onJobHeartbeat" $
              onJobHeartbeat hooks job currentTime startTime
        )
        activeJobs

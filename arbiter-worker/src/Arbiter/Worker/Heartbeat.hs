{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Heartbeat
  ( withJobsHeartbeat
  ) where

import Arbiter.Core.Exceptions (throwJobStolen)
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (Job (..), JobRead, ObservabilityHooks (..))
import Control.Concurrent.MVar qualified as MVar
import Control.Monad (forever, unless)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (toList, traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Void (Void, absurd)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Async (race)
import UnliftIO.Concurrent (threadDelay)

import Arbiter.Worker.Logger (LogConfig)
import Arbiter.Worker.Logger.Internal (runHook)

-- | Run an action with a heartbeat that extends visibility timeout for all jobs
--
-- The heartbeat runs in a separate thread and extends the visibility timeout at
-- regular intervals, preventing long-running jobs from becoming visible and being
-- claimed by another worker.
--
-- Uses 'race' to coordinate the heartbeat and action threads. If the heartbeat
-- detects a stolen job, its exception propagates out (cancelling the action).
-- If the action completes first, the heartbeat is cancelled cleanly — no stale
-- async exceptions can leak into the worker loop.
--
-- The heartbeat distinguishes between:
--
--   * Job successfully heartbeated - continue normally
--   * Job already completed (acked\/canceled by handler) - ignore, not an error
--   * Job stolen by another worker (attempts changed) - throw to stop duplicate work
--
-- Calls onJobHeartbeat hook at each interval for monitoring long-running jobs.
withJobsHeartbeat
  :: forall m registry payload a
   . ( JobOperation m registry payload
     , MonadUnliftIO m
     )
  => ObservabilityHooks m payload
  -- ^ Observability hooks (for heartbeat hook)
  -> Int
  -- ^ Heartbeat interval in seconds (e.g., 30)
  -> NominalDiffTime
  -- ^ Visibility timeout in seconds (e.g., 60)
  -> UTCTime
  -- ^ Start time (for calculating elapsed time in heartbeat hook)
  -> NonEmpty (JobRead payload)
  -- ^ The job(s) being processed
  -> LogConfig
  -- ^ Log configuration
  -> Maybe (MVar.MVar ())
  -- ^ Liveness signal (pulsed after each successful heartbeat)
  -> m a
  -- ^ Action to run with heartbeat protection
  -> m a
withJobsHeartbeat hooks intervalSecs timeoutSecs startTime jobs logCfg mLivenessMVar action =
  either absurd id <$> race (heartbeatLoop hooks intervalSecs timeoutSecs startTime jobs logCfg mLivenessMVar) action

-- | Heartbeat loop that extends visibility for all jobs at regular intervals
--
-- Runs forever, so the return type is 'Void' — it can only exit by throwing.
-- Uses batch operations for detailed per-job status. Only throws on stolen jobs
-- (another worker reclaimed the job). Jobs that were already acked/canceled by
-- the handler are silently ignored.
heartbeatLoop
  :: forall m registry payload
   . ( JobOperation m registry payload
     , MonadUnliftIO m
     )
  => ObservabilityHooks m payload
  -- ^ Observability hooks
  -> Int
  -- ^ Interval in seconds
  -> NominalDiffTime
  -- ^ Timeout in seconds
  -> UTCTime
  -- ^ Start time
  -> NonEmpty (JobRead payload)
  -- ^ Job(s) to heartbeat
  -> LogConfig
  -- ^ Log configuration
  -> Maybe (MVar.MVar ())
  -- ^ Liveness signal
  -> m Void
heartbeatLoop hooks intervalSecs timeoutSecs startTime jobs logCfg mLivenessMVar = forever $ do
  -- Wait for the interval
  liftIO $ threadDelay (intervalSecs * 1_000_000)

  -- Extend visibility and get detailed status for each job
  results <- Arb.setVisibilityTimeoutBatch timeoutSecs (toList jobs)

  -- Signal liveness after successful heartbeat
  traverse_ (\mv -> liftIO $ MVar.tryPutMVar mv ()) mLivenessMVar

  -- Check for stolen jobs (another worker reclaimed them)
  let stolenJobs = [jobId | Arb.JobReclaimed jobId _ _ <- results]
  unless (null stolenJobs) $
    throwJobStolen $
      "Heartbeat detected stolen jobs: "
        <> T.intercalate ", " (map (T.pack . show) stolenJobs)
        <> " (another worker reclaimed them, stopping to prevent duplicate processing)"

  -- Call heartbeat hook only for jobs that are still active (successfully heartbeated)
  -- Jobs that were acked/canceled (JobNotFound) are no longer being worked
  let activeJobIds = [jobId | Arb.VisibilityExtended jobId <- results]
      activeJobs = filter (\job -> primaryKey job `elem` activeJobIds) (toList jobs)
  currentTime <- liftIO getCurrentTime
  traverse_
    ( \job ->
        runHook logCfg "onJobHeartbeat" $
          onJobHeartbeat hooks job currentTime startTime
    )
    activeJobs

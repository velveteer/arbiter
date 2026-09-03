{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Heartbeat
  ( withJobsHeartbeat
  ) where

import Arbiter.Core.Exceptions (JobForceCancelled (..), displayEx, throwJobDeadline, throwJobGoneIds)
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (JobRead, ObservabilityHooks (..), primaryKey)
import Arbiter.Core.Trace (capturingContext)
import Control.Exception (throwIO)
import Control.Monad (forever, unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Set qualified as Set
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Data.Void (Void, absurd)
import GHC.Clock (getMonotonicTime)
import UnliftIO.Async (race)
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.Exception (tryAny)
import UnliftIO.STM (TMVar, TVar, atomically)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Logger (LogConfig, LogLevel (..), tryLog)
import Arbiter.Worker.Logger.Internal (runHook, withJobContext, withJobContextOne)
import Arbiter.Worker.Retry (isJobSignal)
import Arbiter.Worker.Settle (hasIdIn)

-- | Shortest gap between failed extends. Bounds how many fit, not whether they land.
minRetryPause :: Double
minRetryPause = 0.25

-- | Wait before the next extend: a beat at most, 'minRetryPause' at least.
heartbeatWait :: NominalDiffTime -> Bool -> Double -> Double
heartbeatWait intervalSecs extended remaining
  | extended, remaining > beat = beat
  | otherwise = min beat (max minRetryPause (remaining / 2))
  where
    beat = realToFrac intervalSecs

-- | Run an action with a heartbeat thread that extends job visibility at each
-- interval. Skip jobs that the handler finalized. An absent job is a normal
-- condition. A reclaimed job causes an exception because all jobs in a batch
-- share one deadline. Call the heartbeat hook after each successful extension.
withJobsHeartbeat
  :: forall payload m a
   . (JobOperation m payload)
  => ObservabilityHooks m payload
  -- ^ Observability hooks (for heartbeat hook)
  -> NominalDiffTime
  -- ^ Heartbeat interval
  -> NominalDiffTime
  -- ^ Visibility timeout
  -> Maybe NominalDiffTime
  -- ^ Longest the handler may run before the fence interrupts it
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
withJobsHeartbeat hooks intervalSecs timeoutSecs maxDuration startTime jobs pending logCfg signal action = do
  -- 'race' forks each side, so the handler and both guards need the job span reattached.
  inherited <- capturingContext
  wallNow <- liftIO getCurrentTime
  monoNow <- liftIO getMonotonicTime
  let elapsed = diffUTCTime wallNow startTime
      durationDeadline = (\limit -> monoNow + realToFrac limit) <$> maxDuration
  lease <- STM.newTVarIO (monoNow + realToFrac (timeoutSecs - elapsed))
  -- Renewing a claim the fence gave up would re-hide the row for another timeout.
  abandoned <- STM.newTVarIO False
  -- The heartbeat sits outside the fence, so a fenced handler keeps its lease while it unwinds.
  outcome <-
    race
      (inherited (absurd <$> heartbeatThread lease abandoned))
      (race (inherited (absurd <$> fenceThread lease durationDeadline abandoned)) (inherited action))
  pure (either id (either id id) outcome)
  where
    heartbeatThread lease abandoned = beat True
      where
        beat extended = do
          leaseUntil <- STM.readTVarIO lease
          now <- liftIO getMonotonicTime
          threadDelay (ceiling (heartbeatWait intervalSecs extended (leaseUntil - now) * 1_000_000))
          -- Read after the wait, so a fence that gave up during it stops the next extend.
          givenUp <- STM.readTVarIO abandoned
          if givenUp then idle >> beat extended else attempt
        idle = threadDelay (ceiling (intervalSecs * 1_000_000))
        attempt = do
          outcome <- tryAny (tick lease)
          case outcome of
            Right () -> beat True
            Left e
              | isJobSignal e -> liftIO (throwIO e)
              | otherwise -> do
                  tryLog (withJobContext logCfg jobs) Error ("Heartbeat error (retrying): " <> displayEx e)
                  beat False

    tick lease = do
      live <- pending
      -- Read before the extend, so the tracked deadline never outlasts the row's.
      issuedAt <- liftIO getMonotonicTime
      results <- Arb.setVisibilityTimeoutBatch timeoutSecs live
      -- A row this worker settled while the statement ran says nothing about its lease.
      stillPending <- Set.fromList . map primaryKey <$> pending
      let cancelledJobs = [jobId | Arb.JobCancelled jobId <- results]
          stolenJobs = [jobId | Arb.JobReclaimed jobId _ _ <- results]
          goneJobs = [jobId | Arb.JobGone jobId <- results]
          unmoved = [() | Arb.VisibilityUnchanged jobId <- results, Set.member jobId stillPending]
      atomically $ do
        when (null unmoved) $ STM.writeTVar lease (issuedAt + realToFrac timeoutSecs)
        void $ STM.tryPutTMVar signal ()
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

    fenceThread :: TVar Double -> Maybe Double -> TVar Bool -> m Void
    fenceThread lease durationDeadline abandoned = forever $ do
      leaseUntil <- STM.readTVarIO lease
      now <- liftIO getMonotonicTime
      let due = maybe leaseUntil (min leaseUntil) durationDeadline
      if now < due
        then threadDelay (ceiling ((due - now) * 1_000_000))
        else pending >>= interrupt durationDeadline abandoned leaseUntil now

    interrupt durationDeadline abandoned leaseUntil now live
      | not (null live)
      , now >= leaseUntil = do
          atomically (STM.writeTVar abandoned True)
          throwJobGoneIds "lease expired without renewal" (map primaryKey live)
      | maybe False (now >=) durationDeadline = throwJobDeadline durationMessage
      | otherwise = threadDelay (ceiling (maybe poll (min poll . subtract now) durationDeadline * 1_000_000))
      where
        poll = realToFrac intervalSecs

    durationMessage =
      "handler ran past the maximum job duration"
        <> foldMap ((" of " <>) . T.pack . show) maxDuration

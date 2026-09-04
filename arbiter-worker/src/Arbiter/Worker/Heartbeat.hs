{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Heartbeat
  ( withJobsHeartbeat
  ) where

import Arbiter.Core.Exceptions (JobDeadlineExceeded (..), JobForceCancelled (..), displayEx, throwJobGoneIds)
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (JobRead, ObservabilityHooks (..), primaryKey)
import Arbiter.Core.Trace (capturingContext)
import Control.Concurrent (ThreadId, myThreadId)
import Control.Exception (Exception (..), asyncExceptionFromException, asyncExceptionToException, throwIO, throwTo)
import Control.Monad (unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Set qualified as Set
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Data.Void (Void, absurd)
import GHC.Clock (getMonotonicTime)
import UnliftIO.Async (race)
import UnliftIO.Concurrent (forkIO, threadDelay)
import UnliftIO.Exception (catchSyncOrAsync, tryAny)
import UnliftIO.STM (TMVar, TVar, atomically)
import UnliftIO.STM qualified as STM
import UnliftIO.Timeout (timeout)

import Arbiter.Worker.Logger (LogConfig, LogLevel (..), tryLog)
import Arbiter.Worker.Logger.Internal (runHook, withJobContext, withJobContextOne)
import Arbiter.Worker.Retry (isJobSignal)
import Arbiter.Worker.Settle (hasIdIn)

-- | Thrown into the handler thread at the duration deadline. Async, so sync catches cannot swallow it.
newtype DeadlineSignal = DeadlineSignal T.Text
  deriving stock (Show)

instance Exception DeadlineSignal where
  backtraceDesired _ = False
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

-- | Shortest gap between failed extends.
minRetryPause :: Double
minRetryPause = 0.25

-- | Wait before the next extend. At most a beat and at least 'minRetryPause'.
heartbeatWait :: NominalDiffTime -> Bool -> Double -> Double
heartbeatWait intervalSecs extended remaining
  | extended, remaining > beat = beat
  | otherwise = min beat (max minRetryPause (remaining / 2))
  where
    beat = realToFrac intervalSecs

-- | Run an action with a heartbeat thread that extends job visibility at each
-- interval. Skip jobs that the handler finalized. An absent job is a normal
-- condition. A reclaimed job causes an exception. Call the heartbeat hook after
-- each successful extension.
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
  -- 'race' forks each side. The handler and the guard reattach the job span.
  inherited <- capturingContext
  wallNow <- liftIO getCurrentTime
  monoNow <- liftIO getMonotonicTime
  let elapsed = diffUTCTime wallNow startTime
      durationDeadline = (\limit -> monoNow + realToFrac limit) <$> maxDuration
  lease <- STM.newTVarIO (monoNow + realToFrac (timeoutSecs - elapsed))
  handlerId <- STM.newTVarIO Nothing
  outcome <-
    race
      (inherited (absurd <$> guardThread lease durationDeadline handlerId))
      (inherited (liftIO myThreadId >>= atomically . STM.writeTVar handlerId . Just >> deadlineAsSync action))
  pure (either id id outcome)
  where
    -- The signal leaves the handler thread as the sync exception settlement classifies.
    deadlineAsSync = flip catchSyncOrAsync (\(DeadlineSignal msg) -> liftIO (throwIO (JobDeadlineExceeded msg)))

    -- Sleeps until the earliest of the next beat, the lease end and the duration deadline.
    guardThread :: TVar Double -> Maybe Double -> TVar (Maybe ThreadId) -> m Void
    guardThread lease durationDeadline handlerId = loop True False
      where
        loop extended fenced = do
          leaseUntil <- STM.readTVarIO lease
          now <- liftIO getMonotonicTime
          let beatAt = now + heartbeatWait intervalSecs extended (leaseUntil - now)
              fenceAt = if fenced then Nothing else durationDeadline
              due = minimum (beatAt : leaseUntil : maybe [] pure fenceAt)
          threadDelay (max 0 (ceiling ((due - now) * 1_000_000)))
          woke <- liftIO getMonotonicTime
          leaseNow <- STM.readTVarIO lease
          live <- if woke >= leaseNow then pending else pure []
          if woke >= leaseNow && not (null live)
            then throwJobGoneIds "lease expired without renewal" (map primaryKey live)
            else case fenceAt of
              Just deadline | woke >= deadline -> do
                -- Delivered off-thread so a masked handler cannot stall the beat. The handler unwinds under a live lease.
                STM.readTVarIO handlerId >>= traverse_ (\tid -> void (forkIO (liftIO (throwTo tid (DeadlineSignal durationMessage)))))
                loop extended True
              _
                | woke >= beatAt -> attempt fenced
                | otherwise -> loop extended fenced
        -- An extend that hangs past the lease must not hold up the lease check.
        attempt fenced = do
          leaseUntil <- STM.readTVarIO lease
          now <- liftIO getMonotonicTime
          outcome <- timeout (ceiling (max minRetryPause (leaseUntil - now) * 1_000_000)) (tryAny (tick lease))
          case outcome of
            Nothing -> loop False fenced
            Just (Right ()) -> loop True fenced
            Just (Left exception)
              | isJobSignal exception -> liftIO (throwIO exception)
              | otherwise -> do
                  tryLog (withJobContext logCfg jobs) Error ("Heartbeat error (retrying): " <> displayEx exception)
                  loop False fenced

    tick lease = do
      live <- pending
      -- Read before the extend. The tracked deadline stays inside the row's.
      issuedAt <- liftIO getMonotonicTime
      results <- Arb.setVisibilityTimeoutBatch timeoutSecs live
      -- Rows this worker settled during the statement do not count.
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

    durationMessage =
      "handler ran past the maximum job duration"
        <> foldMap ((" of " <>) . T.pack . show) maxDuration

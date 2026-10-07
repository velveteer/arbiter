{-# LANGUAGE OverloadedStrings #-}

-- | Guard loop for lease fencing, deadlines, and heartbeat extension.
-- Extensions run on separate threads to keep fencing responsive to stalled
-- statements. Registration wakes the loop for an earlier deadline.
--
-- The lease fence waits for an extend that carries the batch. It waits until
-- that extend gives up. If the extend lands, it waits a settle grace past
-- that. The fence and the settle each take the batch's status in one
-- transaction. Whichever commits first wins and the other sees it.
module Arbiter.Worker.Heartbeat.Guard.Loop
  ( runHeartbeatGuard
  , trySync
  ) where

import Arbiter.Core.Exceptions (JobDeadlineExceeded (..), JobForceCancelled (..), JobGoneException (..), displayEx)
import Arbiter.Core.HighLevel (SetVisibilityResult (..))
import Arbiter.Core.Job.Types (JobId)
import Control.Concurrent.Class.MonadSTM
  ( MonadSTM
  , STM
  , atomically
  , check
  , modifyTVar'
  , orElse
  , readTVar
  , retry
  , stateTVar
  , writeTVar
  )
import Control.Monad (filterM, forever, unless, void, when, (>=>))
import Control.Monad.Class.MonadFork (MonadFork (..))
import Control.Monad.Class.MonadThrow (Exception (..), MonadCatch (..), MonadMask (..), MonadThrow (..), SomeException)
import Control.Monad.Class.MonadTime.SI
  ( DiffTime
  , MonadMonotonicTime (..)
  , MonadTime (..)
  , Time
  , UTCTime
  , addTime
  , diffTime
  )
import Control.Monad.Class.MonadTimer.SI (MonadTimer (..))
import Data.Foldable (for_, toList, traverse_)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust, isNothing, mapMaybe)
import Data.Set qualified as Set
import Data.Text qualified as T
import Data.Void (Void)
import UnliftIO.Exception (isSyncException)

import Arbiter.Worker.Heartbeat.Guard.Signal (signal)
import Arbiter.Worker.Heartbeat.Guard.State
import Arbiter.Worker.Logger (LogLevel (..))

-- | The guard loop. Fences what is due, then extends what is due.
runHeartbeatGuard
  :: (MonadFork n, MonadMask n, MonadTime n, MonadTimer n)
  => HeartbeatGuard n job
  -> n Void
{-# SPECIALIZE runHeartbeatGuard :: HeartbeatGuard IO job -> IO Void #-}
runHeartbeatGuard guard = forever $ do
  (target, count) <- atomically (plan guard)
  sleepUntil guard count target
  woke <- getMonotonicTime
  (inFlight, current) <- atomically $ do
    abandon guard woke
    (,) <$> readTVar (guardInFlight guard) <*> snapshot guard
  -- A batch the fence stops gets no beat.
  leased <- filterM (fence guard woke) current
  let due = [entry | (entry, status) <- leased, beatAt status <= woke]
  unless (null due || isJust inFlight) (issue guard woke leased due)

-- ---------------------------------------------------------------------------
-- Sleeping
-- ---------------------------------------------------------------------------

-- | Publish the loop's target, the earliest due time.
plan :: (MonadSTM n) => HeartbeatGuard n job -> STM n (Maybe Time, Int)
plan guard = do
  inFlight <- readTVar (guardInFlight guard)
  abandoned <- readTVar (guardAbandoned guard)
  entries <- snapshot guard
  let times = toList (abandonAt abandoned inFlight) <> concatMap (dueTimes inFlight) entries
  let target = if null times then Nothing else Just (minimum times)
  writeTVar (wakeTarget (guardWake guard)) target
  (,) target <$> readTVar (wakeCount (guardWake guard))

-- | When the guard next acts on a batch. Beats wait for the extend in flight.
dueTimes :: Maybe InFlight -> (Guarded n job, Status n) -> [Time]
dueTimes inFlight (entry, status) =
  [leaseFenceAt inFlight entry status | not (leaseLapsed status)]
    <> [beatAt status | not (leaseLapsed status), isNothing inFlight]
    <> [deadline | not (deadlineSent status), Just deadline <- [guardedDeadline entry]]

-- | When the lease fence fires. An extend carrying the batch holds it until the extend
-- gives up, or, once landed, a settle grace past that.
leaseFenceAt :: Maybe InFlight -> Guarded n job -> Status n -> Time
leaseFenceAt inFlight entry status = case inFlight of
  Just running
    | carried running, landed running -> max lease (addTime settleGrace (givesUp running))
    | carried running -> max lease (givesUp running)
  _ -> lease
  where
    lease = leaseAt status
    carried running = Set.member (guardedToken entry) (carries running) && issuedAt running < lease

-- | Sleep until @at@ or the next wake.
sleepUntil :: (MonadTimer n) => HeartbeatGuard n job -> Int -> Maybe Time -> n ()
sleepUntil guard count at = do
  alarm <- traverse arm at
  atomically $
    (readTVar (wakeCount (guardWake guard)) >>= check . (/= count))
      `orElse` maybe retry (readTVar >=> check) alarm
  where
    arm target = do
      now <- getMonotonicTime
      registerDelay (max 0 (target `diffTime` now))

-- ---------------------------------------------------------------------------
-- Fencing
-- ---------------------------------------------------------------------------

-- | Signal a handler past its lease or its deadline. Whether its lease still stands.
fence :: (MonadFork n, MonadSTM n) => HeartbeatGuard n job -> Time -> (Guarded n job, Status n) -> n Bool
fence guard woke (entry, status) = do
  (lapsed, standing) <- atomically (lapse guard woke entry)
  when lapsed $ do
    live <- pendingOf entry
    unless (null live) $
      signal guard woke entry (toException (JobGoneException leaseExpiredReason (map (guardKey guard) live)))
  for_ (guardedDeadline entry) $ \deadline ->
    when (not (deadlineSent status) && woke >= deadline) $ do
      adjust entry (\current -> current {deadlineSent = True})
      signal guard woke entry (toException (JobDeadlineExceeded (durationMessage guard)))
  pure standing

-- | Mark the lease lapsed if it is, against the extend in flight. Whether it lapsed now, and whether it stands.
lapse :: (MonadSTM n) => HeartbeatGuard n job -> Time -> Guarded n job -> STM n (Bool, Bool)
lapse guard woke entry = do
  inFlight <- readTVar (guardInFlight guard)
  status <- readTVar (guardedStatus entry)
  let lapsed = not (leaseLapsed status) && woke >= leaseFenceAt inFlight entry status
  when lapsed (writeTVar (guardedStatus entry) status {leaseLapsed = True})
  pure (lapsed, not (leaseLapsed status || lapsed))

durationMessage :: HeartbeatGuard n job -> T.Text
durationMessage guard =
  "claim hooks and handler ran past the maximum job duration"
    <> foldMap ((" of " <>) . T.pack . show) (configMaxDuration (guardConfig guard))

-- ---------------------------------------------------------------------------
-- The extend in flight
-- ---------------------------------------------------------------------------

-- | Issue one extend over @due@, bounded by the earliest lease among @leased@.
issue
  :: (MonadFork n, MonadMask n, MonadTime n, MonadTimer n)
  => HeartbeatGuard n job
  -> Time
  -> [(Guarded n job, Status n)]
  -> [Guarded n job]
  -> n ()
issue guard woke leased due = do
  issued <- getMonotonicTime
  atomically $ do
    writeTVar
      (guardInFlight guard)
      (Just (InFlight issued (addTime bound issued) (Set.fromList (map guardedToken due)) False))
    traverse_ (\entry -> modifyTVar' (guardedStatus entry) (\status -> status {recheckAsked = False})) due
  void . forkIO $ (extend guard issued bound due `finally` finish guard issued) >>= traverse_ (report guard due)
  where
    bound =
      max
        minRetryPause
        (minimum (configTimeout (guardConfig guard) : [leaseAt status `diffTime` woke | (_, status) <- leased]))

-- | Abandon the extend in flight past its give-up and settle grace.
abandon :: (MonadSTM n) => HeartbeatGuard n job -> Time -> STM n ()
abandon guard woke = do
  inFlight <- readTVar (guardInFlight guard)
  abandoned <- readTVar (guardAbandoned guard)
  for_ ((,) <$> inFlight <*> abandonAt abandoned inFlight) $ \(running, at) ->
    when (woke >= at) $ do
      writeTVar (guardInFlight guard) Nothing
      writeTVar (guardAbandoned guard) (Just (issuedAt running))

-- | When the extend in flight is abandoned. Nothing while another abandoned extend runs.
abandonAt :: Maybe Time -> Maybe InFlight -> Maybe Time
abandonAt abandoned inFlight = case (abandoned, inFlight) of
  (Nothing, Just running) -> Just (addTime settleGrace (givesUp running))
  _ -> Nothing

-- | The statement returned. The fence holds its batches until it is over.
land :: (MonadSTM n) => HeartbeatGuard n job -> Time -> n Bool
land guard issued = atomically $ do
  current <- ownsAttempt guard issued
  when current $ modifyTVar' (guardInFlight guard) (fmap (\running -> running {landed = True}))
  pure current

ownsAttempt :: (MonadSTM n) => HeartbeatGuard n job -> Time -> STM n Bool
ownsAttempt guard issued = maybe False ((== issued) . issuedAt) <$> readTVar (guardInFlight guard)

-- | The extend is over. The fence is due at the leases again.
finish :: (MonadSTM n) => HeartbeatGuard n job -> Time -> n ()
finish guard issued = atomically $ do
  current <- ownsAttempt guard issued
  when current (writeTVar (guardInFlight guard) Nothing *> wake guard)
  abandoned <- readTVar (guardAbandoned guard)
  when (abandoned == Just issued) (writeTVar (guardAbandoned guard) Nothing *> wake guard)

-- | One extend statement over every due batch, bounded by @bound@.
extend
  :: (MonadCatch n, MonadFork n, MonadTime n, MonadTimer n)
  => HeartbeatGuard n job
  -> Time
  -> DiffTime
  -> [Guarded n job]
  -> n (Maybe SomeException)
extend guard issued bound due = do
  lives <- traverse (\entry -> (,) entry <$> pendingOf entry) due
  outcome <- timeout bound (trySync (configExtend config (concatMap snd lives)))
  case outcome of
    Nothing -> Nothing <$ traverse_ (retryLater guard issued) due
    Just (Left exception) -> Just exception <$ traverse_ (retryLater guard issued) due
    Just (Right results) -> do
      current <- land guard issued
      when current $ do
        configExtended config
        currentTime <- getCurrentTime
        let byJob = Map.fromList [(resultId result, result) | result <- results]
        traverse_ (settle guard issued currentTime byJob) lives
      pure Nothing
  where
    config = guardConfig guard

-- | Log a refused extend.
report :: (Applicative n) => HeartbeatGuard n job -> [Guarded n job] -> SomeException -> n ()
report guard due exception =
  for_ due $ \entry ->
    configLog
      (guardConfig guard)
      Error
      (toList (batchJobs (guardedBatch entry)))
      ("Heartbeat error (retrying): " <> displayEx exception)

-- | Beat again after a failed extend.
retryLater :: (MonadMonotonicTime n, MonadSTM n) => HeartbeatGuard n job -> Time -> Guarded n job -> n ()
retryLater guard issued entry = do
  now <- getMonotonicTime
  atomically $ do
    current <- ownsAttempt guard issued
    when current $ modifyTVar' (guardedStatus entry) $ \status ->
      rebeat (addTime (heartbeatWait (configInterval (guardConfig guard)) False (leaseAt status `diffTime` now)) now) status

-- | @try@ for synchronous exceptions only.
trySync :: (MonadCatch n) => n a -> n (Either SomeException a)
trySync = tryJust (\exc -> if isSyncException exc then Just exc else Nothing)

-- ---------------------------------------------------------------------------
-- Settling
-- ---------------------------------------------------------------------------

-- | Act on one batch's verdicts from the extend. A stopped batch takes none.
settle
  :: (MonadFork n, MonadMonotonicTime n, MonadSTM n)
  => HeartbeatGuard n job
  -> Time
  -> UTCTime
  -> Map JobId SetVisibilityResult
  -> (Guarded n job, [job])
  -> n ()
settle guard issued currentTime byJob (entry, live) = do
  -- Rows this worker settled during the statement do not count.
  stillPending <- Set.fromList . map key <$> pendingOf entry
  now <- getMonotonicTime
  let pendingLive = filter ((`Set.member` stillPending) . key) live
      verdictOf job = Map.lookup (key job) byJob
      verdicts = mapMaybe verdictOf pendingLive
      cancelledJobs = [jobId | JobCancelled jobId <- verdicts]
      stolenJobs = [jobId | JobReclaimed jobId _ _ <- verdicts]
      gone = Set.fromList [jobId | JobGone jobId <- verdicts]
      extended = filter (maybe False renewed . verdictOf) pendingLive
      leaseFrom status
        | all (maybe False renewed . verdictOf) pendingLive = addTime (configTimeout config) issued
        | otherwise = leaseAt status
      cadence lease = addTime (heartbeatWait (configInterval config) True (lease `diffTime` issued)) issued
      -- A first sighting can be an ack not yet recorded. The next extend confirms it.
      beatFrom status
        | gone `Set.isSubsetOf` goneSeen status = cadence (leaseFrom status)
        | otherwise = min (cadence (leaseFrom status)) (addTime minRetryPause now)
      renew status = rebeat (beatFrom status) status {leaseAt = leaseFrom status, goneSeen = gone}
      stopFor seen
        | not (null cancelledJobs) = Just (toException (JobForceCancelled cancelledJobs (stolenJobs <> Set.toList gone)))
        | not (null stolenJobs) = Just (toException (JobGoneException reclaimedReason stolenJobs))
        | not (Set.null deleted) = Just (toException (JobGoneException deletedReason (Set.toList deleted)))
        | otherwise = Nothing
        where
          deleted = Set.intersection gone seen
  -- What the last extend found gone. Nothing for a batch the fence stopped.
  seenBefore <- atomically $ do
    current <- ownsAttempt guard issued
    stateTVar (guardedStatus entry) $ \status ->
      if current && not (leaseLapsed status) then (Just (goneSeen status), renew status) else (Nothing, status)
  for_ seenBefore (maybe (heartbeats extended) (signal guard now entry) . stopFor)
  where
    heartbeats extended =
      unless (null extended) . void . forkIO . batchInherit batch $
        for_ extended $ \job -> do
          pending <- pendingOf entry
          when (any ((== key job) . key) pending) $
            configHeartbeat config job currentTime (batchStart batch)
    config = guardConfig guard
    key = configKey config
    batch = guardedBatch entry

-- | Only a successful extend renews the lease.
renewed :: SetVisibilityResult -> Bool
renewed VisibilityExtended {} = True
renewed _ = False

resultId :: SetVisibilityResult -> JobId
resultId result = case result of
  VisibilityExtended jobId -> jobId
  VisibilityUnchanged jobId -> jobId
  JobCancelled jobId -> jobId
  JobReclaimed jobId _ _ -> jobId
  JobGone jobId -> jobId
  JobSuspended jobId -> jobId

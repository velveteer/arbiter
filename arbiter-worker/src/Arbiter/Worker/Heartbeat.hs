{-# LANGUAGE OverloadedStrings #-}

-- | One guard per pool. It fences every batch in flight and extends their leases.
--
-- A batch registers a 'Guarded' entry and unregisters it when the handler
-- returns. The guard loop sleeps until the earliest lease, deadline or beat.
-- On waking it signals the handlers it finds past a lease or deadline, then
-- extends every batch due a beat in one statement, on a thread of its own so a
-- hung statement cannot stall the fence. A register wakes the loop only when
-- it precedes the published target.
--
-- A signal is thrown from a courier thread, so a masked handler cannot stall
-- the guard. Unregister kills the couriers, which revokes a signal still in
-- flight. A second signal within one beat is dropped.
--
-- The lease fence waits for an extend that was issued inside the lease, until
-- that extend gives up. Otherwise the fence could stop a batch whose row the
-- extend has just re-leased.
module Arbiter.Worker.Heartbeat
  ( HeartbeatGuard
  , newHeartbeatGuard
  , runHeartbeatGuard
  , withJobsHeartbeat
  ) where

import Arbiter.Core.Exceptions (JobDeadlineExceeded (..), JobForceCancelled (..), JobGoneException (..), displayEx)
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (JobId, JobRead, ObservabilityHooks (..), primaryKey)
import Arbiter.Core.Trace (capturingContext)
import Control.Concurrent (ThreadId, killThread, myThreadId)
import Control.Exception
  ( Exception (..)
  , SomeException
  , asyncExceptionFromException
  , asyncExceptionToException
  , throwIO
  , throwTo
  )
import Control.Monad (forever, unless, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Foldable (for_, traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, isJust, isNothing, maybeToList)
import Data.Set qualified as Set
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Data.Void (Void)
import GHC.Clock (getMonotonicTime)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Concurrent (forkIO)
import UnliftIO.Exception (bracket, catchSyncOrAsync, finally, tryAny)
import UnliftIO.STM (STM, TMVar, TVar, atomically)
import UnliftIO.STM qualified as STM
import UnliftIO.Timeout (timeout)

import Arbiter.Worker.Config (WorkerConfig (..), heartbeatSignal)
import Arbiter.Worker.Handoff (hasIdIn)
import Arbiter.Worker.Logger (LogConfig, LogLevel (..), tryLog)
import Arbiter.Worker.Logger.Internal (runHook, withJobContext, withJobContextOne)

-- | A guard signal, rethrown as sync at the handler boundary.
newtype GuardSignal = GuardSignal SomeException
  deriving stock (Show)

instance Exception GuardSignal where
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

-- | One batch under guard.
data Guarded m payload = Guarded
  { guardedJobs :: NonEmpty (JobRead payload)
  , guardedPending :: m [JobRead payload]
  -- ^ The batch's jobs still awaiting an outcome.
  , guardedHandler :: ThreadId
  , guardedStart :: UTCTime
  , guardedInherit :: m () -> m ()
  -- ^ Runs an action under the batch's job span.
  , guardedDeadline :: Maybe Double
  -- ^ Monotonic time the duration fence fires.
  , guardedTimers :: TVar Timers
  }

-- | A batch's timers and flags. Times are monotonic.
data Timers = Timers
  { leaseAt :: !Double
  -- ^ When the lease runs out.
  , beatAt :: !Double
  -- ^ When the next extend is due.
  , stopped :: !Bool
  -- ^ The lease lapsed. No further extends.
  , fenced :: !Bool
  -- ^ The deadline signal went out.
  , signalledAt :: !(Maybe Double)
  -- ^ When the last signal went out.
  , couriers :: !(Maybe [ThreadId])
  -- ^ Threads carrying a signal to the handler. Nothing once unregistered.
  }

-- | The extend statement in flight.
data Extending = Extending
  { issuedAt :: !Double
  , givesUp :: !Double
  }

-- | The pool's heartbeat guard.
data HeartbeatGuard m payload = HeartbeatGuard
  { guardHooks :: ObservabilityHooks m payload
  , guardInterval :: NominalDiffTime
  , guardTimeout :: NominalDiffTime
  , guardMaxDuration :: Maybe NominalDiffTime
  , guardLog :: LogConfig
  , guardSignal :: TMVar ()
  -- ^ Proof-of-work signal pulsed after each successful extend.
  , guardEntries :: TVar (Map Int (Guarded m payload))
  , guardNextToken :: TVar Int
  , guardExtending :: TVar (Maybe Extending)
  , guardWake :: Wake
  }

-- | How the guard loop is woken before its target.
data Wake = Wake
  { wakeCount :: TVar Int
  -- ^ Bumped by each wake.
  , wakeTarget :: TVar (Maybe Double)
  -- ^ The monotonic time the loop sleeps until.
  }

newHeartbeatGuard :: (MonadIO m) => WorkerConfig m payload -> m (HeartbeatGuard m payload)
newHeartbeatGuard config =
  HeartbeatGuard
    (observabilityHooks config)
    (jobHeartbeatInterval config)
    (visibilityTimeout config)
    (maxJobDuration config)
    (logConfig config)
    (heartbeatSignal config)
    <$> STM.newTVarIO Map.empty
    <*> STM.newTVarIO 0
    <*> STM.newTVarIO Nothing
    <*> (Wake <$> STM.newTVarIO 0 <*> STM.newTVarIO Nothing)

-- | Run @action@ on the calling thread with the batch registered with the guard.
withJobsHeartbeat
  :: forall payload m a
   . (JobOperation m payload)
  => HeartbeatGuard m payload
  -> UTCTime
  -- ^ Start time (for calculating elapsed time in heartbeat hook)
  -> NonEmpty (JobRead payload)
  -- ^ The job(s) being processed
  -> m [JobRead payload]
  -- ^ Read each tick, on the guard's threads, for the job(s) still awaiting an outcome.
  -> m a
  -- ^ Action to run with heartbeat protection
  -> m a
withJobsHeartbeat guard startTime jobs pending action =
  asSync (bracket register unregister (const action))
  where
    register = do
      inherit <- capturingContext
      wallNow <- liftIO getCurrentTime
      monoNow <- liftIO getMonotonicTime
      handler <- liftIO myThreadId
      let elapsed = diffUTCTime wallNow startTime
          leaseUntil = monoNow + realToFrac (guardTimeout guard - elapsed)
          firstBeat = monoNow + heartbeatWait (guardInterval guard) True (leaseUntil - monoNow)
          deadline = (\limit -> monoNow + realToFrac limit) <$> guardMaxDuration guard
      entry <-
        Guarded jobs pending handler startTime inherit deadline
          <$> STM.newTVarIO (Timers leaseUntil firstBeat False False Nothing (Just []))
      atomically $ do
        token <- STM.stateTVar (guardNextToken guard) (\next -> (next, next + 1))
        STM.modifyTVar' (guardEntries guard) (Map.insert token entry)
        wakeFor guard (minimum (leaseUntil : firstBeat : maybeToList deadline))
        pure (token, entry)

    asSync act = act `catchSyncOrAsync` (\(GuardSignal exc) -> liftIO (throwIO exc))

    unregister (token, entry) = do
      carrying <- atomically $ do
        STM.modifyTVar' (guardEntries guard) (Map.delete token)
        STM.stateTVar (guardedTimers entry) (\timers -> (fromMaybe [] (couriers timers), timers {couriers = Nothing}))
      liftIO (traverse_ killThread carrying)

-- | Every registered batch with its timers.
snapshot :: HeartbeatGuard m payload -> STM [(Guarded m payload, Timers)]
snapshot guard = do
  entries <- STM.readTVar (guardEntries guard)
  traverse (\entry -> (,) entry <$> STM.readTVar (guardedTimers entry)) (Map.elems entries)

adjust :: (MonadIO m) => Guarded n payload -> (Timers -> Timers) -> m ()
adjust entry = atomically . STM.modifyTVar' (guardedTimers entry)

-- | When the lease fence fires. An extend issued inside the lease holds it until the extend gives up.
leaseFence :: Maybe Extending -> Double -> Double
leaseFence extending lease = case extending of
  Just running | issuedAt running < lease -> max lease (givesUp running)
  _ -> lease

-- | When the guard next acts on a batch. Beats wait for the extend in flight.
dueTimes :: Maybe Extending -> (Guarded m payload, Timers) -> [Double]
dueTimes extending (entry, timers) =
  [leaseFence extending (leaseAt timers) | not (stopped timers)]
    <> [beatAt timers | not (stopped timers), isNothing extending]
    <> [deadline | not (fenced timers), Just deadline <- [guardedDeadline entry]]

-- | Publish the loop's target, the earliest due time.
plan :: HeartbeatGuard m payload -> STM (Maybe Double, Int)
plan guard = do
  times <- concatMap <$> (dueTimes <$> STM.readTVar (guardExtending guard)) <*> snapshot guard
  let target = if null times then Nothing else Just (minimum times)
  STM.writeTVar (wakeTarget (guardWake guard)) target
  (,) target <$> STM.readTVar (wakeCount (guardWake guard))

-- | Wake the loop when @at@ precedes its target.
wakeFor :: HeartbeatGuard m payload -> Double -> STM ()
wakeFor guard at = do
  target <- STM.readTVar (wakeTarget (guardWake guard))
  when (maybe True (at <) target) (wake guard)

wake :: HeartbeatGuard m payload -> STM ()
wake guard = STM.modifyTVar' (wakeCount (guardWake guard)) (+ 1)

-- | Sleep until @at@ or the next wake.
sleepUntil :: (MonadIO m) => HeartbeatGuard n payload -> Int -> Maybe Double -> m ()
sleepUntil guard count at = do
  alarm <- traverse arm at
  atomically $
    (STM.readTVar (wakeCount (guardWake guard)) >>= STM.checkSTM . (/= count))
      `STM.orElse` maybe STM.retrySTM (\fired -> STM.readTVar fired >>= STM.checkSTM) alarm
  where
    arm target = do
      now <- liftIO getMonotonicTime
      STM.registerDelay (max 0 (ceiling ((target - now) * 1_000_000)))

-- | The guard loop. Fences what is due, then extends what is due.
runHeartbeatGuard :: forall payload m. (JobOperation m payload) => HeartbeatGuard m payload -> m Void
runHeartbeatGuard guard = forever $ do
  (target, count) <- atomically (plan guard)
  sleepUntil guard count target
  woke <- liftIO getMonotonicTime
  (extending, current) <- atomically ((,) <$> STM.readTVar (guardExtending guard) <*> snapshot guard)
  for_ current (fence guard woke extending)
  let due = [entry | (entry, timers) <- current, not (stopped timers), beatAt timers <= woke]
  unless (null due || isJust extending) $ do
    let bound =
          max
            minRetryPause
            (minimum (realToFrac (guardTimeout guard) : [leaseAt timers - woke | (_, timers) <- current, not (stopped timers)]))
    issued <- liftIO getMonotonicTime
    atomically (STM.writeTVar (guardExtending guard) (Just (Extending issued (issued + bound))))
    void . forkIO $
      extend guard issued bound due
        `finally` atomically (STM.writeTVar (guardExtending guard) Nothing >> wake guard)

-- | Signal a handler past its lease or its deadline.
fence
  :: (MonadUnliftIO m) => HeartbeatGuard m payload -> Double -> Maybe Extending -> (Guarded m payload, Timers) -> m ()
fence guard woke extending (entry, timers) = do
  when (not (stopped timers) && woke >= leaseFence extending (leaseAt timers)) $ do
    live <- guardedPending entry
    adjust entry (\t -> t {stopped = True})
    unless (null live) $
      signal guard woke entry (toException (JobGoneException "lease expired without renewal" (map primaryKey live)))
  for_ (guardedDeadline entry) $ \deadline ->
    when (not (fenced timers) && woke >= deadline) $ do
      adjust entry (\t -> t {fenced = True})
      signal guard woke entry (toException (JobDeadlineExceeded (durationMessage guard)))

-- | Ask the handler to stop, from a courier thread. A second ask within one beat is dropped.
signal :: (MonadUnliftIO m) => HeartbeatGuard m payload -> Double -> Guarded m payload -> SomeException -> m ()
signal guard now entry exc = do
  fresh <- atomically $ STM.stateTVar (guardedTimers entry) $ \timers ->
    let due = maybe True (\at -> now >= at + realToFrac (guardInterval guard)) (signalledAt timers)
     in (due, if due then timers {signalledAt = Just now} else timers)
  when fresh . void . forkIO . liftIO $ do
    courier <- myThreadId
    claimed <- atomically $ STM.stateTVar (guardedTimers entry) $ \timers -> case couriers timers of
      Nothing -> (False, timers)
      Just carrying -> (True, timers {couriers = Just (courier : carrying)})
    when claimed (throwTo (guardedHandler entry) (GuardSignal exc))

-- | One extend statement over every due batch, bounded by @bound@ seconds.
extend :: (JobOperation m payload) => HeartbeatGuard m payload -> Double -> Double -> [Guarded m payload] -> m ()
extend guard issued bound due = do
  lives <- traverse (\entry -> (,) entry <$> guardedPending entry) due
  outcome <-
    timeout
      (ceiling (bound * 1_000_000))
      (tryAny (Arb.setVisibilityTimeoutBatch (guardTimeout guard) (concatMap snd lives)))
  case outcome of
    Nothing -> traverse_ retryLater due
    Just (Left exception) -> do
      for_ due $ \entry ->
        tryLog
          (withJobContext (guardLog guard) (guardedJobs entry))
          Error
          ("Heartbeat error (retrying): " <> displayEx exception)
      traverse_ retryLater due
    Just (Right results) -> do
      atomically (void (STM.tryPutTMVar (guardSignal guard) ()))
      currentTime <- liftIO getCurrentTime
      let byJob = Map.fromListWith (flip (<>)) [(resultId result, [result]) | result <- results]
      traverse_ (settle guard issued currentTime byJob) lives
  where
    retryLater entry = do
      now <- liftIO getMonotonicTime
      adjust entry (\timers -> timers {beatAt = now + heartbeatWait (guardInterval guard) False (leaseAt timers - now)})

-- | Act on one batch's verdicts from the extend.
settle
  :: (JobOperation m payload)
  => HeartbeatGuard m payload
  -> Double
  -> UTCTime
  -> Map JobId [Arb.SetVisibilityResult]
  -> (Guarded m payload, [JobRead payload])
  -> m ()
settle guard issued currentTime byJob (entry, live) = do
  let mine = concatMap (\job -> Map.findWithDefault [] (primaryKey job) byJob) live
  -- Rows this worker settled during the statement do not count.
  stillPending <- Set.fromList . map primaryKey <$> guardedPending entry
  let cancelledJobs = [jobId | Arb.JobCancelled jobId <- mine]
      stolenJobs = [jobId | Arb.JobReclaimed jobId _ _ <- mine]
      goneJobs = [jobId | Arb.JobGone jobId <- mine]
      unmoved = [() | Arb.VisibilityUnchanged jobId <- mine, Set.member jobId stillPending]
      extendedIds = Set.fromList [jobId | Arb.VisibilityExtended jobId <- mine]
      extended = filter (hasIdIn extendedIds) live
  adjust entry $ \timers ->
    let lease = if null unmoved then issued + realToFrac (guardTimeout guard) else leaseAt timers
     in timers {leaseAt = lease, beatAt = issued + heartbeatWait (guardInterval guard) True (lease - issued)}
  now <- liftIO getMonotonicTime
  case (cancelledJobs, stolenJobs) of
    (_ : _, _) -> signal guard now entry (toException (JobForceCancelled cancelledJobs (stolenJobs <> goneJobs)))
    ([], _ : _) -> signal guard now entry (toException (JobGoneException "reclaimed by another worker" stolenJobs))
    ([], []) ->
      unless (null extended) $
        void $
          forkIO $
            guardedInherit entry $
              for_ extended $ \job ->
                runHook (withJobContextOne (guardLog guard) job) "onJobHeartbeat" $
                  onJobHeartbeat (guardHooks guard) job currentTime (guardedStart entry)

resultId :: Arb.SetVisibilityResult -> JobId
resultId result = case result of
  Arb.VisibilityExtended jobId -> jobId
  Arb.VisibilityUnchanged jobId -> jobId
  Arb.JobCancelled jobId -> jobId
  Arb.JobReclaimed jobId _ _ -> jobId
  Arb.JobGone jobId -> jobId
  Arb.JobSuspended jobId -> jobId

durationMessage :: HeartbeatGuard m payload -> T.Text
durationMessage guard =
  "handler ran past the maximum job duration"
    <> foldMap ((" of " <>) . T.pack . show) (guardMaxDuration guard)

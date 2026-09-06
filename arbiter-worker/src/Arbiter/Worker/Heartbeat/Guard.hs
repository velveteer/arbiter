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
-- The lease fence waits for an extend that carries the batch, until that extend
-- gives up. Otherwise the fence could stop a batch whose row the extend has just
-- re-leased.
--
-- Written against io-classes, so the pool runs it in IO and the tests run it
-- under io-sim.
module Arbiter.Worker.Heartbeat.Guard
  ( HeartbeatGuard
  , GuardConfig (..)
  , newHeartbeatGuard
  , runHeartbeatGuard
  , guardKey
  , Batch (..)
  , guardBatch
  , trySync
  , toDiffTime
  , minRetryPause
  , leaseExpiredReason
  , reclaimedReason
  ) where

import Arbiter.Core.Exceptions (JobDeadlineExceeded (..), JobForceCancelled (..), JobGoneException (..), displayEx)
import Arbiter.Core.HighLevel (SetVisibilityResult (..))
import Arbiter.Core.Job.Types (JobId)
import Control.Concurrent.Class.MonadSTM
  ( MonadSTM
  , STM
  , TVar
  , atomically
  , check
  , modifyTVar'
  , newTVarIO
  , orElse
  , readTVar
  , readTVarIO
  , retry
  , stateTVar
  , writeTVar
  )
import Control.Exception (asyncExceptionFromException, asyncExceptionToException)
import Control.Monad (filterM, forever, unless, void, when, (>=>))
import Control.Monad.Class.MonadFork (MonadFork (..), MonadThread (..))
import Control.Monad.Class.MonadThrow (Exception (..), MonadCatch (..), MonadMask (..), MonadThrow (..), SomeException)
import Control.Monad.Class.MonadTime.SI
  ( DiffTime
  , MonadMonotonicTime (..)
  , MonadTime (..)
  , Time
  , UTCTime
  , addTime
  , diffTime
  , diffUTCTime
  )
import Control.Monad.Class.MonadTimer.SI (MonadTimer (..))
import Data.Fixed (Fixed (..))
import Data.Foldable (for_, toList, traverse_)
import Data.List.NonEmpty (NonEmpty)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, isJust, isNothing, mapMaybe, maybeToList)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock (NominalDiffTime, nominalDiffTimeToSeconds, picosecondsToDiffTime)
import Data.Void (Void)
import UnliftIO.Exception (isSyncException)

import Arbiter.Worker.Logger (LogLevel (..))

-- | A guard signal, rethrown as sync at the handler boundary.
newtype GuardSignal = GuardSignal SomeException
  deriving stock (Show)

instance Exception GuardSignal where
  backtraceDesired _ = False
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

-- | Shortest gap between failed extends.
minRetryPause :: DiffTime
minRetryPause = 0.25

-- | The reason a batch is stopped at its lease.
leaseExpiredReason :: Text
leaseExpiredReason = "lease expired without renewal"

-- | The reason a batch another worker reclaimed is stopped.
reclaimedReason :: Text
reclaimedReason = "reclaimed by another worker"

-- | A wall-clock span as a monotonic one. Exact, no Rational detour.
toDiffTime :: NominalDiffTime -> DiffTime
toDiffTime elapsed = picosecondsToDiffTime picos
  where
    MkFixed picos = nominalDiffTimeToSeconds elapsed

-- | Wait before the next extend. At most a beat and at least 'minRetryPause'.
heartbeatWait :: DiffTime -> Bool -> DiffTime -> DiffTime
heartbeatWait beat extended remaining
  | extended, remaining > beat = beat
  | otherwise = min beat (max minRetryPause (remaining / 2))

-- | What the guard needs from the pool.
data GuardConfig n job = GuardConfig
  { configInterval :: DiffTime
  , configTimeout :: DiffTime
  , configMaxDuration :: Maybe DiffTime
  , configKey :: job -> JobId
  , configExtend :: [job] -> n [SetVisibilityResult]
  -- ^ Extend the jobs' leases by 'configTimeout', reporting each.
  , configExtended :: n ()
  -- ^ Runs after each extend that reached the database.
  , configLog :: LogLevel -> [job] -> Text -> n ()
  -- ^ The pool log, with the jobs in context.
  , configHeartbeat :: job -> UTCTime -> UTCTime -> n ()
  -- ^ The heartbeat hook: the job, now, and the batch start.
  }

-- | A batch under guard.
data Batch n job = Batch
  { batchJobs :: NonEmpty job
  , batchPending :: n [job]
  -- ^ Read on the guard's threads, for the jobs still awaiting an outcome.
  , batchStart :: UTCTime
  , batchInherit :: n () -> n ()
  -- ^ Runs the heartbeat hooks under the batch's context.
  }

-- | A registered batch.
data Guarded n job = Guarded
  { guardedToken :: Int
  , guardedBatch :: Batch n job
  , guardedHandler :: ThreadId n
  , guardedDeadline :: Maybe Time
  -- ^ When the duration fence fires.
  , guardedTimers :: TVar n (Timers n)
  }

-- | A batch's timers and flags.
data Timers n = Timers
  { leaseAt :: !Time
  -- ^ When the lease runs out.
  , beatAt :: !Time
  -- ^ When the next extend is due.
  , stopped :: !Bool
  -- ^ The lease lapsed. No further extends.
  , fenced :: !Bool
  -- ^ The deadline signal went out.
  , signalledAt :: !(Maybe Time)
  -- ^ When the last signal went out.
  , couriers :: !(Maybe [ThreadId n])
  -- ^ Threads carrying a signal to the handler. Nothing once unregistered.
  }

-- | The extend statement in flight.
data Extending = Extending
  { issuedAt :: !Time
  , givesUp :: !Time
  , carries :: !(Set Int)
  -- ^ The batches the statement covers.
  }

-- | The pool's heartbeat guard.
data HeartbeatGuard n job = HeartbeatGuard
  { guardConfig :: GuardConfig n job
  , guardEntries :: TVar n (Map Int (Guarded n job))
  , guardNextToken :: TVar n Int
  , guardExtending :: TVar n (Maybe Extending)
  , guardWake :: Wake n
  }

-- | How the guard loop is woken before its target.
data Wake n = Wake
  { wakeCount :: TVar n Int
  -- ^ Bumped by each wake.
  , wakeTarget :: TVar n (Maybe Time)
  -- ^ The time the loop sleeps until.
  }

newHeartbeatGuard :: (MonadSTM n) => GuardConfig n job -> n (HeartbeatGuard n job)
newHeartbeatGuard config =
  HeartbeatGuard config
    <$> newTVarIO Map.empty
    <*> newTVarIO 0
    <*> newTVarIO Nothing
    <*> (Wake <$> newTVarIO 0 <*> newTVarIO Nothing)

-- | The key the guard tracks jobs by.
guardKey :: HeartbeatGuard n job -> job -> JobId
guardKey = configKey . guardConfig

-- | Run @action@ on the calling thread with the batch registered with the guard.
guardBatch
  :: (MonadFork n, MonadMask n, MonadMonotonicTime n, MonadSTM n, MonadTime n)
  => HeartbeatGuard n job
  -> Batch n job
  -> n a
  -> n a
{-# SPECIALIZE guardBatch :: HeartbeatGuard IO job -> Batch IO job -> IO a -> IO a #-}
guardBatch guard batch action =
  asSync (bracket register unregister (const action))
  where
    config = guardConfig guard
    register = do
      wallNow <- getCurrentTime
      monoNow <- getMonotonicTime
      handler <- myThreadId
      let elapsed = toDiffTime (diffUTCTime wallNow (batchStart batch))
          leaseUntil = addTime (configTimeout config - elapsed) monoNow
          firstBeat = addTime (heartbeatWait (configInterval config) True (leaseUntil `diffTime` monoNow)) monoNow
          deadline = (`addTime` monoNow) <$> configMaxDuration config
      timers <- newTVarIO (Timers leaseUntil firstBeat False False Nothing (Just []))
      atomically $ do
        token <- stateTVar (guardNextToken guard) (\next -> (next, next + 1))
        let entry = Guarded token batch handler deadline timers
        modifyTVar' (guardEntries guard) (Map.insert token entry)
        wakeFor guard (minimum (leaseUntil : firstBeat : maybeToList deadline))
        pure entry

    asSync act = act `catch` (\(GuardSignal exc) -> throwIO exc)

    unregister entry = do
      carrying <- uninterruptibleMask_ $ do
        carrying <- atomically $ do
          modifyTVar' (guardEntries guard) (Map.delete (guardedToken entry))
          stateTVar (guardedTimers entry) (\timers -> (fromMaybe [] (couriers timers), timers {couriers = Nothing}))
        carrying <$ traverse_ killThread carrying
      -- A signal a courier had already sent lands here, inside the boundary, and is dropped.
      unless (null carrying) $ interruptible (pure ()) `catch` \GuardSignal {} -> pure ()

-- | Every registered batch with its timers.
snapshot :: (MonadSTM n) => HeartbeatGuard n job -> STM n [(Guarded n job, Timers n)]
snapshot guard = do
  entries <- readTVar (guardEntries guard)
  traverse (\entry -> (,) entry <$> readTVar (guardedTimers entry)) (Map.elems entries)

adjust :: (MonadSTM n) => Guarded n job -> (Timers n -> Timers n) -> n ()
adjust entry = atomically . modifyTVar' (guardedTimers entry)

-- | The batch's jobs still awaiting an outcome. None once it unregistered.
pendingOf :: (MonadSTM n) => Guarded n job -> n [job]
pendingOf entry = do
  timers <- readTVarIO (guardedTimers entry)
  if isJust (couriers timers) then batchPending (guardedBatch entry) else pure []

-- | When the lease fence fires. An extend carrying the batch holds it until the extend gives up.
leaseFence :: Maybe Extending -> Guarded n job -> Timers n -> Time
leaseFence extending entry timers = case extending of
  Just running | Set.member (guardedToken entry) (carries running), issuedAt running < lease -> max lease (givesUp running)
  _ -> lease
  where
    lease = leaseAt timers

-- | When the guard next acts on a batch. Beats wait for the extend in flight.
dueTimes :: Maybe Extending -> (Guarded n job, Timers n) -> [Time]
dueTimes extending (entry, timers) =
  [leaseFence extending entry timers | not (stopped timers)]
    <> [beatAt timers | not (stopped timers), isNothing extending]
    <> [deadline | not (fenced timers), Just deadline <- [guardedDeadline entry]]

-- | Publish the loop's target, the earliest due time.
plan :: (MonadSTM n) => HeartbeatGuard n job -> STM n (Maybe Time, Int)
plan guard = do
  times <- concatMap <$> (dueTimes <$> readTVar (guardExtending guard)) <*> snapshot guard
  let target = if null times then Nothing else Just (minimum times)
  writeTVar (wakeTarget (guardWake guard)) target
  (,) target <$> readTVar (wakeCount (guardWake guard))

-- | Wake the loop when @at@ precedes its target.
wakeFor :: (MonadSTM n) => HeartbeatGuard n job -> Time -> STM n ()
wakeFor guard at = do
  target <- readTVar (wakeTarget (guardWake guard))
  when (maybe True (at <) target) (wake guard)

wake :: (MonadSTM n) => HeartbeatGuard n job -> STM n ()
wake guard = modifyTVar' (wakeCount (guardWake guard)) (+ 1)

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
  (extending, current) <- atomically ((,) <$> readTVar (guardExtending guard) <*> snapshot guard)
  -- A batch the fence stops gets no beat.
  leased <- filterM (fence guard woke extending) current
  let due = [entry | (entry, timers) <- leased, beatAt timers <= woke]
  unless (null due || isJust extending) $ do
    let bound =
          max
            minRetryPause
            (minimum (configTimeout (guardConfig guard) : [leaseAt timers `diffTime` woke | (_, timers) <- leased]))
    issued <- getMonotonicTime
    atomically $
      writeTVar (guardExtending guard) (Just (Extending issued (addTime bound issued) (Set.fromList (map guardedToken due))))
    void . forkIO $
      extend guard issued bound due `finally` atomically over
  where
    -- The extend is over. The fence is due at the leases again.
    over = writeTVar (guardExtending guard) Nothing *> wake guard

-- | Signal a handler past its lease or its deadline. Whether its lease still stands.
fence
  :: (MonadFork n, MonadSTM n) => HeartbeatGuard n job -> Time -> Maybe Extending -> (Guarded n job, Timers n) -> n Bool
fence guard woke extending (entry, timers) = do
  let lapsed = not (stopped timers) && woke >= leaseFence extending entry timers
  when lapsed $ do
    live <- pendingOf entry
    adjust entry (\t -> t {stopped = True})
    unless (null live) $
      signal
        guard
        woke
        entry
        (toException (JobGoneException leaseExpiredReason (map (configKey (guardConfig guard)) live)))
  for_ (guardedDeadline entry) $ \deadline ->
    when (not (fenced timers) && woke >= deadline) $ do
      adjust entry (\t -> t {fenced = True})
      signal guard woke entry (toException (JobDeadlineExceeded (durationMessage guard)))
  pure (not (stopped timers || lapsed))

-- | Ask the handler to stop, from a courier thread. A second ask within one beat is dropped.
signal :: (MonadFork n, MonadSTM n) => HeartbeatGuard n job -> Time -> Guarded n job -> SomeException -> n ()
signal guard now entry exc = do
  fresh <- atomically $ stateTVar (guardedTimers entry) $ \timers ->
    let due = maybe True (\at -> now >= addTime (configInterval (guardConfig guard)) at) (signalledAt timers)
     in (due, if due then timers {signalledAt = Just now} else timers)
  when fresh . void . forkIO $ do
    courier <- myThreadId
    claimed <- atomically $ stateTVar (guardedTimers entry) $ \timers -> case couriers timers of
      Nothing -> (False, timers)
      Just carrying -> (True, timers {couriers = Just (courier : carrying)})
    when claimed (throwTo (guardedHandler entry) (GuardSignal exc))

-- | One extend statement over every due batch, bounded by @bound@.
extend
  :: (MonadCatch n, MonadFork n, MonadTime n, MonadTimer n)
  => HeartbeatGuard n job
  -> Time
  -> DiffTime
  -> [Guarded n job]
  -> n ()
extend guard issued bound due = do
  lives <- traverse (\entry -> (,) entry <$> pendingOf entry) due
  outcome <- timeout bound (trySync (configExtend config (concatMap snd lives)))
  case outcome of
    Nothing -> traverse_ retryLater due
    Just (Left exception) -> do
      for_ due $ \entry ->
        configLog config Error (toList (batchJobs (guardedBatch entry))) ("Heartbeat error (retrying): " <> displayEx exception)
      traverse_ retryLater due
    Just (Right results) -> do
      configExtended config
      currentTime <- getCurrentTime
      let byJob = Map.fromList [(resultId result, result) | result <- results]
      traverse_ (settle guard issued currentTime byJob) lives
  where
    config = guardConfig guard
    retryLater entry = do
      now <- getMonotonicTime
      adjust
        entry
        (\timers -> timers {beatAt = addTime (heartbeatWait (configInterval config) False (leaseAt timers `diffTime` now)) now})

-- | 'try' for synchronous exceptions only.
trySync :: (MonadCatch n) => n a -> n (Either SomeException a)
trySync = tryJust (\exc -> if isSyncException exc then Just exc else Nothing)

-- | Act on one batch's verdicts from the extend.
settle
  :: (MonadFork n, MonadMonotonicTime n, MonadSTM n)
  => HeartbeatGuard n job
  -> Time
  -> UTCTime
  -> Map JobId SetVisibilityResult
  -> (Guarded n job, [job])
  -> n ()
settle guard issued currentTime byJob (entry, live) = do
  let key = configKey config
      mine = mapMaybe ((`Map.lookup` byJob) . key) live
  -- Rows this worker settled during the statement do not count.
  stillPending <- Set.fromList . map key <$> pendingOf entry
  let cancelledJobs = [jobId | JobCancelled jobId <- mine]
      stolenJobs = [jobId | JobReclaimed jobId _ _ <- mine]
      goneJobs = [jobId | JobGone jobId <- mine]
      unmoved = [() | VisibilityUnchanged jobId <- mine, Set.member jobId stillPending]
      extendedIds = Set.fromList [jobId | VisibilityExtended jobId <- mine]
      extended = filter ((`Set.member` extendedIds) . key) live
  adjust entry $ \timers ->
    let lease = if null unmoved then addTime (configTimeout config) issued else leaseAt timers
     in timers {leaseAt = lease, beatAt = addTime (heartbeatWait (configInterval config) True (lease `diffTime` issued)) issued}
  now <- getMonotonicTime
  case (cancelledJobs, stolenJobs) of
    (_ : _, _) -> signal guard now entry (toException (JobForceCancelled cancelledJobs (stolenJobs <> goneJobs)))
    ([], _ : _) -> signal guard now entry (toException (JobGoneException reclaimedReason stolenJobs))
    ([], []) ->
      unless (null extended) . void . forkIO . batchInherit batch $
        for_ extended $
          \job -> configHeartbeat config job currentTime (batchStart batch)
  where
    config = guardConfig guard
    batch = guardedBatch entry

resultId :: SetVisibilityResult -> JobId
resultId result = case result of
  VisibilityExtended jobId -> jobId
  VisibilityUnchanged jobId -> jobId
  JobCancelled jobId -> jobId
  JobReclaimed jobId _ _ -> jobId
  JobGone jobId -> jobId
  JobSuspended jobId -> jobId

durationMessage :: HeartbeatGuard n job -> Text
durationMessage guard =
  "handler ran past the maximum job duration"
    <> foldMap ((" of " <>) . T.pack . show) (configMaxDuration (guardConfig guard))

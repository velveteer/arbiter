{-# LANGUAGE OverloadedStrings #-}

-- | A batch's lifecycle: the handoff, the settle ordering, the outcome report and
-- the force-cancel finalizer.
--
-- The pool supplies every statement and hook as an 'Effects' action. Each
-- transactional unit is one effect, so the lifecycle never composes across a
-- transaction. Written against io-classes, so the pool runs it in IO and the
-- tests run it under io-sim.
module Arbiter.Worker.Batch
  ( -- * The pool's side
    Effects (..)
  , Report (..)
  , Failure
  , FailureKind (..)
  , Outcome (..)
  , cancelsTree
  , Mode (..)
  , Callbacks (..)

    -- * Running a batch
  , Handoff
  , newHandoff
  , runBatch
  , afterBatch
  ) where

import Arbiter.Core.Exceptions
  ( BranchCancelException (..)
  , JobDeadlineExceeded (..)
  , JobException (..)
  , JobForceCancelled (..)
  , JobGoneException (..)
  , JobNackException (..)
  , JobPermanentException (..)
  , JobRetryableException (..)
  , ParsingException (..)
  , TreeCancelException (..)
  , displayEx
  , namedJobIds
  )
import Arbiter.Core.Job.Types (JobId)
import Control.Concurrent.Class.MonadSTM
  ( MonadSTM
  , TVar
  , atomically
  , modifyTVar'
  , newTVarIO
  , readTVarIO
  , stateTVar
  )
import Control.Monad (unless, void, when)
import Control.Monad.Class.MonadFork (MonadFork)
import Control.Monad.Class.MonadThrow (Exception (..), MonadCatch (..), MonadMask (..), MonadThrow (..), SomeException)
import Control.Monad.Class.MonadTime.SI (MonadMonotonicTime, MonadTime (..), UTCTime)
import Data.Foldable (toList, traverse_)
import Data.List (partition, sortOn)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (isNothing)
import Data.Ord (Down (..))
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)

import Arbiter.Worker.Heartbeat.Guard (Batch (..), HeartbeatGuard, guardBatch, guardKey, trySync)
import Arbiter.Worker.Logger (LogLevel (..))

-- ---------------------------------------------------------------------------
-- The pool's side
-- ---------------------------------------------------------------------------

-- | A hook the lifecycle fires.
data Report job
  = Claimed job UTCTime
  | Succeeded job UTCTime UTCTime
  | -- | A handler failure, written as the outcome says.
    Failed job Text UTCTime UTCTime Outcome
  | -- | A force-cancel or a tree cancel took the job, for the reason given.
    Cancelled job Text
  | -- | This worker can no longer act on the job.
    Unavailable job Text

-- | Whether a failure deletes a job tree or updates the job claim.
data FailureKind = RetryFailure | PermanentFailure | TreeCancelFailure | BranchCancelFailure
  deriving stock (Eq, Show)

-- | A handler failure: its message and disposition.
type Failure = (Text, FailureKind)

-- | Where a failure write left the job.
data Outcome = Retrying NominalDiffTime | DeadLettered | TreeCancelled
  deriving stock (Eq, Show)

-- | The statements and hooks a batch drives, each in the context @ctx@ it is
-- called from. A callback runs in the handler's own context, so a statement it
-- drives joins a transaction the handler holds. Each is one transaction or one hook.
data Effects n ctx job stored = Effects
  { effectAmbient :: ctx
  -- ^ The context the batch runs in, outside the handler.
  , effectSpan :: ((n () -> n ()) -> n ()) -> n ()
  -- ^ Runs the batch under its consumer span, handing it the capture that runs
  -- the heartbeat hooks under that span.
  , effectAck :: ctx -> job -> Maybe stored -> n ()
  -- ^ Ack one job and store its result. Throws 'JobGoneException' for a job held elsewhere.
  , effectAckAll :: ctx -> [(job, Maybe stored)] -> n ([job], [job])
  -- ^ Ack in bulk. The jobs acked, and the jobs found held elsewhere.
  , effectFail :: ctx -> Failure -> job -> n (Either Text Outcome)
  -- ^ Write one job's failure. 'Left' why no row was written.
  , effectFailAll :: ctx -> Failure -> [job] -> [job] -> n [(job, Either Text Outcome)]
  -- ^ Write the failures of the jobs left unfinalized, and cancel the jobs held
  -- elsewhere when the failure cancels a tree, in one transaction.
  , effectDeleteCancelled :: ctx -> [job] -> n (Set JobId)
  -- ^ Delete the jobs a force-cancel flagged. The ids deleted.
  , effectRelease :: ctx -> [job] -> n (Set JobId)
  -- ^ Hand back the attempt the claim consumed. The ids released.
  , effectReport :: ctx -> Report job -> n ()
  , effectLog :: LogLevel -> [job] -> Text -> n ()
  }

-- | How the pool runs a batch's handler.
data Mode n ctx job stored
  = -- | One transaction that runs the handler, acks the job and stores its result.
    SingleMode (job -> n ())
  | -- | The handler settles each job through the callbacks, in its context at the call.
    BatchedMode (NonEmpty job -> Callbacks n ctx job stored -> n ())

-- | The settle operations a batch handler drives its jobs through.
data Callbacks n ctx job stored = Callbacks
  { callbackAck :: ctx -> job -> Maybe stored -> n ()
  , callbackAckAll :: ctx -> [(job, Maybe stored)] -> n ()
  , callbackFail :: ctx -> Failure -> job -> n ()
  , callbackNack :: ctx -> job -> n ()
  }

-- ---------------------------------------------------------------------------
-- The handoff
-- ---------------------------------------------------------------------------

-- | What a batch has settled so far.
data Progress n = Progress
  { progressHandled :: !(Set JobId)
  -- ^ Jobs whose outcome has been recorded.
  , progressUnowned :: !(Set JobId)
  -- ^ Jobs a batch ack found under another claim.
  , progressCancelled :: !(Set JobId)
  -- ^ Jobs a force-cancel accounted for.
  , progressReported :: !(Set JobId)
  -- ^ Jobs whose terminal report fired.
  , progressDeferred :: !(Maybe (n ()))
  -- ^ The hooks of a settle a signal can interrupt.
  , progressFinalized :: !Bool
  -- ^ Whether the force-cancel finalizer ran to completion.
  }

-- | Finalization state shared by the handler and the force-cancel finalizer, with
-- the batch's job key.
data Handoff n job = Handoff
  { handoffKey :: job -> JobId
  , handoffVar :: TVar n (Progress n)
  }

-- | A handoff keyed the way the guard keys its jobs.
newHandoff :: (MonadSTM n) => HeartbeatGuard n job -> n (Handoff n job)
{-# SPECIALIZE newHandoff :: HeartbeatGuard IO job -> IO (Handoff IO job) #-}
newHandoff guard = Handoff (guardKey guard) <$> newTVarIO (Progress mempty mempty mempty mempty Nothing False)

readProgress :: (MonadSTM n) => Handoff n job -> n (Progress n)
readProgress = readTVarIO . handoffVar

alterProgress :: (MonadSTM n) => Handoff n job -> (Progress n -> Progress n) -> n ()
alterProgress handoff = atomically . modifyTVar' (handoffVar handoff)

onProgress :: (MonadSTM n) => Handoff n job -> (Progress n -> (a, Progress n)) -> n a
onProgress handoff = atomically . stateTVar (handoffVar handoff)

-- | What a settle accounted for: the jobs it finalized, and the jobs it found under
-- another claim.
data Settled job = Settled [job] [job]

finalized :: [job] -> Settled job
finalized jobs = Settled jobs []

-- | Record a finalization. Jobs owned by another claim also count as handled.
recorded :: Handoff n job -> Settled job -> Progress n -> Progress n
recorded handoff (Settled handled unowned) progress =
  progress
    { progressHandled = progressHandled progress <> ids handled <> gone
    , progressUnowned = progressUnowned progress <> gone
    }
  where
    gone = ids unowned
    ids = Set.fromList . map (handoffKey handoff)

record :: (MonadSTM n) => Handoff n job -> Settled job -> n ()
record handoff = alterProgress handoff . recorded handoff

-- | Whether a set of job ids names this job.
hasIdIn :: Handoff n job -> Set JobId -> job -> Bool
hasIdIn handoff ids job = Set.member (handoffKey handoff job) ids

-- | Select jobs from the batch in descending identifier order. This gives ack,
-- force-cancel and heartbeat operations the same row-lock order.
byIdDesc :: Handoff n job -> (job -> Bool) -> NonEmpty job -> [job]
byIdDesc handoff keep = sortOn (Down . handoffKey handoff) . filter keep . toList

-- | Jobs in the batch by membership in one of the recorded sets.
jobsBy :: (MonadSTM n) => Handoff n job -> (Progress n -> Set JobId) -> (Bool -> Bool) -> NonEmpty job -> n [job]
jobsBy handoff recordedSet member jobs = do
  progress <- readProgress handoff
  pure (byIdDesc handoff (member . hasIdIn handoff (recordedSet progress)) jobs)

-- | Jobs in the batch that have no recorded outcome.
pendingJobs :: (MonadSTM n) => Handoff n job -> NonEmpty job -> n [job]
pendingJobs handoff = jobsBy handoff progressHandled not

-- | The batch's jobs a settle found under another claim.
unownedJobs :: (MonadSTM n) => Handoff n job -> NonEmpty job -> n [job]
unownedJobs handoff = jobsBy handoff progressUnowned id

-- | Add to the jobs a force-cancel accounted for, returning every id recorded so far.
recordCancelled :: (MonadSTM n) => Handoff n job -> Set JobId -> n (Set JobId)
recordCancelled handoff ids =
  onProgress handoff $ \progress ->
    let cancelled = progressCancelled progress <> ids
     in (cancelled, progress {progressCancelled = cancelled})

-- | Drop the hooks the batch's next report would finish.
clearDeferred :: (MonadSTM n) => Handoff n job -> n ()
clearDeferred handoff = alterProgress handoff $ \progress -> progress {progressDeferred = Nothing}

-- | Run the hooks a signal interrupted.
finishDeferred :: (MonadSTM n) => Handoff n job -> n ()
finishDeferred handoff = readProgress handoff >>= traverse_ (*> clearDeferred handoff) . progressDeferred

markFinalized :: (MonadSTM n) => Handoff n job -> n ()
markFinalized handoff = alterProgress handoff $ \progress -> progress {progressFinalized = True}

-- ---------------------------------------------------------------------------
-- Running a batch
-- ---------------------------------------------------------------------------

-- | What every settle path works with: the effects, the context to run them in,
-- and the handoff. A callback rebuilds it with the handler's context.
data Run n ctx job stored = Run
  { runEffects :: Effects n ctx job stored
  , runContext :: ctx
  , runHandoff :: Handoff n job
  }

-- | The batch's run outside the handler.
ambientRun :: Effects n ctx job stored -> Handoff n job -> Run n ctx job stored
ambientRun effects = Run effects (effectAmbient effects)

-- | Run the batch on the calling thread: the claim hooks, the handler under the
-- guard, the outcome report, and the force-cancel finalizer.
runBatch
  :: (MonadFork n, MonadMask n, MonadMonotonicTime n, MonadSTM n, MonadTime n)
  => Effects n ctx job stored
  -> HeartbeatGuard n job
  -> Mode n ctx job stored
  -> Handoff n job
  -> NonEmpty job
  -> n ()
{-# SPECIALIZE runBatch ::
  Effects IO ctx job stored
  -> HeartbeatGuard IO job
  -> Mode IO ctx job stored
  -> Handoff IO job
  -> NonEmpty job
  -> IO ()
  #-}
runBatch effects guard mode handoff jobs = do
  startTime <- getCurrentTime
  let run = ambientRun effects handoff
      (firstJob :| _) = jobs
      -- Rethrown as it came. The flag is set last. An interrupted finalizer
      -- leaves the rest to 'afterBatch'.
      onForceCancel exc@(JobForceCancelled cancelledIds goneIds) = do
        finalizeForceCancelled run jobs cancelledIds goneIds
        markFinalized handoff
        throwIO exc
  -- The span covers the claim hooks, the outcome report and the force-cancel
  -- finalizer.
  effectSpan effects $ \inherit -> (`catch` onForceCancel) $ do
    traverse_ (\job -> report run (Claimed job startTime)) jobs
    result <-
      trySync $
        guardBatch guard (Batch jobs (pendingJobs handoff jobs) startTime inherit) $
          case mode of
            -- The commit stays interruptible. The transaction contains the handler.
            SingleMode transaction ->
              settleWith run (\restore -> restore (transaction firstJob)) (const (finalized [firstJob])) $ \at () ->
                reportSuccess at startTime firstJob
            BatchedMode handler -> handler jobs (callbacks run startTime)
    endTime <- getCurrentTime
    reportBatchOutcome run jobs startTime endTime result

-- | Finalize a force-cancel that the batch left undone: one delivered before the
-- catch in 'runBatch', or one that interrupted its finalizer.
afterBatch
  :: (MonadMask n, MonadSTM n) => Effects n ctx job stored -> Handoff n job -> NonEmpty job -> JobForceCancelled -> n ()
{-# SPECIALIZE afterBatch ::
  Effects IO ctx job stored -> Handoff IO job -> NonEmpty job -> JobForceCancelled -> IO ()
  #-}
afterBatch effects handoff jobs (JobForceCancelled cancelledIds goneIds) = do
  already <- progressFinalized <$> readProgress handoff
  unless already $ finalizeForceCancelled (ambientRun effects handoff) jobs cancelledIds goneIds

-- ---------------------------------------------------------------------------
-- Settling
--
-- Settle: commit a statement, record what it accounted for, then fire its hooks.
-- Report: fire hooks only. Release: hand back the attempt, the nack. Finalize:
-- the force-cancel path.
-- ---------------------------------------------------------------------------

-- | An effect, in the run's context.
effect :: (Effects n ctx job stored -> ctx -> a) -> Run n ctx job stored -> a
effect field run = field (runEffects run) (runContext run)

report :: Run n ctx job stored -> Report job -> n ()
report = effect effectReport

logAt :: Run n ctx job stored -> LogLevel -> [job] -> Text -> n ()
logAt run = effectLog (runEffects run)

-- | Commit a settle, record it, then run its hooks. The mask covers the commit,
-- the record and the deferral, so a signal that interrupts the hooks leaves them
-- to the batch's next report, under the ambient context. The commit may lift the
-- mask for its own run. A nested settle keeps the outermost deferral. Returns
-- what the commit did.
settleWith
  :: (MonadMask n, MonadSTM n)
  => Run n ctx job stored
  -> ((forall x. n x -> n x) -> n a)
  -> (a -> Settled job)
  -> (Run n ctx job stored -> a -> n ())
  -> n a
settleWith run commit accounts hooks = do
  (result, outermost) <- mask $ \restore -> do
    result <- commit restore
    let later = hooks (ambientRun (runEffects run) handoff) result
        settled progress =
          ( isNothing (progressDeferred progress)
          , (recorded handoff (accounts result) progress)
              { progressDeferred = maybe (Just later) Just (progressDeferred progress)
              }
          )
    (,) result <$> onProgress handoff settled
  hooks run result
  result <$ when outermost (clearDeferred handoff)
  where
    handoff = runHandoff run

-- | 'settleWith' for a set known before the commit.
settle
  :: (MonadMask n, MonadSTM n)
  => Run n ctx job stored -> Settled job -> n a -> (Run n ctx job stored -> a -> n ()) -> n a
settle run settled commit = settleWith run (\_ -> commit) (const settled)

-- | Fire a job's terminal report, once per batch. The claim and the report are one
-- unit against a signal.
reportOnce :: (MonadMask n, MonadSTM n) => Run n ctx job stored -> job -> n () -> n ()
reportOnce run job fire = mask_ $ do
  fresh <- onProgress handoff $ \progress ->
    ( not (Set.member key (progressReported progress))
    , progress {progressReported = Set.insert key (progressReported progress)}
    )
  when fresh fire
  where
    handoff = runHandoff run
    key = handoffKey handoff job

reportSuccess :: (MonadMask n, MonadSTM n, MonadTime n) => Run n ctx job stored -> UTCTime -> job -> n ()
reportSuccess run startTime job = reportOnce run job (getCurrentTime >>= report run . Succeeded job startTime)

-- | Report a failure's write.
reportFailed
  :: (MonadMask n, MonadSTM n) => Run n ctx job stored -> Text -> UTCTime -> UTCTime -> job -> Either Text Outcome -> n ()
reportFailed run msg startTime endTime job = traverse_ (reportOnce run job . report run . toReport)
  where
    toReport TreeCancelled = Cancelled job msg
    toReport outcome = Failed job msg startTime endTime outcome

-- | The settle operations a batch handler drives its jobs through, each from the
-- handler's context at the call.
callbacks :: (MonadMask n, MonadSTM n, MonadTime n) => Run n ctx job stored -> UTCTime -> Callbacks n ctx job stored
callbacks base startTime =
  Callbacks
    { callbackAck = \ctx job stored -> within ctx $ \run ->
        settle run (finalized [job]) (effect effectAck run job stored) $ \at () ->
          reportSuccess at startTime job
    , -- Only the commit knows which jobs it acked and which had moved.
      callbackAckAll = \ctx pairs -> within ctx $ \run ->
        void $ settleWith run (\_ -> effect effectAckAll run pairs) (uncurry Settled) $ \at (done, reclaimed) -> do
          traverse_ (reportSuccess at startTime) done
          unless (null reclaimed) $ do
            logAt at Info reclaimed ("Jobs " <> unownedReason <> " during bulk completion, skipped")
            void (settleGoneJobs at unownedReason reclaimed)
    , callbackFail = \ctx failure@(msg, _) job -> within ctx $ \run -> do
        endTime <- getCurrentTime
        void $ settle run (finalized [job]) (effect effectFail run failure job) $ \at outcome -> do
          reportFailed at msg startTime endTime job outcome
          settleUnwritten at [(job, outcome)]
    , callbackNack = \ctx job -> within ctx $ \run -> releaseJobs run [job]
    }
  where
    within ctx body = body base {runContext = ctx}

unownedReason :: Text
unownedReason = "no longer claimed by this worker"

forceCancelledLog :: Text
forceCancelledLog = "Job(s) force-cancelled"

-- | The reason a force-cancelled job reports.
forceCancelledReason :: Text
forceCancelledReason = "force-cancelled"

-- | Delete the jobs a force-cancel flagged, report them, and hand back the attempt
-- the claim consumed for the batch siblings it interrupted.
finalizeForceCancelled
  :: (MonadMask n, MonadSTM n)
  => Run n ctx job stored
  -> NonEmpty job
  -> [JobId]
  -- ^ The jobs the cancel named.
  -> [JobId]
  -- ^ Jobs the same signal found unavailable. Reported without a nack.
  -> n ()
finalizeForceCancelled run jobs cancelledIds goneIds = do
  finishDeferred handoff
  logAt run Info (toList jobs) forceCancelledLog
  pending <- pendingJobs handoff jobs
  -- The cancel can name a job the handler finalized after the cancel took effect.
  let settling = byIdDesc handoff (hasIdIn handoff (Set.fromList (cancelledIds <> map (handoffKey handoff) pending))) jobs
      goneSet = Set.fromList goneIds
      deletable = filter (not . hasIdIn handoff goneSet) settling
  deleted <- deleteCancelled run deletable
  cancelled <- recordCancelled handoff (deleted <> Set.fromList cancelledIds)
  let (gone, interrupted) = partition (hasIdIn handoff cancelled) settling
      (unavailable, siblings) = partition (hasIdIn handoff goneSet) interrupted
  unless (null unavailable) $ logAt run Info unavailable ("Job(s) " <> unownedReason <> ", skipping retry")
  reportGoneJobs run unownedReason (gone <> unavailable)
  releaseOrWarn run "Releasing a force-cancel batch sibling failed" siblings
  where
    handoff = runHandoff run

-- | Hand back the attempt the claim consumed for the jobs left unfinalized, in one
-- statement, and report whichever of them the release found under another claim.
releaseJobs :: (MonadMask n, MonadSTM n) => Run n ctx job stored -> [job] -> n ()
releaseJobs _ [] = pure ()
releaseJobs run jobs =
  void $ settle run (finalized jobs) (effect effectRelease run jobs) $ \at released ->
    settleUnwritten at [(job, Left unownedReason) | job <- jobs, not (hasIdIn (runHandoff run) released job)]

-- | 'releaseJobs' on an unwinding path. A failure is logged as a warning.
releaseOrWarn :: (MonadMask n, MonadSTM n) => Run n ctx job stored -> Text -> [job] -> n ()
releaseOrWarn run warning jobs = warnOn run jobs warning () (releaseJobs run jobs)

-- | Run an action, logging a warning and returning @fallback@ when it fails.
warnOn :: (MonadCatch n) => Run n ctx job stored -> [job] -> Text -> a -> n a -> n a
warnOn run jobs label fallback act =
  trySync act
    >>= either (\exception -> fallback <$ logAt run Warning jobs (label <> ": " <> displayEx exception)) pure

-- | Delete whichever of @jobs@ a force-cancel flagged, returning the ids deleted. A
-- failure is logged as a warning.
deleteCancelled :: (MonadCatch n) => Run n ctx job stored -> [job] -> n (Set JobId)
deleteCancelled run jobs =
  warnOn run jobs "Deleting force-cancelled jobs failed" mempty (effect effectDeleteCancelled run jobs)

-- | Settle the jobs a failure could not be written for, each under its own reason.
settleUnwritten :: (MonadMask n, MonadSTM n) => Run n ctx job stored -> [(job, Either Text outcome)] -> n ()
settleUnwritten run pairs =
  traverse_ settleOne (Map.toList (Map.fromListWith (<>) [(reason, [job]) | (job, Left reason) <- pairs]))
  where
    settleOne (reason, jobs) = do
      cancelled <- settleGoneJobs run reason jobs
      let (forced, unavailable) = partition (hasIdIn (runHandoff run) cancelled) jobs
      unless (null forced) $ logAt run Info forced forceCancelledLog
      unless (null unavailable) $ logAt run Warning unavailable ("Job(s) " <> reason)

-- | Delete whichever of @jobs@ a force-cancel flagged and report them all, as one
-- settle. Returns the ids deleted.
settleGoneJobs :: (MonadMask n, MonadSTM n) => Run n ctx job stored -> Text -> [job] -> n (Set JobId)
settleGoneJobs _ _ [] = pure mempty
settleGoneJobs run reason jobs =
  settleWith run (\_ -> deleteCancelled run jobs) (const (finalized jobs)) $ \at cancelled -> do
    void (recordCancelled (runHandoff at) cancelled)
    reportGoneJobs at reason jobs

-- | Report jobs this worker can no longer act on. A job a force-cancel deleted is
-- reported as cancelled. Each is recorded against the handoff.
reportGoneJobs :: (MonadMask n, MonadSTM n) => Run n ctx job stored -> Text -> [job] -> n ()
reportGoneJobs run reason jobs = do
  record handoff (finalized jobs)
  cancelled <- progressCancelled <$> readProgress handoff
  traverse_ (reportOne cancelled) jobs
  where
    handoff = runHandoff run
    reportOne cancelled job =
      reportOnce run job . report run $
        if hasIdIn handoff cancelled job then Cancelled job forceCancelledReason else Unavailable job reason

-- | Interpret a finished batch. Warn when the handler left jobs unfinalized. Skip
-- retry for gone or nacked jobs. Fail the rest.
reportBatchOutcome
  :: (MonadMask n, MonadSTM n)
  => Run n ctx job stored
  -> NonEmpty job
  -> UTCTime
  -> UTCTime
  -> Either SomeException ()
  -> n ()
reportBatchOutcome run jobs startTime endTime outcome = do
  finishDeferred handoff
  unhandled <- pendingJobs handoff jobs
  case outcome of
    Right () ->
      unless (null unhandled) $
        logAt run Warning unhandled "Handler left jobs unfinalized, will reprocess"
    Left exc
      | Just (JobGoneException reason gone) <- fromException exc -> do
          logAt run Info (toList jobs) ("Job(s) " <> reason <> ", skipping retry" <> namedJobIds gone)
          -- An exception naming no job speaks for none of them. The remainder keeps
          -- its attempt.
          let (jobsGone, siblings) = partition (hasIdIn handoff (Set.fromList gone)) unhandled
          void (settleGoneJobs run reason jobsGone)
          unless (null gone) $
            releaseOrWarn run "Releasing an interrupted batch sibling failed" siblings
      | Just JobNackException <- fromException exc -> do
          -- Hand back the attempt the claim consumed for every job the handler
          -- left unfinalized.
          releaseOrWarn run "Handing back a nacked batch's attempt failed" unhandled
          logAt run Info (toList jobs) "Job(s) nacked, will be reprocessed"
      | otherwise -> do
          -- Fail the jobs the handler did not finalize, in a separate transaction.
          let failure@(reason, kind) = classifyException exc
          when (null unhandled) $
            logAt run Warning (toList jobs) ("Handler stopped with its jobs already finalized: " <> reason)
          -- A tree or branch cancel acts on the whole tree.
          unowned <- if cancelsTree kind then unownedJobs handoff jobs else pure []
          void
            $ settle
              run
              (finalized unhandled)
              (effect effectFailAll run failure unhandled unowned)
            $ \at outcomes -> do
              traverse_ (reportOne at reason) outcomes
              settleUnwritten at outcomes
  where
    handoff = runHandoff run
    reportOne at reason (job, write) =
      warnOn at [job] "Reporting a job's failure failed" () (reportFailed at reason startTime endTime job write)

-- | Classify a handler exception into an error message and failure disposition.
-- 'reportBatchOutcome' intercepts 'JobGoneException' before this.
classifyException :: SomeException -> Failure
classifyException exception
  | Just (Retryable (JobRetryableException msg)) <- fromException exception = (msg, RetryFailure)
  | Just (Permanent (JobPermanentException msg)) <- fromException exception = (msg, PermanentFailure)
  | Just (TreeCancel (TreeCancelException msg)) <- fromException exception = (msg, TreeCancelFailure)
  | Just (BranchCancel (BranchCancelException msg)) <- fromException exception = (msg, BranchCancelFailure)
  | Just (ParsingException msg) <- fromException exception = (msg, PermanentFailure)
  | Just (JobDeadlineExceeded msg) <- fromException exception = (msg, RetryFailure)
  | otherwise = (T.pack (show exception), RetryFailure)

-- | Whether a failure deletes a job tree or updates the job claim.
cancelsTree :: FailureKind -> Bool
cancelsTree kind = kind `elem` [TreeCancelFailure, BranchCancelFailure]

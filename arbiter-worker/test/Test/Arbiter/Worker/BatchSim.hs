{-# LANGUAGE OverloadedStrings #-}

-- | The batch lifecycle under io-sim. The database is a row table implementing
-- the column contract of the statements the lifecycle drives. Generated plans
-- script the handler and outside actors, then IOSimPOR explores the schedules.
module Test.Arbiter.Worker.BatchSim (spec) where

import Arbiter.Core.Exceptions
  ( BranchCancelException (..)
  , JobException (..)
  , JobForceCancelled (..)
  , JobGoneException (..)
  , JobNackException (..)
  , JobPermanentException (..)
  , JobRetryableException (..)
  , TreeCancelException (..)
  )
import Arbiter.Core.HighLevel (SetVisibilityResult (..))
import Arbiter.Core.Job.Types (JobId)
import Control.Concurrent.Class.MonadSTM (TVar, atomically, newTVarIO, readTVarIO, stateTVar, writeTVar)
import Control.Monad (filterM, unless, void, when)
import Control.Monad.Class.MonadAsync (MonadAsync (..))
import Control.Monad.Class.MonadFork (MonadFork (..), MonadThread (..))
import Control.Monad.Class.MonadTest (exploreRaces)
import Control.Monad.Class.MonadThrow (MonadThrow (..), SomeException, fromException, toException)
import Control.Monad.Class.MonadTime.SI (DiffTime, Time (..), addTime, getMonotonicTime)
import Control.Monad.Class.MonadTimer.SI (threadDelay)
import Control.Monad.IOSim (IOSim)
import Data.Foldable (for_, toList)
import Data.List (mapAccumL, partition)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, listToMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Test.Hspec (Spec, describe, it)
import Test.QuickCheck
  ( Gen
  , Property
  , checkCoverage
  , choose
  , conjoin
  , counterexample
  , cover
  , coverTable
  , elements
  , frequency
  , listOf
  , tabulate
  , vectorOf
  , (===)
  )

import Arbiter.Worker.Batch
import Arbiter.Worker.Heartbeat.Guard
import Test.Arbiter.Worker.Sim
  ( Recorder
  , escaped
  , explorePlans
  , exploreScenario
  , hangExtend
  , newRecorder
  , refuseExtend
  , scripted
  , startGuard
  )

-- ---------------------------------------------------------------------------
-- The row table
-- ---------------------------------------------------------------------------

data Holder = Us | Other
  deriving stock (Eq, Show)

-- | One queue row: the columns the lifecycle's statements read and write.
data Row = Row
  { rowSeq :: !Int
  , rowHolder :: !(Maybe Holder)
  , rowLease :: !(Maybe Time)
  , rowFlagged :: !Bool
  , rowAttempts :: !Int
  }
  deriving stock (Eq, Show)

-- | A claimed job as this worker sees it: the row id and the claim token it holds.
data Job = Job
  { jobId :: JobId
  , jobSeq :: Int
  , jobAttempt :: Int
  -- ^ The attempt count this worker read at claim time.
  }
  deriving stock (Eq, Show)

type Rows s = TVar (IOSim s) (Map JobId Row)

-- | A row write: this worker's, or the reaper's sweep.
data Op = Acked | Retried | Dlqed | Released | Deleted | Flagged | Swept
  deriving stock (Eq, Show)

-- | An outside actor's move.
data Move
  = -- | A force-cancel: flag a live claim and bump its token, delete the rest, notify the holder.
    Flag JobId
  | -- | Another worker claims the row after its lease lapsed.
    Steal JobId
  | -- | The reaper deletes flagged rows whose lease lapsed.
    Sweep
  deriving stock (Eq, Show)

-- | What the scripted database answers an extend with. Consumed in order, the
-- last one repeats.
data ExtendReply = Extends | Refuses | Hangs
  deriving stock (Eq, Show)

-- | A terminal hook the lifecycle fired.
data Kind = SuccessK | CancelledK | UnavailableK Text | RetryK | DlqK
  deriving stock (Eq, Show)

data Ending = Returned | Threw String | Escaped String
  deriving stock (Eq, Show)

data Event
  = Reported JobId Kind Time
  | Landed JobId Op Time
  | Acted Move Time
  | Ended Ending Time
  | Issued [JobId] Time
  | Logged [JobId] Text Time
  deriving stock (Eq, Show)

-- | The retry delay the model writes. The lifecycle only reports it.
modelBackoff :: NominalDiffTime
modelBackoff = 0

-- | The reason a handler gives when it throws a job gone itself. The lifecycle takes
-- its word: the row is left to wait out its lease.
handlerGone :: Text
handlerGone = "gone"

-- | What the model answers when a failure write finds no row.
unwrittenRetry, unwrittenDlq :: Text
unwrittenRetry = "no longer available for retry"
unwrittenDlq = "no longer available for the dead-letter queue"

-- | The pool's timeout, and the lease the row model hands out. Short, so a batch
-- whose extends fail outlives it.
leaseTimeout :: DiffTime
leaseTimeout = 3

-- | How long a statement takes. A signal can land while it runs.
statementLatency :: DiffTime
statementLatency = 0.01

-- | How long a terminal hook takes. A signal can land after it fires.
reportLatency :: DiffTime
reportLatency = 0.005

-- | How long a retry parks a row.
retryPark :: DiffTime
retryPark = 60

-- | How long a plan waits after its batch, past every lease and move.
horizon :: DiffTime
horizon = 30

-- | A statement over the rows, at a time.
type Statement a = Time -> Map JobId Row -> (a, Map JobId Row)

-- | Run a statement over the rows. The write lands after the latency, at once.
statement :: Rows s -> Statement a -> IOSim s a
statement rows run = do
  threadDelay statementLatency
  now <- getMonotonicTime
  atomically (stateTVar rows (run now))

-- | Ack: delete the row the token still matches.
ackRow :: Job -> Statement Bool
ackRow job _ rows = case Map.lookup (jobId job) rows of
  Just row | rowSeq row == jobSeq job -> (True, Map.delete (jobId job) rows)
  _ -> (False, rows)

-- | Retry: park the row and release the claim when the token matches.
retryRow :: Job -> Statement Bool
retryRow job now rows = case Map.lookup (jobId job) rows of
  Just row
    | rowSeq row == jobSeq job ->
        (True, Map.insert (jobId job) row {rowHolder = Nothing, rowLease = Just (addTime retryPark now)} rows)
  _ -> (False, rows)

-- | Nack: release the claim and hand back the attempt when the token matches and a claim is held.
releaseRow :: Job -> Statement Bool
releaseRow job _ rows = case Map.lookup (jobId job) rows of
  Just row
    | rowSeq row == jobSeq job
    , isJust (rowHolder row) ->
        (True, Map.insert (jobId job) row {rowHolder = Nothing, rowAttempts = handedBack job row} rows)
  _ -> (False, rows)

-- | The attempt count a nack leaves behind.
handedBack :: Job -> Row -> Int
handedBack job row = min (max (jobAttempt job - 1) (max (rowAttempts row - 1) 0)) (rowAttempts row)

-- | Delete a flagged row this worker holds or no live lease holds.
deleteCancelledRow :: Job -> Statement Bool
deleteCancelledRow job now rows = case Map.lookup (jobId job) rows of
  Just row | rowFlagged row, rowHolder row == Just Us || maybe True (<= now) (rowLease row) -> (True, Map.delete (jobId job) rows)
  _ -> (False, rows)

-- | The heartbeat's reading of a row.
extendRow :: Job -> Statement SetVisibilityResult
extendRow job now rows = case Map.lookup (jobId job) rows of
  Nothing -> (JobGone (jobId job), rows)
  Just row
    | rowFlagged row, rowHolder row == Just Us, rowSeq row == jobSeq job + 1 -> (JobCancelled (jobId job), rows)
    | rowSeq row /= jobSeq job -> (JobReclaimed (jobId job) (fromIntegral (jobSeq job)) (fromIntegral (rowSeq row)), rows)
    | otherwise ->
        (VisibilityExtended (jobId job), Map.insert (jobId job) row {rowLease = Just (addTime leaseTimeout now)} rows)

-- | A force-cancel over one row. The holder to notify, and the op that landed.
flagRow :: JobId -> Statement (Maybe Holder, Maybe Op)
flagRow job now rows = case Map.lookup job rows of
  Just row
    | isJust (rowHolder row)
    , maybe False (> now) (rowLease row) ->
        ((rowHolder row, Just Flagged), Map.insert job row {rowFlagged = True, rowSeq = rowSeq row + 1} rows)
    | otherwise -> ((rowHolder row, Just Deleted), Map.delete job rows)
  Nothing -> ((Nothing, Nothing), rows)

-- | The reaper's pass: delete every flagged row whose lease lapsed.
sweepRows :: Statement [JobId]
sweepRows now rows = (Map.keys swept, rows `Map.difference` swept)
  where
    swept = Map.filter (\row -> rowFlagged row && maybe True (<= now) (rowLease row)) rows

-- | Another worker's claim, once the lease lapsed.
stealRow :: JobId -> Statement Bool
stealRow job now rows = case Map.lookup job rows of
  Just row
    | not (rowFlagged row) ->
        ( True
        , Map.insert
            job
            row
              { rowSeq = rowSeq row + 1
              , rowHolder = Just Other
              , rowLease = Just (addTime leaseTimeout now)
              , rowAttempts = rowAttempts row + 1
              }
            rows
        )
  _ -> (False, rows)

-- ---------------------------------------------------------------------------
-- The pool's side, over the row table
-- ---------------------------------------------------------------------------

-- | Run a job's row statement, recording the op when it lands. Whether it did.
landing :: Rows s -> Recorder s Event -> Op -> (Job -> Statement Bool) -> Job -> IOSim s Bool
landing rows recorder op run job = do
  done <- statement rows (run job)
  done <$ when done (recorder (Landed (jobId job) op))

-- | 'landing' over jobs. The ids that landed.
landedIds :: Rows s -> Recorder s Event -> Op -> (Job -> Statement Bool) -> [Job] -> IOSim s (Set.Set JobId)
landedIds rows recorder op run jobs = Set.fromList . map jobId <$> filterM (landing rows recorder op run) jobs

-- | The ack inside the pool's transaction. Throws for a job held elsewhere.
ackOrGone :: Rows s -> Recorder s Event -> Job -> IOSim s ()
ackOrGone rows recorder job = do
  acked <- landing rows recorder Acked ackRow job
  unless acked (throwIO (JobGoneException reclaimedReason [jobId job]))

modelEffects :: Rows s -> Recorder s Event -> Effects (IOSim s) () Job ()
modelEffects rows recorder =
  Effects
    { effectAmbient = ()
    , effectSpan = \body -> body id
    , effectAck = \() job _ -> ackOrGone rows recorder job
    , effectAckAll = \() pairs -> do
        let jobs = map fst pairs
        acked <- landedIds rows recorder Acked ackRow jobs
        pure (partition ((`Set.member` acked) . jobId) jobs)
    , effectFail = \() failure job -> failRow failure job
    , effectFailAll = \() failure unhandled unowned -> do
        outcomes <- traverse (\job -> (job,) <$> failRow failure job) unhandled
        for_ unowned cancelRow
        pure outcomes
    , effectDeleteCancelled = \() -> landedIds rows recorder Deleted deleteCancelledRow
    , effectRelease = \() -> landedIds rows recorder Released releaseRow
    , effectReport = \() -> \case
        Claimed _ _ -> pure ()
        Succeeded job _ _ -> reported job SuccessK
        Failed job _ _ _ outcome -> reported job (kindOf outcome)
        Cancelled job _ -> reported job CancelledK
        Unavailable job reason -> reported job (UnavailableK reason)
    , effectLog = \_ jobs message -> recorder (Logged (map jobId jobs) message)
    }
  where
    reported job kind = recorder (Reported (jobId job) kind) >> threadDelay reportLatency
    -- The model holds no tree, so a tree cancel is a force-cancel of the row.
    cancelRow job = do
      (_, op) <- statement rows (flagRow (jobId job))
      for_ op (recorder . Landed (jobId job))
    failRow (_, kind) job
      | cancelsTree kind = Right TreeCancelled <$ cancelRow job
      | kind == PermanentFailure = written Dlqed DeadLettered ackRow unwrittenDlq job
      | otherwise = written Retried (Retrying modelBackoff) retryRow unwrittenRetry job
    kindOf = \case
      Retrying _ -> RetryK
      DeadLettered -> DlqK
      TreeCancelled -> CancelledK
    written op outcome stmt reason job = do
      landed <- landing rows recorder op stmt job
      pure (if landed then Right outcome else Left reason)

guardConfigFor
  :: Rows s -> Recorder s Event -> TVar (IOSim s) [ExtendReply] -> Maybe DiffTime -> GuardConfig (IOSim s) Job
guardConfigFor rows recorder script deadline =
  GuardConfig
    { configInterval = 1
    , configTimeout = leaseTimeout
    , configMaxDuration = deadline
    , configKey = jobId
    , configExtend = \jobs -> do
        recorder (Issued (map jobId jobs))
        reply <- atomically (stateTVar script (scripted Extends))
        case reply of
          Refuses -> refuseExtend
          Hangs -> hangExtend
          Extends -> traverse (\job -> statement rows (extendRow job)) jobs
    , configExtended = pure ()
    , configLog = \_ _ _ -> pure ()
    , configHeartbeat = \_ _ _ -> pure ()
    }

-- ---------------------------------------------------------------------------
-- Plans
-- ---------------------------------------------------------------------------

-- | A step of a batched handler. Job numbers index the batch.
data Step
  = AckOne Int
  | AckMany [Int]
  | FailOne FailureKind Int
  | NackOne Int
  | Sleep DiffTime
  | Throw Thrown
  deriving stock (Show)

-- | What a handler throws.
data Thrown
  = ThrowRetryable
  | ThrowPermanent
  | ThrowTreeCancel
  | ThrowBranchCancel
  | ThrowGone Int
  | ThrowNack
  | ThrowOther
  deriving stock (Eq, Show)

-- | The throw takes out rows this worker no longer owns, reporting none of them.
cancelsWholeTree :: Thrown -> Bool
cancelsWholeTree thrown = thrown `elem` [ThrowTreeCancel, ThrowBranchCancel]

-- | The plan's handler throws such a cancel.
throwsTreeCancel :: Plan -> Bool
throwsTreeCancel plan =
  any isTreeThrow (planSteps plan) || maybe False (either cancelsWholeTree (const False)) (planSingle plan)
  where
    isTreeThrow (Throw thrown) = cancelsWholeTree thrown
    isTreeThrow _ = False

data Plan = Plan
  { planJobs :: Int
  , planSingle :: Maybe (Either Thrown DiffTime)
  -- ^ Single mode: what the transaction does. Otherwise batched with 'planSteps'.
  , planSteps :: [Step]
  , planMoves :: [(DiffTime, Move)]
  , planReplies :: [ExtendReply]
  -- ^ What the guard's extends answer, in order.
  , planDeadline :: Maybe DiffTime
  }
  deriving stock (Show)

genPlan :: Gen Plan
genPlan = do
  single <- frequency [(1, Just <$> genSingle), (3, pure Nothing)]
  -- Single mode claims one job per batch.
  count <- maybe (choose (1, 3)) (const (pure 1)) single
  steps <- choose (0, 4) >>= (`vectorOf` genStep count)
  ending <- frequency [(2, pure []), (1, (: []) . Throw <$> genThrown count)]
  moves <- listOf (genMove count)
  replies <- choose (1, 3) >>= (`vectorOf` genReply)
  deadline <- frequency [(4, pure Nothing), (1, Just <$> elements [0.4, 1.5, 3])]
  pure (Plan count single (settleOnce (steps <> ending)) (take 3 moves) replies deadline)
  where
    -- A handler settles each job at most once. Later steps naming a settled job are dropped.
    settleOnce = catMaybes . snd . mapAccumL keep Set.empty
      where
        keep seen step = case settles step of
          Just indexes
            | any (`Set.member` seen) indexes -> (seen, Nothing)
            | otherwise -> (Set.union seen (Set.fromList indexes), Just step)
          Nothing -> (seen, Just step)
    settles = \case
      AckOne index -> Just [index]
      AckMany indexes -> Just indexes
      FailOne _ index -> Just [index]
      NackOne index -> Just [index]
      _ -> Nothing
    genSingle = frequency [(2, Right <$> elements [0.5, 2]), (1, Left <$> genThrown 1)]
    genStep count =
      frequency
        [ (3, AckOne <$> choose (1, count))
        , (1, AckMany . enumFromTo 1 <$> choose (1, count))
        , (2, FailOne <$> elements [RetryFailure, PermanentFailure, TreeCancelFailure, BranchCancelFailure] <*> choose (1, count))
        , (1, NackOne <$> choose (1, count))
        , (3, Sleep <$> elements [0.005, 0.5, 1, 2.5])
        ]
    genThrown count =
      frequency
        [ (2, pure ThrowRetryable)
        , (1, pure ThrowPermanent)
        , (1, pure ThrowTreeCancel)
        , (1, pure ThrowBranchCancel)
        , (1, ThrowGone <$> choose (1, count))
        , (1, pure ThrowNack)
        , (1, pure ThrowOther)
        ]
    genReply = frequency [(6, pure Extends), (2, pure Refuses), (1, pure Hangs)]
    genMove count =
      (,)
        <$> elements [0.005, 0.02, 0.25, 0.505, 0.75, 1.5, 3, 4.5]
        <*> frequency [(4, elements [Flag, Steal] <*> (fromIntegral <$> choose (1, count))), (1, pure Sweep)]

thrownException :: NonEmpty Job -> Thrown -> SomeException
thrownException jobs = \case
  ThrowRetryable -> toException (Retryable (JobRetryableException "transient"))
  ThrowPermanent -> toException (Permanent (JobPermanentException "permanent"))
  ThrowTreeCancel -> toException (TreeCancel (TreeCancelException "cancel the tree"))
  ThrowBranchCancel -> toException (BranchCancel (BranchCancelException "cancel the branch"))
  ThrowGone index -> toException (JobGoneException handlerGone [jobId (toList jobs !! (index - 1))])
  ThrowNack -> toException JobNackException
  ThrowOther -> toException (userError "handler crashed")

-- | Run a plan: claim the rows, run the batch on a handler thread as the pool
-- does, let the actors move, and wait past everything.
runPlan :: Plan -> IOSim s (Plan, [Event], Map JobId Row)
runPlan plan = do
  exploreRaces
  (recorder, readEvents) <- newRecorder
  now <- getMonotonicTime
  let batch = Job 1 1 1 :| [Job (fromIntegral index) 1 1 | index <- [2 .. planJobs plan]]
      claimed =
        Map.fromList
          [(jobId job, Row 1 (Just Us) (Just (addTime leaseTimeout now)) False (jobAttempt job)) | job <- toList batch]
  rows <- newTVarIO claimed
  script <- newTVarIO (planReplies plan)
  guard <- startGuard (guardConfigFor rows recorder script (planDeadline plan))
  handlerVar <- newTVarIO Nothing
  for_ (planMoves plan) $ \(delay, move) -> forkIO $ do
    threadDelay delay
    act rows recorder handlerVar move
  worker (modelEffects rows recorder) guard (modeFor rows recorder batch plan) handlerVar batch recorder
  threadDelay horizon
  events <- readEvents
  table <- readTVarIO rows
  pure (plan, events, table)

-- | The worker thread: run the batch on its own thread, then finish what it left.
worker
  :: Effects (IOSim s) () Job ()
  -> HeartbeatGuard (IOSim s) Job
  -> Mode (IOSim s) () Job ()
  -> TVar (IOSim s) (Maybe (ThreadId (IOSim s)))
  -> NonEmpty Job
  -> Recorder s Event
  -> IOSim s ()
worker effects guard mode handlerVar batch recorder = do
  handoff <- newHandoff guard
  handler <- async (runBatch effects guard mode handoff batch)
  atomically (writeTVar handlerVar (Just (asyncThreadId handler)))
  result <- waitCatch handler
  case result of
    Right () -> recorder (Ended Returned)
    Left exception -> do
      for_ (fromException exception) (afterBatch effects handoff batch)
      recorder (Ended (ending exception))
  where
    ending exception
      | Just JobForceCancelled {} <- fromException exception = Threw "force-cancelled"
      | Just message <- escaped exception = Escaped message
      | otherwise = Threw (show exception)

act :: Rows s -> Recorder s Event -> TVar (IOSim s) (Maybe (ThreadId (IOSim s))) -> Move -> IOSim s ()
act rows recorder handlerVar move = case move of
  Flag job -> do
    (holder, _) <- statement rows (flagRow job)
    recorder (Acted move)
    -- The NOTIFY, carried to the holder's handler thread.
    when (holder == Just Us) $ do
      target <- readTVarIO handlerVar
      for_ target $ \tid -> void (forkIO (throwTo tid (JobForceCancelled [job] [])))
  Steal job -> do
    stolen <- statement rows (stealRow job)
    when stolen (recorder (Acted move))
  Sweep -> do
    swept <- statement rows sweepRows
    recorder (Acted move)
    for_ swept $ \job -> recorder (Landed job Swept)

-- | Single mode is the pool's transaction: the handler, then the ack.
modeFor :: Rows s -> Recorder s Event -> NonEmpty Job -> Plan -> Mode (IOSim s) () Job ()
modeFor rows recorder jobs plan = case planSingle plan of
  Just single -> SingleMode $ \job -> case single of
    Right runFor -> threadDelay runFor >> ackOrGone rows recorder job
    Left thrown -> throwIO (thrownException jobs thrown)
  Nothing -> BatchedMode $ \batch callbacks -> for_ (planSteps plan) $ \step -> case step of
    AckOne index -> callbackAck callbacks () (pick batch index) Nothing
    AckMany indexes -> callbackAckAll callbacks () [(pick batch index, Nothing) | index <- indexes]
    FailOne kind index -> callbackFail callbacks () ("failed", kind) (pick batch index)
    NackOne index -> callbackNack callbacks () (pick batch index)
    Sleep delay -> threadDelay delay
    Throw thrown -> throwIO (thrownException batch thrown)
  where
    pick batch index = toList batch !! (index - 1)

-- ---------------------------------------------------------------------------
-- Invariants
-- ---------------------------------------------------------------------------

-- | A plan, the events its run logged, and the rows it left.
type Result = (Plan, [Event], Map JobId Row)

jobsOf :: Plan -> [JobId]
jobsOf plan = [fromIntegral index | index <- [1 .. planJobs plan]]

batchThrew :: [Event] -> Bool
batchThrew events = not (null [() | Ended (Threw _) _ <- events])

-- | The terminal hooks fired for a job, with their log indexes.
reportedKinds :: [Event] -> JobId -> [(Int, Kind)]
reportedKinds events job = [(index, kind) | (index, Reported j kind _) <- zip [0 :: Int ..] events, j == job]

landedOp :: [Event] -> JobId -> Op -> Bool
landedOp events job op = not (null [() | Landed j o _ <- events, j == job, o == op])

-- | The lifecycle logged @message@ against the job.
loggedFor :: [Event] -> JobId -> Text -> Bool
loggedFor events job message = not (null [() | Logged ids m _ <- events, job `elem` ids, m == message])

-- | The handler returned and left the job unsettled. The lifecycle keeps the
-- claim and lets the lease lapse.
unfinalized :: [Event] -> JobId -> Bool
unfinalized events job = not (batchThrew events) && null (reportedKinds events job) && not (landedOp events job Released)

judgePlan :: Result -> Property
judgePlan (plan, events, table) =
  tabulate "batch ending" [batchEnding]
    . tabulate "job outcome" (map jobOutcome jobs)
    . tabulate "moves that landed" [show move | Acted move _ <- events]
    $ conjoin
      [ counterexample "the batch ended with an async exception" (null [() | Ended (Escaped _) _ <- events])
      , counterexample "the batch never ended" (length [() | Ended _ _ <- events] === 1)
      , conjoin (map judgeJob jobs)
      ]
  where
    batchEnding = maybe "never ended" describeEnding (listToMaybe [end | Ended end _ <- events])
    describeEnding = \case
      Returned -> "returned"
      Threw reason -> "threw " <> reason
      Escaped reason -> "escaped " <> reason
    jobOutcome job = case reportsOf job of
      (_, kind) : _ -> show kind
      [] | landed job Released -> "released"
      [] | Map.notMember job table -> "unfinalized, row gone"
      [] -> "unfinalized, row kept"

    jobs = jobsOf plan
    indexed = zip [0 :: Int ..] events
    endedAt = fromMaybe (Time 0) (listToMaybe [at | Ended _ at <- events])
    threw = batchThrew events
    reportsOf = reportedKinds events
    landed = landedOp events
    logged = loggedFor events
    moved job = not (null [() | Acted move _ <- events, target move == Just job])
    flagged job = not (null [() | Acted (Flag j) _ <- events, j == job])
    target (Flag j) = Just j
    target (Steal j) = Just j
    target Sweep = Nothing
    row job = Map.lookup job table
    treeCancelled = throwsTreeCancel plan
    stranded job = maybe False (\r -> rowHolder r == Just Us && maybe False (> endedAt) (rowLease r)) (row job)

    judgeJob job =
      conjoin
        [ counterexample ("job " <> show job <> " was reported more than once") (length reports <= 1)
        , counterexample ("job " <> show job <> " was reported successful without its ack landing") $
            SuccessK `notElem` kinds || landed job Acked
        , counterexample ("job " <> show job <> " was reported retried without its retry landing") $
            RetryK `notElem` kinds || landed job Retried
        , counterexample ("job " <> show job <> " was reported dead-lettered without the move landing") $
            DlqK `notElem` kinds || landed job Dlqed
        , counterexample ("job " <> show job <> " was reported cancelled without a cancel") $
            CancelledK `notElem` kinds || moved job || landed job Deleted || landed job Flagged
        , counterexample ("job " <> show job <> " was reported unavailable without cause") $
            and [moved job || threw || reason == leaseExpiredReason || reason == handlerGone | UnavailableK reason <- kinds]
        , counterexample ("job " <> show job <> " was reported unavailable without its resolution's log") $
            and [logged job ("Job(s) " <> reason) | UnavailableK reason <- kinds, reason `elem` [unwrittenRetry, unwrittenDlq]]
        , counterexample ("job " <> show job <> " was reported but its row is still leased to this worker") $
            null reports || heldExplained
        , counterexample ("job " <> show job <> " was neither reported nor released after the batch threw") $
            not threw || not (null reports) || landed job Released
        , counterexample ("job " <> show job <> " was left unfinalized and its row is gone without a cancel") $
            not (unfinalized events job) || flagged job || treeCancelled || Map.member job table
        , counterexample ("job " <> show job <> " survived a tree cancel untouched") $
            not treeCancelled || not (unfinalized events job) || landed job Flagged || Map.notMember job table
        , counterexample ("job " <> show job <> " was left unfinalized and this worker no longer holds it") $
            not (unfinalized events job) || moved job || maybe False ((== Just Us) . rowHolder) (row job)
        , counterexample ("a statement carried job " <> show job <> " after the batch ended") $
            null [() | Issued carried at <- events, job `elem` carried, at > endedAt]
        , counterexample ("job " <> show job <> " is still leased to this worker although the batch threw") $
            not threw || heldExplained
        , counterexample ("job " <> show job <> " was released and also reported") $
            not (landed job Released) || threw || null reports
        , counterexample ("job " <> show job <> " was nacked but kept its attempt") $
            not (landed job Released) || moved job || maybe True ((== 0) . rowAttempts) (row job)
        , counterexample ("job " <> show job <> " was released but this worker still holds it") $
            not (landed job Released) || maybe True ((/= Just Us) . rowHolder) (row job)
        , counterexample ("a statement carried job " <> show job <> " after its outcome was reported") $
            null
              [ ()
              | (i, Issued carried at) <- indexed
              , job `elem` carried
              , (reportIndex, reportAt) <- reportedAt
              , i > reportIndex
              , at > reportAt
              ]
        ]
      where
        reports = reportsOf job
        kinds = map snd reports
        -- A cancel flags the row it holds. The reaper reclaims it after the lease.
        cancelledAndFlagged = CancelledK `elem` kinds && maybe False rowFlagged (row job)
        heldExplained = UnavailableK handlerGone `elem` kinds || not (stranded job) || cancelledAndFlagged
        reportedAt = [(index, at) | (index, Reported j _ at) <- indexed, j == job]

-- | A force-cancel of one sibling lands after another sibling's ack commits.
ackThenSiblingCancel :: Plan
ackThenSiblingCancel = Plan 2 Nothing [Sleep 1, Sleep 0.5, AckMany [1]] [(1.5, Flag 2)] [Extends] Nothing

-- | A stolen job's failure write finds no row, and a sibling's force-cancel lands
-- inside the statement that resolves why.
unwrittenThenSiblingCancel :: Plan
unwrittenThenSiblingCancel =
  Plan
    3
    Nothing
    [Sleep 1, Sleep 0.5, FailOne RetryFailure 3, Throw ThrowRetryable]
    [(1.5, Flag 2), (1.5, Steal 3), (1.5, Steal 3)]
    [Extends]
    Nothing

-- | A stolen job's failure write finds no row, and a sibling's force-cancel lands
-- past the settle nested inside the resolution.
unwrittenThenLateCancel :: Plan
unwrittenThenLateCancel =
  Plan 2 Nothing [Sleep 1, FailOne RetryFailure 2] [(0.5, Steal 2), (1.01, Flag 1)] [Extends] Nothing

-- | The handler calls one job gone, and a sibling's force-cancel lands while the
-- lifecycle reports it.
goneThenSiblingCancel :: Plan
goneThenSiblingCancel = Plan 2 Nothing [Sleep 0.005, Throw (ThrowGone 1)] [(0.005, Flag 2)] [Extends] Nothing

-- | The share of plans that must leave a job unfinalized.
unfinalizedTarget :: Double
unfinalizedTarget = 10

-- | The share of explored schedules in which the cancel lands after the ack.
reportInterruptedTarget :: Double
reportInterruptedTarget = 20

-- | The share of explored schedules in which the cancel lands inside the resolution.
resolutionInterruptedTarget :: Double
resolutionInterruptedTarget = 5

-- | The share of explored schedules in which the cancel lands inside the gone report.
goneReportInterruptedTarget :: Double
goneReportInterruptedTarget = 5

-- | The share of explored schedules in which the cancel lands past the nested settle.
lateCancelTarget :: Double
lateCancelTarget = 5

reachedTag :: String
reachedTag = "reached"

missedTag :: String
missedTag = "missed"

-- | How many generated plans the lifecycle property explores.
planRuns :: Int
planRuns = 800

-- | Explore a scenario's schedules, requiring a share of them to reach the
-- interleaving it names.
scenario :: Plan -> String -> Double -> (Result -> Bool) -> Property
scenario plan label target reached =
  checkCoverage . coverTable label [(reachedTag, target)] $
    exploreScenario
      (\result -> tabulate label [if reached result then reachedTag else missedTag] (judgePlan result))
      (runPlan plan)

spec :: Spec
spec = describe "Batch simulation" $ do
  it "reports an acked sibling when a force-cancel interrupts the report" $
    scenario ackThenSiblingCancel "the cancel landed after the ack" reportInterruptedTarget $ \(_, events, _) ->
      batchThrew events && SuccessK `elem` map snd (reportedKinds events 1)
  it "reports a stolen sibling when a force-cancel interrupts the resolution" $
    scenario unwrittenThenSiblingCancel "the cancel landed inside the resolution" resolutionInterruptedTarget $ \(_, events, _) ->
      batchThrew events && UnavailableK unwrittenRetry `elem` map snd (reportedKinds events 3)
  it "logs a stolen sibling's resolution when a force-cancel lands past its nested settle" $
    scenario unwrittenThenLateCancel "the cancel landed past the nested settle" lateCancelTarget $ \(_, events, _) ->
      batchThrew events && UnavailableK unwrittenRetry `elem` map snd (reportedKinds events 2)
  it "reports a job the handler called gone when a force-cancel interrupts the report" $
    scenario goneThenSiblingCancel "the cancel landed inside the gone report" goneReportInterruptedTarget $ \(_, events, _) ->
      batchThrew events && UnavailableK handlerGone `elem` map snd (reportedKinds events 1)
  it "keeps its invariants over generated plans"
    $ checkCoverage
    $ explorePlans
      planRuns
      ( \result@(plan, events, _) -> cover unfinalizedTarget (any (unfinalized events) (jobsOf plan)) "a job left unfinalized" (judgePlan result)
      )
      (runPlan <$> genPlan)

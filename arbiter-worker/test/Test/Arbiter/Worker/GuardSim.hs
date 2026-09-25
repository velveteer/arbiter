{-# LANGUAGE OverloadedStrings #-}

-- | The heartbeat guard under io-sim. Scenarios script the database and the
-- handlers, then IOSimPOR explores the thread schedules. Generated plans check
-- the same guard against invariants that hold for any plan.
module Test.Arbiter.Worker.GuardSim (spec) where

import Arbiter.Core.Exceptions (JobDeadlineExceeded (..), JobForceCancelled (..), JobGoneException (..))
import Arbiter.Core.HighLevel (SetVisibilityResult (..))
import Arbiter.Core.Job.Types (JobId)
import Control.Concurrent.Class.MonadSTM
  ( TVar
  , atomically
  , newEmptyTMVarIO
  , newTVarIO
  , putTMVar
  , readTMVar
  , readTVarIO
  , stateTVar
  , writeTVar
  )
import Control.Monad (void, when)
import Control.Monad.Class.MonadFork (forkIO)
import Control.Monad.Class.MonadTest (exploreRaces)
import Control.Monad.Class.MonadThrow (MonadCatch (..), MonadMask (..), MonadThrow (..), SomeException, fromException)
import Control.Monad.Class.MonadTime.SI
  ( DiffTime
  , Time (..)
  , UTCTime
  , addTime
  , addUTCTime
  , diffTime
  , getCurrentTime
  , getMonotonicTime
  )
import Control.Monad.Class.MonadTimer.SI (threadDelay)
import Control.Monad.IOSim (IOSim)
import Data.Foldable (for_)
import Data.List (unfoldr)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Maybe (catMaybes, fromMaybe, isJust, listToMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Traversable (for, mapAccumL)
import Test.Hspec (Spec, describe, it)
import Test.QuickCheck
  ( Gen
  , Property
  , checkCoverage
  , choose
  , conjoin
  , counterexample
  , cover
  , elements
  , frequency
  , property
  , vectorOf
  , (===)
  )

import Arbiter.Worker.Heartbeat.Guard
import Arbiter.Worker.Logger (LogLevel (..))
import Test.Arbiter.Worker.Sim
  ( Recorder
  , escaped
  , explorePlans
  , exploreScenario
  , hangExtend
  , neverReturns
  , newRecorder
  , refuseExtend
  , scripted
  , startGuard
  )

-- | What the database says about one job.
data Verdict = Extend | Unchanged | Reclaim | Cancel | Vanish | Suspend
  deriving stock (Eq, Show)

-- | What the scripted database answers an extend with.
data Reply
  = -- | Each listed job's verdict after a delay. Unlisted jobs are extended.
    Answer DiffTime [(JobId, Verdict)]
  | -- | The statement omits every requested job from its response.
    Omit
  | -- | A partial response reports only the listed jobs.
    Partial DiffTime [(JobId, Verdict)]
  | -- | An exception, after a delay.
    Refuse DiffTime
  | -- | Never returns. The timeout interrupts it.
    Hang
  | -- | Never returns and cannot be interrupted, as a blocking driver call.
    HangHard
  | -- | Each listed job's verdict after a delay the timeout cannot interrupt.
    HardDelay DiffTime [(JobId, Verdict)]
  deriving stock (Show)

-- | How a handler ended.
data Outcome
  = Done
  | Gone Text
  | Cancelled
  | Deadline
  | Escaped String
  | Other String
  deriving stock (Eq, Show)

-- | What happened, at what monotonic time. The log is in order of recording.
data Event
  = Registered JobId Time
  | Ended JobId Outcome Time
  | Caught JobId Time
  | Heartbeat JobId Time
  | -- | A statement issued, by its attempt number.
    Issued Int [JobId] Time
  | Finished Int [(JobId, Verdict)] Time
  | -- | The extend call returned or threw.
    Exited Int Time
  | Logged LogLevel [JobId] Time
  | CancelledIds [JobId] [JobId] Time
  deriving stock (Eq, Show)

data Setup = Setup
  { interval :: DiffTime
  , leaseTimeout :: DiffTime
  , maxDuration :: Maybe DiffTime
  , replies :: [Reply]
  -- ^ Consumed in order. The last one repeats.
  , rowLeases :: [(JobId, DiffTime)]
  -- ^ Each row's @not_visible_until@, from the start of the simulation.
  , settleLags :: [DiffTime]
  -- ^ How long each returned statement takes to reach the timers, in landing order.
  -- The last one repeats.
  , logBlocks :: Bool
  -- ^ The log sink never returns.
  }
  deriving stock (Show)

-- | A setup whose rows carry no lease of their own.
plainSetup :: DiffTime -> DiffTime -> Maybe DiffTime -> [Reply] -> Setup
plainSetup every timeout duration answers = Setup every timeout duration answers [] [] False

-- | The guard under test and the scenario's event recorder.
data World s = World
  { worldGuard :: HeartbeatGuard (IOSim s) JobId
  , record :: Recorder s Event
  }

-- | Build the world for a setup. Returns it with the action that reads its log.
world :: Setup -> IOSim s (World s, IOSim s [Event])
world setup = do
  exploreRaces
  epoch <- getCurrentTime
  (recorder, events) <- newRecorder
  script <- newTVarIO (replies setup)
  attempts <- newTVarIO (0 :: Int)
  lags <- newTVarIO (settleLags setup)
  guard <- startGuard (guardConfig setup epoch recorder script attempts lags)
  pure (World guard recorder, events)

-- | Explore every schedule of a scenario and check its event log.
simulate :: Setup -> (forall s. World s -> IOSim s ()) -> ([Event] -> Property) -> Property
simulate setup scenario judge = exploreScenario judge run
  where
    run :: IOSim s [Event]
    run = do
      (w, events) <- world setup
      scenario w
      events

guardConfig
  :: Setup
  -> UTCTime
  -> Recorder s Event
  -> TVar (IOSim s) [Reply]
  -> TVar (IOSim s) Int
  -> TVar (IOSim s) [DiffTime]
  -> GuardConfig (IOSim s) JobId
guardConfig setup epoch recorder script attempts lags =
  GuardConfig
    { configInterval = interval setup
    , configTimeout = leaseTimeout setup
    , configMaxDuration = maxDuration setup
    , configKey = id
    , configLease = \job -> (`addUTCTime` epoch) . realToFrac <$> lookup job (rowLeases setup)
    , configExtend = extend
    , configExtended = atomically (stateTVar lags (scripted 0)) >>= threadDelay
    , configLog = \level jobs _ -> recorder (Logged level jobs) >> when (logBlocks setup) (threadDelay neverReturns)
    , configHeartbeat = \job _ _ -> recorder (Heartbeat job)
    }
  where
    extend jobs = do
      (attempt, reply) <- atomically $ do
        attempt <- stateTVar attempts (\count -> (count, count + 1))
        (,) attempt <$> stateTVar script (scripted (Answer 0 []))
      recorder (Issued attempt jobs)
      answer attempt reply jobs `finally` recorder (Exited attempt)
    answer attempt reply jobs = case reply of
      Answer delay verdicts -> threadDelay delay >> respond attempt [(job, fromMaybe Extend (lookup job verdicts)) | job <- jobs]
      Omit -> respond attempt []
      Partial delay verdicts -> threadDelay delay >> respond attempt [(job, verdict) | job <- jobs, Just verdict <- [lookup job verdicts]]
      Refuse delay -> threadDelay delay >> refuseExtend
      Hang -> hangExtend
      HangHard -> uninterruptibleMask_ (threadDelay neverReturns) >> pure []
      HardDelay delay verdicts -> do
        uninterruptibleMask_ (threadDelay delay)
        respond attempt [(job, fromMaybe Extend (lookup job verdicts)) | job <- jobs]
    respond attempt resolved = do
      recorder (Finished attempt resolved)
      pure (map (uncurry verdictResult) resolved)

verdictResult :: JobId -> Verdict -> SetVisibilityResult
verdictResult job verdict = case verdict of
  Extend -> VisibilityExtended job
  Unchanged -> VisibilityUnchanged job
  Reclaim -> JobReclaimed job 1 2
  Cancel -> JobCancelled job
  Vanish -> JobGone job
  Suspend -> JobSuspended job

-- | Run @body@ under the guard on a thread of its own, recording how it ends.
-- @elapsed@ is how long ago the batch started.
handler :: World s -> JobId -> DiffTime -> IOSim s () -> IOSim s ()
handler = handlerUnder id

-- | 'handler' with the guarded call wrapped in @under@.
handlerUnder :: (IOSim s () -> IOSim s ()) -> World s -> JobId -> DiffTime -> IOSim s () -> IOSim s ()
handlerUnder under w job elapsed = void . forkIO . guarded w under job elapsed (pure [job])

-- | Run @body@ under the guard on the calling thread. @pending@ is the batch's pending set.
guarded :: World s -> (IOSim s () -> IOSim s ()) -> JobId -> DiffTime -> IOSim s [JobId] -> IOSim s () -> IOSim s ()
guarded w under job elapsed pending body = do
  start <- addUTCTime (negate (realToFrac elapsed)) <$> getCurrentTime
  record w (Registered job)
  outcome <- try (under (guardBatch (worldGuard w) (Batch (job :| []) pending start id) body))
  record w (Ended job (classify outcome))

-- | Register a multi-job batch and record the complete force-cancel payload.
guardedBatch :: World s -> NonEmpty JobId -> IOSim s [JobId] -> IOSim s () -> IOSim s ()
guardedBatch w jobs@(first :| _) pending body = do
  start <- getCurrentTime
  outcome <- try (guardBatch (worldGuard w) (Batch jobs pending start id) body)
  case outcome of
    Left exc -> case fromException exc of
      Just (JobForceCancelled cancelled unavailable) -> record w (CancelledIds cancelled unavailable)
      Nothing -> pure ()
    Right () -> pure ()
  record w (Ended first (classify outcome))

classify :: Either SomeException () -> Outcome
classify (Right ()) = Done
classify (Left exc)
  | Just (JobGoneException reason _) <- fromException exc = Gone reason
  | Just JobForceCancelled {} <- fromException exc = Cancelled
  | Just JobDeadlineExceeded {} <- fromException exc = Deadline
  | Just message <- escaped exc = Escaped message
  | otherwise = Other (show exc)

-- | A handler body that swallows every signal and keeps running.
swallowing :: World s -> JobId -> DiffTime -> IOSim s ()
swallowing w job remaining = do
  from <- getMonotonicTime
  threadDelay remaining `catch` \(_ :: SomeException) -> do
    record w (Caught job)
    now <- getMonotonicTime
    swallowing w job (remaining - (now `diffTime` from))

endings :: [Event] -> [(JobId, Outcome, Time)]
endings events = [(job, outcome, at) | Ended job outcome at <- events]

within :: Time -> Time -> Time -> Bool
within low high at = low <= at && at <= high

-- | Within a retry pause after @from@.
withinPause :: Time -> Time -> Bool
withinPause from = within from (addTime minRetryPause from)

-- | The list has exactly this many elements.
is :: [a] -> Int -> Property
is items count = length items === count

-- | Each statement issued while others ran: its attempt, the attempts still running, and when.
overlaps :: [Event] -> [(Int, [Int], Time)]
overlaps = catMaybes . snd . mapAccumL step Set.empty
  where
    step running event = case event of
      Issued attempt _ at ->
        (Set.insert attempt running, if Set.null running then Nothing else Just (attempt, Set.toList running, at))
      Exited attempt _ -> (Set.delete attempt running, Nothing)
      _ -> (running, Nothing)

-- ---------------------------------------------------------------------------
-- Generated plans
-- ---------------------------------------------------------------------------

-- | One handler in a generated plan.
data BatchPlan = BatchPlan
  { planJob :: JobId
  , startsAt :: DiffTime
  -- ^ When the handler is forked.
  , elapsed :: DiffTime
  -- ^ How long the batch had run before it registered.
  , claimGap :: DiffTime
  -- ^ How long before the batch started that the claim leased its row.
  , runs :: DiffTime
  , acksAt :: Maybe DiffTime
  -- ^ When the body settles its job, emptying the pending set.
  , swallows :: Bool
  -- ^ The body catches every signal and keeps running.
  }
  deriving stock (Show)

data Plan = Plan
  { planSetup :: Setup
  , planBatches :: [BatchPlan]
  }
  deriving stock (Show)

genPlan :: Gen Plan
genPlan = do
  count <- choose (1, 3)
  batches <- for [1 .. count] genBatch
  -- A row the extend left un-leased only where no handler settles, so the judge's lease
  -- model stays exact.
  let verdicts = if any (isJust . acksAt) batches then movedVerdicts else Unchanged : Suspend : movedVerdicts
  setup <-
    Setup
      <$> elements [0.5, 1, 2]
      <*> elements [2, 3, 4]
      <*> frequency [(2, pure Nothing), (1, Just <$> elements [1, 2.5])]
      <*> frequency [(1, pure [Answer 0 []]), (2, choose (1, 4) >>= (`vectorOf` genReply verdicts))]
      <*> pure []
      <*> pure []
      <*> pure False
  pure (Plan setup {rowLeases = [(planJob b, rowLeaseAt setup b) | b <- batches]} batches)

-- | The row's @not_visible_until@: the claim leased it before the batch started.
rowLeaseAt :: Setup -> BatchPlan -> DiffTime
rowLeaseAt setup batch = startsAt batch - elapsed batch - claimGap batch + leaseTimeout setup

genBatch :: JobId -> Gen BatchPlan
genBatch job = do
  runFor <- elements [0.002, 1, 2.5, 4, 8]
  ack <- frequency [(3, pure Nothing), (1, Just <$> elements [0.3, 1.2])]
  BatchPlan job
    <$> elements [0, 0.5, 1, 1.5]
    <*> frequency [(4, pure 0), (2, pure 0.5), (1, pure 1.7), (1, pure 5)]
    <*> elements [0, 0.25, 1]
    <*> pure runFor
    <*> pure (ack >>= \at -> if at < runFor then Just at else Nothing)
    <*> frequency [(4, pure False), (1, pure True)]

-- | The verdicts for a row the extend moved.
movedVerdicts :: [Verdict]
movedVerdicts = [Extend, Reclaim, Cancel, Vanish]

genReply :: [Verdict] -> Gen Reply
genReply verdicts =
  frequency
    [ (6, Answer <$> elements [0, 0.13, 0.5] <*> genVerdicts)
    , (2, Refuse <$> elements [0, 0.1])
    , (1, pure Hang)
    , (1, pure HangHard)
    , (1, HardDelay <$> elements [3.37, 5.71] <*> genVerdicts)
    ]
  where
    genVerdicts = choose (0, 2) >>= (`vectorOf` genVerdict)
    genVerdict = (,) <$> choose (1, 3) <*> elements verdicts

-- | Run a plan: fork each handler at its start, then wait past every lease.
runPlan :: Plan -> IOSim s (Plan, [Event])
runPlan plan = do
  (w, events) <- world setup
  for_ (planBatches plan) $ \batch -> void . forkIO $ do
    threadDelay (startsAt batch)
    settled <- newTVarIO False
    let pending = readTVarIO settled >>= \done -> pure [planJob batch | not done]
    guarded w id (planJob batch) (elapsed batch) pending (body w batch settled)
  threadDelay horizon
  (,) plan <$> events
  where
    setup = planSetup plan
    horizon = maximum (0 : [startsAt batch + runs batch | batch <- planBatches plan]) + leaseTimeout setup + 1
    body w batch settled = case acksAt batch of
      Nothing -> run (runs batch)
      Just at -> run at >> atomically (writeTVar settled True) >> run (runs batch - at)
      where
        run duration = if swallows batch then swallowing w (planJob batch) duration else threadDelay duration

-- | A statement the plan issued: when, for which jobs, and what came back.
data Statement = Statement
  { issuedAt :: Time
  , carried :: [JobId]
  , finished :: Maybe (Int, [(JobId, Verdict)])
  -- ^ The log index of the reply and the verdicts. Nothing for a reply after the
  -- next statement issued.
  }

statements :: [Event] -> [Statement]
statements events =
  [ Statement
      at
      jobs
      (listToMaybe [(i, verdicts) | (i, Finished a verdicts _) <- indexed, a == attempt, i > index, i < nextIssue index])
  | (index, Issued attempt jobs at) <- indexed
  ]
  where
    indexed = zip [0 :: Int ..] events
    nextIssue index = fromMaybe (length events) (listToMaybe [i | (i, Issued {}) <- indexed, i > index])

-- | Invariants every plan must keep.
judgePlan :: (Plan, [Event]) -> Property
judgePlan (plan, events) =
  conjoin
    [ counterexample "a signal left the handler boundary asynchronous" (null [() | Ended _ (Escaped _) _ <- events])
    , counterexample "a handler ended with an unexpected exception" (null [() | Ended _ (Other _) _ <- events])
    , counterexample "more than one abandoned extend statement in flight" $
        null [() | (_, running, _) <- overlaps events, length running > 1]
    , counterexample "an extend statement overlapped one that could still be interrupted" $
        and [all (maybe False uninterruptibleReply . replyOf) running | (_, running, _) <- overlaps events]
    , conjoin (map judgeBatch (planBatches plan))
    ]
  where
    setup = planSetup plan
    uninterruptibleReply HangHard = True
    uninterruptibleReply (HardDelay _ _) = True
    uninterruptibleReply _ = False
    timeoutFor = leaseTimeout setup
    indexed = zip [0 :: Int ..] events
    issued = statements events
    replyOf attempt = listToMaybe (drop attempt (unfoldr (Just . scripted (Answer 0 [])) (replies setup)))
    registeredAt job = fromMaybe (Time 0) (listToMaybe [at | Registered j at <- events, j == job])
    endedAt job = [(index, outcome, at) | (index, Ended j outcome at) <- indexed, j == job]

    judgeBatch batch =
      conjoin
        [ counterexample ("batch " <> show job <> " ended twice or never") (length (endedAt job) === 1)
        , conjoin [judgeEnd batch index outcome at | (index, outcome, at) <- endedAt job]
        ]
      where
        job = planJob batch

    judgeEnd batch index outcome at =
      conjoin $
        [ counterexample ("a statement carried job " <> show job <> " after its handler ended") $
            null
              [ ()
              | (i, Issued _ jobs issuedAt') <- indexed
              , i > index
              , job `elem` jobs
              , issuedAt' > at || outcome == Gone leaseExpiredReason
              ]
        , counterexample ("job " <> show job <> " was cancelled without a cancel verdict") $
            outcome /= Cancelled || verdictBefore Cancel
        , counterexample ("job " <> show job <> " was reclaimed without a reclaim verdict") $
            outcome /= Gone reclaimedReason || verdictBefore Reclaim
        , counterexample ("job " <> show job <> " was stopped although every extend landed") $
            not (promptlyExtended && elapsed batch == 0 && maxDuration setup == Nothing) || outcome == Done
        ]
          -- A handler that swallows its signals ends at a later one, or not at all.
          <> [ timed
             | not (swallows batch)
             , timed <-
                 [ counterexample ("job " <> show job <> " was stopped outside its lease window") $
                     outcome /= Gone leaseExpiredReason || withinPause fenceFrom at
                 , counterexample ("job " <> show job <> " outran its deadline") $
                     maybe True (\limit -> at <= addTime limit registered) (maxDuration setup)
                 , counterexample ("job " <> show job <> " hit its deadline at the wrong time") $
                     outcome /= Deadline || Just at == ((`addTime` registered) <$> maxDuration setup)
                 ]
             ]
      where
        job = planJob batch
        registered = registeredAt job
        -- An expired initial lease refuses entry at registration.
        initialLease =
          minimum
            ( addTime (timeoutFor - elapsed batch) registered
                : [Time off | Just off <- [lookup job (rowLeases setup)]]
            )
        renewals =
          [ addTime timeoutFor (issuedAt statement)
          | statement <- issued
          , job `elem` carried statement
          , Just (replyIndex, verdicts) <- [finished statement]
          , replyIndex < index
          , lookup job verdicts == Just Extend
          ]
        lease = maximum (initialLease : renewals)
        -- A lease already gone at registration is fenced at registration.
        fenceFrom = max lease registered
        verdictBefore wanted =
          or
            [ lookup job verdicts == Just wanted
            | statement <- issued
            , Just (replyIndex, verdicts) <- [finished statement]
            , replyIndex < index
            ]
        promptlyExtended = all promptExtend (replies setup)
        promptExtend (Answer 0 verdicts) = all ((== Extend) . snd) verdicts
        promptExtend _ = False

-- | How many generated plans the guard property explores.
planRuns :: Int
planRuns = 250

spec :: Spec
spec = describe "Guard simulation" $ do
  it "renews later batches after an uninterruptible extend times out" $
    simulate
      (plainSetup 0.5 2 Nothing [HardDelay 5 [], Answer 0 []])
      ( \w -> do
          handler w 1 0 (threadDelay 10)
          threadDelay 2.5
          handler w 2 0 (threadDelay 5)
          threadDelay 6
      )
      ( \events ->
          conjoin
            [ [() | (2, Done, _) <- endings events] `is` 1
            , property (not (null [() | Issued _ jobs _ <- events, 2 `elem` jobs]))
            ]
      )
  it "keeps a replacement extend in flight when an abandoned attempt exits" $
    simulate
      (plainSetup 0.5 2 Nothing [HardDelay 5 [], Answer 0.8 []])
      ( \w -> do
          handler w 1 0 (threadDelay 10)
          threadDelay 2.5
          handler w 2 0 (threadDelay 6)
          threadDelay 7
      )
      ( \events ->
          conjoin
            [ [() | (2, Done, _) <- endings events] `is` 1
            , [() | (_, _, at) <- overlaps events, at > Time 5.5] `is` 0
            ]
      )
  it "holds renewal behind a stuck extend while an abandoned one still runs" $
    simulate
      (plainSetup 0.5 2 Nothing [HardDelay 10 [], HardDelay 10 [], Answer 0 []])
      ( \w -> do
          handler w 1 0 (threadDelay 20)
          threadDelay 2.5
          handler w 2 0 (threadDelay 20)
          threadDelay 3.5
          handler w 3 0 (threadDelay 20)
          threadDelay 5
          handler w 4 0 (threadDelay 3)
          threadDelay 4
      )
      ( \events ->
          conjoin
            [ [() | (_, running, _) <- overlaps events, length running > 1] `is` 0
            , [() | Issued _ jobs at <- events, 3 `elem` jobs, at < Time 10] `is` 0
            , [() | (4, Done, _) <- endings events] `is` 1
            ]
      )
  let lateReply verdicts =
        simulate
          (plainSetup 0.5 4 Nothing [Answer 0 verdicts, Answer 0 []]) {rowLeases = [(1, 1)], settleLags = [4.5, 0]}
          ( \w -> do
              handler w 1 0 (threadDelay 10)
              handler w 2 0 (threadDelay 6)
              threadDelay 8
          )
          ( \events ->
              conjoin
                [ [() | (2, Done, at) <- endings events, at == Time 6] `is` 1
                , property (not (null [() | Issued _ jobs at <- events, 2 `elem` jobs, at > Time 5]))
                ]
          )
  it "ignores a late renewal from an abandoned extend" $ lateReply []
  it "ignores a late cancel from an abandoned extend" $ lateReply [(2, Cancel)]
  it "stops a batch whose lease lapses without renewal" $
    simulate (plainSetup 1 2 Nothing [Refuse 0]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1

  it "keeps a batch whose extend lands after the lease" $
    simulate (plainSetup 1 2 Nothing [Answer 0.13 []]) (\w -> handler w 1 1.7 (threadDelay 1) >> threadDelay 3) $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , [() | Heartbeat 1 _ <- events] `is` 1
        ]

  it "keeps a batch whose extend lands as the fence gives up"
    $ simulate
      (plainSetup 1 2 Nothing [Answer 0.995 []]) {settleLags = [0.01]}
      (\w -> handler w 1 0 (threadDelay 3) >> threadDelay 5)
    $ \events -> [() | (1, Done, _) <- endings events] `is` 1

  it "stops a batch whose failed extend's log never returns, at the lease" $
    simulate (plainSetup 1 2 Nothing [Refuse 0]) {logBlocks = True} (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1

  let afterBlockedLog w = do
        handler w 1 0 (threadDelay 10)
        threadDelay 3
        handler w 2 0 (threadDelay 3)
        threadDelay 5
  it "extends a batch registered after a failed extend whose log never returns" $
    simulate (plainSetup 1 2 Nothing [Refuse 0, Answer 0 []]) {logBlocks = True} afterBlockedLog $ \events ->
      [() | (2, Done, _) <- endings events] `is` 1

  it "stops a batch whose landed extend never settles, a settle grace past the give-up"
    $ simulate
      (plainSetup 1 2 Nothing [Answer 0 []]) {settleLags = [neverReturns]}
      (\w -> handler w 1 0 (threadDelay 4) >> threadDelay 5)
    $ \events ->
      [ () | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (addTime settleGrace (Time 2)) at
      ]
        `is` 1

  it "stops a batch whose extend hangs, at the lease" $
    simulate (plainSetup 1 2 Nothing [Hang]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1

  it "stops a batch whose extend cannot be interrupted, at the lease" $
    simulate (plainSetup 1 2 Nothing [HangHard]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1

  it "stops a batch whose rows all read back suspended, at the lease" $
    simulate (plainSetup 1 2 Nothing [Answer 0 [(1, Suspend)]]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      conjoin
        [ [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1
        , [() | Heartbeat 1 _ <- events] `is` 0
        ]

  it "keeps the original lease when every heartbeat finds the row gone" $
    simulate (plainSetup 1 2 Nothing [Answer 0 [(1, Vanish)]]) (\w -> handler w 1 0 (threadDelay 5) >> threadDelay 3) $ \events ->
      conjoin
        [ [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1
        , property (not (null [() | Finished _ [(1, Vanish)] _ <- events]))
        , [() | Heartbeat 1 _ <- events] `is` 0
        ]

  it "recovers from a transient non-renewal before the lease expires"
    $ simulate
      (plainSetup 1 3 Nothing [Answer 0 [(1, Unchanged)], Answer 0 []])
      (\w -> handler w 1 0 (threadDelay 4) >> threadDelay 5)
    $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , property (not (null [() | Finished _ [(1, Unchanged)] _ <- events]))
        , property (not (null [() | Heartbeat 1 _ <- events]))
        ]

  it "keeps a batch when a gone sibling settled during the heartbeat"
    $ simulate
      (plainSetup 1 2 Nothing [Answer 0.5 [(2, Vanish)]])
      ( \w -> do
          settled <- newTVarIO False
          void . forkIO $ threadDelay 1.2 >> atomically (writeTVar settled True)
          guardedBatch w (1 :| [2]) (readTVarIO settled >>= \done -> pure (if done then [1] else [1, 2])) (threadDelay 3)
          threadDelay 1
      )
    $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , property (any (\case Finished _ [(1, Extend), (2, Vanish)] at -> at == Time 1.5; _ -> False) events)
        , [() | Heartbeat 2 _ <- events] `is` 0
        ]

  it "stops a batch when the extend response omits its job" $
    simulate (plainSetup 1 2 Nothing [Omit]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      conjoin
        [ [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1
        , [() | Heartbeat 1 _ <- events] `is` 0
        ]

  it "keeps the batch's old lease when one of two results is missing"
    $ simulate
      (plainSetup 1 2 Nothing [Partial 0 [(1, Extend)]])
      (\w -> guardedBatch w (1 :| [2]) (pure [1, 2]) (threadDelay 4) >> threadDelay 1)
    $ \events ->
      conjoin
        [ [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1
        , property (not (null [() | Heartbeat 1 _ <- events]))
        , [() | Heartbeat 2 _ <- events] `is` 0
        ]

  it "ignores a missing result when a sibling settles during the extend"
    $ simulate
      (plainSetup 1 2 Nothing [Partial 0.5 [(1, Extend)]])
      ( \w -> do
          settled <- newTVarIO False
          void . forkIO $ threadDelay 1.2 >> atomically (writeTVar settled True)
          guardedBatch w (1 :| [2]) (readTVarIO settled >>= \done -> pure (if done then [1] else [1, 2])) (threadDelay 3)
          threadDelay 1
      )
    $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , [() | Heartbeat 2 _ <- events] `is` 0
        , property (not (null [() | Heartbeat 1 _ <- events]))
        , property (any (\case Finished _ [(1, Extend)] at -> at == Time 1.5; _ -> False) events)
        ]

  it "ignores a reclaim verdict for a settled sibling"
    $ simulate
      (plainSetup 1 3 Nothing [Answer 0.5 [(2, Reclaim)]])
      ( \w -> do
          settled <- newTVarIO False
          void . forkIO $ threadDelay 1.2 >> atomically (writeTVar settled True)
          guardedBatch w (1 :| [2]) (readTVarIO settled >>= \done -> pure (if done then [1] else [1, 2])) (threadDelay 2)
          threadDelay 1
      )
    $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , property (any (\case Finished _ [(1, Extend), (2, Reclaim)] at -> at == Time 1.5; _ -> False) events)
        ]

  it "does not heartbeat a settled sibling in a renewed batch"
    $ simulate
      (plainSetup 1 3 Nothing [Answer 0.5 []])
      ( \w -> do
          settled <- newTVarIO False
          void . forkIO $ threadDelay 1.2 >> atomically (writeTVar settled True)
          guardedBatch w (1 :| [2]) (readTVarIO settled >>= \done -> pure (if done then [1] else [1, 2])) (threadDelay 2)
          threadDelay 1
      )
    $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , property (any (\case Finished _ [(1, Extend), (2, Extend)] at -> at == Time 1.5; _ -> False) events)
        , property (not (null [() | Heartbeat 1 _ <- events]))
        , [() | Heartbeat 2 _ <- events] `is` 0
        ]

  it "passes both cancelled and reclaimed siblings to a multi-job handler"
    $ simulate
      (plainSetup 1 3 Nothing [Answer 0 [(1, Cancel), (2, Reclaim)]])
      (\w -> guardedBatch w (1 :| [2]) (pure [1, 2]) (threadDelay 4) >> threadDelay 1)
    $ \events ->
      conjoin
        [ [() | CancelledIds [1] [2] _ <- events] `is` 1
        , [() | (1, Cancelled, _) <- endings events] `is` 1
        , [() | Heartbeat _ _ <- events] `is` 0
        ]

  let judgeMultiBatch (second, settlesDuring, events) =
        checkCoverage
          . cover 10 (second == Nothing) "missing sibling"
          . cover 10 (second == Just Unchanged) "unchanged sibling"
          . cover 10 (second == Just Extend) "renewed sibling"
          . cover 10 (second == Just Reclaim) "reclaimed sibling"
          . cover 20 settlesDuring "settled during extension"
          $ conjoin
            [ property (not (null [() | Issued _ [1, 2] _ <- events]))
            , if second == Just Reclaim && not settlesDuring
                then [() | (1, Gone reason, _) <- endings events, reason == reclaimedReason] `is` 1
                else
                  if not settlesDuring && second /= Just Extend
                    then [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1
                    else [() | (1, Done, _) <- endings events] `is` 1
            , if second == Just Extend && not settlesDuring
                then property (not (null [() | Heartbeat 2 _ <- events]))
                else [() | Heartbeat 2 _ <- events] `is` 0
            ]
  it "keeps the lease only when every job in a generated batch is renewed" $
    explorePlans 100 judgeMultiBatch $ do
      second <- elements [Nothing, Just Unchanged, Just Extend, Just Reclaim]
      settlesDuring <- elements [False, True]
      lag <- elements [0.2, 0.5]
      let response = (1, Extend) : [(2, verdict) | Just verdict <- [second]]
          run = do
            (w, events) <- world (plainSetup 1 2 Nothing [Partial lag response])
            settled <- newTVarIO False
            when settlesDuring . void . forkIO $ threadDelay 1.1 >> atomically (writeTVar settled True)
            guardedBatch w (1 :| [2]) (readTVarIO settled >>= \done -> pure (if done then [1] else [1, 2])) (threadDelay 2.75)
            (,,) second settlesDuring <$> events
      pure run

  it "takes the initial lease from the row, not from the batch start" $
    simulate (Setup 5 20 Nothing [Refuse 0] [(1, 3)] [] False) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 6) $ \events ->
      [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 3) at] `is` 1

  it "takes the batch start where it precedes the row's own lease" $
    simulate (Setup 5 20 Nothing [Refuse 0] [(1, 9)] [] False) (\w -> handler w 1 17 (threadDelay 10) >> threadDelay 6) $ \events ->
      [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 3) at] `is` 1

  it "refuses to enter a handler with an expired row lease"
    $ simulate
      (Setup 5 20 Nothing [Answer 0 [(1, Reclaim)]] [(1, -3)] [] False)
      (\w -> handler w 1 0 (record w (Caught 1)) >> threadDelay 4)
    $ \events ->
      conjoin
        [ [() | (1, Gone reason, _) <- endings events, reason == leaseExpiredReason] `is` 1
        , [() | Caught 1 _ <- events] `is` 0
        ]

  it "refuses the whole batch when one sibling's row lease expired before registration"
    $ simulate
      (Setup 1 10 Nothing [Answer 0 []] [(1, 8), (2, -1)] [] False)
      (\w -> guardedBatch w (1 :| [2]) (pure [1, 2]) (record w (Caught 1)))
    $ \events ->
      conjoin
        [ [() | (1, Gone reason, _) <- endings events, reason == leaseExpiredReason] `is` 1
        , [() | Caught 1 _ <- events] `is` 0
        ]

  it "cannot be held past the lease by a chain of failing extends" $
    simulate (plainSetup 0.01 2 Nothing [Refuse 0]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      [() | (1, Gone _, at) <- endings events, withinPause (Time 2) at] `is` 1

  it "beats again at its interval once a failed extend lands" $
    simulate (plainSetup 1 4 Nothing [Refuse 0, Answer 0 []]) (\w -> handler w 1 0 (threadDelay 5) >> threadDelay 6) $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , property (length [() | Heartbeat 1 _ <- events] >= 3)
        ]

  it "does not report a heartbeat for a job settled during the extend"
    $ simulate
      (plainSetup 1 4 Nothing [Answer 0.5 []])
      ( \w -> do
          settled <- newTVarIO False
          handlerUnder id w 1 0 (threadDelay 3)
          handlerUnder id w 2 0 (threadDelay 3)
          guarded w id 3 0 (readTVarIO settled >>= \done -> pure [3 | not done]) $ do
            threadDelay 1.2
            atomically (writeTVar settled True)
            threadDelay 1.8
          threadDelay 2
      )
    $ \events ->
      conjoin
        [ [() | (3, Done, _) <- endings events] `is` 1
        , [() | Heartbeat 3 _ <- events] `is` 0
        ]

  it "does not start a heartbeat hook after the job settles while its context is captured"
    $ simulate
      (plainSetup 1 4 Nothing [Answer 0 []])
      ( \w -> do
          settled <- newTVarIO False
          inheritGate <- newEmptyTMVarIO
          started <- newEmptyTMVarIO
          let pending = readTVarIO settled >>= \done -> pure [1 | not done]
              inherit hook = atomically (putTMVar started ()) >> atomically (readTMVar inheritGate) >> hook
          void . forkIO $ do
            start <- getCurrentTime
            outcome <- try (guardBatch (worldGuard w) (Batch (1 :| []) pending start inherit) (threadDelay 3))
            record w (Ended 1 (classify outcome))
          atomically (readTMVar started)
          atomically (writeTVar settled True >> putTMVar inheritGate ())
          threadDelay 4
      )
    $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , [() | Heartbeat 1 _ <- events] `is` 0
        ]

  let reclaimed w = handler w 1 0 (threadDelay 3) >> handler w 2 0 (threadDelay 3) >> threadDelay 5
  it "stops only the batch another worker reclaimed" $
    simulate (plainSetup 1 20 Nothing [Answer 0 [(1, Reclaim)]]) reclaimed $ \events ->
      conjoin
        [ [() | (1, Gone reason, at) <- endings events, reason == reclaimedReason, at == Time 1] `is` 1
        , [() | (2, Done, at) <- endings events, at == Time 3] `is` 1
        , [() | Heartbeat 1 _ <- events] `is` 0
        , property (length [() | Heartbeat 2 _ <- events] >= 2)
        ]

  it "signals a cancelled batch again each beat while it runs" $
    simulate (plainSetup 1 20 Nothing [Answer 0 [(1, Cancel)]]) (\w -> handler w 1 0 (swallowing w 1 10) >> threadDelay 4.5) $ \events ->
      property (length [() | Caught 1 _ <- events] >= 3)

  it "revokes a signal a masked handler outlives"
    $ simulate
      (plainSetup 10 20 (Just 1) [])
      ( \w -> handlerUnder (\call -> mask_ (call >> threadDelay 1)) w 1 0 (uninterruptibleMask_ (threadDelay 3)) >> threadDelay 6
      )
    $ \events ->
      [() | (1, Done, at) <- endings events, at == Time 4] `is` 1

  it "delivers one signal when the lease and the deadline pass together" $
    simulate (plainSetup 10 1 (Just 1) [Refuse 0]) (\w -> handler w 1 0 (swallowing w 1 1.5) >> threadDelay 3) $ \events ->
      conjoin
        [ [() | Caught 1 _ <- events] `is` 1
        , [() | (1, Done, _) <- endings events] `is` 1
        ]

  it "keeps a signal due at register inside the handler boundary" $
    simulate (plainSetup 10 1 Nothing []) (\w -> handler w 1 5 (threadDelay 0.002) >> threadDelay 1) $ \events ->
      conjoin
        [ [() | (1, Escaped _, _) <- endings events] `is` 0
        , [() | (1, _, _) <- endings events] `is` 1
        ]

  let lateArrival w = do
        handler w 1 0 (threadDelay 30)
        threadDelay 3.5
        handler w 2 0 (threadDelay 30)
        threadDelay 4.5
  it "fences a batch registered while another batch's extend hangs" $
    simulate (plainSetup 1 20 (Just 2) [Hang]) lateArrival $ \events ->
      [() | (2, Deadline, at) <- endings events, at == Time 5.5] `is` 1

  let crowded w = do
        handler w 1 0 (threadDelay 30)
        threadDelay 2
        handler w 2 18 (threadDelay 30)
        threadDelay 6
  it "fences a batch its lease outruns while another batch's extend holds the slot" $
    simulate (plainSetup 1 20 Nothing [Hang]) crowded $ \events ->
      conjoin
        [ [() | (2, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 4) at] `is` 1
        , [() | Issued _ jobs _ <- events, 2 `elem` jobs] `is` 0
        ]

  let together w = handler w 1 0 (threadDelay 5) >> handler w 2 0 (threadDelay 5) >> threadDelay 8
  it "extends due batches in one statement, one statement at a time" $
    simulate (plainSetup 1 20 Nothing [Answer 0.5 []]) together $ \events ->
      conjoin
        [ overlaps events `is` 0
        , property (and [length jobs == 2 | Issued _ jobs at <- events, at < Time 5])
        , property (length [() | Issued _ _ _ <- events] >= 4)
        ]

  it "keeps its invariants over generated plans" $
    explorePlans planRuns judgePlan (runPlan <$> genPlan)

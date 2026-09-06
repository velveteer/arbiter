{-# LANGUAGE OverloadedStrings #-}

-- | The heartbeat guard under io-sim. Scenarios script the database and the
-- handlers, then IOSimPOR explores the thread schedules. Generated plans check
-- the same guard against invariants that hold for any plan.
module Test.Arbiter.Worker.GuardSim (spec) where

import Arbiter.Core.Exceptions (JobDeadlineExceeded (..), JobForceCancelled (..), JobGoneException (..))
import Arbiter.Core.HighLevel (SetVisibilityResult (..))
import Arbiter.Core.Job.Types (JobId)
import Control.Concurrent.Class.MonadSTM (TVar, atomically, modifyTVar', newTVarIO, readTVarIO, stateTVar, writeTVar)
import Control.Monad (void, when)
import Control.Monad.Class.MonadFork (forkIO)
import Control.Monad.Class.MonadTest (exploreRaces)
import Control.Monad.Class.MonadThrow (MonadCatch (..), MonadMask (..), MonadThrow (..), SomeException, fromException)
import Control.Monad.Class.MonadTime.SI
  ( DiffTime
  , Time (..)
  , addTime
  , addUTCTime
  , diffTime
  , getCurrentTime
  , getMonotonicTime
  )
import Control.Monad.Class.MonadTimer.SI (threadDelay)
import Control.Monad.IOSim (IOSim)
import Data.Foldable (for_)
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Maybe (fromMaybe, isJust, listToMaybe)
import Data.Text (Text)
import Data.Traversable (for)
import Test.Hspec (Spec, describe, it)
import Test.QuickCheck
  ( Gen
  , Property
  , choose
  , conjoin
  , counterexample
  , elements
  , frequency
  , property
  , vectorOf
  , (===)
  )

import Arbiter.Worker.Heartbeat.Guard
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
  | -- | An exception, after a delay.
    Refuse DiffTime
  | -- | Never returns. The timeout interrupts it.
    Hang
  | -- | Never returns and cannot be interrupted, as a blocking driver call.
    HangHard
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
  | Issued [JobId] Time
  | Finished [(JobId, Verdict)] Time
  | Overlap Time
  deriving stock (Eq, Show)

data Setup = Setup
  { interval :: DiffTime
  , leaseTimeout :: DiffTime
  , maxDuration :: Maybe DiffTime
  , replies :: [Reply]
  -- ^ Consumed in order. The last one repeats.
  }
  deriving stock (Show)

-- | The guard under test and the scenario's event recorder.
data World s = World
  { worldGuard :: HeartbeatGuard (IOSim s) JobId
  , record :: Recorder s Event
  }

-- | Build the world for a setup. Returns it with the action that reads its log.
world :: Setup -> IOSim s (World s, IOSim s [Event])
world setup = do
  exploreRaces
  (recorder, events) <- newRecorder
  script <- newTVarIO (replies setup)
  inFlight <- newTVarIO (0 :: Int)
  guard <- startGuard (guardConfig setup recorder script inFlight)
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

guardConfig :: Setup -> Recorder s Event -> TVar (IOSim s) [Reply] -> TVar (IOSim s) Int -> GuardConfig (IOSim s) JobId
guardConfig setup recorder script inFlight =
  GuardConfig
    { configInterval = interval setup
    , configTimeout = leaseTimeout setup
    , configMaxDuration = maxDuration setup
    , configKey = id
    , configExtend = extend
    , configExtended = pure ()
    , configLog = \_ _ _ -> pure ()
    , configHeartbeat = \job _ _ -> recorder (Heartbeat job)
    }
  where
    extend jobs = do
      recorder (Issued jobs)
      running <- atomically (stateTVar inFlight (\count -> (count, count + 1)))
      when (running > 0) (recorder Overlap)
      reply <- atomically (stateTVar script (scripted (Answer 0 [])))
      answer reply jobs `finally` atomically (modifyTVar' inFlight (subtract 1))
    answer reply jobs = case reply of
      Answer delay verdicts -> do
        threadDelay delay
        let resolved = [(job, fromMaybe Extend (lookup job verdicts)) | job <- jobs]
        recorder (Finished resolved)
        pure (map (uncurry verdictResult) resolved)
      Refuse delay -> threadDelay delay >> refuseExtend
      Hang -> hangExtend
      HangHard -> uninterruptibleMask_ (threadDelay neverReturns) >> pure []

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
  -- An unmoved row only where no handler settles, so the judge's lease model stays exact.
  let verdicts = if any (isJust . acksAt) batches then movedVerdicts else Unchanged : movedVerdicts
  setup <-
    Setup
      <$> elements [0.5, 1, 2]
      <*> elements [2, 3, 4]
      <*> frequency [(2, pure Nothing), (1, Just <$> elements [1, 2.5])]
      <*> frequency [(1, pure [Answer 0 []]), (2, choose (1, 4) >>= (`vectorOf` genReply verdicts))]
  pure (Plan setup batches)

genBatch :: JobId -> Gen BatchPlan
genBatch job = do
  runFor <- elements [0.002, 1, 2.5, 4, 8]
  ack <- frequency [(3, pure Nothing), (1, Just <$> elements [0.3, 1.2])]
  BatchPlan job
    <$> elements [0, 0.5, 1, 1.5]
    <*> frequency [(4, pure 0), (2, pure 0.5), (1, pure 1.7), (1, pure 5)]
    <*> pure runFor
    <*> pure (ack >>= \at -> if at < runFor then Just at else Nothing)
    <*> frequency [(4, pure False), (1, pure True)]

-- | The verdicts for a row the extend moved.
movedVerdicts :: [Verdict]
movedVerdicts = [Extend, Reclaim, Cancel, Vanish, Suspend]

genReply :: [Verdict] -> Gen Reply
genReply verdicts =
  frequency
    [ (6, Answer <$> elements [0, 0.13, 0.5] <*> genVerdicts)
    , (2, Refuse <$> elements [0, 0.1])
    , (1, pure Hang)
    , (1, pure HangHard)
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
  -- ^ The log index of the reply and the verdicts.
  }

statements :: [Event] -> [Statement]
statements events = go (zip [0 :: Int ..] events)
  where
    go [] = []
    go ((_, Issued jobs at) : rest) =
      let (before, next) = break (isIssued . snd) rest
          reply = listToMaybe [(index, verdicts) | (index, Finished verdicts _) <- before]
       in Statement at jobs reply : go next
    go (_ : rest) = go rest
    isIssued Issued {} = True
    isIssued _ = False

-- | Invariants every plan must keep.
judgePlan :: (Plan, [Event]) -> Property
judgePlan (plan, events) =
  conjoin
    [ counterexample "a signal left the handler boundary asynchronous" (null [() | Ended _ (Escaped _) _ <- events])
    , counterexample "a handler ended with an unexpected exception" (null [() | Ended _ (Other _) _ <- events])
    , counterexample "two extend statements in flight" (null [() | Overlap _ <- events])
    , conjoin (map judgeBatch (planBatches plan))
    ]
  where
    setup = planSetup plan
    timeoutFor = leaseTimeout setup
    indexed = zip [0 :: Int ..] events
    issued = statements events
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
              | (i, Issued jobs issuedAt') <- indexed
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
        initialLease = addTime (timeoutFor - elapsed batch) registered
        renewals =
          [ addTime timeoutFor (issuedAt statement)
          | statement <- issued
          , job `elem` carried statement
          , Just (replyIndex, verdicts) <- [finished statement]
          , replyIndex < index
          , lookup job verdicts /= Just Unchanged
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
  it "stops a batch whose lease lapses without renewal" $
    simulate (Setup 1 2 Nothing [Refuse 0]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1

  it "keeps a batch whose extend lands after the lease" $
    simulate (Setup 1 2 Nothing [Answer 0.13 []]) (\w -> handler w 1 1.7 (threadDelay 1) >> threadDelay 3) $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , [() | Heartbeat 1 _ <- events] `is` 1
        ]

  it "stops a batch whose extend hangs, at the lease" $
    simulate (Setup 1 2 Nothing [Hang]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1

  it "stops a batch whose extend cannot be interrupted, at the lease" $
    simulate (Setup 1 2 Nothing [HangHard]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      [() | (1, Gone reason, at) <- endings events, reason == leaseExpiredReason, withinPause (Time 2) at] `is` 1

  it "cannot be held past the lease by a chain of failing extends" $
    simulate (Setup 0.01 2 Nothing [Refuse 0]) (\w -> handler w 1 0 (threadDelay 10) >> threadDelay 5) $ \events ->
      [() | (1, Gone _, at) <- endings events, withinPause (Time 2) at] `is` 1

  it "beats again at its interval once a failed extend lands" $
    simulate (Setup 1 4 Nothing [Refuse 0, Answer 0 []]) (\w -> handler w 1 0 (threadDelay 5) >> threadDelay 6) $ \events ->
      conjoin
        [ [() | (1, Done, _) <- endings events] `is` 1
        , property (length [() | Heartbeat 1 _ <- events] >= 3)
        ]

  let reclaimed w = handler w 1 0 (threadDelay 3) >> handler w 2 0 (threadDelay 3) >> threadDelay 5
  it "stops only the batch another worker reclaimed" $
    simulate (Setup 1 20 Nothing [Answer 0 [(1, Reclaim)]]) reclaimed $ \events ->
      conjoin
        [ [() | (1, Gone reason, at) <- endings events, reason == reclaimedReason, at == Time 1] `is` 1
        , [() | (2, Done, at) <- endings events, at == Time 3] `is` 1
        , [() | Heartbeat 1 _ <- events] `is` 0
        , property (length [() | Heartbeat 2 _ <- events] >= 2)
        ]

  it "signals a cancelled batch again each beat while it runs" $
    simulate (Setup 1 20 Nothing [Answer 0 [(1, Cancel)]]) (\w -> handler w 1 0 (swallowing w 1 10) >> threadDelay 4.5) $ \events ->
      property (length [() | Caught 1 _ <- events] >= 3)

  it "revokes a signal a masked handler outlives"
    $ simulate
      (Setup 10 20 (Just 1) [])
      ( \w -> handlerUnder (\call -> mask_ (call >> threadDelay 1)) w 1 0 (uninterruptibleMask_ (threadDelay 3)) >> threadDelay 6
      )
    $ \events ->
      [() | (1, Done, at) <- endings events, at == Time 4] `is` 1

  it "delivers one signal when the lease and the deadline pass together" $
    simulate (Setup 10 1 (Just 0.001) []) (\w -> handler w 1 5 (swallowing w 1 1.5) >> threadDelay 3) $ \events ->
      conjoin
        [ [() | Caught 1 _ <- events] `is` 1
        , [() | (1, Done, _) <- endings events] `is` 1
        ]

  it "keeps a signal due at register inside the handler boundary" $
    simulate (Setup 10 1 Nothing []) (\w -> handler w 1 5 (threadDelay 0.002) >> threadDelay 1) $ \events ->
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
    simulate (Setup 1 20 (Just 2) [Hang]) lateArrival $ \events ->
      [() | (2, Deadline, at) <- endings events, at == Time 5.5] `is` 1

  let together w = handler w 1 0 (threadDelay 5) >> handler w 2 0 (threadDelay 5) >> threadDelay 8
  it "extends due batches in one statement, one statement at a time" $
    simulate (Setup 1 20 Nothing [Answer 0.5 []]) together $ \events ->
      conjoin
        [ [() | Overlap _ <- events] `is` 0
        , property (and [length jobs == 2 | Issued jobs at <- events, at < Time 5])
        , property (length [() | Issued _ _ <- events] >= 4)
        ]

  it "keeps its invariants over generated plans" $
    explorePlans planRuns judgePlan (runPlan <$> genPlan)

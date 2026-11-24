{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Deterministic and saturated guards for the job engine's fixed behaviors.
module Arbiter.Test.StateMachine.Guards
  ( guardSpec
  ) where

import Arbiter.Core.Gates (arbiterGatesTable)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema (jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , JobRead
  , defaultGroupedJob
  , defaultJob
  , primaryKey
  , priority
  , setDedupKey
  )
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations qualified as Ops
import Control.Concurrent (threadDelay)
import Control.Exception (finally)
import Control.Monad (replicateM_, void)
import Data.Foldable (toList, traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (isJust)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID.Types qualified as UUID
import Database.PostgreSQL.Simple (Only (..))
import Database.PostgreSQL.Simple qualified as PG
import Hedgehog.Gen qualified as Gen
import Test.Hspec
import UnliftIO.Async (mapConcurrently, mapConcurrently_)

import Arbiter.Test.Setup (execute_, toQuery)
import Arbiter.Test.StateMachine.Actions
import Arbiter.Test.StateMachine.Oracle
  ( WithConn
  , countQuery
  , driftViolations
  , exactViolations
  , holViolations
  , installHolDetector
  , removeHolDetector
  , truncateHol
  )

-- | Past a 1 s lease.
pastLeaseMicros :: Int
pastLeaseMicros = 2_000_000

-- | Past the 1 s lease of the crashed workers.
crashedLeaseLapseMicros :: Int
crashedLeaseLapseMicros = 1_500_000

-- | Past a 1 s schedule delay.
pastScheduleMicros :: Int
pastScheduleMicros = 1_500_000

-- | The fixed-behavior guards. The runner's registry must declare a queue for
-- 'SMPayload' at the table name.
guardSpec
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> Spec
guardSpec run schema table withConn reset = do
  it "concurrent dedup moves and claims never double-claim a group" $
    withinSecs 120 (serializationGuard @sm run schema table withConn reset)
  it "trigger-maintained group summary stays exact under concurrency without the reaper" $
    withinSecs 120 (concurrentDriftGuard @sm run schema table withConn reset)
  it "exhausted jobs are capped by the claim guard and swept to the DLQ" $
    withinSecs 60 (exhaustionGuard @sm run schema table withConn reset)
  it "rollup tree resumes on child completion and cascades to the DLQ" $
    withinSecs 60 (treeGuard @sm run schema table withConn reset)
  it "ineligible groups do not starve a productive group" $
    withinSecs 60 (starvationGuard @sm run reset)
  it "a fixed workload fully drains under a fair claim loop" $
    withinSecs 60 (progressGuard @sm run schema table withConn reset)
  it "expired-lease grouped job is reclaimable via triggers and after a reaper recompute" $
    withinSecs 60 (reclaimGuard @sm run reset)
  it "the drift oracle accepts an expired lease's past in-flight time" $
    withinSecs 60 (expiredLeaseDriftGuard @sm run schema table withConn reset)
  it "a dedup-replaced job does not carry a stale claim owner" $
    withinSecs 60 (dedupReplaceStaleLeaseGuard @sm run schema table withConn reset)
  it "promote leaves a live lease and a paused schedule alone" $
    withinSecs 60 (promoteLeaseGuard @sm run schema table withConn reset)
  it "abandoned jobs are reclaimed and acked exactly once under concurrent workers" $
    withinSecs 90 (concurrentReclaimGuard @sm run schema table withConn reset)
  it "claims honor priority order" $
    withinSecs 60 (priorityOrderGuard @sm run reset)
  it "a freshly scheduled job becomes claimable when its delay elapses" $
    withinSecs 60 (scheduledDueGuard @sm run reset)
  it "a DLQ'd rollup tree is restored intact on retry and drains" $
    withinSecs 60 (treeRetryFromDLQGuard @sm run schema table withConn reset)
  it "runGated skips within the interval and serializes concurrent callers" $
    withinSecs 60 (runGatedChecks run schema withConn)
  it "the combined rate and concurrency gate admits only up to the tighter cap" $
    withinSecs 60 (combinedGateGuard @sm run schema withConn reset)
  it "a DLQ retry under a cancel-flagged parent is refused and the sweep leaves no orphan" $
    withinSecs 60 (flaggedParentRetryGuard @sm run schema table withConn reset)

-- | Guard for the dedup group-move double-claim. Saturates the hot groups with
-- concurrent dedup moves and claims under the gap-free HOL detector, with the
-- reaper churning. Asserts the detector stayed empty.
serializationGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
serializationGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  withConn $ \conn -> installHolDetector conn schema table
  flip finally (withConn $ \conn -> removeHolDetector conn schema table) $ do
    truncateHol schema table withConn
    let rounds = 800 :: Int
        nActors = 16 :: Int
        actor = replicateM_ rounds $ do
          act <- Gen.sample (genAct ConcurrentMode)
          withRetry (void (run (interpret @sm (Env schema table withConn) act)))
        reaper = replicateM_ (rounds * 2) $ withRetry (run (void (HL.refreshAllGroupsFully @sm)))
    mapConcurrently_ id (reaper : replicate nActors actor)
    hol <- withConn $ \conn -> holViolations conn schema table
    hol `shouldBe` []

-- | Concurrent churn with no reaper, then assert the group summary matches a fresh recompute.
concurrentDriftGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
concurrentDriftGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  let rounds = 300 :: Int
      nActors = 12 :: Int
      actor = replicateM_ rounds $ do
        act <- Gen.sample (Gen.filter (/= AReaper) (genAct ConcurrentMode))
        withRetry (void (run (interpret @sm (Env schema table withConn) act)))
  mapConcurrently_ id (replicate nActors actor)
  driftViolations schema table withConn >>= (`shouldBe` [])

-- | Deterministic guard for the @max_attempts@ machinery. Inserts ungrouped jobs
-- capped at a single attempt, then claim and release cycles them. The claim guard
-- holds attempts at the limit and the reaper sweep moves every exhausted job to
-- the DLQ.
exhaustionGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
exhaustionGuard run schema table withConn reset = do
  reset
  let jobCount = 20 :: Int
  run (replicateM_ jobCount (void (mkInsert @sm id Nothing Nothing 0 (Just 1))))
  -- Claim everything, then release it back. Only the first cycle claims. Later
  -- cycles are no-ops.
  replicateM_ 3 $
    run $ do
      jobs <- HL.claimNextVisibleJobs 100 60 :: sm [JobRead SMPayload]
      traverse_ (void . HL.setVisibilityTimeout 0) jobs
  -- No live job is past its limit.
  exactViolations schema table withConn >>= (`shouldBe` [])
  -- Every exhausted, claimable job lands in the DLQ.
  void (run (Ops.sweepExhaustedJobs schema [table]))
  let tbl = jobQueueTable schema table
  stranded <-
    countQuery withConn $
      "SELECT count(*) FROM "
        <> tbl
        <> " WHERE attempts >= 1 AND NOT suspended"
        <> " AND (not_visible_until IS NULL OR not_visible_until <= NOW())"
  dlqd <- countQuery withConn ("SELECT count(*) FROM " <> jobQueueDLQTable schema table)
  stranded `shouldBe` 0
  dlqd `shouldBe` fromIntegral jobCount

-- | Claim everything visible and ack it, repeatedly, until a claim comes back
-- empty or the round bound is hit.
drainToEmpty
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Int
  -> IO ()
drainToEmpty run = go
  where
    go bound
      | bound <= 0 = pure ()
      | otherwise = do
          jobs <- run (HL.claimNextVisibleJobs 50 60 :: sm [JobRead SMPayload])
          if null jobs
            then pure ()
            else run (traverse_ (void . HL.ackJob) jobs) >> go (bound - 1)

-- | Deterministic liveness guard. A fixed set of jobs across several groups plus
-- ungrouped, at mixed priorities, drains fully under a fair claim and ack loop
-- within a bounded number of rounds.
progressGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
progressGuard run schema table withConn reset = do
  reset
  run $ do
    traverse_
      ( \index ->
          void
            (mkInsert @sm id (Just ("pg-" <> T.pack (show (index `mod` 8)))) Nothing (index `mod` 5) Nothing)
      )
      [1 .. 40 :: Int]
    traverse_
      (\index -> void (mkInsert @sm id Nothing Nothing (index `mod` 5) Nothing))
      [1 .. 20 :: Int]
  drainToEmpty @sm run 300
  remaining <- withConn $ \conn -> do
    [Only count] <- PG.query_ conn (toQuery ("SELECT count(*) FROM " <> jobQueueTable schema table))
    pure (count :: Int64)
  remaining `shouldBe` 0

-- | Deterministic guard against group starvation. Ineligible groups in backoff
-- or suspended drop out of the ready ranking. A regression makes the productive
-- claim come back empty.
starvationGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
starvationGuard run reset = do
  let crowders = [1 .. 30 :: Int]
      grp prefix index = prefix <> T.pack (show index)
      ins group lbl = void (run (HL.insertJob (defaultGroupedJob group (smPayload lbl))))
      claim1 = run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
  -- Backoff-blocked crowders. Each fails its head into a long backoff with a
  -- ready successor queued behind it.
  reset
  traverse_ (\index -> ins (grp "boff-" index) (grp "boff-" index <> "-head")) crowders
  replicateM_ (length crowders) $ do
    jobs <- claim1
    run (traverse_ (void . HL.updateJobForRetry 3600 "boom") jobs)
  traverse_ (\index -> ins (grp "boff-" index) (grp "boff-" index <> "-succ")) crowders
  ins "productive-g" "productive"
  behindBackoff <- claim1
  length behindBackoff `shouldBe` 1
  -- Suspended crowders. They are not claimable and leave the ready ranking.
  reset
  traverse_
    ( \index -> do
        inserted <- run (HL.insertJob (defaultGroupedJob (grp "susp-" index) (smPayload (grp "susp-" index))))
        run (traverse_ (void . HL.suspendJob @SMPayload . primaryKey) inserted)
    )
    crowders
  ins "productive-g" "productive"
  behindSuspended <- claim1
  length behindSuspended `shouldBe` 1

-- | Deterministic guard for the rollup-tree lifecycle. Part 1: a finalizer over
-- two children resumes and completes once both children are acked. Part 2:
-- DLQ'ing the parent cascades every tree member to the DLQ.
treeGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
treeGuard run schema table withConn reset = do
  let tbl = jobQueueTable schema table
      mainCount = countQuery withConn ("SELECT count(*) FROM " <> tbl)
      dlqCount = countQuery withConn ("SELECT count(*) FROM " <> jobQueueDLQTable schema table)
      mkJob lbl = defaultJob (smPayload lbl)
      twoChildTree = mkJob "tg parent" <~~ (mkJob "tg child 0" :| [mkJob "tg child 1"])
  -- Part 1: acking both children resumes the parent, which then completes.
  reset
  run (mkInsertTree @sm Nothing 2)
  drainToEmpty @sm run 10
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 0)
  -- Part 2: DLQ'ing the suspended parent cascades the whole tree to the DLQ.
  reset
  Right (parent :| _) <- run (HL.insertJobTree @SMPayload twoChildTree)
  void (run (HL.moveToDLQ "tree guard cascade" parent))
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 3)

-- | A dedup-replaced job is fresh and carries no stale claim owner. A grouped
-- job is claimed with a 1s lease and abandoned, then dedup-replaced once the
-- lease expires. The replaced row has no claimant.
dedupReplaceStaleLeaseGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
dedupReplaceStaleLeaseGuard run schema table withConn reset = do
  reset
  let job = setDedupKey (Just (ReplaceDuplicate "drsl-key")) $ defaultGroupedJob "drslg" (smPayload "drsl")
  void (run (HL.insertJob job))
  _ <- run (HL.claimNextVisibleJobsAs 1 1 (UUID.fromWords 0 0 0 1) :: sm [JobRead SMPayload])
  threadDelay pastLeaseMicros
  void (run (HL.insertJob job))
  stale <-
    countQuery withConn $
      "SELECT count(*) FROM " <> jobQueueTable schema table <> " WHERE dedup_key = 'drsl-key' AND claimed_by IS NOT NULL"
  stale `shouldBe` 0

-- | Deterministic guard for the rows promote refuses: back in flight on a second
-- attempt, released, and suspended with a schedule.
promoteLeaseGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
promoteLeaseGuard run schema table withConn reset = do
  -- Back in flight on a second attempt.
  reset
  jid <- run (mkInsert @sm id Nothing Nothing 0 Nothing)
  _ <- run (HL.claimNextVisibleJobsAs 1 60 smWorker :: sm [JobRead SMPayload])
  run (mkRetry @sm jid 0)
  attempt2 <- run (HL.claimNextVisibleJobsAs 1 60 smWorker :: sm [JobRead SMPayload])
  map primaryKey attempt2 `shouldBe` [jid]
  run (HL.promoteJob @SMPayload jid) >>= (`shouldBe` 0)
  isLiveInFlight schema table withConn jid >>= (`shouldBe` True)
  -- Released with a zero visibility timeout. Still claimed, no window left.
  reset
  rid <- run (mkInsert @sm id Nothing Nothing 0 Nothing)
  released <- run (HL.claimNextVisibleJobsAs 1 60 smWorker :: sm [JobRead SMPayload])
  map primaryKey released `shouldBe` [rid]
  run (traverse (HL.setVisibilityTimeout 0) released) >>= (`shouldBe` [1])
  leaseState schema table withConn rid >>= (`shouldBe` (False, False, Nothing))
  run (HL.promoteJob @SMPayload rid) >>= (`shouldBe` 0)
  -- Suspended with a schedule its resume restores.
  reset
  sid <- run (mkInsert @sm id Nothing (Just 30) 0 Nothing)
  run (HL.suspendJob @SMPayload sid) >>= (`shouldBe` 1)
  (_, _, nvu) <- leaseState schema table withConn sid
  run (HL.promoteJob @SMPayload sid) >>= (`shouldBe` 0)
  (_, _, nvu') <- leaseState schema table withConn sid
  nvu' `shouldBe` nvu

-- | Deterministic guard for crashed-worker recovery. A grouped job claimed with
-- a one-second lease becomes reclaimable once the lease expires, both through the
-- @next_due@ trigger path and after a reaper recompute of @in_flight_until@.
reclaimGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
reclaimGuard run reset = do
  let insClaimExpire group = do
        void (run (mkInsert @sm id (Just group) Nothing 0 Nothing))
        _ <- run (HL.claimNextVisibleJobs 1 1 :: sm [JobRead SMPayload])
        threadDelay pastLeaseMicros
      claim1 = run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
  -- Through the next_due trigger path alone.
  reset
  insClaimExpire "rc-trig"
  viaTrigger <- claim1
  length viaTrigger `shouldBe` 1
  -- After a reaper recompute of in_flight_until.
  reset
  insClaimExpire "rc-reaper"
  void (run (HL.refreshAllGroupsFully @sm))
  viaReaper <- claim1
  length viaReaper `shouldBe` 1

-- | An expired lease leaves a past @in_flight_until@, which every reader treats as
-- free. The drift oracle accepts it.
expiredLeaseDriftGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
expiredLeaseDriftGuard run schema table withConn reset = do
  reset
  void (run (mkInsert @sm id (Just "drift-lapse") Nothing 0 Nothing))
  claimed <- run (HL.claimNextVisibleJobs 1 1 :: sm [JobRead SMPayload])
  length claimed `shouldBe` 1
  threadDelay pastLeaseMicros
  driftViolations schema table withConn >>= (`shouldBe` [])

-- | Deterministic checks for 'Ops.runGated'. A second caller within the interval
-- is skipped. Under concurrency exactly one of many callers wins.
runGatedChecks
  :: forall sm
   . (MonadArbiter sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> WithConn
  -> IO ()
runGatedChecks run schema withConn = do
  let truncateGates =
        withConn $ \conn ->
          execute_ conn ("TRUNCATE " <> arbiterGatesTable schema)
  -- A second call within the interval is skipped.
  truncateGates
  firstRun <- run (Ops.runGated schema "sm-gate" 3600 (pure (1 :: Int)))
  secondRun <- run (Ops.runGated schema "sm-gate" 3600 (pure (2 :: Int)))
  firstRun `shouldBe` Just 1
  secondRun `shouldBe` Nothing
  -- Under concurrency exactly one caller wins the gate.
  truncateGates
  results <- mapConcurrently (const (run (Ops.runGated schema "sm-gate2" 3600 (pure ())))) [1 .. 16 :: Int]
  length (filter isJust results) `shouldBe` 1

-- | Deterministic crash-recovery guard. Jobs claimed with a short lease and
-- abandoned are reclaimed and acked exactly once under concurrent workers, with
-- per-group serialization intact.
concurrentReclaimGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
concurrentReclaimGuard run schema table withConn reset = do
  reset
  withConn $ \conn -> installHolDetector conn schema table
  flip finally (withConn $ \conn -> removeHolDetector conn schema table) $ do
    truncateHol schema table withConn
    let groups = [Just ("crg" <> T.pack (show index)) | index <- [1 .. 5 :: Int]]
        seeds = [(Nothing, prio) | prio <- [0 .. 29 :: Int]] <> [(group, prio) | group <- groups, prio <- [0 .. 1 :: Int]]
    ids <- run (traverse (\(group, prio) -> mkInsert @sm id group Nothing prio Nothing) seeds)
    let total = length ids
    -- Claim with a 1s lease and abandon, simulating crashed workers.
    void (run (HL.claimNextVisibleJobs total 1 :: sm [JobRead SMPayload]))
    threadDelay crashedLeaseLapseMicros
    -- Race workers to reclaim and ack until drained, recording every acked id.
    acked <- newIORef []
    let drain = do
          jobs <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead SMPayload])
          if null jobs
            then pure ()
            else do
              run (traverse_ (void . HL.ackJob) jobs)
              atomicModifyIORef' acked (\acc -> (map primaryKey jobs <> acc, ()))
              drain
    mapConcurrently_ id (replicate 10 drain)
    got <- readIORef acked
    length got `shouldBe` total
    Set.size (Set.fromList got) `shouldBe` total
    hol <- withConn $ \conn -> holViolations conn schema table
    hol `shouldBe` []

-- | Deterministic guard that claims honor priority. Ungrouped jobs come out in
-- ascending priority order, and the grouped candidate ranking serves the lowest
-- min-priority group first.
priorityOrderGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
priorityOrderGuard run reset = do
  let ins group prio = void (mkInsert @sm id group Nothing prio Nothing)
      claimPriorities acc = do
        claimed <- run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
        case claimed of
          [] -> pure (reverse acc)
          jobs -> claimPriorities (map priority jobs <> acc)
  -- Ungrouped: claim order is ascending in priority.
  reset
  run (traverse_ (ins Nothing) [3, 1, 4, 1, 5, 0, 2])
  prs <- claimPriorities []
  prs `shouldBe` map fromIntegral [0, 1, 1, 2, 3, 4, 5 :: Int]
  -- Grouped: the lowest min-priority group is served first.
  reset
  run (ins (Just "pg-hi") 5)
  run (ins (Just "pg-lo") 0)
  claimed <- run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
  map priority claimed `shouldBe` [0]

-- | Deterministic guard. A freshly inserted scheduled job becomes claimable when
-- its delay elapses, for both the grouped and ungrouped paths.
scheduledDueGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
scheduledDueGuard run reset = do
  reset
  void (run (mkInsert @sm id (Just "sched-g") (Just 1) 0 Nothing))
  void (run (mkInsert @sm id Nothing (Just 1) 0 Nothing))
  early <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead SMPayload])
  length early `shouldBe` 0
  threadDelay pastScheduleMicros
  due <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead SMPayload])
  length due `shouldBe` 2

-- | Deterministic guard for the recursive @retryFromDLQ@ restore. A rollup tree
-- cascaded to the DLQ and then retried is restored intact and drains to empty.
treeRetryFromDLQGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
treeRetryFromDLQGuard run schema table withConn reset = do
  let tbl = jobQueueTable schema table
      mainCount = countQuery withConn ("SELECT count(*) FROM " <> tbl)
      dlqCount = countQuery withConn ("SELECT count(*) FROM " <> jobQueueDLQTable schema table)
      mkJob lbl = defaultJob (smPayload lbl)
      tree = mkJob "trd parent" <~~ (mkJob "trd child 0" :| [mkJob "trd child 1"])
  reset
  Right (parent :| _) <- run (HL.insertJobTree @SMPayload tree)
  void (run (HL.moveToDLQ "tree retry guard" parent))
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 3)
  -- Retry the parent's DLQ row. The recursive restore brings the whole tree back.
  dlqId <- withConn $ \conn -> do
    [Only rowId] <-
      PG.query
        conn
        (toQuery ("SELECT id FROM " <> jobQueueDLQTable schema table <> " WHERE job_id = ?"))
        (Only (primaryKey parent))
    pure (rowId :: Int64)
  void (run (HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload))))
  mainCount >>= (`shouldBe` 3)
  dlqCount >>= (`shouldBe` 0)
  drainToEmpty @sm run 10
  mainCount >>= (`shouldBe` 0)

-- | A job carrying both a concurrency slot and a rate-limit key is admitted only
-- up to the tighter cap. Assumes 'smBucket' capacity 3 and the cap-a\/cap-c policies.
combinedGateGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
combinedGateGuard run schema withConn reset = do
  resetSeeded reset schema withConn
  let insBoth conc rate count =
        run
          ( replicateM_
              count
              ( void
                  (mkInsert @sm (applyExtras (Extras (Just conc) (Just rate))) Nothing Nothing 0 Nothing)
              )
          )
      claimN limit = run (HL.claimNextVisibleJobsAs limit 60 smWorker :: sm [JobRead SMPayload])
  -- Concurrency is the tighter gate. cap-a admits 1 and the bucket holds 3.
  insBoth "cap-a" "rk-1" 4
  concCapped <- claimN 10
  length concCapped `shouldBe` 1
  -- Spend rk-2 to one token under the roomy cap-c policy.
  insBoth "cap-c" "rk-2" 2
  spent <- claimN 10
  length spent `shouldBe` 2
  run (traverse_ (void . HL.ackJob) spent)
  -- Ack frees the slots and leaves the tokens spent. Rate now caps the fresh batch.
  insBoth "cap-c" "rk-2" 3
  rateCapped <- claimN 10
  length rateCapped `shouldBe` 1

-- | A child dead-lettered under a rollup parent that is then force-cancelled under a
-- live lease. Its DLQ retry is refused and the reaper's sweep of the lapsed parent
-- leaves no orphan.
flaggedParentRetryGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
flaggedParentRetryGuard run schema table withConn reset = do
  reset
  Right (_ :| [child]) <-
    run (HL.insertJobTree @SMPayload (smJob Nothing "fp parent" <~~ (smJob Nothing "fp child" :| [])))
  claimedChild <- run (HL.claimNextVisibleJobs 1 60) :: IO [JobRead SMPayload]
  map primaryKey claimedChild `shouldBe` [primaryKey child]
  run (traverse_ (void . HL.moveToDLQ "fp dlq") claimedChild)
  parents <- run (HL.claimNextVisibleJobs 1 60) :: IO [JobRead SMPayload]
  run (traverse_ (void . HL.forceCancelJob @SMPayload . primaryKey) parents)
  dlqId <- lookupDlqId schema table withConn (primaryKey child)
  restored <- run (HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload)))
  map primaryKey (toList restored) `shouldBe` []
  lapseLeases schema table withConn
  run (runReaper @sm schema table)
  let tbl = jobQueueTable schema table
  countQuery
    withConn
    ( "SELECT count(*) FROM "
        <> tbl
        <> " child WHERE child.parent_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM "
        <> tbl
        <> " parent WHERE parent.id = child.parent_id)"
    )
    >>= (`shouldBe` 0)

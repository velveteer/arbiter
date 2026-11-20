{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Deterministic lock-order and deadlock regressions for the job engine.
module Arbiter.Test.StateMachine.LockOrder
  ( lockOrderSpec
  ) where

import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyTable)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema (jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Job.Types (JobRead, primaryKey, setNotVisibleUntil)
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.MonadArbiter (withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitsTable)
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar, tryPutMVar)
import Control.Exception (finally, throwIO)
import Control.Monad (replicateM_, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (traverse_)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.List (isInfixOf, sortOn)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock (addUTCTime, getCurrentTime)
import Database.PostgreSQL.Simple (Only (..))
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec
import UnliftIO (tryAny)
import UnliftIO.Async (async, link, mapConcurrently_, wait, withAsync)

import Arbiter.Test.Poll (bumpRef)
import Arbiter.Test.Setup (execute_, toQuery)
import Arbiter.Test.StateMachine.Actions
import Arbiter.Test.StateMachine.Oracle (WithConn, countQuery, firstId)

-- | A pause for the statement to park on the held lock.
parkMicros :: Int
parkMicros = 300_000

-- | A pause for the claim to lock its key before the holder reaches for it.
claimReachMicros :: Int
claimReachMicros = 600_000

-- | The lock-order and deadlock regressions. The runner's registry must declare a
-- queue for 'SMPayload' at the table name.
lockOrderSpec
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> Spec
lockOrderSpec run schema table withConn reset = do
  it "concurrent cross-group operations never deadlock" $
    withinSecs 150 (deadlockGuard @sm run reset)
  it "an ack waking a parent in an earlier group does not deadlock with a multi-group insert" $
    withinSecs 30 (crossGroupWakeLockOrder @sm (void . HL.ackJob) run schema table withConn reset)
  it "a batch ack waking a parent in an earlier group does not deadlock with a multi-group insert" $
    withinSecs 30 (crossGroupWakeLockOrder @sm (void . HL.ackJobsBatch . pure) run schema table withConn reset)
  it "a transaction creating a group row does not deadlock with a multi-group insert" $
    withinSecs 30 (newGroupLockOrder @sm run withConn reset)
  it "a spawn into a later group does not deadlock with a multi-group insert" $
    withinSecs 30 (spawnGroupLockOrder @sm run withConn reset)
  it "a rollup respawn does not deadlock with a delete of its dead-lettered child" $
    withinSecs 30 (respawnDLQDeleteLockOrder @sm run schema table withConn reset)
  it "a DLQ retry re-suspending its parent does not deadlock with a delete of the same row" $
    withinSecs 30 (retryDLQDeleteLockOrder @sm run schema table withConn reset)
  it "a DLQ retry re-suspending its parent does not deadlock with a cancel waking the same parent" $
    withinSecs 30 (retryDLQCancelLockOrder @sm run schema table withConn reset)
  it "a cancel waking a parent in an earlier group does not deadlock with a multi-group insert" $
    withinSecs
      30
      (crossGroupWakeLockOrder @sm (void . HL.cancelJob @SMPayload . primaryKey) run schema table withConn reset)
  it "a dead-letter move waking a parent in an earlier group does not deadlock with a multi-group insert" $
    withinSecs 30 (crossGroupWakeLockOrder @sm (void . HL.moveToDLQ "sm dlq") run schema table withConn reset)
  it "a batch dead-letter move waking a parent in an earlier group does not deadlock with a multi-group insert" $
    withinSecs
      30
      (crossGroupWakeLockOrder @sm (\job -> void (HL.moveToDLQBatch [(job, "sm dlq")])) run schema table withConn reset)
  it "a cascade cancel waking a parent in an earlier group does not deadlock with a multi-group insert" $
    withinSecs
      30
      (crossGroupWakeLockOrder @sm (void . HL.cancelJobCascade @SMPayload . primaryKey) run schema table withConn reset)
  it "a batch cancel waking a parent in an earlier group does not deadlock with a multi-group insert" $
    withinSecs
      30
      (crossGroupWakeLockOrder @sm (void . HL.cancelJobsBatch @SMPayload . pure . primaryKey) run schema table withConn reset)
  it "a flagged-job delete waking a parent in an earlier group does not deadlock with a multi-group insert"
    $ withinSecs 30
    $ crossGroupWakeLockOrderAfter @sm
      (void . HL.forceCancelJob @SMPayload . primaryKey)
      ( \job -> do
          holder <- maybe (liftIO (throwIO (userError "the claimed job names no holder"))) pure (Job.claimedBy job)
          deleted <- Ops.deleteCancelledJobs schema table (Ops.HeldBy holder [job])
          liftIO (deleted `shouldBe` [primaryKey job])
      )
      run
      schema
      table
      withConn
      reset
  it "a batch nack locks its rows descending" $
    withinSecs 30 (descendingSettleLockOrder @sm (void . HL.nackJobsBatch) run schema table withConn reset)
  it "a batch ack locks its rows descending" $
    withinSecs 30 (descendingSettleLockOrder @sm (void . HL.ackJobsBatch) run schema table withConn reset)
  it "a batch dead-letter move locks its rows descending" $
    withinSecs
      30
      (descendingSettleLockOrder @sm (void . HL.moveToDLQBatch . map (,"sm dlq")) run schema table withConn reset)
  it "concurrent spawns, dead-letter moves and cascade cancels never deadlock" $
    withinSecs 150 (spawnDeadlockGuard @sm run schema table withConn reset)
  it "a claim's throttle deferral does not deadlock with an ordered concurrency scan" $
    withinSecs 60 (deferralLockOrderGuard @sm run schema table withConn reset)
  it "a stale-leased job is not deferred while another claimer holds its count row" $
    withinSecs 60 (deferralClaimedByFlipGuard @sm run schema table withConn reset)
  it "a batch cancel leaves a row replaced under the same id while it waited" $
    withinSecs
      30
      (replacedRowLockOrder @sm (HL.cancelJobsBatch @SMPayload . map primaryKey) run schema table withConn reset)
  it "a batch dead-letter move leaves a row replaced under the same id while it waited" $
    withinSecs 30 (replacedRowLockOrder @sm (HL.moveToDLQBatch . map (,"sm dlq")) run schema table withConn reset)
  it "a tree lock holds the group a waited-on job moved to" $
    withinSecs 30 (movedGroupLockOrder @sm run schema table withConn reset)

-- | Test for the cross-group trigger deadlock. Two actors repeatedly run
-- conflicting multi-group operations. One inserts a batch across g1 and g3.
-- The other moves a deduplication key between those groups. The @FOR UPDATE@
-- clauses lock group rows in a consistent order. Count @40P01@ errors.
deadlockGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
deadlockGuard run reset = do
  reset
  deadlocks <- newIORef (0 :: Int)
  let rounds = 600 :: Int
      watch = countingDeadlocks deadlocks
      actorA = replicateM_ rounds $ watch (run (mkBatchInsert @sm [(Just "g1", 0), (Just "g3", 0)]))
      actorB =
        traverse_
          (\index -> watch (run (mkDedup @sm "dlk" True (Just (if even index then "g1" else "g3")) 0)))
          [1 .. rounds]
  mapConcurrently_ id [actorA, actorB]
  deadlockCount <- readIORef deadlocks
  deadlockCount `shouldBe` 0

lockWaitSecs :: Int
lockWaitSecs = 5

lockPollMicros :: Int
lockPollMicros = 10_000

-- | Wait until @count@ backends on this database wait on a lock.
awaitLockWaiters :: WithConn -> Int64 -> IO ()
awaitLockWaiters withConn =
  awaitCount
    withConn
    "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND datname = current_database()"

-- | Wait until a backend waits on a lock the backend @holder@ holds.
awaitBlockedBy :: WithConn -> Int -> IO ()
awaitBlockedBy withConn holder =
  awaitCount
    withConn
    ("SELECT count(*) FROM pg_stat_activity WHERE " <> T.pack (show holder) <> " = ANY(pg_blocking_pids(pid))")
    1

-- | Wait until the count query reaches @count@.
awaitCount :: WithConn -> Text -> Int64 -> IO ()
awaitCount withConn sqlText count = withinSecs lockWaitSecs go
  where
    go = do
      reached <- countQuery withConn sqlText
      when (reached < count) (threadDelay lockPollMicros >> go)

-- | Open the gate when the action ends, so a paused worker finishes before it is cancelled.
releasing :: MVar () -> IO a -> IO a
releasing gate action = action `finally` void (tryPutMVar gate ())

-- | A settle that wakes a parent in an earlier group, against a batch insert across both
-- groups. A held middle group stops the insert between its two group locks.
crossGroupWakeLockOrder
  :: forall sm
   . (ArbiterC sm)
  => (JobRead SMPayload -> sm ())
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
crossGroupWakeLockOrder = crossGroupWakeLockOrderAfter (const (pure ()))

-- | 'crossGroupWakeLockOrder' with @prepare@ run on the claimed child before the insert.
crossGroupWakeLockOrderAfter
  :: forall sm
   . (ArbiterC sm)
  => (JobRead SMPayload -> sm ())
  -> (JobRead SMPayload -> sm ())
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
crossGroupWakeLockOrderAfter prepare settle run schema table withConn reset = do
  reset
  later <- addUTCTime 3600 <$> getCurrentTime
  let scheduled group = setNotVisibleUntil (Just later) (smJob (Just group) "sm scheduled")
  void (run (HL.insertJobTree @SMPayload (smJob (Just "aa") "sm parent" <~~ (smJob (Just "zz") "sm child" :| []))))
  void (run (HL.insertJob (scheduled "mm")))
  [child] <- run (HL.claimNextVisibleJobs 1 60) :: IO [JobRead SMPayload]
  run (prepare child)
  held <- newEmptyMVar
  release <- newEmptyMVar
  let holder = withConn $ \conn -> do
        PG.begin conn
        void
          ( PG.query_
              conn
              (toQuery ("SELECT 1 FROM " <> jobQueueGroupsTable schema table <> " WHERE group_key = 'mm' FOR UPDATE"))
              :: IO [Only Int]
          )
        putMVar held ()
        takeMVar release
        PG.commit conn
      inserter = tryAny (run (void (HL.insertJobsBatch_ [scheduled "aa", scheduled "mm", scheduled "zz"])))
      acker = tryAny (run (settle child))
  withAsync holder $ \holding -> do
    link holding
    takeMVar held
    withAsync inserter $ \insertDone -> releasing release $ do
      awaitLockWaiters withConn 1
      withAsync acker $ \ackDone -> releasing release $ do
        awaitLockWaiters withConn 2
        putMVar release ()
        outcomes <- (,) <$> wait insertDone <*> wait ackDone
        let failures = [show err | Left err <- [fst outcomes, snd outcomes]]
        failures `shouldBe` []

-- | A batch settle over two claimed rows, against a transaction that locks the higher row
-- and then the lower one, as every multi-row statement does.
descendingSettleLockOrder
  :: forall sm
   . (ArbiterC sm)
  => ([JobRead SMPayload] -> sm ())
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
descendingSettleLockOrder settle run schema table withConn reset = do
  reset
  void (run (HL.insertJobsBatch_ [smJob Nothing "sm low", smJob Nothing "sm high"]))
  claimed <- sortOn primaryKey <$> (run (HL.claimNextVisibleJobs 2 60) :: IO [JobRead SMPayload])
  [low, high] <- pure claimed
  let lockRow conn job =
        void
          ( PG.query_
              conn
              ( toQuery
                  ("SELECT 1 FROM " <> jobQueueTable schema table <> " WHERE id = " <> T.pack (show (primaryKey job)) <> " FOR UPDATE")
              )
              :: IO [Only Int]
          )
  held <- newEmptyMVar
  proceed <- newEmptyMVar
  let holder = tryAny . withConn $ \conn -> do
        PG.begin conn
        lockRow conn high
        putMVar held ()
        takeMVar proceed
        lockRow conn low
        PG.commit conn
      settler = tryAny (run (settle claimed))
  withAsync holder $ \holding -> releasing proceed $ do
    takeMVar held
    withAsync settler $ \settleDone -> releasing proceed $ do
      awaitLockWaiters withConn 1
      putMVar proceed ()
      outcomes <- (,) <$> wait holding <*> wait settleDone
      [show err | Left err <- [void (fst outcomes), snd outcomes]] `shouldBe` []

-- | A transaction that creates a group row and then touches a later group, against a batch
-- insert across both. The insert cannot see the uncommitted group row when it locks.
newGroupLockOrder
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> WithConn
  -> IO ()
  -> IO ()
newGroupLockOrder run withConn reset = do
  reset
  later <- addUTCTime 3600 <$> getCurrentTime
  let scheduled group = setNotVisibleUntil (Just later) (smJob (Just group) "sm scheduled")
  void (run (HL.insertJob (scheduled "zz")))
  created <- newEmptyMVar
  proceed <- newEmptyMVar
  let creator = tryAny . run . withDbTransaction $ do
        void (HL.insertJob (scheduled "aa"))
        liftIO (putMVar created () >> takeMVar proceed)
        void (HL.insertJob (scheduled "zz"))
      inserter = tryAny (run (void (HL.insertJobsBatch_ [scheduled "aa", scheduled "zz"])))
  withAsync creator $ \creatorDone -> do
    takeMVar created
    withAsync inserter $ \insertDone -> releasing proceed $ do
      awaitLockWaiters withConn 1
      putMVar proceed ()
      outcomes <- (,) <$> wait creatorDone <*> wait insertDone
      [show err | Left err <- [fst outcomes, snd outcomes]] `shouldBe` []

-- | A spawn whose children land in a later group than the parent, against a batch insert
-- across both groups. The spawn pauses between its children insert and its settle.
spawnGroupLockOrder
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> WithConn
  -> IO ()
  -> IO ()
spawnGroupLockOrder run withConn reset = do
  reset
  later <- addUTCTime 3600 <$> getCurrentTime
  let scheduled group = setNotVisibleUntil (Just later) (smJob (Just group) "sm scheduled")
  void (run (HL.insertJobsBatch_ [smJob (Just "aa") "sm parent", scheduled "zz"]))
  [parent] <- run (HL.claimNextVisibleJobs 1 60) :: IO [JobRead SMPayload]
  spawned <- newEmptyMVar
  proceed <- newEmptyMVar
  let spawner = tryAny . run . withDbTransaction $ do
        void (HL.spawnChildren parent (smChildren (Just "zz") "sm spawn child" 1))
        liftIO (putMVar spawned () >> takeMVar proceed)
        void (HL.ackJob parent)
      inserter = tryAny (run (void (HL.insertJobsBatch_ [scheduled "aa", scheduled "zz"])))
  withAsync spawner $ \spawnDone -> do
    takeMVar spawned
    withAsync inserter $ \insertDone -> releasing proceed $ do
      awaitLockWaiters withConn 1
      putMVar proceed ()
      outcomes <- (,) <$> wait spawnDone <*> wait insertDone
      [show err | Left err <- [fst outcomes, snd outcomes]] `shouldBe` []

-- | A rollup's next spawn round, against a delete of its dead-lettered child. A held parent
-- row stops the spawn after it takes the parent lock and before it detaches the child.
respawnDLQDeleteLockOrder
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
respawnDLQDeleteLockOrder run schema table withConn reset = do
  reset
  void (run (HL.insertJobTree @SMPayload (smJob Nothing "sm parent" <~~ smChildren Nothing "sm child" 2)))
  [failed, done] <- run (HL.claimNextVisibleJobs 2 60) :: IO [JobRead SMPayload]
  void (run (HL.moveToDLQ "sm dlq" failed))
  void (run (HL.ackJob done))
  [parent] <- run (HL.claimNextVisibleJobs 1 60) :: IO [JobRead SMPayload]
  dlqId <- lookupDlqId schema table withConn (primaryKey failed)
  held <- newEmptyMVar
  release <- newEmptyMVar
  let holder = withConn $ \conn -> do
        PG.begin conn
        void
          ( PG.query
              conn
              (toQuery ("SELECT 1 FROM " <> jobQueueTable schema table <> " WHERE id = ? FOR UPDATE"))
              (Only (primaryKey parent))
              :: IO [Only Int]
          )
        putMVar held ()
        takeMVar release
        PG.commit conn
      spawner = tryAny (run (void (HL.spawnChildren parent (smChildren Nothing "sm respawn" 1))))
      deleter = tryAny (run (void (HL.deleteDLQJob @SMPayload dlqId)))
  withAsync holder $ \holding -> do
    link holding
    takeMVar held
    withAsync spawner $ \spawnDone -> releasing release $ do
      awaitLockWaiters withConn 1
      withAsync deleter $ \deleteDone -> releasing release $ do
        awaitLockWaiters withConn 2
        putMVar release ()
        outcomes <- (,) <$> wait spawnDone <*> wait deleteDone
        [show err | Left err <- [fst outcomes, snd outcomes]] `shouldBe` []

-- | A retry of a dead-lettered child that re-suspends its woken parent, against a delete of
-- the same DLQ row. A held parent row stops the delete at its tree lock.
retryDLQDeleteLockOrder
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
retryDLQDeleteLockOrder run schema table withConn reset = do
  reset
  void (run (HL.insertJobTree @SMPayload (smJob Nothing "sm parent" <~~ smChildren Nothing "sm child" 2)))
  [failed, done] <- run (HL.claimNextVisibleJobs 2 60) :: IO [JobRead SMPayload]
  void (run (HL.moveToDLQ "sm dlq" failed))
  void (run (HL.ackJob done))
  dlqId <- lookupDlqId schema table withConn (primaryKey failed)
  held <- newEmptyMVar
  release <- newEmptyMVar
  let holder = withConn $ \conn -> do
        PG.begin conn
        void
          ( PG.query_
              conn
              (toQuery ("SELECT 1 FROM " <> jobQueueTable schema table <> " WHERE parent_id IS NULL FOR UPDATE"))
              :: IO [Only Int]
          )
        putMVar held ()
        takeMVar release
        PG.commit conn
      deleter = tryAny (run (void (HL.deleteDLQJob @SMPayload dlqId)))
      retrier = tryAny (run (void (HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload)))))
  withAsync holder $ \holding -> do
    link holding
    takeMVar held
    withAsync deleter $ \deleteDone -> releasing release $ do
      awaitLockWaiters withConn 1
      withAsync retrier $ \retryDone -> releasing release $ do
        awaitLockWaiters withConn 2
        putMVar release ()
        outcomes <- (,) <$> wait deleteDone <*> wait retryDone
        [show err | Left err <- [fst outcomes, snd outcomes]] `shouldBe` []

-- | A retry of a dead-lettered child that re-suspends its parent, against a cancel of a
-- live sibling that wakes the same parent. A held count row stops the cancel after it
-- locks the parent's group.
retryDLQCancelLockOrder
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
retryDLQCancelLockOrder run schema table withConn reset = do
  resetSeeded reset schema withConn
  let sibling = applyExtras (Extras (Just "cap-a") Nothing) (smJob Nothing "sm sibling")
  void (run (HL.insertJobTree @SMPayload (smJob (Just "gp") "sm parent" <~~ (smJob Nothing "sm failed" :| [sibling]))))
  [failed, live] <-
    sortOn (isJust . smConcSlot . Job.payload) <$> (run (HL.claimNextVisibleJobs 2 60) :: IO [JobRead SMPayload])
  void (run (HL.moveToDLQ "sm dlq" failed))
  dlqId <- lookupDlqId schema table withConn (primaryKey failed)
  held <- newEmptyMVar
  release <- newEmptyMVar
  let holder = withConn $ \conn -> do
        PG.begin conn
        lockConcurrencyKey conn (arbiterConcurrencyTable schema) "cap-a:s"
        putMVar held ()
        takeMVar release
        PG.commit conn
      canceller = tryAny (run (void (HL.cancelJobCascade @SMPayload (primaryKey live))))
      retrier = tryAny (run (void (HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload)))))
  withAsync holder $ \holding -> do
    link holding
    takeMVar held
    withAsync canceller $ \cancelDone -> releasing release $ do
      awaitLockWaiters withConn 1
      withAsync retrier $ \retryDone -> releasing release $ do
        awaitLockWaiters withConn 2
        putMVar release ()
        outcomes <- (,) <$> wait cancelDone <*> wait retryDone
        [show err | Left err <- [fst outcomes, snd outcomes]] `shouldBe` []

-- | Guard for the tree lock order a spawn shares with a dead-letter move and a
-- cascade cancel.
spawnDeadlockGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
spawnDeadlockGuard run schema table withConn reset = do
  reset
  deadlocks <- newIORef (0 :: Int)
  spawns <- newIORef (0 :: Int)
  let rounds = 200 :: Int
      watch = countingDeadlocks deadlocks
      spawnRound = do
        run (mkBatchInsert @sm [(Just "spd", 0), (Nothing, 0)])
        committed <- run (claimSpawnCount @sm (Just "spd") 2)
        atomicModifyIORef' spawns (\count -> (count + committed, ()))
      actorA = replicateM_ rounds (watch spawnRound)
      actorB =
        replicateM_ rounds
          $ watch
          $ run (claimToDLQ @sm >> onRandomRollupParent @sm schema table withConn (void . HL.cancelJobCascade @SMPayload))
  mapConcurrently_ id [actorA, actorB]
  -- A saturated run commits a few hundred spawns. Refuse a run that raced itself idle.
  readIORef spawns >>= (`shouldSatisfy` (>= rounds `div` 2))
  deadlockCount <- readIORef deadlocks
  deadlockCount `shouldBe` 0

-- | Deterministic guard for the claim-trigger lock inversion. A claim's throttle
-- deferral row references a count row the claim never locked. The update trigger
-- leaves that row unlocked.
deferralLockOrderGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
deferralLockOrderGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  let ins conc rate = void (run (mkInsert @sm (applyExtras (Extras conc rate)) Nothing Nothing 0 Nothing))
      tbl = jobQueueTable schema table
      concTbl = arbiterConcurrencyTable schema
  -- The cap-a job is rate-denied on a drained bucket. The claim defers it without
  -- holding cap-a:s. The cap-b:s row parks the ordered scan between cap-a:s and
  -- cap-c:s. The cap-c job is admitted and the claim holds cap-c:s.
  ins (Just "cap-a") (Just "rk-1")
  ins (Just "cap-c") Nothing
  withConn $ \conn -> do
    execute_
      conn
      ("UPDATE " <> arbiterRateLimitsTable schema <> " SET tokens = 0, last_refill = NOW() WHERE rate_limit_key = 'smrl:rk-1'")
    execute_
      conn
      ( "INSERT INTO "
          <> concTbl
          <> " (concurrency_key, concurrency_prefix, in_flight) VALUES ('cap-b:s', 'cap-b', 0) ON CONFLICT (concurrency_key) DO NOTHING"
      )
  held <- newEmptyMVar
  release <- newEmptyMVar
  holder <- async $ withConn $ \conn -> do
    PG.begin conn
    lockConcurrencyKey conn concTbl "cap-b:s"
    putMVar held ()
    takeMVar release
    PG.commit conn
  takeMVar held
  -- The ordered scan locks cap-a:s, then parks on the held cap-b:s.
  reconciler <- async (void (run (HL.reconcileConcurrencyCounts :: sm Int64)))
  threadDelay parkMicros
  claimer <- async (run (HL.claimNextVisibleJobsAs 10 60 smWorker :: sm [JobRead SMPayload]))
  threadDelay parkMicros
  putMVar release ()
  wait holder
  claimedJobs <- wait claimer
  wait reconciler
  length claimedJobs `shouldBe` 1
  countQuery withConn ("SELECT count(*) FROM " <> tbl <> " WHERE throttled_until IS NOT NULL") >>= (`shouldBe` 1)

-- | The claimed_by-flip variant of 'deferralLockOrderGuard'. Deferring a
-- stale-leased job flips claimed_by and the trigger locks its count row. The
-- claim skips the deferral while another claimer holds that row.
deferralClaimedByFlipGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
deferralClaimedByFlipGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  let ins conc rate = void (run (mkInsert @sm (applyExtras (Extras conc rate)) Nothing Nothing 0 Nothing))
      tbl = jobQueueTable schema table
      concTbl = arbiterConcurrencyTable schema
      staleWorker = "00000000-0000-0000-0000-000000000009" :: Text
  -- The cap-a job is a stale lease with a drained bucket. A claim would rate-defer
  -- it with a claimed_by flip. The cap-c job is admissible and the claim holds
  -- cap-c:s.
  ins (Just "cap-a") (Just "rk-1")
  ins (Just "cap-c") Nothing
  withConn $ \conn -> do
    execute_
      conn
      ( "UPDATE "
          <> tbl
          <> " SET claimed_by = '"
          <> staleWorker
          <> "', attempts = 1, not_visible_until = NOW() - interval '1 second' WHERE concurrency_key = 'cap-a:s'"
      )
    execute_
      conn
      ("UPDATE " <> arbiterRateLimitsTable schema <> " SET tokens = 0, last_refill = NOW() WHERE rate_limit_key = 'smrl:rk-1'")
  held <- newEmptyMVar
  -- Hold cap-a:s like a concurrent claimer, then reach for the cap-c:s row the
  -- claim holds.
  holder <- async $ withConn $ \conn -> do
    PG.begin conn
    lockConcurrencyKey conn concTbl "cap-a:s"
    putMVar held ()
    threadDelay claimReachMicros
    lockConcurrencyKey conn concTbl "cap-c:s"
    PG.commit conn
  takeMVar held
  claimedJobs <- run (HL.claimNextVisibleJobsAs 10 60 smWorker :: sm [JobRead SMPayload])
  wait holder
  length claimedJobs `shouldBe` 1
  countQuery withConn ("SELECT count(*) FROM " <> tbl <> " WHERE throttled_until IS NOT NULL") >>= (`shouldBe` 0)
  countQuery withConn ("SELECT count(*) FROM " <> tbl <> " WHERE claimed_by = '" <> staleWorker <> "'") >>= (`shouldBe` 1)

-- | Lock one count row @FOR UPDATE@, like a claimer holding the key.
lockConcurrencyKey :: PG.Connection -> Text -> Text -> IO ()
lockConcurrencyKey conn concTbl key =
  void
    ( PG.query_
        conn
        (toQuery ("SELECT 1 FROM " <> concTbl <> " WHERE concurrency_key = '" <> key <> "' FOR UPDATE"))
        :: IO [Only Int64]
    )

-- | Run one round of a deadlock guard, counting the deadlocks.
countingDeadlocks :: IORef Int -> IO () -> IO ()
countingDeadlocks deadlocks act = do
  outcome <- tryAny act
  case outcome of
    Right () -> pure ()
    Left err
      | isDeadlock err -> bumpRef deadlocks
      | "40001" `isInfixOf` show err -> pure ()
      | otherwise -> throwIO err

-- | Apply an id-keyed op to a random rollup-finalizer parent, if any. A parent
-- is a row with a @parent_state@ snapshot.
onRandomRollupParent
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Text
  -> WithConn
  -> (Int64 -> sm ())
  -> sm ()
onRandomRollupParent schema table withConn act = do
  mId <- liftIO (firstId withConn sql)
  traverse_ act mId
  where
    sql =
      "SELECT id FROM " <> jobQueueTable schema table <> " WHERE parent_state IS NOT NULL ORDER BY random() LIMIT 1"

-- | A batch settle over two rows, against a transaction that replaces the lower row
-- under the same id while the settle waits on it. The settle counts only the other row
-- and the replacement survives.
replacedRowLockOrder
  :: forall sm
   . (ArbiterC sm)
  => ([JobRead SMPayload] -> sm Int64)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
replacedRowLockOrder settle run schema table withConn reset = do
  reset
  void (run (HL.insertJobsBatch_ [smJob Nothing "sm low", smJob Nothing "sm high"]))
  jobs <- sortOn primaryKey <$> (run (HL.claimNextVisibleJobs 2 60) :: IO [JobRead SMPayload])
  lowJob <- maybe (throwIO (userError "no job claimed")) pure (headMay jobs)
  let low = primaryKey lowJob
  let tbl = jobQueueTable schema table
      lowId = T.pack (show low)
  held <- newEmptyMVar
  release <- newEmptyMVar
  let holder = withConn $ \conn -> do
        PG.begin conn
        void (PG.query_ conn (toQuery ("SELECT 1 FROM " <> tbl <> " WHERE id = " <> lowId <> " FOR UPDATE")) :: IO [Only Int])
        PG.query_ conn "SELECT pg_backend_pid()" >>= traverse_ (putMVar held . fromOnly)
        takeMVar release
        void (PG.execute_ conn (toQuery ("DELETE FROM " <> tbl <> " WHERE id = " <> lowId)))
        void
          ( PG.execute_
              conn
              ( toQuery
                  ( "INSERT INTO "
                      <> tbl
                      <> " (id, claim_seq, payload) VALUES ("
                      <> lowId
                      <> ", "
                      <> T.pack (show (Job.claimSeq lowJob))
                      <> ", '{\"smMessage\":\"sm replaced\",\"smConcSlot\":null,\"smRateKey\":null}')"
                  )
              )
          )
        PG.commit conn
      settler = run (settle jobs)
  withAsync holder $ \holding -> do
    link holding
    holderPid <- takeMVar held
    withAsync settler $ \settleDone -> releasing release $ do
      awaitBlockedBy withConn holderPid
      putMVar release ()
      settled <- wait settleDone
      survived <- countQuery withConn ("SELECT count(*) FROM " <> tbl <> " WHERE id = " <> lowId)
      (settled, survived) `shouldBe` (1, 1)
  where
    headMay = foldr (\job _ -> Just job) Nothing

-- | A tree lock that waits on a job a concurrent transaction moves to another group
-- holds the group the job moved to.
movedGroupLockOrder
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> WithConn
  -> IO ()
  -> IO ()
movedGroupLockOrder run schema table withConn reset = do
  reset
  moving <- run (mkInsert @sm id (Just "mg-from") Nothing 0 Nothing)
  void (run (mkInsert @sm id (Just "mg-to") Nothing 0 Nothing))
  let tbl = jobQueueTable schema table
  held <- newEmptyMVar
  release <- newEmptyMVar
  locked <- newEmptyMVar
  done <- newEmptyMVar
  let holder = withConn $ \conn -> do
        PG.begin conn
        void (PG.execute_ conn (toQuery ("UPDATE " <> tbl <> " SET group_key = 'mg-to' WHERE id = " <> T.pack (show moving))))
        PG.query_ conn "SELECT pg_backend_pid()" >>= traverse_ (putMVar held . fromOnly)
        takeMVar release
        PG.commit conn
      locker = run . withDbTransaction $ do
        void (Ops.lockJobTrees schema table [moving])
        liftIO (putMVar locked () >> takeMVar done)
  withAsync holder $ \holding -> do
    link holding
    holderPid <- takeMVar held
    withAsync locker $ \lockDone -> releasing done . releasing release $ do
      awaitBlockedBy withConn holderPid
      putMVar release ()
      takeMVar locked
      grabbed <-
        withConn $ \conn ->
          tryAny
            ( PG.query_
                conn
                (toQuery ("SELECT 1 FROM " <> jobQueueGroupsTable schema table <> " WHERE group_key = 'mg-to' FOR UPDATE NOWAIT"))
                :: IO [Only Int]
            )
      putMVar done ()
      wait lockDone
      either (const (pure ())) (const (expectationFailure "the moved job's new group was not locked")) grabbed

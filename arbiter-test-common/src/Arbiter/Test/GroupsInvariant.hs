{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial -Wno-incomplete-uni-patterns #-}

-- | Groups table invariant tests.
--
-- After every operation, asserts @job_count@, @min_priority@, and @min_id@
-- per group match the main table.
module Arbiter.Test.GroupsInvariant (groupsInvariantSpec, assertGroupsConsistent, assertInFlightConsistent) where

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ (DLQJob (..))
import Arbiter.Core.Job.Types
  ( DedupKey (IgnoreDuplicate, ReplaceDuplicate)
  , Job (..)
  , JobPayload
  , defaultJob
  )
import Arbiter.Core.JobTree (leaf, rollup, (<~~))
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.QueueRegistry (TableForPayload)
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, void, when)
import Data.Foldable (toList)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime, diffUTCTime)
import Database.PostgreSQL.Simple qualified as PG
import GHC.TypeLits (KnownSymbol)
import Test.Hspec
import UnliftIO (MonadUnliftIO)

import Arbiter.Test.Setup (execute_)

-- | Expected state of a single group row.
data GroupState = GroupState
  { gsGroupKey :: Text
  , gsMinPriority :: Int
  , gsMinId :: Int64
  , gsJobCount :: Int64
  }
  deriving stock (Eq, Show)

-- | Read the actual groups table state via raw SQL.
readGroupsTable :: PG.Connection -> Text -> Text -> IO [GroupState]
readGroupsTable conn schemaName tableName = do
  let sql =
        "SELECT group_key, min_priority, min_id, job_count FROM "
          <> schemaName
          <> ".\""
          <> tableName
          <> "_groups\" ORDER BY group_key"
  rows <- PG.query_ conn (fromString (T.unpack sql))
  pure [GroupState gk mp mi jc | (gk, mp, mi, jc) <- rows]

-- | Compute expected groups state from the main queue table.
computeExpectedGroups :: PG.Connection -> Text -> Text -> IO [GroupState]
computeExpectedGroups conn schemaName tableName = do
  let sql =
        "SELECT group_key, MIN(priority)::int, MIN(id)::bigint, COUNT(*)::bigint FROM "
          <> schemaName
          <> ".\""
          <> tableName
          <> "\" WHERE group_key IS NOT NULL GROUP BY group_key ORDER BY group_key"
  rows <- PG.query_ conn (fromString (T.unpack sql))
  pure [GroupState gk mp mi jc | (gk, mp :: Int, mi :: Int64, jc :: Int64) <- rows]

-- | Read the actual @in_flight_until@ from the groups table.
readGroupsInFlight :: PG.Connection -> Text -> Text -> IO (Map.Map Text (Maybe UTCTime))
readGroupsInFlight conn schemaName tableName = do
  let sql =
        "SELECT group_key, in_flight_until FROM "
          <> schemaName
          <> ".\""
          <> tableName
          <> "_groups\" WHERE job_count > 0 ORDER BY group_key"
  rows <- PG.query_ conn (fromString (T.unpack sql))
  pure $ Map.fromList rows

-- | Compute expected @in_flight_until@ from the main table.
--
-- For each group: if any job satisfies @not_visible_until > NOW() AND NOT suspended@,
-- then @in_flight_until@ must equal @MAX(not_visible_until)@ over those jobs.
-- Otherwise @in_flight_until@ must be NULL.
computeExpectedInFlight :: PG.Connection -> Text -> Text -> IO (Map.Map Text (Maybe UTCTime))
computeExpectedInFlight conn schemaName tableName = do
  let sql =
        "SELECT group_key, MAX(not_visible_until) FILTER (WHERE not_visible_until > NOW() AND NOT suspended) FROM "
          <> schemaName
          <> ".\""
          <> tableName
          <> "\" WHERE group_key IS NOT NULL GROUP BY group_key ORDER BY group_key"
  rows <- PG.query_ conn (fromString (T.unpack sql))
  pure $ Map.fromList rows

-- | Assert that the groups table's @in_flight_until@ column is consistent
-- with the main table's actual in-flight state.
--
-- Only valid in settled state (no concurrent activity). For each group with
-- @job_count > 0@: if any job has @not_visible_until > NOW() AND NOT suspended@,
-- then @in_flight_until@ must equal the MAX; otherwise it must be NULL.
assertInFlightConsistent :: PG.Connection -> Text -> Text -> String -> IO ()
assertInFlightConsistent conn schemaName tableName ctx = do
  actual <- readGroupsInFlight conn schemaName tableName
  expected <- computeExpectedInFlight conn schemaName tableName
  forM_ (Map.toList actual) $ \(gk, actualIft) -> do
    let expectedIft = Map.findWithDefault Nothing gk expected
    when (not (iftMatch actualIft expectedIft)) $
      expectationFailure $
        ctx
          <> "\n  in_flight_until mismatch for group '"
          <> T.unpack gk
          <> "'"
          <> "\n  Expected: "
          <> show expectedIft
          <> "\n  Actual: "
          <> show actualIft
  where
    -- NULL matches NULL, and non-NULL must match within 1 second tolerance
    -- (NOW() can drift slightly between the two queries)
    iftMatch Nothing Nothing = True
    iftMatch (Just _) Nothing = False
    iftMatch Nothing (Just _) = False
    iftMatch (Just a) (Just b) = abs (diffUTCTime a b) < 1

-- | Assert that the groups table is consistent with the main table.
--
-- Compares @job_count@, @min_priority@, @min_id@, and @in_flight_until@ per group.
-- Empty groups (@job_count = 0@) are pruned inline by triggers, but
-- filtered here as a safety net for transient race conditions.
assertGroupsConsistent :: PG.Connection -> Text -> Text -> String -> IO ()
assertGroupsConsistent conn schemaName tableName ctx = do
  actual <- readGroupsTable conn schemaName tableName
  expected <- computeExpectedGroups conn schemaName tableName
  let toMap = Map.fromList . map (\g -> (gsGroupKey g, (gsJobCount g, gsMinPriority g, gsMinId g)))
      -- Filter out empty groups from actual — they're harmless leftovers
      actualNonEmpty = Map.filter (\(c, _, _) -> c > 0) (toMap actual)
      expectedFull = toMap expected
  when (actualNonEmpty /= expectedFull) $
    expectationFailure $
      ctx
        <> "\n  Expected (group_key -> (job_count, min_priority, min_id)): "
        <> show expectedFull
        <> "\n  Actual (excluding empty groups): "
        <> show actualNonEmpty
  -- Also check in_flight_until consistency
  assertInFlightConsistent conn schemaName tableName ctx

-- | Spec that exercises every operation and checks groups table invariants.
--
-- @runM@ runs a monadic action (e.g., @runSimpleDb env@).
-- @mkPayload@ constructs a payload from a 'Text' label.
-- @withConn@ obtains a raw 'PG.Connection' for direct SQL queries.
groupsInvariantSpec
  :: forall payload registry env m
   . ( Eq payload
     , HasArbiterSchema m registry
     , JobPayload payload
     , KnownSymbol (TableForPayload payload registry)
     , MonadArbiter m
     , MonadUnliftIO m
     )
  => Text
  -- ^ Schema name
  -> Text
  -- ^ Table name
  -> (Text -> payload)
  -- ^ Payload constructor
  -> (forall a. env -> m a -> IO a)
  -- ^ Runner (e.g., @runSimpleDb env@)
  -> (env -> (PG.Connection -> IO ()) -> IO ())
  -- ^ Raw connection accessor
  -> SpecWith env
groupsInvariantSpec schemaName tableName mkPayload runM withConn = do
  let check env label =
        withConn env $ \conn ->
          assertGroupsConsistent conn schemaName tableName label

  describe "Groups Table Invariants" $ do
    it "insertJob: grouped job creates/updates group row" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "g1-job1")) {groupKey = Just "g1"}
      check env "after insertJob g1"

      void $ runM env $ HL.insertJob (defaultJob (mkPayload "g1-job2")) {groupKey = Just "g1"}
      check env "after insertJob g1 (second)"

      void $ runM env $ HL.insertJob (defaultJob (mkPayload "g2-job1")) {groupKey = Just "g2"}
      check env "after insertJob g2"

    it "insertJob: ungrouped job does not affect groups table" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "ungrouped"))
      check env "after ungrouped insertJob"

    it "insertJobsBatch: batch creates/updates group rows" $ \env -> do
      let jobs =
            [ (defaultJob (mkPayload "b1")) {groupKey = Just "batch-g1"}
            , (defaultJob (mkPayload "b2")) {groupKey = Just "batch-g1"}
            , (defaultJob (mkPayload "b3")) {groupKey = Just "batch-g2"}
            , defaultJob (mkPayload "b4") -- ungrouped
            ]
      void $ runM env $ HL.insertJobsBatch jobs
      check env "after insertJobsBatch"

    it "insertJob: different priorities tracked correctly" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "p-high")) {groupKey = Just "prio-g", priority = 10}
      check env "after high-priority insert"

      void $ runM env $ HL.insertJob (defaultJob (mkPayload "p-low")) {groupKey = Just "prio-g", priority = 1}
      check env "after low-priority insert (min_priority should update)"

    it "ackJob: decrement count, update min values" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "ack1")) {groupKey = Just "ack-g"}
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "ack2")) {groupKey = Just "ack-g"}
      check env "after 2 inserts"

      -- Claim and ack first job
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      forM_ claimed $ \j -> void $ runM env $ HL.ackJob j
      check env "after acking 1 of 2"

      -- Claim and ack second job (group should be empty)
      claimed2 <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      forM_ claimed2 $ \j -> void $ runM env $ HL.ackJob j
      check env "after acking 2 of 2 (group empty)"

    it "cancelJob: decrement count" $ \env -> do
      Just j1 <- runM env $ HL.insertJob (defaultJob (mkPayload "cancel1")) {groupKey = Just "cancel-g"}
      Just j2 <- runM env $ HL.insertJob (defaultJob (mkPayload "cancel2")) {groupKey = Just "cancel-g"}
      check env "after 2 inserts"

      void $ runM env $ HL.cancelJob @m @registry @payload (primaryKey j1)
      check env "after cancelling 1 of 2"

      void $ runM env $ HL.cancelJob @m @registry @payload (primaryKey j2)
      check env "after cancelling 2 of 2 (group empty)"

    it "moveToDLQ: decrement count" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "dlq1")) {groupKey = Just "dlq-g"}
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "dlq2")) {groupKey = Just "dlq-g"}
      check env "after 2 inserts"

      -- Claim so we can DLQ
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      forM_ claimed $ \j -> void $ runM env $ HL.moveToDLQ "test error" j
      check env "after DLQ 1 of 2"

    it "cancelJobsBatch: decrement counts for multiple jobs" $ \env -> do
      Just j1 <- runM env $ HL.insertJob (defaultJob (mkPayload "cb1")) {groupKey = Just "cbatch-g"}
      Just j2 <- runM env $ HL.insertJob (defaultJob (mkPayload "cb2")) {groupKey = Just "cbatch-g"}
      Just j3 <- runM env $ HL.insertJob (defaultJob (mkPayload "cb3")) {groupKey = Just "cbatch-g2"}
      check env "after 3 inserts across 2 groups"

      void $ runM env $ HL.cancelJobsBatch @m @registry @payload (map primaryKey [j1, j2, j3])
      check env "after batch cancel of all 3"

    it "moveToDLQBatch: decrement counts" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "dlqb1")) {groupKey = Just "dlqb-g"}
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "dlqb2")) {groupKey = Just "dlqb-g"}
      check env "after 2 inserts"

      -- Claim both
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 2 60
      let jobsWithErrors = map (\j -> (j, "batch error")) claimed
      void $ runM env $ HL.moveToDLQBatch jobsWithErrors
      check env "after batch DLQ"

    it "ackJobsBulk: decrement counts" $ \env -> do
      void $
        runM env $
          HL.insertJobsBatch
            [ (defaultJob (mkPayload "bulk1")) {groupKey = Just "bulk-g"}
            , (defaultJob (mkPayload "bulk2")) {groupKey = Just "bulk-g"}
            , (defaultJob (mkPayload "bulk3")) {groupKey = Just "bulk-g2"}
            ]
      check env "after batch insert"

      -- Claim all
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 10 60
      void $ runM env $ HL.ackJobsBulk claimed
      check env "after bulk ack"

    it "retryFromDLQ: re-increment count" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "retry1")) {groupKey = Just "retry-g"}
      check env "after insert"

      -- Claim and DLQ
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      forM_ claimed $ \j -> void $ runM env $ HL.moveToDLQ "test error" j
      check env "after DLQ (group empty)"

      -- Get DLQ job and retry
      dlqJobs <- runM env $ HL.listDLQJobs @m @registry @payload 10 0
      forM_ dlqJobs $ \dj -> void $ runM env $ HL.retryFromDLQ @m @registry @payload (dlqPrimaryKey dj)
      check env "after retryFromDLQ (group re-created)"

    it "suspendJob: job_count unchanged" $ \env -> do
      Just j1 <- runM env $ HL.insertJob (defaultJob (mkPayload "susp1")) {groupKey = Just "susp-g"}
      check env "after insert"

      rows <- runM env $ HL.suspendJob @m @registry @payload (primaryKey j1)
      rows `shouldBe` 1
      check env "after suspendJob (should be unchanged)"

    it "resumeJob: job_count unchanged" $ \env -> do
      Just j1 <- runM env $ HL.insertJob (defaultJob (mkPayload "resume1")) {groupKey = Just "resume-g"}
      suspRows <- runM env $ HL.suspendJob @m @registry @payload (primaryKey j1)
      suspRows `shouldBe` 1
      check env "after suspend"

      resRows <- runM env $ HL.resumeJob @m @registry @payload (primaryKey j1)
      resRows `shouldBe` 1
      check env "after resumeJob (should be unchanged)"

    it "promoteJob: job_count unchanged" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "promo1")) {groupKey = Just "promo-g"}
      -- Claim to set not_visible_until
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      forM_ claimed $ \j -> do
        void $ runM env $ HL.updateJobForRetry 30 "retry" j
        check env "after updateJobForRetry"
        void $ runM env $ HL.promoteJob @m @registry @payload (primaryKey j)
        check env "after promoteJob (should be unchanged)"

    it "setVisibilityTimeout: job_count unchanged" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "vis1")) {groupKey = Just "vis-g"}
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      check env "after claim"
      forM_ claimed $ \j -> do
        void $ runM env $ HL.setVisibilityTimeout 120 j
        check env "after setVisibilityTimeout (should be unchanged)"

    it "cancelJobCascade: decrement counts for parent and children" $ \env -> do
      let parent = (defaultJob (mkPayload "cascade-parent")) {groupKey = Just "cascade-g"}
          child1 = (defaultJob (mkPayload "cascade-c1")) {groupKey = Just "cascade-g"}
          child2 = (defaultJob (mkPayload "cascade-c2")) {groupKey = Just "cascade-g2"}
      Right inserted <- runM env $ HL.insertJobTree @m @registry (parent <~~ (child1 :| [child2]))
      check env "after insertJobTree"

      let parentJob = head $ filter (\j -> payload j == mkPayload "cascade-parent") (toList inserted)
      void $ runM env $ HL.cancelJobCascade @m @registry @payload (primaryKey parentJob)
      check env "after cancelJobCascade (all groups should reflect deletions)"

    it "insertJobTree with rollup: groups track all tree members" $ \env -> do
      let finalizer = (defaultJob (mkPayload "rollup-fin")) {groupKey = Just "tree-g"}
          c1 = (defaultJob (mkPayload "rollup-c1")) {groupKey = Just "tree-g"}
          c2 = (defaultJob (mkPayload "rollup-c2")) {groupKey = Just "tree-g2"}
      Right _ <- runM env $ HL.insertJobTree @m @registry (rollup finalizer (leaf c1 :| [leaf c2]))
      check env "after rollup insertJobTree"

    it "dedup IgnoreDuplicate: no over-count on conflict" $ \env -> do
      let job1 = (defaultJob (mkPayload "dedup-ign")) {groupKey = Just "dedup-g", dedupKey = Just (IgnoreDuplicate "dup-key-1")}
      void $ runM env $ HL.insertJob job1
      check env "after first insert"

      -- Second insert with same dedup key should be ignored
      void $ runM env $ HL.insertJob job1
      check env "after duplicate IgnoreDuplicate (should be unchanged)"

    it "dedup ReplaceDuplicate: no over-count on replace" $ \env -> do
      let job1 = (defaultJob (mkPayload "dedup-rep1")) {groupKey = Just "dedup-rep-g", dedupKey = Just (ReplaceDuplicate "dup-key-2")}
      void $ runM env $ HL.insertJob job1
      check env "after first insert"

      -- Second insert with same dedup key replaces
      let job2 = (defaultJob (mkPayload "dedup-rep2")) {groupKey = Just "dedup-rep-g", dedupKey = Just (ReplaceDuplicate "dup-key-2")}
      void $ runM env $ HL.insertJob job2
      check env "after ReplaceDuplicate (count should still be 1)"

    it "dedup ReplaceDuplicate with group_key change: old group decremented" $ \env -> do
      let job1 =
            (defaultJob (mkPayload "dedup-move1")) {groupKey = Just "dedup-old-g", dedupKey = Just (ReplaceDuplicate "dup-key-3")}
      void $ runM env $ HL.insertJob job1
      check env "after first insert in old group"

      -- Replace with different group_key
      let job2 =
            (defaultJob (mkPayload "dedup-move2")) {groupKey = Just "dedup-new-g", dedupKey = Just (ReplaceDuplicate "dup-key-3")}
      void $ runM env $ HL.insertJob job2
      check env "after ReplaceDuplicate to new group (old group should be empty, new group count = 1)"

    it "cascade children to DLQ: groups updated for cascaded children" $ \env -> do
      let parent = (defaultJob (mkPayload "cdlq-parent")) {groupKey = Just "cdlq-g"}
          c1 = (defaultJob (mkPayload "cdlq-c1")) {groupKey = Just "cdlq-g"}
          c2 = (defaultJob (mkPayload "cdlq-c2")) {groupKey = Just "cdlq-g2"}
      Right _ <- runM env $ HL.insertJobTree @m @registry (rollup parent (leaf c1 :| [leaf c2]))
      check env "after rollup tree insert"

      -- Claim children and DLQ them (triggers cascade for rollup parents)
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 2 60
      forM_ claimed $ \j -> void $ runM env $ HL.moveToDLQ "child failed" j
      check env "after cascading children to DLQ"

    it "deleteDLQJob with tryResumeParent: groups unchanged" $ \env -> do
      let parent = (defaultJob (mkPayload "deldlq-parent")) {groupKey = Just "deldlq-g"}
          c1 = (defaultJob (mkPayload "deldlq-c1")) {groupKey = Just "deldlq-g"}
      Right _ <- runM env $ HL.insertJobTree @m @registry (rollup parent (leaf c1 :| []))
      check env "after tree insert"

      -- Claim child, DLQ it
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      forM_ claimed $ \j -> void $ runM env $ HL.moveToDLQ "fail" j
      check env "after child DLQ"

      -- Delete the DLQ'd child
      dlqJobs <- runM env $ HL.listDLQJobs @m @registry @payload 10 0
      forM_ dlqJobs $ \dj -> void $ runM env $ HL.deleteDLQJob @m @registry @payload (dlqPrimaryKey dj)
      check env "after deleteDLQJob (parent may have been resumed)"

    it "pauseChildren: job_count unchanged" $ \env -> do
      let parent = (defaultJob (mkPayload "pause-parent")) {groupKey = Just "pause-g"}
          c1 = (defaultJob (mkPayload "pause-c1")) {groupKey = Just "pause-g"}
          c2 = (defaultJob (mkPayload "pause-c2")) {groupKey = Just "pause-g2"}
      Right inserted <- runM env $ HL.insertJobTree @m @registry (rollup parent (leaf c1 :| [leaf c2]))
      check env "after insertJobTree"

      let parentJob = head $ filter (\j -> payload j == mkPayload "pause-parent") (toList inserted)
      paused <- runM env $ HL.pauseChildren @m @registry @payload (primaryKey parentJob)
      paused `shouldBe` 2
      check env "after pauseChildren (job_count should be unchanged)"

    it "resumeChildren: job_count unchanged" $ \env -> do
      let parent = (defaultJob (mkPayload "resc-parent")) {groupKey = Just "resc-g"}
          c1 = (defaultJob (mkPayload "resc-c1")) {groupKey = Just "resc-g"}
          c2 = (defaultJob (mkPayload "resc-c2")) {groupKey = Just "resc-g2"}
      Right inserted <- runM env $ HL.insertJobTree @m @registry (rollup parent (leaf c1 :| [leaf c2]))
      check env "after insertJobTree"

      let parentJob = head $ filter (\j -> payload j == mkPayload "resc-parent") (toList inserted)
      paused <- runM env $ HL.pauseChildren @m @registry @payload (primaryKey parentJob)
      paused `shouldBe` 2
      check env "after pauseChildren"

      resumed <- runM env $ HL.resumeChildren @m @registry @payload (primaryKey parentJob)
      resumed `shouldBe` 2
      check env "after resumeChildren (job_count should be unchanged)"

    it "claimNextVisibleJobsBatched: job_count unchanged" $ \env -> do
      void $
        runM env $
          HL.insertJobsBatch
            [ (defaultJob (mkPayload "batched1")) {groupKey = Just "batched-g1"}
            , (defaultJob (mkPayload "batched2")) {groupKey = Just "batched-g1"}
            , (defaultJob (mkPayload "batched3")) {groupKey = Just "batched-g2"}
            ]
      check env "after batch insert"

      _claimed <- runM env $ HL.claimNextVisibleJobsBatched @m @registry @payload 5 5 60
      check env "after claimNextVisibleJobsBatched (job_count should be unchanged)"

    it "setVisibilityTimeoutBatch: job_count unchanged" $ \env -> do
      void $
        runM env $
          HL.insertJobsBatch
            [ (defaultJob (mkPayload "hvb1")) {groupKey = Just "hvb-g"}
            , (defaultJob (mkPayload "hvb2")) {groupKey = Just "hvb-g"}
            ]
      check env "after insert"

      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 2 60
      check env "after claim"

      void $ runM env $ HL.setVisibilityTimeoutBatch 120 claimed
      check env "after setVisibilityTimeoutBatch (job_count should be unchanged)"

    it "deleteDLQJobsBatch with tryResumeParent: groups unchanged" $ \env -> do
      let parent = (defaultJob (mkPayload "delbatch-parent")) {groupKey = Just "delbatch-g"}
          c1 = (defaultJob (mkPayload "delbatch-c1")) {groupKey = Just "delbatch-g"}
          c2 = (defaultJob (mkPayload "delbatch-c2")) {groupKey = Just "delbatch-g"}
      Right _ <- runM env $ HL.insertJobTree @m @registry (rollup parent (leaf c1 :| [leaf c2]))
      check env "after tree insert"

      -- Claim both children and DLQ them
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 2 60
      forM_ claimed $ \j -> void $ runM env $ HL.moveToDLQ "fail" j
      check env "after DLQ both children"

      -- Batch-delete the DLQ'd children
      dlqJobs <- runM env $ HL.listDLQJobs @m @registry @payload 10 0
      void $ runM env $ HL.deleteDLQJobsBatch @m @registry @payload (map dlqPrimaryKey dlqJobs)
      check env "after deleteDLQJobsBatch (parent may have been resumed)"

    it "refreshGroups: corrects any drift" $ \env -> do
      -- Insert some grouped jobs
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "reaper1")) {groupKey = Just "reaper-g"}
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "reaper2")) {groupKey = Just "reaper-g"}
      check env "before manual drift"

      -- Manually corrupt the groups table
      withConn env $ \conn -> do
        execute_ conn $
          "UPDATE " <> schemaName <> ".\"" <> tableName <> "_groups\" SET job_count = 999 WHERE group_key = 'reaper-g'"

      -- Run refreshGroups to correct
      void $ runM env $ HL.refreshGroups @m @registry @payload 0
      check env "after refreshGroups (should correct drift)"

    it "refreshGroups: skips when interval not elapsed" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "seq1")) {groupKey = Just "seq-g"}
      check env "initial state"

      -- Manually corrupt the groups table
      withConn env $ \conn ->
        execute_ conn $
          "UPDATE " <> schemaName <> ".\"" <> tableName <> "_groups\" SET job_count = 999 WHERE group_key = 'seq-g'"

      -- First refresh with large interval — runs because sequence is 0
      void $ runM env $ HL.refreshGroups @m @registry @payload 300
      check env "after first refreshGroups (should correct drift)"

      -- Corrupt again
      withConn env $ \conn ->
        execute_ conn $
          "UPDATE " <> schemaName <> ".\"" <> tableName <> "_groups\" SET job_count = 888 WHERE group_key = 'seq-g'"

      -- Second refresh with same large interval — should skip (just ran)
      void $ runM env $ HL.refreshGroups @m @registry @payload 300

      -- Verify drift was NOT corrected (reaper skipped)
      withConn env $ \conn -> do
        [PG.Only cnt] <-
          PG.query_
            conn
            ( fromString $
                T.unpack $
                  "SELECT job_count FROM " <> schemaName <> ".\"" <> tableName <> "_groups\" WHERE group_key = 'seq-g'"
            )
        cnt `shouldBe` (888 :: Int)

    it "refreshGroups: runs when interval elapsed" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "elapsed1")) {groupKey = Just "elapsed-g"}

      -- Run with interval 0 — always runs
      void $ runM env $ HL.refreshGroups @m @registry @payload 0

      -- Corrupt
      withConn env $ \conn ->
        execute_ conn $
          "UPDATE " <> schemaName <> ".\"" <> tableName <> "_groups\" SET job_count = 777 WHERE group_key = 'elapsed-g'"

      -- Run again with interval 0 — should run again
      void $ runM env $ HL.refreshGroups @m @registry @payload 0
      check env "after refreshGroups with interval 0 (should always correct)"

    it "mixed operations sequence: groups stay consistent" $ \env -> do
      -- Insert various jobs
      Just _j1 <- runM env $ HL.insertJob (defaultJob (mkPayload "mix1")) {groupKey = Just "mix-g1"}
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "mix2")) {groupKey = Just "mix-g1"}
      Just j3 <- runM env $ HL.insertJob (defaultJob (mkPayload "mix3")) {groupKey = Just "mix-g2"}
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "mix4")) -- ungrouped
      check env "after mixed inserts"

      -- Claim and ack one
      claimed <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      forM_ claimed $ \j -> void $ runM env $ HL.ackJob j
      check env "after ack"

      -- Cancel one
      void $ runM env $ HL.cancelJob @m @registry @payload (primaryKey j3)
      check env "after cancel"

      -- Insert more
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "mix5")) {groupKey = Just "mix-g1"}
      check env "after another insert"

      -- Claim and DLQ
      claimed2 <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      forM_ claimed2 $ \j -> void $ runM env $ HL.moveToDLQ "test" j
      check env "after DLQ"

      -- Retry from DLQ
      dlqJobs <- runM env $ HL.listDLQJobs @m @registry @payload 10 0
      forM_ (take 1 dlqJobs) $ \dj -> void $ runM env $ HL.retryFromDLQ @m @registry @payload (dlqPrimaryKey dj)
      check env "after retryFromDLQ"

    -- -----------------------------------------------------------------
    -- in_flight_until correctness tests
    -- -----------------------------------------------------------------

    it "cancelJob on non-in-flight job preserves in_flight_until" $ \env -> do
      -- Insert 2 jobs in same group
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "ift-a")) {groupKey = Just "ift-cancel-g"}
      Just jobB <- runM env $ HL.insertJob (defaultJob (mkPayload "ift-b")) {groupKey = Just "ift-cancel-g"}
      check env "after 2 inserts"

      -- Claim one job (sets group in-flight)
      [jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      check env "after claiming job A"

      -- Cancel the other job (non-in-flight) — should NOT clear in_flight_until
      void $ runM env $ HL.cancelJob @m @registry @payload (primaryKey jobB)
      check env "after cancelling non-in-flight job B"

      -- Insert a new job so there's something to claim
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "ift-c")) {groupKey = Just "ift-cancel-g"}

      -- Try to claim — should get nothing (group still blocked by in-flight job)
      blocked <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      length blocked `shouldBe` 0

      -- Ack the in-flight job to unblock
      void $ runM env $ HL.ackJob jobA

      -- Now the group should be claimable
      unblocked <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      length unblocked `shouldBe` 1
      check env "after ack unblocks group"

    it "cancelJobsBatch on non-in-flight jobs preserves in_flight_until" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "iftb-a")) {groupKey = Just "ift-batch-g"}
      Just jobB <- runM env $ HL.insertJob (defaultJob (mkPayload "iftb-b")) {groupKey = Just "ift-batch-g"}
      Just jobC <- runM env $ HL.insertJob (defaultJob (mkPayload "iftb-c")) {groupKey = Just "ift-batch-g"}
      check env "after 3 inserts"

      -- Claim one job
      [jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      check env "after claiming job A"

      -- Batch-cancel the other two (non-in-flight)
      void $ runM env $ HL.cancelJobsBatch @m @registry @payload [primaryKey jobB, primaryKey jobC]
      check env "after batch cancel of non-in-flight jobs"

      -- Insert a new job to try claiming
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "iftb-d")) {groupKey = Just "ift-batch-g"}

      -- Should be blocked
      blocked <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      length blocked `shouldBe` 0

      -- Ack to unblock
      void $ runM env $ HL.ackJob jobA
      unblocked <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      length unblocked `shouldBe` 1
      check env "after ack unblocks group"

    it "ackJob clears in_flight_until" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "ifta-a")) {groupKey = Just "ift-ack-g"}
      check env "after insert"

      -- Claim it (group now in-flight)
      [jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60

      -- Insert another job in same group
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "ifta-b")) {groupKey = Just "ift-ack-g"}

      -- Blocked while in-flight
      blocked <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      length blocked `shouldBe` 0

      -- Ack clears in_flight_until via DELETE trigger
      void $ runM env $ HL.ackJob jobA

      -- Now claimable
      unblocked <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      length unblocked `shouldBe` 1
      check env "after ack clears in_flight_until"

    it "moveToDLQ clears in_flight_until" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "iftd-a")) {groupKey = Just "ift-dlq-g"}
      check env "after insert"

      -- Claim it
      [jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60

      -- Insert another job in same group
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "iftd-b")) {groupKey = Just "ift-dlq-g"}

      -- Blocked while in-flight
      blocked <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      length blocked `shouldBe` 0

      -- DLQ removes from main table via DELETE trigger
      void $ runM env $ HL.moveToDLQ "test error" jobA

      -- Now claimable
      unblocked <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      length unblocked `shouldBe` 1
      check env "after DLQ clears in_flight_until"

    it "heartbeat increases in_flight_until" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "hb-a")) {groupKey = Just "hb-g"}
      -- Claim with 30s visibility
      [jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 30
      check env "after claim with 30s"

      -- Extend to 120s via heartbeat
      void $ runM env $ HL.setVisibilityTimeout 120 jobA
      check env "after setVisibilityTimeout 120 (in_flight_until should reflect extension)"

    it "setVisibilityTimeout 0 (release) clears in_flight_until" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "rel-a")) {groupKey = Just "rel-g"}
      -- Claim with 60s
      [jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      check env "after claim with 60s"

      -- Release by setting visibility to 0
      void $ runM env $ HL.setVisibilityTimeout 0 jobA
      check env "after setVisibilityTimeout 0 (in_flight_until should be NULL)"

    it "resume in group with in-flight job preserves in_flight_until" $ \env -> do
      -- Insert 2 jobs in same group
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "rif-a")) {groupKey = Just "rif-g"}
      Just jobB <- runM env $ HL.insertJob (defaultJob (mkPayload "rif-b")) {groupKey = Just "rif-g"}

      -- Suspend job B
      void $ runM env $ HL.suspendJob @m @registry @payload (primaryKey jobB)

      -- Claim job A (sets in_flight_until)
      [_jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      check env "after claim with suspended sibling"

      -- Resume job B — should NOT clear in_flight_until from job A's claim
      void $ runM env $ HL.resumeJob @m @registry @payload (primaryKey jobB)
      check env "after resume (in_flight_until should still reflect job A)"

    it "INSERT clears expired in_flight_until" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "exp-a")) {groupKey = Just "exp-g"}
      -- Claim with very short (1s) visibility
      [_jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 1
      check env "after claim with 1s visibility"

      -- Wait for it to expire
      threadDelay 2_000_000

      -- Insert a new job in same group — INSERT trigger should clear expired in_flight_until
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "exp-b")) {groupKey = Just "exp-g"}
      check env "after insert clears expired in_flight_until"

    it "promoteJob clears in_flight_until" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "prm-a")) {groupKey = Just "prm-g"}
      -- Claim it
      [jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60
      check env "after claim"

      -- Update for retry (sets not_visible_until to future)
      void $ runM env $ HL.updateJobForRetry 30 "retry" jobA
      check env "after updateJobForRetry"

      -- Promote (sets not_visible_until = NULL)
      void $ runM env $ HL.promoteJob @m @registry @payload (primaryKey jobA)
      check env "after promoteJob (in_flight_until should be NULL)"

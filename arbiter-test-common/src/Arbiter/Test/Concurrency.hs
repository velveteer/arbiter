{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

-- | Parameterized concurrency test suite that works with any MonadArbiter implementation.
module Arbiter.Test.Concurrency
  ( concurrencySpec
  , raceConditionSpec
  , groupsConsistencyStressSpec
  , inFlightConcurrencySpec
  , installHolDetector
  , countHolViolations
  , removeHolDetector
  ) where

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.QueueRegistry (TableForPayload)
import Control.Concurrent (threadDelay)
import Control.Monad (forM, forM_, replicateM, replicateM_, void, when)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.List (nub, sort)
import Data.List.NonEmpty qualified as NE
import Data.Maybe (catMaybes)
import Data.Set qualified as Set
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple qualified as PG
import GHC.TypeLits (KnownSymbol)
import Test.Hspec
import UnliftIO.Async (mapConcurrently, replicateConcurrently_)

import Arbiter.Test.GroupsInvariant (assertGroupsConsistent)

-- | Claim and ack jobs in a loop until no more are available.
-- Backs off with 10ms delay between empty claim attempts, gives up
-- after @maxStreak@ consecutive empties.
claimAckLoop
  :: IO [a]
  -> Int
  -- ^ Max consecutive empty claims before giving up
  -> (a -> IO ())
  -- ^ Per-job action (typically ack, optionally recording IDs)
  -> IO ()
claimAckLoop claim maxStreak onJob = go 0
  where
    go streak = do
      jobs <- claim
      if null jobs
        then when (streak < maxStreak) $ threadDelay 10_000 >> go (streak + 1)
        else forM_ jobs onJob >> go 0

-- | Drain all remaining jobs with no streak limit.
drainAll :: IO [a] -> (a -> IO ()) -> IO ()
drainAll claim onJob = do
  jobs <- claim
  if null jobs then pure () else forM_ jobs onJob >> drainAll claim onJob

-- | Find duplicate entries in a list. O(n log n).
findDuplicates :: (Ord a) => [a] -> [a]
findDuplicates = go Set.empty Set.empty
  where
    go _ dups [] = Set.toList dups
    go seen dups (x : xs)
      | Set.member x seen = go seen (Set.insert x dups) xs
      | otherwise = go (Set.insert x seen) dups xs

-- | Count HOL violations from a log of @(group_key, claim_seq, ack_seq)@ entries.
-- Two entries from the same group with overlapping @[claim_seq, ack_seq]@ intervals
-- indicate a head-of-line violation (two workers processing the same group concurrently).
-- | Install a database-level HOL violation detector.
--
-- Creates a trigger that fires inside claim transactions (AFTER UPDATE on the
-- jobs table). When a job becomes in-flight, the trigger checks if another job
-- in the same group is already in-flight. If so, it logs a violation.
installHolDetector :: PG.Connection -> Text -> Text -> IO ()
installHolDetector conn schemaName tableName = do
  let tbl = schemaName <> ".\"" <> tableName <> "\""
      vTbl = schemaName <> ".\"" <> tableName <> "_hol_violations\""
      fn = schemaName <> ".\"detect_hol_" <> tableName <> "_fn\"()"
      trigger = "detect_hol_" <> tableName
      exec = PG.execute_ conn . fromString . T.unpack
  _ <- exec "SET client_min_messages TO 'warning'"
  mapM_
    exec
    [ "CREATE TABLE IF NOT EXISTS "
        <> vTbl
        <> " (group_key TEXT NOT NULL, job_id BIGINT NOT NULL)"
    , "TRUNCATE " <> vTbl
    , "CREATE OR REPLACE FUNCTION "
        <> fn
        <> " RETURNS TRIGGER AS $t$ "
        <> "BEGIN "
        <> "IF NEW.not_visible_until IS NOT NULL "
        <> "AND NEW.not_visible_until > NOW() "
        <> "AND NOT NEW.suspended "
        <> "AND NEW.group_key IS NOT NULL "
        <> "AND EXISTS ("
        <> "SELECT 1 FROM "
        <> tbl
        <> " "
        <> "WHERE group_key = NEW.group_key AND id != NEW.id "
        <> "AND not_visible_until IS NOT NULL "
        <> "AND not_visible_until > NOW() "
        <> "AND NOT suspended"
        <> ") THEN "
        <> "INSERT INTO "
        <> vTbl
        <> " (group_key, job_id) "
        <> "VALUES (NEW.group_key, NEW.id); "
        <> "END IF; "
        <> "RETURN NULL; "
        <> "END; $t$ LANGUAGE plpgsql"
    , "DROP TRIGGER IF EXISTS " <> trigger <> " ON " <> tbl
    , "CREATE TRIGGER "
        <> trigger
        <> " AFTER UPDATE ON "
        <> tbl
        <> " FOR EACH ROW EXECUTE FUNCTION "
        <> fn
    ]
  _ <- exec "RESET client_min_messages"
  pure ()

-- | Count violations detected by the HOL detector trigger.
countHolViolations :: PG.Connection -> Text -> Text -> IO Int64
countHolViolations conn schemaName tableName = do
  let vTbl = schemaName <> ".\"" <> tableName <> "_hol_violations\""
  [PG.Only count] <-
    PG.query_ conn $
      fromString $
        T.unpack $
          "SELECT count(*)::bigint FROM " <> vTbl
  pure count

-- | Remove the HOL detector trigger and violations table.
removeHolDetector :: PG.Connection -> Text -> Text -> IO ()
removeHolDetector conn schemaName tableName = do
  let tbl = schemaName <> ".\"" <> tableName <> "\""
      vTbl = schemaName <> ".\"" <> tableName <> "_hol_violations\""
      fn = schemaName <> ".\"detect_hol_" <> tableName <> "_fn\""
      trigger = "detect_hol_" <> tableName
      exec = PG.execute_ conn . fromString . T.unpack
  _ <- exec "SET client_min_messages TO 'warning'"
  mapM_
    exec
    [ "DROP TRIGGER IF EXISTS " <> trigger <> " ON " <> tbl
    , "DROP FUNCTION IF EXISTS " <> fn
    , "DROP TABLE IF EXISTS " <> vTbl
    ]
  _ <- exec "RESET client_min_messages"
  pure ()

-- | Parameterized concurrency test suite.
--
-- IMPORTANT: These tests require a connection pool with multiple connections (at least 10)
-- to properly test concurrent access patterns.
--
-- The payload type must be registered in the registry and satisfy JobPayload constraints.
concurrencySpec
  :: forall payload registry env m
   . ( HasArbiterSchema m registry
     , JobPayload payload
     , KnownSymbol (TableForPayload payload registry)
     , MonadArbiter m
     )
  => (Text -> payload)
  -- ^ Constructor for a simple test message payload
  -> (forall a. env -> m a -> IO a)
  -- ^ Runner function to execute monad actions (e.g., runSimpleDb env or runOrvilleTest env)
  -> SpecWith env
concurrencySpec mkMessage runM = do
  describe "Heartbeat Visibility Across Connections" $ do
    it "setVisibilityTimeout updates are immediately visible to concurrent workers" $ \env -> do
      -- Insert a job
      void $ runM env $ HL.insertJob ((defaultJob (mkMessage "Heartbeat")) {groupKey = Just "heartbeat-visibility-test"})

      -- Worker A claims the job with 5 second visibility
      claimed1 <- runM env (HL.claimNextVisibleJobs 1 5) :: IO [JobRead payload]
      length claimed1 `shouldBe` 1
      let job = head claimed1

      -- Worker A extends visibility to 60 seconds (simulating heartbeat)
      -- This happens on a connection from the pool
      rowsUpdated <- runM env (HL.setVisibilityTimeout 60 job)
      rowsUpdated `shouldBe` 1

      -- CRITICAL: Worker B tries to claim immediately after (on different connection from pool)
      -- This should NOT claim the job because visibility was extended
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 5) :: IO [JobRead payload]

      length claimed2 `shouldBe` 0

    it "heartbeat prevents reclaim after original visibility timeout expires" $ \env -> do
      -- Insert a job
      void $ runM env $ HL.insertJob ((defaultJob (mkMessage "Timeout")) {groupKey = Just "heartbeat-timeout-test"})

      -- Claim with SHORT visibility (2 seconds)
      claimed1 <- runM env (HL.claimNextVisibleJobs 1 2) :: IO [JobRead payload]
      length claimed1 `shouldBe` 1
      let job = head claimed1

      -- Wait 1 second, then extend visibility by 10 more seconds
      threadDelay 1_000_000
      void $ runM env (HL.setVisibilityTimeout 10 job)

      -- Wait another 2 seconds (now 3 seconds total - past original 2s timeout)
      threadDelay 2_000_000

      -- Try to claim - should FAIL because visibility was extended
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 2) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

    it "visibility timeout of 0 makes job immediately claimable" $ \env -> do
      -- Insert and claim job
      void $ runM env $ HL.insertJob ((defaultJob (mkMessage "Zero")) {groupKey = Just "visibility-zero-test"})
      claimed1 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed1 `shouldBe` 1
      let job = head claimed1

      -- Set visibility to 0 (make immediately visible)
      void $ runM env (HL.setVisibilityTimeout 0 job)

      -- Should be able to claim immediately
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1

  describe "Concurrent Job Claims" $ do
    it "multiple workers claim different ungrouped jobs concurrently" $ \env -> do
      -- Insert 6 ungrouped jobs
      void $
        runM env $
          HL.insertJobsBatch (replicate 6 $ defaultJob (mkMessage "Concurrent"))

      -- Three workers claiming CONCURRENTLY (truly parallel database access)
      -- This tests that FOR UPDATE SKIP LOCKED prevents duplicate claims
      [c1, c2, c3] <-
        mapConcurrently
          (\_ -> runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload])
          [1 :: Int, 2, 3]

      -- Total claimed should be 6
      (length c1 + length c2 + length c3) `shouldBe` 6

      -- CRITICAL: No job should be claimed by multiple workers (check IDs are unique)
      -- If FOR UPDATE SKIP LOCKED wasn't working, we'd see duplicate IDs
      let allIds = map primaryKey (c1 <> c2 <> c3)
      length allIds `shouldBe` length (nub allIds)

    it "concurrent workers respect head-of-line blocking for grouped jobs" $ \env -> do
      -- 500 iterations: seed a group, race 10 workers to claim + concurrent
      -- inserts to the same group. HOL means exactly 1 claim per round.
      let numIterations = 500
          numWorkers = 10
      violationRef <- newIORef (0 :: Int)

      forM_ [(1 :: Int) .. numIterations] $ \_ -> do
        -- Insert 5 jobs in one group
        void $
          runM env $
            HL.insertJobsBatch (replicate 5 $ (defaultJob (mkMessage "Grouped")) {groupKey = Just "hol-stress"})

        -- Race N workers to claim + 2 concurrent inserters to the same group.
        -- The INSERT triggers fire concurrently with the claim CTEs,
        -- exercising the groups row lock contention path.
        results <-
          mapConcurrently
            id
            $ map
              (\_ -> runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload])
              [(1 :: Int) .. numWorkers]
              <> [ void (runM env $ HL.insertJob (defaultJob (mkMessage "concurrent")) {groupKey = Just "hol-stress"}) >> pure []
                 , void (runM env $ HL.insertJob (defaultJob (mkMessage "concurrent")) {groupKey = Just "hol-stress"}) >> pure []
                 ]

        -- HOL invariant: at most 1 claim per group. totalClaimed == 0 is
        -- normal when the INSERT trigger's groups row lock causes claims to
        -- skip the group (FOR UPDATE SKIP LOCKED). totalClaimed > 1 is a
        -- real HOL violation.
        let totalClaimed = sum (map length results)
        when (totalClaimed > 1) $
          atomicModifyIORef' violationRef (\n -> (n + 1, ()))

        -- Ack and drain
        forM_ (concat results) $ \j -> void $ runM env (HL.ackJob j)
        drainAll (runM env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead payload]) $
          \j -> void $ runM env (HL.ackJob j)

      violations <- readIORef violationRef
      violations `shouldBe` 0

    it "FOR UPDATE SKIP LOCKED prevents concurrent claims of same job" $ \env -> do
      -- Insert a single job
      void $ runM env $ HL.insertJob ((defaultJob (mkMessage "Single")) {groupKey = Just "skip-locked-test"})

      -- Three workers racing to claim the same job CONCURRENTLY
      -- This tests that SKIP LOCKED allows workers to skip locked rows without blocking
      [c1, c2, c3] <-
        mapConcurrently
          (\_ -> runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload])
          [1 :: Int, 2, 3]

      -- CRITICAL: Exactly ONE worker should have claimed it
      -- Without SKIP LOCKED, workers would block waiting for the lock
      -- With SKIP LOCKED, workers skip the locked row and return empty
      let totalClaimed = length c1 + length c2 + length c3
      totalClaimed `shouldBe` 1

  describe "Optimistic Locking Under Concurrent Load" $ do
    it "ack fails when another worker reclaimed job (real concurrent scenario)" $ \env -> do
      -- Insert job
      void $ runM env $ HL.insertJob ((defaultJob (mkMessage "Race")) {groupKey = Just "optimistic-lock-test"})

      -- Worker A claims
      claimedA <- runM env (HL.claimNextVisibleJobs 1 1) :: IO [JobRead payload]
      let jobA = head claimedA

      -- Wait for visibility to expire
      threadDelay 1_500_000

      -- Worker B claims (job now visible again)
      claimedB <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedB `shouldBe` 1
      let jobB = head claimedB

      -- Worker A tries to ack (should fail - attempts mismatch)
      rowsAcked <- runM env (HL.ackJob jobA)
      rowsAcked `shouldBe` 0

      -- Worker B should be able to ack successfully
      rowsAcked2 <- runM env (HL.ackJob jobB)
      rowsAcked2 `shouldBe` 1

-- | Aggressive race condition tests designed to surface concurrency bugs.
--
-- These tests use high contention and many iterations to maximize the
-- probability of hitting race windows. They stress the system with:
-- - Many concurrent workers (40+)
-- - Large job counts (500+)
-- - Tight timing windows
-- - Real database contention
raceConditionSpec
  :: forall payload registry env m
   . ( HasArbiterSchema m registry
     , JobPayload payload
     , KnownSymbol (TableForPayload payload registry)
     , MonadArbiter m
     )
  => (Text -> payload)
  -> (forall a. env -> m a -> IO a)
  -> SpecWith env
raceConditionSpec mkMessage runM = do
  describe "Race Condition Stress Tests" $ do
    describe "High-Contention Claim Stress" $ do
      it "no duplicate claims under extreme contention (ungrouped)" $ \env -> do
        let numJobs = 500
            numWorkers = 40

        void $ runM env $ HL.insertJobsBatch (replicate numJobs $ defaultJob (mkMessage "stress"))

        claimedRef <- newIORef ([] :: [Int64])

        replicateConcurrently_ numWorkers $
          claimAckLoop (runM env (HL.claimNextVisibleJobs 3 60) :: IO [JobRead payload]) 20 $ \job -> do
            atomicModifyIORef' claimedRef (\acc -> (primaryKey job : acc, ()))
            void $ runM env (HL.ackJob job)

        allClaimed <- readIORef claimedRef
        findDuplicates allClaimed `shouldBe` []
        length allClaimed `shouldBe` numJobs

      it "no duplicate claims under extreme contention (many groups)" $ \env -> do
        let numGroups = 50
            jobsPerGroup = 10
            numWorkers = 40

        forM_ [1 .. numGroups] $ \(g :: Int) -> do
          let groupName = "stress-group-" <> T.pack (show g)
          void $
            runM env $
              HL.insertJobsBatch
                (replicate jobsPerGroup $ (defaultJob (mkMessage "grouped")) {groupKey = Just groupName})

        claimedRef <- newIORef ([] :: [Int64])

        replicateConcurrently_ numWorkers $
          claimAckLoop (runM env (HL.claimNextVisibleJobs 5 60) :: IO [JobRead payload]) 20 $ \job -> do
            atomicModifyIORef' claimedRef (\acc -> (primaryKey job : acc, ()))
            void $ runM env (HL.ackJob job)

        allClaimed <- readIORef claimedRef
        let totalJobs = numGroups * jobsPerGroup
        findDuplicates allClaimed `shouldBe` []
        length allClaimed `shouldBe` totalJobs

    describe "Insert + Claim Race" $ do
      it "no lost jobs under concurrent insert/claim pressure" $ \env -> do
        let numRounds = 100
            jobsPerRound = 20
            numClaimers = 10

        insertedRef <- newIORef Set.empty
        claimedRef <- newIORef Set.empty

        -- Run multiple rounds of concurrent insert + claim
        replicateM_ numRounds $ do
          _ <-
            mapConcurrently
              id
              ( [ do
                    -- Inserter
                    jobs <- runM env $ HL.insertJobsBatch (replicate jobsPerRound $ defaultJob (mkMessage "race"))
                    let ids = Set.fromList $ map primaryKey jobs
                    atomicModifyIORef' insertedRef (\acc -> (Set.union acc ids, ()))
                ]
                  <> replicate
                    numClaimers
                    ( do
                        -- Claimer
                        claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
                        let ids = Set.fromList $ map primaryKey claimed
                        atomicModifyIORef' claimedRef (\acc -> (Set.union acc ids, ()))
                        forM_ claimed $ \job -> runM env (HL.ackJob job)
                    )
              )
          pure ()

        -- Drain remaining
        drainAll (runM env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead payload]) $ \job -> do
          atomicModifyIORef' claimedRef (\acc -> (Set.insert (primaryKey job) acc, ()))
          void $ runM env (HL.ackJob job)

        inserted <- readIORef insertedRef
        claimed <- readIORef claimedRef

        -- No jobs should be lost
        Set.size inserted `shouldBe` (numRounds * jobsPerRound)
        claimed `shouldBe` inserted

    describe "Visibility Timeout Races" $ do
      it "no duplicate processing with aggressive visibility expiration" $ \env -> do
        let numJobs = 50
            numWorkers = 20
            visibilityMs = 100 -- 100ms visibility - very aggressive
        void $ runM env $ HL.insertJobsBatch (replicate numJobs $ defaultJob (mkMessage "timeout-race"))

        -- Track which jobs were successfully acked
        ackedRef <- newIORef Set.empty

        replicateConcurrently_ numWorkers $
          -- 50 empty-streak limit (500ms at 10ms each) so workers retry while
          -- jobs are in-flight with 100ms visibility instead of exiting immediately.
          claimAckLoop (runM env (HL.claimNextVisibleJobs 2 (fromIntegral visibilityMs / 1000)) :: IO [JobRead payload]) 50 $
            \job -> do
              threadDelay (visibilityMs * 500) -- Half the visibility time
              rows <- runM env (HL.ackJob job)
              when (rows > 0) $
                atomicModifyIORef' ackedRef (\acc -> (Set.insert (primaryKey job) acc, ()))

        acked <- readIORef ackedRef
        -- All jobs should eventually be acked exactly once
        Set.size acked `shouldBe` numJobs

      it "exactly one ack succeeds per job under reclaim pressure" $ \env -> do
        let numAttempts = 50
            numWorkersPerAttempt = 5

        -- Insert a fresh job per iteration so every round has a real race
        successCounts <- replicateM numAttempts $ do
          void $ runM env $ HL.insertJob (defaultJob (mkMessage "single-race"))

          results <-
            mapConcurrently
              id
              $ replicate numWorkersPerAttempt
              $ do
                claimed <- runM env (HL.claimNextVisibleJobs 1 0.1) :: IO [JobRead payload]
                case claimed of
                  [] -> pure 0
                  (job : _) -> do
                    threadDelay 50_000 -- 50ms
                    rows <- runM env (HL.ackJob job)
                    pure (fromIntegral rows :: Int)

          pure (sum results)

        -- Each iteration should have exactly 1 successful ack
        successCounts `shouldBe` replicate numAttempts 1

    describe "Batched Mode Stress" $ do
      it "no duplicate claims in batched mode under heavy contention" $ \env -> do
        let numGroups = 30
            jobsPerGroup = 20
            numWorkers = 25

        forM_ [1 .. numGroups] $ \(g :: Int) -> do
          let groupName = "batch-stress-" <> T.pack (show g)
          void $
            runM env $
              HL.insertJobsBatch
                (replicate jobsPerGroup $ (defaultJob (mkMessage "batched")) {groupKey = Just groupName})

        claimedRef <- newIORef ([] :: [Int64])

        replicateConcurrently_ numWorkers $
          claimAckLoop (concatMap NE.toList <$> runM env (HL.claimNextVisibleJobsBatched 5 4 60) :: IO [JobRead payload]) 20 $ \job -> do
            atomicModifyIORef' claimedRef (\acc -> (primaryKey job : acc, ()))
            void $ runM env (HL.ackJob job)

        allClaimed <- readIORef claimedRef
        let totalJobs = numGroups * jobsPerGroup
        findDuplicates allClaimed `shouldBe` []
        length allClaimed `shouldBe` totalJobs

      it "batched claims from same group are contiguous (no interleaving)" $ \env -> do
        -- Advisory locks serialize concurrent batched claims per group.
        -- Without them, SKIP LOCKED causes interleaving: A gets [1,3,5],
        -- B gets [2,4,6]. With advisory locks each worker gets a contiguous
        -- block: A gets [1,2,3], then B gets [4,5,6].
        let numGroups = 5
            jobsPerGroup = 50
            numWorkers = 20
            numRounds = 30

        gapCount <- newIORef (0 :: Int)

        replicateM_ numRounds $ do
          forM_ [1 .. numGroups] $ \(g :: Int) ->
            void $
              runM env $
                HL.insertJobsBatch
                  (replicate jobsPerGroup $ (defaultJob (mkMessage "split")) {groupKey = Just ("split-" <> T.pack (show g))})

          resultsRef <- newIORef ([] :: [(Int, [(Maybe Text, Int64)])])
          _ <-
            mapConcurrently
              ( \wid -> do
                  claimed <- concatMap NE.toList <$> runM env (HL.claimNextVisibleJobsBatched 10 3 60) :: IO [JobRead payload]
                  atomicModifyIORef' resultsRef (\acc -> ((wid, map (\j -> (groupKey j, primaryKey j)) claimed) : acc, ()))
                  forM_ claimed $ \job -> runM env (HL.ackJob job)
              )
              [1 .. numWorkers]

          -- For each group, check that per-worker IDs are contiguous (no gaps).
          -- Gaps mean another worker interleaved claims from the same group.
          results <- readIORef resultsRef
          let perGroup gk =
                [ sort ids
                | (_, jobs) <- results
                , let ids = [jid | (Just g, jid) <- jobs, g == gk]
                , not (null ids)
                ]
              allGks = nub [gk | (_, jobs) <- results, (Just gk, _) <- jobs]
              hasGaps ids = any (\(a, b) -> b - a /= 1) $ zip ids (drop 1 ids)
              gappedGroups =
                [ gk
                | gk <- allGks
                , any hasGaps (perGroup gk)
                ]

          when (not (null gappedGroups)) $
            atomicModifyIORef' gapCount (\n -> (n + length gappedGroups, ()))

          -- Drain remaining
          drainAll (concatMap NE.toList <$> runM env (HL.claimNextVisibleJobsBatched 100 20 60) :: IO [JobRead payload]) $
            \j -> void $ runM env (HL.ackJob j)

        readIORef gapCount >>= (`shouldBe` 0)

      it "batched and single mode don't interfere" $ \env -> do
        -- Mix of batched and single mode workers on same table
        let numGroups = 20
            jobsPerGroup = 15
            numBatchedWorkers = 10
            numSingleWorkers = 10

        forM_ [1 .. numGroups] $ \(g :: Int) -> do
          let groupName = "mixed-mode-" <> T.pack (show g)
          void $
            runM env $
              HL.insertJobsBatch
                (replicate jobsPerGroup $ (defaultJob (mkMessage "mixed")) {groupKey = Just groupName})

        claimedRef <- newIORef ([] :: [Int64])

        let onJob job = do
              atomicModifyIORef' claimedRef (\acc -> (primaryKey job : acc, ()))
              void $ runM env (HL.ackJob job)
            batchedWorker =
              claimAckLoop (concatMap NE.toList <$> runM env (HL.claimNextVisibleJobsBatched 3 3 60) :: IO [JobRead payload]) 50 onJob
            singleWorker =
              claimAckLoop (runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]) 50 onJob

        _ <-
          mapConcurrently
            id
            (replicate numBatchedWorkers batchedWorker <> replicate numSingleWorkers singleWorker)

        allClaimed <- readIORef claimedRef
        let totalJobs = numGroups * jobsPerGroup
        findDuplicates allClaimed `shouldBe` []
        length allClaimed `shouldBe` totalJobs

    describe "Head-of-Line Blocking Under Stress" $ do
      it "group ordering maintained under concurrent pressure" $ \env -> do
        let numGroups = 10
            jobsPerGroup = 30
            numWorkers = 20

        -- Insert jobs and track expected order per group
        groupJobIds <- forM [1 .. numGroups] $ \(g :: Int) -> do
          let groupName = "order-stress-" <> T.pack (show g)
          mJobs <-
            replicateM jobsPerGroup $
              runM env $
                HL.insertJob ((defaultJob (mkMessage "order")) {groupKey = Just groupName})
          let jobs = catMaybes mJobs
          pure (groupName, sort $ map primaryKey jobs)

        processedRef <- newIORef ([] :: [(Text, Int64)])

        replicateConcurrently_ numWorkers $ do
          let claimLoop (emptyStreak :: Int) = do
                claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
                case claimed of
                  [] | emptyStreak < 200 -> do
                    threadDelay 50_000 -- 50ms backoff for transient empties
                    claimLoop (emptyStreak + 1)
                  [] -> pure ()
                  (job : _) -> do
                    case groupKey job of
                      Just gk ->
                        atomicModifyIORef' processedRef (\acc -> (acc <> [(gk, primaryKey job)], ()))
                      Nothing -> pure ()
                    void $ runM env (HL.ackJob job)
                    claimLoop 0
          claimLoop 0

        processed <- readIORef processedRef

        -- Verify all jobs were processed
        let totalExpected = numGroups * jobsPerGroup
        length processed `shouldBe` totalExpected

        -- Verify ordering for each group
        forM_ groupJobIds $ \(gk, expectedOrder) -> do
          let actualOrder = map snd $ filter (\(g, _) -> g == gk) processed
          actualOrder `shouldBe` expectedOrder

    describe "Ack Race Conditions" $ do
      it "concurrent ack attempts on same job - exactly one succeeds" $ \env -> do
        let numIterations = 100
            numRacingWorkers = 10

        successCounts <- forM [1 .. numIterations] $ \(_ :: Int) -> do
          -- Insert a job
          void $ runM env $ HL.insertJob (defaultJob (mkMessage "ack-race"))

          -- Claim it
          claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
          case claimed of
            [] -> pure 0
            (job : _) -> do
              -- Many workers try to ack the same job concurrently
              results <-
                mapConcurrently
                  id
                  $ replicate numRacingWorkers
                  $ do
                    rows <- runM env (HL.ackJob job)
                    pure (fromIntegral rows :: Int)
              pure (sum results)

        -- Each iteration should have exactly 1 successful ack
        successCounts `shouldBe` replicate numIterations 1

-- | Stress test that verifies groups table consistency after a concurrent claim storm.
groupsConsistencyStressSpec
  :: forall payload registry env m
   . ( HasArbiterSchema m registry
     , JobPayload payload
     , KnownSymbol (TableForPayload payload registry)
     , MonadArbiter m
     )
  => Text
  -> Text
  -> (Text -> payload)
  -> (forall a. env -> m a -> IO a)
  -> (env -> (PG.Connection -> IO ()) -> IO ())
  -> SpecWith env
groupsConsistencyStressSpec schemaName tableName mkPayload runM withConn = do
  describe "Groups Consistency Under Stress" $ do
    it "HOL detector fires on a synthetic violation" $ \env -> do
      -- Self-test: prove the trigger actually detects violations by
      -- manufacturing one with a raw UPDATE (bypassing the claim CTE).
      withConn env $ \conn -> do
        installHolDetector conn schemaName tableName

        -- Insert 2 jobs in the same group
        void $ runM env $ HL.insertJob (defaultJob (mkPayload "det-a")) {groupKey = Just "detector-test"}
        void $ runM env $ HL.insertJob (defaultJob (mkPayload "det-b")) {groupKey = Just "detector-test"}

        -- Claim one job normally — puts it in-flight
        [claimed] <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

        -- Force the second job in-flight with a raw UPDATE, bypassing the claim CTE.
        -- The trigger should detect this as an HOL violation.
        let tbl = schemaName <> ".\"" <> tableName <> "\""
        _ <-
          PG.execute_ conn $
            fromString $
              T.unpack $
                "UPDATE "
                  <> tbl
                  <> " SET not_visible_until = NOW() + interval '60 seconds'"
                  <> " WHERE group_key = 'detector-test' AND id != "
                  <> T.pack (show (primaryKey claimed))

        countHolViolations conn schemaName tableName >>= (`shouldSatisfy` (> 0))

        -- Clean up
        void $ runM env (HL.ackJob claimed)
        drainAll (runM env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead payload]) $
          \job -> void $ runM env (HL.ackJob job)
        removeHolDetector conn schemaName tableName

    it "groups table consistent after concurrent claim storm" $ \env -> do
      -- Insert 30 groups x 10 jobs = 300 total
      let numGroups = 30 :: Int
          jobsPerGroup = 10
          numWorkers = 20

      withConn env $ \conn -> installHolDetector conn schemaName tableName

      forM_ [1 .. numGroups] $ \g -> do
        let groupName = "storm-" <> T.pack (show g)
        void $
          runM env $
            HL.insertJobsBatch
              (replicate jobsPerGroup $ (defaultJob (mkPayload "storm")) {groupKey = Just groupName})

      -- 20 concurrent workers claim + ack until empty
      replicateConcurrently_ numWorkers $
        claimAckLoop (runM env (HL.claimNextVisibleJobs 5 60) :: IO [JobRead payload]) 30 $
          \job -> void $ runM env (HL.ackJob job)

      withConn env $ \conn -> do
        countHolViolations conn schemaName tableName >>= (`shouldBe` 0)
        assertGroupsConsistent conn schemaName tableName "after concurrent claim storm"
        removeHolDetector conn schemaName tableName

    it "groups table consistent under concurrent inserts and acks to same group" $ \env -> do
      let numIterations = 100 :: Int
          numInserters = 5
          numClaimers = 10
          groupName = "hol-race"

      withConn env $ \conn -> installHolDetector conn schemaName tableName

      forM_ [(1 :: Int) .. numIterations] $ \_ -> do
        doneRef <- newIORef False

        -- Seed the group so there's always something to claim
        void $ runM env $ HL.insertJob (defaultJob (mkPayload "seed")) {groupKey = Just groupName}

        let inserter =
              replicateM_ 20 $
                void $
                  runM env $
                    HL.insertJob (defaultJob (mkPayload "churn")) {groupKey = Just groupName}

            claimer = do
              let claimLoop = do
                    done <- readIORef doneRef
                    claimed <- runM env (HL.claimNextVisibleJobs 5 60) :: IO [JobRead payload]
                    forM_ claimed $ \job ->
                      void $ runM env (HL.ackJob job)
                    if done && null claimed
                      then pure ()
                      else do
                        when (null claimed) $ threadDelay 1_000
                        claimLoop
              claimLoop

        insertersDone <- newIORef (0 :: Int)
        _ <-
          mapConcurrently
            id
            $ [ do inserter; atomicModifyIORef' insertersDone (\n -> (n + 1, ()))
              | _ <- [(1 :: Int) .. numInserters]
              ]
              <> replicate numClaimers claimer
              <> [ do
                     let waitLoop = do
                           n <- readIORef insertersDone
                           if n >= numInserters
                             then atomicModifyIORef' doneRef (const (True, ()))
                             else threadDelay 10_000 >> waitLoop
                     waitLoop
                 ]

        -- Drain remaining jobs
        drainAll (runM env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead payload]) $
          \job -> void $ runM env (HL.ackJob job)

      withConn env $ \conn -> do
        countHolViolations conn schemaName tableName >>= (`shouldBe` 0)
        assertGroupsConsistent conn schemaName tableName "after concurrent insert+ack churn"
        removeHolDetector conn schemaName tableName

-- | Concurrent tests for the groups table's @in_flight_until@ column.
--
-- Exercises the UPDATE trigger's GREATEST fast path (claims, heartbeats)
-- and full rescan path (releases, suspensions), plus DELETE trigger
-- interactions, under concurrent load.
inFlightConcurrencySpec
  :: forall payload registry env m
   . ( HasArbiterSchema m registry
     , JobPayload payload
     , KnownSymbol (TableForPayload payload registry)
     , MonadArbiter m
     )
  => Text
  -> Text
  -> (Text -> payload)
  -> (forall a. env -> m a -> IO a)
  -> (env -> (PG.Connection -> IO ()) -> IO ())
  -> SpecWith env
inFlightConcurrencySpec schemaName tableName mkPayload runM withConn = do
  let check env label =
        withConn env $ \conn ->
          assertGroupsConsistent conn schemaName tableName label

  describe "in_flight_until Concurrency" $ do
    it "concurrent claim + claim: exactly 1 claimed, in_flight_until correct" $ \env -> do
      -- Insert 2 jobs in same group
      void $
        runM env $
          HL.insertJobsBatch
            [ (defaultJob (mkPayload "cc-1")) {groupKey = Just "cc-g"}
            , (defaultJob (mkPayload "cc-2")) {groupKey = Just "cc-g"}
            ]

      -- 2 workers race to claim
      [c1, c2] <-
        mapConcurrently
          (\_ -> runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload])
          [1 :: Int, 2]

      -- Head-of-line: exactly 1 should claim
      (length c1 + length c2) `shouldBe` 1
      check env "after concurrent claim+claim"

      -- Cleanup
      forM_ (c1 <> c2) $ \j -> void $ runM env $ HL.ackJob j

    it "concurrent claim + suspend: groups consistent over 100 iterations" $ \env -> do
      forM_ [(1 :: Int) .. 100] $ \_ -> do
        -- Insert 3 jobs in same group
        void $ runM env $ HL.insertJob (defaultJob (mkPayload "cs-1")) {groupKey = Just "cs-g"}
        void $ runM env $ HL.insertJob (defaultJob (mkPayload "cs-2")) {groupKey = Just "cs-g"}
        Just job3 <- runM env $ HL.insertJob (defaultJob (mkPayload "cs-3")) {groupKey = Just "cs-g"}

        -- Concurrently: claim head + suspend job3
        -- The suspend fires the UPDATE trigger on the groups row
        -- concurrently with the claim CTE's FOR UPDATE SKIP LOCKED.
        -- The claim may return 0 (SKIP LOCKED skips the group while the
        -- suspend's trigger holds the groups row lock) or 1. Both are correct.
        [claimResult, _] <-
          mapConcurrently
            id
            [ runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
            , do
                void $ runM env $ HL.suspendJob @m @registry @payload (primaryKey job3)
                pure []
            ]

        length claimResult `shouldSatisfy` (<= 1)

        -- Cleanup
        forM_ claimResult $ \j -> void $ runM env $ HL.ackJob j
        drainAll (runM env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead payload]) $
          \j -> void $ runM env $ HL.ackJob j

      check env "after 100 concurrent claim+suspend iterations"

    it "concurrent ack + claim: in_flight_until correct over 50 iterations" $ \env -> do
      forM_ [(1 :: Int) .. 50] $ \i -> do
        -- Insert 2 jobs
        void $
          runM env $
            HL.insertJobsBatch
              [ (defaultJob (mkPayload ("ac-a-" <> T.pack (show i)))) {groupKey = Just "ac-g"}
              , (defaultJob (mkPayload ("ac-b-" <> T.pack (show i)))) {groupKey = Just "ac-g"}
              ]

        -- Claim first
        [jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60

        -- Concurrently: ack first + claim next
        concurrentlyClaimed <-
          mapConcurrently
            id
            [ [] <$ runM env (HL.ackJob jobA)
            , runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
            ]

        -- Ack any jobs claimed during the concurrent phase
        forM_ (concat concurrentlyClaimed) $ \j -> void $ runM env $ HL.ackJob j

        -- Drain remaining
        drainAll (runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]) $
          \j -> void $ runM env $ HL.ackJob j

      check env "after 50 concurrent ack+claim iterations"

    it "concurrent claim + resume: in_flight_until reflects in-flight job" $ \env -> do
      -- Insert 2 jobs, suspend one
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "cr-1")) {groupKey = Just "cr-g"}
      Just job2 <- runM env $ HL.insertJob (defaultJob (mkPayload "cr-2")) {groupKey = Just "cr-g"}
      void $ runM env $ HL.suspendJob @m @registry @payload (primaryKey job2)

      -- Claim the non-suspended job
      [claimed] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60

      -- Concurrently: resume suspended + try to claim (should get nothing due to head-of-line)
      _ <-
        mapConcurrently
          id
          [ void $ runM env $ HL.resumeJob @m @registry @payload (primaryKey job2)
          , void (runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload])
          ]

      check env "after concurrent claim+resume"

      -- Cleanup
      void $ runM env $ HL.ackJob claimed

    it "concurrent claim + heartbeat: in_flight_until reflects extension" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "ch-1")) {groupKey = Just "ch-g"}
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "ch-2")) {groupKey = Just "ch-g"}

      -- Claim one with 30s
      [claimed] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 30

      -- Concurrently: extend to 120s + try second claim
      _ <-
        mapConcurrently
          id
          [ void $ runM env $ HL.setVisibilityTimeout 120 claimed
          , void (runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload])
          ]

      check env "after concurrent claim+heartbeat"

      -- Cleanup
      void $ runM env $ HL.ackJob claimed

    it "concurrent claim + moveToDLQ: in_flight_until correct" $ \env -> do
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "cd-1")) {groupKey = Just "cd-g"}
      void $ runM env $ HL.insertJob (defaultJob (mkPayload "cd-2")) {groupKey = Just "cd-g"}

      -- Claim first
      [jobA] <- runM env $ HL.claimNextVisibleJobs @m @registry @payload 1 60

      -- Concurrently: DLQ first + claim second
      _ <-
        mapConcurrently
          id
          [ void $ runM env $ HL.moveToDLQ "test" jobA
          , void (runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload])
          ]

      -- Drain
      drainAll (runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]) $
        \j -> void $ runM env $ HL.ackJob j

      check env "after concurrent claim+moveToDLQ"

    it "stress: claim+ack storm with HOL detector" $ \env -> do
      -- 20 groups x 10 jobs, 15 workers draining everything.
      -- A database trigger detects any concurrent in-flight jobs within a group.
      let numGroups = 20 :: Int
          jobsPerGroup = 10
          numWorkers = 15

      withConn env $ \conn -> installHolDetector conn schemaName tableName

      forM_ [1 .. numGroups] $ \g -> do
        let groupName = "ift-storm-" <> T.pack (show g)
        void $
          runM env $
            HL.insertJobsBatch
              (replicate jobsPerGroup $ (defaultJob (mkPayload "storm")) {groupKey = Just groupName})

      replicateConcurrently_ numWorkers $
        claimAckLoop (runM env (HL.claimNextVisibleJobs 5 60) :: IO [JobRead payload]) 30 $
          \job -> void $ runM env (HL.ackJob job)

      withConn env $ \conn -> do
        countHolViolations conn schemaName tableName >>= (`shouldBe` 0)
        removeHolDetector conn schemaName tableName
      check env "after claim+ack storm (all in_flight_until should be NULL)"

    it "refreshGroups during concurrent operations" $ \env -> do
      -- 10 groups x 5 jobs
      let numGroups = 10 :: Int
          jobsPerGroup = 5
          numWorkers = 8

      forM_ [1 .. numGroups] $ \g -> do
        let groupName = "ift-refresh-" <> T.pack (show g)
        void $
          runM env $
            HL.insertJobsBatch
              (replicate jobsPerGroup $ (defaultJob (mkPayload "refresh")) {groupKey = Just groupName})

      -- Workers drain while background thread runs refreshGroups in a loop.
      -- Workers signal completion via a counter; refresher stops when all workers are done.
      doneCountRef <- newIORef (0 :: Int)

      _ <-
        mapConcurrently
          id
          $ replicate
            numWorkers
            ( do
                claimAckLoop (runM env (HL.claimNextVisibleJobs 3 60) :: IO [JobRead payload]) 30 $
                  \job -> void $ runM env (HL.ackJob job)
                atomicModifyIORef' doneCountRef (\n -> (n + 1, ()))
            )
            <> [ do
                   -- Background refresher — runs until all workers have finished
                   let refreshLoop = do
                         doneCount <- readIORef doneCountRef
                         if doneCount >= numWorkers
                           then pure ()
                           else do
                             void $ runM env $ HL.refreshGroups @m @registry @payload 0
                             threadDelay 50_000
                             refreshLoop
                   refreshLoop
               ]

      check env "after refreshGroups during concurrent operations"

    it "no orphaned jobs under concurrent insert + claim" $ \env -> do
      let numGroups = 20 :: Int
          jobsPerGroup = 15
          numWorkers = 10
          totalJobs = numGroups * jobsPerGroup

      -- Multiple producers and consumers run concurrently.
      -- Each producer inserts across all groups to maximize trigger
      -- contention between concurrent inserts and claims.
      let numProducers = 5
      ackedRef <- newIORef (0 :: Int)

      _ <-
        mapConcurrently
          id
          $
          -- Producers: each inserts jobsPerGroup/numProducers jobs per group
          [ forM_ [1 .. jobsPerGroup `div` numProducers] $ \j ->
              forM_ [1 .. numGroups] $ \g -> do
                let groupName = "orphan-" <> T.pack (show g)
                    label = "o-" <> T.pack (show p) <> "-" <> T.pack (show g) <> "-" <> T.pack (show j)
                void $ runM env $ HL.insertJob (defaultJob (mkPayload label)) {groupKey = Just groupName}
          | p <- [1 .. numProducers]
          ]
            <>
            -- Consumers: claim and ack
            replicate
              numWorkers
              ( claimAckLoop (runM env (HL.claimNextVisibleJobs 3 60) :: IO [JobRead payload]) 80 $ \job -> do
                  void $ runM env (HL.ackJob job)
                  atomicModifyIORef' ackedRef (\n -> (n + 1, ()))
              )

      -- Assert zero jobs remain
      remaining <- runM env $ HL.countJobs @m @registry @payload
      ackedTotal <- readIORef ackedRef
      when (remaining /= 0) $
        expectationFailure $
          "Orphaned jobs: "
            <> show remaining
            <> " jobs remain after draining."
            <> " Acked "
            <> show ackedTotal
            <> " of "
            <> show totalJobs
            <> " total."
      ackedTotal `shouldBe` totalJobs
      check env "after no-orphan concurrent insert+claim stress test"

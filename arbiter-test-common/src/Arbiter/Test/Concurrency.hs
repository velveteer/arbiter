{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

-- | Parameterized concurrency test suite that works with any MonadArbiter implementation.
module Arbiter.Test.Concurrency
  ( concurrencySpec
  , raceConditionSpec
  , installHolDetector
  , countHolViolations
  , removeHolDetector
  ) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf)
import Arbiter.Core.QueueRegistry (TableForPayload)
import Control.Concurrent (threadDelay)
import Control.Monad (forM, forM_, replicateM, replicateM_, void, when)
import Data.Foldable (traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.List (nub, sort)
import Data.List.NonEmpty qualified as NE
import Data.Set qualified as Set
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple qualified as PG
import GHC.TypeLits (KnownSymbol)
import Test.Hspec
import UnliftIO.Async (mapConcurrently, replicateConcurrently_)

import Arbiter.Test.StateMachine (holInstallSql, holRemoveSql, holViolTbl)

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

-- | Install the gap-free HOL violation detector on a raw connection. The trigger
-- definition is shared with the state-machine suite.
installHolDetector :: PG.Connection -> Text -> Text -> IO ()
installHolDetector conn schemaName tableName =
  traverse_ (void . PG.execute_ conn . fromString . T.unpack) (holInstallSql schemaName tableName)

-- | Count violations detected by the HOL detector trigger.
countHolViolations :: PG.Connection -> Text -> Text -> IO Int64
countHolViolations conn schemaName tableName = do
  [PG.Only count] <-
    PG.query_ conn $
      fromString $
        T.unpack $
          "SELECT count(*)::bigint FROM " <> holViolTbl schemaName tableName
  pure count

-- | Remove the HOL detector trigger, function, and violations table.
removeHolDetector :: PG.Connection -> Text -> Text -> IO ()
removeHolDetector conn schemaName tableName =
  traverse_ (void . PG.execute_ conn . fromString . T.unpack) (holRemoveSql schemaName tableName)

-- | Parameterized concurrency test suite.
--
-- IMPORTANT: These tests require a connection pool with multiple connections (at least 10)
-- to properly test concurrent access patterns.
--
-- The payload type must be registered in the registry and satisfy JobPayload constraints.
concurrencySpec
  :: forall payload m env
   . ( JobPayload payload
     , KnownSymbol (TableForPayload payload (RegistryOf m))
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
    it "concurrent workers claim disjoint ungrouped jobs" $ \env -> do
      -- Insert 6 ungrouped jobs, remembering their ids.
      inserted <- runM env $ HL.insertJobsBatch (replicate 6 $ defaultJob (mkMessage "Concurrent"))
      let insertedIds = Set.fromList (map primaryKey inserted)

      [c1, c2, c3] <-
        mapConcurrently
          (\_ -> runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload])
          [1 :: Int, 2, 3]

      let raced = map primaryKey (c1 <> c2 <> c3)
      -- No job claimed twice in the racy pass.
      raced `shouldBe` nub raced
      -- Drive any jobs missed by the race to completion (claimed jobs hold a
      -- 60s lease, so a follow-up claim only surfaces the still-unclaimed ones).
      drainedRef <- newIORef ([] :: [Int64])
      drainAll (runM env (HL.claimNextVisibleJobs 6 60) :: IO [JobRead payload]) $ \j ->
        atomicModifyIORef' drainedRef (\acc -> (primaryKey j : acc, ()))
      drained <- readIORef drainedRef
      -- Real progress: every inserted job was claimed exactly once across the
      -- racing workers and the drain.
      let allClaimed = raced <> drained
      allClaimed `shouldBe` nub allClaimed
      Set.fromList allClaimed `shouldBe` insertedIds

    it "concurrent workers respect per-group ordering for grouped jobs" $ \env -> do
      -- 500 iterations: seed a group, race 10 workers to claim + concurrent
      -- inserts to the same group. Per-group ordering means exactly 1 claim per round.
      let numIterations = 500
          numWorkers = 10
          seedPerRound = 5
          insertersPerRound = 2
      violationRef <- newIORef (0 :: Int)
      insertedRef <- newIORef (0 :: Int)
      processedRef <- newIORef (Set.empty :: Set.Set Int64)

      forM_ [(1 :: Int) .. numIterations] $ \_ -> do
        -- Insert 5 jobs in one group
        void $
          runM env $
            HL.insertJobsBatch (replicate seedPerRound $ (defaultJob (mkMessage "Grouped")) {groupKey = Just "hol-stress"})
        atomicModifyIORef' insertedRef (\n -> (n + seedPerRound + insertersPerRound, ()))

        -- Race N workers to claim + 2 concurrent inserters to the same group.
        -- The INSERT triggers fire concurrently with the claim CTEs,
        -- exercising the groups row lock contention path.
        results <-
          mapConcurrently
            id
            $ map
              (\_ -> runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload])
              [(1 :: Int) .. numWorkers]
              <> replicate
                insertersPerRound
                (void (runM env $ HL.insertJob (defaultJob (mkMessage "concurrent")) {groupKey = Just "hol-stress"}) >> pure [])

        -- Ordering invariant: at most 1 claim per group. totalClaimed == 0 is
        -- normal when the INSERT trigger's groups row lock causes claims to
        -- skip the group (FOR UPDATE SKIP LOCKED). totalClaimed > 1 is a
        -- real ordering violation.
        let totalClaimed = sum (map length results)
        when (totalClaimed > 1) $
          atomicModifyIORef' violationRef (\n -> (n + 1, ()))

        -- Ack the racy claims, then drain the rest, recording every processed id.
        let record j = atomicModifyIORef' processedRef (\s -> (Set.insert (primaryKey j) s, ()))
        forM_ (concat results) $ \j -> record j >> void (runM env (HL.ackJob j))
        drainAll (runM env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead payload]) $
          \j -> record j >> void (runM env (HL.ackJob j))

      violations <- readIORef violationRef
      violations `shouldBe` 0
      -- Total progress: every job inserted across all rounds was claimed and
      -- acked exactly once, so a zero-progress (nothing ever claimed) run fails.
      inserted <- readIORef insertedRef
      processed <- readIORef processedRef
      Set.size processed `shouldBe` inserted

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

-- | Aggressive race condition tests designed to surface concurrency bugs.
--
-- These tests use high contention and many iterations to maximize the
-- probability of hitting race windows. They stress the system with:
-- - Many concurrent workers (40+)
-- - Large job counts (500+)
-- - Tight timing windows
-- - Real database contention
raceConditionSpec
  :: forall payload m env
   . ( JobPayload payload
     , KnownSymbol (TableForPayload payload (RegistryOf m))
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

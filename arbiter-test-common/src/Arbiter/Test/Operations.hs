{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial -Wno-incomplete-uni-patterns #-}

-- | Parameterized operations test suite that works with any MonadArbiter implementation.
--
-- This module provides a single test suite that can be instantiated for both
-- postgresql-simple and Orville implementations, eliminating code duplication.
module Arbiter.Test.Operations
  ( operationsSpec
  ) where

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema)
import Arbiter.Core.HighLevel (SetVisibilityResult (..))
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Types
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.QueueRegistry (TableForPayload)
import Control.Monad (forM, forM_, void)
import Data.Aeson qualified as Aeson
import Data.Int (Int64)
import Data.List (nub, sort)
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime (..), addUTCTime, getCurrentTime, picosecondsToDiffTime)
import GHC.TypeLits (KnownSymbol)
import Test.Hspec
import UnliftIO (MonadUnliftIO)

-- | Parameterized operations test suite.
--
-- This function creates a test suite that works with any MonadArbiter implementation.
-- It takes a runner function that knows how to execute monad actions for a specific
-- implementation.
--
-- The payload type must be registered in the registry and satisfy JobPayload constraints.
operationsSpec
  :: forall payload registry env m
   . ( Eq payload
     , HasArbiterSchema m registry
     , JobPayload payload
     , KnownSymbol (TableForPayload payload registry)
     , MonadArbiter m
     , MonadUnliftIO m
     , Show payload
     )
  => (Text -> payload)
  -- ^ Constructor for a simple test message payload
  -> (forall a. env -> m a -> IO a)
  -- ^ Runner function to execute monad actions (e.g., runSimpleDb env or runOrvilleTest env)
  -> SpecWith env
operationsSpec mkMessage runM = do
  -- Test helpers
  let truncateToMicros :: UTCTime -> UTCTime
      truncateToMicros (UTCTime d t) =
        let micros = floor (t * 1e6) :: Integer
         in UTCTime d (picosecondsToDiffTime (micros * 1000000))
      claimJobs env n = runM env (HL.claimNextVisibleJobs n 60) :: IO [JobRead payload]
      getJob env jobId = runM env (HL.getJobById @m @registry @payload jobId)
      assertSuspended env jobId = do
        Just j <- getJob env jobId
        suspended j `shouldBe` True
      assertNotSuspended env jobId = do
        Just j <- getJob env jobId
        suspended j `shouldBe` False
      assertGone env jobId = do
        j <- getJob env jobId
        j `shouldBe` Nothing
      dlqAll env = runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]

  describe "claimNextVisibleJobs" $ do
    it "claims jobs in priority order" $ \env -> do
      -- Insert jobs with different priorities
      let highPriority =
            (defaultGroupedJob "claim-priority-test" (mkMessage "High"))
              { priority = 0
              }
          lowPriority =
            (defaultGroupedJob "claim-priority-test" (mkMessage "Low"))
              { priority = 10
              }

      void $ runM env (HL.insertJob lowPriority)
      void $ runM env (HL.insertJob highPriority)

      -- Claim one job
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1
      payload (head claimed) `shouldBe` mkMessage "High"

    it "claims jobs from different groups" $ \env -> do
      let job1 = (defaultJob (mkMessage "G1")) {groupKey = Just "group1"}
          job2 = (defaultJob (mkMessage "G2")) {groupKey = Just "group2"}

      void $ runM env (HL.insertJob job1)
      void $ runM env (HL.insertJob job2)

      claimed <- runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]

      length claimed `shouldBe` 2
      map groupKey claimed `shouldMatchList` [Just "group1", Just "group2"]

    it "respects head-of-line blocking per group" $ \env -> do
      -- Insert two jobs in the same group
      let job1 = (defaultJob (mkMessage "First")) {groupKey = Just "claim-hol-test"}
          job2 = (defaultJob (mkMessage "Second")) {groupKey = Just "claim-hol-test"}

      void $ runM env (HL.insertJob job1)
      void $ runM env (HL.insertJob job2)

      -- Claim only 1 job - should get the first job
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1
      payload (head claimed) `shouldBe` mkMessage "First"

      -- The second job should still be in the queue but not claimable yet
      -- (because the first job hasn't been acked)
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

    it "ungrouped jobs can be claimed in parallel" $ \env -> do
      -- Insert two ungrouped jobs
      let job1 = defaultJob (mkMessage "Ungrouped1")
          job2 = defaultJob (mkMessage "Ungrouped2")

      void $ runM env (HL.insertJob job1)
      void $ runM env (HL.insertJob job2)

      -- Both should be claimable at once
      claimed <- runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]

      length claimed `shouldBe` 2
      map groupKey claimed `shouldMatchList` [Nothing, Nothing]

    it "ungrouped and grouped jobs compete fairly by insertion order" $ \env -> do
      -- Insert interleaved: ungrouped get lower IDs than grouped
      void $ runM env (HL.insertJob (defaultJob (mkMessage "U1")))
      void $ runM env (HL.insertJob (defaultGroupedJob "fairness-single-a" (mkMessage "G1")))
      void $ runM env (HL.insertJob (defaultJob (mkMessage "U2")))
      void $ runM env (HL.insertJob (defaultGroupedJob "fairness-single-b" (mkMessage "G2")))
      void $ runM env (HL.insertJob (defaultJob (mkMessage "U3")))

      -- Claim only 3 out of 5 — should get the 3 with lowest IDs (FIFO)
      claimed <- runM env (HL.claimNextVisibleJobs 3 60) :: IO [JobRead payload]

      length claimed `shouldBe` 3
      let ungroupedCount = length $ filter (\j -> groupKey j == Nothing) claimed
      let groupedCount = length $ filter (\j -> groupKey j /= Nothing) claimed
      -- U1 (id=1), G1 (id=2), U2 (id=3) claimed; G2 (id=4) and U3 (id=5) left
      ungroupedCount `shouldBe` 2
      groupedCount `shouldBe` 1
      map payload claimed `shouldBe` [mkMessage "U1", mkMessage "G1", mkMessage "U2"]

    it "increments attempts on claim" $ \env -> do
      let job = (defaultJob (mkMessage "Test")) {groupKey = Just "claim-attempts-test"}

      Just inserted <- runM env (HL.insertJob job)
      attempts inserted `shouldBe` 0

      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      attempts (head claimed) `shouldBe` 1

    it "claimed jobs are not re-claimable" $ \env -> do
      let job = (defaultJob (mkMessage "Test")) {groupKey = Just "claim-visibility-test"}

      void $ runM env (HL.insertJob job)

      -- Claim the job
      claimed1 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed1 `shouldBe` 1

      -- Try to claim again immediately - should get nothing
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

  describe "ackJob" $ do
    it "removes a job from the queue" $ \env -> do
      let job = (defaultJob (mkMessage "Test")) {groupKey = Just "ack-remove-test"}

      void $ runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1

      -- Acknowledge the job
      void $ runM env (HL.ackJob (head claimed))

      -- Try to claim again - should get nothing (job was deleted)
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

    it "allows next job in group to be claimed after ack" $ \env -> do
      let job1 = (defaultJob (mkMessage "First")) {groupKey = Just "ack-next-test"}
          job2 = (defaultJob (mkMessage "Second")) {groupKey = Just "ack-next-test"}

      void $ runM env (HL.insertJob job1)
      void $ runM env (HL.insertJob job2)

      -- Claim first job
      claimed1 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed1 `shouldBe` 1
      payload (head claimed1) `shouldBe` mkMessage "First"

      -- Ack it
      void $ runM env (HL.ackJob (head claimed1))

      -- Now second job should be claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1
      payload (head claimed2) `shouldBe` mkMessage "Second"

  describe "setVisibilityTimeout" $ do
    it "extends visibility timeout for retry" $ \env -> do
      let job = (defaultJob (mkMessage "Test")) {groupKey = Just "visibility-extend-test"}

      void $ runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1

      -- Extend visibility timeout and verify it succeeded
      result1 <- runM env (HL.setVisibilityTimeout 120 (head claimed))
      result1 `shouldBe` 1

      -- Job should still not be claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- Set a zeroed visibility timeout and verify it succeeded
      result2 <- runM env (HL.setVisibilityTimeout 0 (head claimed))
      result2 `shouldBe` 1

      -- Should be able to re-claim job now
      claimed' <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed' `shouldBe` 1

  describe "ackJobsBatch" $ do
    it "removes multiple jobs in a single operation" $ \env -> do
      let jobs = [defaultJob (mkMessage $ "Job" <> T.pack (show i)) | i <- [1 .. 5 :: Int]]

      void $ runM env (HL.insertJobsBatch jobs)
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]

      length claimed `shouldBe` 5

      -- Ack all jobs in batch
      deleted <- runM env (HL.ackJobsBatch claimed)
      deleted `shouldBe` 5

      -- Try to claim again - should get nothing (all jobs deleted)
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

    it "allows next jobs in groups to be claimed after batch ack" $ \env -> do
      let batch1 =
            [ (defaultJob (mkMessage "First-A")) {groupKey = Just "batch-ack-test-1"}
            , (defaultJob (mkMessage "First-B")) {groupKey = Just "batch-ack-test-2"}
            ]
          batch2 =
            [ (defaultJob (mkMessage "Second-A")) {groupKey = Just "batch-ack-test-1"}
            , (defaultJob (mkMessage "Second-B")) {groupKey = Just "batch-ack-test-2"}
            ]

      void $ runM env (HL.insertJobsBatch (batch1 <> batch2))

      -- Claim first jobs from each group
      claimed1 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed1 `shouldBe` 2

      -- Ack them in batch
      acked <- runM env (HL.ackJobsBatch claimed1)
      acked `shouldBe` 2

      -- Now second jobs should be claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 2

  describe "setVisibilityTimeoutBatch" $ do
    it "extends visibility timeout for multiple jobs" $ \env -> do
      let jobs = [defaultJob (mkMessage $ "Job" <> T.pack (show i)) | i <- [1 .. 3 :: Int]]

      void $ runM env (HL.insertJobsBatch jobs)
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]

      length claimed `shouldBe` 3

      -- Extend visibility for all jobs in batch
      results <- runM env (HL.setVisibilityTimeoutBatch 120 claimed)
      let successes = [() | VisibilityExtended _ <- results]
      length successes `shouldBe` 3

      -- Jobs should still not be claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- Reset visibility to zero for all
      _ <- runM env (HL.setVisibilityTimeoutBatch 0 claimed)

      -- Should be able to re-claim jobs now
      claimed' <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed' `shouldBe` 3

    it "returns JobGone for manually acked jobs (not an error)" $ \env -> do
      -- This test ensures the heartbeat logic correctly handles the case where
      -- a batch handler manually acks some jobs before the heartbeat fires
      let jobs = [defaultJob (mkMessage $ "Job" <> T.pack (show i)) | i <- [1 .. 5 :: Int]]

      void $ runM env (HL.insertJobsBatch jobs)
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 5

      -- Simulate handler manually acking 2 jobs mid-processing
      let (toAck, stillProcessing) = splitAt 2 claimed
      forM_ toAck $ \job -> void $ runM env (HL.ackJob job)

      -- Now the heartbeat fires
      results <- runM env (HL.setVisibilityTimeoutBatch 120 claimed)

      -- The 2 acked jobs should be JobGone
      let goneJobs = [jobId | JobGone jobId <- results]
      length goneJobs `shouldBe` 2

      -- The 3 still-processing jobs should be VisibilityExtended
      let successJobs = [jobId | VisibilityExtended jobId <- results]
      length successJobs `shouldBe` 3

      -- Verify the IDs match
      sort goneJobs `shouldBe` sort (map primaryKey toAck)
      sort successJobs `shouldBe` sort (map primaryKey stillProcessing)

  describe "Job Deduplication" $ do
    it "No dedup key allows multiple jobs with same payload" $ \env -> do
      let job1 =
            (defaultGroupedJob "dedup-always-test" (mkMessage "Same"))
              { dedupKey = Nothing
              }
          job2 =
            (defaultGroupedJob "dedup-always-test" (mkMessage "Same"))
              { dedupKey = Nothing
              }

      Just inserted1 <- runM env (HL.insertJob job1)
      Just inserted2 <- runM env (HL.insertJob job2)

      -- Both jobs should be inserted with different IDs
      primaryKey inserted1 `shouldNotBe` primaryKey inserted2
      payload inserted1 `shouldBe` mkMessage "Same"
      payload inserted2 `shouldBe` mkMessage "Same"

    it "IgnoreDuplicate returns Nothing on conflict" $ \env -> do
      let job1 =
            (defaultGroupedJob "dedup-ignore-test-1" (mkMessage "First"))
              { dedupKey = Just (IgnoreDuplicate "unique-key-1")
              }
          job2 =
            (defaultGroupedJob "dedup-ignore-test-2" (mkMessage "Second"))
              { dedupKey = Just (IgnoreDuplicate "unique-key-1") -- Same dedup key!
              }

      Just inserted1 <- runM env (HL.insertJob job1)
      inserted2 <- runM env (HL.insertJob job2)

      -- Second insert should return Nothing (conflict)
      inserted2 `shouldBe` Nothing
      payload inserted1 `shouldBe` mkMessage "First"
    it "IgnoreDuplicate with different keys creates separate jobs" $ \env -> do
      let job1 =
            (defaultGroupedJob "dedup-diffkey-test-1" (mkMessage "Job1"))
              { dedupKey = Just (IgnoreDuplicate "key-1")
              }
          job2 =
            (defaultGroupedJob "dedup-diffkey-test-2" (mkMessage "Job2"))
              { dedupKey = Just (IgnoreDuplicate "key-2") -- Different dedup key
              }

      Just inserted1 <- runM env (HL.insertJob job1)
      Just inserted2 <- runM env (HL.insertJob job2)

      -- Both should be inserted
      primaryKey inserted1 `shouldNotBe` primaryKey inserted2
      payload inserted1 `shouldBe` mkMessage "Job1"
      payload inserted2 `shouldBe` mkMessage "Job2"

    it "ReplaceDuplicate replaces existing job completely" $ \env -> do
      let job1 =
            (defaultGroupedJob "dedup-replace-test-1" (mkMessage "Original"))
              { priority = 10
              , dedupKey = Just (ReplaceDuplicate "replace-key-1")
              }
          job2 =
            (defaultGroupedJob "dedup-replace-test-2" (mkMessage "Replacement"))
              { priority = 5
              , dedupKey = Just (ReplaceDuplicate "replace-key-1") -- Same dedup key
              }

      Just inserted1 <- runM env (HL.insertJob job1)
      Just inserted2 <- runM env (HL.insertJob job2)

      -- Should be same job ID (replaced)
      primaryKey inserted1 `shouldBe` primaryKey inserted2

      -- But payload and group should be updated to new values
      payload inserted2 `shouldBe` mkMessage "Replacement"
      groupKey inserted2 `shouldBe` Just "dedup-replace-test-2"
      priority inserted2 `shouldBe` 5

      -- Attempts should be reset
      attempts inserted2 `shouldBe` 0

    it "ReplaceDuplicate resets job state (attempts, errors)" $ \env -> do
      let job1 =
            (defaultGroupedJob "dedup-reset-test-1" (mkMessage "First"))
              { dedupKey = Just (ReplaceDuplicate "reset-key")
              }

      Just inserted1 <- runM env (HL.insertJob job1)

      -- Claim and fail the job to increment attempts
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      claimed `shouldNotBe` []
      let claimedJob = head claimed
      attempts claimedJob `shouldBe` 1

      -- Update with error
      void $ runM env (HL.updateJobForRetry 1 "Test error" claimedJob)

      -- Now insert replacement job
      let job2 =
            (defaultGroupedJob "dedup-reset-test-2" (mkMessage "Replacement"))
              { dedupKey = Just (ReplaceDuplicate "reset-key")
              }

      Just inserted2 <- runM env (HL.insertJob job2)

      -- Job should be replaced with fresh state
      primaryKey inserted1 `shouldBe` primaryKey inserted2
      attempts inserted2 `shouldBe` 0
      lastError inserted2 `shouldBe` Nothing
      payload inserted2 `shouldBe` mkMessage "Replacement"

    it "ReplaceDuplicate returns Nothing when job is actively in-flight (first attempt)" $ \env -> do
      let job1 =
            (defaultGroupedJob "dedup-inflight-test-1" (mkMessage "Original"))
              { dedupKey = Just (ReplaceDuplicate "inflight-test-key")
              }

      Just _inserted1 <- runM env (HL.insertJob job1)

      -- Claim the job (now in first attempt: attempts=1, last_error=NULL, not_visible_until > NOW)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      let claimedJob = head claimed
      attempts claimedJob `shouldBe` 1
      lastError claimedJob `shouldBe` Nothing

      -- Try to replace while actively in-flight - should return Nothing (unsafe to replace)
      let job2 =
            (defaultGroupedJob "dedup-inflight-test-2" (mkMessage "Replacement"))
              { dedupKey = Just (ReplaceDuplicate "inflight-test-key")
              }

      replaced <- runM env (HL.insertJob job2)
      replaced `shouldBe` Nothing

    it "ReplaceDuplicate succeeds when job is in retry backoff (has last_error)" $ \env -> do
      let job1 =
            (defaultGroupedJob "dedup-backoff-test-1" (mkMessage "Original"))
              { dedupKey = Just (ReplaceDuplicate "retry-backoff-key")
              }

      Just inserted1 <- runM env (HL.insertJob job1)

      -- Claim the job
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      let claimedJob = head claimed

      -- Update for retry (sets last_error and not_visible_until to future)
      rowsUpdated <- runM env (HL.updateJobForRetry 5 "Simulated failure" claimedJob)
      rowsUpdated `shouldBe` 1

      -- Now try to replace - should succeed because last_error IS NOT NULL (safe to replace)
      let job2 =
            (defaultGroupedJob "dedup-backoff-test-2" (mkMessage "Replacement"))
              { dedupKey = Just (ReplaceDuplicate "retry-backoff-key")
              }

      Just replaced <- runM env (HL.insertJob job2)

      -- Should be same ID but with fresh state
      primaryKey replaced `shouldBe` primaryKey inserted1
      payload replaced `shouldBe` mkMessage "Replacement"
      attempts replaced `shouldBe` 0
      lastError replaced `shouldBe` Nothing

    it "Dedup key only applies to jobs in queue (not after ack)" $ \env -> do
      let job1 =
            (defaultGroupedJob "dedup-ack-test-1" (mkMessage "First"))
              { dedupKey = Just (IgnoreDuplicate "ack-test-key")
              }

      Just inserted1 <- runM env (HL.insertJob job1)

      -- Claim and ack the job (removes from queue)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      void $ runM env (HL.ackJob (head claimed))

      -- Insert another job with same dedup key
      let job2 =
            (defaultGroupedJob "dedup-ack-test-2" (mkMessage "Second"))
              { dedupKey = Just (IgnoreDuplicate "ack-test-key")
              }

      Just inserted2 <- runM env (HL.insertJob job2)

      -- Should create a new job (old one was acked and removed)
      primaryKey inserted1 `shouldNotBe` primaryKey inserted2
      payload inserted2 `shouldBe` mkMessage "Second"

    it "Mixed dedup strategies work independently" $ \env -> do
      let noDedupJob =
            (defaultGroupedJob "dedup-mixed-test-1" (mkMessage "NoDedupe"))
              { dedupKey = Nothing
              }
          ignoreJob1 =
            (defaultGroupedJob "dedup-mixed-test-2" (mkMessage "Ignore1"))
              { dedupKey = Just (IgnoreDuplicate "ignore-key")
              }
          ignoreJob2 =
            (defaultGroupedJob "dedup-mixed-test-3" (mkMessage "Ignore2"))
              { dedupKey = Just (IgnoreDuplicate "ignore-key") -- Duplicate
              }
          replaceJob1 =
            (defaultGroupedJob "dedup-mixed-test-4" (mkMessage "Replace1"))
              { dedupKey = Just (ReplaceDuplicate "replace-key")
              }
          replaceJob2 =
            (defaultGroupedJob "dedup-mixed-test-5" (mkMessage "Replace2"))
              { dedupKey = Just (ReplaceDuplicate "replace-key") -- Duplicate
              }

      Just noDedup <- runM env (HL.insertJob noDedupJob)
      Just ignore1 <- runM env (HL.insertJob ignoreJob1)
      ignore2 <- runM env (HL.insertJob ignoreJob2) -- Should return Nothing
      Just replace1 <- runM env (HL.insertJob replaceJob1)
      Just replace2 <- runM env (HL.insertJob replaceJob2) -- Should replace replace1

      -- No dedup key creates unique job
      payload noDedup `shouldBe` mkMessage "NoDedupe"

      -- IgnoreDuplicate returns Nothing on conflict
      ignore2 `shouldBe` Nothing
      payload ignore1 `shouldBe` mkMessage "Ignore1"

      -- ReplaceDuplicate replaces
      primaryKey replace1 `shouldBe` primaryKey replace2
      payload replace2 `shouldBe` mkMessage "Replace2"

    it "Dedup works with ungrouped jobs" $ \env -> do
      let job1 =
            (defaultJob (mkMessage "Ungrouped1"))
              { dedupKey = Just (IgnoreDuplicate "ungrouped-key")
              }
          job2 =
            (defaultJob (mkMessage "Ungrouped2"))
              { dedupKey = Just (IgnoreDuplicate "ungrouped-key")
              }

      Just inserted1 <- runM env (HL.insertJob job1)
      inserted2 <- runM env (HL.insertJob job2)

      -- Second insert should return Nothing
      inserted2 `shouldBe` Nothing
      payload inserted1 `shouldBe` mkMessage "Ungrouped1"

    it "IgnoreDuplicate and ReplaceDuplicate with same key conflict" $ \env -> do
      let job1 =
            (defaultJob (mkMessage "IgnoreFirst"))
              { dedupKey = Just (IgnoreDuplicate "cross-strategy-key")
              }
          job2 =
            (defaultJob (mkMessage "ReplaceSecond"))
              { dedupKey = Just (ReplaceDuplicate "cross-strategy-key")
              }

      Just inserted1 <- runM env (HL.insertJob job1)
      Just inserted2 <- runM env (HL.insertJob job2)

      -- ReplaceDuplicate should conflict with the existing key and replace it
      primaryKey inserted1 `shouldBe` primaryKey inserted2
      payload inserted2 `shouldBe` mkMessage "ReplaceSecond"

  describe "insertJobsBatch Deduplication" $ do
    it "batch insert without dedup keys inserts all jobs" $ \env -> do
      let jobs =
            [ defaultJob (mkMessage $ "Batch" <> T.pack (show i))
            | i <- [1 .. 5 :: Int]
            ]

      inserted <- runM env (HL.insertJobsBatch jobs)
      length inserted `shouldBe` 5

    it "batch insert with IgnoreDuplicate skips conflicts" $ \env -> do
      -- Pre-insert a job with a dedup key
      let existingJob =
            (defaultJob (mkMessage "Existing"))
              { dedupKey = Just (IgnoreDuplicate "batch-ignore-key")
              }
      Just _ <- runM env (HL.insertJob existingJob)

      -- Batch insert: one conflicting, two new
      let batchJobs =
            [ (defaultJob (mkMessage "Conflict"))
                { dedupKey = Just (IgnoreDuplicate "batch-ignore-key")
                }
            , defaultJob (mkMessage "New1")
            , defaultJob (mkMessage "New2")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- The conflicting job is skipped; only the 2 new ones are returned
      length inserted `shouldBe` 2
      map payload inserted `shouldMatchList` [mkMessage "New1", mkMessage "New2"]

    it "batch insert with ReplaceDuplicate replaces existing job" $ \env -> do
      -- Pre-insert a job with a dedup key
      let existingJob =
            (defaultJob (mkMessage "Original"))
              { dedupKey = Just (ReplaceDuplicate "batch-replace-key")
              , priority = 10
              }
      Just original <- runM env (HL.insertJob existingJob)

      -- Batch insert with a replacement
      let batchJobs =
            [ (defaultJob (mkMessage "Replacement"))
                { dedupKey = Just (ReplaceDuplicate "batch-replace-key")
                , priority = 5
                }
            , defaultJob (mkMessage "Other")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      length inserted `shouldBe` 2

      -- Find the replaced job (same ID as original)
      let replacedJob = filter (\j -> primaryKey j == primaryKey original) inserted
      length replacedJob `shouldBe` 1
      payload (head replacedJob) `shouldBe` mkMessage "Replacement"
      priority (head replacedJob) `shouldBe` 5

    it "batch insert with mixed strategies" $ \env -> do
      -- Pre-insert two jobs
      let ignoreExisting =
            (defaultJob (mkMessage "IgnoreExisting"))
              { dedupKey = Just (IgnoreDuplicate "batch-mixed-ignore")
              }
          replaceExisting =
            (defaultJob (mkMessage "ReplaceExisting"))
              { dedupKey = Just (ReplaceDuplicate "batch-mixed-replace")
              }
      Just _ <- runM env (HL.insertJob ignoreExisting)
      Just origReplace <- runM env (HL.insertJob replaceExisting)

      -- Batch: conflict on ignore (skipped), conflict on replace (updated),
      -- plus one job with no dedup key (always inserted)
      let batchJobs =
            [ (defaultJob (mkMessage "IgnoreConflict"))
                { dedupKey = Just (IgnoreDuplicate "batch-mixed-ignore")
                }
            , (defaultJob (mkMessage "ReplaceConflict"))
                { dedupKey = Just (ReplaceDuplicate "batch-mixed-replace")
                }
            , defaultJob (mkMessage "NoDedupJob")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- IgnoreDuplicate skipped, ReplaceDuplicate replaced, no-dedup inserted
      length inserted `shouldBe` 2
      map payload inserted `shouldMatchList` [mkMessage "ReplaceConflict", mkMessage "NoDedupJob"]

      -- Verify the replacement happened on the same row
      let replacedJob = filter (\j -> primaryKey j == primaryKey origReplace) inserted
      length replacedJob `shouldBe` 1
      payload (head replacedJob) `shouldBe` mkMessage "ReplaceConflict"

    it "batch insert cross-strategy conflict (IgnoreDuplicate then ReplaceDuplicate)" $ \env -> do
      -- Pre-insert with IgnoreDuplicate
      let existingJob =
            (defaultJob (mkMessage "IgnoreFirst"))
              { dedupKey = Just (IgnoreDuplicate "batch-cross-key")
              }
      Just original <- runM env (HL.insertJob existingJob)

      -- Batch insert with ReplaceDuplicate on the same key
      let batchJobs =
            [ (defaultJob (mkMessage "ReplaceSecond"))
                { dedupKey = Just (ReplaceDuplicate "batch-cross-key")
                }
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- ReplaceDuplicate should replace the existing IgnoreDuplicate job
      length inserted `shouldBe` 1
      primaryKey (head inserted) `shouldBe` primaryKey original
      payload (head inserted) `shouldBe` mkMessage "ReplaceSecond"

    it "batch ReplaceDuplicate does not replace in-flight job" $ \env -> do
      -- Insert and claim a job (making it in-flight on first attempt)
      let existingJob =
            (defaultJob (mkMessage "InFlight"))
              { dedupKey = Just (ReplaceDuplicate "batch-inflight-key")
              }
      Just _ <- runM env (HL.insertJob existingJob)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      -- Job is now in-flight: attempts=1, not_visible_until > NOW, last_error IS NULL

      -- Batch insert with ReplaceDuplicate on the same key
      let batchJobs =
            [ (defaultJob (mkMessage "Replacement"))
                { dedupKey = Just (ReplaceDuplicate "batch-inflight-key")
                }
            , defaultJob (mkMessage "Other")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- The in-flight job should NOT be replaced; only "Other" is inserted
      length inserted `shouldBe` 1
      payload (head inserted) `shouldBe` mkMessage "Other"

    it "duplicate IgnoreDuplicate keys within batch: first wins" $ \env -> do
      let batchJobs =
            [ (defaultJob (mkMessage "First"))
                { dedupKey = Just (IgnoreDuplicate "ign-ign-key")
                }
            , (defaultJob (mkMessage "Second"))
                { dedupKey = Just (IgnoreDuplicate "ign-ign-key")
                }
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      length inserted `shouldBe` 1
      payload (head inserted) `shouldBe` mkMessage "First"

    it "duplicate ReplaceDuplicate keys within batch: last wins" $ \env -> do
      let batchJobs =
            [ (defaultJob (mkMessage "First"))
                { dedupKey = Just (ReplaceDuplicate "rep-rep-key")
                }
            , (defaultJob (mkMessage "Second"))
                { dedupKey = Just (ReplaceDuplicate "rep-rep-key")
                }
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      length inserted `shouldBe` 1
      payload (head inserted) `shouldBe` mkMessage "Second"

    it "mixed strategies within batch: ReplaceDuplicate takes precedence" $ \env -> do
      let batchJobs =
            [ (defaultJob (mkMessage "First"))
                { dedupKey = Just (IgnoreDuplicate "mixed-key")
                }
            , defaultJob (mkMessage "Middle")
            , (defaultJob (mkMessage "Last"))
                { dedupKey = Just (ReplaceDuplicate "mixed-key")
                }
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- First is dropped (Replace takes precedence), Middle and Last kept
      length inserted `shouldBe` 2
      map payload inserted `shouldMatchList` [mkMessage "Middle", mkMessage "Last"]

      let dedupJobs = filter (\j -> dedupKey j /= Nothing) inserted
      length dedupJobs `shouldBe` 1
      payload (head dedupJobs) `shouldBe` mkMessage "Last"
      dedupKey (head dedupJobs) `shouldBe` Just (ReplaceDuplicate "mixed-key")

    it "batch ReplaceDuplicate succeeds when job is in retry backoff" $ \env -> do
      -- Insert, claim, then fail the job (putting it in retry backoff)
      let existingJob =
            (defaultJob (mkMessage "WillFail"))
              { dedupKey = Just (ReplaceDuplicate "batch-backoff-key")
              }
      Just original <- runM env (HL.insertJob existingJob)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.updateJobForRetry 5 "Simulated failure" (head claimed))

      -- Batch insert with ReplaceDuplicate — should succeed because last_error IS NOT NULL
      let batchJobs =
            [ (defaultJob (mkMessage "FreshReplacement"))
                { dedupKey = Just (ReplaceDuplicate "batch-backoff-key")
                }
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      length inserted `shouldBe` 1
      primaryKey (head inserted) `shouldBe` primaryKey original
      payload (head inserted) `shouldBe` mkMessage "FreshReplacement"
      attempts (head inserted) `shouldBe` 0
      lastError (head inserted) `shouldBe` Nothing

  describe "updateJobForRetry" $ do
    it "updates job with error message and visibility timeout" $ \env -> do
      let job = (defaultJob (mkMessage "Test")) {groupKey = Just "retry-update-test"}

      void $ runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1
      let claimedJob = head claimed
      attempts claimedJob `shouldBe` 1
      lastError claimedJob `shouldBe` Nothing

      -- Update for retry with error
      retryResult <- runM env (HL.updateJobForRetry 5 "Something went wrong" claimedJob)
      retryResult `shouldBe` 1

      -- Job should not be immediately claimable (visibility timeout)
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- Verify the error message was actually persisted
      Just updated <- runM env (HL.getJobById @m @registry @payload (primaryKey claimedJob))
      lastError updated `shouldBe` Just "Something went wrong"
      attempts updated `shouldBe` 1 -- attempts unchanged by updateJobForRetry
  describe "Dead Letter Queue Operations" $ do
    it "moveToDLQ moves failed job to DLQ and removes from main queue" $ \env -> do
      let job = (defaultJob (mkMessage "Failed")) {groupKey = Just "dlq-move-test"}

      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      let claimedJob = head claimed

      -- Update the job in the DB to have an error message and make it immediately visible
      void $ runM env (HL.updateJobForRetry 0 "Job failed" claimedJob)

      -- Claim the job again to get the updated state (attempts=2, last_error is now set)
      updatedClaimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length updatedClaimed `shouldBe` 1
      let jobToDLQ = head updatedClaimed

      -- Move to DLQ with final error message
      rowsAffected <- runM env (HL.moveToDLQ "Final failure" jobToDLQ)
      rowsAffected `shouldBe` 1

      -- Job should not be in main queue
      allJobs <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length allJobs `shouldBe` 0

      -- Job should be in DLQ with the final error message
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1
      let dlqJobSnapshot = DLQ.jobSnapshot (head dlqJobs)
      payload dlqJobSnapshot `shouldBe` mkMessage "Failed"
      lastError dlqJobSnapshot `shouldBe` Just "Final failure"
      attempts dlqJobSnapshot `shouldBe` 2

    it "retryFromDLQ moves job back to main queue with attempts reset" $ \env -> do
      let job = (defaultJob (mkMessage "Retry")) {groupKey = Just "dlq-retry-test"}

      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      claimed `shouldNotBe` []
      let claimedJob = head claimed

      -- Move to DLQ
      void $ runM env (HL.moveToDLQ "Failed" claimedJob)

      -- Get DLQ job
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1

      -- Retry from DLQ
      Just retried <- runM env (HL.retryFromDLQ (DLQ.dlqPrimaryKey (head dlqJobs)))
      attempts retried `shouldBe` 0
      lastError retried `shouldBe` Nothing
      payload retried `shouldBe` mkMessage "Retry"

      -- Should be claimable from main queue
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1
      payload (head claimed2) `shouldBe` mkMessage "Retry"

      -- Should be removed from DLQ
      dlqJobs2 <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs2 `shouldBe` 0

    it "retryFromDLQ returns Nothing for non-existent DLQ job" $ \env -> do
      -- Fabricate a DLQ job with a bogus ID
      let job = (defaultJob (mkMessage "Phantom")) {groupKey = Just "dlq-phantom"}
      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.moveToDLQ "err" (head claimed))
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      -- Delete it first, then retry the stale reference
      _ <- runM env (HL.deleteDLQJob @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      result <- runM env (HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      result `shouldBe` Nothing

    it "deleteDLQJob returns 0 for non-existent DLQ job" $ \env -> do
      let job = (defaultJob (mkMessage "Ghost")) {groupKey = Just "dlq-ghost"}
      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.moveToDLQ "err" (head claimed))
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      _ <- runM env (HL.deleteDLQJob @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      -- Second delete should return 0
      n <- runM env (HL.deleteDLQJob @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      n `shouldBe` 0

    it "deleteDLQJob permanently removes job from DLQ" $ \env -> do
      let job = (defaultJob (mkMessage "Delete")) {groupKey = Just "dlq-delete-test"}

      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      claimed `shouldNotBe` []
      let claimedJob = head claimed

      -- Move to DLQ
      void $ runM env (HL.moveToDLQ "Delete me" claimedJob)

      -- Verify in DLQ
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1

      -- Delete from DLQ
      deleted <- runM env (HL.deleteDLQJob @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      deleted `shouldBe` 1

      -- Should be gone
      dlqJobs2 <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs2 `shouldBe` 0

    it "listDLQJobs supports pagination" $ \env -> do
      -- Insert and move 5 jobs to DLQ
      let jobs =
            [ (defaultJob (mkMessage ("Job" <> T.pack (show i))))
                { groupKey = Just ("dlq-pagination-test-" <> T.pack (show i))
                }
            | i <- [1 .. 5 :: Int]
            ]

      forM_ jobs $ \job -> do
        Just _inserted <- runM env (HL.insertJob job)
        claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
        claimed `shouldNotBe` []
        let claimedJob = head claimed
        void $ runM env (HL.moveToDLQ "Failed" claimedJob)

      -- List first 2
      dlqJobs1 <- runM env (HL.listDLQJobs 2 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs1 `shouldBe` 2

      -- List next 2
      dlqJobs2 <- runM env (HL.listDLQJobs 2 2) :: IO [DLQ.DLQJob payload]
      length dlqJobs2 `shouldBe` 2

      -- List last 1
      dlqJobs3 <- runM env (HL.listDLQJobs 2 4) :: IO [DLQ.DLQJob payload]
      length dlqJobs3 `shouldBe` 1

      -- All pages should contain distinct jobs
      let allDlqIds = map DLQ.dlqPrimaryKey (dlqJobs1 ++ dlqJobs2 ++ dlqJobs3)
      length allDlqIds `shouldBe` length (nub allDlqIds)

    it "moveToDLQ returns 0 when job already claimed by another worker" $ \env -> do
      let job = (defaultJob (mkMessage "Race")) {groupKey = Just "dlq-race-move-test"}

      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      claimed `shouldNotBe` []
      let claimedJob = head claimed

      -- Simulate another worker claiming by making job visible and claiming again
      void $ runM env (HL.setVisibilityTimeout 0 claimedJob)
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1

      -- Try to DLQ with old attempts value (race lost)
      rowsAffected <- runM env (HL.moveToDLQ "Failed" claimedJob)
      rowsAffected `shouldBe` 0

    it "updateJobForRetry returns 0 when job already claimed by another worker" $ \env -> do
      let job = (defaultJob (mkMessage "Race")) {groupKey = Just "dlq-race-retry-test"}

      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      claimed `shouldNotBe` []
      let claimedJob = head claimed

      -- Simulate another worker claiming by making job visible and claiming again
      void $ runM env (HL.setVisibilityTimeout 0 claimedJob)
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1

      -- Try to update for retry with old attempts value (race lost)
      rowsAffected <- runM env (HL.updateJobForRetry 5 "Failed" claimedJob)
      rowsAffected `shouldBe` 0

    it "ackJob returns 0 when job already claimed by another worker" $ \env -> do
      let job = (defaultJob (mkMessage "Race")) {groupKey = Just "dlq-race-ack-test"}

      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      claimed `shouldNotBe` []
      let claimedJob = head claimed

      -- Simulate another worker claiming by making job visible and claiming again
      void $ runM env (HL.setVisibilityTimeout 0 claimedJob)
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1

      -- Try to ack with old attempts value (race lost)
      rowsAffected <- runM env (HL.ackJob claimedJob)
      rowsAffected `shouldBe` 0

  describe "Batched Claims (claimNextVisibleJobsBatched)" $ do
    let claimBatched env batchSize limit = runM env (HL.claimNextVisibleJobsBatched batchSize limit 60)
        claimBatchedFlat env batchSize limit = concatMap NE.toList <$> claimBatched env batchSize limit

    it "claims multiple jobs from the same group up to batch size" $ \env -> do
      -- Insert 5 jobs in the same group
      forM_ [1 .. 5 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-size-test" (mkMessage (T.pack $ "Job" <> show i))))

      -- Claim with batch size 3, limit 10
      batches <- claimBatched env 3 10 :: IO [NonEmpty (JobRead payload)]

      -- Should get exactly 1 batch of 3 jobs from batch-size-test
      length batches `shouldBe` 1
      let batch = NE.toList (head batches)
      length batch `shouldBe` 3
      forM_ batch $ \j -> groupKey j `shouldBe` Just "batch-size-test"

    it "respects batch size limit per group" $ \env -> do
      -- Insert 10 jobs in group batch-limit-test-1 and 10 in group batch-limit-test-2
      forM_ [1 .. 10 :: Int] $ \i -> do
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-limit-test-1" (mkMessage (T.pack $ "G1-" <> show i))))
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-limit-test-2" (mkMessage (T.pack $ "G2-" <> show i))))

      -- Claim with batch size 3, limit 100
      batches <- claimBatched env 3 100 :: IO [NonEmpty (JobRead payload)]

      -- Should get 2 batches of 3 jobs each (one per group)
      length batches `shouldBe` 2
      forM_ batches $ \b -> NE.length b `shouldBe` 3
      let batchGroups = map (\b -> groupKey (NE.head b)) batches
      batchGroups `shouldMatchList` [Just "batch-limit-test-1", Just "batch-limit-test-2"]

    it "respects overall limit across groups" $ \env -> do
      -- Insert 10 jobs each in 5 different groups
      forM_ [1 .. 5 :: Int] $ \g ->
        forM_ [1 .. 10 :: Int] $ \i ->
          void $
            runM
              env
              (HL.insertJob (defaultGroupedJob (T.pack $ "batch-overall-test-" <> show g) (mkMessage (T.pack $ "Job" <> show i))))

      -- Claim with batch size 10, limit to 3 batches
      -- Group-aware: SQL selects 3 groups, up to 10 jobs each = exactly 3 batches
      batches <- claimBatched env 10 3 :: IO [NonEmpty (JobRead payload)]

      -- Should get exactly 3 batches (one per group, limited to 3)
      length batches `shouldBe` 3
      -- All batches should be from groups (no ungrouped in this test)
      forM_ batches $ \b -> groupKey (NE.head b) `shouldSatisfy` isJust

    it "ungrouped and grouped jobs compete fairly for batch slots" $ \env -> do
      -- Insert ungrouped first (low IDs), then grouped (high IDs)
      forM_ [1 .. 3 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "U" <> show i))))
      forM_ [1 .. 2 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultGroupedJob "fair-batch-a" (mkMessage (T.pack $ "Ga" <> show i))))
      forM_ [1 .. 2 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultGroupedJob "fair-batch-b" (mkMessage (T.pack $ "Gb" <> show i))))

      -- Claim with batchSize=2, limit=3 (3 batch slots)
      -- Slot allocation by FIFO (min_id):
      --   slot 1: ungrouped batch 1 (U1+U2, min_id=1)
      --   slot 2: ungrouped batch 2 (U3, min_id=3)
      --   slot 3: group "a" (Ga1+Ga2, min_id=4)
      -- Group "b" (min_id=6) gets no slot.
      batches <- claimBatched env 2 3 :: IO [NonEmpty (JobRead payload)]

      -- 3 batch slots: 2 ungrouped batches + 1 group batch
      length batches `shouldBe` 3
      let ungroupedBatches = filter (\b -> groupKey (NE.head b) == Nothing) batches
      let groupedBatches = filter (\b -> groupKey (NE.head b) /= Nothing) batches
      length ungroupedBatches `shouldBe` 2
      length groupedBatches `shouldBe` 1
      -- Ungrouped batches: sizes 2 and 1 (U1+U2, U3)
      sort (map NE.length ungroupedBatches) `shouldBe` [1, 2]
      -- Grouped batch: group "a" with 2 jobs
      NE.length (head groupedBatches) `shouldBe` 2
      groupKey (NE.head (head groupedBatches)) `shouldBe` Just "fair-batch-a"

    it "respects head-of-line blocking within groups" $ \env -> do
      -- Insert jobs in batch-hol-test
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-hol-test" (mkMessage "First")))
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-hol-test" (mkMessage "Second")))
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-hol-test" (mkMessage "Third")))

      -- Claim batch of 2
      claimed1 <- claimBatchedFlat env 2 10 :: IO [JobRead payload]
      length claimed1 `shouldBe` 2
      map payload claimed1 `shouldMatchList` [mkMessage "First", mkMessage "Second"]

      -- Third job should NOT be claimable (first two haven't been acked)
      claimed2 <- claimBatchedFlat env 2 10 :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- Ack the first two jobs
      forM_ claimed1 $ \job -> void $ runM env (HL.ackJob job)

      -- Now the third job should be claimable
      claimed3 <- claimBatchedFlat env 2 10 :: IO [JobRead payload]
      length claimed3 `shouldBe` 1
      payload (head claimed3) `shouldBe` mkMessage "Third"

    it "respects priority within batches" $ \env -> do
      -- Insert jobs with different priorities in same group
      void $ runM env (HL.insertJob ((defaultGroupedJob "batch-priority-test" (mkMessage "Low")) {priority = 10}))
      void $ runM env (HL.insertJob ((defaultGroupedJob "batch-priority-test" (mkMessage "High")) {priority = 0}))
      void $ runM env (HL.insertJob ((defaultGroupedJob "batch-priority-test" (mkMessage "Med")) {priority = 5}))

      -- Claim batch of 3
      batches <- claimBatched env 3 10 :: IO [NonEmpty (JobRead payload)]

      -- Single batch containing all 3 priority levels
      length batches `shouldBe` 1
      let batch = NE.toList (head batches)
      length batch `shouldBe` 3
      forM_ batch $ \j -> groupKey j `shouldBe` Just "batch-priority-test"
      map priority batch `shouldBe` [0, 5, 10]

    it "increments attempts for all jobs in batch" $ \env -> do
      -- Insert 3 jobs in same group
      forM_ [1 .. 3 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-attempts-test" (mkMessage (T.pack $ "Job" <> show i))))

      -- Claim batch
      claimed <- claimBatchedFlat env 3 10 :: IO [JobRead payload]
      length claimed `shouldBe` 3

      -- All should have attempts = 1
      forM_ claimed $ \j -> attempts j `shouldBe` 1

    it "blocks group while batch is in-flight" $ \env -> do
      -- Insert 10 jobs in same group
      forM_ [1 .. 10 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-block-test" (mkMessage (T.pack $ "Job" <> show i))))

      -- Claim first batch of 5
      firstBatch <- claimBatchedFlat env 5 10 :: IO [JobRead payload]
      length firstBatch `shouldBe` 5

      -- Try to claim more from same group while first batch is claimed (not yet acked)
      -- Should get 0 jobs because the group is blocked by head-of-line blocking
      secondClaim <- claimBatchedFlat env 5 10 :: IO [JobRead payload]
      length secondClaim `shouldBe` 0

      -- Ack the first batch
      forM_ firstBatch $ \job -> void $ runM env (HL.ackJob job)

      -- Now we should be able to claim the remaining 5 jobs
      thirdClaim <- claimBatchedFlat env 5 10 :: IO [JobRead payload]
      length thirdClaim `shouldBe` 5

    it "handles mixed grouped and ungrouped jobs correctly" $ \env -> do
      -- Create a more complex scenario
      void $ runM env (HL.insertJob (defaultJob (mkMessage "U1")))
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-mixed-test-1" (mkMessage "G1-1")))
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-mixed-test-1" (mkMessage "G1-2")))
      void $ runM env (HL.insertJob (defaultJob (mkMessage "U2")))
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-mixed-test-2" (mkMessage "G2-1")))
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-mixed-test-2" (mkMessage "G2-2")))

      -- Claim with batch size 2, limit 10
      batches <- claimBatched env 2 10 :: IO [NonEmpty (JobRead payload)]

      -- 1 ungrouped batch (U1+U2) + 2 group batches (G1, G2) = 3 batches
      length batches `shouldBe` 3
      let ungroupedBatches = filter (\b -> groupKey (NE.head b) == Nothing) batches
      let groupedBatches = filter (\b -> groupKey (NE.head b) /= Nothing) batches
      length ungroupedBatches `shouldBe` 1
      NE.length (head ungroupedBatches) `shouldBe` 2
      length groupedBatches `shouldBe` 2
      forM_ groupedBatches $ \b -> NE.length b `shouldBe` 2
      let batchGroups = sort $ map (\b -> groupKey (NE.head b)) groupedBatches
      batchGroups `shouldBe` [Just "batch-mixed-test-1", Just "batch-mixed-test-2"]

    it "claims full group batch even when members are separated by ungrouped jobs" $ \env -> do
      -- Insert grouped and ungrouped interleaved so group members have
      -- non-consecutive IDs (simulates a real workload where group jobs
      -- are inserted over time with ungrouped jobs in between)
      void $ runM env (HL.insertJob (defaultGroupedJob "spaced-group" (mkMessage "G1")))
      forM_ [1 .. 3 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "U" <> show i))))
      void $ runM env (HL.insertJob (defaultGroupedJob "spaced-group" (mkMessage "G2")))
      forM_ [4 .. 6 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "U" <> show i))))
      void $ runM env (HL.insertJob (defaultGroupedJob "spaced-group" (mkMessage "G3")))

      -- Claim with batchSize=3 — should get all 3 group members despite
      -- them being spread across IDs 1, 5, 10 (with ungrouped in between)
      batches <- claimBatched env 3 10 :: IO [NonEmpty (JobRead payload)]

      -- 1 group batch (G1+G2+G3) + 2 ungrouped batches (U1-U3, U4-U6) = 3 batches
      length batches `shouldBe` 3
      -- Verify grouped jobs form a single batch of 3
      let groupBatches = filter (\b -> groupKey (NE.head b) == Just "spaced-group") batches
      length groupBatches `shouldBe` 1
      NE.length (head groupBatches) `shouldBe` 3
      map payload (NE.toList (head groupBatches)) `shouldMatchList` [mkMessage "G1", mkMessage "G2", mkMessage "G3"]
      -- Ungrouped jobs form 2 batches of 3
      let ungroupedBatches = filter (\b -> groupKey (NE.head b) == Nothing) batches
      length ungroupedBatches `shouldBe` 2
      forM_ ungroupedBatches $ \b -> NE.length b `shouldBe` 3

    it "batched mode excludes children and finalizers" $ \env -> do
      -- Insert a rollup tree: finalizer + 2 children
      Right (_parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "BatchExclParent"))
              ( JT.leaf (defaultJob (mkMessage "BatchExclChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "BatchExclChild2"))]
              )

      -- Insert a regular (non-tree) job
      void $ runM env (HL.insertJob (defaultJob (mkMessage "BatchExclRegular")))

      -- Batched claim should only get the regular job
      batches <- claimBatchedFlat env 10 10 :: IO [JobRead payload]
      let claimedPayloads = map payload batches
      claimedPayloads `shouldBe` [mkMessage "BatchExclRegular"]

  describe "Batch Admin Operations" $ do
    it "cancelJobsBatch deletes multiple jobs" $ \env -> do
      -- Insert 5 jobs
      insertedJobs <- forM [1 .. 5 :: Int] $ \i -> do
        Just job <- runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "Cancel" <> show i))))
        pure job

      -- Cancel 3 of them
      let idsToCancel = map primaryKey (take 3 insertedJobs)
      deleted <- runM env (HL.cancelJobsBatch @m @registry @payload idsToCancel)
      deleted `shouldBe` 3

      -- Verify only 2 remain
      remaining <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length remaining `shouldBe` 2

    it "cancelJobsBatch returns 0 for empty list" $ \env -> do
      deleted <- runM env (HL.cancelJobsBatch @m @registry @payload [])
      deleted `shouldBe` 0

    it "cancelJobsBatch handles non-existent IDs gracefully" $ \env -> do
      -- Insert 2 jobs
      Just job1 <- runM env (HL.insertJob (defaultJob (mkMessage "Keep1")))
      Just job2 <- runM env (HL.insertJob (defaultJob (mkMessage "Keep2")))

      -- Try to cancel with mix of valid and invalid IDs
      let idsToCancel = [primaryKey job1, 999999, primaryKey job2, 888888]
      deleted <- runM env (HL.cancelJobsBatch @m @registry @payload idsToCancel)
      deleted `shouldBe` 2

    it "moveToDLQBatch moves multiple jobs with individual error messages" $ \env -> do
      -- Insert and claim 3 jobs
      claimedJobs <- forM [1 .. 3 :: Int] $ \i -> do
        Just _ <-
          runM env (HL.insertJob (defaultGroupedJob ("dlq-batch-" <> T.pack (show i)) (mkMessage (T.pack $ "DLQ" <> show i))))
        jobs <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
        pure (head jobs)

      -- Move all to DLQ with different error messages
      let jobsWithErrors = zip claimedJobs ["Error 1", "Error 2", "Error 3"]
      moved <- runM env (HL.moveToDLQBatch jobsWithErrors)
      moved `shouldBe` 3

      -- Verify all are in DLQ with correct error messages
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 3
      let errors = map (lastError . DLQ.jobSnapshot) dlqJobs
      sort errors `shouldBe` [Just "Error 1", Just "Error 2", Just "Error 3"]

    it "moveToDLQBatch returns 0 for empty list" $ \env -> do
      moved <- runM env (HL.moveToDLQBatch @m @registry @payload [])
      moved `shouldBe` 0

    it "moveToDLQBatch skips jobs with stale attempts (optimistic locking)" $ \env -> do
      -- Insert and claim 2 jobs
      Just _ <- runM env (HL.insertJob (defaultGroupedJob "dlq-batch-stale-1" (mkMessage "Stale1")))
      Just _ <- runM env (HL.insertJob (defaultGroupedJob "dlq-batch-stale-2" (mkMessage "Stale2")))
      claimed <- runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2
      [job1, job2] <- pure claimed

      -- Simulate job1 being reclaimed by another worker
      void $ runM env (HL.setVisibilityTimeout 0 job1)
      _ <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      -- Try to move both to DLQ - job1 should fail (stale attempts), job2 should succeed
      let jobsWithErrors = [(job1, "Error 1"), (job2, "Error 2")]
      moved <- runM env (HL.moveToDLQBatch jobsWithErrors)
      moved `shouldBe` 1

      -- Verify only job2 is in DLQ
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1
      lastError (DLQ.jobSnapshot (head dlqJobs)) `shouldBe` Just "Error 2"

    it "deleteDLQJobsBatch deletes multiple DLQ jobs" $ \env -> do
      -- Insert, claim, and move 5 jobs to DLQ
      forM_ [1 .. 5 :: Int] $ \i -> do
        Just _ <-
          runM
            env
            (HL.insertJob (defaultGroupedJob ("dlq-delete-batch-" <> T.pack (show i)) (mkMessage (T.pack $ "Del" <> show i))))
        jobs <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
        void $ runM env (HL.moveToDLQ "Failed" (head jobs))

      -- Get DLQ job IDs
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 5

      -- Delete 3 of them
      let idsToDelete = map DLQ.dlqPrimaryKey (take 3 dlqJobs)
      deleted <- runM env (HL.deleteDLQJobsBatch @m @registry @payload idsToDelete)
      deleted `shouldBe` 3

      -- Verify only 2 remain
      remaining <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length remaining `shouldBe` 2

    it "deleteDLQJobsBatch returns 0 for empty list" $ \env -> do
      deleted <- runM env (HL.deleteDLQJobsBatch @m @registry @payload [])
      deleted `shouldBe` 0

  describe "Admin Operations" $ do
    it "listJobs returns jobs with pagination" $ \env -> do
      -- Insert 5 jobs
      forM_ [1 .. 5 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "List" <> show i))))

      -- List first 2
      jobs1 <- runM env (HL.listJobs @m @registry @payload 2 0)
      length jobs1 `shouldBe` 2

      -- List next 2
      jobs2 <- runM env (HL.listJobs @m @registry @payload 2 2)
      length jobs2 `shouldBe` 2

      -- List last 1
      jobs3 <- runM env (HL.listJobs @m @registry @payload 2 4)
      length jobs3 `shouldBe` 1

      -- All should be distinct
      let allIds = map primaryKey (jobs1 ++ jobs2 ++ jobs3)
      length allIds `shouldBe` length (nub allIds)

    it "getJobById returns the job when it exists" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "FindMe")))

      found <- runM env (HL.getJobById @m @registry @payload (primaryKey inserted))
      found `shouldBe` Just inserted

    it "getJobById returns Nothing when job doesn't exist" $ \env -> do
      found <- runM env (HL.getJobById @m @registry @payload 999999)
      found `shouldBe` Nothing

    it "getJobsByGroup returns jobs filtered by group key" $ \env -> do
      -- Insert jobs in different groups
      forM_ [1 .. 3 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultGroupedJob "group-filter-a" (mkMessage (T.pack $ "A" <> show i))))
      forM_ [1 .. 2 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultGroupedJob "group-filter-b" (mkMessage (T.pack $ "B" <> show i))))

      -- Get only group A
      groupAJobs <- runM env (HL.getJobsByGroup @m @registry @payload "group-filter-a" 10 0)
      length groupAJobs `shouldBe` 3
      forM_ groupAJobs $ \j -> groupKey j `shouldBe` Just "group-filter-a"

    it "getInFlightJobs returns only claimed jobs" $ \env -> do
      -- Insert 3 jobs
      forM_ [1 .. 3 :: Int] $ \i ->
        void $
          runM env (HL.insertJob (defaultGroupedJob ("inflight-test-" <> T.pack (show i)) (mkMessage (T.pack $ "IF" <> show i))))

      -- Initially no in-flight jobs
      inFlight0 <- runM env (HL.getInFlightJobs @m @registry @payload 10 0)
      length inFlight0 `shouldBe` 0

      -- Claim 2 jobs
      _ <- runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]

      -- Now 2 in-flight
      inFlight2 <- runM env (HL.getInFlightJobs @m @registry @payload 10 0)
      length inFlight2 `shouldBe` 2
      forM_ inFlight2 $ \j -> attempts j `shouldSatisfy` (> 0)

    it "promoteJob makes delayed job immediately visible" $ \env -> do
      -- Insert a job, claim it, and put it in retry backoff
      let delayedJob = defaultJob (mkMessage "Delayed")
      Just inserted <- runM env (HL.insertJob delayedJob)

      -- Claim and update for retry (makes it invisible for 60 seconds)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.updateJobForRetry 60 "Retry later" (head claimed))

      -- Job should not be claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- Promote the job
      promoted <- runM env (HL.promoteJob @m @registry @payload (primaryKey inserted))
      promoted `shouldBe` 1

      -- Now it should be claimable
      claimed3 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed3 `shouldBe` 1

    it "getQueueStats returns correct statistics" $ \env -> do
      -- Insert 5 jobs
      forM_ [1 .. 5 :: Int] $ \i ->
        void $
          runM env (HL.insertJob (defaultGroupedJob ("stats-test-" <> T.pack (show i)) (mkMessage (T.pack $ "S" <> show i))))

      -- Check stats before claiming
      stats1 <- runM env (HL.getQueueStats @m @registry @payload)
      HL.totalJobs stats1 `shouldBe` 5
      HL.visibleJobs stats1 `shouldBe` 5
      HL.invisibleJobs stats1 `shouldBe` 0

      -- Claim 2 jobs
      _ <- runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]

      -- Check stats after claiming
      stats2 <- runM env (HL.getQueueStats @m @registry @payload)
      HL.totalJobs stats2 `shouldBe` 5
      HL.visibleJobs stats2 `shouldBe` 3
      HL.invisibleJobs stats2 `shouldBe` 2

  describe "Count Operations" $ do
    it "countJobs returns total job count" $ \env -> do
      -- Insert 4 jobs
      forM_ [1 .. 4 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "Count" <> show i))))

      count <- runM env (HL.countJobs @m @registry @payload)
      count `shouldBe` 4

    it "countJobsByGroup returns count for specific group" $ \env -> do
      -- Insert jobs in different groups
      forM_ [1 .. 3 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultGroupedJob "count-group-x" (mkMessage (T.pack $ "X" <> show i))))
      forM_ [1 .. 2 :: Int] $ \i ->
        void $ runM env (HL.insertJob (defaultGroupedJob "count-group-y" (mkMessage (T.pack $ "Y" <> show i))))

      countX <- runM env (HL.countJobsByGroup @m @registry @payload "count-group-x")
      countX `shouldBe` 3

      countY <- runM env (HL.countJobsByGroup @m @registry @payload "count-group-y")
      countY `shouldBe` 2

    it "countInFlightJobs returns count of claimed jobs" $ \env -> do
      -- Insert 5 jobs
      forM_ [1 .. 5 :: Int] $ \i ->
        void $
          runM env (HL.insertJob (defaultGroupedJob ("count-inflight-" <> T.pack (show i)) (mkMessage (T.pack $ "IF" <> show i))))

      -- Initially 0 in-flight
      count0 <- runM env (HL.countInFlightJobs @m @registry @payload)
      count0 `shouldBe` 0

      -- Claim 3
      _ <- runM env (HL.claimNextVisibleJobs 3 60) :: IO [JobRead payload]

      -- Now 3 in-flight
      count3 <- runM env (HL.countInFlightJobs @m @registry @payload)
      count3 `shouldBe` 3

    it "countDLQJobs returns count of DLQ jobs" $ \env -> do
      -- Initially 0 in DLQ
      count0 <- runM env (HL.countDLQJobs @m @registry @payload)
      count0 `shouldBe` 0

      -- Move 2 jobs to DLQ
      forM_ [1 .. 2 :: Int] $ \i -> do
        Just _ <-
          runM env (HL.insertJob (defaultGroupedJob ("count-dlq-" <> T.pack (show i)) (mkMessage (T.pack $ "DLQ" <> show i))))
        jobs <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
        void $ runM env (HL.moveToDLQ "Failed" (head jobs))

      -- Now 2 in DLQ
      count2 <- runM env (HL.countDLQJobs @m @registry @payload)
      count2 `shouldBe` 2

  describe "Job Dependencies" $ do
    it "no children: normal ack deletes job" $ \env -> do
      -- Insert job with no children
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "NoChildren")))

      -- Claim and ack
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      rowsAffected <- runM env (HL.ackJob (head claimed))
      rowsAffected `shouldBe` 1

      -- Job should be deleted
      found <- runM env (HL.getJobById @m @registry @payload (primaryKey inserted))
      found `shouldBe` Nothing

    it "pause/resume children" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "PauseParent"))
              (JT.leaf (defaultJob (mkMessage "PauseChild1")) :| [JT.leaf (defaultJob (mkMessage "PauseChild2"))])

      -- Children start unsuspended — pause them
      paused <- runM env (HL.pauseChildren @m @registry @payload (primaryKey parent))
      paused `shouldBe` 2

      -- Children should not be claimable
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 0

      -- Resume children
      resumed <- runM env (HL.resumeChildren @m @registry @payload (primaryKey parent))
      resumed `shouldBe` 2

      -- Children should now be claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      let claimedPayloads = map payload claimed2
      claimedPayloads `shouldContain` [mkMessage "PauseChild1"]
      claimedPayloads `shouldContain` [mkMessage "PauseChild2"]

    it "moveToDLQ on only child wakes parent" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQParent"))
              (JT.leaf (defaultJob (mkMessage "DLQChild")) :| [])

      -- Claim the child
      claimedChild <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedChild `shouldBe` 1

      -- Move child to DLQ — parent wakes (no more children in main queue)
      void $ runM env (HL.moveToDLQ "Child failed" (head claimedChild))

      -- Parent should be resumed (not suspended)
      Just parentResumed <- runM env (HL.getJobById @m @registry @payload (primaryKey parent))
      suspended parentResumed `shouldBe` False

    it "multi-level: grandparent wakes when all descendants complete" $ \env -> do
      -- Build: Grandparent → Parent → [Child1, Child2] using nested finalizers
      Right (grandparent :| rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "Grandparent"))
              ( JT.rollup
                  (defaultJob (mkMessage "Parent"))
                  (JT.leaf (defaultJob (mkMessage "MLChild1")) :| [JT.leaf (defaultJob (mkMessage "MLChild2"))])
                  :| []
              )

      let parent = head rest

      -- Only children should be claimable (GP and Parent are suspended finalizers)
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2

      -- Ack child1
      void $ runM env (HL.ackJob (head claimed))
      pStillExists <- runM env (HL.getJobById @m @registry @payload (primaryKey parent))
      pStillExists `shouldNotBe` Nothing

      -- Ack child2 (last child) → resumes parent for completion round
      void $ runM env (HL.ackJob (claimed !! 1))

      -- Parent should be resumed
      Just parentResumed <- runM env (HL.getJobById @m @registry @payload (primaryKey parent))
      suspended parentResumed `shouldBe` False

      -- Claim and ack parent (completion round) → resumes grandparent
      claimedP <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedP `shouldBe` 1
      void $ runM env (HL.ackJob (head claimedP))

      pGone <- runM env (HL.getJobById @m @registry @payload (primaryKey parent))
      pGone `shouldBe` Nothing

      -- Grandparent should be resumed
      Just gpResumed <- runM env (HL.getJobById @m @registry @payload (primaryKey grandparent))
      suspended gpResumed `shouldBe` False

      -- Claim and ack grandparent (completion round)
      claimedGP <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedGP `shouldBe` 1
      void $ runM env (HL.ackJob (head claimedGP))

      gpGone <- runM env (HL.getJobById @m @registry @payload (primaryKey grandparent))
      gpGone `shouldBe` Nothing

    it "multi-level: partial completion doesn't wake ancestors" $ \env -> do
      -- Build: Grandparent finalizer → [Parent1 finalizer → [C1a], Parent2 finalizer → [C2a]]
      Right (grandparent :| rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "GPPartial"))
              ( JT.rollup
                  (defaultJob (mkMessage "P1Partial"))
                  (JT.leaf (defaultJob (mkMessage "C1aPartial")) :| [])
                  :| [ JT.rollup
                         (defaultJob (mkMessage "P2Partial"))
                         (JT.leaf (defaultJob (mkMessage "C2aPartial")) :| [])
                     ]
              )

      let parent1 = head rest
          parent2 = rest !! 2 -- parent2 is after parent1 and its child

      -- Only C1a and C2a are claimable
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2

      -- Ack child1a → resumes parent1
      void $ runM env (HL.ackJob (head claimed))

      -- Parent1 should be resumed for completion round, but grandparent still has parent2
      claimedP1 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedP1 `shouldBe` 1
      payload (head claimedP1) `shouldBe` mkMessage "P1Partial"
      void $ runM env (HL.ackJob (head claimedP1))

      -- Parent1 gone, but grandparent still waiting
      p1Gone <- runM env (HL.getJobById @m @registry @payload (primaryKey parent1))
      p1Gone `shouldBe` Nothing
      gpStill <- runM env (HL.getJobById @m @registry @payload (primaryKey grandparent))
      gpStill `shouldNotBe` Nothing

      -- Ack child2a → resumes parent2
      void $ runM env (HL.ackJob (claimed !! 1))

      -- Claim and ack parent2 completion round
      claimedP2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedP2 `shouldBe` 1
      void $ runM env (HL.ackJob (head claimedP2))

      p2Gone <- runM env (HL.getJobById @m @registry @payload (primaryKey parent2))
      p2Gone `shouldBe` Nothing

      -- Grandparent should now be resumed
      claimedGP <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedGP `shouldBe` 1
      void $ runM env (HL.ackJob (head claimedGP))

      gpGone <- runM env (HL.getJobById @m @registry @payload (primaryKey grandparent))
      gpGone `shouldBe` Nothing

    it "multi-level: cancel cascade deletes all descendants" $ \env -> do
      Right (grandparent :| _rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CascadeGP"))
              ( JT.rollup
                  (defaultJob (mkMessage "CascadeMLParent"))
                  (JT.leaf (defaultJob (mkMessage "CascadeMLChild1")) :| [JT.leaf (defaultJob (mkMessage "CascadeMLChild2"))])
                  :| []
              )

      -- Cancel cascade from grandparent
      deleted <- runM env (HL.cancelJobCascade @m @registry @payload (primaryKey grandparent))
      deleted `shouldBe` 4

      -- Nothing remains
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 0

    it "multi-level: DLQ at leaf wakes parent but not grandparent" $ \env -> do
      Right (grandparent :| rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQGrandparent"))
              ( JT.rollup
                  (defaultJob (mkMessage "DLQMLParent"))
                  (JT.leaf (defaultJob (mkMessage "DLQMLChild")) :| [])
                  :| []
              )

      let parent = head rest

      -- Claim child, then move to DLQ
      claimedC <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedC `shouldBe` 1
      void $ runM env (HL.moveToDLQ "Child failed" (head claimedC))

      -- Parent should be resumed (no children in main queue)
      Just pResumed <- runM env (HL.getJobById @m @registry @payload (primaryKey parent))
      suspended pResumed `shouldBe` False

      -- Grandparent should still be suspended (parent still exists in main queue)
      Just gpStill <- runM env (HL.getJobById @m @registry @payload (primaryKey grandparent))
      suspended gpStill `shouldBe` True

    it "suspendJob/resumeJob on a standalone job" $ \env -> do
      -- Insert an ungrouped job
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "SuspendMe")))
      suspended inserted `shouldBe` False

      -- Suspend it
      n <- runM env (HL.suspendJob @m @registry @payload (primaryKey inserted))
      n `shouldBe` 1

      -- Not claimable
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 0

      -- Verify suspended flag
      Just found <- runM env (HL.getJobById @m @registry @payload (primaryKey inserted))
      suspended found `shouldBe` True

      -- Resume it
      n2 <- runM env (HL.resumeJob @m @registry @payload (primaryKey inserted))
      n2 `shouldBe` 1

      -- Now claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1
      payload (head claimed2) `shouldBe` mkMessage "SuspendMe"

    it "suspendJob on in-flight job is rejected" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "InFlightSuspend")))

      -- Claim the job (makes it in-flight)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1

      -- Try to suspend — should fail (0 rows)
      n <- runM env (HL.suspendJob @m @registry @payload (primaryKey inserted))
      n `shouldBe` 0

    it "suspendJob on already-suspended job returns 0" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "DoubleSuspend")))
      n1 <- runM env (HL.suspendJob @m @registry @payload (primaryKey inserted))
      n1 `shouldBe` 1
      n2 <- runM env (HL.suspendJob @m @registry @payload (primaryKey inserted))
      n2 `shouldBe` 0

    it "resumeJob on non-suspended job returns 0" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "NotSuspended")))
      n <- runM env (HL.resumeJob @m @registry @payload (primaryKey inserted))
      n `shouldBe` 0

    it "phantom parent: insert child with non-existent parentId returns Nothing" $ \env -> do
      let child = (defaultJob (mkMessage "Orphan")) {parentId = Just 999999, suspended = True}
      result <- runM env (HL.insertJob child)
      result `shouldBe` Nothing

    it "insertJob respects explicit suspended = True" $ \env -> do
      -- User explicitly creates a pre-suspended job
      let job = (defaultJob (mkMessage "PreSuspended")) {suspended = True}
      Just inserted <- runM env (HL.insertJob job)
      suspended inserted `shouldBe` True

      -- Job should not be claimable
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 0

      -- Resume it
      n <- runM env (HL.resumeJob @m @registry @payload (primaryKey inserted))
      n `shouldBe` 1

      -- Now it should be claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1
      payload (head claimed2) `shouldBe` mkMessage "PreSuspended"

    it "insertJob respects notVisibleUntil (delayed job)" $ \env -> do
      now <- getCurrentTime
      let futureTime = truncateToMicros (addUTCTime 3600 now)
          job = (defaultJob (mkMessage "Delayed")) {notVisibleUntil = Just futureTime}
      Just inserted <- runM env (HL.insertJob job)
      notVisibleUntil inserted `shouldBe` Just futureTime

      -- Job should not be claimable
      claimed <- claimJobs env 10
      length claimed `shouldBe` 0

    it "insertJobsBatch respects notVisibleUntil" $ \env -> do
      now <- getCurrentTime
      let futureTime = addUTCTime 3600 now
          jobs =
            [ (defaultJob (mkMessage "BatchDelayed1")) {notVisibleUntil = Just futureTime}
            , defaultJob (mkMessage "BatchImmediate")
            ]
      inserted <- runM env (HL.insertJobsBatch jobs)
      length inserted `shouldBe` 2

      -- Only the immediate job should be claimable
      claimed <- claimJobs env 10
      length claimed `shouldBe` 1
      payload (head claimed) `shouldBe` mkMessage "BatchImmediate"

    it "retryFromDLQ preserves parent_id and clears DLQ" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQRetryParent"))
              (JT.leaf (defaultJob (mkMessage "DLQRetryChild")) :| [])

      -- Claim and move child to DLQ
      claimedChild <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedChild `shouldBe` 1
      void $ runM env (HL.moveToDLQ "Child failed" (head claimedChild))

      -- Retry from DLQ
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1
      Just retried <- runM env (HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))

      -- parent_id should be preserved
      parentId retried `shouldBe` Just (primaryKey parent)

      -- DLQ should be empty
      dlqAfter <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      length dlqAfter `shouldBe` 0

    it "cancelJobCascade on suspended parent with paused children" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CascadeSuspParent"))
              (JT.leaf (defaultJob (mkMessage "CascadeSuspChild1")) :| [JT.leaf (defaultJob (mkMessage "CascadeSuspChild2"))])

      -- Pause children (makes them suspended)
      _ <- runM env (HL.pauseChildren @m @registry @payload (primaryKey parent))

      -- Cancel cascade — should delete parent + all suspended children
      deleted <- runM env (HL.cancelJobCascade @m @registry @payload (primaryKey parent))
      deleted `shouldBe` 3

      -- Nothing remains
      remaining <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length remaining `shouldBe` 0

    it "cancelJob on last child wakes suspended parent" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CancelWakeParent"))
              (JT.leaf (defaultJob (mkMessage "CancelWakeChild")) :| [])

      let child = head children

      assertSuspended env (primaryKey parent)

      -- Cancel the child (non-cascade) — should resume the parent
      deleted <- runM env (HL.cancelJob @m @registry @payload (primaryKey child))
      deleted `shouldBe` 1

      assertNotSuspended env (primaryKey parent)

    it "cancelJob on parent with children returns 0 (guard)" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CancelGuardParent"))
              (JT.leaf (defaultJob (mkMessage "CancelGuardChild")) :| [])

      -- cancelJob (non-cascade) should refuse to delete a parent with children
      deleted <- runM env (HL.cancelJob @m @registry @payload (primaryKey parent))
      deleted `shouldBe` 0

      -- Parent and child should still exist
      assertSuspended env (primaryKey parent)
      Just childJob <- getJob env (primaryKey (head children))
      payload childJob `shouldBe` mkMessage "CancelGuardChild"

  describe "insertJobTree" $ do
    it "rollup: parent suspended, children not suspended, children claimable" $ \env -> do
      let parentJob = defaultJob (mkMessage "FanOutParent")
          childJobs =
            defaultJob (mkMessage "FanOutChild1")
              NE.:| [defaultJob (mkMessage "FanOutChild2")]

      Right (parent :| children) <- runM env (HL.insertJobTree (JT.rollup parentJob (JT.leaf <$> childJobs)))

      -- Parent should be suspended (finalizer)
      suspended parent `shouldBe` True
      parentId parent `shouldBe` Nothing

      -- Children should not be suspended (immediately claimable)
      length children `shouldBe` 2
      forM_ children $ \c -> suspended c `shouldBe` False
      forM_ children $ \c -> parentId c `shouldBe` Just (primaryKey parent)

      -- Only children should be claimable (parent is suspended)
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      let claimedPayloads = map payload claimed
      claimedPayloads `shouldNotContain` [mkMessage "FanOutParent"]
      claimedPayloads `shouldContain` [mkMessage "FanOutChild1"]
      claimedPayloads `shouldContain` [mkMessage "FanOutChild2"]

    it "rollup: acking all children resumes parent for completion round" $ \env -> do
      let parentJob = defaultJob (mkMessage "FanOutAckParent")
          childJobs =
            defaultJob (mkMessage "FanOutAckChild1")
              NE.:| [defaultJob (mkMessage "FanOutAckChild2")]

      Right (parent :| _children) <- runM env (HL.insertJobTree (JT.rollup parentJob (JT.leaf <$> childJobs)))

      -- Claim and ack child 1
      [c1] <- claimJobs env 1
      void $ runM env (HL.ackJob c1)

      -- Claim and ack child 2 (last child → parent should be resumed)
      [c2] <- claimJobs env 1
      void $ runM env (HL.ackJob c2)

      assertNotSuspended env (primaryKey parent)

      -- Completion round: claim and ack parent
      [parentJob'] <- claimJobs env 1
      payload parentJob' `shouldBe` mkMessage "FanOutAckParent"
      void $ runM env (HL.ackJob parentJob')

      assertGone env (primaryKey parent)

    it "rollup: parent and children can share group key" $ \env -> do
      -- Children sharing the parent's group key should work because
      -- suspended jobs are excluded from claim queries (AND NOT suspended).
      let parentJob = defaultGroupedJob "shared-group" (mkMessage "SharedGKParent")
          childJobs =
            defaultGroupedJob "shared-group" (mkMessage "SharedGKChild1")
              NE.:| [defaultGroupedJob "shared-group" (mkMessage "SharedGKChild2")]

      Right (parent :| children) <- runM env (HL.insertJobTree (JT.rollup parentJob (JT.leaf <$> childJobs)))

      -- Parent should be suspended (finalizer)
      suspended parent `shouldBe` True
      groupKey parent `shouldBe` Just "shared-group"

      -- Children should not be suspended and share the group key
      forM_ children $ \c -> suspended c `shouldBe` False
      forM_ children $ \c -> groupKey c `shouldBe` Just "shared-group"

      -- Children should be claimable despite sharing group key with suspended parent
      -- Head-of-line blocking applies only to non-suspended jobs
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      let claimedPayload = payload (head claimed)
      -- One of the children should be claimed (not the parent)
      claimedPayload `shouldSatisfy` (`elem` [mkMessage "SharedGKChild1", mkMessage "SharedGKChild2"])

  describe "DLQ Child Counts" $ do
    it "countDLQChildrenBatch returns counts for DLQ'd children" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQCountParent"))
              (JT.leaf (defaultJob (mkMessage "DLQCountChild1")) :| [JT.leaf (defaultJob (mkMessage "DLQCountChild2"))])

      -- Claim and DLQ both children
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2
      void $ runM env (HL.moveToDLQ "fail1" (head claimed))
      void $ runM env (HL.moveToDLQ "fail2" (claimed !! 1))

      -- countDLQChildrenBatch should show 2 DLQ'd children for parent
      dlqCounts <- runM env (HL.countDLQChildrenBatch @m @registry @payload [primaryKey parent])
      Map.lookup (primaryKey parent) dlqCounts `shouldBe` Just 2

    it "countDLQChildrenBatch returns empty for non-parents" $ \env -> do
      Just standalone <- runM env (HL.insertJob (defaultJob (mkMessage "DLQCountStandalone")))
      dlqCounts <- runM env (HL.countDLQChildrenBatch @m @registry @payload [primaryKey standalone])
      Map.lookup (primaryKey standalone) dlqCounts `shouldBe` Nothing

    it "countDLQChildrenBatch returns empty list for empty input" $ \env -> do
      dlqCounts <- runM env (HL.countDLQChildrenBatch @m @registry @payload [])
      dlqCounts `shouldBe` Map.empty

    it "dlqJobExists returns True for existing DLQ job" $ \env -> do
      Just _job <- runM env (HL.insertJob (defaultJob (mkMessage "DLQExistsJob")))
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.moveToDLQ "test error" (head claimed))
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      exists <- runM env (HL.dlqJobExists @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      exists `shouldBe` True

    it "dlqJobExists returns False for non-existent DLQ job" $ \env -> do
      exists <- runM env (HL.dlqJobExists @m @registry @payload 99999)
      exists `shouldBe` False

    it "retryFromDLQ refuses when parent no longer exists" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "OrphanRetryParent"))
              (JT.leaf (defaultJob (mkMessage "OrphanRetryChild")) :| [])

      -- Claim and DLQ the child
      claimedC <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.moveToDLQ "child failed" (head claimedC))

      -- Cancel (delete) the parent
      void $ runM env (HL.cancelJobCascade @m @registry @payload (primaryKey parent))

      -- Retry should return Nothing (parent is gone)
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1
      result <- runM env (HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      result `shouldBe` Nothing

      -- DLQ job should still be there (not deleted)
      exists <- runM env (HL.dlqJobExists @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      exists `shouldBe` True

  describe "Dependency Bug Fixes" $ do
    it "cancelJobsBatch wakes parent when last child is batch-cancelled" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "TreeCancelParent"))
              (JT.leaf (defaultJob (mkMessage "TreeCancelChild1")) :| [JT.leaf (defaultJob (mkMessage "TreeCancelChild2"))])

      -- Batch-cancel both children
      let childIds = map primaryKey children
      deleted <- runM env (HL.cancelJobsBatch @m @registry @payload childIds)
      deleted `shouldBe` 2

      -- Parent should be resumed (completion round, not deleted)
      Just parentResumed <- runM env (HL.getJobById @m @registry @payload (primaryKey parent))
      suspended parentResumed `shouldBe` False

    it "cancelJobCascade on mid-level node wakes grandparent" $ \env -> do
      Right (grandparent :| rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CascadeWakeGP"))
              ( JT.rollup
                  (defaultJob (mkMessage "CascadeWakeMid"))
                  (JT.leaf (defaultJob (mkMessage "CascadeWakeC1")) :| [JT.leaf (defaultJob (mkMessage "CascadeWakeC2"))])
                  :| []
              )

      let parent = head rest

      -- Cancel cascade from the mid-level parent
      deleted <- runM env (HL.cancelJobCascade @m @registry @payload (primaryKey parent))
      deleted `shouldBe` 3 -- parent + 2 children
      assertNotSuspended env (primaryKey grandparent)

    it "insertJobsBatch skips jobs with invalid parentId" $ \env -> do
      -- Insert one job with a valid parentId and one with a phantom parentId
      Just parent <- runM env (HL.insertJob (defaultJob (mkMessage "BatchValidParent")))
      let validChild = (defaultJob (mkMessage "BatchValidChild")) {parentId = Just (primaryKey parent), suspended = True}
          phantomChild = (defaultJob (mkMessage "BatchPhantomChild")) {parentId = Just 999999, suspended = True}
          standaloneJob = defaultJob (mkMessage "BatchStandalone")

      inserted <- runM env (HL.insertJobsBatch [validChild, phantomChild, standaloneJob])

      -- Only the valid child and standalone should be inserted (phantom skipped)
      length inserted `shouldBe` 2
      let payloads = map payload inserted
      payloads `shouldContain` [mkMessage "BatchValidChild"]
      payloads `shouldContain` [mkMessage "BatchStandalone"]
      payloads `shouldNotContain` [mkMessage "BatchPhantomChild"]

    it "retryFromDLQ then ack wakes parent (end-to-end DLQ recovery)" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQRecoveryParent"))
              (JT.leaf (defaultJob (mkMessage "DLQRecoveryChild")) :| [])

      -- Claim and DLQ the child — parent wakes (no children in main queue)
      [c] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "child failed" c)

      -- Retry from DLQ — child re-inserted into main queue
      dlqJobs <- dlqAll env
      Just retried <- runM env (HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      parentId retried `shouldBe` Just (primaryKey parent)

      -- Both parent (woken earlier) and retried child are claimable.
      -- Claiming parent first is harmless — ack's suspend CTE re-suspends it.
      claimed <- claimJobs env 2
      length claimed `shouldBe` 2

      -- Ack both: parent gets re-suspended (has child), child gets deleted
      forM_ claimed $ \j -> void $ runM env (HL.ackJob j)

      -- Parent should be resumed again (child ack woke it)
      assertNotSuspended env (primaryKey parent)

    it "retryFromDLQ auto-retries parent from DLQ when retrying child" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "AutoRetryParent"))
              ( JT.leaf (defaultJob (mkMessage "AutoRetryChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "AutoRetryChild2"))]
              )

      -- Claim child1, DLQ it
      [c1] <- claimJobs env 1
      payload c1 `shouldBe` mkMessage "AutoRetryChild1"
      void $ runM env (HL.moveToDLQ "child1 failed" c1)

      -- Claim child2, ack it → parent wakes (no children in main queue)
      [c2] <- claimJobs env 1
      void $ runM env (HL.ackJob c2)

      -- Parent woke — claim it, DLQ it → both parent and child1 in DLQ
      [p] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "parent failed" p)

      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 2

      -- Retry child1 from DLQ — should auto-retry parent too
      let child1Dlq = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "AutoRetryChild1") dlqJobs
      Just retriedChild <- runM env (HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey child1Dlq))
      suspended retriedChild `shouldBe` False

      assertSuspended env (primaryKey parent)

      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Claim and ack the retried child → parent wakes
      [rc] <- claimJobs env 1
      void $ runM env (HL.ackJob rc)

      assertNotSuspended env (primaryKey parent)

    it "retryFromDLQ auto-retries DLQ'd children when retrying rollup finalizer" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "SuspFinParent"))
              ( JT.leaf (defaultJob (mkMessage "SuspFinChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "SuspFinChild2"))]
              )

      -- Claim both children, DLQ both → parent wakes
      claimed <- claimJobs env 2
      length claimed `shouldBe` 2
      forM_ claimed $ \j -> void $ runM env (HL.moveToDLQ "child failed" j)

      -- Parent woke — claim it, DLQ it → all three in DLQ
      [p] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "parent failed" p)

      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Retry parent from DLQ — should auto-retry both children AND come back suspended
      let parentDlq = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "SuspFinParent") dlqJobs
      Just retriedParent <- runM env (HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey parentDlq))
      suspended retriedParent `shouldBe` True

      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Claim and ack both children — parent still suspended until last child acked
      claimedChildren <- claimJobs env 2
      length claimedChildren `shouldBe` 2
      void $ runM env (HL.ackJob (head claimedChildren))
      assertSuspended env (primaryKey parent)

      void $ runM env (HL.ackJob (claimedChildren !! 1))
      assertNotSuspended env (primaryKey parent)

    it "retryFromDLQ auto-retries parent and all siblings when retrying child" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "SibRetryParent"))
              ( JT.leaf (defaultJob (mkMessage "SibRetryChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "SibRetryChild2"))]
              )

      -- Claim both children, DLQ both → parent wakes
      claimed <- claimJobs env 2
      length claimed `shouldBe` 2
      forM_ claimed $ \j -> void $ runM env (HL.moveToDLQ "child failed" j)

      -- Parent woke — claim it, DLQ it → all three in DLQ
      [p] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "parent failed" p)

      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Retry just child1 — should auto-retry parent (suspended) AND sibling child2
      let child1Dlq = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "SibRetryChild1") dlqJobs
      Just retriedChild1 <- runM env (HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey child1Dlq))
      suspended retriedChild1 `shouldBe` False

      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      assertSuspended env (primaryKey parent)

      -- Claim and ack both children → parent wakes
      claimedChildren <- claimJobs env 2
      length claimedChildren `shouldBe` 2
      forM_ claimedChildren $ \j -> void $ runM env (HL.ackJob j)

      assertNotSuspended env (primaryKey parent)

    it "retryFromDLQ does not suspend finalizer without DLQ'd children" $ \env -> do
      Right (_parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "NoSuspFinParent"))
              (JT.leaf (defaultJob (mkMessage "NoSuspFinChild")) :| [])

      -- Claim child, ack it → parent wakes
      [c] <- claimJobs env 1
      void $ runM env (HL.ackJob c)

      -- Claim parent, DLQ it (failed for its own reasons)
      [p] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "parent failed" p)

      -- Retry parent from DLQ — should come back NOT suspended (no DLQ'd children)
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 1
      Just retriedParent <- runM env (HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      suspended retriedParent `shouldBe` False

      -- Parent should be immediately claimable
      [rc] <- claimJobs env 1
      payload rc `shouldBe` mkMessage "NoSuspFinParent"

    it "ackJobsBatch with finalizer and children" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "AckBatchParent"))
              (JT.leaf (defaultJob (mkMessage "AckBatchChild1")) NE.:| [JT.leaf (defaultJob (mkMessage "AckBatchChild2"))])

      claimedChildren <- claimJobs env 10
      length claimedChildren `shouldBe` 2

      batchResult <- runM env (HL.ackJobsBatch claimedChildren)
      batchResult `shouldBe` 2

      assertNotSuspended env (primaryKey parent)

    it "promoteJob on suspended job returns 0" $ \env -> do
      -- Insert a job, then suspend it
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "PromoteSusp")))
      n <- runM env (HL.suspendJob @m @registry @payload (primaryKey inserted))
      n `shouldBe` 1

      -- Promote should return 0 (suspended jobs have no not_visible_until to clear)
      promoted <- runM env (HL.promoteJob @m @registry @payload (primaryKey inserted))
      promoted `shouldBe` 0

      -- Job should still be suspended
      Just found <- runM env (HL.getJobById @m @registry @payload (primaryKey inserted))
      suspended found `shouldBe` True

    it "promoteJob refuses to promote in-flight job" $ \env -> do
      -- Insert and claim a job (claiming sets not_visible_until to a future time)
      Just _inserted <- runM env (HL.insertJob (defaultJob (mkMessage "PromoteInFlight")))
      claimed <- runM env (HL.claimNextVisibleJobs 1 3600) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      claimed `shouldNotBe` []
      let claimedJob = head claimed

      -- Job is now in-flight (attempts > 0, last_error IS NULL)
      -- Promote should return 0 (in-flight jobs are protected)
      promoted <- runM env (HL.promoteJob @m @registry @payload (primaryKey claimedJob))
      promoted `shouldBe` 0

      -- Job should still not be claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

    it "cancelJobsBatch partial cancel does not wake parent" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "PartialCancelParent"))
              ( JT.leaf (defaultJob (mkMessage "PartialCancelC1"))
                  :| [ JT.leaf (defaultJob (mkMessage "PartialCancelC2"))
                     , JT.leaf (defaultJob (mkMessage "PartialCancelC3"))
                     ]
              )

      length children `shouldBe` 3
      let [c1, c2, c3] = children

      -- Cancel 2 of 3 children — parent should NOT be woken yet
      deleted <- runM env (HL.cancelJobsBatch @m @registry @payload [primaryKey c1, primaryKey c2])
      deleted `shouldBe` 2
      assertSuspended env (primaryKey parent)

      -- Cancel the last child — parent should be resumed
      deleted2 <- runM env (HL.cancelJobsBatch @m @registry @payload [primaryKey c3])
      deleted2 `shouldBe` 1
      assertNotSuspended env (primaryKey parent)

    it "moveToDLQ on last main-queue child wakes parent" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQDelWakeParent"))
              (JT.leaf (defaultJob (mkMessage "DLQDelWakeChild1")) :| [JT.leaf (defaultJob (mkMessage "DLQDelWakeChild2"))])

      claimedChildren <- claimJobs env 2
      length claimedChildren `shouldBe` 2
      void $ runM env (HL.ackJob (head claimedChildren))

      -- Move second child to DLQ — parent wakes (no children in main queue)
      void $ runM env (HL.moveToDLQ "child2 failed" (claimedChildren !! 1))
      assertNotSuspended env (primaryKey parent)

    it "delete DLQ'd child when main-queue siblings still exist - parent stays suspended" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQDelNoWakeParent"))
              ( JT.leaf (defaultJob (mkMessage "DLQDelNoWakeC1"))
                  :| [ JT.leaf (defaultJob (mkMessage "DLQDelNoWakeC2"))
                     , JT.leaf (defaultJob (mkMessage "DLQDelNoWakeC3"))
                     ]
              )

      [c1] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "child1 failed" c1)

      -- Delete the DLQ'd child — parent should NOT wake (child2, child3 still in main queue)
      dlqJobs <- dlqAll env
      deleted <- runM env (HL.deleteDLQJob @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      deleted `shouldBe` 1

      assertSuspended env (primaryKey parent)

    it "moveToDLQ on both children wakes parent after last one" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQBatchWakeParent"))
              (JT.leaf (defaultJob (mkMessage "DLQBatchWakeC1")) :| [JT.leaf (defaultJob (mkMessage "DLQBatchWakeC2"))])

      claimedC <- claimJobs env 2
      length claimedC `shouldBe` 2

      -- First moveToDLQ: sibling still in main queue, parent stays suspended
      void $ runM env (HL.moveToDLQ "c1 failed" (head claimedC))
      assertSuspended env (primaryKey parent)

      -- Second moveToDLQ: no children in main queue, parent wakes
      void $ runM env (HL.moveToDLQ "c2 failed" (claimedC !! 1))
      assertNotSuspended env (primaryKey parent)

    it "cancelJob on non-last child does NOT wake parent" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CancelNonLastParent"))
              (JT.leaf (defaultJob (mkMessage "CancelNonLastC1")) :| [JT.leaf (defaultJob (mkMessage "CancelNonLastC2"))])

      -- Cancel first child only
      deleted <- runM env (HL.cancelJob @m @registry @payload (primaryKey (head children)))
      deleted `shouldBe` 1

      assertSuspended env (primaryKey parent)

    it "moveToDLQ on child - sibling ack wakes parent" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree
            ( JT.rollup
                (defaultJob (mkMessage "DLQSibAckParent"))
                (JT.leaf (defaultJob (mkMessage "DLQSibAckC1")) NE.:| [JT.leaf (defaultJob (mkMessage "DLQSibAckC2"))])
            )

      claimed <- claimJobs env 2
      length claimed `shouldBe` 2

      -- Move first child to DLQ (parent stays suspended — sibling still in main queue)
      void $ runM env (HL.moveToDLQ "child1 failed" (head claimed))
      assertSuspended env (primaryKey parent)

      -- Ack second child (last one in main queue) — parent wakes
      void $ runM env (HL.ackJob (claimed !! 1))
      assertNotSuspended env (primaryKey parent)

    it "cancelJob on child with DLQ'd sibling wakes parent" $ \env -> do
      -- Insert parent + 2 children (rollup)
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree
            ( JT.rollup
                (defaultJob (mkMessage "CancelDLQSibParent"))
                (JT.leaf (defaultJob (mkMessage "CancelDLQSibC1")) NE.:| [JT.leaf (defaultJob (mkMessage "CancelDLQSibC2"))])
            )

      -- Claim both children
      claimed <- claimJobs env 2
      length claimed `shouldBe` 2

      -- Move first child to DLQ
      void $ runM env (HL.moveToDLQ "child1 failed" (head claimed))

      -- Cancel the second child (non-DLQ) — parent wakes (no children in main queue)
      deleted <- runM env (HL.cancelJob @m @registry @payload (primaryKey (claimed !! 1)))
      deleted `shouldBe` 1

      assertNotSuspended env (primaryKey parent)

    it "ReplaceDuplicate is blocked for parent with children" $ \env -> do
      -- Insert a parent with a dedup key + children
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree
            ( JT.rollup
                ((defaultJob (mkMessage "DedupParentOrig")) {dedupKey = Just (ReplaceDuplicate "parent-dedup-key")})
                (JT.leaf (defaultJob (mkMessage "DedupParentChild")) NE.:| [])
            )

      -- Try to replace — blocked because child rows exist
      let replacement = (defaultJob (mkMessage "DedupParentReplacement")) {dedupKey = Just (ReplaceDuplicate "parent-dedup-key")}
      result <- runM env (HL.insertJob replacement)
      result `shouldBe` Nothing

      -- Original parent should still exist with original payload
      Just found <- runM env (HL.getJobById @m @registry @payload (primaryKey parent))
      payload found `shouldBe` mkMessage "DedupParentOrig"

    it "ReplaceDuplicate blocked when child is in DLQ" $ \env -> do
      -- Insert parent + child (rollup: child starts unsuspended)
      Right (_parent :| _children) <-
        runM env $
          HL.insertJobTree
            ( JT.rollup
                ((defaultJob (mkMessage "DedupDLQParentOrig")) {dedupKey = Just (ReplaceDuplicate "dlq-dedup-key")})
                (JT.leaf (defaultJob (mkMessage "DedupDLQChild")) NE.:| [])
            )

      -- Claim and DLQ the child
      [c] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "child failed" c)

      -- Try to replace parent — blocked because DLQ child row exists
      let replacement = (defaultJob (mkMessage "DedupDLQParentRepl")) {dedupKey = Just (ReplaceDuplicate "dlq-dedup-key")}
      result <- runM env (HL.insertJob replacement)
      result `shouldBe` Nothing

  describe "insertJobTree edge cases" $ do
    it "insertJobTree dedup conflict on root returns Left" $ \env -> do
      -- Pre-insert a job with a dedup key
      Just _existing <-
        runM
          env
          (HL.insertJob (defaultJob (mkMessage "TreeDedupExisting")) {dedupKey = Just (IgnoreDuplicate "tree-dedup-root")})

      -- Try to insert a tree whose root has the same dedup key
      let tree =
            JT.rollup
              ((defaultJob (mkMessage "TreeDedupConflict")) {dedupKey = Just (IgnoreDuplicate "tree-dedup-root")})
              (JT.leaf (defaultJob (mkMessage "TreeDedupChild")) NE.:| [])
      result <- runM env (HL.insertJobTree tree)
      case result of
        Left _ -> pure () -- Expected
        Right _ -> expectationFailure "Expected Left for dedup conflict on root"

      -- Verify the conflicting tree's children were NOT orphaned into the DB
      allJobs <- runM env (HL.listJobs 100 0) :: IO [JobRead payload]
      let orphans = filter (\j -> payload j == mkMessage "TreeDedupChild") allJobs
      length orphans `shouldBe` 0

  describe "Parent State Aggregation" $ do
    it "insertResult writes child result to results table" $ \env -> do
      -- Insert a tree using rollup (sets isRollup = True)
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "AggParent1"))
              (JT.leaf (defaultJob (mkMessage "AggChild1")) :| [])
      let [child] = children

      -- Verify parent is a rollup
      isRollup parent `shouldBe` True

      -- Insert a result for the child
      rowsInserted <-
        runM env $
          HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child) (Aeson.String "child1-done")
      rowsInserted `shouldBe` 1

      -- Fetch results from the results table
      results <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.lookup (primaryKey child) results `shouldBe` Just (Aeson.String "child1-done")

      -- isRollup should still be True (results stored in separate table, not on job)
      Just updatedParent <- runM env $ HL.getJobById @m @registry @payload (primaryKey parent)
      isRollup updatedParent `shouldBe` True

    it "insertResult stores multiple children" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "AggParent2"))
              ( JT.leaf (defaultJob (mkMessage "AggChild2a"))
                  :| [JT.leaf (defaultJob (mkMessage "AggChild2b"))]
              )
      let [child1, child2] = children

      -- Insert results for both children
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child1) (Aeson.Number 42)
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child2) (Aeson.Number 99)

      -- Verify both results in the results table
      results <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.lookup (primaryKey child1) results `shouldBe` Just (Aeson.Number 42)
      Map.lookup (primaryKey child2) results `shouldBe` Just (Aeson.Number 99)

    it "isRollup is False for regular jobs" $ \env -> do
      Just job <- runM env $ HL.insertJob (defaultJob (mkMessage "EitherNone"))
      isRollup job `shouldBe` False

    it "getDLQChildErrorsByParent returns errors for DLQ'd children" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQErrParent"))
              ( JT.leaf (defaultJob (mkMessage "DLQErrChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "DLQErrChild2"))]
              )
      let [child1, child2] = children

      -- Claim and DLQ both children with different error messages
      claimed <- claimJobs env 10
      length claimed `shouldBe` 2
      let c1 = head $ filter (\j -> primaryKey j == primaryKey child1) claimed
          c2 = head $ filter (\j -> primaryKey j == primaryKey child2) claimed
      void $ runM env $ HL.moveToDLQ "error-from-child-1" c1
      void $ runM env $ HL.moveToDLQ "error-from-child-2" c2

      -- getDLQChildErrorsByParent should return both errors
      errors <- runM env $ HL.getDLQChildErrorsByParent @m @registry @payload (primaryKey parent)
      Map.size errors `shouldBe` 2
      Map.lookup (primaryKey child1) errors `shouldBe` Just "error-from-child-1"
      Map.lookup (primaryKey child2) errors `shouldBe` Just "error-from-child-2"

    it "getDLQChildErrorsByParent returns empty map for parent with no DLQ'd children" $ \env -> do
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQErr0Parent"))
              (JT.leaf (defaultJob (mkMessage "DLQErr0Child")) :| [])
      errors <- runM env $ HL.getDLQChildErrorsByParent @m @registry @payload (primaryKey parent)
      errors `shouldBe` Map.empty

    it "results table and DLQ errors coexist for mixed outcomes" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "MixedParent"))
              ( JT.leaf (defaultJob (mkMessage "MixedChild1"))
                  :| [ JT.leaf (defaultJob (mkMessage "MixedChild2"))
                     , JT.leaf (defaultJob (mkMessage "MixedChild3"))
                     ]
              )
      let [child1, _child2, child3] = children

      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child1) (Aeson.String "ok-1")

      claimed <- claimJobs env 10
      let c3 = head $ filter (\j -> primaryKey j == primaryKey child3) claimed
      void $ runM env $ HL.moveToDLQ "child3-failed" c3

      results <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.lookup (primaryKey child1) results `shouldBe` Just (Aeson.String "ok-1")

      errors <- runM env $ HL.getDLQChildErrorsByParent @m @registry @payload (primaryKey parent)
      Map.lookup (primaryKey child3) errors `shouldBe` Just "child3-failed"

    it "results table stores only successful child results" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "MergeMixParent"))
              ( JT.leaf (defaultJob (mkMessage "MergeMixChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "MergeMixChild2"))]
              )
      let [child1, _child2] = children

      void $
        runM env $
          HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child1) (Aeson.toJSON (["hello"] :: [Text]))

      results <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size results `shouldBe` 1
      Map.lookup (primaryKey child1) results `shouldBe` Just (Aeson.toJSON (["hello"] :: [Text]))

    it "countDLQChildren returns count for parent with DLQ'd children" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CountDLQParent"))
              ( JT.leaf (defaultJob (mkMessage "CountDLQChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "CountDLQChild2"))]
              )
      length children `shouldBe` 2
      -- Claim and DLQ both children
      claimed <- claimJobs env 10
      forM_ claimed $ \j -> void $ runM env $ HL.moveToDLQ "fail" j
      count <- runM env $ HL.countDLQChildren @m @registry @payload (primaryKey parent)
      count `shouldBe` 2

    it "countDLQChildren returns 0 for parent with no DLQ'd children" $ \env -> do
      Right (parent :| _) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CountDLQ0Parent"))
              (JT.leaf (defaultJob (mkMessage "CountDLQ0Child")) :| [])
      count <- runM env $ HL.countDLQChildren @m @registry @payload (primaryKey parent)
      count `shouldBe` 0

    it "double DLQ round-trip preserves snapshot" $ \env -> do
      -- Insert tree, store results, ack children, claim parent
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DblDLQParent"))
              ( JT.leaf (defaultJob (mkMessage "DblDLQChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "DblDLQChild2"))]
              )
      let [child1, child2] = children
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child1) (Aeson.String "r1")
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child2) (Aeson.String "r2")
      claimed <- claimJobs env 10
      forM_ claimed $ \j -> void $ runM env (HL.ackJob j)
      [parentJob] <- claimJobs env 1

      -- First DLQ round-trip: snapshot results, move to DLQ, retry
      resultMap <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size resultMap `shouldBe` 2
      let merged = Map.map Right resultMap :: Map.Map Int64 (Either Text Aeson.Value)
      void $ runM env $ HL.persistParentState @m @registry @payload (primaryKey parent) (Aeson.toJSON merged)
      void $ runM env $ HL.moveToDLQ "round-1" parentJob
      dlqJobs1 <- dlqAll env
      let dlq1 = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "DblDLQParent") dlqJobs1
      Just retried1 <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey dlq1)

      -- Verify snapshot survived first round-trip (content, not just existence)
      isRollup retried1 `shouldBe` True
      snap1 <- runM env $ HL.getParentStateSnapshot @m @registry @payload (primaryKey retried1)
      snap1 `shouldBe` Just (Aeson.toJSON merged)

      -- Second DLQ round-trip: claim, results table is empty so worker skips persist.
      -- parent_state column already has the snapshot from retryFromDLQ, and moveToDLQ
      -- copies the full row (including parent_state) to the DLQ table.
      [parentJob2] <- claimJobs env 1
      resultMap2 <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parentJob2)
      Map.size resultMap2 `shouldBe` 0
      -- parent_state should already be populated from retryFromDLQ
      snap2a <- runM env $ HL.getParentStateSnapshot @m @registry @payload (primaryKey parentJob2)
      snap2a `shouldBe` Just (Aeson.toJSON merged)
      void $ runM env $ HL.moveToDLQ "round-2" parentJob2
      dlqJobs2 <- dlqAll env
      let dlq2 = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "DblDLQParent") dlqJobs2
      Just retried2 <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey dlq2)

      -- Verify snapshot survived second round-trip (content preserved)
      isRollup retried2 `shouldBe` True
      snap2b <- runM env $ HL.getParentStateSnapshot @m @registry @payload (primaryKey retried2)
      snap2b `shouldBe` Just (Aeson.toJSON merged)

  describe "Nested rollup with <~~ operator" $ do
    it "builds a 3-level tree with correct structure" $ \env -> do
      --  root (rollup — aggregates section results)
      --  ├── section-1 (finalizer — waits for its mappers)
      --  │   ├── mapper-1a  (leaf)
      --  │   └── mapper-1b  (leaf)
      --  ├── section-2 (finalizer)
      --  │   ├── mapper-2a  (leaf)
      --  │   └── mapper-2b  (leaf)
      --  └── mapper-solo    (leaf — direct child of root)
      Right allJobs <-
        runM env $
          HL.insertJobTree $
            JT.rollup (defaultJob (mkMessage "root: compile report")) $
              ( defaultJob (mkMessage "section-1: charts")
                  <~~ (defaultJob (mkMessage "mapper-1a") :| [defaultJob (mkMessage "mapper-1b")])
              )
                :| [ defaultJob (mkMessage "section-2: tables")
                       <~~ (defaultJob (mkMessage "mapper-2a") :| [defaultJob (mkMessage "mapper-2b")])
                   , JT.leaf (defaultJob (mkMessage "mapper-solo"))
                   ]

      -- Pre-order: root, section-1, mapper-1a, mapper-1b, section-2, mapper-2a, mapper-2b, mapper-solo
      let jobs = NE.toList allJobs
      length jobs `shouldBe` 8
      let [root, sec1, m1a, m1b, sec2, m2a, m2b, solo] = jobs

      -- Root is a rollup — suspended with isRollup
      payload root `shouldBe` mkMessage "root: compile report"
      suspended root `shouldBe` True
      isRollup root `shouldBe` True
      parentId root `shouldBe` Nothing

      -- Section finalizers — suspended, parented to root, rollup enabled
      suspended sec1 `shouldBe` True
      parentId sec1 `shouldBe` Just (primaryKey root)
      isRollup sec1 `shouldBe` True

      suspended sec2 `shouldBe` True
      parentId sec2 `shouldBe` Just (primaryKey root)
      isRollup sec2 `shouldBe` True

      -- Leaf mappers — not suspended, parented to their section
      forM_ [m1a, m1b] $ \m -> do
        suspended m `shouldBe` False
        parentId m `shouldBe` Just (primaryKey sec1)

      forM_ [m2a, m2b] $ \m -> do
        suspended m `shouldBe` False
        parentId m `shouldBe` Just (primaryKey sec2)

      -- Solo mapper — not suspended, parented directly to root
      suspended solo `shouldBe` False
      parentId solo `shouldBe` Just (primaryKey root)

      -- Only leaves are claimable (all 5 mappers)
      claimed <- claimJobs env 10
      length claimed `shouldBe` 5
      let claimedPayloads = map payload claimed
          expectedLeaves = map payload [m1a, m1b, m2a, m2b, solo]
      forM_ expectedLeaves $ \p -> claimedPayloads `shouldContain` [p]

    it "rollup aggregates child results in results table" $ \env -> do
      -- Rollup: 3 children produce partial word lists stored in results table.
      Right allJobs <-
        runM env $
          HL.insertJobTree $
            defaultJob (mkMessage "reducer")
              <~~ ( defaultJob (mkMessage "mapper-a")
                      :| [ defaultJob (mkMessage "mapper-b")
                         , defaultJob (mkMessage "mapper-c")
                         ]
                  )
      let [reducer, mapperA, mapperB, mapperC] = NE.toList allJobs

      -- Claim all 3 mappers
      claimed <- claimJobs env 10
      length claimed `shouldBe` 3

      -- Each mapper inserts its partial result
      let insertRes childId result =
            runM env $
              HL.insertResult @_ @registry @payload
                (primaryKey reducer)
                childId
                (Aeson.toJSON (result :: [Text]))
      void $ insertRes (primaryKey mapperA) ["sales", "growth"]
      void $ insertRes (primaryKey mapperB) ["revenue"]
      void $ insertRes (primaryKey mapperC) ["forecast", "trend"]

      -- Ack all mappers → wakes the reducer
      forM_ claimed $ \j -> void $ runM env (HL.ackJob j)

      -- Claim the reducer
      [reducerJob] <- claimJobs env 1
      primaryKey reducerJob `shouldBe` primaryKey reducer

      -- Read results table directly
      resultMap <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey reducer)
      Map.size resultMap `shouldBe` 3

      -- Decode and merge (what the worker does internally via readChildResults)
      let decode v = case Aeson.fromJSON v of Aeson.Success a -> a; _ -> []
          merged = foldMap (decode :: Aeson.Value -> [Text]) (Map.elems resultMap)
      length merged `shouldBe` 5
      merged `shouldMatchList` ["sales", "growth", "revenue", "forecast", "trend"]

    it "nested rollup: section finalizers aggregate then root merges" $ \env -> do
      -- Two-level rollup:
      --   root (rollup) — merges section results
      --   ├── section-1 (rollup) — merges mapper results
      --   │   ├── mapper-1a  → ["sales", "growth"]
      --   │   └── mapper-1b  → ["revenue"]
      --   └── section-2 (rollup) — merges mapper results
      --       ├── mapper-2a  → ["forecast"]
      --       └── mapper-2b  → ["trend", "outlook"]
      Right allJobs <-
        runM env $
          HL.insertJobTree $
            JT.rollup (defaultJob (mkMessage "root")) $
              ( defaultJob (mkMessage "section-1")
                  <~~ (defaultJob (mkMessage "mapper-1a") :| [defaultJob (mkMessage "mapper-1b")])
              )
                :| [ defaultJob (mkMessage "section-2")
                       <~~ (defaultJob (mkMessage "mapper-2a") :| [defaultJob (mkMessage "mapper-2b")])
                   ]
      -- Pre-order: root, section-1, mapper-1a, mapper-1b, section-2, mapper-2a, mapper-2b
      let [root, sec1, m1a, m1b, sec2, m2a, m2b] = NE.toList allJobs

      let insertRes parentPk childPk result =
            runM env $
              HL.insertResult @_ @registry @payload
                parentPk
                childPk
                (Aeson.toJSON (result :: [Text]))

      -- Step 1: Claim all 4 leaf mappers
      claimed <- claimJobs env 10
      length claimed `shouldBe` 4

      -- Step 2: Each mapper inserts its partial result into its section's results table
      void $ insertRes (primaryKey sec1) (primaryKey m1a) ["sales", "growth"]
      void $ insertRes (primaryKey sec1) (primaryKey m1b) ["revenue"]
      void $ insertRes (primaryKey sec2) (primaryKey m2a) ["forecast"]
      void $ insertRes (primaryKey sec2) (primaryKey m2b) ["trend", "outlook"]

      -- Step 3: Ack all mappers → wakes both section finalizers
      forM_ claimed $ \j -> void $ runM env (HL.ackJob j)

      -- Step 4: Claim both section finalizers
      sections <- claimJobs env 10
      length sections `shouldBe` 2

      let decode v = case Aeson.fromJSON v of Aeson.Success a -> a; _ -> ([] :: [Text])

      -- Step 5: Each section reads its children's results, merges, inserts result to root
      forM_ sections $ \secJob -> do
        resultMap <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey secJob)
        let merged = foldMap decode (Map.elems resultMap)
        void $ insertRes (primaryKey root) (primaryKey secJob) merged

      -- Step 6: Ack both sections → wakes root
      forM_ sections $ \j -> void $ runM env (HL.ackJob j)

      -- Step 7: Claim the root reducer
      [rootJob] <- claimJobs env 1
      primaryKey rootJob `shouldBe` primaryKey root

      -- Step 8: Read root's results table
      rootResultMap <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey root)

      -- Root merges section results — all 6 words present
      let finalMerged = foldMap decode (Map.elems rootResultMap)
      length finalMerged `shouldBe` 6
      finalMerged
        `shouldMatchList` ["sales", "growth", "revenue", "forecast", "trend", "outlook"]

  describe "Results Table" $ do
    it "CASCADE cleanup: acking parent deletes results rows" $ \env -> do
      -- Insert a rollup tree with 2 children
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CascParent"))
              ( JT.leaf (defaultJob (mkMessage "CascChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "CascChild2"))]
              )
      let [child1, child2] = children

      -- Insert results for both children
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child1) (Aeson.String "r1")
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child2) (Aeson.String "r2")

      -- Verify results exist
      results <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size results `shouldBe` 2

      -- Ack both children then the parent
      claimed <- claimJobs env 10
      forM_ claimed $ \j -> void $ runM env (HL.ackJob j)
      -- Parent should now be claimable
      [parentJob] <- claimJobs env 1
      primaryKey parentJob `shouldBe` primaryKey parent
      void $ runM env (HL.ackJob parentJob)

      -- Results should be deleted by FK CASCADE (parent row is gone)
      resultsAfter <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size resultsAfter `shouldBe` 0

    it "CASCADE cleanup: cancelJobCascade deletes results rows" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CascCancelParent"))
              (JT.leaf (defaultJob (mkMessage "CascCancelChild")) :| [])
      let [child] = children

      -- Insert a result
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child) (Aeson.String "r")

      -- Verify result exists
      results <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size results `shouldBe` 1

      -- Cascade cancel the parent (deletes parent + children)
      void $ runM env $ HL.cancelJobCascade @m @registry @payload (primaryKey parent)

      -- Results should be gone
      resultsAfter <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size resultsAfter `shouldBe` 0

    it "idempotent upsert: duplicate insertResult overwrites" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "UpsertParent"))
              (JT.leaf (defaultJob (mkMessage "UpsertChild")) :| [])
      let [child] = children

      -- Insert result
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child) (Aeson.String "v1")

      -- Insert again with different value — should overwrite, not fail
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child) (Aeson.String "v2")

      -- Verify latest value
      results <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.lookup (primaryKey child) results `shouldBe` Just (Aeson.String "v2")

    it "rollup finalizer: isRollup is set, results table starts empty" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            defaultJob (mkMessage "PlainFinParent")
              <~~ (defaultJob (mkMessage "PlainFinChild") :| [])
      let [child] = children

      isRollup parent `shouldBe` True

      -- Results table starts empty
      results <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size results `shouldBe` 0

      -- Manually inserting works
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child) (Aeson.String "manual")
      results2 <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size results2 `shouldBe` 1

    it "DLQ preserves accumulated results via parent_state snapshot" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQSnapParent"))
              ( JT.leaf (defaultJob (mkMessage "DLQSnapChild1"))
                  :| [JT.leaf (defaultJob (mkMessage "DLQSnapChild2"))]
              )
      let [child1, child2] = children

      -- Insert results for both children
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child1) (Aeson.String "snap-r1")
      void $ runM env $ HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child2) (Aeson.String "snap-r2")

      -- Ack children to wake the parent
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2
      forM_ claimed $ \j -> void $ runM env (HL.ackJob j)

      -- Parent should be claimable now
      [parentJob] <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      primaryKey parentJob `shouldBe` primaryKey parent

      -- Simulate worker: persist results snapshot, then move to DLQ
      resultMap <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size resultMap `shouldBe` 2
      let mergedSnap = Map.map Right resultMap :: Map.Map Int64 (Either Text Aeson.Value)
      void $ runM env $ HL.persistParentState @m @registry @payload (primaryKey parent) (Aeson.toJSON mergedSnap)
      void $ runM env $ HL.moveToDLQ "test-failure" parentJob

      -- Results table rows should be gone (FK CASCADE on DELETE)
      resultsAfter <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      Map.size resultsAfter `shouldBe` 0

      -- DLQ job should still be marked as rollup
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      let dlqJob = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "DLQSnapParent") dlqJobs
      isRollup (DLQ.jobSnapshot dlqJob) `shouldBe` True

    it "DLQ retry preserves parent_state snapshot" $ \env -> do
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "DLQRetryParent"))
              (JT.leaf (defaultJob (mkMessage "DLQRetryChild")) :| [])
      let [child] = children

      -- Insert result, ack child, claim parent
      void $
        runM env $
          HL.insertResult @_ @registry @payload (primaryKey parent) (primaryKey child) (Aeson.String "retry-val")
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      forM_ claimed $ \j -> void $ runM env (HL.ackJob j)
      [parentJob] <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      -- Persist results and move to DLQ
      resultMap <- runM env $ HL.getResultsByParent @_ @registry @payload (primaryKey parent)
      let mergedSnap = Map.map Right resultMap :: Map.Map Int64 (Either Text Aeson.Value)
      void $ runM env $ HL.persistParentState @m @registry @payload (primaryKey parent) (Aeson.toJSON mergedSnap)
      void $ runM env $ HL.moveToDLQ "test-err" parentJob

      -- Retry from DLQ — snapshot should be preserved in parent_state column
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      let dlqJob = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "DLQRetryParent") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey dlqJob)
      isRollup retried `shouldBe` True
      snap <- runM env $ HL.getParentStateSnapshot @m @registry @payload (primaryKey retried)
      snap `shouldBe` Just (Aeson.toJSON mergedSnap)

  describe "Cascade DLQ for rollup trees" $ do
    it "moveToDLQ on rollup parent cascades children to DLQ" $ \env -> do
      -- Insert rollup tree: parent + 2 children
      Right (parent :| children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CascDLQParent"))
              (JT.leaf (defaultJob (mkMessage "CascDLQChild1")) :| [JT.leaf (defaultJob (mkMessage "CascDLQChild2"))])

      -- Parent is suspended (rollup), children are claimable
      assertSuspended env (primaryKey parent)
      length children `shouldBe` 2

      -- Admin-style: moveToDLQ on parent before children finish
      void $ runM env (HL.moveToDLQ "Admin DLQ" parent)

      -- All 3 should be in DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Parent in DLQ with original error
      let parentDLQ = filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "CascDLQParent") dlqJobs
      length parentDLQ `shouldBe` 1
      lastError (DLQ.jobSnapshot (head parentDLQ)) `shouldBe` Just "Admin DLQ"

      -- Children in DLQ with cascade error
      let childDLQs = filter (\d -> payload (DLQ.jobSnapshot d) /= mkMessage "CascDLQParent") dlqJobs
      length childDLQs `shouldBe` 2
      forM_ childDLQs $ \d ->
        lastError (DLQ.jobSnapshot d) `shouldBe` Just "Parent moved to DLQ"

      -- Main queue should be empty
      remaining <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length remaining `shouldBe` 0

    it "moveToDLQ cascade + retryFromDLQ recovers full tree" $ \env -> do
      -- Insert rollup tree: parent + 2 children
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CascRetryParent"))
              (JT.leaf (defaultJob (mkMessage "CascRetryChild1")) :| [JT.leaf (defaultJob (mkMessage "CascRetryChild2"))])

      -- moveToDLQ on parent → all 3 in DLQ
      void $ runM env (HL.moveToDLQ "Admin DLQ" parent)
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- retryFromDLQ on parent → entire tree recovered
      let parentDLQ = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "CascRetryParent") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey parentDLQ)
      isRollup retried `shouldBe` True
      suspended retried `shouldBe` True

      -- DLQ should be empty now (children auto-retried)
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Children should be claimable
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2
      map payload claimed `shouldMatchList` [mkMessage "CascRetryChild1", mkMessage "CascRetryChild2"]

      -- Ack children → parent wakes
      forM_ claimed $ \j -> void $ runM env (HL.ackJob j)
      assertNotSuspended env (primaryKey retried)

      -- Parent should be claimable
      [parentClaimed] <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      payload parentClaimed `shouldBe` mkMessage "CascRetryParent"

    it "moveToDLQ cascade handles multi-level nesting" $ \env -> do
      -- Build 3-level tree: grandparent → parent → [child1, child2]
      Right (grandparent :| rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "CascGrandparent"))
              ( JT.rollup
                  (defaultJob (mkMessage "CascMidParent"))
                  (JT.leaf (defaultJob (mkMessage "CascGrandChild1")) :| [JT.leaf (defaultJob (mkMessage "CascGrandChild2"))])
                  :| []
              )

      -- grandparent is suspended, mid-parent is suspended, children are claimable
      let midParent = head rest
      assertSuspended env (primaryKey grandparent)
      assertSuspended env (primaryKey midParent)

      -- moveToDLQ on grandparent
      void $ runM env (HL.moveToDLQ "Admin cascade" grandparent)

      -- All 4 should be in DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 4

      -- Main queue should be empty
      mainCount <- runM env (HL.countJobs @m @registry @payload)
      mainCount `shouldBe` 0

    it "moveToDLQ on non-rollup job does not cascade" $ \env -> do
      -- Insert a rollup tree, then moveToDLQ a child (non-rollup)
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "NoCascParent"))
              (JT.leaf (defaultJob (mkMessage "NoCascChild1")) :| [JT.leaf (defaultJob (mkMessage "NoCascChild2"))])

      -- Claim children
      claimed <- runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2

      -- moveToDLQ on one child (not a rollup)
      let child1 = head claimed
      isRollup child1 `shouldBe` False
      void $ runM env (HL.moveToDLQ "child error" child1)

      -- Only that child should be in DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 1
      payload (DLQ.jobSnapshot (head dlqJobs)) `shouldBe` payload child1

      -- Parent still in main queue (suspended), sibling still in main queue
      assertSuspended env (primaryKey parent)
      mainCount <- runM env (HL.countJobs @m @registry @payload)
      mainCount `shouldBe` 2 -- parent + remaining child
  describe "moveToDLQBatch cascades for rollup parents" $ do
    it "moveToDLQBatch on rollup parent cascades children to DLQ" $ \env -> do
      -- Insert rollup tree: parent + 2 children (parent is suspended with attempts=0)
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "BatchCascParent"))
              (JT.leaf (defaultJob (mkMessage "BatchCascChild1")) :| [JT.leaf (defaultJob (mkMessage "BatchCascChild2"))])

      -- moveToDLQBatch on the parent
      moved <- runM env (HL.moveToDLQBatch [(parent, "Batch admin DLQ")])
      moved `shouldBe` 1

      -- All 3 should be in DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Children should have cascade error message
      let childDLQs = filter (\d -> payload (DLQ.jobSnapshot d) /= mkMessage "BatchCascParent") dlqJobs
      length childDLQs `shouldBe` 2
      forM_ childDLQs $ \d ->
        lastError (DLQ.jobSnapshot d) `shouldBe` Just "Parent moved to DLQ"

      -- Main queue should be empty
      mainCount <- runM env (HL.countJobs @m @registry @payload)
      mainCount `shouldBe` 0

  describe "Intermediate rollup snapshots survive cascade DLQ" $ do
    it "3-level tree: mid-level rollup snapshot preserved after cascade" $ \env -> do
      -- Build 3-level tree: grandparent → mid-parent (rollup) → [child1, child2]
      Right (grandparent :| rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "SnapGrandparent"))
              ( JT.rollup
                  (defaultJob (mkMessage "SnapMidParent"))
                  (JT.leaf (defaultJob (mkMessage "SnapChild1")) :| [JT.leaf (defaultJob (mkMessage "SnapChild2"))])
                  :| []
              )

      let midParent = head rest

      -- Claim and ack one child (so mid-parent has a partial result)
      claimed <- claimJobs env 2
      length claimed `shouldBe` 2
      let child1 = head claimed
      -- Insert a result for child1 under mid-parent, then ack child1
      void $
        runM env $
          HL.insertResult @_ @registry @payload (primaryKey midParent) (primaryKey child1) (Aeson.String "child1-result")
      void $ runM env (HL.ackJob child1)

      -- Mid-parent still suspended (one child remains)
      assertSuspended env (primaryKey midParent)

      -- Cascade DLQ on grandparent — should snapshot mid-parent's results first
      void $ runM env (HL.moveToDLQ "Admin cascade" grandparent)

      -- 3 in DLQ (grandparent, mid-parent, child2 — child1 was already acked)
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Retry the whole tree from grandparent
      let gpDLQ = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "SnapGrandparent") dlqJobs
      Just retriedGP <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey gpDLQ)

      -- DLQ should be empty — whole tree retried
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Grandparent should be suspended (has children)
      assertSuspended env (primaryKey retriedGP)

      -- Mid-parent should have its snapshot preserved with child1's result
      snap <- runM env $ HL.getParentStateSnapshot @m @registry @payload (primaryKey midParent)
      let expectedSnap = Aeson.toJSON (Map.singleton (primaryKey child1) (Right (Aeson.String "child1-result") :: Either Text Aeson.Value))
      snap `shouldBe` Just expectedSnap

  describe "N-level retryFromDLQ" $ do
    it "retryFromDLQ from a child retries the entire tree" $ \env -> do
      -- Build 2-level tree: parent → [child1, child2]
      Right (_parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "NLevelRetryParent"))
              (JT.leaf (defaultJob (mkMessage "NLevelRetryChild1")) :| [JT.leaf (defaultJob (mkMessage "NLevelRetryChild2"))])

      -- DLQ the parent (cascades children)
      Just parent' <- runM env $ HL.getJobById @m @registry @payload (primaryKey _parent)
      void $ runM env (HL.moveToDLQ "Admin" parent')

      -- All in DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Retry from a CHILD — should recover the whole tree
      let child1DLQ = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "NLevelRetryChild1") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey child1DLQ)

      -- Returned job should be the child we requested
      payload retried `shouldBe` mkMessage "NLevelRetryChild1"

      -- DLQ should be empty
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Parent should be in main queue, suspended
      assertSuspended env (primaryKey _parent)

      -- Both children should be claimable
      claimed <- claimJobs env 10
      length claimed `shouldBe` 2
      map payload claimed `shouldMatchList` [mkMessage "NLevelRetryChild1", mkMessage "NLevelRetryChild2"]

    it "retryFromDLQ from a grandchild retries the entire 3-level tree" $ \env -> do
      -- Build 3-level tree
      Right (grandparent :| rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "3LRetryGP"))
              ( JT.rollup
                  (defaultJob (mkMessage "3LRetryMid"))
                  (JT.leaf (defaultJob (mkMessage "3LRetryLeaf1")) :| [JT.leaf (defaultJob (mkMessage "3LRetryLeaf2"))])
                  :| []
              )

      let midParent = head rest

      -- DLQ grandparent → cascades all 4
      void $ runM env (HL.moveToDLQ "Admin" grandparent)
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 4

      -- Retry from a LEAF — should recover the entire 3-level tree
      let leafDLQ = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "3LRetryLeaf1") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey leafDLQ)

      -- Returned job should be the leaf
      payload retried `shouldBe` mkMessage "3LRetryLeaf1"

      -- DLQ should be empty
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Grandparent suspended (has mid-parent child)
      assertSuspended env (primaryKey grandparent)
      -- Mid-parent suspended (has leaf children)
      assertSuspended env (primaryKey midParent)

      -- Leaf children should be claimable
      claimed <- claimJobs env 10
      length claimed `shouldBe` 2
      map payload claimed `shouldMatchList` [mkMessage "3LRetryLeaf1", mkMessage "3LRetryLeaf2"]

      -- Ack both leaves → mid-parent wakes
      forM_ claimed $ \j -> void $ runM env (HL.ackJob j)
      assertNotSuspended env (primaryKey midParent)

      -- Claim and ack mid-parent → grandparent wakes
      [midClaimed] <- claimJobs env 1
      payload midClaimed `shouldBe` mkMessage "3LRetryMid"
      void $ runM env (HL.ackJob midClaimed)
      assertNotSuspended env (primaryKey grandparent)

      -- Grandparent should be claimable
      [gpClaimed] <- claimJobs env 1
      payload gpClaimed `shouldBe` mkMessage "3LRetryGP"

    it "single child: moveToDLQ cascades and retries correctly" $ \env -> do
      Right (parent :| _) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "SingleDLQParent"))
              (JT.leaf (defaultJob (mkMessage "SingleDLQChild")) :| [])

      -- Cascade DLQ the parent
      void $ runM env (HL.moveToDLQ "admin" parent)
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 2

      -- Retry from child
      let childDLQ = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "SingleDLQChild") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey childDLQ)
      payload retried `shouldBe` mkMessage "SingleDLQChild"

      -- DLQ empty, parent suspended, child claimable
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0
      assertSuspended env (primaryKey parent)
      [claimed] <- claimJobs env 1
      payload claimed `shouldBe` mkMessage "SingleDLQChild"

  describe "retryFromDLQ edge cases" $ do
    it "retryFromDLQ on standalone non-rollup job" $ \env -> do
      -- Insert a regular job, claim it, DLQ it, retry it
      Just _job <- runM env (HL.insertJob (defaultJob (mkMessage "StandaloneRetry")))
      [claimed] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "error" claimed)

      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 1

      Just retried <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs))
      payload retried `shouldBe` mkMessage "StandaloneRetry"
      suspended retried `shouldBe` False
      isRollup retried `shouldBe` False

      -- Should be claimable
      [reClaimed] <- claimJobs env 1
      payload reClaimed `shouldBe` mkMessage "StandaloneRetry"

    it "retryFromDLQ on child whose parent is still in main queue" $ \env -> do
      -- Tree: parent + 2 children. DLQ one child while parent is alive.
      -- retryFromDLQ should retry just that child without touching the parent.
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "LiveParent"))
              (JT.leaf (defaultJob (mkMessage "LiveChild1")) :| [JT.leaf (defaultJob (mkMessage "LiveChild2"))])

      -- Claim only 1 child and DLQ it (leave the other unclaimed)
      [child1] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "child fail" child1)

      -- Parent is still in main queue (suspended, other child still exists)
      assertSuspended env (primaryKey parent)

      -- retryFromDLQ on child1 — parent is in main queue, so ancestor walk stops
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 1
      Just retried <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey (head dlqJobs))
      payload retried `shouldBe` payload child1

      -- DLQ empty, child1 is back in main queue
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Parent still suspended (both children in main queue again)
      assertSuspended env (primaryKey parent)

      -- Claim and ack both children → parent wakes
      reClaimed <- claimJobs env 2
      length reClaimed `shouldBe` 2
      forM_ reClaimed $ \j -> void $ runM env (HL.ackJob j)
      assertNotSuspended env (primaryKey parent)

  describe "moveToDLQBatch mixed rollup and non-rollup" $ do
    it "moveToDLQBatch cascades rollup parents but not regular jobs" $ \env -> do
      -- Insert standalone first and claim it before tree exists
      Just _standalone <- runM env (HL.insertJob (defaultGroupedJob "mix-standalone" (mkMessage "MixBatchStandalone")))
      [standaloneClaimed] <- claimJobs env 1
      payload standaloneClaimed `shouldBe` mkMessage "MixBatchStandalone"

      -- Now insert the rollup tree
      Right (parent :| _) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "MixBatchParent"))
              (JT.leaf (defaultJob (mkMessage "MixBatchChild1")) :| [JT.leaf (defaultJob (mkMessage "MixBatchChild2"))])

      -- moveToDLQBatch with both the rollup parent and the standalone job
      moved <- runM env (HL.moveToDLQBatch [(parent, "rollup error"), (standaloneClaimed, "standalone error")])
      moved `shouldBe` 2

      -- DLQ should have: parent + 2 cascaded children + standalone = 4
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 4

      -- Standalone has its own error
      let standaloneDLQ = filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "MixBatchStandalone") dlqJobs
      length standaloneDLQ `shouldBe` 1
      lastError (DLQ.jobSnapshot (head standaloneDLQ)) `shouldBe` Just "standalone error"

      -- Children have cascade error
      let childDLQs = filter (\d -> payload (DLQ.jobSnapshot d) `elem` [mkMessage "MixBatchChild1", mkMessage "MixBatchChild2"]) dlqJobs
      length childDLQs `shouldBe` 2
      forM_ childDLQs $ \d ->
        lastError (DLQ.jobSnapshot d) `shouldBe` Just "Parent moved to DLQ"

      -- Main queue empty
      mainCount <- runM env (HL.countJobs @m @registry @payload)
      mainCount `shouldBe` 0

  describe "4-level nesting" $ do
    it "4-level tree: full lifecycle (insert, ack bottom-up, completion cascade)" $ \env -> do
      -- L1 (root) → L2 (rollup) → L3 (rollup) → [L4a, L4b]
      Right (l1 :| rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "L1Root"))
              ( JT.rollup
                  (defaultJob (mkMessage "L2Mid"))
                  ( JT.rollup
                      (defaultJob (mkMessage "L3Inner"))
                      (JT.leaf (defaultJob (mkMessage "L4LeafA")) :| [JT.leaf (defaultJob (mkMessage "L4LeafB"))])
                      :| []
                  )
                  :| []
              )

      -- 5 jobs total: L1, L2, L3, L4a, L4b
      length rest `shouldBe` 4

      let l2 = head rest
          l3 = rest !! 1

      -- All rollup ancestors suspended
      assertSuspended env (primaryKey l1)
      assertSuspended env (primaryKey l2)
      assertSuspended env (primaryKey l3)

      -- Only leaves are claimable
      leaves <- claimJobs env 10
      length leaves `shouldBe` 2
      map payload leaves `shouldMatchList` [mkMessage "L4LeafA", mkMessage "L4LeafB"]

      -- Ack both leaves → L3 wakes
      forM_ leaves $ \j -> void $ runM env (HL.ackJob j)
      assertNotSuspended env (primaryKey l3)
      assertSuspended env (primaryKey l2)
      assertSuspended env (primaryKey l1)

      -- Claim and ack L3 → L2 wakes
      [l3Claimed] <- claimJobs env 1
      payload l3Claimed `shouldBe` mkMessage "L3Inner"
      void $ runM env (HL.ackJob l3Claimed)
      assertNotSuspended env (primaryKey l2)
      assertSuspended env (primaryKey l1)

      -- Claim and ack L2 → L1 wakes
      [l2Claimed] <- claimJobs env 1
      payload l2Claimed `shouldBe` mkMessage "L2Mid"
      void $ runM env (HL.ackJob l2Claimed)
      assertNotSuspended env (primaryKey l1)

      -- Claim and ack L1 → done
      [l1Claimed] <- claimJobs env 1
      payload l1Claimed `shouldBe` mkMessage "L1Root"
      void $ runM env (HL.ackJob l1Claimed)
      assertGone env (primaryKey l1)

    it "4-level tree: cascade DLQ from root and retry from deepest leaf" $ \env -> do
      Right (l1 :| rest) <-
        runM env $
          HL.insertJobTree $
            JT.rollup
              (defaultJob (mkMessage "4LDLQRoot"))
              ( JT.rollup
                  (defaultJob (mkMessage "4LDLQMid"))
                  ( JT.rollup
                      (defaultJob (mkMessage "4LDLQInner"))
                      (JT.leaf (defaultJob (mkMessage "4LDLQLeafA")) :| [JT.leaf (defaultJob (mkMessage "4LDLQLeafB"))])
                      :| []
                  )
                  :| []
              )

      let l2 = head rest
          l3 = rest !! 1

      -- DLQ from root → all 5 in DLQ
      void $ runM env (HL.moveToDLQ "admin" l1)
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 5

      -- Retry from deepest leaf
      let leafDLQ = head $ filter (\d -> payload (DLQ.jobSnapshot d) == mkMessage "4LDLQLeafA") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @m @registry @payload (DLQ.dlqPrimaryKey leafDLQ)
      payload retried `shouldBe` mkMessage "4LDLQLeafA"

      -- DLQ empty
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- All rollup ancestors suspended
      assertSuspended env (primaryKey l1)
      assertSuspended env (primaryKey l2)
      assertSuspended env (primaryKey l3)

      -- Only leaves claimable
      leaves <- claimJobs env 10
      length leaves `shouldBe` 2
      map payload leaves `shouldMatchList` [mkMessage "4LDLQLeafA", mkMessage "4LDLQLeafB"]

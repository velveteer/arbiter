{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial -Wno-incomplete-uni-patterns #-}

-- | Parameterized operations test suite, instantiated for each 'MonadArbiter' backend.
module Arbiter.Test.Operations
  ( operationsSpec
  ) where

import Arbiter.Core.Codec (Col (..), col, pval)
import Arbiter.Core.HighLevel (SetVisibilityResult (..))
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types
import Arbiter.Core.JobResult (EncodeJobResult)
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf, ResultOf, getSchema)
import Arbiter.Core.MonadArbiter qualified as MA
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (TableForPayload)
import Arbiter.Core.Sql.DLQ qualified as Tmpl
import Arbiter.Core.Sql.Groups qualified as GroupsTmpl
import Arbiter.Core.Sql.Tree qualified as TreeTmpl
import Control.Concurrent (threadDelay)
import Control.Monad (forM, forM_, void)
import Data.Aeson qualified as Aeson
import Data.Int (Int32, Int64)
import Data.List (find, nub, sort)
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust, listToMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (addUTCTime, getCurrentTime)
import Data.UUID.Types qualified as UUID
import GHC.TypeLits (KnownSymbol)
import Test.Hspec
import UnliftIO.Async (concurrently)

import Arbiter.Test.Setup (execQuery, execStatement, truncateToMicros)

-- | Build a test suite for the given 'MonadArbiter' runner.
operationsSpec
  :: forall payload m env
   . ( EncodeJobResult (ResultOf m payload)
     , Eq payload
     , JobPayload payload
     , KnownSymbol (TableForPayload payload (RegistryOf m))
     , MonadArbiter m
     , Show payload
     )
  => (Text -> payload)
  -- ^ Constructor for a simple test message payload
  -> (Text -> ResultOf m payload)
  -- ^ Constructor for the queue's declared handler result
  -> (forall a. env -> m a -> IO a)
  -- ^ Runner for monad actions
  -> SpecWith env
operationsSpec mkMessage mkResult runM = do
  -- Test helpers
  let claimJobs env count = runM env (HL.claimNextVisibleJobs count 60) :: IO [JobRead payload]
      claimJobsAs env count worker = runM env (HL.claimNextVisibleJobsAs count 60 worker) :: IO [JobRead payload]
      getJob env jobId = runM env (HL.getJobById @payload jobId)
      assertSuspended env jobId = do
        Just job <- getJob env jobId
        suspended job `shouldBe` True
      assertNotSuspended env jobId = do
        Just job <- getJob env jobId
        suspended job `shouldBe` False
      assertGone env jobId = do
        fetched <- getJob env jobId
        fetched `shouldBe` Nothing
      dlqAll env = runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      deleteCancelledAs env owner jobIds =
        runM env $ do
          schemaName <- getSchema
          Ops.deleteCancelledJobs schemaName (HL.queueTable @payload @m) (Just owner) jobIds
      groupsTable = do
        schemaName <- getSchema
        pure (Schema.jobQueueGroupsTable schemaName (HL.queueTable @payload @m))
      deleteRowDirectly env jobId =
        runM env $ do
          schemaName <- getSchema
          let tbl = Schema.jobQueueTable schemaName (HL.queueTable @payload @m)
          void $ execStatement ("DELETE FROM " <> tbl <> " WHERE id = ?") [pval CInt8 jobId]
      lockedFromRoot env jobIds =
        runM env $ do
          schemaName <- getSchema
          sum <$> MA.executeQuery (TreeTmpl.lockJobTreesFromRootSQL schemaName (HL.queueTable @payload @m) jobIds)
      driftGroupCount env key count =
        runM env $ do
          tbl <- groupsTable
          void $
            execStatement
              ("UPDATE " <> tbl <> " SET job_count = ? WHERE group_key = ?")
              [pval CInt4 count, pval CText key]
      groupCount env key =
        runM env $ do
          tbl <- groupsTable
          rows <- execQuery ("SELECT job_count FROM " <> tbl <> " WHERE group_key = ?") [pval CText key] (col "job_count" CInt4)
          pure (listToMaybe rows :: Maybe Int32)

  describe "job kind" $ do
    it "stores the label its payload derives" $ \env -> do
      let job = setGroupKey (Just "kind-store") $ defaultJob (mkMessage "labelled")
      Just inserted <- runM env (HL.insertJob job)
      jobKind (payloadKeys inserted) `shouldBe` kindOf (mkMessage "labelled" :: payload)
      jobKind (payloadKeys inserted) `shouldNotBe` Nothing

    it "narrows a listing to one label" $ \env -> do
      let job = setGroupKey (Just "kind-filter") $ defaultJob (mkMessage "filtered")
      void $ runM env (HL.insertJob job)
      Just kind <- pure (kindOf (mkMessage "filtered" :: payload))
      matched <- runM env (HL.listJobsFiltered [Ops.FilterKind kind] 10 0) :: IO [JobRead payload]
      map payload matched `shouldBe` [mkMessage "filtered"]
      missed <- runM env (HL.listJobsFiltered [Ops.FilterKind "NoSuchKind"] 10 0) :: IO [JobRead payload]
      missed `shouldBe` []

    it "counts queue depth by label" $ \env -> do
      let counted = mkMessage "counted" :: payload
      void $ runM env (HL.insertJob (setGroupKey (Just "kind-count") (defaultJob counted)))
      void $ runM env (HL.insertJob (setGroupKey (Just "kind-count") (defaultJob counted)))
      Just kind <- pure (kindOf counted)
      stats <- runM env (HL.getQueueStats @payload)
      Map.lookup kind (Ops.kindCounts stats) `shouldBe` Just 2

    it "carries the label into the dead-letter queue" $ \env -> do
      let job = setGroupKey (Just "kind-dlq") $ defaultJob (mkMessage "dead")
      void $ runM env (HL.insertJob job)
      claimed <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "boom" (head claimed))
      Just kind <- pure (kindOf (mkMessage "dead" :: payload))
      dead <- runM env (HL.listDLQFiltered [Ops.FilterKind kind] 10 0) :: IO [DLQ.DLQJob payload]
      map (jobKind . payloadKeys . DLQ.jobSnapshot) dead `shouldBe` [Just kind]

  describe "claimNextVisibleJobs" $ do
    it "claims jobs in priority order" $ \env -> do
      -- Insert jobs with different priorities
      let highPriority =
            setPriority 0 $ defaultGroupedJob "claim-priority-test" (mkMessage "High")
          lowPriority =
            setPriority 10 $ defaultGroupedJob "claim-priority-test" (mkMessage "Low")

      void $ runM env (HL.insertJob lowPriority)
      void $ runM env (HL.insertJob highPriority)

      -- Claim one job
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1
      payload (head claimed) `shouldBe` mkMessage "High"

    it "claims the ready top-priority job ahead of a scheduled sibling in the same group" $ \env -> do
      now <- getCurrentTime
      -- A low-priority job scheduled to run in the future. It is inserted first
      -- and has the lower id.
      let future = truncateToMicros (addUTCTime 3600 now)
          deprioritizedScheduled =
            setNotVisibleUntil (Just future) $ setPriority 10 $ defaultGroupedJob "claim-priority-scheduled" (mkMessage "Scheduled")
          topPriority =
            setPriority 0 $ defaultGroupedJob "claim-priority-scheduled" (mkMessage "Top")

      void $ runM env (HL.insertJob deprioritizedScheduled)
      void $ runM env (HL.insertJob topPriority)

      -- The not-yet-due scheduled job does not block the group. The ready
      -- top-priority job is claimed.
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1
      payload (head claimed) `shouldBe` mkMessage "Top"
      priority (head claimed) `shouldBe` 0

    it "claims jobs from different groups" $ \env -> do
      let job1 = setGroupKey (Just "group1") $ defaultJob (mkMessage "G1")
          job2 = setGroupKey (Just "group2") $ defaultJob (mkMessage "G2")

      void $ runM env (HL.insertJob job1)
      void $ runM env (HL.insertJob job2)

      claimed <- runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]

      length claimed `shouldBe` 2
      map groupKey claimed `shouldMatchList` [Just "group1", Just "group2"]

    it "respects per-group ordering" $ \env -> do
      -- Insert two jobs in the same group
      let job1 = setGroupKey (Just "claim-hol-test") $ defaultJob (mkMessage "First")
          job2 = setGroupKey (Just "claim-hol-test") $ defaultJob (mkMessage "Second")

      void $ runM env (HL.insertJob job1)
      void $ runM env (HL.insertJob job2)

      -- Claim only 1 job.
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1
      payload (head claimed) `shouldBe` mkMessage "First"

      -- The second job stays in the queue and is not claimable until the first is acked.
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

    it "ungrouped jobs can be claimed in parallel" $ \env -> do
      -- Insert two ungrouped jobs
      let job1 = defaultJob (mkMessage "Ungrouped1")
          job2 = defaultJob (mkMessage "Ungrouped2")

      void $ runM env (HL.insertJob job1)
      void $ runM env (HL.insertJob job2)

      -- Both are claimable at once
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

      -- Claim 3 out of 5. The 3 lowest ids come out.
      claimed <- runM env (HL.claimNextVisibleJobs 3 60) :: IO [JobRead payload]

      length claimed `shouldBe` 3
      let ungroupedCount = length $ filter (\job -> groupKey job == Nothing) claimed
      let groupedCount = length $ filter (\job -> groupKey job /= Nothing) claimed
      -- U1 (id=1), G1 (id=2), U2 (id=3) claimed. G2 (id=4) and U3 (id=5) left
      ungroupedCount `shouldBe` 2
      groupedCount `shouldBe` 1
      map payload claimed `shouldBe` [mkMessage "U1", mkMessage "G1", mkMessage "U2"]

    it "increments attempts on claim" $ \env -> do
      let job = setGroupKey (Just "claim-attempts-test") $ defaultJob (mkMessage "Test")

      Just inserted <- runM env (HL.insertJob job)
      attempts inserted `shouldBe` 0

      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      attempts (head claimed) `shouldBe` 1

    it "claimed jobs are not re-claimable" $ \env -> do
      let job = setGroupKey (Just "claim-visibility-test") $ defaultJob (mkMessage "Test")

      void $ runM env (HL.insertJob job)

      -- Claim the job
      claimed1 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed1 `shouldBe` 1

      -- A second claim gets nothing.
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

  describe "ackJob" $ do
    it "removes a job from the queue" $ \env -> do
      let job = setGroupKey (Just "ack-remove-test") $ defaultJob (mkMessage "Test")

      void $ runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1

      -- Acknowledge the job
      void $ runM env (HL.ackJob (head claimed))

      -- A second claim gets nothing.
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

    it "allows next job in group to be claimed after ack" $ \env -> do
      let job1 = setGroupKey (Just "ack-next-test") $ defaultJob (mkMessage "First")
          job2 = setGroupKey (Just "ack-next-test") $ defaultJob (mkMessage "Second")

      void $ runM env (HL.insertJob job1)
      void $ runM env (HL.insertJob job2)

      -- Claim first job
      claimed1 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed1 `shouldBe` 1
      payload (head claimed1) `shouldBe` mkMessage "First"

      -- Ack it
      void $ runM env (HL.ackJob (head claimed1))

      -- The second job is claimable now
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1
      payload (head claimed2) `shouldBe` mkMessage "Second"

    it "does not complete a force-cancel-flagged job" $ \env -> do
      -- A handler that acks after force-cancel has flagged its job does not complete it.
      let job = defaultJob (mkMessage "cancel-then-ack")
      Just inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobsAs 1 60 UUID.nil) :: IO [JobRead payload]
      length claimed `shouldBe` 1

      flagged <- runM env (HL.forceCancelJob @payload (primaryKey inserted))
      flagged `shouldBe` 1

      acked <- runM env (HL.ackJob (head claimed))
      acked `shouldBe` 0

      -- The flagged row survives, left for the cancel path to reap.
      getJob env (primaryKey inserted) >>= (`shouldSatisfy` isJust)

    it "leaves a force-cancel-flagged job for the worker holding its lease" $ \env -> do
      -- Deleting it elsewhere takes the flag away before its holder can read it.
      let owner = UUID.nil
          other = UUID.fromWords 1 1 1 1
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "cancel-lease-owner")))
      let jobId = primaryKey inserted
      claimed <- runM env (HL.claimNextVisibleJobsAs 1 60 owner) :: IO [JobRead payload]
      length claimed `shouldBe` 1

      flagged <- runM env (HL.forceCancelJob @payload jobId)
      flagged `shouldBe` 1

      deleteCancelledAs env other [jobId] >>= (`shouldBe` [])
      getJob env jobId >>= (`shouldSatisfy` isJust)

      deleteCancelledAs env owner [jobId] >>= (`shouldBe` [jobId])
      assertGone env jobId

  describe "nackJob" $ do
    it "refunds one attempt however often the same claim nacks" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "nack-repeat")))
      firstClaim <- claimJobsAs env 1 UUID.nil
      void $ runM env (HL.setVisibilityTimeout 0 (head firstClaim))
      held <- claimJobsAs env 1 UUID.nil
      map attempts held `shouldBe` [2]

      runM env (HL.nackJob (head held)) `shouldReturn` 1
      runM env (HL.nackJob (head held)) `shouldReturn` 0

      Just reread <- getJob env (primaryKey inserted)
      attempts reread `shouldBe` 1
      claimedBy reread `shouldBe` Nothing

    it "a nacked job is not counted in flight" $ \env -> do
      Just _inserted <- runM env (HL.insertJob (defaultJob (mkMessage "nack-status")))
      firstClaim <- claimJobsAs env 1 UUID.nil
      void $ runM env (HL.setVisibilityTimeout 0 (head firstClaim))
      held <- claimJobsAs env 1 UUID.nil
      map attempts held `shouldBe` [2]
      runM env (HL.nackJob (head held)) `shouldReturn` 1

      stats <- runM env (HL.getQueueStats @payload)
      HL.inFlightJobs stats `shouldBe` 0
      HL.backoffJobs stats `shouldBe` 1

    it "settles two batches over the same parents concurrently without deadlocking" $ \env -> do
      -- A bulk ack and a bulk DLQ move each hold one parent and want the other.
      -- Both take the union of parent locks up front.
      forM_ [1 .. 8 :: Int] $ \round' -> do
        let name side = mkMessage ("lockorder-" <> T.pack (show round') <> "-" <> side)
            tree side =
              JT.rollup
                (defaultJob (name (side <> "-parent")))
                (JT.leaf (defaultJob (name (side <> "-ack"))) :| [JT.leaf (defaultJob (name (side <> "-dlq")))])
        Right _ <- runM env (HL.insertJobTree (tree "a"))
        Right _ <- runM env (HL.insertJobTree (tree "b"))
        children <- claimJobs env 4
        length children `shouldBe` 4
        let pick suffix = find ((== name suffix) . payload) children
            Just ackA = pick "a-ack"
            Just ackB = pick "b-ack"
            Just dlqA = pick "a-dlq"
            Just dlqB = pick "b-dlq"
        -- Opposite child order on each side.
        (acked, moved) <-
          concurrently
            (runM env (HL.ackJobsBatch [ackB, ackA]))
            (runM env (HL.moveToDLQBatch [(dlqA, "boom"), (dlqB, "boom")]))
        length acked `shouldBe` 2
        moved `shouldBe` 2
        -- Both parents woke. The two settles serialized on the parent lock.
        parents <- claimJobs env 2
        length parents `shouldBe` 2
        runM env (HL.ackJobsBatch parents) >>= ((`shouldBe` 2) . length)
        runM env (HL.listJobs @payload 100 0) >>= (`shouldBe` [])

    it "nacks a batch in one statement, leaving a reclaimed job alone" $ \env -> do
      Just jobA <- runM env (HL.insertJob (defaultJob (mkMessage "nack-batch-a")))
      Just jobB <- runM env (HL.insertJob (defaultJob (mkMessage "nack-batch-b")))
      held <- claimJobsAs env 2 UUID.nil
      map attempts held `shouldBe` [1, 1]

      Just heldB <- pure (find ((== primaryKey jobB) . primaryKey) held)
      void $ runM env (HL.setVisibilityTimeout 0 heldB)
      stolen <- claimJobsAs env 1 (UUID.fromWords 4 4 4 4)
      map primaryKey stolen `shouldBe` [primaryKey jobB]

      runM env (HL.nackJobsBatch held) >>= (`shouldBe` [primaryKey jobA])
      Just rereadA <- getJob env (primaryKey jobA)
      Just rereadB <- getJob env (primaryKey jobB)
      attempts rereadA `shouldBe` 0
      attempts rereadB `shouldBe` 2

  describe "refreshGroups" $ do
    it "covers the keys past the ones a pass already walked" $ \env -> do
      let keys = ["grp-cursor-a", "grp-cursor-b", "grp-cursor-c"]
          groupsWindow limit cursor =
            runM env $ do
              schemaName <- getSchema
              MA.executeQuery (GroupsTmpl.groupsWindowSQL schemaName (HL.queueTable @payload @m) limit cursor)
      runM env $ forM_ keys $ \key -> void (HL.insertJob (defaultGroupedJob key (mkMessage key)))

      groupsWindow 2 Nothing >>= (`shouldBe` take 2 keys)
      groupsWindow 2 (Just "grp-cursor-b") >>= (`shouldBe` drop 2 keys)
      groupsWindow 2 (Just "grp-cursor-c") >>= (`shouldBe` [])

    it "reclaims an emptied group the cursor has already passed" $ \env -> do
      let emptied = "grp-reclaim-a"
          laterKeys = ["grp-reclaim-b", "grp-reclaim-c"]
      runM env $ forM_ (emptied : laterKeys) $ \key -> void (HL.insertJob (defaultGroupedJob key (mkMessage key)))
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.ackJob (head claimed))

      -- Acking the only job resets the summary in place.
      groupCount env emptied `shouldReturn` Just 0

      runM env $ do
        schemaName <- getSchema
        void
          ( Ops.refreshGroupsForQueue
              schemaName
              (HL.queueTable @payload @m)
              2
              (Just (Ops.GroupsCursor (Just "grp-reclaim-b") Nothing))
          )
      groupCount env emptied `shouldReturn` Nothing

    it "resumes the emptied scan past the keys a pass already drained" $ \env -> do
      let live = ["grp-drain-m1", "grp-drain-m2"]
          low = "grp-drain-a"
          high = "grp-drain-z"
          pass cursor =
            runM env $ do
              schemaName <- getSchema
              Ops.passResume <$> Ops.refreshGroupsForQueue schemaName (HL.queueTable @payload @m) 1 cursor
          emptyOut key = do
            Just job <- runM env (HL.insertJob (defaultGroupedJob key (mkMessage key)))
            deleteRowDirectly env (primaryKey job)
      runM env $ forM_ live $ \key -> void (HL.insertJob (defaultGroupedJob key (mkMessage key)))
      emptyOut low
      emptyOut high
      groupCount env low `shouldReturn` Just 0
      groupCount env high `shouldReturn` Just 0

      -- One emptied key per pass. The first drains the low one and stops there.
      afterFirst <- pass Nothing
      (afterFirst >>= Ops.groupsEmptiedFrom) `shouldBe` Just low
      groupCount env low `shouldReturn` Nothing
      groupCount env high `shouldReturn` Just 0

      -- The low key empties again. The scan resumes above it.
      emptyOut low
      void (pass afterFirst)
      groupCount env high `shouldReturn` Nothing
      groupCount env low `shouldReturn` Just 0

    it "repairs a tail group only once the cursor reaches it" $ \env -> do
      let keys = ["grp-pass-1", "grp-pass-2", "grp-pass-3", "grp-pass-4", "grp-pass-5"]
          tailKey = last keys
          pass cursor =
            runM env $ do
              schemaName <- getSchema
              Ops.passResume <$> Ops.refreshGroupsForQueue schemaName (HL.queueTable @payload @m) 2 cursor
      runM env $ forM_ keys $ \key -> void (HL.insertJob (defaultGroupedJob key (mkMessage key)))
      driftGroupCount env tailKey 99
      groupCount env tailKey `shouldReturn` Just 99

      -- Two keys per pass. The drifted fifth is out of reach until the third.
      afterFirst <- pass Nothing
      (afterFirst >>= Ops.groupsWindowFrom) `shouldBe` Just "grp-pass-2"
      groupCount env tailKey `shouldReturn` Just 99

      afterSecond <- pass afterFirst
      (afterSecond >>= Ops.groupsWindowFrom) `shouldBe` Just "grp-pass-4"
      groupCount env tailKey `shouldReturn` Just 99

      afterThird <- pass afterSecond
      (afterThird >>= Ops.groupsWindowFrom) `shouldBe` Just tailKey
      groupCount env tailKey `shouldReturn` Just 1

      -- The cycle ends on a pass that finds nothing.
      pass afterThird `shouldReturn` Nothing

  describe "setVisibilityTimeout" $ do
    it "extends visibility timeout for retry" $ \env -> do
      let job = setGroupKey (Just "visibility-extend-test") $ defaultJob (mkMessage "Test")

      void $ runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1

      -- Extend visibility timeout and verify it succeeded
      result1 <- runM env (HL.setVisibilityTimeout 120 (head claimed))
      result1 `shouldBe` 1

      -- The job is still not claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- Set a zeroed visibility timeout and verify it succeeded
      result2 <- runM env (HL.setVisibilityTimeout 0 (head claimed))
      result2 `shouldBe` 1

      -- The job is re-claimable now
      claimed' <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed' `shouldBe` 1

    it "supports fractional timeouts" $ \env -> do
      let job = defaultJob (mkMessage "fractional-timeout")
      void $ runM env (HL.insertJob job)

      -- Claim with fractional visibility timeout (0.5 seconds)
      claimed <- runM env (HL.claimNextVisibleJobs 1 0.5) :: IO [JobRead payload]
      length claimed `shouldBe` 1

      -- Extend with fractional timeout
      result <- runM env (HL.setVisibilityTimeout 0.5 (head claimed))
      result `shouldBe` 1

      -- Wait for it to expire
      threadDelay 600_000

      -- Claimable again
      reclaimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length reclaimed `shouldBe` 1

  describe "ackJobsBatch" $ do
    it "removes multiple jobs in a single operation" $ \env -> do
      let jobs = [defaultJob (mkMessage $ "Job" <> T.pack (show index)) | index <- [1 .. 5 :: Int]]

      void $ runM env (HL.insertJobsBatch jobs)
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]

      length claimed `shouldBe` 5

      -- Ack all jobs in batch
      deleted <- runM env (HL.ackJobsBatch claimed)
      length deleted `shouldBe` 5

      -- A second claim gets nothing.
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

    it "allows next jobs in groups to be claimed after batch ack" $ \env -> do
      let batch1 =
            [ setGroupKey (Just "batch-ack-test-1") $ defaultJob (mkMessage "First-A")
            , setGroupKey (Just "batch-ack-test-2") $ defaultJob (mkMessage "First-B")
            ]
          batch2 =
            [ setGroupKey (Just "batch-ack-test-1") $ defaultJob (mkMessage "Second-A")
            , setGroupKey (Just "batch-ack-test-2") $ defaultJob (mkMessage "Second-B")
            ]

      void $ runM env (HL.insertJobsBatch (batch1 <> batch2))

      -- Claim first jobs from each group
      claimed1 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed1 `shouldBe` 2

      -- Ack them in batch
      acked <- runM env (HL.ackJobsBatch claimed1)
      length acked `shouldBe` 2

      -- The second jobs are claimable now
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 2

  describe "setVisibilityTimeoutBatch" $ do
    it "extends visibility timeout for multiple jobs" $ \env -> do
      let jobs = [defaultJob (mkMessage $ "Job" <> T.pack (show index)) | index <- [1 .. 3 :: Int]]

      void $ runM env (HL.insertJobsBatch jobs)
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]

      length claimed `shouldBe` 3

      -- Extend visibility for all jobs in batch
      results <- runM env (HL.setVisibilityTimeoutBatch 120 claimed)
      let successes = [() | VisibilityExtended _ <- results]
      length successes `shouldBe` 3

      -- The jobs are still not claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- Reset visibility to zero for all
      _ <- runM env (HL.setVisibilityTimeoutBatch 0 claimed)

      -- The jobs are re-claimable now
      claimed' <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed' `shouldBe` 3

    it "returns JobGone for manually acked jobs (not an error)" $ \env -> do
      -- A batch handler acks some jobs before the heartbeat fires.
      let jobs = [defaultJob (mkMessage $ "Job" <> T.pack (show index)) | index <- [1 .. 5 :: Int]]

      void $ runM env (HL.insertJobsBatch jobs)
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 5

      -- Simulate handler manually acking 2 jobs mid-processing
      let (toAck, stillProcessing) = splitAt 2 claimed
      forM_ toAck $ \job -> void $ runM env (HL.ackJob job)

      -- Now the heartbeat fires
      results <- runM env (HL.setVisibilityTimeoutBatch 120 claimed)

      -- The 2 acked jobs are JobGone
      let goneJobs = [jobId | JobGone jobId <- results]
      length goneJobs `shouldBe` 2

      -- The 3 still-processing jobs are VisibilityExtended
      let successJobs = [jobId | VisibilityExtended jobId <- results]
      length successJobs `shouldBe` 3

      -- Verify the IDs match
      sort goneJobs `shouldBe` sort (map primaryKey toAck)
      sort successJobs `shouldBe` sort (map primaryKey stillProcessing)

    it "leaves a finalizer a DLQ retry re-suspended out of the beat" $ \env -> do
      -- Window 1. A woken finalizer whose child comes back from the DLQ is
      -- re-suspended under the claim it still holds.
      Right (parent :| [child]) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "SuspendedFinalizer"))
            (JT.leaf (defaultJob (mkMessage "SuspendedFinalizerChild")) :| [])
      assertSuspended env (primaryKey parent)

      -- The child dies to the DLQ, which wakes the finalizer for its round.
      [claimedChild] <- claimJobs env 1
      primaryKey claimedChild `shouldBe` primaryKey child
      runM env (HL.moveToDLQ "boom" claimedChild) `shouldReturn` 1
      assertNotSuspended env (primaryKey parent)

      [claimedParent] <- claimJobs env 1
      primaryKey claimedParent `shouldBe` primaryKey parent
      void $ runM env (HL.setVisibilityTimeout 0 claimedParent)

      [dlqChild] <- dlqAll env
      Just _ <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey dlqChild))
      assertSuspended env (primaryKey parent)

      runM env (HL.setVisibilityTimeoutBatch 120 [claimedParent])
        >>= (`shouldBe` [JobSuspended (primaryKey parent)])

    it "refuses a flagged job to a lapsed claim carrying the same worker id" $ \env -> do
      let owner = UUID.nil
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "same-pool-flag")))
      let jobId = primaryKey inserted
      [stale] <- claimJobsAs env 1 owner
      void $ runM env (HL.setVisibilityTimeout 0 stale)
      [live] <- claimJobsAs env 1 owner
      claimSeq live `shouldBe` claimSeq stale + 1
      runM env (HL.forceCancelJob @payload jobId) `shouldReturn` 1

      runM env (HL.setVisibilityTimeoutBatch 120 [stale])
        >>= (`shouldBe` [JobReclaimed jobId (claimSeq stale) (claimSeq live + 1)])
      runM env (HL.setVisibilityTimeoutBatch 120 [live])
        >>= (`shouldBe` [JobCancelled jobId])

  describe "Handoff windows" $ do
    it "refuses the retry write for a claim that was stolen" $ \env -> do
      -- Window 2.
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "window2")))
      [held] <- claimJobsAs env 1 UUID.nil
      void $ runM env (HL.setVisibilityTimeout 0 held)
      [stolen] <- claimJobsAs env 1 (UUID.fromWords 3 3 3 3)
      primaryKey stolen `shouldBe` primaryKey inserted
      claimSeq stolen `shouldNotBe` claimSeq held

      runM env (HL.updateJobForRetry 60 "boom" held) `shouldReturn` 0
      runM env (HL.updateJobForRetry 60 "boom" stolen) `shouldReturn` 1

    it "leaves a written retry's backoff alone when a tick lands on it" $ \env -> do
      -- Window 2, with the heartbeat as the second actor. The retry releases the
      -- row and keeps its token. The holder tells the two apart.
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "window2-backoff")))
      [held] <- claimJobsAs env 1 UUID.nil
      runM env (HL.updateJobForRetry 3600 "boom" held) `shouldReturn` 1
      backoff <- getJob env (primaryKey inserted)

      runM env (HL.setVisibilityTimeoutBatch 120 [held])
        >>= (`shouldBe` [VisibilityUnchanged (primaryKey inserted)])

      beaten <- getJob env (primaryKey inserted)
      (notVisibleUntil <$> beaten) `shouldBe` (notVisibleUntil <$> backoff)

    it "reports a flagged job and a vanished one in the same tick" $ \env -> do
      -- Window 4.
      Just flagged <- runM env (HL.insertJob (defaultJob (mkMessage "window4-flagged")))
      Just vanished <- runM env (HL.insertJob (defaultJob (mkMessage "window4-gone")))
      claimed <- claimJobsAs env 2 UUID.nil
      length claimed `shouldBe` 2

      runM env (HL.forceCancelJob @payload (primaryKey flagged)) `shouldReturn` 1
      runM env (HL.cancelJob @payload (primaryKey vanished)) `shouldReturn` 1

      results <- runM env (HL.setVisibilityTimeoutBatch 120 claimed)
      results `shouldMatchList` [JobCancelled (primaryKey flagged), JobGone (primaryKey vanished)]

    it "sweeps a flagged job only once its lease lapses" $ \env -> do
      -- Window 5.
      let owner = UUID.nil
          reaper = UUID.fromWords 2 2 2 2
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "window5")))
      let jobId = primaryKey inserted
      claimed <- claimJobsAs env 1 owner
      length claimed `shouldBe` 1
      runM env (HL.forceCancelJob @payload jobId) `shouldReturn` 1

      deleteCancelledAs env reaper [jobId] >>= (`shouldBe` [])

      -- The flag bumped the token. The lapse is written against the row's.
      Just flaggedRow <- getJob env jobId
      void $ runM env (HL.setVisibilityTimeout 0 flaggedRow)
      deleteCancelledAs env reaper [jobId] >>= (`shouldBe` [jobId])
      assertGone env jobId

  describe "Tree locks" $
    it "locks an orphaned job's own subtree when its parent row is gone" $ \env -> do
      Right (root :| [mid, leaf]) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "orphan-root"))
            (JT.rollup (defaultJob (mkMessage "orphan-mid")) (JT.leaf (defaultJob (mkMessage "orphan-leaf")) :| []) :| [])

      lockedFromRoot env [primaryKey leaf] `shouldReturn` 3
      deleteRowDirectly env (primaryKey root)
      lockedFromRoot env [primaryKey mid] `shouldReturn` 2

  describe "Job Deduplication" $ do
    it "No dedup key allows multiple jobs with same payload" $ \env -> do
      let job1 =
            setDedupKey Nothing $ defaultGroupedJob "dedup-always-test" (mkMessage "Same")
          job2 =
            setDedupKey Nothing $ defaultGroupedJob "dedup-always-test" (mkMessage "Same")

      Just inserted1 <- runM env (HL.insertJob job1)
      Just inserted2 <- runM env (HL.insertJob job2)

      -- Both jobs are inserted with different ids
      primaryKey inserted1 `shouldNotBe` primaryKey inserted2
      payload inserted1 `shouldBe` mkMessage "Same"
      payload inserted2 `shouldBe` mkMessage "Same"

    it "IgnoreDuplicate returns Nothing on conflict (grouped and ungrouped)" $ \env -> do
      -- groupKey does not participate in dedup. The same key conflicts whether
      -- the jobs are grouped or ungrouped.
      let job1 =
            setDedupKey (Just (IgnoreDuplicate "unique-key-1")) $ defaultGroupedJob "dedup-ignore-test-1" (mkMessage "First")
          job2 =
            setDedupKey (Just (IgnoreDuplicate "unique-key-1")) $ defaultGroupedJob "dedup-ignore-test-2" (mkMessage "Second")

      Just inserted1 <- runM env (HL.insertJob job1)
      inserted2 <- runM env (HL.insertJob job2)

      -- The second insert returns Nothing.
      inserted2 `shouldBe` Nothing
      payload inserted1 `shouldBe` mkMessage "First"

      -- Same conflict holds for ungrouped jobs sharing a dedup key.
      let ungrouped1 =
            setDedupKey (Just (IgnoreDuplicate "ungrouped-key")) $ defaultJob (mkMessage "Ungrouped1")
          ungrouped2 =
            setDedupKey (Just (IgnoreDuplicate "ungrouped-key")) $ defaultJob (mkMessage "Ungrouped2")

      Just insertedU1 <- runM env (HL.insertJob ungrouped1)
      insertedU2 <- runM env (HL.insertJob ungrouped2)
      insertedU2 `shouldBe` Nothing
      payload insertedU1 `shouldBe` mkMessage "Ungrouped1"
    it "IgnoreDuplicate with different keys creates separate jobs" $ \env -> do
      let job1 =
            setDedupKey (Just (IgnoreDuplicate "key-1")) $ defaultGroupedJob "dedup-diffkey-test-1" (mkMessage "Job1")
          job2 =
            setDedupKey (Just (IgnoreDuplicate "key-2")) $ defaultGroupedJob "dedup-diffkey-test-2" (mkMessage "Job2")

      Just inserted1 <- runM env (HL.insertJob job1)
      Just inserted2 <- runM env (HL.insertJob job2)

      -- Both are inserted
      primaryKey inserted1 `shouldNotBe` primaryKey inserted2
      payload inserted1 `shouldBe` mkMessage "Job1"
      payload inserted2 `shouldBe` mkMessage "Job2"

    it "ReplaceDuplicate replaces existing job completely" $ \env -> do
      let job1 =
            setDedupKey (Just (ReplaceDuplicate "replace-key-1"))
              $ setPriority 10
              $ defaultGroupedJob "dedup-replace-test-1" (mkMessage "Original")
          job2 =
            setDedupKey (Just (ReplaceDuplicate "replace-key-1"))
              $ setPriority 5
              $ defaultGroupedJob "dedup-replace-test-2" (mkMessage "Replacement")

      Just inserted1 <- runM env (HL.insertJob job1)
      Just inserted2 <- runM env (HL.insertJob job2)

      -- Same job id.
      primaryKey inserted1 `shouldBe` primaryKey inserted2

      -- Payload and group carry the new values.
      payload inserted2 `shouldBe` mkMessage "Replacement"
      groupKey inserted2 `shouldBe` Just "dedup-replace-test-2"
      priority inserted2 `shouldBe` 5

      -- Attempts are reset
      attempts inserted2 `shouldBe` 0

    it "ReplaceDuplicate resets job state (attempts, errors)" $ \env -> do
      let job1 =
            setDedupKey (Just (ReplaceDuplicate "reset-key")) $ defaultGroupedJob "dedup-reset-test-1" (mkMessage "First")

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
            setDedupKey (Just (ReplaceDuplicate "reset-key")) $ defaultGroupedJob "dedup-reset-test-2" (mkMessage "Replacement")

      Just inserted2 <- runM env (HL.insertJob job2)

      -- The job is replaced with fresh state
      primaryKey inserted1 `shouldBe` primaryKey inserted2
      attempts inserted2 `shouldBe` 0
      lastError inserted2 `shouldBe` Nothing
      payload inserted2 `shouldBe` mkMessage "Replacement"

    it "ReplaceDuplicate returns Nothing when the existing job is actively claimed" $ \env -> do
      let job1 =
            setDedupKey (Just (ReplaceDuplicate "inflight-test-key")) $
              defaultGroupedJob "dedup-inflight-test-1" (mkMessage "Original")

      Just _inserted1 <- runM env (HL.insertJob job1)

      -- Claim the job (attempts=1, last_error=NULL, not_visible_until > NOW).
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      let claimedJob = head claimed
      attempts claimedJob `shouldBe` 1
      lastError claimedJob `shouldBe` Nothing

      -- A replace while in flight returns Nothing.
      let job2 =
            setDedupKey (Just (ReplaceDuplicate "inflight-test-key")) $
              defaultGroupedJob "dedup-inflight-test-2" (mkMessage "Replacement")

      replaced <- runM env (HL.insertJob job2)
      replaced `shouldBe` Nothing

      -- The original job is preserved unchanged. Make it visible and re-claim it.
      void $ runM env (HL.setVisibilityTimeout 0 claimedJob)
      reclaimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length reclaimed `shouldBe` 1
      primaryKey (head reclaimed) `shouldBe` primaryKey claimedJob
      payload (head reclaimed) `shouldBe` mkMessage "Original"

    it "ReplaceDuplicate returns Nothing for a force-cancel-flagged job" $ \env -> do
      let job1 =
            setDedupKey (Just (ReplaceDuplicate "flagged-test-key")) $
              defaultGroupedJob "dedup-flagged-test-1" (mkMessage "Original")
      Just inserted1 <- runM env (HL.insertJob job1)
      let jobId = primaryKey inserted1
      claimed <- claimJobsAs env 1 UUID.nil
      map primaryKey claimed `shouldBe` [jobId]
      runM env (HL.forceCancelJob @payload jobId) `shouldReturn` 1

      -- Lapse the lease. The refusal below is the flag's.
      Just flaggedRow <- getJob env jobId
      void $ runM env (HL.setVisibilityTimeout 0 flaggedRow)

      let job2 =
            setDedupKey (Just (ReplaceDuplicate "flagged-test-key")) $
              defaultGroupedJob "dedup-flagged-test-2" (mkMessage "Replacement")
      runM env (HL.insertJob job2) >>= (`shouldBe` Nothing)

      Just untouched <- getJob env jobId
      payload untouched `shouldBe` mkMessage "Original"
      claimJobs env 1 >>= (`shouldBe` [])

      deleteCancelledAs env UUID.nil [jobId] >>= (`shouldBe` [jobId])
      Just fresh <- runM env (HL.insertJob job2)
      payload fresh `shouldBe` mkMessage "Replacement"

    it "ReplaceDuplicate succeeds when job is in retry backoff (has last_error)" $ \env -> do
      let job1 =
            setDedupKey (Just (ReplaceDuplicate "retry-backoff-key")) $
              defaultGroupedJob "dedup-backoff-test-1" (mkMessage "Original")

      Just inserted1 <- runM env (HL.insertJob job1)

      -- Claim the job
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      let claimedJob = head claimed

      -- Update for retry (sets last_error and not_visible_until to future)
      rowsUpdated <- runM env (HL.updateJobForRetry 5 "Simulated failure" claimedJob)
      rowsUpdated `shouldBe` 1

      -- The replace succeeds on a job in backoff.
      let job2 =
            setDedupKey (Just (ReplaceDuplicate "retry-backoff-key")) $
              defaultGroupedJob "dedup-backoff-test-2" (mkMessage "Replacement")

      Just replaced <- runM env (HL.insertJob job2)

      -- Same id with fresh state
      primaryKey replaced `shouldBe` primaryKey inserted1
      payload replaced `shouldBe` mkMessage "Replacement"
      attempts replaced `shouldBe` 0
      lastError replaced `shouldBe` Nothing

    it "ReplaceDuplicate returns Nothing when a retried job is running its next attempt" $ \env -> do
      let job1 =
            setDedupKey (Just (ReplaceDuplicate "retry-live-key")) $
              defaultGroupedJob "dedup-retry-live-1" (mkMessage "Original")
      Just _ <- runM env (HL.insertJob job1)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.updateJobForRetry 0 "Simulated failure" (head claimed))
      reclaimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length reclaimed `shouldBe` 1
      lastError (head reclaimed) `shouldBe` Just "Simulated failure"

      let job2 =
            setDedupKey (Just (ReplaceDuplicate "retry-live-key")) $
              defaultGroupedJob "dedup-retry-live-2" (mkMessage "Replacement")
      replaced <- runM env (HL.insertJob job2)
      replaced `shouldBe` Nothing

    it "Dedup key only applies to jobs in queue (not after ack)" $ \env -> do
      let job1 =
            setDedupKey (Just (IgnoreDuplicate "ack-test-key")) $ defaultGroupedJob "dedup-ack-test-1" (mkMessage "First")

      Just inserted1 <- runM env (HL.insertJob job1)

      -- Claim and ack the job (removes from queue)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      void $ runM env (HL.ackJob (head claimed))

      -- Insert another job with same dedup key
      let job2 =
            setDedupKey (Just (IgnoreDuplicate "ack-test-key")) $ defaultGroupedJob "dedup-ack-test-2" (mkMessage "Second")

      Just inserted2 <- runM env (HL.insertJob job2)

      -- A new job is created.
      primaryKey inserted1 `shouldNotBe` primaryKey inserted2
      payload inserted2 `shouldBe` mkMessage "Second"

    it "Mixed dedup strategies work independently" $ \env -> do
      let noDedupJob =
            setDedupKey Nothing $ defaultGroupedJob "dedup-mixed-test-1" (mkMessage "NoDedupe")
          ignoreJob1 =
            setDedupKey (Just (IgnoreDuplicate "ignore-key")) $ defaultGroupedJob "dedup-mixed-test-2" (mkMessage "Ignore1")
          ignoreJob2 =
            setDedupKey (Just (IgnoreDuplicate "ignore-key")) $ defaultGroupedJob "dedup-mixed-test-3" (mkMessage "Ignore2")
          replaceJob1 =
            setDedupKey (Just (ReplaceDuplicate "replace-key")) $ defaultGroupedJob "dedup-mixed-test-4" (mkMessage "Replace1")
          replaceJob2 =
            setDedupKey (Just (ReplaceDuplicate "replace-key")) $ defaultGroupedJob "dedup-mixed-test-5" (mkMessage "Replace2")

      Just noDedup <- runM env (HL.insertJob noDedupJob)
      Just ignore1 <- runM env (HL.insertJob ignoreJob1)
      ignore2 <- runM env (HL.insertJob ignoreJob2) -- Returns Nothing
      Just replace1 <- runM env (HL.insertJob replaceJob1)
      Just replace2 <- runM env (HL.insertJob replaceJob2) -- Replaces replace1

      -- No dedup key creates unique job
      payload noDedup `shouldBe` mkMessage "NoDedupe"

      -- IgnoreDuplicate returns Nothing on conflict
      ignore2 `shouldBe` Nothing
      payload ignore1 `shouldBe` mkMessage "Ignore1"

      -- ReplaceDuplicate replaces
      primaryKey replace1 `shouldBe` primaryKey replace2
      payload replace2 `shouldBe` mkMessage "Replace2"

    it "IgnoreDuplicate and ReplaceDuplicate with same key conflict" $ \env -> do
      let job1 =
            setDedupKey (Just (IgnoreDuplicate "cross-strategy-key")) $ defaultJob (mkMessage "IgnoreFirst")
          job2 =
            setDedupKey (Just (ReplaceDuplicate "cross-strategy-key")) $ defaultJob (mkMessage "ReplaceSecond")

      Just inserted1 <- runM env (HL.insertJob job1)
      Just inserted2 <- runM env (HL.insertJob job2)

      -- ReplaceDuplicate conflicts with the existing key and replaces it
      primaryKey inserted1 `shouldBe` primaryKey inserted2
      payload inserted2 `shouldBe` mkMessage "ReplaceSecond"

  describe "insertJobsBatch Deduplication" $ do
    it "batch insert without dedup keys inserts all jobs" $ \env -> do
      let jobs =
            [ defaultJob (mkMessage $ "Batch" <> T.pack (show index))
            | index <- [1 .. 5 :: Int]
            ]

      inserted <- runM env (HL.insertJobsBatch jobs)
      length inserted `shouldBe` 5

    it "batch insert with IgnoreDuplicate skips conflicts" $ \env -> do
      -- Pre-insert a job with a dedup key
      let existingJob =
            setDedupKey (Just (IgnoreDuplicate "batch-ignore-key")) $ defaultJob (mkMessage "Existing")
      Just _ <- runM env (HL.insertJob existingJob)

      -- Batch insert: one conflicting, two new
      let batchJobs =
            [ setDedupKey (Just (IgnoreDuplicate "batch-ignore-key")) $ defaultJob (mkMessage "Conflict")
            , defaultJob (mkMessage "New1")
            , defaultJob (mkMessage "New2")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- The conflicting job is skipped. Only the 2 new ones are returned
      length inserted `shouldBe` 2
      map payload inserted `shouldMatchList` [mkMessage "New1", mkMessage "New2"]

    it "batch insert with ReplaceDuplicate replaces existing job" $ \env -> do
      -- Pre-insert a job with a dedup key
      let existingJob =
            setPriority 10 $ setDedupKey (Just (ReplaceDuplicate "batch-replace-key")) $ defaultJob (mkMessage "Original")
      Just original <- runM env (HL.insertJob existingJob)

      -- Batch insert with a replacement
      let batchJobs =
            [ setPriority 5 $ setDedupKey (Just (ReplaceDuplicate "batch-replace-key")) $ defaultJob (mkMessage "Replacement")
            , defaultJob (mkMessage "Other")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      length inserted `shouldBe` 2

      -- Find the replaced job (same ID as original)
      let replacedJob = filter (\job -> primaryKey job == primaryKey original) inserted
      length replacedJob `shouldBe` 1
      payload (head replacedJob) `shouldBe` mkMessage "Replacement"
      priority (head replacedJob) `shouldBe` 5

    it "batch insert with mixed strategies" $ \env -> do
      -- Pre-insert two jobs
      let ignoreExisting =
            setDedupKey (Just (IgnoreDuplicate "batch-mixed-ignore")) $ defaultJob (mkMessage "IgnoreExisting")
          replaceExisting =
            setDedupKey (Just (ReplaceDuplicate "batch-mixed-replace")) $ defaultJob (mkMessage "ReplaceExisting")
      Just _ <- runM env (HL.insertJob ignoreExisting)
      Just origReplace <- runM env (HL.insertJob replaceExisting)

      -- Batch: conflict on ignore (skipped), conflict on replace (updated),
      -- plus one job with no dedup key (always inserted)
      let batchJobs =
            [ setDedupKey (Just (IgnoreDuplicate "batch-mixed-ignore")) $ defaultJob (mkMessage "IgnoreConflict")
            , setDedupKey (Just (ReplaceDuplicate "batch-mixed-replace")) $ defaultJob (mkMessage "ReplaceConflict")
            , defaultJob (mkMessage "NoDedupJob")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- IgnoreDuplicate skipped, ReplaceDuplicate replaced, no-dedup inserted
      length inserted `shouldBe` 2
      map payload inserted `shouldMatchList` [mkMessage "ReplaceConflict", mkMessage "NoDedupJob"]

      -- Verify the replacement happened on the same row
      let replacedJob = filter (\job -> primaryKey job == primaryKey origReplace) inserted
      length replacedJob `shouldBe` 1
      payload (head replacedJob) `shouldBe` mkMessage "ReplaceConflict"

    it "batch insert cross-strategy conflict (IgnoreDuplicate then ReplaceDuplicate)" $ \env -> do
      -- Pre-insert with IgnoreDuplicate
      let existingJob =
            setDedupKey (Just (IgnoreDuplicate "batch-cross-key")) $ defaultJob (mkMessage "IgnoreFirst")
      Just original <- runM env (HL.insertJob existingJob)

      -- Batch insert with ReplaceDuplicate on the same key
      let batchJobs =
            [ setDedupKey (Just (ReplaceDuplicate "batch-cross-key")) $ defaultJob (mkMessage "ReplaceSecond")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- ReplaceDuplicate replaces the existing IgnoreDuplicate job
      length inserted `shouldBe` 1
      primaryKey (head inserted) `shouldBe` primaryKey original
      payload (head inserted) `shouldBe` mkMessage "ReplaceSecond"

    it "batch ReplaceDuplicate does not replace in-flight job" $ \env -> do
      -- Insert and claim a job, making it actively owned by a worker.
      let existingJob =
            setDedupKey (Just (ReplaceDuplicate "batch-inflight-key")) $ defaultJob (mkMessage "InFlight")
      Just _ <- runM env (HL.insertJob existingJob)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      -- Job is now in-flight: attempts=1, not_visible_until > NOW, last_error IS NULL

      -- Batch insert with ReplaceDuplicate on the same key
      let batchJobs =
            [ setDedupKey (Just (ReplaceDuplicate "batch-inflight-key")) $ defaultJob (mkMessage "Replacement")
            , defaultJob (mkMessage "Other")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- The in-flight job is kept. Only "Other" is inserted
      length inserted `shouldBe` 1
      payload (head inserted) `shouldBe` mkMessage "Other"

    it "duplicate IgnoreDuplicate keys within batch: first wins" $ \env -> do
      let batchJobs =
            [ setDedupKey (Just (IgnoreDuplicate "ign-ign-key")) $ defaultJob (mkMessage "First")
            , setDedupKey (Just (IgnoreDuplicate "ign-ign-key")) $ defaultJob (mkMessage "Second")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      length inserted `shouldBe` 1
      payload (head inserted) `shouldBe` mkMessage "First"

    it "duplicate ReplaceDuplicate keys within batch: last wins" $ \env -> do
      let batchJobs =
            [ setDedupKey (Just (ReplaceDuplicate "rep-rep-key")) $ defaultJob (mkMessage "First")
            , setDedupKey (Just (ReplaceDuplicate "rep-rep-key")) $ defaultJob (mkMessage "Second")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      length inserted `shouldBe` 1
      payload (head inserted) `shouldBe` mkMessage "Second"

    it "mixed strategies within batch: ReplaceDuplicate takes precedence" $ \env -> do
      let batchJobs =
            [ setDedupKey (Just (IgnoreDuplicate "mixed-key")) $ defaultJob (mkMessage "First")
            , defaultJob (mkMessage "Middle")
            , setDedupKey (Just (ReplaceDuplicate "mixed-key")) $ defaultJob (mkMessage "Last")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      -- First is dropped. Middle and Last are kept.
      length inserted `shouldBe` 2
      map payload inserted `shouldMatchList` [mkMessage "Middle", mkMessage "Last"]

      let dedupJobs = filter (\job -> dedupKey job /= Nothing) inserted
      length dedupJobs `shouldBe` 1
      payload (head dedupJobs) `shouldBe` mkMessage "Last"
      dedupKey (head dedupJobs) `shouldBe` Just (ReplaceDuplicate "mixed-key")

    it "batch ReplaceDuplicate succeeds when job is in retry backoff" $ \env -> do
      -- Insert, claim, then fail the job (putting it in retry backoff)
      let existingJob =
            setDedupKey (Just (ReplaceDuplicate "batch-backoff-key")) $ defaultJob (mkMessage "WillFail")
      Just original <- runM env (HL.insertJob existingJob)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.updateJobForRetry 5 "Simulated failure" (head claimed))

      -- Batch insert with ReplaceDuplicate succeeds on a job in backoff
      let batchJobs =
            [ setDedupKey (Just (ReplaceDuplicate "batch-backoff-key")) $ defaultJob (mkMessage "FreshReplacement")
            ]

      inserted <- runM env (HL.insertJobsBatch batchJobs)
      length inserted `shouldBe` 1
      primaryKey (head inserted) `shouldBe` primaryKey original
      payload (head inserted) `shouldBe` mkMessage "FreshReplacement"
      attempts (head inserted) `shouldBe` 0
      lastError (head inserted) `shouldBe` Nothing

    it "batch ReplaceDuplicate does not replace a retried job running its next attempt" $ \env -> do
      let existingJob =
            setDedupKey (Just (ReplaceDuplicate "batch-retry-live-key")) $ defaultJob (mkMessage "Original")
      Just _ <- runM env (HL.insertJob existingJob)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.updateJobForRetry 0 "Simulated failure" (head claimed))
      reclaimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length reclaimed `shouldBe` 1

      let batchJobs =
            [ setDedupKey (Just (ReplaceDuplicate "batch-retry-live-key")) $ defaultJob (mkMessage "Replacement")
            , defaultJob (mkMessage "Other")
            ]
      inserted <- runM env (HL.insertJobsBatch batchJobs)
      map payload inserted `shouldBe` [mkMessage "Other"]

  describe "updateJobForRetry" $ do
    it "updates job with error message and visibility timeout" $ \env -> do
      let job = setGroupKey (Just "retry-update-test") $ defaultJob (mkMessage "Test")

      void $ runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      length claimed `shouldBe` 1
      let claimedJob = head claimed
      attempts claimedJob `shouldBe` 1
      lastError claimedJob `shouldBe` Nothing

      -- Update for retry with error
      retryResult <- runM env (HL.updateJobForRetry 5 "Something went wrong" claimedJob)
      retryResult `shouldBe` 1

      -- The job is not claimable during its backoff.
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- The error message is persisted.
      Just updated <- runM env (HL.getJobById @payload (primaryKey claimedJob))
      lastError updated `shouldBe` Just "Something went wrong"
      attempts updated `shouldBe` 1 -- attempts unchanged by updateJobForRetry
    it "clears claimed_by on retry" $ \env -> do
      void $ runM env (HL.insertJob (defaultJob (mkMessage "retry-clears-claimed-by")))
      claimed <- runM env (HL.claimNextVisibleJobsAs 1 60 UUID.nil) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      let claimedJob = head claimed
      claimedBy claimedJob `shouldBe` Just UUID.nil

      void $ runM env (HL.updateJobForRetry 5 "boom" claimedJob)

      Just updated <- runM env (HL.getJobById @payload (primaryKey claimedJob))
      claimedBy updated `shouldBe` Nothing
    it "does not retry a force-cancel-flagged job" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "cancel-then-retry")))
      claimed <- runM env (HL.claimNextVisibleJobsAs 1 60 UUID.nil) :: IO [JobRead payload]
      length claimed `shouldBe` 1

      flagged <- runM env (HL.forceCancelJob @payload (primaryKey inserted))
      flagged `shouldBe` 1

      retried <- runM env (HL.updateJobForRetry 5 "boom" (head claimed))
      retried `shouldBe` 0

      getJob env (primaryKey inserted) >>= (`shouldSatisfy` isJust)
  describe "Dead Letter Queue Operations" $ do
    it "moveToDLQ moves failed job to DLQ and removes from main queue" $ \env -> do
      let job = setGroupKey (Just "dlq-move-test") $ defaultJob (mkMessage "Failed")

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

      -- The job is out of the main queue
      allJobs <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length allJobs `shouldBe` 0

      -- The job is in the DLQ with the final error message
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1
      let dlqJobSnapshot = DLQ.jobSnapshot (head dlqJobs)
      payload dlqJobSnapshot `shouldBe` mkMessage "Failed"
      lastError dlqJobSnapshot `shouldBe` Just "Final failure"
      attempts dlqJobSnapshot `shouldBe` 2

    it "retryFromDLQ moves job back to main queue with attempts reset" $ \env -> do
      let job = setGroupKey (Just "dlq-retry-test") $ defaultJob (mkMessage "Retry")

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

      -- Claimable from the main queue
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1
      payload (head claimed2) `shouldBe` mkMessage "Retry"

      -- Removed from the DLQ
      dlqJobs2 <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs2 `shouldBe` 0

    it "retryFromDLQ advances the claim token, so the pre-DLQ claim cannot ack it" $ \env -> do
      Just _inserted <- runM env (HL.insertJob (defaultJob (mkMessage "dlq-retry-token")))
      claimed <- claimJobs env 1
      claimSeq (head claimed) `shouldBe` 1
      void $ runM env (HL.moveToDLQ "Failed" (head claimed))

      dlqJobs <- dlqAll env
      Just retried <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      claimSeq retried `shouldBe` 2

      runM env (HL.ackJob (head claimed)) `shouldReturn` 0
      getJob env (primaryKey retried) >>= (`shouldSatisfy` isJust)

    it "an exhausted-sweep move refuses a job whose nack restored the attempt" $ \env -> do
      Just inserted <- runM env (HL.insertJob $ setMaxAttempts (Just 1) $ defaultJob (mkMessage "sweep-nack"))
      claimed <- claimJobsAs env 1 UUID.nil
      map attempts claimed `shouldBe` [1]
      runM env (HL.nackJob (head claimed)) `shouldReturn` 1

      -- Make the job visible. Only the attempt budget can refuse the move.
      runM env (HL.promoteJob @payload (primaryKey inserted)) `shouldReturn` 1

      -- The sweep's snapshot, taken while the job was still out of attempts.
      moved <- runM env $ do
        schemaName <- getSchema
        Ops.moveToDLQFields
          Ops.TakeLocks
          Tmpl.MoveIfExhausted
          schemaName
          (HL.queueTable @payload @m)
          "max attempts exceeded (reaper sweep)"
          (primaryKey inserted)
          (claimSeq (head claimed))
          Nothing
          False
      moved `shouldBe` 0
      dlqAll env >>= (`shouldBe` [])
      getJob env (primaryKey inserted) >>= (`shouldSatisfy` isJust)

    it "retryFromDLQ returns Nothing for non-existent DLQ job" $ \env -> do
      -- Fabricate a DLQ job with a bogus ID
      let job = setGroupKey (Just "dlq-phantom") $ defaultJob (mkMessage "Phantom")
      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.moveToDLQ "err" (head claimed))
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      -- Delete it first, then retry the stale reference
      _ <- runM env (HL.deleteDLQJob @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      result <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      result `shouldBe` Nothing

    it "deleteDLQJob returns 0 for non-existent DLQ job" $ \env -> do
      let job = setGroupKey (Just "dlq-ghost") $ defaultJob (mkMessage "Ghost")
      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.moveToDLQ "err" (head claimed))
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      _ <- runM env (HL.deleteDLQJob @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      -- A second delete returns 0
      deletedAgain <- runM env (HL.deleteDLQJob @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      deletedAgain `shouldBe` 0

    it "deleteDLQJob permanently removes job from DLQ" $ \env -> do
      let job = setGroupKey (Just "dlq-delete-test") $ defaultJob (mkMessage "Delete")

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
      deleted <- runM env (HL.deleteDLQJob @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      deleted `shouldBe` 1

      -- Gone
      dlqJobs2 <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs2 `shouldBe` 0

    it "listDLQJobs supports pagination" $ \env -> do
      -- Insert and move 5 jobs to DLQ
      let jobs =
            [ setGroupKey (Just ("dlq-pagination-test-" <> T.pack (show index))) $
                defaultJob (mkMessage ("Job" <> T.pack (show index)))
            | index <- [1 .. 5 :: Int]
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

      -- All pages contain distinct jobs
      let allDlqIds = map DLQ.dlqPrimaryKey (dlqJobs1 ++ dlqJobs2 ++ dlqJobs3)
      length allDlqIds `shouldBe` length (nub allDlqIds)

    it "moveToDLQ returns 0 when job already claimed by another worker" $ \env -> do
      let job = setGroupKey (Just "dlq-race-move-test") $ defaultJob (mkMessage "Race")

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
      let job = setGroupKey (Just "dlq-race-retry-test") $ defaultJob (mkMessage "Race")

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
      let job = setGroupKey (Just "dlq-race-ack-test") $ defaultJob (mkMessage "Race")

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

    it "maxAttempts=2 retries once then moves to DLQ on the second attempt" $ \env -> do
      let job =
            setMaxAttempts (Just 2) $ setGroupKey (Just "max-attempts-2-test") $ defaultJob (mkMessage "MaxAtt2")
      Just _inserted <- runM env (HL.insertJob job)

      -- First attempt. The claim brings attempts to 1, below maxAttempts.
      claimed1 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed1 `shouldBe` 1
      let attempt1 = head claimed1
      attempts attempt1 `shouldBe` 1
      -- Zero backoff makes the retry re-claimable at once.
      void $ runM env (HL.updateJobForRetry 0 "fail 1" attempt1)

      -- Second attempt. The claim brings attempts to 2, which reaches maxAttempts.
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1
      let attempt2 = head claimed2
      attempts attempt2 `shouldBe` 2

      -- This attempt moves the job to the DLQ.
      moved <- runM env (HL.moveToDLQ "fail 2 (exhausted)" attempt2)
      moved `shouldBe` 1

      -- The job is gone from the main queue and sits in the DLQ at attempts=2.
      remaining <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length remaining `shouldBe` 0
      dlqJobs <- dlqAll env
      let dlq = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "MaxAtt2") dlqJobs
      attempts (DLQ.jobSnapshot dlq) `shouldBe` 2
      lastError (DLQ.jobSnapshot dlq) `shouldBe` Just "fail 2 (exhausted)"

    it "retryFromDLQ drops dedup_key so the same key no longer dedups" $ \env -> do
      -- A DLQ'd job carrying an IgnoreDuplicate key.
      let job =
            setDedupKey (Just (IgnoreDuplicate "retry-drop-key")) $ defaultJob (mkMessage "RetryDropKey")
      Just _inserted <- runM env (HL.insertJob job)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.moveToDLQ "boom" (head claimed))

      dlqJobs <- dlqAll env
      let dlq = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "RetryDropKey") dlqJobs

      -- Retry restores the job with attempts reset and no dedup_key.
      Just retried <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey dlq))
      dedupKey retried `shouldBe` Nothing

      -- A fresh insert with the original key is not deduped.
      let again =
            setDedupKey (Just (IgnoreDuplicate "retry-drop-key")) $ defaultJob (mkMessage "RetryDropKeyAgain")
      Just insertedAgain <- runM env (HL.insertJob again)
      primaryKey insertedAgain `shouldNotBe` primaryKey retried
      payload insertedAgain `shouldBe` mkMessage "RetryDropKeyAgain"

  describe "Batched Claims (claimNextVisibleJobsBatched)" $ do
    let claimBatched env batchSize limit = runM env (HL.claimNextVisibleJobsBatched batchSize limit 60)
        claimBatchedFlat env batchSize limit = concatMap NE.toList <$> claimBatched env batchSize limit

    it "claims multiple jobs from the same group up to batch size" $ \env -> do
      -- Insert 5 jobs in the same group
      forM_ [1 .. 5 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-size-test" (mkMessage (T.pack $ "Job" <> show index))))

      -- Claim with batch size 3, limit 10
      batches <- claimBatched env 3 10 :: IO [NonEmpty (JobRead payload)]

      -- Exactly 1 batch of 3 jobs from batch-size-test
      length batches `shouldBe` 1
      let batch = NE.toList (head batches)
      length batch `shouldBe` 3
      forM_ batch $ \job -> groupKey job `shouldBe` Just "batch-size-test"

    it "respects batch size limit per group" $ \env -> do
      -- Insert 10 jobs in group batch-limit-test-1 and 10 in group batch-limit-test-2
      forM_ [1 .. 10 :: Int] $ \index -> do
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-limit-test-1" (mkMessage (T.pack $ "G1-" <> show index))))
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-limit-test-2" (mkMessage (T.pack $ "G2-" <> show index))))

      -- Claim with batch size 3, limit 100
      batches <- claimBatched env 3 100 :: IO [NonEmpty (JobRead payload)]

      -- 2 batches of 3 jobs each, one per group
      length batches `shouldBe` 2
      forM_ batches $ \batch -> NE.length batch `shouldBe` 3
      let batchGroups = map (\batch -> groupKey (NE.head batch)) batches
      batchGroups `shouldMatchList` [Just "batch-limit-test-1", Just "batch-limit-test-2"]

    it "respects overall limit across groups" $ \env -> do
      -- Insert 10 jobs each in 5 different groups
      forM_ [1 .. 5 :: Int] $ \groupIndex ->
        forM_ [1 .. 10 :: Int] $ \index ->
          void $
            runM
              env
              ( HL.insertJob
                  (defaultGroupedJob (T.pack $ "batch-overall-test-" <> show groupIndex) (mkMessage (T.pack $ "Job" <> show index)))
              )

      -- Claim with batch size 10, limit 3 batches. The SQL selects 3 groups, up
      -- to 10 jobs each.
      batches <- claimBatched env 10 3 :: IO [NonEmpty (JobRead payload)]

      -- Exactly 3 batches, one per group
      length batches `shouldBe` 3
      -- Every batch is grouped
      forM_ batches $ \batch -> groupKey (NE.head batch) `shouldSatisfy` isJust

    it "ungrouped and grouped jobs compete fairly for batch slots" $ \env -> do
      -- Insert ungrouped first (low IDs), then grouped (high IDs)
      forM_ [1 .. 3 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "U" <> show index))))
      forM_ [1 .. 2 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultGroupedJob "fair-batch-a" (mkMessage (T.pack $ "Ga" <> show index))))
      forM_ [1 .. 2 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultGroupedJob "fair-batch-b" (mkMessage (T.pack $ "Gb" <> show index))))

      -- Claim with batchSize=2, limit=3 (3 batch slots)
      -- Slot allocation by FIFO (min_id):
      --   slot 1: ungrouped batch 1 (U1+U2, min_id=1)
      --   slot 2: ungrouped batch 2 (U3, min_id=3)
      --   slot 3: group "a" (Ga1+Ga2, min_id=4)
      -- Group "b" (min_id=6) gets no slot.
      batches <- claimBatched env 2 3 :: IO [NonEmpty (JobRead payload)]

      -- 3 batch slots: 2 ungrouped batches + 1 group batch
      length batches `shouldBe` 3
      let ungroupedBatches = filter (\batch -> groupKey (NE.head batch) == Nothing) batches
      let groupedBatches = filter (\batch -> groupKey (NE.head batch) /= Nothing) batches
      length ungroupedBatches `shouldBe` 2
      length groupedBatches `shouldBe` 1
      -- Ungrouped batches: sizes 2 and 1 (U1+U2, U3)
      sort (map NE.length ungroupedBatches) `shouldBe` [1, 2]
      -- Grouped batch: group "a" with 2 jobs
      NE.length (head groupedBatches) `shouldBe` 2
      groupKey (NE.head (head groupedBatches)) `shouldBe` Just "fair-batch-a"

    it "ranks an ungrouped batch by its head row" $ \env -> do
      -- The oldest ungrouped row carries the worst priority and is not the batch
      -- head. The batch ranks on the head's (priority, id) pair.
      void $ runM env (HL.insertJob (setPriority 5 $ defaultJob (mkMessage "SlotRankTail")))
      void $ runM env (HL.insertJob (setPriority 0 $ defaultGroupedJob "slot-head-rank" (mkMessage "SlotRankGroup")))
      void $ runM env (HL.insertJob (setPriority 0 $ defaultJob (mkMessage "SlotRankHead")))

      -- One slot. The group head has a lower id than the ungrouped head at the
      -- same priority and takes it.
      batches <- claimBatched env 2 1 :: IO [NonEmpty (JobRead payload)]
      length batches `shouldBe` 1
      groupKey (NE.head (head batches)) `shouldBe` Just "slot-head-rank"

    it "respects per-group ordering within groups" $ \env -> do
      -- Insert jobs in batch-hol-test
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-hol-test" (mkMessage "First")))
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-hol-test" (mkMessage "Second")))
      void $ runM env (HL.insertJob (defaultGroupedJob "batch-hol-test" (mkMessage "Third")))

      -- Claim batch of 2
      claimed1 <- claimBatchedFlat env 2 10 :: IO [JobRead payload]
      length claimed1 `shouldBe` 2
      map payload claimed1 `shouldMatchList` [mkMessage "First", mkMessage "Second"]

      -- The third job is not claimable until the first two are acked.
      claimed2 <- claimBatchedFlat env 2 10 :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- Ack the first two jobs
      forM_ claimed1 $ \job -> void $ runM env (HL.ackJob job)

      -- The third job is claimable now
      claimed3 <- claimBatchedFlat env 2 10 :: IO [JobRead payload]
      length claimed3 `shouldBe` 1
      payload (head claimed3) `shouldBe` mkMessage "Third"

    it "respects priority within batches" $ \env -> do
      -- Insert jobs with different priorities in same group
      void $ runM env (HL.insertJob (setPriority 10 $ defaultGroupedJob "batch-priority-test" (mkMessage "Low")))
      void $ runM env (HL.insertJob (setPriority 0 $ defaultGroupedJob "batch-priority-test" (mkMessage "High")))
      void $ runM env (HL.insertJob (setPriority 5 $ defaultGroupedJob "batch-priority-test" (mkMessage "Med")))

      -- Claim batch of 3
      batches <- claimBatched env 3 10 :: IO [NonEmpty (JobRead payload)]

      -- Single batch containing all 3 priority levels
      length batches `shouldBe` 1
      let batch = NE.toList (head batches)
      length batch `shouldBe` 3
      forM_ batch $ \job -> groupKey job `shouldBe` Just "batch-priority-test"
      map priority batch `shouldBe` [0, 5, 10]

    it "increments attempts for all jobs in batch" $ \env -> do
      -- Insert 3 jobs in same group
      forM_ [1 .. 3 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-attempts-test" (mkMessage (T.pack $ "Job" <> show index))))

      -- Claim batch
      claimed <- claimBatchedFlat env 3 10 :: IO [JobRead payload]
      length claimed `shouldBe` 3

      -- Every job has attempts = 1
      forM_ claimed $ \job -> attempts job `shouldBe` 1

    it "blocks group while batch is in-flight" $ \env -> do
      -- Insert 10 jobs in same group
      forM_ [1 .. 10 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultGroupedJob "batch-block-test" (mkMessage (T.pack $ "Job" <> show index))))

      -- Claim first batch of 5
      firstBatch <- claimBatchedFlat env 5 10 :: IO [JobRead payload]
      length firstBatch `shouldBe` 5

      -- A claim on the same group while the first batch is in flight gets 0 jobs.
      secondClaim <- claimBatchedFlat env 5 10 :: IO [JobRead payload]
      length secondClaim `shouldBe` 0

      -- Ack the first batch
      forM_ firstBatch $ \job -> void $ runM env (HL.ackJob job)

      -- The remaining 5 jobs are claimable now
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
      let ungroupedBatches = filter (\batch -> groupKey (NE.head batch) == Nothing) batches
      let groupedBatches = filter (\batch -> groupKey (NE.head batch) /= Nothing) batches
      length ungroupedBatches `shouldBe` 1
      NE.length (head ungroupedBatches) `shouldBe` 2
      length groupedBatches `shouldBe` 2
      forM_ groupedBatches $ \batch -> NE.length batch `shouldBe` 2
      let batchGroups = sort $ map (\batch -> groupKey (NE.head batch)) groupedBatches
      batchGroups `shouldBe` [Just "batch-mixed-test-1", Just "batch-mixed-test-2"]

    it "claims full group batch even when members are separated by ungrouped jobs" $ \env -> do
      -- Insert grouped and ungrouped interleaved. Group members have
      -- non-consecutive ids.
      void $ runM env (HL.insertJob (defaultGroupedJob "spaced-group" (mkMessage "G1")))
      forM_ [1 .. 3 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "U" <> show index))))
      void $ runM env (HL.insertJob (defaultGroupedJob "spaced-group" (mkMessage "G2")))
      forM_ [4 .. 6 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "U" <> show index))))
      void $ runM env (HL.insertJob (defaultGroupedJob "spaced-group" (mkMessage "G3")))

      -- Claim with batch size 3. All 3 group members come out together.
      batches <- claimBatched env 3 10 :: IO [NonEmpty (JobRead payload)]

      -- 1 group batch (G1+G2+G3) + 2 ungrouped batches (U1-U3, U4-U6) = 3 batches
      length batches `shouldBe` 3
      -- Verify grouped jobs form a single batch of 3
      let groupBatches = filter (\batch -> groupKey (NE.head batch) == Just "spaced-group") batches
      length groupBatches `shouldBe` 1
      NE.length (head groupBatches) `shouldBe` 3
      map payload (NE.toList (head groupBatches)) `shouldMatchList` [mkMessage "G1", mkMessage "G2", mkMessage "G3"]
      -- Ungrouped jobs form 2 batches of 3
      let ungroupedBatches = filter (\batch -> groupKey (NE.head batch) == Nothing) batches
      length ungroupedBatches `shouldBe` 2
      forM_ ungroupedBatches $ \batch -> NE.length batch `shouldBe` 3

    it "batched mode claims children but not suspended finalizers" $ \env -> do
      -- Insert a rollup tree: finalizer + 2 children
      Right (_parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "BatchExclParent"))
            ( JT.leaf (defaultJob (mkMessage "BatchExclChild1"))
                :| [JT.leaf (defaultJob (mkMessage "BatchExclChild2"))]
            )

      -- Insert a regular (non-tree) job
      void $ runM env (HL.insertJob (defaultJob (mkMessage "BatchExclRegular")))

      -- Batched claim gets the children and the regular job. The suspended
      -- finalizer stays.
      batches <- claimBatchedFlat env 10 10 :: IO [JobRead payload]
      let claimedPayloads = map payload batches
      claimedPayloads
        `shouldMatchList` [ mkMessage "BatchExclChild1"
                          , mkMessage "BatchExclChild2"
                          , mkMessage "BatchExclRegular"
                          ]

  describe "Batch Admin Operations" $ do
    it "cancelJobsBatch deletes multiple jobs" $ \env -> do
      -- Insert 5 jobs
      insertedJobs <- forM [1 .. 5 :: Int] $ \index -> do
        Just job <- runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "Cancel" <> show index))))
        pure job

      -- Cancel 3 of them
      let idsToCancel = map primaryKey (take 3 insertedJobs)
      deleted <- runM env (HL.cancelJobsBatch @payload idsToCancel)
      deleted `shouldBe` 3

      -- Verify only 2 remain
      remaining <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length remaining `shouldBe` 2

    it "cancelJobsBatch returns 0 for empty list" $ \env -> do
      deleted <- runM env (HL.cancelJobsBatch @payload [])
      deleted `shouldBe` 0

    it "cancelJobsBatch handles non-existent IDs gracefully" $ \env -> do
      -- Insert 2 jobs
      Just job1 <- runM env (HL.insertJob (defaultJob (mkMessage "Keep1")))
      Just job2 <- runM env (HL.insertJob (defaultJob (mkMessage "Keep2")))

      -- Try to cancel with mix of valid and invalid IDs
      let idsToCancel = [primaryKey job1, 999999, primaryKey job2, 888888]
      deleted <- runM env (HL.cancelJobsBatch @payload idsToCancel)
      deleted `shouldBe` 2

    it "cancels two batches over the same parents concurrently without deadlocking" $ \env -> do
      forM_ [1 .. 8 :: Int] $ \round' -> do
        let name side = mkMessage ("cancel-lockorder-" <> T.pack (show round') <> "-" <> side)
            tree side =
              JT.rollup
                (defaultJob (name (side <> "-parent")))
                (JT.leaf (defaultJob (name (side <> "-x"))) :| [JT.leaf (defaultJob (name (side <> "-y")))])
        Right (_ :| [childAx, childAy]) <- runM env (HL.insertJobTree (tree "a"))
        Right (_ :| [childBx, childBy]) <- runM env (HL.insertJobTree (tree "b"))
        -- Opposite parent order on each side.
        (left, right) <-
          concurrently
            (runM env (HL.cancelJobsBatch @payload [primaryKey childAx, primaryKey childBy]))
            (runM env (HL.cancelJobsBatch @payload [primaryKey childBx, primaryKey childAy]))
        left `shouldBe` 2
        right `shouldBe` 2
        parents <- claimJobs env 2
        length parents `shouldBe` 2
        runM env (HL.ackJobsBatch parents) >>= ((`shouldBe` 2) . length)
        runM env (HL.listJobs @payload 100 0) >>= (`shouldBe` [])

    it "moveToDLQBatch moves multiple jobs with individual error messages" $ \env -> do
      -- Insert and claim 3 jobs
      claimedJobs <- forM [1 .. 3 :: Int] $ \index -> do
        Just _ <-
          runM
            env
            (HL.insertJob (defaultGroupedJob ("dlq-batch-" <> T.pack (show index)) (mkMessage (T.pack $ "DLQ" <> show index))))
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
      moved <- runM env (HL.moveToDLQBatch @payload [])
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

      -- Move both to the DLQ. job1 has stale attempts and fails, job2 succeeds.
      let jobsWithErrors = [(job1, "Error 1"), (job2, "Error 2")]
      moved <- runM env (HL.moveToDLQBatch jobsWithErrors)
      moved `shouldBe` 1

      -- Verify only job2 is in DLQ
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1
      lastError (DLQ.jobSnapshot (head dlqJobs)) `shouldBe` Just "Error 2"

    it "deleteDLQJobsBatch deletes multiple DLQ jobs" $ \env -> do
      -- Insert, claim, and move 5 jobs to DLQ
      forM_ [1 .. 5 :: Int] $ \index -> do
        Just _ <-
          runM
            env
            (HL.insertJob (defaultGroupedJob ("dlq-delete-batch-" <> T.pack (show index)) (mkMessage (T.pack $ "Del" <> show index))))
        jobs <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
        void $ runM env (HL.moveToDLQ "Failed" (head jobs))

      -- Get DLQ job IDs
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 5

      -- Delete 3 of them
      let idsToDelete = map DLQ.dlqPrimaryKey (take 3 dlqJobs)
      deleted <- runM env (HL.deleteDLQJobsBatch @payload idsToDelete)
      deleted `shouldBe` 3

      -- Verify only 2 remain
      remaining <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      length remaining `shouldBe` 2

    it "deleteDLQJobsBatch returns 0 for empty list" $ \env -> do
      deleted <- runM env (HL.deleteDLQJobsBatch @payload [])
      deleted `shouldBe` 0

  describe "Admin Operations" $ do
    it "listJobs returns jobs with pagination" $ \env -> do
      -- Insert 5 jobs
      forM_ [1 .. 5 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "List" <> show index))))

      -- List first 2
      jobs1 <- runM env (HL.listJobs @payload 2 0)
      length jobs1 `shouldBe` 2

      -- List next 2
      jobs2 <- runM env (HL.listJobs @payload 2 2)
      length jobs2 `shouldBe` 2

      -- List last 1
      jobs3 <- runM env (HL.listJobs @payload 2 4)
      length jobs3 `shouldBe` 1

      -- All are distinct
      let allIds = map primaryKey (jobs1 ++ jobs2 ++ jobs3)
      length allIds `shouldBe` length (nub allIds)

    it "getJobById returns the job when it exists" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "FindMe")))

      found <- runM env (HL.getJobById @payload (primaryKey inserted))
      found `shouldBe` Just inserted

    it "getJobById returns Nothing when job doesn't exist" $ \env -> do
      found <- runM env (HL.getJobById @payload 999999)
      found `shouldBe` Nothing

    it "getJobsByGroup returns jobs filtered by group key" $ \env -> do
      -- Insert jobs in different groups
      forM_ [1 .. 3 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultGroupedJob "group-filter-a" (mkMessage (T.pack $ "A" <> show index))))
      forM_ [1 .. 2 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultGroupedJob "group-filter-b" (mkMessage (T.pack $ "B" <> show index))))

      -- Get only group A
      groupAJobs <- runM env (HL.getJobsByGroup @payload "group-filter-a" 10 0)
      length groupAJobs `shouldBe` 3
      forM_ groupAJobs $ \job -> groupKey job `shouldBe` Just "group-filter-a"

    it "promoteJob makes delayed job immediately visible" $ \env -> do
      -- Insert a job, claim it, and put it in retry backoff
      let delayedJob = defaultJob (mkMessage "Delayed")
      Just inserted <- runM env (HL.insertJob delayedJob)

      -- Claim and update for retry (makes it invisible for 60 seconds)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.updateJobForRetry 60 "Retry later" (head claimed))

      -- The job is not claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

      -- Promote the job
      promoted <- runM env (HL.promoteJob @payload (primaryKey inserted))
      promoted `shouldBe` 1

      -- It is claimable now
      claimed3 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed3 `shouldBe` 1

    it "getQueueStats returns correct statistics" $ \env -> do
      -- Insert 5 jobs
      forM_ [1 .. 5 :: Int] $ \index ->
        void $
          runM
            env
            (HL.insertJob (defaultGroupedJob ("stats-test-" <> T.pack (show index)) (mkMessage (T.pack $ "S" <> show index))))

      -- Check stats before claiming
      stats1 <- runM env (HL.getQueueStats @payload)
      HL.totalJobs stats1 `shouldBe` 5
      HL.readyJobs stats1 `shouldBe` 5
      HL.inFlightJobs stats1 `shouldBe` 0

      -- Claim 2 jobs
      _ <- runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]

      -- Check stats after claiming. Claimed jobs are in flight.
      stats2 <- runM env (HL.getQueueStats @payload)
      HL.totalJobs stats2 `shouldBe` 5
      HL.readyJobs stats2 `shouldBe` 3
      HL.inFlightJobs stats2 `shouldBe` 2
      HL.scheduledJobs stats2 `shouldBe` 0
      HL.suspendedJobs stats2 `shouldBe` 0

  describe "Count Operations" $ do
    it "countJobs returns total job count" $ \env -> do
      -- Insert 4 jobs
      forM_ [1 .. 4 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultJob (mkMessage (T.pack $ "Count" <> show index))))

      count <- runM env (HL.countJobs @payload)
      count `shouldBe` 4

    it "countJobsByGroup returns count for specific group" $ \env -> do
      -- Insert jobs in different groups
      forM_ [1 .. 3 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultGroupedJob "count-group-x" (mkMessage (T.pack $ "X" <> show index))))
      forM_ [1 .. 2 :: Int] $ \index ->
        void $ runM env (HL.insertJob (defaultGroupedJob "count-group-y" (mkMessage (T.pack $ "Y" <> show index))))

      countX <- runM env (HL.countJobsByGroup @payload "count-group-x")
      countX `shouldBe` 3

      countY <- runM env (HL.countJobsByGroup @payload "count-group-y")
      countY `shouldBe` 2

    it "countDLQJobs returns count of DLQ jobs" $ \env -> do
      -- Initially 0 in DLQ
      count0 <- runM env (HL.countDLQJobs @payload)
      count0 `shouldBe` 0

      -- Move 2 jobs to DLQ
      forM_ [1 .. 2 :: Int] $ \index -> do
        Just _ <-
          runM
            env
            (HL.insertJob (defaultGroupedJob ("count-dlq-" <> T.pack (show index)) (mkMessage (T.pack $ "DLQ" <> show index))))
        jobs <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
        void $ runM env (HL.moveToDLQ "Failed" (head jobs))

      -- Now 2 in DLQ
      count2 <- runM env (HL.countDLQJobs @payload)
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

      -- The job is deleted
      found <- runM env (HL.getJobById @payload (primaryKey inserted))
      found `shouldBe` Nothing

    it "pause/resume children" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "PauseParent"))
            (JT.leaf (defaultJob (mkMessage "PauseChild1")) :| [JT.leaf (defaultJob (mkMessage "PauseChild2"))])

      -- Children start unsuspended. Pause them.
      paused <- runM env (HL.pauseChildren @payload (primaryKey parent))
      paused `shouldBe` 2

      -- Children are not claimable
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 0

      -- Resume children
      resumed <- runM env (HL.resumeChildren @payload (primaryKey parent))
      resumed `shouldBe` 2

      -- Children are claimable now
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      let claimedPayloads = map payload claimed2
      claimedPayloads `shouldContain` [mkMessage "PauseChild1"]
      claimedPayloads `shouldContain` [mkMessage "PauseChild2"]

    it "pause/resume children descends through naturally-suspended rollups" $ \env -> do
      -- Tree: Grandparent → Parent (rollup) → [Leaf1, Leaf2]
      -- Parent is naturally suspended. pauseChildren on the grandparent pauses
      -- the leaves and leaves Parent alone. resumeChildren on the grandparent
      -- resumes the leaves only.
      Right (grandparent :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "NestedGP"))
            ( JT.rollup
                (defaultJob (mkMessage "NestedParent"))
                (JT.leaf (defaultJob (mkMessage "NestedLeaf1")) :| [JT.leaf (defaultJob (mkMessage "NestedLeaf2"))])
                :| []
            )

      let parent = head rest

      -- Pause the grandparent's subtree. The leaves are paused.
      paused <- runM env (HL.pauseChildren @payload (primaryKey grandparent))
      paused `shouldBe` 2

      -- Nothing is claimable now.
      noneClaimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length noneClaimed `shouldBe` 0

      -- Parent stays suspended.
      Just parentAfterPause <- runM env (HL.getJobById @payload (primaryKey parent))
      suspended parentAfterPause `shouldBe` True

      -- Resume the grandparent's subtree. The leaves resume and the Parent
      -- rollup stays suspended.
      resumed <- runM env (HL.resumeChildren @payload (primaryKey grandparent))
      resumed `shouldBe` 2

      -- Parent stays suspended.
      Just parentAfterResume <- runM env (HL.getJobById @payload (primaryKey parent))
      suspended parentAfterResume `shouldBe` True

      -- The leaves are claimable.
      claimedLeaves <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      let leafPayloads = map payload claimedLeaves
      leafPayloads `shouldContain` [mkMessage "NestedLeaf1"]
      leafPayloads `shouldContain` [mkMessage "NestedLeaf2"]

    it "moveToDLQ on only child wakes parent" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQParent"))
            (JT.leaf (defaultJob (mkMessage "DLQChild")) :| [])

      -- Claim the child
      claimedChild <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedChild `shouldBe` 1

      -- Move the child to the DLQ. The parent wakes.
      void $ runM env (HL.moveToDLQ "Child failed" (head claimedChild))

      -- The parent is resumed
      Just parentResumed <- runM env (HL.getJobById @payload (primaryKey parent))
      suspended parentResumed `shouldBe` False
      (_, childFailures, _, _) <- runM env (HL.readChildResultsRaw @payload (primaryKey parent))
      Map.keys childFailures `shouldBe` [primaryKey (head claimedChild)]

    it "moveToDLQ snapshots the rollup's own child results before the cascade" $ \env -> do
      Right (parent :| _) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "SnapshotParent"))
            (JT.leaf (defaultJob (mkMessage "SnapshotChild")) :| [])
      [child] <- claimJobs env 1
      void $ runM env (HL.insertResult @payload (primaryKey parent) (primaryKey child) (mkResult "child-done"))
      void $ runM env (HL.ackJob child)

      [claimedParent] <- claimJobs env 1
      primaryKey claimedParent `shouldBe` primaryKey parent
      runM env (HL.moveToDLQ "rollup failed" claimedParent) `shouldReturn` 1

      [dlq] <- dlqAll env
      Just requeued <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey dlq))
      (results, failures, snapshot, _) <- runM env (HL.readChildResultsRaw @payload (primaryKey requeued))
      Map.keys results `shouldBe` []
      Map.keys (HL.mergeRawChildResults results failures snapshot) `shouldBe` [primaryKey child]

    it "multi-level: grandparent wakes when all descendants complete" $ \env -> do
      -- Build: Grandparent → Parent → [Child1, Child2] using nested finalizers
      Right (grandparent :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "Grandparent"))
            ( JT.rollup
                (defaultJob (mkMessage "Parent"))
                (JT.leaf (defaultJob (mkMessage "MLChild1")) :| [JT.leaf (defaultJob (mkMessage "MLChild2"))])
                :| []
            )

      let parent = head rest

      -- Only the children are claimable.
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2

      -- Ack child1
      void $ runM env (HL.ackJob (head claimed))
      pStillExists <- runM env (HL.getJobById @payload (primaryKey parent))
      pStillExists `shouldNotBe` Nothing

      -- Ack child2 (last child) → resumes parent for completion round
      void $ runM env (HL.ackJob (claimed !! 1))

      -- The parent is resumed
      Just parentResumed <- runM env (HL.getJobById @payload (primaryKey parent))
      suspended parentResumed `shouldBe` False

      -- Claim and ack parent (completion round) → resumes grandparent
      claimedP <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedP `shouldBe` 1
      void $ runM env (HL.ackJob (head claimedP))

      pGone <- runM env (HL.getJobById @payload (primaryKey parent))
      pGone `shouldBe` Nothing

      -- The grandparent is resumed
      Just gpResumed <- runM env (HL.getJobById @payload (primaryKey grandparent))
      suspended gpResumed `shouldBe` False

      -- Claim and ack grandparent (completion round)
      claimedGP <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedGP `shouldBe` 1
      void $ runM env (HL.ackJob (head claimedGP))

      gpGone <- runM env (HL.getJobById @payload (primaryKey grandparent))
      gpGone `shouldBe` Nothing

    it "multi-level: partial completion doesn't wake ancestors" $ \env -> do
      -- Build: Grandparent finalizer → [Parent1 finalizer → [C1a], Parent2 finalizer → [C2a]]
      Right (grandparent :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
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

      -- Parent1 is resumed for its completion round. The grandparent still has parent2.
      claimedP1 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedP1 `shouldBe` 1
      payload (head claimedP1) `shouldBe` mkMessage "P1Partial"
      void $ runM env (HL.ackJob (head claimedP1))

      -- Parent1 is gone. The grandparent still waits.
      p1Gone <- runM env (HL.getJobById @payload (primaryKey parent1))
      p1Gone `shouldBe` Nothing
      gpStill <- runM env (HL.getJobById @payload (primaryKey grandparent))
      gpStill `shouldNotBe` Nothing

      -- Ack child2a → resumes parent2
      void $ runM env (HL.ackJob (claimed !! 1))

      -- Claim and ack parent2 completion round
      claimedP2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedP2 `shouldBe` 1
      void $ runM env (HL.ackJob (head claimedP2))

      p2Gone <- runM env (HL.getJobById @payload (primaryKey parent2))
      p2Gone `shouldBe` Nothing

      -- The grandparent is resumed now
      claimedGP <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedGP `shouldBe` 1
      void $ runM env (HL.ackJob (head claimedGP))

      gpGone <- runM env (HL.getJobById @payload (primaryKey grandparent))
      gpGone `shouldBe` Nothing

    it "multi-level: cancel cascade deletes all descendants" $ \env -> do
      Right (grandparent :| _rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CascadeGP"))
            ( JT.rollup
                (defaultJob (mkMessage "CascadeMLParent"))
                (JT.leaf (defaultJob (mkMessage "CascadeMLChild1")) :| [JT.leaf (defaultJob (mkMessage "CascadeMLChild2"))])
                :| []
            )

      -- Cancel cascade from grandparent
      deleted <- runM env (HL.cancelJobCascade @payload (primaryKey grandparent))
      deleted `shouldBe` 4

      -- Nothing remains
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 0

    it "multi-level: DLQ at leaf wakes parent but not grandparent" $ \env -> do
      Right (grandparent :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
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

      -- The parent is resumed
      Just pResumed <- runM env (HL.getJobById @payload (primaryKey parent))
      suspended pResumed `shouldBe` False

      -- The grandparent stays suspended while the parent is in the main queue
      Just gpStill <- runM env (HL.getJobById @payload (primaryKey grandparent))
      suspended gpStill `shouldBe` True

    it "suspendJob/resumeJob on a standalone job" $ \env -> do
      -- Insert an ungrouped job
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "SuspendMe")))
      suspended inserted `shouldBe` False

      -- Suspend it
      suspendedRows <- runM env (HL.suspendJob @payload (primaryKey inserted))
      suspendedRows `shouldBe` 1

      -- Not claimable
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 0

      -- Verify suspended flag
      Just found <- runM env (HL.getJobById @payload (primaryKey inserted))
      suspended found `shouldBe` True

      -- Resume it
      resumedRows <- runM env (HL.resumeJob @payload (primaryKey inserted))
      resumedRows `shouldBe` 1

      -- Now claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 1
      payload (head claimed2) `shouldBe` mkMessage "SuspendMe"

    it "suspendJob on in-flight job is rejected" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "InFlightSuspend")))

      -- Claim the job (makes it in-flight)
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1

      -- Suspend fails with 0 rows.
      suspendedRows <- runM env (HL.suspendJob @payload (primaryKey inserted))
      suspendedRows `shouldBe` 0

    it "suspendJob on already-suspended job returns 0" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "DoubleSuspend")))
      firstSuspend <- runM env (HL.suspendJob @payload (primaryKey inserted))
      firstSuspend `shouldBe` 1
      secondSuspend <- runM env (HL.suspendJob @payload (primaryKey inserted))
      secondSuspend `shouldBe` 0

    it "resumeJob on non-suspended job returns 0" $ \env -> do
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "NotSuspended")))
      resumedRows <- runM env (HL.resumeJob @payload (primaryKey inserted))
      resumedRows `shouldBe` 0

    it "insertJob respects notVisibleUntil (delayed job)" $ \env -> do
      now <- getCurrentTime
      let futureTime = truncateToMicros (addUTCTime 3600 now)
          job = setNotVisibleUntil (Just futureTime) $ defaultJob (mkMessage "Delayed")
      Just inserted <- runM env (HL.insertJob job)
      notVisibleUntil inserted `shouldBe` Just futureTime

      -- The job is not claimable
      claimed <- claimJobs env 10
      length claimed `shouldBe` 0

    it "insertJobsBatch respects notVisibleUntil" $ \env -> do
      now <- getCurrentTime
      let futureTime = addUTCTime 3600 now
          jobs =
            [ setNotVisibleUntil (Just futureTime) $ defaultJob (mkMessage "BatchDelayed1")
            , defaultJob (mkMessage "BatchImmediate")
            ]
      inserted <- runM env (HL.insertJobsBatch jobs)
      length inserted `shouldBe` 2

      -- Only the immediate job is claimable
      claimed <- claimJobs env 10
      length claimed `shouldBe` 1
      payload (head claimed) `shouldBe` mkMessage "BatchImmediate"

    it "retryFromDLQ preserves parent_id and clears DLQ" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQRetryParent"))
            (JT.leaf (defaultJob (mkMessage "DLQRetryChild")) :| [])

      -- Claim and move child to DLQ
      claimedChild <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimedChild `shouldBe` 1
      void $ runM env (HL.moveToDLQ "Child failed" (head claimedChild))

      -- Retry from DLQ
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1
      Just retried <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey (head dlqJobs)))

      -- parent_id is preserved
      parentId retried `shouldBe` Just (primaryKey parent)

      -- The DLQ is empty
      dlqAfter <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      length dlqAfter `shouldBe` 0

    it "cancelJobCascade on suspended parent with paused children" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CascadeSuspParent"))
            (JT.leaf (defaultJob (mkMessage "CascadeSuspChild1")) :| [JT.leaf (defaultJob (mkMessage "CascadeSuspChild2"))])

      -- Pause children (makes them suspended)
      _ <- runM env (HL.pauseChildren @payload (primaryKey parent))

      -- Cancel cascade deletes the parent and all suspended children.
      deleted <- runM env (HL.cancelJobCascade @payload (primaryKey parent))
      deleted `shouldBe` 3

      -- Nothing remains
      remaining <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length remaining `shouldBe` 0

    it "cancelJob on last child wakes suspended parent" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CancelWakeParent"))
            (JT.leaf (defaultJob (mkMessage "CancelWakeChild")) :| [])

      let child = head children

      assertSuspended env (primaryKey parent)

      -- Cancel the child without cascade. The parent resumes.
      deleted <- runM env (HL.cancelJob @payload (primaryKey child))
      deleted `shouldBe` 1

      assertNotSuspended env (primaryKey parent)

    it "cancelJob on parent with children returns 0 (guard)" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CancelGuardParent"))
            (JT.leaf (defaultJob (mkMessage "CancelGuardChild")) :| [])

      -- cancelJob without cascade refuses to delete a parent with children
      deleted <- runM env (HL.cancelJob @payload (primaryKey parent))
      deleted `shouldBe` 0

      -- Parent and child still exist
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

      -- The parent is suspended
      suspended parent `shouldBe` True
      parentId parent `shouldBe` Nothing

      -- The children are not suspended
      length children `shouldBe` 2
      forM_ children $ \child -> suspended child `shouldBe` False
      forM_ children $ \child -> parentId child `shouldBe` Just (primaryKey parent)

      -- Only the children are claimable
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      let claimedPayloads = map payload claimed
      claimedPayloads `shouldNotContain` [mkMessage "FanOutParent"]
      claimedPayloads `shouldContain` [mkMessage "FanOutChild1"]
      claimedPayloads `shouldContain` [mkMessage "FanOutChild2"]

    it "returns mixed nested trees in pre-order" $ \env -> do
      let job label = defaultJob (mkMessage label)
          tree =
            JT.rollup
              (job "OrderRoot")
              ( JT.leaf (job "OrderLeaf1")
                  :| [ JT.rollup (job "OrderNested") (JT.leaf (job "OrderGrandchild") :| [])
                     , JT.leaf (job "OrderLeaf2")
                     ]
              )

      Right inserted <- runM env (HL.insertJobTree tree)
      map payload (NE.toList inserted)
        `shouldBe` map mkMessage ["OrderRoot", "OrderLeaf1", "OrderNested", "OrderGrandchild", "OrderLeaf2"]

    it "rollup: acking all children resumes parent for completion round" $ \env -> do
      let parentJob = defaultJob (mkMessage "FanOutAckParent")
          childJobs =
            defaultJob (mkMessage "FanOutAckChild1")
              NE.:| [defaultJob (mkMessage "FanOutAckChild2")]

      Right (parent :| _children) <- runM env (HL.insertJobTree (JT.rollup parentJob (JT.leaf <$> childJobs)))

      -- Claim and ack child 1
      [child1] <- claimJobs env 1
      void $ runM env (HL.ackJob child1)

      -- Claim and ack child 2. The last ack resumes the parent.
      [child2] <- claimJobs env 1
      void $ runM env (HL.ackJob child2)

      assertNotSuspended env (primaryKey parent)

      -- Completion round: claim and ack parent
      [parentJob'] <- claimJobs env 1
      payload parentJob' `shouldBe` mkMessage "FanOutAckParent"
      void $ runM env (HL.ackJob parentJob')

      assertGone env (primaryKey parent)

    it "rollup: parent and children can share group key" $ \env -> do
      -- Children can share the parent's group key. Suspended jobs are excluded
      -- from claim queries.
      let parentJob = defaultGroupedJob "shared-group" (mkMessage "SharedGKParent")
          childJobs =
            defaultGroupedJob "shared-group" (mkMessage "SharedGKChild1")
              NE.:| [defaultGroupedJob "shared-group" (mkMessage "SharedGKChild2")]

      Right (parent :| children) <- runM env (HL.insertJobTree (JT.rollup parentJob (JT.leaf <$> childJobs)))

      -- The parent is suspended
      suspended parent `shouldBe` True
      groupKey parent `shouldBe` Just "shared-group"

      -- The children are not suspended and share the group key
      forM_ children $ \child -> suspended child `shouldBe` False
      forM_ children $ \child -> groupKey child `shouldBe` Just "shared-group"

      -- A suspended job does not hold the group. The children are claimable.
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      let claimedPayload = payload (head claimed)
      -- One of the children is claimed
      claimedPayload `shouldSatisfy` (`elem` [mkMessage "SharedGKChild1", mkMessage "SharedGKChild2"])

  describe "DLQ Child Counts" $ do
    it "countDLQChildrenBatch returns counts for DLQ'd children" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQCountParent"))
            (JT.leaf (defaultJob (mkMessage "DLQCountChild1")) :| [JT.leaf (defaultJob (mkMessage "DLQCountChild2"))])

      -- Claim and DLQ both children
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2
      void $ runM env (HL.moveToDLQ "fail1" (head claimed))
      void $ runM env (HL.moveToDLQ "fail2" (claimed !! 1))

      -- countDLQChildrenBatch shows 2 DLQ'd children for the parent
      dlqCounts <- runM env (HL.countDLQChildrenBatch @payload [primaryKey parent])
      Map.lookup (primaryKey parent) dlqCounts `shouldBe` Just 2

    it "countDLQChildrenBatch returns empty for non-parents" $ \env -> do
      Just standalone <- runM env (HL.insertJob (defaultJob (mkMessage "DLQCountStandalone")))
      dlqCounts <- runM env (HL.countDLQChildrenBatch @payload [primaryKey standalone])
      Map.lookup (primaryKey standalone) dlqCounts `shouldBe` Nothing

    it "countDLQChildrenBatch returns empty list for empty input" $ \env -> do
      dlqCounts <- runM env (HL.countDLQChildrenBatch @payload [])
      dlqCounts `shouldBe` Map.empty

    it "dlqJobExists returns True for existing DLQ job" $ \env -> do
      Just _job <- runM env (HL.insertJob (defaultJob (mkMessage "DLQExistsJob")))
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.moveToDLQ "test error" (head claimed))
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      exists <- runM env (HL.dlqJobExists @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      exists `shouldBe` True

    it "dlqJobExists returns False for non-existent DLQ job" $ \env -> do
      exists <- runM env (HL.dlqJobExists @payload 99999)
      exists `shouldBe` False

    it "retryFromDLQ refuses when parent no longer exists" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "OrphanRetryParent"))
            (JT.leaf (defaultJob (mkMessage "OrphanRetryChild")) :| [])

      -- Claim and DLQ the child
      claimedC <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      void $ runM env (HL.moveToDLQ "child failed" (head claimedC))

      -- Cancel (delete) the parent
      void $ runM env (HL.cancelJobCascade @payload (primaryKey parent))

      -- Retry returns Nothing.
      dlqJobs <- runM env (HL.listDLQJobs 1 0) :: IO [DLQ.DLQJob payload]
      length dlqJobs `shouldBe` 1
      result <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      result `shouldBe` Nothing

      -- The DLQ job is still there
      exists <- runM env (HL.dlqJobExists @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      exists `shouldBe` True

  describe "Dependency Bug Fixes" $ do
    it "cancelJobsBatch wakes parent when last child is batch-cancelled" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "TreeCancelParent"))
            (JT.leaf (defaultJob (mkMessage "TreeCancelChild1")) :| [JT.leaf (defaultJob (mkMessage "TreeCancelChild2"))])

      -- Batch-cancel both children
      let childIds = map primaryKey children
      deleted <- runM env (HL.cancelJobsBatch @payload childIds)
      deleted `shouldBe` 2

      -- The parent is resumed for its completion round
      Just parentResumed <- runM env (HL.getJobById @payload (primaryKey parent))
      suspended parentResumed `shouldBe` False

    it "cancelJobCascade on mid-level node wakes grandparent" $ \env -> do
      Right (grandparent :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CascadeWakeGP"))
            ( JT.rollup
                (defaultJob (mkMessage "CascadeWakeMid"))
                (JT.leaf (defaultJob (mkMessage "CascadeWakeC1")) :| [JT.leaf (defaultJob (mkMessage "CascadeWakeC2"))])
                :| []
            )

      let parent = head rest

      -- Cancel cascade from the mid-level parent
      deleted <- runM env (HL.cancelJobCascade @payload (primaryKey parent))
      deleted `shouldBe` 3 -- parent + 2 children
      assertNotSuspended env (primaryKey grandparent)

    it "retryFromDLQ then ack wakes parent (end-to-end DLQ recovery)" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQRecoveryParent"))
            (JT.leaf (defaultJob (mkMessage "DLQRecoveryChild")) :| [])

      -- Claim and DLQ the child. The parent wakes.
      [child] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "child failed" child)

      -- Retry from DLQ. The child is re-inserted and the woken rollup parent is
      -- re-suspended.
      dlqJobs <- dlqAll env
      Just retried <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      parentId retried `shouldBe` Just (primaryKey parent)

      -- Only the retried child is claimable. The parent is suspended again.
      Just parentAfterRetry <- runM env (HL.getJobById @payload (primaryKey parent))
      suspended parentAfterRetry `shouldBe` True

      claimed <- claimJobs env 2
      length claimed `shouldBe` 1
      payload (head claimed) `shouldBe` mkMessage "DLQRecoveryChild"

      -- Ack the child. The parent wakes.
      void $ runM env (HL.ackJob (head claimed))
      assertNotSuspended env (primaryKey parent)

      -- Parent is now claimable for its completion round.
      claimedParent <- claimJobs env 1
      length claimedParent `shouldBe` 1
      primaryKey (head claimedParent) `shouldBe` primaryKey parent
      void $ runM env (HL.ackJob (head claimedParent))

    it "retryFromDLQ auto-retries parent from DLQ when retrying child" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "AutoRetryParent"))
            ( JT.leaf (defaultJob (mkMessage "AutoRetryChild1"))
                :| [JT.leaf (defaultJob (mkMessage "AutoRetryChild2"))]
            )

      -- Claim child1, DLQ it
      [child1] <- claimJobs env 1
      payload child1 `shouldBe` mkMessage "AutoRetryChild1"
      void $ runM env (HL.moveToDLQ "child1 failed" child1)

      -- Claim child2 and ack it. The parent wakes.
      [child2] <- claimJobs env 1
      void $ runM env (HL.ackJob child2)

      -- Claim the woken parent and DLQ it. Both the parent and child1 are in the DLQ.
      [parentClaim] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "parent failed" parentClaim)

      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 2

      -- Retry child1 from the DLQ. The parent is retried too.
      let child1Dlq = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "AutoRetryChild1") dlqJobs
      Just retriedChild <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey child1Dlq))
      suspended retriedChild `shouldBe` False

      assertSuspended env (primaryKey parent)

      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Claim and ack the retried child. The parent wakes.
      [retriedClaim] <- claimJobs env 1
      void $ runM env (HL.ackJob retriedClaim)

      assertNotSuspended env (primaryKey parent)

    it "retryFromDLQ auto-retries DLQ'd children when retrying rollup finalizer" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "SuspFinParent"))
            ( JT.leaf (defaultJob (mkMessage "SuspFinChild1"))
                :| [JT.leaf (defaultJob (mkMessage "SuspFinChild2"))]
            )

      -- Claim both children and DLQ both. The parent wakes.
      claimed <- claimJobs env 2
      length claimed `shouldBe` 2
      forM_ claimed $ \job -> void $ runM env (HL.moveToDLQ "child failed" job)

      -- Claim the woken parent and DLQ it. All three are in the DLQ.
      [parentClaim] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "parent failed" parentClaim)

      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Retry the parent from the DLQ. Both children are retried and the parent comes back suspended.
      let parentDlq = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "SuspFinParent") dlqJobs
      Just retriedParent <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey parentDlq))
      suspended retriedParent `shouldBe` True

      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Claim and ack both children. The parent stays suspended until the last child is acked.
      claimedChildren <- claimJobs env 2
      length claimedChildren `shouldBe` 2
      void $ runM env (HL.ackJob (head claimedChildren))
      assertSuspended env (primaryKey parent)

      void $ runM env (HL.ackJob (claimedChildren !! 1))
      assertNotSuspended env (primaryKey parent)

    it "retryFromDLQ auto-retries parent and all siblings when retrying child" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "SibRetryParent"))
            ( JT.leaf (defaultJob (mkMessage "SibRetryChild1"))
                :| [JT.leaf (defaultJob (mkMessage "SibRetryChild2"))]
            )

      -- Claim both children and DLQ both. The parent wakes.
      claimed <- claimJobs env 2
      length claimed `shouldBe` 2
      forM_ claimed $ \job -> void $ runM env (HL.moveToDLQ "child failed" job)

      -- Claim the woken parent and DLQ it. All three are in the DLQ.
      [parentClaim] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "parent failed" parentClaim)

      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Retry child1 alone. The parent and sibling child2 are retried too.
      let child1Dlq = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "SibRetryChild1") dlqJobs
      Just retriedChild1 <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey child1Dlq))
      suspended retriedChild1 `shouldBe` False

      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      assertSuspended env (primaryKey parent)

      -- Claim and ack both children. The parent wakes.
      claimedChildren <- claimJobs env 2
      length claimedChildren `shouldBe` 2
      forM_ claimedChildren $ \job -> void $ runM env (HL.ackJob job)

      assertNotSuspended env (primaryKey parent)

    it "retryFromDLQ does not suspend finalizer without DLQ'd children" $ \env -> do
      Right (_parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "NoSuspFinParent"))
            (JT.leaf (defaultJob (mkMessage "NoSuspFinChild")) :| [])

      -- Claim the child and ack it. The parent wakes.
      [child] <- claimJobs env 1
      void $ runM env (HL.ackJob child)

      -- Claim the parent and DLQ it.
      [parentClaim] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "parent failed" parentClaim)

      -- Retry the parent from the DLQ. It comes back unsuspended.
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 1
      Just retriedParent <- runM env (HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      suspended retriedParent `shouldBe` False

      -- The parent is claimable at once
      [reclaimed] <- claimJobs env 1
      payload reclaimed `shouldBe` mkMessage "NoSuspFinParent"

    it "ackJobsBatch with finalizer and children" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "AckBatchParent"))
            (JT.leaf (defaultJob (mkMessage "AckBatchChild1")) NE.:| [JT.leaf (defaultJob (mkMessage "AckBatchChild2"))])

      claimedChildren <- claimJobs env 10
      length claimedChildren `shouldBe` 2

      batchResult <- runM env (HL.ackJobsBatch claimedChildren)
      length batchResult `shouldBe` 2

      assertNotSuspended env (primaryKey parent)

    it "ackJobsBatch partial ack leaves the parent suspended" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "PartialAckParent"))
            (JT.leaf (defaultJob (mkMessage "PartialChild1")) NE.:| [JT.leaf (defaultJob (mkMessage "PartialChild2"))])

      claimedChildren <- claimJobs env 10
      length claimedChildren `shouldBe` 2

      -- Ack only one of the two children. The parent stays suspended.
      acked <- runM env (HL.ackJobsBatch (take 1 claimedChildren))
      length acked `shouldBe` 1

      assertSuspended env (primaryKey parent)

    it "promoteJob on suspended job returns 0" $ \env -> do
      -- Insert a job, then suspend it
      Just inserted <- runM env (HL.insertJob (defaultJob (mkMessage "PromoteSusp")))
      suspendedRows <- runM env (HL.suspendJob @payload (primaryKey inserted))
      suspendedRows `shouldBe` 1

      -- Promote returns 0 on a suspended job.
      promoted <- runM env (HL.promoteJob @payload (primaryKey inserted))
      promoted `shouldBe` 0

      -- The job stays suspended
      Just found <- runM env (HL.getJobById @payload (primaryKey inserted))
      suspended found `shouldBe` True

    it "promoteJob leaves a suspended scheduled job's delay alone" $ \env -> do
      now <- getCurrentTime
      let futureTime = truncateToMicros (addUTCTime 3600 now)
          job = setNotVisibleUntil (Just futureTime) $ defaultJob (mkMessage "PromoteSuspSched")
      Just inserted <- runM env (HL.insertJob job)
      runM env (HL.suspendJob @payload (primaryKey inserted)) `shouldReturn` 1
      promoted <- runM env (HL.promoteJob @payload (primaryKey inserted))
      promoted `shouldBe` 0
      Just found <- runM env (HL.getJobById @payload (primaryKey inserted))
      notVisibleUntil found `shouldBe` Just futureTime

    it "promoteJob refuses to promote in-flight job" $ \env -> do
      -- Insert and claim a job (claiming sets not_visible_until to a future time)
      Just _inserted <- runM env (HL.insertJob (defaultJob (mkMessage "PromoteInFlight")))
      claimed <- runM env (HL.claimNextVisibleJobs 1 3600) :: IO [JobRead payload]
      length claimed `shouldBe` 1
      claimed `shouldNotBe` []
      let claimedJob = head claimed

      -- The job is in flight. Promote returns 0.
      promoted <- runM env (HL.promoteJob @payload (primaryKey claimedJob))
      promoted `shouldBe` 0

      -- The job is still not claimable
      claimed2 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed2 `shouldBe` 0

    it "promoteJob refuses to promote a retried job back in flight" $ \env -> do
      Just _inserted <- runM env (HL.insertJob (defaultJob (mkMessage "PromoteRetriedInFlight")))
      [attempt1] <- claimJobsAs env 1 UUID.nil
      void $ runM env (HL.updateJobForRetry 0 "fail 1" attempt1)
      [attempt2] <- claimJobsAs env 1 UUID.nil
      attempts attempt2 `shouldBe` 2

      promotedAgain <- runM env (HL.promoteJob @payload (primaryKey attempt2))
      promotedAgain `shouldBe` 0

      claimed3 <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      length claimed3 `shouldBe` 0

    it "cancelJobsBatch partial cancel does not wake parent" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "PartialCancelParent"))
            ( JT.leaf (defaultJob (mkMessage "PartialCancelC1"))
                :| [ JT.leaf (defaultJob (mkMessage "PartialCancelC2"))
                   , JT.leaf (defaultJob (mkMessage "PartialCancelC3"))
                   ]
            )

      length children `shouldBe` 3
      let [child1, child2, child3] = children

      -- Cancel 2 of 3 children. The parent stays suspended.
      deleted <- runM env (HL.cancelJobsBatch @payload [primaryKey child1, primaryKey child2])
      deleted `shouldBe` 2
      assertSuspended env (primaryKey parent)

      -- Cancel the last child. The parent resumes.
      deleted2 <- runM env (HL.cancelJobsBatch @payload [primaryKey child3])
      deleted2 `shouldBe` 1
      assertNotSuspended env (primaryKey parent)

    it "moveToDLQ on last main-queue child wakes parent" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQDelWakeParent"))
            (JT.leaf (defaultJob (mkMessage "DLQDelWakeChild1")) :| [JT.leaf (defaultJob (mkMessage "DLQDelWakeChild2"))])

      claimedChildren <- claimJobs env 2
      length claimedChildren `shouldBe` 2
      void $ runM env (HL.ackJob (head claimedChildren))

      -- Move the second child to the DLQ. The parent wakes.
      void $ runM env (HL.moveToDLQ "child2 failed" (claimedChildren !! 1))
      assertNotSuspended env (primaryKey parent)

    it "delete DLQ'd child when main-queue siblings still exist - parent stays suspended" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQDelNoWakeParent"))
            ( JT.leaf (defaultJob (mkMessage "DLQDelNoWakeC1"))
                :| [ JT.leaf (defaultJob (mkMessage "DLQDelNoWakeC2"))
                   , JT.leaf (defaultJob (mkMessage "DLQDelNoWakeC3"))
                   ]
            )

      [child1] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "child1 failed" child1)

      -- Delete the DLQ'd child. The parent stays suspended while child2 and
      -- child3 are in the main queue.
      dlqJobs <- dlqAll env
      deleted <- runM env (HL.deleteDLQJob @payload (DLQ.dlqPrimaryKey (head dlqJobs)))
      deleted `shouldBe` 1

      assertSuspended env (primaryKey parent)

    it "moveToDLQ on both children wakes parent after last one" $ \env -> do
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQBatchWakeParent"))
            (JT.leaf (defaultJob (mkMessage "DLQBatchWakeC1")) :| [JT.leaf (defaultJob (mkMessage "DLQBatchWakeC2"))])

      claimedC <- claimJobs env 2
      length claimedC `shouldBe` 2

      -- First moveToDLQ. A sibling is still in the main queue and the parent stays suspended.
      void $ runM env (HL.moveToDLQ "c1 failed" (head claimedC))
      assertSuspended env (primaryKey parent)

      -- Second moveToDLQ. No children remain and the parent wakes.
      void $ runM env (HL.moveToDLQ "c2 failed" (claimedC !! 1))
      assertNotSuspended env (primaryKey parent)

    it "cancelJob on non-last child does NOT wake parent" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CancelNonLastParent"))
            (JT.leaf (defaultJob (mkMessage "CancelNonLastC1")) :| [JT.leaf (defaultJob (mkMessage "CancelNonLastC2"))])

      -- Cancel first child only
      deleted <- runM env (HL.cancelJob @payload (primaryKey (head children)))
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

      -- Move the first child to the DLQ. The parent stays suspended.
      void $ runM env (HL.moveToDLQ "child1 failed" (head claimed))
      assertSuspended env (primaryKey parent)

      -- Ack the second child. The parent wakes.
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

      -- Cancel the second child. The parent wakes.
      deleted <- runM env (HL.cancelJob @payload (primaryKey (claimed !! 1)))
      deleted `shouldBe` 1

      assertNotSuspended env (primaryKey parent)

    it "ReplaceDuplicate is blocked for parent with children" $ \env -> do
      -- Insert a parent with a dedup key + children
      Right (parent :| _children) <-
        runM env $
          HL.insertJobTree
            ( JT.rollup
                (setDedupKey (Just (ReplaceDuplicate "parent-dedup-key")) $ defaultJob (mkMessage "DedupParentOrig"))
                (JT.leaf (defaultJob (mkMessage "DedupParentChild")) NE.:| [])
            )

      -- The replace is blocked while child rows exist.
      let replacement = setDedupKey (Just (ReplaceDuplicate "parent-dedup-key")) $ defaultJob (mkMessage "DedupParentReplacement")
      result <- runM env (HL.insertJob replacement)
      result `shouldBe` Nothing

      -- The original parent still exists with its payload
      Just found <- runM env (HL.getJobById @payload (primaryKey parent))
      payload found `shouldBe` mkMessage "DedupParentOrig"

    it "ReplaceDuplicate blocked when child is in DLQ" $ \env -> do
      -- Insert parent + child (rollup: child starts unsuspended)
      Right (_parent :| _children) <-
        runM env $
          HL.insertJobTree
            ( JT.rollup
                (setDedupKey (Just (ReplaceDuplicate "dlq-dedup-key")) $ defaultJob (mkMessage "DedupDLQParentOrig"))
                (JT.leaf (defaultJob (mkMessage "DedupDLQChild")) NE.:| [])
            )

      -- Claim and DLQ the child
      [child] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "child failed" child)

      -- The replace is blocked while a DLQ child row exists.
      let replacement = setDedupKey (Just (ReplaceDuplicate "dlq-dedup-key")) $ defaultJob (mkMessage "DedupDLQParentRepl")
      result <- runM env (HL.insertJob replacement)
      result `shouldBe` Nothing

  describe "insertJobTree edge cases" $ do
    it "insertJobTree dedup conflict on root returns Left" $ \env -> do
      -- Pre-insert a job with a dedup key
      Just _existing <-
        runM
          env
          (HL.insertJob $ setDedupKey (Just (IgnoreDuplicate "tree-dedup-root")) $ defaultJob (mkMessage "TreeDedupExisting"))

      -- Try to insert a tree whose root has the same dedup key
      let tree =
            JT.rollup
              (setDedupKey (Just (IgnoreDuplicate "tree-dedup-root")) $ defaultJob (mkMessage "TreeDedupConflict"))
              (JT.leaf (defaultJob (mkMessage "TreeDedupChild")) NE.:| [])
      result <- runM env (HL.insertJobTree tree)
      case result of
        Left _ -> pure ()
        Right _ -> expectationFailure "Expected Left for dedup conflict on root"

      -- The conflicting tree's children are not in the DB
      allJobs <- runM env (HL.listJobs 100 0) :: IO [JobRead payload]
      let orphans = filter (\job -> payload job == mkMessage "TreeDedupChild") allJobs
      length orphans `shouldBe` 0

  describe "Parent State Aggregation" $ do
    it "insertResult writes single and multiple child results to results table" $ \env -> do
      -- Insert a tree using rollup (sets isRollup = True)
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "AggParent1"))
            ( JT.leaf (defaultJob (mkMessage "AggChild1"))
                :| [JT.leaf (defaultJob (mkMessage "AggChild2"))]
            )
      let [child1, child2] = children

      -- Verify parent is a rollup
      isRollup parent `shouldBe` True

      -- Single child result upsert returns 1 row
      rowsInserted <-
        runM env $
          HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child1) (Aeson.String "child1-done")
      rowsInserted `shouldBe` 1

      -- A second child upserts independently into its own (parent, child) row
      void $ runM env $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child2) (Aeson.Number 99)

      -- Both results are readable from the results table
      results <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.lookup (primaryKey child1) results `shouldBe` Just (Aeson.String "child1-done")
      Map.lookup (primaryKey child2) results `shouldBe` Just (Aeson.Number 99)

      -- isRollup stays True
      Just updatedParent <- runM env $ HL.getJobById @payload (primaryKey parent)
      isRollup updatedParent `shouldBe` True

    it "isRollup is False for regular jobs" $ \env -> do
      Just job <- runM env $ HL.insertJob (defaultJob (mkMessage "EitherNone"))
      isRollup job `shouldBe` False

    it "getDLQChildErrorsByParent returns errors for DLQ'd children" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQErrParent"))
            ( JT.leaf (defaultJob (mkMessage "DLQErrChild1"))
                :| [JT.leaf (defaultJob (mkMessage "DLQErrChild2"))]
            )
      let [child1, child2] = children

      -- Claim and DLQ both children with different error messages
      claimed <- claimJobs env 10
      length claimed `shouldBe` 2
      let claimed1 = head $ filter (\job -> primaryKey job == primaryKey child1) claimed
          claimed2 = head $ filter (\job -> primaryKey job == primaryKey child2) claimed
      void $ runM env $ HL.moveToDLQ "error-from-child-1" claimed1
      void $ runM env $ HL.moveToDLQ "error-from-child-2" claimed2

      -- getDLQChildErrorsByParent returns both errors
      errors <- runM env $ HL.getDLQChildErrorsByParent @payload (primaryKey parent)
      Map.size errors `shouldBe` 2
      Map.lookup (primaryKey child1) errors `shouldBe` Just "error-from-child-1"
      Map.lookup (primaryKey child2) errors `shouldBe` Just "error-from-child-2"

    it "getDLQChildErrorsByParent maps only DLQ'd children, ignoring live siblings" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQErrMixParent"))
            ( JT.leaf (defaultJob (mkMessage "DLQErrMixChild1"))
                :| [JT.leaf (defaultJob (mkMessage "DLQErrMixChild2"))]
            )
      let [child1, child2] = children

      -- Before any failure the map is empty.
      emptyMap <- runM env $ HL.getDLQChildErrorsByParent @payload (primaryKey parent)
      emptyMap `shouldBe` Map.empty

      -- DLQ only child1, leaving child2 live in the main queue.
      claimed <- claimJobs env 10
      let claimed1 = head $ filter (\job -> primaryKey job == primaryKey child1) claimed
      void $ runM env $ HL.moveToDLQ "only-child1-failed" claimed1

      -- The map contains exactly the DLQ'd child.
      errors <- runM env $ HL.getDLQChildErrorsByParent @payload (primaryKey parent)
      Map.size errors `shouldBe` 1
      Map.lookup (primaryKey child1) errors `shouldBe` Just "only-child1-failed"
      Map.lookup (primaryKey child2) errors `shouldBe` Nothing

    it "results table and DLQ errors coexist for mixed outcomes" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "MixedParent"))
            ( JT.leaf (defaultJob (mkMessage "MixedChild1"))
                :| [ JT.leaf (defaultJob (mkMessage "MixedChild2"))
                   , JT.leaf (defaultJob (mkMessage "MixedChild3"))
                   ]
            )
      let [child1, _child2, child3] = children

      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child1) (Aeson.String "ok-1")

      claimed <- claimJobs env 10
      let claimed3 = head $ filter (\job -> primaryKey job == primaryKey child3) claimed
      void $ runM env $ HL.moveToDLQ "child3-failed" claimed3

      results <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.lookup (primaryKey child1) results `shouldBe` Just (Aeson.String "ok-1")

      errors <- runM env $ HL.getDLQChildErrorsByParent @payload (primaryKey parent)
      Map.lookup (primaryKey child3) errors `shouldBe` Just "child3-failed"

    it "results table stores only successful child results" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "MergeMixParent"))
            ( JT.leaf (defaultJob (mkMessage "MergeMixChild1"))
                :| [JT.leaf (defaultJob (mkMessage "MergeMixChild2"))]
            )
      let [child1, _child2] = children

      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child1) (Aeson.toJSON (["hello"] :: [Text]))

      results <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size results `shouldBe` 1
      Map.lookup (primaryKey child1) results `shouldBe` Just (Aeson.toJSON (["hello"] :: [Text]))

    it "countDLQChildren returns count for parent with DLQ'd children" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CountDLQParent"))
            ( JT.leaf (defaultJob (mkMessage "CountDLQChild1"))
                :| [JT.leaf (defaultJob (mkMessage "CountDLQChild2"))]
            )
      length children `shouldBe` 2
      -- Claim and DLQ both children
      claimed <- claimJobs env 10
      forM_ claimed $ \job -> void $ runM env $ HL.moveToDLQ "fail" job
      count <- runM env $ HL.countDLQChildren @payload (primaryKey parent)
      count `shouldBe` 2

    it "countDLQChildren returns 0 for parent with no DLQ'd children" $ \env -> do
      Right (parent :| _) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CountDLQ0Parent"))
            (JT.leaf (defaultJob (mkMessage "CountDLQ0Child")) :| [])
      count <- runM env $ HL.countDLQChildren @payload (primaryKey parent)
      count `shouldBe` 0

    it "double DLQ round-trip preserves snapshot" $ \env -> do
      -- Insert tree, store results, ack children, claim parent
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DblDLQParent"))
            ( JT.leaf (defaultJob (mkMessage "DblDLQChild1"))
                :| [JT.leaf (defaultJob (mkMessage "DblDLQChild2"))]
            )
      let [child1, child2] = children
      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child1) (Aeson.String "r1")
      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child2) (Aeson.String "r2")
      claimed <- claimJobs env 10
      forM_ claimed $ \job -> void $ runM env (HL.ackJob job)
      [parentJob] <- claimJobs env 1

      -- First DLQ round-trip: snapshot results, move to DLQ, retry
      resultMap <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size resultMap `shouldBe` 2
      let merged = Map.map Right resultMap :: Map.Map Int64 (Either Text Aeson.Value)
      void $ runM env $ HL.persistParentState @payload (primaryKey parent) (Aeson.toJSON merged)
      void $ runM env $ HL.moveToDLQ "round-1" parentJob
      dlqJobs1 <- dlqAll env
      let dlq1 = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "DblDLQParent") dlqJobs1
      Just retried1 <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey dlq1)

      -- The snapshot content survived the first round-trip
      isRollup retried1 `shouldBe` True
      snap1 <- runM env $ HL.getParentStateSnapshot @payload (primaryKey retried1)
      snap1 `shouldBe` Just (Aeson.toJSON merged)

      -- Second DLQ round-trip. The results table is empty and the worker skips
      -- persist. The parent_state column already holds the snapshot from
      -- retryFromDLQ. moveToDLQ copies the full row to the DLQ table.
      [parentJob2] <- claimJobs env 1
      resultMap2 <- runM env $ HL.getResultsByParent @payload (primaryKey parentJob2)
      Map.size resultMap2 `shouldBe` 0
      -- parent_state is already populated from retryFromDLQ
      snap2a <- runM env $ HL.getParentStateSnapshot @payload (primaryKey parentJob2)
      snap2a `shouldBe` Just (Aeson.toJSON merged)
      void $ runM env $ HL.moveToDLQ "round-2" parentJob2
      dlqJobs2 <- dlqAll env
      let dlq2 = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "DblDLQParent") dlqJobs2
      Just retried2 <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey dlq2)

      -- The snapshot content survived the second round-trip
      isRollup retried2 `shouldBe` True
      snap2b <- runM env $ HL.getParentStateSnapshot @payload (primaryKey retried2)
      snap2b `shouldBe` Just (Aeson.toJSON merged)

  describe "Nested rollup with <~~ operator" $ do
    it "builds a 3-level tree with correct structure" $ \env -> do
      --  root (rollup - aggregates section results)
      --  ├── section-1 (finalizer - waits for its mappers)
      --  │   ├── mapper-1a  (leaf)
      --  │   └── mapper-1b  (leaf)
      --  ├── section-2 (finalizer)
      --  │   ├── mapper-2a  (leaf)
      --  │   └── mapper-2b  (leaf)
      --  └── mapper-solo    (leaf - direct child of root)
      Right allJobs <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup (defaultJob (mkMessage "root: compile report"))
          $ ( defaultJob (mkMessage "section-1: charts")
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

      -- Root is a rollup. Suspended with isRollup.
      payload root `shouldBe` mkMessage "root: compile report"
      suspended root `shouldBe` True
      isRollup root `shouldBe` True
      parentId root `shouldBe` Nothing

      -- Section finalizers are suspended, parented to root, rollup enabled
      suspended sec1 `shouldBe` True
      parentId sec1 `shouldBe` Just (primaryKey root)
      isRollup sec1 `shouldBe` True

      suspended sec2 `shouldBe` True
      parentId sec2 `shouldBe` Just (primaryKey root)
      isRollup sec2 `shouldBe` True

      -- Leaf mappers are not suspended, parented to their section
      forM_ [m1a, m1b] $ \mapper -> do
        suspended mapper `shouldBe` False
        parentId mapper `shouldBe` Just (primaryKey sec1)

      forM_ [m2a, m2b] $ \mapper -> do
        suspended mapper `shouldBe` False
        parentId mapper `shouldBe` Just (primaryKey sec2)

      -- Solo mapper is not suspended, parented directly to root
      suspended solo `shouldBe` False
      parentId solo `shouldBe` Just (primaryKey root)

      -- Only leaves are claimable (all 5 mappers)
      claimed <- claimJobs env 10
      length claimed `shouldBe` 5
      let claimedPayloads = map payload claimed
          expectedLeaves = map payload [m1a, m1b, m2a, m2b, solo]
      forM_ expectedLeaves $ \expected -> claimedPayloads `shouldContain` [expected]

    it "rollup aggregates child results in results table" $ \env -> do
      -- Rollup: 3 children produce partial word lists stored in results table.
      Right allJobs <-
        runM env
          $ HL.insertJobTree
          $ defaultJob (mkMessage "reducer")
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
              HL.insertResultUnsafe @payload
                (primaryKey reducer)
                childId
                (Aeson.toJSON (result :: [Text]))
      void $ insertRes (primaryKey mapperA) ["sales", "growth"]
      void $ insertRes (primaryKey mapperB) ["revenue"]
      void $ insertRes (primaryKey mapperC) ["forecast", "trend"]

      -- Ack all mappers → wakes the reducer
      forM_ claimed $ \job -> void $ runM env (HL.ackJob job)

      -- Claim the reducer
      [reducerJob] <- claimJobs env 1
      primaryKey reducerJob `shouldBe` primaryKey reducer

      -- Read results table directly
      resultMap <- runM env $ HL.getResultsByParent @payload (primaryKey reducer)
      Map.size resultMap `shouldBe` 3

      -- Decode and merge.
      let decode value = case Aeson.fromJSON value of Aeson.Success parsed -> parsed; _ -> []
          merged = foldMap (decode :: Aeson.Value -> [Text]) (Map.elems resultMap)
      length merged `shouldBe` 5
      merged `shouldMatchList` ["sales", "growth", "revenue", "forecast", "trend"]

    it "nested rollup: section finalizers aggregate then root merges" $ \env -> do
      -- Two-level rollup:
      --   root (rollup) - merges section results
      --   ├── section-1 (rollup) - merges mapper results
      --   │   ├── mapper-1a  → ["sales", "growth"]
      --   │   └── mapper-1b  → ["revenue"]
      --   └── section-2 (rollup) - merges mapper results
      --       ├── mapper-2a  → ["forecast"]
      --       └── mapper-2b  → ["trend", "outlook"]
      Right allJobs <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup (defaultJob (mkMessage "root"))
          $ ( defaultJob (mkMessage "section-1")
                <~~ (defaultJob (mkMessage "mapper-1a") :| [defaultJob (mkMessage "mapper-1b")])
            )
            :| [ defaultJob (mkMessage "section-2")
                   <~~ (defaultJob (mkMessage "mapper-2a") :| [defaultJob (mkMessage "mapper-2b")])
               ]
      -- Pre-order: root, section-1, mapper-1a, mapper-1b, section-2, mapper-2a, mapper-2b
      let [root, sec1, m1a, m1b, sec2, m2a, m2b] = NE.toList allJobs

      let insertRes parentPk childPk result =
            runM env $
              HL.insertResultUnsafe @payload
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
      forM_ claimed $ \job -> void $ runM env (HL.ackJob job)

      -- Step 4: Claim both section finalizers
      sections <- claimJobs env 10
      length sections `shouldBe` 2

      let decode value = case Aeson.fromJSON value of Aeson.Success parsed -> parsed; _ -> ([] :: [Text])

      -- Step 5: Each section reads its children's results, merges, inserts result to root
      forM_ sections $ \secJob -> do
        resultMap <- runM env $ HL.getResultsByParent @payload (primaryKey secJob)
        let merged = foldMap decode (Map.elems resultMap)
        void $ insertRes (primaryKey root) (primaryKey secJob) merged

      -- Step 6: Ack both sections → wakes root
      forM_ sections $ \job -> void $ runM env (HL.ackJob job)

      -- Step 7: Claim the root reducer
      [rootJob] <- claimJobs env 1
      primaryKey rootJob `shouldBe` primaryKey root

      -- Step 8: Read root's results table
      rootResultMap <- runM env $ HL.getResultsByParent @payload (primaryKey root)

      -- Root merges section results. All 6 words are present.
      let finalMerged = foldMap decode (Map.elems rootResultMap)
      length finalMerged `shouldBe` 6
      finalMerged
        `shouldMatchList` ["sales", "growth", "revenue", "forecast", "trend", "outlook"]

  describe "Results Table" $ do
    it "insertResult encodes the queue's declared result type" $ \env -> do
      Right (parent :| [child]) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "TypedResultParent"))
            (JT.leaf (defaultJob (mkMessage "TypedResultChild")) :| [])

      rowsInserted <-
        runM env $
          HL.insertResult @payload (primaryKey parent) (primaryKey child) (mkResult "typed")
      rowsInserted `shouldBe` 1

      results <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.lookup (primaryKey child) results `shouldBe` Just (Aeson.toJSON (mkResult "typed"))

    it "CASCADE cleanup: acking parent deletes results rows" $ \env -> do
      -- Insert a rollup tree with 2 children
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CascParent"))
            ( JT.leaf (defaultJob (mkMessage "CascChild1"))
                :| [JT.leaf (defaultJob (mkMessage "CascChild2"))]
            )
      let [child1, child2] = children

      -- Insert results for both children
      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child1) (Aeson.String "r1")
      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child2) (Aeson.String "r2")

      -- Verify results exist
      results <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size results `shouldBe` 2

      -- Ack both children then the parent
      claimed <- claimJobs env 10
      forM_ claimed $ \job -> void $ runM env (HL.ackJob job)
      -- The parent is claimable now
      [parentJob] <- claimJobs env 1
      primaryKey parentJob `shouldBe` primaryKey parent
      void $ runM env (HL.ackJob parentJob)

      -- The results are deleted by FK cascade
      resultsAfter <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size resultsAfter `shouldBe` 0

    it "CASCADE cleanup: cancelJobCascade deletes results rows" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CascCancelParent"))
            (JT.leaf (defaultJob (mkMessage "CascCancelChild")) :| [])
      let [child] = children

      -- Insert a result
      void $ runM env $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child) (Aeson.String "r")

      -- Verify result exists
      results <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size results `shouldBe` 1

      -- Cascade cancel the parent (deletes parent + children)
      void $ runM env $ HL.cancelJobCascade @payload (primaryKey parent)

      -- The results are gone
      resultsAfter <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size resultsAfter `shouldBe` 0

    it "idempotent upsert: duplicate insertResult overwrites" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "UpsertParent"))
            (JT.leaf (defaultJob (mkMessage "UpsertChild")) :| [])
      let [child] = children

      -- Insert result
      void $ runM env $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child) (Aeson.String "v1")

      -- Insert again with a different value. It overwrites.
      void $ runM env $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child) (Aeson.String "v2")

      -- Verify latest value
      results <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.lookup (primaryKey child) results `shouldBe` Just (Aeson.String "v2")

    it "rollup finalizer: isRollup is set, results table starts empty" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ defaultJob (mkMessage "PlainFinParent")
            <~~ (defaultJob (mkMessage "PlainFinChild") :| [])
      let [child] = children

      isRollup parent `shouldBe` True

      -- Results table starts empty
      results <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size results `shouldBe` 0

      -- Manually inserting works
      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child) (Aeson.String "manual")
      results2 <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size results2 `shouldBe` 1

    it "DLQ preserves accumulated results via parent_state snapshot" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQSnapParent"))
            ( JT.leaf (defaultJob (mkMessage "DLQSnapChild1"))
                :| [JT.leaf (defaultJob (mkMessage "DLQSnapChild2"))]
            )
      let [child1, child2] = children

      -- Insert results for both children
      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child1) (Aeson.String "snap-r1")
      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child2) (Aeson.String "snap-r2")

      -- Ack children to wake the parent
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2
      forM_ claimed $ \job -> void $ runM env (HL.ackJob job)

      -- The parent is claimable now
      [parentJob] <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      primaryKey parentJob `shouldBe` primaryKey parent

      -- Simulate a worker. Persist the results snapshot, then move to the DLQ.
      resultMap <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size resultMap `shouldBe` 2
      let mergedSnap = Map.map Right resultMap :: Map.Map Int64 (Either Text Aeson.Value)
      void $ runM env $ HL.persistParentState @payload (primaryKey parent) (Aeson.toJSON mergedSnap)
      void $ runM env $ HL.moveToDLQ "test-failure" parentJob

      -- The results table rows are gone
      resultsAfter <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      Map.size resultsAfter `shouldBe` 0

      -- The DLQ job is still marked as rollup
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      let dlqJob = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "DLQSnapParent") dlqJobs
      isRollup (DLQ.jobSnapshot dlqJob) `shouldBe` True

    it "DLQ retry preserves parent_state snapshot" $ \env -> do
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "DLQRetryParent"))
            (JT.leaf (defaultJob (mkMessage "DLQRetryChild")) :| [])
      let [child] = children

      -- Insert result, ack child, claim parent
      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey parent) (primaryKey child) (Aeson.String "retry-val")
      claimed <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      forM_ claimed $ \job -> void $ runM env (HL.ackJob job)
      [parentJob] <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]

      -- Persist results and move to DLQ
      resultMap <- runM env $ HL.getResultsByParent @payload (primaryKey parent)
      let mergedSnap = Map.map Right resultMap :: Map.Map Int64 (Either Text Aeson.Value)
      void $ runM env $ HL.persistParentState @payload (primaryKey parent) (Aeson.toJSON mergedSnap)
      void $ runM env $ HL.moveToDLQ "test-err" parentJob

      -- Retry from the DLQ. The snapshot is preserved in the parent_state column.
      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      let dlqJob = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "DLQRetryParent") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey dlqJob)
      isRollup retried `shouldBe` True
      snap <- runM env $ HL.getParentStateSnapshot @payload (primaryKey retried)
      snap `shouldBe` Just (Aeson.toJSON mergedSnap)

  describe "Cascade DLQ for rollup trees" $ do
    it "moveToDLQ on rollup parent cascades children to DLQ" $ \env -> do
      -- Insert rollup tree: parent + 2 children
      Right (parent :| children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CascDLQParent"))
            (JT.leaf (defaultJob (mkMessage "CascDLQChild1")) :| [JT.leaf (defaultJob (mkMessage "CascDLQChild2"))])

      -- Parent is suspended (rollup), children are claimable
      assertSuspended env (primaryKey parent)
      length children `shouldBe` 2

      -- moveToDLQ on the parent before the children finish
      void $ runM env (HL.moveToDLQ "Admin DLQ" parent)

      -- All 3 are in the DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Parent in DLQ with original error
      let parentDLQ = filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "CascDLQParent") dlqJobs
      length parentDLQ `shouldBe` 1
      lastError (DLQ.jobSnapshot (head parentDLQ)) `shouldBe` Just "Admin DLQ"

      -- Children in DLQ with cascade error
      let childDLQs = filter (\candidate -> payload (DLQ.jobSnapshot candidate) /= mkMessage "CascDLQParent") dlqJobs
      length childDLQs `shouldBe` 2
      forM_ childDLQs $ \candidate ->
        lastError (DLQ.jobSnapshot candidate) `shouldBe` Just "Parent moved to DLQ"

      -- The main queue is empty
      remaining <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length remaining `shouldBe` 0

    it "moveToDLQ cascade + retryFromDLQ recovers full tree" $ \env -> do
      -- Insert rollup tree: parent + 2 children
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "CascRetryParent"))
            (JT.leaf (defaultJob (mkMessage "CascRetryChild1")) :| [JT.leaf (defaultJob (mkMessage "CascRetryChild2"))])

      -- moveToDLQ on parent → all 3 in DLQ
      void $ runM env (HL.moveToDLQ "Admin DLQ" parent)
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- retryFromDLQ on parent → entire tree recovered
      let parentDLQ = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "CascRetryParent") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey parentDLQ)
      isRollup retried `shouldBe` True
      suspended retried `shouldBe` True

      -- The DLQ is empty now
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- The children are claimable
      claimed <- runM env (HL.claimNextVisibleJobs 10 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2
      map payload claimed `shouldMatchList` [mkMessage "CascRetryChild1", mkMessage "CascRetryChild2"]

      -- Ack children → parent wakes
      forM_ claimed $ \job -> void $ runM env (HL.ackJob job)
      assertNotSuspended env (primaryKey retried)

      -- The parent is claimable
      [parentClaimed] <- runM env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead payload]
      payload parentClaimed `shouldBe` mkMessage "CascRetryParent"

    it "moveToDLQ cascade handles multi-level nesting" $ \env -> do
      -- Build 3-level tree: grandparent → parent → [child1, child2]
      Right (grandparent :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
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

      -- All 4 are in the DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 4

      -- The main queue is empty
      mainCount <- runM env (HL.countJobs @payload)
      mainCount `shouldBe` 0

    it "moveToDLQ on non-rollup job does not cascade" $ \env -> do
      -- Insert a rollup tree, then moveToDLQ a child (non-rollup)
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "NoCascParent"))
            (JT.leaf (defaultJob (mkMessage "NoCascChild1")) :| [JT.leaf (defaultJob (mkMessage "NoCascChild2"))])

      -- Claim children
      claimed <- runM env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead payload]
      length claimed `shouldBe` 2

      -- moveToDLQ on one child
      let child1 = head claimed
      isRollup child1 `shouldBe` False
      void $ runM env (HL.moveToDLQ "child error" child1)

      -- Only that child is in the DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 1
      payload (DLQ.jobSnapshot (head dlqJobs)) `shouldBe` payload child1

      -- Parent still in main queue (suspended), sibling still in main queue
      assertSuspended env (primaryKey parent)
      mainCount <- runM env (HL.countJobs @payload)
      mainCount `shouldBe` 2 -- parent + remaining child
  describe "moveToDLQBatch cascades for rollup parents" $ do
    it "moveToDLQBatch snapshots a rollup it names alongside its parent" $ \env -> do
      Right tree <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "BatchSnapRoot"))
            ( JT.rollup
                (defaultJob (mkMessage "BatchSnapMid"))
                (JT.leaf (defaultJob (mkMessage "BatchSnapLeaf")) :| [])
                :| []
            )
      let nodeNamed name = find ((== mkMessage name) . payload) (NE.toList tree)
      Just root <- pure (nodeNamed "BatchSnapRoot")
      Just mid <- pure (nodeNamed "BatchSnapMid")
      Just leaf <- pure (nodeNamed "BatchSnapLeaf")

      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey mid) (primaryKey leaf) (Aeson.String "batch-snap")
      resultMap <- runM env $ HL.getResultsByParent @payload (primaryKey mid)
      Map.size resultMap `shouldBe` 1

      -- Naming both puts the child's delete in the same statement as the parent's.
      runM env (HL.moveToDLQBatch [(root, "root err"), (mid, "mid err")]) `shouldReturn` 2

      dlqJobs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob payload]
      Just midDlq <- pure (find ((== mkMessage "BatchSnapMid") . payload . DLQ.jobSnapshot) dlqJobs)
      let expected = Map.map Right resultMap :: Map.Map Int64 (Either Text Aeson.Value)
      parentState (DLQ.jobSnapshot midDlq) `shouldBe` Just (Aeson.toJSON expected)

    it "moveToDLQBatch on rollup parent cascades children to DLQ" $ \env -> do
      -- Insert rollup tree: parent + 2 children (parent is suspended with attempts=0)
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "BatchCascParent"))
            (JT.leaf (defaultJob (mkMessage "BatchCascChild1")) :| [JT.leaf (defaultJob (mkMessage "BatchCascChild2"))])

      -- moveToDLQBatch on the parent
      moved <- runM env (HL.moveToDLQBatch [(parent, "Batch admin DLQ")])
      moved `shouldBe` 1

      -- All 3 are in the DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- The children carry the cascade error message
      let childDLQs = filter (\candidate -> payload (DLQ.jobSnapshot candidate) /= mkMessage "BatchCascParent") dlqJobs
      length childDLQs `shouldBe` 2
      forM_ childDLQs $ \candidate ->
        lastError (DLQ.jobSnapshot candidate) `shouldBe` Just "Parent moved to DLQ"

      -- The main queue is empty
      mainCount <- runM env (HL.countJobs @payload)
      mainCount `shouldBe` 0

  describe "Intermediate rollup snapshots survive cascade DLQ" $ do
    it "3-level tree: mid-level rollup snapshot preserved after cascade" $ \env -> do
      -- Build 3-level tree: grandparent → mid-parent (rollup) → [child1, child2]
      Right (grandparent :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "SnapGrandparent"))
            ( JT.rollup
                (defaultJob (mkMessage "SnapMidParent"))
                (JT.leaf (defaultJob (mkMessage "SnapChild1")) :| [JT.leaf (defaultJob (mkMessage "SnapChild2"))])
                :| []
            )

      let midParent = head rest

      -- Claim and ack one child. The mid-parent gets a partial result.
      claimed <- claimJobs env 2
      length claimed `shouldBe` 2
      let child1 = head claimed
      -- Insert a result for child1 under mid-parent, then ack child1
      void
        $ runM env
        $ HL.insertResultUnsafe @payload (primaryKey midParent) (primaryKey child1) (Aeson.String "child1-result")
      void $ runM env (HL.ackJob child1)

      -- Mid-parent still suspended (one child remains)
      assertSuspended env (primaryKey midParent)

      -- Cascade DLQ on the grandparent. The mid-parent's results are snapshotted first.
      void $ runM env (HL.moveToDLQ "Admin cascade" grandparent)

      -- 3 in the DLQ: grandparent, mid-parent, child2.
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Retry the whole tree from grandparent
      let gpDLQ = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "SnapGrandparent") dlqJobs
      Just retriedGP <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey gpDLQ)

      -- The DLQ is empty. The whole tree is retried.
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- The grandparent is suspended
      assertSuspended env (primaryKey retriedGP)

      -- The mid-parent keeps its snapshot with child1's result
      snap <- runM env $ HL.getParentStateSnapshot @payload (primaryKey midParent)
      let expectedSnap = Aeson.toJSON (Map.singleton (primaryKey child1) (Right (Aeson.String "child1-result") :: Either Text Aeson.Value))
      snap `shouldBe` Just expectedSnap

  describe "N-level retryFromDLQ" $ do
    it "retryFromDLQ from a child retries the entire tree" $ \env -> do
      -- Build 2-level tree: parent → [child1, child2]
      Right (_parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "NLevelRetryParent"))
            (JT.leaf (defaultJob (mkMessage "NLevelRetryChild1")) :| [JT.leaf (defaultJob (mkMessage "NLevelRetryChild2"))])

      -- DLQ the parent (cascades children)
      Just parent' <- runM env $ HL.getJobById @payload (primaryKey _parent)
      void $ runM env (HL.moveToDLQ "Admin" parent')

      -- All in DLQ
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 3

      -- Retry from a child. The whole tree is recovered.
      let child1DLQ = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "NLevelRetryChild1") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey child1DLQ)

      -- The returned job is the requested child
      payload retried `shouldBe` mkMessage "NLevelRetryChild1"

      -- The DLQ is empty
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- The parent is in the main queue, suspended
      assertSuspended env (primaryKey _parent)

      -- Both children are claimable
      claimed <- claimJobs env 10
      length claimed `shouldBe` 2
      map payload claimed `shouldMatchList` [mkMessage "NLevelRetryChild1", mkMessage "NLevelRetryChild2"]

    it "retryFromDLQ from a grandchild retries the entire 3-level tree" $ \env -> do
      -- Build 3-level tree
      Right (grandparent :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
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

      -- Retry from a leaf. The entire 3-level tree is recovered.
      let leafDLQ = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "3LRetryLeaf1") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey leafDLQ)

      -- The returned job is the leaf
      payload retried `shouldBe` mkMessage "3LRetryLeaf1"

      -- The DLQ is empty
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Grandparent suspended (has mid-parent child)
      assertSuspended env (primaryKey grandparent)
      -- Mid-parent suspended (has leaf children)
      assertSuspended env (primaryKey midParent)

      -- The leaf children are claimable
      claimed <- claimJobs env 10
      length claimed `shouldBe` 2
      map payload claimed `shouldMatchList` [mkMessage "3LRetryLeaf1", mkMessage "3LRetryLeaf2"]

      -- Ack both leaves → mid-parent wakes
      forM_ claimed $ \job -> void $ runM env (HL.ackJob job)
      assertNotSuspended env (primaryKey midParent)

      -- Claim and ack mid-parent → grandparent wakes
      [midClaimed] <- claimJobs env 1
      payload midClaimed `shouldBe` mkMessage "3LRetryMid"
      void $ runM env (HL.ackJob midClaimed)
      assertNotSuspended env (primaryKey grandparent)

      -- The grandparent is claimable
      [gpClaimed] <- claimJobs env 1
      payload gpClaimed `shouldBe` mkMessage "3LRetryGP"

    it "single child: moveToDLQ cascades and retries correctly" $ \env -> do
      Right (parent :| _) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "SingleDLQParent"))
            (JT.leaf (defaultJob (mkMessage "SingleDLQChild")) :| [])

      -- Cascade DLQ the parent
      void $ runM env (HL.moveToDLQ "admin" parent)
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 2

      -- Retry from child
      let childDLQ = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "SingleDLQChild") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey childDLQ)
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

      Just retried <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey (head dlqJobs))
      payload retried `shouldBe` mkMessage "StandaloneRetry"
      suspended retried `shouldBe` False
      isRollup retried `shouldBe` False

      -- Claimable
      [reClaimed] <- claimJobs env 1
      payload reClaimed `shouldBe` mkMessage "StandaloneRetry"

    it "retryFromDLQ on child whose parent is still in main queue" $ \env -> do
      -- Tree: parent + 2 children. DLQ one child while the parent is alive.
      -- retryFromDLQ retries that child alone.
      Right (parent :| _children) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "LiveParent"))
            (JT.leaf (defaultJob (mkMessage "LiveChild1")) :| [JT.leaf (defaultJob (mkMessage "LiveChild2"))])

      -- Claim only 1 child and DLQ it (leave the other unclaimed)
      [child1] <- claimJobs env 1
      void $ runM env (HL.moveToDLQ "child fail" child1)

      -- Parent is still in main queue (suspended, other child still exists)
      assertSuspended env (primaryKey parent)

      -- retryFromDLQ on child1. The ancestor walk stops at the live parent.
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 1
      Just retried <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey (head dlqJobs))
      payload retried `shouldBe` payload child1

      -- DLQ empty, child1 is back in main queue
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- Parent still suspended (both children in main queue again)
      assertSuspended env (primaryKey parent)

      -- Claim and ack both children. The parent wakes.
      reClaimed <- claimJobs env 2
      length reClaimed `shouldBe` 2
      forM_ reClaimed $ \job -> void $ runM env (HL.ackJob job)
      assertNotSuspended env (primaryKey parent)

  describe "moveToDLQBatch mixed rollup and non-rollup" $ do
    it "moveToDLQBatch cascades rollup parents but not regular jobs" $ \env -> do
      -- Insert standalone first and claim it before tree exists
      Just _standalone <- runM env (HL.insertJob (defaultGroupedJob "mix-standalone" (mkMessage "MixBatchStandalone")))
      [standaloneClaimed] <- claimJobs env 1
      payload standaloneClaimed `shouldBe` mkMessage "MixBatchStandalone"

      -- Now insert the rollup tree
      Right (parent :| _) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
            (defaultJob (mkMessage "MixBatchParent"))
            (JT.leaf (defaultJob (mkMessage "MixBatchChild1")) :| [JT.leaf (defaultJob (mkMessage "MixBatchChild2"))])

      -- moveToDLQBatch with both the rollup parent and the standalone job
      moved <- runM env (HL.moveToDLQBatch [(parent, "rollup error"), (standaloneClaimed, "standalone error")])
      moved `shouldBe` 2

      -- The DLQ holds the parent, 2 cascaded children, and the standalone job
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 4

      -- Standalone has its own error
      let standaloneDLQ = filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "MixBatchStandalone") dlqJobs
      length standaloneDLQ `shouldBe` 1
      lastError (DLQ.jobSnapshot (head standaloneDLQ)) `shouldBe` Just "standalone error"

      -- Children have cascade error
      let childDLQs =
            filter
              (\candidate -> payload (DLQ.jobSnapshot candidate) `elem` [mkMessage "MixBatchChild1", mkMessage "MixBatchChild2"])
              dlqJobs
      length childDLQs `shouldBe` 2
      forM_ childDLQs $ \candidate ->
        lastError (DLQ.jobSnapshot candidate) `shouldBe` Just "Parent moved to DLQ"

      -- Main queue empty
      mainCount <- runM env (HL.countJobs @payload)
      mainCount `shouldBe` 0

  describe "4-level nesting" $ do
    it "4-level tree: full lifecycle (insert, ack bottom-up, completion cascade)" $ \env -> do
      -- L1 (root) → L2 (rollup) → L3 (rollup) → [L4a, L4b]
      Right (level1 :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
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

      let level2 = head rest
          level3 = rest !! 1

      -- All rollup ancestors suspended
      assertSuspended env (primaryKey level1)
      assertSuspended env (primaryKey level2)
      assertSuspended env (primaryKey level3)

      -- Only leaves are claimable
      leaves <- claimJobs env 10
      length leaves `shouldBe` 2
      map payload leaves `shouldMatchList` [mkMessage "L4LeafA", mkMessage "L4LeafB"]

      -- Ack both leaves → L3 wakes
      forM_ leaves $ \job -> void $ runM env (HL.ackJob job)
      assertNotSuspended env (primaryKey level3)
      assertSuspended env (primaryKey level2)
      assertSuspended env (primaryKey level1)

      -- Claim and ack L3 → L2 wakes
      [l3Claimed] <- claimJobs env 1
      payload l3Claimed `shouldBe` mkMessage "L3Inner"
      void $ runM env (HL.ackJob l3Claimed)
      assertNotSuspended env (primaryKey level2)
      assertSuspended env (primaryKey level1)

      -- Claim and ack L2 → L1 wakes
      [l2Claimed] <- claimJobs env 1
      payload l2Claimed `shouldBe` mkMessage "L2Mid"
      void $ runM env (HL.ackJob l2Claimed)
      assertNotSuspended env (primaryKey level1)

      -- Claim and ack L1 → done
      [l1Claimed] <- claimJobs env 1
      payload l1Claimed `shouldBe` mkMessage "L1Root"
      void $ runM env (HL.ackJob l1Claimed)
      assertGone env (primaryKey level1)

    it "4-level tree: cascade DLQ from root and retry from deepest leaf" $ \env -> do
      Right (level1 :| rest) <-
        runM env
          $ HL.insertJobTree
          $ JT.rollup
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

      let level2 = head rest
          level3 = rest !! 1

      -- DLQ from root → all 5 in DLQ
      void $ runM env (HL.moveToDLQ "admin" level1)
      dlqJobs <- dlqAll env
      length dlqJobs `shouldBe` 5

      -- Retry from deepest leaf
      let leafDLQ = head $ filter (\candidate -> payload (DLQ.jobSnapshot candidate) == mkMessage "4LDLQLeafA") dlqJobs
      Just retried <- runM env $ HL.retryFromDLQ @payload (DLQ.dlqPrimaryKey leafDLQ)
      payload retried `shouldBe` mkMessage "4LDLQLeafA"

      -- DLQ empty
      dlqAfter <- dlqAll env
      length dlqAfter `shouldBe` 0

      -- All rollup ancestors suspended
      assertSuspended env (primaryKey level1)
      assertSuspended env (primaryKey level2)
      assertSuspended env (primaryKey level3)

      -- Only leaves claimable
      leaves <- claimJobs env 10
      length leaves `shouldBe` 2
      map payload leaves `shouldMatchList` [mkMessage "4LDLQLeafA", mkMessage "4LDLQLeafB"]

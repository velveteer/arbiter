{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | Backend-parameterized integration tests for per-job rate limiting. Each
-- backend supplies a runner over the shared 'RLReg' registry.
module Arbiter.Test.RateLimit
  ( RLPayload (..)
  , RLReg
  , rateLimitTable
  , setupRateLimitPolicy
  , rateLimitSpec
  ) where

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema (..), HasRegistry)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema (jobQueueTable)
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , JobRead
  , JobStatus (..)
  , JobWrite
  , admission
  , attempts
  , dedupKey
  , defaultGroupedJob
  , defaultJob
  , jobRateLimitKey
  , payload
  )
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Core.RateLimit.Schema
  ( arbiterRateLimitPoliciesTable
  , arbiterRateLimitsTable
  , toPolicyRow
  , upsertPolicyRowSQL
  )
import Arbiter.Core.RateLimit.Spec
  ( HasRateLimit (..)
  , Policy
  , RateLimitFor
  , RateLimitKey (..)
  , chooseWhen
  , collectPolicies
  , limitBy
  , limitByCase
  , noLimit
  , registryRateLimitPolicies
  , runRateLimitFor
  , tokenBucket
  )
import Arbiter.Core.RateLimit.Stats
  ( RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)
  , RateLimitPolicyView (..)
  )
import Control.Exception (finally)
import Control.Monad (foldM_, void)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Foldable (find, traverse_)
import Data.List.NonEmpty qualified as NE
import Data.Maybe (fromMaybe, listToMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import GHC.Generics (Generic)
import Hedgehog (Gen, assert, check, evalIO, forAll, property, withTests, (===))
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Hspec
import UnliftIO.Async (mapConcurrently)

import Arbiter.Test.Setup (drainWith, execStatement, execute_)

data RLPayload = RLPayload {rlTenant :: Text, rlCost :: Double}
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type RLReg = '[Queue "arbiter_ratelimit_test" RLPayload]

-- 3 tokens, burst 3, refilling 3 every 2 seconds (1.5 tokens/sec).
rlPolicy :: Policy
rlPolicy = tokenBucket "rl" 3 2

instance HasRateLimit RLPayload where
  rateLimitFor = limitBy rlPolicy rlTenant
  rateLimitCost = rlCost

-- | Table name for 'RLReg', shared across backends (each in its own schema).
rateLimitTable :: Text
rateLimitTable = "arbiter_ratelimit_test"

-- | Upsert the registry's reflected policy rows into a schema (the same set the
-- migration seeds, collected from each payload's 'rateLimitFor').
setupRateLimitPolicy :: ByteString -> Text -> IO ()
setupRateLimitPolicy connStr schema = do
  conn <- connectPostgreSQL connStr
  traverse_ (execute_ conn . upsertPolicyRowSQL schema . toPolicyRow) (Set.toList (registryRateLimitPolicies @RLReg))
  close conn

job :: Text -> JobWrite RLPayload
job tenant = defaultJob (RLPayload tenant 1)

costJob :: Text -> Double -> JobWrite RLPayload
costJob tenant cost = defaultJob (RLPayload tenant cost)

groupedJob :: Text -> Text -> JobWrite RLPayload
groupedJob gk tenant = defaultGroupedJob gk (RLPayload tenant 1)

rateLimitSpec
  :: forall env m
   . (HasRegistry m RLReg, MonadArbiter m)
  => (forall a. env -> m a -> IO a)
  -> SpecWith env
rateLimitSpec runM = do
  let enqueue env js = void (runM env (HL.insertJobsBatch js) :: IO [JobRead RLPayload])
      claim env = runM env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead RLPayload]
      tshow = T.pack . show :: Int -> Text
      -- Delete every bucket, as the reaper's prune does to a full idle bucket.
      deleteBuckets env = runM env $ do
        schema <- getSchema
        void $ execStatement ("DELETE FROM " <> arbiterRateLimitsTable schema) []
      -- Clear the queue, leaving the buckets, so a model step starts from an empty queue.
      deleteJobs env = runM env $ do
        schema <- getSchema
        void $ execStatement ("DELETE FROM " <> jobQueueTable schema rateLimitTable) []
      -- Fast-forward time by backdating every bucket and job timer, so window-based
      -- tests are deterministic instead of sleeping. The job UPDATE fires the groups
      -- trigger, which recomputes in_flight_until so a stalled group resumes.
      fastForward env secs = runM env $ do
        schema <- getSchema
        let by = " - " <> tshow secs <> " * interval '1 second'"
        void $
          execStatement
            ( "UPDATE "
                <> arbiterRateLimitsTable schema
                <> " SET last_refill = last_refill"
                <> by
            )
            []
        void $
          execStatement
            ( "UPDATE "
                <> jobQueueTable schema rateLimitTable
                <> " SET not_visible_until = not_visible_until"
                <> by
                <> ", throttled_until = throttled_until"
                <> by
            )
            []
      setOverride env mx rf =
        void $
          runM env (HL.updateRateLimitPolicyOverrides "rl" (RateLimitPolicyUpdate (Just (Just mx)) (Just (Just rf)) Nothing))
      clearOverride env =
        void $ runM env (HL.updateRateLimitPolicyOverrides "rl" (RateLimitPolicyUpdate (Just Nothing) (Just Nothing) Nothing))

  it "admits up to the bucket size and defers the rest" $ \env -> do
    enqueue env (replicate 10 (job "burst"))
    kept <- claim env
    length kept `shouldBe` 3

  it "keeps a separate bucket per key" $ \env -> do
    enqueue env (replicate 5 (job "iso-a") <> replicate 5 (job "iso-b"))
    kept <- claim env
    length kept `shouldBe` 6

  it "skips a throttled key's fresh jobs at claim time" $ \env -> do
    enqueue env (replicate 10 (job "claimskip"))
    admitted <- claim env
    length admitted `shouldBe` 3
    -- A drained key's fresh jobs are denied at claim and deferred, never handed out.
    enqueue env (replicate 3 (job "claimskip"))
    skipped <- claim env
    length skipped `shouldBe` 0

  it "refills over time" $ \env -> do
    enqueue env (replicate 3 (job "refill"))
    first <- claim env
    length first `shouldBe` 3
    enqueue env (replicate 3 (job "refill"))
    emptied <- claim env
    length emptied `shouldBe` 0
    fastForward env 3
    refilled <- claim env
    length refilled `shouldBe` 3

  it "refills at the policy rate, partially (not all-or-nothing)" $ \env -> do
    -- rl is 3 tokens / 2s = 1.5 tokens/sec, so backdating last_refill by 1s accrues
    -- ~1.5 tokens: exactly one cost-1 job admits (leaving ~0.5), not a full bucket.
    -- The 0.5-token margin to the next integer absorbs the sub-second real time
    -- between backdate and claim, so the admitted count is a deterministic 1.
    enqueue env (replicate 3 (job "refilldet"))
    drained <- claim env
    length drained `shouldBe` 3
    fastForward env 1
    enqueue env (replicate 3 (job "refilldet"))
    refilled <- claim env
    length refilled `shouldBe` 1

  it "caps accrued refill at the bucket max" $ \env -> do
    -- Backdating 10s accrues 15 tokens, but LEAST(max) caps it at 3.
    enqueue env (replicate 3 (job "refillcap"))
    _ <- claim env
    fastForward env 10
    enqueue env (replicate 5 (job "refillcap"))
    capped <- claim env
    length capped `shouldBe` 3

  it "gates grouped heads across groups sharing a key" $ \env -> do
    enqueue env [groupedJob ("grp-" <> tshow i) "g-shared" | i <- [1 .. 5]]
    kept <- claim env
    length kept `shouldBe` 3

  it "stalls a throttled group instead of overtaking with a sibling" $ \env -> do
    enqueue env (replicate 3 (job "g-stall"))
    _ <- claim env
    enqueue env [groupedJob "stallgrp" "g-stall", groupedJob "stallgrp" "g-stall"]
    k1 <- claim env
    length k1 `shouldBe` 0
    stalled <- claim env
    length stalled `shouldBe` 0
    fastForward env 3
    resumed <- claim env
    length resumed `shouldBe` 1

  it "stalls a mixed-key group on its head instead of overtaking" $ \env -> do
    enqueue env (replicate 3 (job "mk-head"))
    _ <- claim env
    enqueue env [groupedJob "mixed" "mk-head", groupedJob "mixed" "mk-sib"]
    k1 <- claim env
    length k1 `shouldBe` 0
    stalled <- claim env
    length stalled `shouldBe` 0
    fastForward env 3
    resumed <- claim env
    case resumed of
      [j] -> rlTenant (payload j) `shouldBe` "mk-head"
      _ -> expectationFailure ("expected exactly the head, got " <> show (length resumed))

  it "does not spend a grouped job's attempt when throttled" $ \env -> do
    -- A throttled grouped job is deferred at claim with no attempt charged, so when
    -- it finally runs it is on its first attempt and throttling never eats its budget.
    enqueue env (replicate 3 (job "gb"))
    drained <- claim env
    length drained `shouldBe` 3
    enqueue env [groupedJob "gbgrp" "gb"]
    throttled <- claim env
    length throttled `shouldBe` 0
    fastForward env 3
    resumed <- claim env
    case resumed of
      [j] -> do
        rlTenant (payload j) `shouldBe` "gb"
        attempts j `shouldBe` 1
      _ -> expectationFailure ("expected exactly the resumed head, got " <> show (length resumed))

  it "preserves group order in batched mode across mixed keys" $ \env -> do
    enqueue env [groupedJob "bgroup" "bk-head", groupedJob "bgroup" "bk-sib"]
    batches <- runM env (HL.claimNextVisibleJobsBatched 5 100 60) :: IO [NE.NonEmpty (JobRead RLPayload)]
    map (rlTenant . payload) (concatMap NE.toList batches) `shouldBe` ["bk-head", "bk-sib"]

  it "stalls a throttled grouped head over a fresh-key sibling in batched mode" $ \env -> do
    -- Drain the head's key. A batched claim of a mixed-key group must defer the whole
    -- batch on its throttled head rather than run the fresh-key sibling ahead of it.
    enqueue env (replicate 3 (job "bh-head"))
    _ <- claim env
    enqueue env [groupedJob "bgrp" "bh-head", groupedJob "bgrp" "bh-sib"]
    stalled <- runM env (HL.claimNextVisibleJobsBatched 5 100 60) :: IO [NE.NonEmpty (JobRead RLPayload)]
    concatMap NE.toList stalled `shouldSatisfy` null
    fastForward env 3
    resumed <- runM env (HL.claimNextVisibleJobsBatched 5 100 60)
    map (rlTenant . payload) (concatMap NE.toList resumed) `shouldBe` ["bh-head", "bh-sib"]

  it "re-seeds a missing bucket instead of admitting unthrottled" $ \env -> do
    -- A policied key whose bucket was pruned/reset must be re-limited, not run unbounded.
    enqueue env (replicate 10 (job "reseed"))
    deleteBuckets env
    first <- claim env
    length first `shouldBe` 0
    second <- claim env
    length second `shouldBe` 3

  it "keeps a throttled head stalled after a granted batch-mate is acked" $ \env -> do
    -- Drain the shared key to one token, then a batch grants the head and throttles
    -- its sibling. Acking the granted head must not drop the group's in-flight marker
    -- and let a later ready sibling (on a fresh key) overtake the throttled one.
    enqueue env (replicate 2 (job "ov"))
    _ <- claim env
    enqueue env [groupedJob "ovgrp" "ov", groupedJob "ovgrp" "ov", groupedJob "ovgrp" "ov-free"]
    batches <- runM env (HL.claimNextVisibleJobsBatched 2 100 60) :: IO [NE.NonEmpty (JobRead RLPayload)]
    let granted = concatMap NE.toList batches
    map (rlTenant . payload) granted `shouldBe` ["ov"]
    _ <- runM env (HL.ackJobsBatch granted)
    -- The fresh-key sibling must never be handed out ahead of the throttled head,
    -- whether the group stays stalled (claim []) or unstalls in order (claim [head]).
    overtaken <- claim env
    map (rlTenant . payload) overtaken `shouldSatisfy` notElem "ov-free"

  it "keeps a throttled survivor stalled when a sibling is dedup-moved to another group" $ \env -> do
    -- Both grouped "ov" jobs throttle on a drained key. Dedup-replacing one of them
    -- into another group must recompute the old group's in-flight marker from the
    -- throttled survivor, not null it, or the ready "ov-free" sibling overtakes.
    enqueue env (replicate 3 (job "ov"))
    _ <- claim env
    let mover = (groupedJob "ovgrp" "ov") {dedupKey = Just (ReplaceDuplicate "mover-key")}
    enqueue env [groupedJob "ovgrp" "ov", mover, groupedJob "ovgrp" "ov-free"]
    batches <- runM env (HL.claimNextVisibleJobsBatched 2 100 60) :: IO [NE.NonEmpty (JobRead RLPayload)]
    length (concatMap NE.toList batches) `shouldBe` 0
    enqueue env [(groupedJob "othergrp" "ov") {dedupKey = Just (ReplaceDuplicate "mover-key")}]
    overtaken <- claim env
    map (rlTenant . payload) overtaken `shouldSatisfy` notElem "ov-free"

  it "defers the over-budget jobs with a future wake" $ \env -> do
    -- Five jobs share a key (bucket size 3): the claim admits 3 and parks the rest
    -- ahead of now, so a re-claim finds nothing until the bucket refills.
    enqueue env (replicate 5 (job "throttlecb"))
    kept <- claim env
    length kept `shouldBe` 3
    again <- claim env
    length again `shouldBe` 0
    fastForward env 3
    woken <- claim env
    length woken `shouldBe` 2

  it "reports a throttle-deferred job as throttled status" $ \env -> do
    enqueue env (replicate 5 (job "statuskey"))
    _ <- claim env
    throttled <- runM env (HL.countJobsFiltered @RLPayload [HL.FilterStatus Throttled])
    throttled `shouldBe` 2

  it "spends a job's cost, not just one token" $ \env -> do
    -- bucket holds 3, so of two cost-2 jobs only the first fits
    enqueue env [costJob "weighted" 2, costJob "weighted" 2]
    kept <- claim env
    length kept `shouldBe` 1

  it "debits the bucket on admission and parks the overflow without charging its attempt" $ \env -> do
    -- Freeze refill (rf=0) so the post-claim balance is exact. Admitting must both
    -- deny the overflow AND spend tokens: a regression that hands jobs out without
    -- debiting leaves tokens pinned at max, which the remaining-token check catches.
    flip finally (clearOverride env) $ do
      setOverride env 3 0
      deleteBuckets env
      enqueue env (replicate 5 (job "debit"))
      kept <- claim env
      length kept `shouldBe` 3
      remaining <- listToMaybe . map tokens <$> runM env (HL.listRateLimitBuckets "rl" 100 0)
      remaining `shouldBe` Just 0
      -- Top up refills the frozen bucket and wakes the two parked jobs. Throttling
      -- deferred them without spending an attempt, so they run on their first.
      runM env (HL.addRateLimitTokens (RateLimitKey "rl" "debit") 3)
      woken <- claim env
      map attempts woken `shouldBe` [1, 1]

  it "tops up a bucket manually" $ \env -> do
    enqueue env (replicate 3 (job "topup"))
    drained <- claim env
    length drained `shouldBe` 3
    runM env (HL.addRateLimitTokens (RateLimitKey "rl" "topup") 3)
    enqueue env (replicate 3 (job "topup"))
    toppedUp <- claim env
    length toppedUp `shouldBe` 3

  it "wakes a key's deferred jobs on a top-up instead of leaving them parked" $ \env -> do
    enqueue env (replicate 6 (job "topupwake"))
    admitted <- claim env
    length admitted `shouldBe` 3
    -- The top-up refills the bucket and wakes the parked jobs, so they run now
    -- rather than waiting out their deferral.
    runM env (HL.addRateLimitTokens (RateLimitKey "rl" "topupwake") 3)
    woken <- claim env
    length woken `shouldBe` 3

  it "seeds an absent bucket at full on a top-up, not at the top-up amount" $ \env -> do
    -- An absent bucket is conceptually full (consume self-seeds at max), so a
    -- top-up of 1 must leave it full (admitting the bucket size), not set it to 1.
    runM env (HL.addRateLimitTokens (RateLimitKey "rl" "seedfull") 1)
    enqueue env (replicate 5 (job "seedfull"))
    kept <- claim env
    length kept `shouldBe` 3

  it "wakes deferred jobs on reset so a window release is immediate" $ \env -> do
    enqueue env (replicate 6 (job "windowkey"))
    admitted <- claim env
    length admitted `shouldBe` 3
    -- Reset wakes the deferred jobs instead of leaving them parked until their wait elapses.
    _ <- runM env (HL.resetRateLimitBuckets "rl")
    afterReset <- claim env
    length afterReset `shouldBe` 3

  it "clears a stale throttle marker on claim so a wake cannot double-claim it" $ \env -> do
    enqueue env [job "claimclears"]
    -- A ready job with a stale marker. Claiming must clear it so a wake cannot double-claim.
    runM env $ do
      schema <- getSchema
      void $
        execStatement
          ( "UPDATE "
              <> jobQueueTable schema rateLimitTable
              <> " SET throttled_until = NOW() + interval '60 second'"
          )
          []
    claimed <- claim env
    length claimed `shouldBe` 1
    _ <- runM env (HL.resetRateLimitBuckets "rl")
    reclaimed <- claim env
    length reclaimed `shouldBe` 0

  it "runs a too-costly job once per window instead of livelocking" $ \env -> do
    -- cost 5 exceeds the bucket max 3. The spend clamps to the max so the first
    -- job drains a full bucket and runs, the second is deferred. The pre-fund
    -- also seeds the bucket through the upsert top-up path.
    runM env (HL.addRateLimitTokens (RateLimitKey "rl" "cmax") 3)
    enqueue env [costJob "cmax" 5, costJob "cmax" 5]
    kept <- claim env
    length kept `shouldBe` 1

  it "preserves the rate-limit key through a DLQ retry" $ \env -> do
    enqueue env [job "dlqkey"]
    claimed <- claim env
    case claimed of
      [c] -> do
        _ <- runM env (HL.moveToDLQ "boom" c)
        dlqs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob RLPayload]
        case find ((== "dlqkey") . rlTenant . payload . DLQ.jobSnapshot) dlqs of
          Just d -> do
            retried <- runM env (HL.retryFromDLQ (DLQ.dlqPrimaryKey d)) :: IO (Maybe (JobRead RLPayload))
            (jobRateLimitKey . admission <$> retried) `shouldBe` Just (Just (RateLimitKey "rl" "dlqkey"))
          Nothing -> expectationFailure "job did not reach the DLQ"
      _ -> expectationFailure ("expected exactly one claimed job, got " <> show (length claimed))

  it "does not prune a drained bucket" $ \env -> do
    -- A drained bucket is not full, so pruning must leave it in place. Were it
    -- pruned, the next claim would self-seed at full and admit a burst.
    enqueue env (replicate 3 (job "pd"))
    drained <- claim env
    length drained `shouldBe` 3
    _ <- runM env (HL.pruneRateLimitBuckets 0)
    enqueue env (replicate 3 (job "pd"))
    again <- claim env
    length again `shouldBe` 0

  it "prunes a full bucket without creating a burst" $ \env -> do
    enqueue env (replicate 3 (job "pf"))
    _ <- claim env
    fastForward env 3
    pruned <- runM env (HL.pruneRateLimitBuckets 0)
    pruned `shouldSatisfy` (>= 1)
    enqueue env (replicate 10 (job "pf"))
    kept <- claim env
    length kept `shouldBe` 3

  it "never over-admits one key under concurrent claiming" $ \env -> do
    -- Many workers claim the same key at once. The bucket's FOR UPDATE SKIP LOCKED
    -- lets one claimer through per cycle, so exactly the bucket size is admitted.
    enqueue env (replicate 60 (job "conc"))
    results <- mapConcurrently (const (claim env)) [1 .. 10 :: Int]
    length (concat results) `shouldBe` 3

  it "never over-admits across many keys under concurrent claiming" $ \env -> do
    -- rf=0 freezes refill, so the per-key drain is exact across distinct buckets.
    let tenants = [tshow i | i <- [1 .. 12]]
    flip finally (clearOverride env) $ do
      setOverride env 3 0
      deleteBuckets env
      enqueue env (concat [replicate 8 (job t) | t <- tenants])
      burst <- concat <$> mapConcurrently (const (claim env)) [1 .. 8 :: Int]
      rest <- drainWith (claim env)
      let admitted = burst <> rest
          perTenant t = length (filter ((== t) . rlTenant . payload) admitted)
      map perTenant tenants `shouldBe` replicate (length tenants) 3

  it "reports policy and bucket stats through the management plane" $ \env -> do
    enqueue env (replicate 2 (job "stat"))
    _ <- claim env
    policies <- runM env HL.listRateLimitPolicies
    case filter ((== "rl") . prefix) policies of
      [v] -> do
        defaultMaxTokens v `shouldBe` 3
        bucketCount v `shouldSatisfy` (>= 1)
      _ -> expectationFailure "expected exactly the rl policy"
    bs <- runM env (HL.listRateLimitBuckets "rl" 100 0)
    length bs `shouldSatisfy` (>= 1)
    map policyPrefix bs `shouldSatisfy` all (== "rl")

  it "applies and clears a policy override through the management plane" $ \env -> do
    -- Override max to 0 pauses the prefix. Clearing it restores the default.
    -- 'finally' restores the shared 'rl' policy even if an assertion fails.
    flip finally (clearOverride env) $ do
      n1 <- runM env (HL.updateRateLimitPolicyOverrides "rl" (RateLimitPolicyUpdate (Just (Just 0)) Nothing Nothing))
      n1 `shouldBe` 1
      enqueue env [job "paused"]
      blocked <- claim env
      length blocked `shouldBe` 0
      clearOverride env
      enqueue env [job "resumed"]
      admitted <- claim env
      length admitted `shouldBe` 1

  it "admits a job whose prefix has no policy (fail-open)" $ \env -> do
    -- With no policy row the gate has no limit to apply, so every job runs.
    let restore = runM env $ do
          schema <- getSchema
          void $ execStatement (upsertPolicyRowSQL schema (toPolicyRow rlPolicy)) []
    flip finally restore $ do
      runM env $ do
        schema <- getSchema
        void $ execStatement ("DELETE FROM " <> arbiterRateLimitPoliciesTable schema <> " WHERE prefix_id = 'rl'") []
      enqueue env (replicate 5 (job "failopen"))
      admitted <- claim env
      length admitted `shouldBe` 5

  it "gate admission matches the reference token-bucket model over random ops" $ \env -> do
    -- An rf=0 override makes token math integral. Each consume enqueues+claims one cost-job, checked against the pure model.
    let modelTenant = "model"
        modelKey = RateLimitKey "rl" modelTenant
        consume cost = do
          enqueue env [costJob modelTenant (fromIntegral cost)]
          claimed <- claim env
          deleteJobs env
          pure (not (null claimed))
        -- rf=0 keeps refill at 0, so the bucket view's token count is the raw stored count.
        readStored = listToMaybe . map tokens <$> runM env (HL.listRateLimitBuckets "rl" 100 0)
        step toks op = do
          toks' <- case op of
            Consume cost -> do
              granted <- evalIO (consume cost)
              let (expected, next) = modelConsume toks cost
              granted === expected
              pure next
            TopUp amt -> do
              evalIO (runM env (HL.addRateLimitTokens modelKey (fromIntegral amt)))
              pure (modelTopUp toks amt)
            Prune -> do
              evalIO (void (runM env (HL.pruneRateLimitBuckets 0)))
              pure toks
          -- A pruned (full) bucket reads as absent, which equals a full bucket.
          stored <- evalIO readStored
          fromMaybe (fromIntegral modelMax) stored === fromIntegral toks'
          pure toks'
    flip finally (clearOverride env) $ do
      ok <- check $ withTests 50 $ property $ do
        ops <- forAll genOps
        evalIO (setOverride env (fromIntegral modelMax) 0 >> deleteBuckets env)
        foldM_ step modelMax ops
      ok `shouldBe` True

  it "never over- or under-admits one key under concurrent mixed-cost claims" $ \env -> do
    -- rf=0, bucket 10. Concurrent claimers drain one key whose jobs carry random costs.
    flip finally (clearOverride env) $ do
      ok <- check $ withTests 20 $ property $ do
        costs <- forAll (Gen.list (Range.linear 2 15) (Gen.integral (Range.linear (1 :: Int) 3)))
        evalIO (setOverride env 10 0 >> deleteBuckets env >> deleteJobs env)
        evalIO (enqueue env [costJob "concmix" (fromIntegral c) | c <- costs])
        -- Drain to quiescence so a single sweep's SKIP-LOCKED skips do not read as under-admission.
        let sweep = concat <$> mapConcurrently (const (claim env)) [1 .. 8 :: Int]
        claimed <- evalIO (drainWith sweep)
        let admittedCost = sum (map (rlCost . payload) claimed) :: Double
            total = fromIntegral (sum costs) :: Double
            slack = fromIntegral (maximum costs - 1) :: Double
        -- The bucket's FOR UPDATE SKIP LOCKED serializes spend, so never over the cap...
        assert (admittedCost <= 10)
        -- ...and it fills to within one job's cost, so it never parks jobs that fit.
        assert (admittedCost >= min total 10 - slack)
      ok `shouldBe` True

  it "collects both branches of a choice selector and runs the chosen one" $ \_env -> do
    -- The selective property: a per-job choice still statically yields every
    -- policy it could use, so the migration seeds both branches.
    let pa = tokenBucket "ca" 1 1
        pb = tokenBucket "cb" 2 2
        sel :: RateLimitFor Bool
        sel = chooseWhen id (limitBy pa (const "x")) (limitBy pb (const "y"))
    Set.toList (collectPolicies sel) `shouldMatchList` [pa, pb]
    (rlkPrefix <$> runRateLimitFor True sel) `shouldBe` Just "ca"
    (rlkPrefix <$> runRateLimitFor False sel) `shouldBe` Just "cb"

  it "limitByCase collects every branch and runs the matched one" $ \_env -> do
    let pa = tokenBucket "la" 1 1
        pb = tokenBucket "lb" 2 2
        sel :: RateLimitFor Ordering
        sel = limitByCase id $ \case
          LT -> limitBy pa (const "x")
          EQ -> noLimit
          GT -> limitBy pb (const "y")
    Set.toList (collectPolicies sel) `shouldMatchList` [pa, pb]
    (rlkPrefix <$> runRateLimitFor LT sel) `shouldBe` Just "la"
    runRateLimitFor EQ sel `shouldBe` Nothing
    (rlkPrefix <$> runRateLimitFor GT sel) `shouldBe` Just "lb"

-- Pure reference bucket (rf=0, so tokens are integral and never refill).

modelMax :: Int
modelMax = 10

modelConsume :: Int -> Int -> (Bool, Int)
modelConsume toks cost
  | modelMax > 0 && toks >= ecost = (True, toks - ecost)
  | otherwise = (False, toks)
  where
    ecost = max 0 (min cost modelMax)

modelTopUp :: Int -> Int -> Int
modelTopUp toks = min modelMax . (toks +)

data Op = Consume Int | TopUp Int | Prune
  deriving stock (Show)

genOps :: Gen [Op]
genOps =
  Gen.list (Range.linear 1 30) $
    Gen.choice
      [ Consume <$> Gen.integral (Range.linearFrom 1 (-3) (modelMax + 3))
      , TopUp <$> Gen.integral (Range.linear 0 (modelMax + 3))
      , pure Prune
      ]

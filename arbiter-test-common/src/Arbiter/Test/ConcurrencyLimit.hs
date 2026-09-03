{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

-- | Backend-parameterized integration tests for per-job concurrency limits over the shared
-- 'CLReg' registry. Each job's key comes from the 'HasConcurrency CLPayload' instance.
module Arbiter.Test.ConcurrencyLimit
  ( CLPayload (..)
  , CLReg
  , concurrencyTable
  , concurrencyLimitSpec
  ) where

import Arbiter.Core.Codec (Col (..), col, pval)
import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyTable)
import Arbiter.Core.Concurrency.Spec
  ( ConcurrencyFor
  , ConcurrencyKey (..)
  , ConcurrencyPolicy (..)
  , HasConcurrency (..)
  , chooseWhen
  , collectPolicies
  , concurrencyBy
  , concurrencyByCase
  , concurrencyPool
  , registryConcurrencyPolicies
  , runConcurrencyFor
  )
import Arbiter.Core.Concurrency.Stats (ConcurrencyPolicyUpdate (..))
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema (jobQueueTable)
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , JobRead
  , JobWrite
  , defaultGroupedJob
  , defaultJob
  , payload
  , setDedupKey
  )
import Arbiter.Core.MonadArbiter (HasRegistry, getSchema, withDbTransaction)
import Arbiter.Core.MonadArbiter qualified as MA
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Core.Sql.Concurrency qualified as Tmpl
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.List.NonEmpty qualified as NE
import Data.Maybe (listToMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID.Types qualified as UUID
import GHC.Generics (Generic)
import System.Timeout (timeout)
import Test.Hspec
import UnliftIO.Async (async, mapConcurrently, wait)

import Arbiter.Test.Setup (drainWith, execQuery, execStatement, seedConcurrencyPoolSQL)

-- | A payload declaring one concurrency pool.
newtype CLPayload = CLPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | A finite tag per payload. "mx", "my", and "mz" select those pools. Any other
-- text selects "declpool", keyed by the payload text.
data CLTag = CLDecl | CLMx | CLMy | CLMz
  deriving stock (Bounded, Enum, Eq)

instance HasConcurrency CLPayload where
  concurrencyFor = concurrencyByCase tagOf sel
    where
      tagOf (CLPayload text)
        | text == "mx" = CLMx
        | text == "my" = CLMy
        | text == "mz" = CLMz
        | otherwise = CLDecl
      sel CLDecl = concurrencyBy (concurrencyPool "declpool" 2) (\(CLPayload text) -> text)
      sel CLMx = concurrencyBy (concurrencyPool "mx" 1) (const "k")
      sel CLMy = concurrencyBy (concurrencyPool "my" 2) (const "k")
      sel CLMz = concurrencyBy (concurrencyPool "mz" 3) (const "k")

-- | A one-queue registry over 'CLPayload'.
type CLReg = '[Queue "arbiter_concurrency_test" CLPayload]

-- | A second payload declaring a different pool, with a two-payload registry.
newtype CLPayload2 = CLPayload2 Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

instance HasConcurrency CLPayload2 where
  concurrencyFor = concurrencyBy (concurrencyPool "declpool2" 5) (\(CLPayload2 text) -> text)

type CLReg2 = '[Queue "clq1" CLPayload, Queue "clq2" CLPayload2]

-- | Table name for 'CLReg', shared across backends.
concurrencyTable :: Text
concurrencyTable = "arbiter_concurrency_test"

fullKey :: Text -> Text -> Text
fullKey pool suffix = "declpool:" <> pool <> ":" <> suffix

job :: Text -> Text -> JobWrite CLPayload
job pool suffix = defaultJob (CLPayload (pool <> ":" <> suffix))

groupedJob :: Text -> Text -> Text -> JobWrite CLPayload
groupedJob groupKey pool suffix = defaultGroupedJob groupKey (CLPayload (pool <> ":" <> suffix))

-- | The concurrency-limit suite, run against any backend.
concurrencyLimitSpec
  :: forall env m
   . (HasRegistry m CLReg)
  => (forall a. env -> m a -> IO a)
  -> SpecWith env
concurrencyLimitSpec runM = do
  let wid = UUID.nil
      tshow = T.pack . show :: Int -> Text
      enqueue env jobs = void (runM env (HL.insertJobsBatch jobs) :: IO [JobRead CLPayload])
      -- Claims are attributed.
      claimAs env = runM env (HL.claimNextVisibleJobsAs 100 60 wid) :: IO [JobRead CLPayload]
      ackAll env jobs = runM env (traverse_ HL.ackJob jobs)
      retryAll env jobs = runM env (traverse_ (HL.updateJobForRetry 60 "boom") jobs)
      nackAll env jobs = runM env (traverse_ HL.nackJob jobs)
      overridePool env lim = void (runM env (HL.updateConcurrencyPolicyOverrides "declpool" (ConcurrencyPolicyUpdate (Just lim))) :: IO Int64)
      prune env = runM env HL.pruneConcurrencyKeys :: IO Int64
      reconcile env = void (runM env HL.reconcileConcurrencyCounts :: IO Int64)
      reconcileIfStale env = void (runM env HL.reconcileConcurrencyCountsIfStale) :: IO ()
      -- Empty the UNLOGGED count table the way a Postgres crash recovery would.
      truncateCounts env =
        runM env $ do
          schema <- getSchema
          void $ execStatement ("DELETE FROM " <> arbiterConcurrencyTable schema) []
      -- Seed the declpool default limit and clear any override.
      seed env (lim :: Int) =
        runM env $ do
          schema <- getSchema
          traverse_
            (\statement -> void (execStatement statement []))
            (seedConcurrencyPoolSQL schema "declpool" (fromIntegral lim))
      corrupt env key count =
        runM env $ do
          schema <- getSchema
          void $
            execStatement
              ("UPDATE " <> arbiterConcurrencyTable schema <> " SET in_flight = " <> tshow count <> " WHERE concurrency_key = ?")
              [pval CText key]
      -- Mark a grouped job as already failed.
      markAttempted env key =
        runM env $ do
          schema <- getSchema
          void $
            execStatement
              ( "UPDATE "
                  <> jobQueueTable schema concurrencyTable
                  <> " SET attempts = 1 WHERE concurrency_key = ? AND group_key IS NOT NULL"
              )
              [pval CText key]
      deleteRow env key =
        runM env $ do
          schema <- getSchema
          void $
            execStatement
              ("DELETE FROM " <> arbiterConcurrencyTable schema <> " WHERE concurrency_key = ?")
              [pval CText key]
      inFlight env key =
        runM env $ do
          schema <- getSchema
          rows <-
            execQuery
              ("SELECT in_flight FROM " <> arbiterConcurrencyTable schema <> " WHERE concurrency_key = ?")
              [pval CText key]
              (col "in_flight" CInt4)
          pure (listToMaybe rows :: Maybe Int32)
      -- Backdate every job's visibility. Claimed jobs time out and become reclaimable.
      timeOut env secs = runM env $ do
        schema <- getSchema
        void $
          execStatement
            ( "UPDATE "
                <> jobQueueTable schema concurrencyTable
                <> " SET not_visible_until = not_visible_until - "
                <> tshow secs
                <> " * interval '1 second'"
            )
            []

  it "admits up to the pool limit and blocks the rest" $ \env -> do
    seed env 3
    enqueue env (replicate 5 (job "cap" "a"))
    first <- claimAs env
    length first `shouldBe` 3
    again <- claimAs env
    length again `shouldBe` 0

  it "an undeclared pool runs uncapped (fail open)" $ \env -> do
    -- Claim well above any seeded limit.
    enqueue env (replicate 20 (job "undeclared" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 20

  it "caps by a HasConcurrency instance's declared pool (no manual key)" $ \env -> do
    seed env 2
    enqueue env (replicate 5 (job "declpool" "tx"))
    first <- claimAs env
    length first `shouldBe` 2
    inFlight env (fullKey "declpool" "tx") `shouldReturn` Just 2

  it "the registry collects the declared pool for migration seeding" $ \_ ->
    registryConcurrencyPolicies @CLReg
      `shouldBe` Set.fromList
        [ ConcurrencyPolicy "declpool" 2
        , ConcurrencyPolicy "mx" 1
        , ConcurrencyPolicy "my" 2
        , ConcurrencyPolicy "mz" 3
        ]

  it "the registry unions declared pools across all payloads" $ \_ ->
    -- Two payloads, two pools.
    registryConcurrencyPolicies @CLReg2
      `shouldBe` Set.fromList
        [ ConcurrencyPolicy "declpool" 2
        , ConcurrencyPolicy "mx" 1
        , ConcurrencyPolicy "my" 2
        , ConcurrencyPolicy "mz" 3
        , ConcurrencyPolicy "declpool2" 5
        ]

  it "concurrencyPool floors a non-positive limit at 1 (the policies table requires a positive default)" $ \_ -> do
    cpLimit (concurrencyPool "p" 0) `shouldBe` 1
    cpLimit (concurrencyPool "p" (-5)) `shouldBe` 1

  it "chooseWhen collects both concurrency branches and runs the chosen one" $ \_ -> do
    let policyA = concurrencyPool "ca" 1
        policyB = concurrencyPool "cb" 2
        sel :: ConcurrencyFor Bool
        sel = chooseWhen id (concurrencyBy policyA (const "x")) (concurrencyBy policyB (const "y"))
    Set.toList (collectPolicies sel) `shouldMatchList` [policyA, policyB]
    (ckPrefix <$> runConcurrencyFor True sel) `shouldBe` Just "ca"
    (ckPrefix <$> runConcurrencyFor False sel) `shouldBe` Just "cb"

  it "frees a slot on ack" $ \env -> do
    seed env 3
    enqueue env (replicate 5 (job "freeack" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 3
    ackAll env (take 1 claimed)
    refilled <- claimAs env
    length refilled `shouldBe` 1

  it "frees a slot when a job goes back for retry" $ \env -> do
    seed env 3
    enqueue env (replicate 5 (job "freeretry" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 3
    retryAll env (take 2 claimed)
    refilled <- claimAs env
    length refilled `shouldBe` 2

  it "frees a slot on nack" $ \env -> do
    seed env 3
    enqueue env (replicate 5 (job "freenack" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 3
    nackAll env (take 2 claimed)
    refilled <- claimAs env
    length refilled `shouldBe` 2

  it "reclaims a timed-out job without exceeding the limit" $ \env -> do
    seed env 3
    enqueue env (replicate 5 (job "reclaim" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 3
    timeOut env 120
    reclaimed <- claimAs env
    length reclaimed `shouldBe` 3

  it "keeps separate keys under one pool independent" $ \env -> do
    seed env 2
    enqueue env (replicate 5 (job "iso" "x") <> replicate 5 (job "iso" "y"))
    claimed <- claimAs env
    length claimed `shouldBe` 4

  it "a pool override lowers the cap live" $ \env -> do
    seed env 3
    enqueue env (replicate 5 (job "ovlo" "a"))
    overridePool env (Just 1)
    claimed <- claimAs env
    length claimed `shouldBe` 1

  it "a pool override raises the cap live, across every key under the prefix" $ \env -> do
    seed env 2
    enqueue env (replicate 6 (job "ovhi" "x") <> replicate 6 (job "ovhi" "y"))
    overridePool env (Just 5)
    claimed <- claimAs env
    -- Both keys under the pool admit up to 5.
    length claimed `shouldBe` 10

  it "an override of 0 pauses the pool until cleared" $ \env -> do
    seed env 3
    enqueue env (replicate 3 (job "pause" "a"))
    overridePool env (Just 0)
    paused <- claimAs env
    length paused `shouldBe` 0
    overridePool env Nothing
    resumed <- claimAs env
    length resumed `shouldBe` 3

  it "an empty policy patch leaves the override unchanged, an explicit null clears it" $ \env -> do
    seed env 3
    enqueue env (replicate 6 (job "patch" "a"))
    overridePool env (Just 1)
    void (runM env (HL.updateConcurrencyPolicyOverrides "declpool" (ConcurrencyPolicyUpdate Nothing)) :: IO Int64)
    claimed <- claimAs env
    length claimed `shouldBe` 1
    overridePool env Nothing
    more <- claimAs env
    length more `shouldBe` 2

  it "in_flight tracks claims and acks" $ \env -> do
    seed env 4
    enqueue env (replicate 4 (job "track" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 4
    inFlight env (fullKey "track" "a") `shouldReturn` Just 4
    ackAll env (take 3 claimed)
    inFlight env (fullKey "track" "a") `shouldReturn` Just 1

  it "floors in_flight at zero when a decrement would underflow a drifted count" $ \env -> do
    -- Drift can leave a count row below its live claimed jobs. Acking one clamps
    -- the count at 0.
    seed env 3
    enqueue env [job "floor" "a"]
    claimed <- claimAs env
    length claimed `shouldBe` 1
    -- One claimed job. Force the count to read 0.
    corrupt env (fullKey "floor" "a") 0
    ackAll env claimed
    inFlight env (fullKey "floor" "a") `shouldReturn` Just 0

  it "prunes drained key rows" $ \env -> do
    seed env 3
    enqueue env (replicate 2 (job "drain" "x") <> replicate 2 (job "drain" "y"))
    claimAs env >>= ackAll env
    prune env `shouldReturn` 2
    inFlight env (fullKey "drain" "x") `shouldReturn` Nothing
    inFlight env (fullKey "drain" "y") `shouldReturn` Nothing

  it "prune skips a key held by an open enqueue transaction and the job still runs" $ \env -> do
    seed env 2
    enqueue env [job "skip" "a"]
    claimAs env >>= ackAll env
    inFlight env (fullKey "skip" "a") `shouldReturn` Just 0
    entered <- newEmptyMVar
    release <- newEmptyMVar
    enqueuer <- async $ runM env $ withDbTransaction $ do
      _ <- HL.insertJobsBatch [job "skip" "a"] :: m [JobRead CLPayload]
      liftIO $ putMVar entered ()
      liftIO $ takeMVar release
    takeMVar entered
    -- The open enqueue holds the key's shared advisory lock. Prune returns without it.
    pruned <- timeout 5000000 (prune env)
    pruned `shouldBe` Just 0
    inFlight env (fullKey "skip" "a") `shouldReturn` Just 0
    putMVar release ()
    wait enqueuer
    claimed <- claimAs env
    length claimed `shouldBe` 1
    ackAll env claimed
    prune env `shouldReturn` 1
    inFlight env (fullKey "skip" "a") `shouldReturn` Nothing

  it "prune racing a multi-statement descending-key enqueue transaction does not deadlock" $ \env -> do
    seed env 2
    enqueue env [job "dk" "a", job "dk" "b"]
    claimAs env >>= ackAll env
    entered <- newEmptyMVar
    pruneDone <- newEmptyMVar
    enqueuer <- async $ runM env $ withDbTransaction $ do
      _ <- HL.insertJobsBatch [job "dk" "b"] :: m [JobRead CLPayload]
      liftIO $ putMVar entered ()
      liftIO $ takeMVar pruneDone
      _ <- HL.insertJobsBatch [job "dk" "a"] :: m [JobRead CLPayload]
      pure ()
    takeMVar entered
    -- Prune sees both keys dead. It takes a and skips the held b.
    pruned <- timeout 5000000 (prune env)
    pruned `shouldBe` Just 1
    inFlight env (fullKey "dk" "a") `shouldReturn` Nothing
    putMVar pruneDone ()
    wait enqueuer
    -- The second insert reseeds the pruned key. Both jobs stay claimable.
    claimed <- claimAs env
    length claimed `shouldBe` 2

  it "preserves the concurrency cap through a DLQ round-trip" $ \env -> do
    seed env 1
    enqueue env [job "dlq" "a"]
    claimed <- claimAs env
    runM env (traverse_ (void . HL.moveToDLQ "boom") claimed)
    dlqs <- runM env (HL.listDLQJobs 10 0) :: IO [DLQ.DLQJob CLPayload]
    runM
      env
      (traverse_ (\dlqJob -> void (HL.retryFromDLQ (DLQ.dlqPrimaryKey dlqJob) :: m (Maybe (JobRead CLPayload)))) dlqs)
    reclaimed <- claimAs env
    length reclaimed `shouldBe` 1
    inFlight env (fullKey "dlq" "a") `shouldReturn` Just 1

  it "reconcile repairs a drifted in_flight count" $ \env -> do
    seed env 5
    enqueue env (replicate 3 (job "recon" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 3
    corrupt env (fullKey "recon" "a") 99
    inFlight env (fullKey "recon" "a") `shouldReturn` Just 99
    reconcile env
    inFlight env (fullKey "recon" "a") `shouldReturn` Just 3

  it "reconcile does not crash on a drained-but-unpruned key" $ \env -> do
    seed env 3
    enqueue env (replicate 2 (job "draincon" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 2
    -- Ack deletes the jobs. The count row drains and stays unpruned.
    ackAll env claimed
    -- Reconcile repairs a key with no live jobs.
    reconcile env
    inFlight env (fullKey "draincon" "a") `shouldReturn` Just 0

  it "reconcile recreates a missing count row from live jobs" $ \env -> do
    seed env 5
    enqueue env (replicate 3 (job "reconm" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 3
    deleteRow env (fullKey "reconm" "a")
    inFlight env (fullKey "reconm" "a") `shouldReturn` Nothing
    reconcile env
    inFlight env (fullKey "reconm" "a") `shouldReturn` Just 3

  it "reconcile against an in-flight claim does not overwrite the count below the truth" $ \env -> do
    -- Reconcile runs while the second job is claimed in an open transaction. It counts that job.
    seed env 5
    enqueue env [job "recrace" "a"]
    firstClaim <- claimAs env
    length firstClaim `shouldBe` 1
    enqueue env [job "recrace" "a"]
    claimed <- newEmptyMVar
    release <- newEmptyMVar
    claimer <- async $ runM env $ withDbTransaction $ do
      claimedJobs <- HL.claimNextVisibleJobsAs 100 60 wid :: m [JobRead CLPayload]
      liftIO $ putMVar claimed (length claimedJobs)
      liftIO $ takeMVar release
    takeMVar claimed `shouldReturn` 1
    reconciler <- async (reconcile env)
    threadDelay 200000
    putMVar release ()
    wait claimer
    wait reconciler
    inFlight env (fullKey "recrace" "a") `shouldReturn` Just 2

  it "reconcile does not overwrite a claim on a key seeded after its lock pass" $ \env -> do
    -- The key is born between the lock pass and the recount. The recount leaves
    -- it to its triggers.
    seed env 5
    lockTaken <- newEmptyMVar
    resume <- newEmptyMVar
    reconciler <- async $ runM env $ withDbTransaction $ do
      schema <- getSchema
      held <- MA.executeQuery (Tmpl.lockConcurrencyCountsSQL schema)
      liftIO $ putMVar lockTaken ()
      liftIO $ takeMVar resume
      void $
        MA.executeQuery
          (Tmpl.reconcileConcurrencyCountsSQL schema [concurrencyTable] held)
    takeMVar lockTaken
    enqueue env [job "lateseed" "a"]
    claimed <- newEmptyMVar
    release <- newEmptyMVar
    claimer <- async $ runM env $ withDbTransaction $ do
      claimedJobs <- HL.claimNextVisibleJobsAs 100 60 wid :: m [JobRead CLPayload]
      liftIO $ putMVar claimed (length claimedJobs)
      liftIO $ takeMVar release
    takeMVar claimed `shouldReturn` 1
    putMVar resume ()
    threadDelay 200000
    putMVar release ()
    wait claimer
    wait reconciler
    inFlight env (fullKey "lateseed" "a") `shouldReturn` Just 1

  it "a stale rebuild restores counts a crash truncated, so keyed jobs stay claimable" $ \env -> do
    seed env 2
    enqueue env (replicate 4 (job "stale" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 2
    -- A crash truncates the UNLOGGED count table. Keyed jobs are now unclaimable.
    truncateCounts env
    inFlight env (fullKey "stale" "a") `shouldReturn` Nothing
    blocked <- claimAs env
    length blocked `shouldBe` 0
    -- The startup stale rebuild repopulates the count from the live claimed jobs.
    reconcileIfStale env
    inFlight env (fullKey "stale" "a") `shouldReturn` Just 2

  it "a stale rebuild still fires when a post-crash enqueue re-seeds one key" $ \env -> do
    seed env 2
    enqueue env (replicate 4 (job "stale" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 2
    truncateCounts env
    -- A fresh enqueue seeds its own count row. The truncation is still detected.
    enqueue env [job "stale" "b"]
    reconcileIfStale env
    inFlight env (fullKey "stale" "a") `shouldReturn` Just 2

  it "a stale rebuild is a no-op on a healthy count table (does not clobber drift it should not touch)" $ \env -> do
    seed env 5
    enqueue env (replicate 3 (job "healthy" "a"))
    claimed <- claimAs env
    length claimed `shouldBe` 3
    -- The table is non-empty. The stale check skips the full reconcile.
    corrupt env (fullKey "healthy" "a") 99
    reconcileIfStale env
    inFlight env (fullKey "healthy" "a") `shouldReturn` Just 99

  it "moves the count row when a dedup replace changes the concurrency key" $ \env -> do
    seed env 1
    let mkJob suffix = setDedupKey (Just (ReplaceDuplicate "movekey")) $ job "ck" suffix
    enqueue env [mkJob "old"]
    enqueue env [mkJob "new"]
    inFlight env (fullKey "ck" "old") `shouldReturn` Just 0
    claimed <- claimAs env
    length claimed `shouldBe` 1
    inFlight env (fullKey "ck" "new") `shouldReturn` Just 1
    void (prune env)
    inFlight env (fullKey "ck" "old") `shouldReturn` Nothing

  it "caps across groups sharing one key" $ \env -> do
    seed env 2
    enqueue env [groupedJob ("g-" <> tshow index) "shared" "c" | index <- [1 .. 5]]
    claimed <- claimAs env
    length claimed `shouldBe` 2

  it "stalls a concurrency-blocked grouped head over a free-key sibling in batched mode" $ \env -> do
    -- With the head's key at its cap, a batched claim defers the whole group.
    seed env 1
    enqueue env [job "bh" "head"]
    filled <- claimAs env
    length filled `shouldBe` 1
    enqueue env [groupedJob "bgrp" "bh" "head", groupedJob "bgrp" "bh" "sib"]
    stalled <- runM env (HL.claimNextVisibleJobsBatched 5 100 60) :: IO [NE.NonEmpty (JobRead CLPayload)]
    concatMap NE.toList stalled `shouldSatisfy` null

  it "concurrent claimers never exceed the limit, and the key fills to the cap" $ \env -> do
    seed env 5
    enqueue env (replicate 40 (job "race" "a"))
    results <- mapConcurrently (const (claimAs env)) [1 .. 8 :: Int]
    let total = sum (map length results)
    -- At least one claimer makes progress. None pushes the key over the cap.
    total `shouldSatisfy` (\count -> count >= 1 && count <= 5)
    -- An uncontended follow-up fills the key to the cap. The contended total is
    -- not deterministic.
    more <- claimAs env
    (total + length more) `shouldBe` 5
    inFlight env (fullKey "race" "a") `shouldReturn` Just 5

  it "concurrent claimers across many keys never exceed any cap" $ \env -> do
    -- Many distinct keys at once.
    seed env 2
    let keys = [tshow index | index <- [1 .. 12]]
    enqueue env (concat [replicate 6 (job "declpool" suffix) | suffix <- keys])
    _ <- mapConcurrently (const (claimAs env)) [1 .. 8 :: Int]
    overCap <- runM env $ do
      schema <- getSchema
      execQuery
        ( "SELECT concurrency_key FROM "
            <> jobQueueTable schema concurrencyTable
            <> " WHERE concurrency_key IS NOT NULL GROUP BY concurrency_key"
            <> " HAVING count(*) FILTER (WHERE claimed_by IS NOT NULL) > 2"
        )
        []
        (col "concurrency_key" CText)
    (overCap :: [Text]) `shouldBe` []
    -- An uncontended drain fills every key to the cap.
    void (drainWith (claimAs env))
    traverse_ (\suffix -> inFlight env (fullKey "declpool" suffix) `shouldReturn` Just 2) keys

  it "a full concurrency key does not starve admissible ungrouped jobs behind it" $ \env -> do
    -- Fill the hot key to its cap, then flood it past the bounded candidate window.
    -- The cold job on another key still claims.
    seed env 2
    enqueue env (replicate 150 (job "declpool" "hot"))
    filled <- claimAs env
    length filled `shouldBe` 2
    enqueue env [job "declpool" "cold"]
    _ <- claimAs env
    inFlight env (fullKey "declpool" "cold") `shouldReturn` Just 1

  it "a full concurrency key does not starve admissible grouped jobs behind it" $ \env -> do
    -- Each blocked group takes a slot in the bounded window. A flood of groups on
    -- one full key leaves room for a cold group behind them.
    seed env 2
    enqueue env [groupedJob ("hotg-" <> tshow index) "declpool" "ghot" | index <- [1 .. 150]]
    filled <- claimAs env
    length filled `shouldBe` 2
    enqueue env [groupedJob "coldg" "declpool" "gcold"]
    _ <- claimAs env
    inFlight env (fullKey "declpool" "gcold") `shouldReturn` Just 1

  it "gates a group on the row it would claim" $ \env -> do
    -- A failed job keeps the head of its group's line. The head gate judges that
    -- row. Here the fresh low-id sibling sits on a full key.
    seed env 1
    enqueue env [job "declpool" "gfresh"]
    filled <- claimAs env
    length filled `shouldBe` 1
    enqueue env [groupedJob "rg" "declpool" "gfresh"]
    enqueue env [groupedJob "rg" "declpool" "gretry"]
    markAttempted env (fullKey "declpool" "gretry")
    _ <- claimAs env
    inFlight env (fullKey "declpool" "gretry") `shouldReturn` Just 1

  it "keeps a group whose batch still has a claimable row under a blocked retry" $ \env -> do
    -- The batch is taken attempts-first and cut by (priority, id). It yields a
    -- claim when its lowest-id row is admissible. The gate judges that row.
    seed env 1
    enqueue env [job "declpool" "bhot"]
    filled <- claimAs env
    length filled `shouldBe` 1
    enqueue env [groupedJob "bg" "declpool" "bfree"]
    enqueue env [groupedJob "bg" "declpool" "bhot"]
    markAttempted env (fullKey "declpool" "bhot")
    claimed <- runM env (HL.claimNextVisibleJobsBatched 2 100 60) :: IO [NE.NonEmpty (JobRead CLPayload)]
    map payload (concatMap NE.toList claimed) `shouldBe` [CLPayload "declpool:bfree"]

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Property tests for per-job concurrency limits: a deterministic exact-model
-- sweep over a job lifecycle (insert, claim, ack, retry, override, prune,
-- reconcile), a concurrent never-over-admit check, and a grouped drain-to-empty
-- check. Each model key is a seeded pool driven through a single suffix. Claims
-- are attributed.
module Arbiter.Test.ConcurrencyModel
  ( concurrencyModelSpec
  ) where

import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyTable)
import Arbiter.Core.Concurrency.Stats (ConcurrencyPolicyUpdate (..))
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema (jobQueueTable)
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , JobRead
  , defaultGroupedJob
  , defaultJob
  , payload
  , setDedupKey
  , setMaxAttempts
  )
import Arbiter.Core.MonadArbiter (HasRegistry)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (foldM_, void)
import Data.Foldable (for_, traverse_)
import Data.Int (Int32)
import Data.List.NonEmpty qualified as NE
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID.Types qualified as UUID
import Database.PostgreSQL.Simple qualified as PG
import Hedgehog
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import Test.Hspec
import UnliftIO.Async (mapConcurrently)

import Arbiter.Test.ConcurrencyLimit (CLPayload (..), CLReg, concurrencyTable)
import Arbiter.Test.Setup (execute_, seedConcurrencyPoolSQL)

-- A worker id that attributes claims.
worker :: UUID.UUID
worker = UUID.fromWords 0 0 0 11

-- Pools with a fixed seeded limit. The model is keyed by pool prefix.
modelPools :: [(Text, Int)]
modelPools = [("mx", 1), ("my", 2), ("mz", 3)]

-- A single suffix per pool. Each pool drives one count-row key @prefix:k@.
poolSuffix :: Text
poolSuffix = "k"

storedKey :: Text -> Text
storedKey prefix = prefix <> ":" <> poolSuffix

limitOf :: Text -> Int
limitOf prefix = fromMaybe 1 (lookup prefix modelPools)

-- A claim batch larger than any per-key gate.
claimBatch :: Int
claimBatch = 500

-- | The concurrency state-machine properties, run against any backend.
concurrencyModelSpec
  :: forall sm
   . (HasRegistry sm CLReg)
  => (forall a. sm a -> IO a)
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Text
  -> Spec
concurrencyModelSpec run withConn schema = do
  it "the lifecycle keeps in_flight exact for every key" $
    check (prop_model run withConn schema) >>= (`shouldBe` True)
  it "concurrent claimers never admit more than a key's limit" $
    check (prop_concurrent run withConn schema) >>= (`shouldBe` True)
  it "grouped jobs always drain, whatever the batch size and key layout" $
    check (prop_groupedDrain run withConn schema) >>= (`shouldBe` True)

-- Pure reference model.

data KS = KS
  { ksLimit :: Int
  , ksPending :: Int
  , ksClaimed :: Int
  }

-- Count rows, keyed by pool prefix.
type MM = Map Text KS

-- Pool overrides on the policy, keyed by prefix.
type Ov = Map Text Int

eff :: Ov -> Text -> KS -> Int
eff overrides prefix state = fromMaybe (ksLimit state) (Map.lookup prefix overrides)

ensure :: Text -> MM -> MM
ensure prefix = Map.insertWith (\_ old -> old) prefix (KS (limitOf prefix) 0 0)

data Op
  = OInsert Text Int
  | OClaim
  | OAck Text Int
  | ORetry Text Int
  | OOverride Text (Maybe Int)
  | OPrune
  | OReconcile
  | -- | Dedup-replace a fresh job from one pool's key onto another's. The third
    -- field is a dedup key that is unique per op.
    OMove Text Text Text
  deriving stock (Show)

genOps :: Gen [Op]
genOps =
  Gen.list (Range.linear 1 25) $
    Gen.frequency
      [ (3, OInsert <$> genKey <*> Gen.int (Range.linear 1 4))
      , (4, pure OClaim)
      , (2, OAck <$> genKey <*> Gen.int (Range.linear 1 4))
      , (2, ORetry <$> genKey <*> Gen.int (Range.linear 1 4))
      , (1, OOverride <$> genKey <*> Gen.maybe (Gen.int (Range.linear 0 4)))
      , (1, pure OPrune)
      , (1, pure OReconcile)
      , (1, OMove <$> genKey <*> genKey <*> genDedup)
      ]
  where
    genKey = Gen.element (map fst modelPools)
    genDedup = Gen.text (Range.singleton 12) Gen.alphaNum

-- Apply a non-claim op to the pure model. Claim is handled with the live result.
applyModel :: Op -> (MM, Ov) -> (MM, Ov)
applyModel operation (model, overrides) = case operation of
  OInsert prefix count -> (Map.adjust (\state -> state {ksPending = ksPending state + count}) prefix (ensure prefix model), overrides)
  OAck prefix count -> (Map.adjust (\state -> state {ksClaimed = ksClaimed state - min count (ksClaimed state)}) prefix model, overrides)
  ORetry prefix count ->
    ( Map.adjust
        ( \state ->
            let moved = min count (ksClaimed state)
             in state {ksClaimed = ksClaimed state - moved, ksPending = ksPending state + moved}
        )
        prefix
        model
    , overrides
    )
  OOverride prefix newLimit -> (model, maybe (Map.delete prefix) (Map.insert prefix) newLimit overrides)
  OPrune -> (Map.filter (\state -> ksPending state + ksClaimed state /= 0) model, overrides)
  OReconcile -> (model, overrides)
  OClaim -> (model, overrides)
  -- The moved job is unclaimed. Pending gains one on the destination and the
  -- source keeps a drained count row.
  OMove source destination _ ->
    ( Map.adjust (\state -> state {ksPending = ksPending state + 1}) destination (ensure destination (ensure source model))
    , overrides
    )

-- Per-key admissions a claim grants.
claimDeltas :: Ov -> MM -> Map Text Int
claimDeltas overrides = Map.mapWithKey (\prefix state -> min (ksPending state) (max 0 (eff overrides prefix state - ksClaimed state)))

prop_model
  :: (HasRegistry sm CLReg)
  => (forall a. sm a -> IO a)
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Text
  -> Property
prop_model run withConn schema = withTests 60 $ property $ do
  ops <- forAll genOps
  evalIO (resetState withConn schema)
  foldM_ (step run withConn schema) (Map.empty, Map.empty, Map.empty) ops

-- Live jobs currently held claimed, grouped by key.
type Held = Map Text [JobRead CLPayload]

step
  :: (HasRegistry sm CLReg)
  => (forall a. sm a -> IO a)
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Text
  -> (MM, Ov, Held)
  -> Op
  -> PropertyT IO (MM, Ov, Held)
step run withConn schema (model, overrides, held) operation = do
  -- Non-claim branches run their effect and defer the model update to applyModel.
  let done held' = let (nextModel, nextOverrides) = applyModel operation (model, overrides) in pure (nextModel, nextOverrides, held')
  (model', overrides', held') <- case operation of
    OInsert prefix count -> do
      let job = setMaxAttempts (Just 1000) $ defaultJob (CLPayload prefix)
      evalIO (void (run (HL.insertJobsBatch (replicate count job)) :: IO [JobRead CLPayload]))
      done held
    OClaim -> do
      claimed <- evalIO (run (HL.claimNextVisibleJobsAs claimBatch 60 worker) :: IO [JobRead CLPayload])
      let grouped = groupByKey claimed
          expected = claimDeltas overrides model
      -- The gate admitted exactly the model's per-key free slots.
      for_ (map fst modelPools) $ \prefix ->
        Map.findWithDefault 0 prefix (Map.map length grouped) === Map.findWithDefault 0 prefix expected
      let nextModel =
            Map.mapWithKey
              ( \prefix state ->
                  let admitted = Map.findWithDefault 0 prefix expected
                   in state {ksClaimed = ksClaimed state + admitted, ksPending = ksPending state - admitted}
              )
              model
      pure (nextModel, overrides, Map.unionWith (++) held grouped)
    OAck prefix count -> do
      let (toAck, rest) = splitAt count (Map.findWithDefault [] prefix held)
      evalIO (run (traverse_ HL.ackJob toAck))
      done (Map.insert prefix rest held)
    ORetry prefix count -> do
      let (toRetry, rest) = splitAt count (Map.findWithDefault [] prefix held)
      evalIO (run (traverse_ (HL.updateJobForRetry 0 "model retry") toRetry))
      done (Map.insert prefix rest held)
    OOverride prefix newLimit -> do
      evalIO
        (void (run (HL.updateConcurrencyPolicyOverrides prefix (ConcurrencyPolicyUpdate (Just (fromIntegral <$> newLimit))))))
      done held
    OPrune -> do
      evalIO (void (run HL.pruneConcurrencyKeys))
      done held
    OReconcile -> do
      evalIO (void (run HL.reconcileConcurrencyCounts))
      done held
    OMove source destination dedup -> do
      let mkJob prefix = setDedupKey (Just (ReplaceDuplicate dedup)) $ setMaxAttempts (Just 1000) $ defaultJob (CLPayload prefix)
      -- Seed the source key, then dedup-replace onto the destination.
      evalIO (void (run (HL.insertJobsBatch [mkJob source]) :: IO [JobRead CLPayload]))
      evalIO (void (run (HL.insertJobsBatch [mkJob destination]) :: IO [JobRead CLPayload]))
      done held
  -- A count row exists from the first insert until a prune drops it.
  stored <- evalIO (readCounts withConn schema)
  for_ (map fst modelPools) $ \prefix ->
    case Map.lookup prefix model' of
      Just state ->
        Map.lookup (storedKey prefix) stored === Just (fromIntegral (ksClaimed state))
      Nothing -> Map.lookup (storedKey prefix) stored === Nothing
  pure (model', overrides', held')

prop_concurrent
  :: (HasRegistry sm CLReg)
  => (forall a. sm a -> IO a)
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Text
  -> Property
prop_concurrent run withConn schema = withTests 30 $ property $ do
  lim <- forAll (Gen.int (Range.linear 1 3))
  extra <- forAll (Gen.int (Range.linear 1 6))
  claimers <- forAll (Gen.int (Range.linear 2 8))
  evalIO (resetState withConn schema)
  let prefix = "mx"
      jobCount = lim + extra
      job = setMaxAttempts (Just 1000) $ defaultJob (CLPayload prefix)
  evalIO (withConn $ \conn -> seedPool conn schema prefix lim)
  evalIO (void (run (HL.insertJobsBatch (replicate jobCount job)) :: IO [JobRead CLPayload]))
  results <-
    evalIO
      (mapConcurrently (const (run (HL.claimNextVisibleJobsAs claimBatch 60 worker) :: IO [JobRead CLPayload])) [1 .. claimers])
  let total = sum (map length results)
  -- A key's cap holds under contention.
  assert (total <= lim)
  -- An uncontended follow-up claim fills the key to the cap. The contended total
  -- is not deterministic.
  more <- evalIO (run (HL.claimNextVisibleJobsAs claimBatch 60 worker) :: IO [JobRead CLPayload])
  let claimed = total + length more
  claimed === lim
  evalIO (void (run HL.reconcileConcurrencyCounts))
  stored <- evalIO (readCounts withConn schema)
  Map.lookup (storedKey prefix) stored === Just (fromIntegral claimed)

-- | One pool's key is pinned at its cap by a job that is never acked. Every group
-- with no job on that key still drains. Generated over batch size, per-poll slot
-- budget, group size, and key layout.
prop_groupedDrain
  :: (HasRegistry sm CLReg)
  => (forall a. sm a -> IO a)
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Text
  -> Property
prop_groupedDrain run withConn schema = withTests 40 $ property $ do
  perGroup <- forAll (Gen.int (Range.linear 1 3))
  batchSize <- forAll (Gen.int (Range.linear 1 3))
  -- Small enough for the slot budget to bind.
  maxBatches <- forAll (Gen.int (Range.linear 1 3))
  keys <- forAll (Gen.list (Range.linear 2 16) (Gen.element (map fst modelPools)))
  evalIO (resetState withConn schema)
  -- Pin the hot pool at its cap with a claim that is never acked.
  evalIO (void (run (HL.insertJobsBatch [defaultJob (CLPayload hotPool)]) :: IO [JobRead CLPayload]))
  pinned <- evalIO (run (HL.claimNextVisibleJobsAs claimBatch 600 worker) :: IO [JobRead CLPayload])
  length pinned === limitOf hotPool
  let groupOf index = "dg" <> T.pack (show (index `div` perGroup :: Int))
      tagged = [(groupOf index, prefix) | (index, prefix) <- zip [0 :: Int ..] keys]
      jobs = [setMaxAttempts (Just 1000) (defaultGroupedJob groupKey (CLPayload prefix)) | (groupKey, prefix) <- tagged]
      -- A group with no job on the pinned key is cold.
      cold =
        [ groupKey
        | groupKey <- Set.toList (Set.fromList (map fst tagged))
        , all ((/= hotPool) . snd) (filter ((== groupKey) . fst) tagged)
        ]
      claimSql = Ops.mkJobStatements (Proxy :: Proxy CLPayload) schema concurrencyTable batchSize 0 60 worker
      round_ =
        run
          ( do
              claimed <- Ops.claimJobsBatchedCached claimSql maxBatches
              let got = concatMap NE.toList (claimed :: [NE.NonEmpty (JobRead CLPayload)])
              traverse_ HL.ackJob got
              pure (length got)
          )
      loop remaining
        | remaining <= (0 :: Int) = pure ()
        | otherwise = round_ >>= \claimedCount -> if claimedCount == 0 then pure () else loop (remaining - 1)
  evalIO (void (run (HL.insertJobsBatch jobs) :: IO [JobRead CLPayload]))
  evalIO (loop (length jobs + 4))
  left <- evalIO (remainingGroups withConn schema)
  filter (`elem` cold) left === []

-- Helpers.

-- | The pool pinned at its cap by 'prop_groupedDrain'.
hotPool :: Text
hotPool = "mx"

-- | Group keys that still have rows.
remainingGroups :: (forall a. (PG.Connection -> IO a) -> IO a) -> Text -> IO [Text]
remainingGroups withConn schema =
  withConn $ \conn -> do
    rows <-
      PG.query_
        conn
        (stmt ("SELECT DISTINCT group_key FROM " <> jobQueueTable schema concurrencyTable <> " WHERE group_key IS NOT NULL"))
    pure (map PG.fromOnly rows)

groupByKey :: [JobRead CLPayload] -> Map Text [JobRead CLPayload]
groupByKey = foldl' (\acc job -> Map.insertWith (++) (keyOf job) [job] acc) Map.empty
  where
    keyOf job = let CLPayload prefix = payload job in prefix

resetState :: (forall a. (PG.Connection -> IO a) -> IO a) -> Text -> IO ()
resetState withConn schema =
  withConn $ \conn -> do
    execute_ conn ("DELETE FROM " <> jobQueueTable schema concurrencyTable)
    execute_ conn ("TRUNCATE " <> arbiterConcurrencyTable schema)
    traverse_ (uncurry (seedPool conn schema)) modelPools

seedPool :: PG.Connection -> Text -> Text -> Int -> IO ()
seedPool conn schema prefix lim =
  traverse_ (execute_ conn) (seedConcurrencyPoolSQL schema prefix (fromIntegral lim))

readCounts :: (forall a. (PG.Connection -> IO a) -> IO a) -> Text -> IO (Map Text Int32)
readCounts withConn schema =
  withConn $ \conn -> do
    rows <-
      PG.query_
        conn
        (stmt ("SELECT concurrency_key, in_flight FROM " <> arbiterConcurrencyTable schema))
    pure (Map.fromList rows)

stmt :: Text -> PG.Query
stmt = fromString . T.unpack

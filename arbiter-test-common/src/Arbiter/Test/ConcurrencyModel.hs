{-# LANGUAGE DataKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Property tests for per-job concurrency limits: a deterministic exact-model
-- sweep over a job lifecycle (insert, claim, ack, retry, override, prune,
-- reconcile) and a concurrent never-over-admit check. Each model key is a seeded
-- pool driven through a single suffix. Claims are attributed so the gate engages.
module Arbiter.Test.ConcurrencyModel
  ( concurrencyModelSpec
  ) where

import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyTable)
import Arbiter.Core.Concurrency.Stats (ConcurrencyPolicyUpdate (..))
import Arbiter.Core.HasArbiterSchema (HasRegistry)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema (jobQueueTable)
import Arbiter.Core.Job.Types (DedupKey (..), JobRead, dedupKey, defaultJob, maxAttempts, payload)
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Control.Monad (foldM_, void)
import Data.Foldable (for_, traverse_)
import Data.Int (Int32)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
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

-- A worker id so claims are attributed (the gate counts off claimed_by).
worker :: UUID.UUID
worker = UUID.fromWords 0 0 0 11

-- Pools with a fixed seeded limit. The model is keyed by pool prefix.
modelPools :: [(Text, Int)]
modelPools = [("mx", 1), ("my", 2), ("mz", 3)]

-- A single suffix per pool, so each pool drives one count-row key @prefix:k@.
poolSuffix :: Text
poolSuffix = "k"

storedKey :: Text -> Text
storedKey prefix = prefix <> ":" <> poolSuffix

limitOf :: Text -> Int
limitOf k = fromMaybe 1 (lookup k modelPools)

-- Large enough that the claim batch never binds before the per-key gate.
claimBatch :: Int
claimBatch = 500

concurrencyModelSpec
  :: forall sm
   . (HasRegistry sm CLReg, MonadArbiter sm)
  => (forall a. sm a -> IO a)
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Text
  -> Spec
concurrencyModelSpec run withConn schema = do
  it "the lifecycle keeps in_flight exact for every key" $
    check (prop_model run withConn schema) >>= (`shouldBe` True)
  it "concurrent claimers never admit more than a key's limit" $
    check (prop_concurrent run withConn schema) >>= (`shouldBe` True)

-- Pure reference model.

data KS = KS
  { ksLimit :: Int
  , ksPending :: Int
  , ksClaimed :: Int
  }

-- Count rows, keyed by pool prefix.
type MM = Map Text KS

-- Pool overrides (on the policy, not the count row), keyed by prefix.
type Ov = Map Text Int

eff :: Ov -> Text -> KS -> Int
eff ov k ks = fromMaybe (ksLimit ks) (Map.lookup k ov)

ensure :: Text -> MM -> MM
ensure k = Map.insertWith (\_ old -> old) k (KS (limitOf k) 0 0)

data Op
  = OInsert Text Int
  | OClaim
  | OAck Text Int
  | ORetry Text Int
  | OOverride Text (Maybe Int)
  | OPrune
  | OReconcile
  | -- | Dedup-replace a fresh job from one pool's key onto another's, firing the
    -- update trigger's key-move branch. The third field is the dedup key, unique
    -- per op so an earlier mover is never re-moved.
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
applyModel op (m, ov) = case op of
  OInsert k n -> (Map.adjust (\ks -> ks {ksPending = ksPending ks + n}) k (ensure k m), ov)
  OAck k n -> (Map.adjust (\ks -> ks {ksClaimed = ksClaimed ks - min n (ksClaimed ks)}) k m, ov)
  ORetry k n ->
    ( Map.adjust
        (\ks -> let x = min n (ksClaimed ks) in ks {ksClaimed = ksClaimed ks - x, ksPending = ksPending ks + x})
        k
        m
    , ov
    )
  OOverride k mo -> (m, maybe (Map.delete k) (Map.insert k) mo ov)
  OPrune -> (Map.filter (\ks -> ksPending ks + ksClaimed ks /= 0) m, ov)
  OReconcile -> (m, ov)
  OClaim -> (m, ov)
  -- The moved job is unclaimed (in_flight delta 0 on both keys), so only pending
  -- shifts: net +1 on the destination, the source left with a drained count row.
  OMove k1 k2 _ -> (Map.adjust (\ks -> ks {ksPending = ksPending ks + 1}) k2 (ensure k2 (ensure k1 m)), ov)

-- Per-key admissions a claim grants: min pending (cap - claimed).
claimDeltas :: Ov -> MM -> Map Text Int
claimDeltas ov = Map.mapWithKey (\k ks -> min (ksPending ks) (max 0 (eff ov k ks - ksClaimed ks)))

prop_model
  :: (HasRegistry sm CLReg, MonadArbiter sm)
  => (forall a. sm a -> IO a)
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Text
  -> Property
prop_model run withConn schema = withTests 60 $ property $ do
  ops <- forAll genOps
  evalIO (resetState withConn schema)
  foldM_ (step run withConn schema) (Map.empty, Map.empty, Map.empty) ops

-- Threaded state: the pure model, pool overrides, and the live jobs we currently
-- hold claimed, grouped by key, so ack and retry can target real rows.
type Held = Map Text [JobRead CLPayload]

step
  :: (HasRegistry sm CLReg, MonadArbiter sm)
  => (forall a. sm a -> IO a)
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Text
  -> (MM, Ov, Held)
  -> Op
  -> PropertyT IO (MM, Ov, Held)
step run withConn schema (m, ov, held) op = do
  -- Non-claim branches run their effect and defer the model update to applyModel.
  let done held' = let (m2, ov2) = applyModel op (m, ov) in pure (m2, ov2, held')
  (m', ov', held') <- case op of
    OInsert k n -> do
      let j = (defaultJob (CLPayload k)) {maxAttempts = Just 1000}
      evalIO (void (run (HL.insertJobsBatch (replicate n j)) :: IO [JobRead CLPayload]))
      done held
    OClaim -> do
      claimed <- evalIO (run (HL.claimNextVisibleJobsAs claimBatch 60 worker) :: IO [JobRead CLPayload])
      let grouped = groupByKey claimed
          expected = claimDeltas ov m
      -- The gate admitted exactly the model's per-key free slots.
      for_ (map fst modelPools) $ \k ->
        Map.findWithDefault 0 k (Map.map length grouped) === Map.findWithDefault 0 k expected
      let m2 =
            Map.mapWithKey
              ( \k ks ->
                  let a = Map.findWithDefault 0 k expected
                   in ks {ksClaimed = ksClaimed ks + a, ksPending = ksPending ks - a}
              )
              m
      pure (m2, ov, Map.unionWith (++) held grouped)
    OAck k n -> do
      let (toAck, rest) = splitAt n (Map.findWithDefault [] k held)
      evalIO (run (traverse_ HL.ackJob toAck))
      done (Map.insert k rest held)
    ORetry k n -> do
      let (toRetry, rest) = splitAt n (Map.findWithDefault [] k held)
      evalIO (run (traverse_ (HL.updateJobForRetry 0 "model retry") toRetry))
      done (Map.insert k rest held)
    OOverride k mo -> do
      evalIO (void (run (HL.updateConcurrencyPolicyOverrides k (ConcurrencyPolicyUpdate (Just (fromIntegral <$> mo))))))
      done held
    OPrune -> do
      evalIO (void (run HL.pruneConcurrencyKeys))
      done held
    OReconcile -> do
      evalIO (void (run HL.reconcileConcurrencyCounts))
      done held
    OMove k1 k2 d -> do
      let mk k = (defaultJob (CLPayload k)) {maxAttempts = Just 1000, dedupKey = Just (ReplaceDuplicate d)}
      -- Seed the source key, then dedup-replace onto the destination, moving the row.
      evalIO (void (run (HL.insertJobsBatch [mk k1]) :: IO [JobRead CLPayload]))
      evalIO (void (run (HL.insertJobsBatch [mk k2]) :: IO [JobRead CLPayload]))
      done held
  -- A count row exists once an insert creates it and stays until a prune drops it,
  -- so a key the model holds has a stored row and one it dropped has none.
  stored <- evalIO (readCounts withConn schema)
  for_ (map fst modelPools) $ \k ->
    case Map.lookup k m' of
      Just ks ->
        Map.lookup (storedKey k) stored === Just (fromIntegral (ksClaimed ks))
      Nothing -> Map.lookup (storedKey k) stored === Nothing
  pure (m', ov', held')

prop_concurrent
  :: (HasRegistry sm CLReg, MonadArbiter sm)
  => (forall a. sm a -> IO a)
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Text
  -> Property
prop_concurrent run withConn schema = withTests 30 $ property $ do
  lim <- forAll (Gen.int (Range.linear 1 3))
  extra <- forAll (Gen.int (Range.linear 1 6))
  claimers <- forAll (Gen.int (Range.linear 2 8))
  evalIO (resetState withConn schema)
  let k = "mx"
      n = lim + extra
      j = (defaultJob (CLPayload k)) {maxAttempts = Just 1000}
  evalIO (withConn $ \c -> seedPool c schema k lim)
  evalIO (void (run (HL.insertJobsBatch (replicate n j)) :: IO [JobRead CLPayload]))
  results <-
    evalIO
      (mapConcurrently (const (run (HL.claimNextVisibleJobsAs claimBatch 60 worker) :: IO [JobRead CLPayload])) [1 .. claimers])
  let total = sum (map length results)
  -- The hard invariant: a key's cap is never breached under contention.
  assert (total <= lim)
  -- Liveness: an uncontended follow-up claim fills the key to exactly the cap, so
  -- the race neither deadlocks nor permanently under-admits. (The contended total
  -- is not deterministic: the count-row-lock winner admits only its SKIP-LOCKED share.)
  more <- evalIO (run (HL.claimNextVisibleJobsAs claimBatch 60 worker) :: IO [JobRead CLPayload])
  let claimed = total + length more
  claimed === lim
  evalIO (void (run HL.reconcileConcurrencyCounts))
  stored <- evalIO (readCounts withConn schema)
  Map.lookup (storedKey k) stored === Just (fromIntegral claimed)

-- Helpers.

groupByKey :: [JobRead CLPayload] -> Map Text [JobRead CLPayload]
groupByKey = foldl' (\acc j -> Map.insertWith (++) (keyOf j) [j] acc) Map.empty
  where
    keyOf j = let CLPayload k = payload j in k

resetState :: (forall a. (PG.Connection -> IO a) -> IO a) -> Text -> IO ()
resetState withConn schema =
  withConn $ \c -> do
    execute_ c ("DELETE FROM " <> jobQueueTable schema concurrencyTable)
    execute_ c ("TRUNCATE " <> arbiterConcurrencyTable schema)
    traverse_ (uncurry (seedPool c schema)) modelPools

seedPool :: PG.Connection -> Text -> Text -> Int -> IO ()
seedPool c schema prefix lim =
  traverse_ (execute_ c) (seedConcurrencyPoolSQL schema prefix (fromIntegral lim))

readCounts :: (forall a. (PG.Connection -> IO a) -> IO a) -> Text -> IO (Map Text Int32)
readCounts withConn schema =
  withConn $ \c -> do
    rows <-
      PG.query_
        c
        (stmt ("SELECT concurrency_key, in_flight FROM " <> arbiterConcurrencyTable schema))
    pure (Map.fromList rows)

stmt :: Text -> PG.Query
stmt = fromString . T.unpack

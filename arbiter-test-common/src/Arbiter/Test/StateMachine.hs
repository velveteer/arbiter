{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

-- | Parameterized stateful property-based tests for the core job engine, using
-- hedgehog. Works against any 'MonadArbiter' backend via a passed runner and a
-- raw-connection accessor.
--
-- Generates random sequences of engine operations against the real database
-- (insert at varying priority, scheduled insert, claim, ack, cancel, suspend,
-- resume, promote, retry, extend-lease, move-to-DLQ, retry-from-DLQ, batch
-- insert/cancel/DLQ, dedup insert, and the reaper) and checks the core
-- invariants after every step:
--
--   * at most one in-flight job per group (the serialization guarantee)
--   * no job exceeds its @max_attempts@
--   * no duplicate live @dedup_key@
--   * no concurrency key has more claimed-in-flight jobs than its effective cap
--   * every group summary column matches its recompute from the main table
--
-- Generated inserts also carry rate-limit keys and concurrency slots.
--
-- A second property ('prop_concurrent') generates N independent branches of
-- self-contained actions, runs them concurrently under a gap-free serialization
-- detector (a row trigger that fires inside every claim), then quiesces with a
-- reaper tick and asserts both the detector log and the full settled oracle are
-- clean. All contention is in the generated, shrinkable model.
-- Deterministic guards back the known-critical races.
module Arbiter.Test.StateMachine
  ( stateMachineSpec
  , SMPayload (..)
  , holViolTbl
  , holInstallSql
  , holRemoveSql
  ) where

import Arbiter.Core.Concurrency.Spec
  ( HasConcurrency (..)
  , concurrencyBy
  , concurrencyByCase
  , concurrencyPool
  , noConcurrency
  )
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema.Groups (inFlightPredicate)
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , JobRead
  , JobWrite
  , attempts
  , defaultGroupedJob
  , defaultJob
  , defaultMaxAttempts
  , notVisibleUntil
  , primaryKey
  , priority
  , setDedupKey
  , setMaxAttempts
  , setNotVisibleUntil
  , setPayload
  , setPriority
  , suspended
  )
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables, TableForPayload)
import Arbiter.Core.RateLimit.Schema (toPolicyRow, upsertPolicyRowSQL)
import Arbiter.Core.RateLimit.Spec
  ( HasRateLimit (..)
  , Policy
  , limitBy
  , limitByCase
  , noLimit
  , tokenBucket
  )
import Barbies qualified as B
import Control.Concurrent (threadDelay)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Exception (SomeException, finally, throwIO)
import Control.Monad (replicateM_, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.Foldable (traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int32, Int64)
import Data.Kind (Type)
import Data.List (isInfixOf, nub)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromJust, isJust, listToMaybe)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock (NominalDiffTime, UTCTime, addUTCTime, getCurrentTime)
import Data.UUID.Types qualified as UUID
import Database.PostgreSQL.Simple (Only (..))
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import GHC.TypeLits (KnownSymbol)
import Hedgehog
import Hedgehog.Gen qualified as Gen
import Hedgehog.Range qualified as Range
import System.Timeout (timeout)
import Test.Hspec
import UnliftIO (MonadUnliftIO, tryAny)
import UnliftIO.Async (async, mapConcurrently, mapConcurrently_, wait)

import Arbiter.Test.Setup (execute_, seedConcurrencyPoolSQL)

-- | Constraints every HL-issuing helper in this module needs.
type ArbiterC m =
  ( KnownSymbol (TableForPayload SMPayload (RegistryOf m))
  , MonadArbiter m
  , MonadUnliftIO m
  , RegistryTables (RegistryOf m)
  )

-- ---------------------------------------------------------------------------
-- Model
-- ---------------------------------------------------------------------------

-- | Live main-queue jobs and DLQ jobs, each keyed by its id and carrying its
-- group. Generates valid command targets. The invariants are checked against
-- the database.
data Model (v :: Type -> Type) = Model
  { mLive :: Map (Var Int64 v) (Maybe Text)
  , mDlq :: Map (Var Int64 v) (Maybe Text)
  }

initialModel :: Model v
initialModel = Model Map.empty Map.empty

-- ---------------------------------------------------------------------------
-- Raw SQL string builders (schema/table threaded in)
-- ---------------------------------------------------------------------------

-- | Names of the head-of-line detector's table, function, and trigger.
holViolTbl, holFn, holTrigger :: Text -> Text -> Text
holViolTbl schema table = schema <> "." <> table <> "_hol_violations"
holFn schema table = schema <> ".detect_hol_" <> table <> "_fn"
holTrigger _ table = "detect_hol_" <> table

-- | DDL to install the gap-free HOL detector. A row trigger logs a violation
-- when a job becomes in-flight while another @attempts > 0@ job in its group
-- already is. The @attempts > 0@ filter excludes scheduled jobs.
holInstallSql :: Text -> Text -> [Text]
holInstallSql schema table =
  [ "SET client_min_messages TO warning"
  , "CREATE TABLE IF NOT EXISTS " <> holViolTbl schema table <> " (group_key TEXT NOT NULL, job_id BIGINT NOT NULL)"
  , "TRUNCATE " <> holViolTbl schema table
  , "CREATE OR REPLACE FUNCTION "
      <> holFn schema table
      <> "() RETURNS TRIGGER AS $t$ BEGIN IF NEW.not_visible_until > NOW()"
      <> " AND NOT NEW.suspended AND NEW.attempts > 0 AND NEW.group_key IS NOT NULL"
      <> " AND EXISTS (SELECT 1 FROM "
      <> tbl
      <> " WHERE group_key = NEW.group_key AND id <> NEW.id"
      <> " AND not_visible_until > NOW() AND NOT suspended AND attempts > 0)"
      <> " THEN INSERT INTO "
      <> holViolTbl schema table
      <> " (group_key, job_id) VALUES (NEW.group_key, NEW.id); END IF; RETURN NULL; END; $t$ LANGUAGE plpgsql"
  , "DROP TRIGGER IF EXISTS " <> holTrigger schema table <> " ON " <> tbl
  , "CREATE TRIGGER "
      <> holTrigger schema table
      <> " AFTER UPDATE ON "
      <> tbl
      <> " FOR EACH ROW EXECUTE FUNCTION "
      <> holFn schema table
      <> "()"
  ]
  where
    tbl = schema <> "." <> table

-- | DDL to remove the HOL detector trigger, function, and violations table.
holRemoveSql :: Text -> Text -> [Text]
holRemoveSql schema table =
  [ "SET client_min_messages TO warning"
  , "DROP TRIGGER IF EXISTS " <> holTrigger schema table <> " ON " <> schema <> "." <> table
  , "DROP FUNCTION IF EXISTS " <> holFn schema table <> "()"
  , "DROP TABLE IF EXISTS " <> holViolTbl schema table
  ]

-- ---------------------------------------------------------------------------
-- Invariant checks (queried from the database after each command)
-- ---------------------------------------------------------------------------

-- Per-step check. The HOL detector is read after a parallel run.
checkInvariants
  :: (MonadIO m, MonadTest m)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> m ()
checkInvariants = checkInvariantsL "?"

checkInvariantsL
  :: (MonadIO m, MonadTest m)
  => String
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> m ()
checkInvariantsL lbl schema table withConn = do
  violations <-
    evalIO $
      mconcat
        <$> sequence
          [ exactViolations schema table withConn
          , orphanViolations schema table withConn
          , driftViolations schema table withConn
          ]
  map (("[after " <> lbl <> "] ") <>) violations === []

-- | First @id@ of a query, or Nothing if it returns no rows.
firstId :: (forall a. (PG.Connection -> IO a) -> IO a) -> Text -> IO (Maybe Int64)
firstId withConn sql = withConn $ \conn ->
  listToMaybe . map fromOnly <$> PG.query_ conn (fromString (T.unpack sql))

-- | A scalar @count(*)@ query through the raw-connection accessor.
countQuery :: (forall a. (PG.Connection -> IO a) -> IO a) -> Text -> IO Int64
countQuery withConn sql = withConn $ \conn -> do
  [Only count] <- PG.query_ conn (fromString (T.unpack sql))
  pure count

-- | Lock one count row @FOR UPDATE@, like a claimer holding the key.
lockConcurrencyKey :: PG.Connection -> Text -> Text -> IO ()
lockConcurrencyKey conn concTbl key =
  void
    ( PG.query_
        conn
        (fromString (T.unpack ("SELECT 1 FROM " <> concTbl <> " WHERE concurrency_key = '" <> key <> "' FOR UPDATE")))
        :: IO [Only Int64]
    )

truncateHol :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
truncateHol schema table withConn = withConn $ \conn ->
  void $ PG.execute_ conn (fromString (T.unpack ("TRUNCATE " <> holViolTbl schema table)))

-- Test schema and table names are plain lowercase identifiers. The SQL
-- interpolates them unquoted.
queryViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
queryViolations schema table withConn =
  (<>) <$> exactViolations schema table withConn <*> driftViolations schema table withConn

-- | Summary-drift oracle. Full-join the stored @_groups@ row against a fresh
-- recompute and report any column that disagrees.
driftViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
driftViolations schema table withConn = withConn $ \conn -> do
  rows <- PG.query_ conn (toQuery oracleSql)
  pure ["group " <> T.unpack groupName <> " summary drift: " <> T.unpack cols | (groupName, cols) <- rows]
  where
    toQuery = fromString . T.unpack
    tbl = schema <> "." <> table
    groupsTbl = schema <> "." <> table <> "_groups"
    -- NOW() is evaluated once per statement. The recompute filters mirror the
    -- trigger definitions.
    oracleSql =
      "SELECT group_name, drift FROM (SELECT COALESCE(summary.group_key, expected.group_key) AS group_name, concat_ws(', '"
        <> ", CASE WHEN summary.group_key IS NULL THEN 'missing summary row' END"
        <> ", CASE WHEN expected.group_key IS NULL THEN 'orphan summary row' END"
        <> ", CASE WHEN summary.min_priority IS DISTINCT FROM expected.min_priority THEN 'min_priority' END"
        <> ", CASE WHEN summary.min_id IS DISTINCT FROM expected.min_id THEN 'min_id' END"
        <> ", CASE WHEN summary.job_count IS DISTINCT FROM expected.job_count THEN 'job_count' END"
        <> ", CASE WHEN summary.ready_count IS DISTINCT FROM expected.ready_count THEN 'ready_count' END"
        <> ", CASE WHEN summary.next_due IS DISTINCT FROM expected.next_due THEN 'next_due' END"
        <> ", CASE WHEN summary.in_flight_until IS DISTINCT FROM expected.in_flight_until THEN 'in_flight_until' END"
        <> ") AS drift FROM (SELECT group_key, min_priority, min_id, job_count, ready_count, next_due, in_flight_until FROM "
        <> groupsTbl
        <> " WHERE job_count > 0) summary FULL OUTER JOIN (SELECT group_key"
        <> ", MIN(priority)::int AS min_priority"
        <> ", (MIN(ARRAY[priority::bigint, id]))[2]::bigint AS min_id"
        <> ", COUNT(*)::bigint AS job_count"
        <> ", COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended)::bigint AS ready_count"
        <> ", MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended) AS next_due"
        <> ", MAX(not_visible_until) FILTER (WHERE "
        <> inFlightPredicate ""
        <> ") AS in_flight_until FROM "
        <> tbl
        <> " WHERE group_key IS NOT NULL GROUP BY group_key) expected ON summary.group_key = expected.group_key) compared WHERE drift <> ''"

-- | Exact invariant violations, safe to sample live during concurrent churn:
--
--   * serialization: more than one in-flight job per group
--   * attempt bound: a live job past its limit
--   * dedup uniqueness: two live jobs sharing a @dedup_key@
--   * concurrency cap: more claimed jobs on a key than its effective cap
--   * rate-limit integrity: a bucket's tokens outside [0, max]
exactViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
exactViolations schema table withConn = withConn $ \conn -> do
  serial <- PG.query_ conn (fromString (T.unpack serialSql))
  over <- PG.query_ conn (fromString (T.unpack overSql))
  dups <- PG.query_ conn (fromString (T.unpack dupSql))
  conc <- PG.query_ conn (fromString (T.unpack concSql))
  overSpent <- PG.query_ conn (fromString (T.unpack rlSql))
  rlMax <- PG.query_ conn (fromString (T.unpack rlMaxSql))
  serialMsgs <- traverse (diagnoseSerial conn) [groupName | Only groupName <- serial]
  pure $
    serialMsgs
      <> ["job " <> show (jid :: Int64) <> " exceeded its max_attempts" | Only jid <- over]
      <> ["duplicate live dedup_key " <> T.unpack key | Only key <- dups]
      <> ["concurrency cap exceeded for key " <> T.unpack key | Only key <- conc]
      <> ["rate-limit bucket " <> T.unpack key <> " over-spent (negative tokens)" | Only key <- overSpent]
      <> ["rate-limit bucket " <> T.unpack key <> " over-credited (tokens above max)" | Only key <- rlMax]
  where
    tbl = schema <> "." <> table
    concPolicies = schema <> ".arbiter_concurrency_policies"
    rlBuckets = schema <> ".arbiter_rate_limits"
    rlPolicies = schema <> ".arbiter_rate_limit_policies"
    groupsTbl = schema <> "." <> table <> "_groups"
    dma = T.pack (show defaultMaxAttempts)
    -- On a serialization violation, dump the offending group's jobs and summary
    -- row. Timestamps are ms relative to NOW.
    diagnoseSerial conn groupName = do
      jobs <- PG.query conn (fromString (T.unpack jobsSql)) (Only groupName)
      summ <- PG.query conn (fromString (T.unpack summSql)) (Only groupName)
      let jobStr (jid, att, maxAtts, susp, nvu, dedup, upd, att_ms) =
            "{id="
              <> show (jid :: Int64)
              <> " att="
              <> show (att :: Int32)
              <> " max="
              <> show (maxAtts :: Maybe Int32)
              <> " susp="
              <> show (susp :: Bool)
              <> " nvu_ms="
              <> show (nvu :: Maybe Int64)
              <> " dk="
              <> show (dedup :: Maybe Text)
              <> " upd_ago="
              <> show (upd :: Maybe Int64)
              <> " att_ago="
              <> show (att_ms :: Maybe Int64)
              <> "}"
          summStr (jobCount, readyCount, inFlightUntil, nextDue) =
            "jc="
              <> show (jobCount :: Int64)
              <> " rc="
              <> show (readyCount :: Int64)
              <> " ifu_ms="
              <> show (inFlightUntil :: Maybe Int64)
              <> " nd_ms="
              <> show (nextDue :: Maybe Int64)
      pure $
        "multiple in-flight in group "
          <> T.unpack groupName
          <> " | summary["
          <> maybe "MISSING" summStr (listToMaybe summ)
          <> "]"
          <> " | jobs "
          <> unwords (map jobStr jobs)
    jobsSql =
      "SELECT id, attempts, max_attempts, suspended,"
        <> " round(extract(epoch FROM (not_visible_until - NOW())) * 1000)::bigint,"
        <> " dedup_key,"
        <> " round(extract(epoch FROM (NOW() - updated_at)) * 1000)::bigint,"
        <> " round(extract(epoch FROM (NOW() - last_attempted_at)) * 1000)::bigint"
        <> " FROM "
        <> tbl
        <> " WHERE group_key = ? ORDER BY id"
    summSql =
      "SELECT job_count, ready_count,"
        <> " round(extract(epoch FROM (in_flight_until - NOW())) * 1000)::bigint,"
        <> " round(extract(epoch FROM (next_due - NOW())) * 1000)::bigint"
        <> " FROM "
        <> groupsTbl
        <> " WHERE group_key = ?"
    serialSql =
      "SELECT group_key FROM "
        <> tbl
        <> " WHERE group_key IS NOT NULL AND not_visible_until > NOW()"
        <> " AND NOT suspended AND attempts > 0"
        <> " GROUP BY group_key HAVING COUNT(*) > 1"
    overSql =
      "SELECT id FROM "
        <> tbl
        <> " WHERE attempts > COALESCE(max_attempts, "
        <> dma
        <> ")"
    dupSql =
      "SELECT dedup_key FROM "
        <> tbl
        <> " WHERE dedup_key IS NOT NULL GROUP BY dedup_key HAVING COUNT(*) > 1"
    -- More claimed jobs on a key than its effective cap. The effective cap is the
    -- pool override, else the pool default.
    concSql =
      "SELECT job.concurrency_key FROM "
        <> tbl
        <> " job LEFT JOIN "
        <> concPolicies
        <> " policy ON policy.prefix_id = job.concurrency_prefix"
        <> " WHERE job.concurrency_key IS NOT NULL"
        <> " GROUP BY job.concurrency_key, policy.override_limit, policy.default_limit"
        <> " HAVING COUNT(*) FILTER (WHERE job.claimed_by IS NOT NULL)"
        <> " > COALESCE(policy.override_limit, policy.default_limit)"
    -- Negative tokens mean the gate over-spent. The epsilon tolerates float rounding.
    rlSql = "SELECT rate_limit_key FROM " <> rlBuckets <> " WHERE tokens < -0.001"
    -- Tokens above the effective max mean a refill, top-up, or seed skipped the cap.
    rlMaxSql =
      "SELECT bucket.rate_limit_key FROM "
        <> rlBuckets
        <> " bucket JOIN "
        <> rlPolicies
        <> " policy ON policy.prefix_id = bucket.policy_prefix"
        <> " WHERE bucket.tokens > COALESCE(policy.override_max_tokens, policy.default_max_tokens) + 0.001"

-- | A live child whose parent has left the main queue. This is a per-step check
-- for the sequential property. Under concurrency a parent can be acked between
-- the DLQ retry's refuse-check and a child's re-insert, which leaves a benign
-- orphan.
orphanViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
orphanViolations schema table withConn = withConn $ \conn -> do
  orphans <- PG.query_ conn (fromString (T.unpack orphanSql))
  pure ["orphaned child " <> show (jid :: Int64) <> " (parent gone)" | Only jid <- orphans]
  where
    tbl = schema <> "." <> table
    orphanSql =
      "SELECT child.id FROM "
        <> tbl
        <> " child WHERE child.parent_id IS NOT NULL AND NOT EXISTS"
        <> " (SELECT 1 FROM "
        <> tbl
        <> " parent WHERE parent.id = child.parent_id)"

-- ---------------------------------------------------------------------------
-- Commands
-- ---------------------------------------------------------------------------

-- | Worker id for the generated claim paths.
smWorker :: UUID.UUID
smWorker = UUID.fromWords 0 0 0 7

-- | Per-job rate-limit key and concurrency pool a generated insert can carry.
data Extras = Extras (Maybe Text) (Maybe Text)
  deriving stock (Eq, Show)

-- | Seeded pools with a fixed limit.
smConcSlots :: [(Text, Int32)]
smConcSlots = [("cap-a", 1), ("cap-b", 2), ("cap-c", 3)]

-- | One suffix per pool. Each pool drives a single count-row key.
smConcSuffix :: Text
smConcSuffix = "s"

smRateKeys :: [Text]
smRateKeys = ["rk-1", "rk-2"]

genExtras :: (MonadGen g) => g Extras
genExtras = Extras <$> Gen.maybe (Gen.element (map fst smConcSlots)) <*> Gen.maybe (Gen.element smRateKeys)

applyExtras :: Extras -> JobWrite SMPayload -> JobWrite SMPayload
applyExtras (Extras concSlot rateKey) job = setPayload ((Job.payload job) {smConcSlot = concSlot, smRateKey = rateKey}) job

-- | A payload carrying optional concurrency and rate-limit keys.
data SMPayload = SMPayload
  { smMessage :: Text
  , smConcSlot :: Maybe Text
  , smRateKey :: Maybe Text
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

smPayload :: Text -> SMPayload
smPayload text = SMPayload text Nothing Nothing

data SMSlot = SlotNone | SlotA | SlotB | SlotC
  deriving stock (Bounded, Enum, Eq)

instance HasConcurrency SMPayload where
  concurrencyFor = concurrencyByCase slotTag slotSel
    where
      slotTag payload = case smConcSlot payload of
        Just "cap-a" -> SlotA
        Just "cap-b" -> SlotB
        Just "cap-c" -> SlotC
        _ -> SlotNone
      slotSel SlotNone = noConcurrency
      slotSel SlotA = concurrencyBy (concurrencyPool "cap-a" 1) (const smConcSuffix)
      slotSel SlotB = concurrencyBy (concurrencyPool "cap-b" 2) (const smConcSuffix)
      slotSel SlotC = concurrencyBy (concurrencyPool "cap-c" 3) (const smConcSuffix)

data SMRate = RateNone | RateK1 | RateK2
  deriving stock (Bounded, Enum, Eq)

-- | A binding bucket. Capacity 3 with negligible refill over a test run.
smBucket :: Policy
smBucket = tokenBucket "smrl" 3 60

instance HasRateLimit SMPayload where
  rateLimitFor = limitByCase rateTag rateSel
    where
      rateTag payload = case smRateKey payload of
        Just "rk-1" -> RateK1
        Just "rk-2" -> RateK2
        _ -> RateNone
      rateSel RateNone = noLimit
      rateSel RateK1 = limitBy smBucket (const "rk-1")
      rateSel RateK2 = limitBy smBucket (const "rk-2")

-- | Seed the concurrency pools. Idempotent.
seedConcurrencyPools :: Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
seedConcurrencyPools schema withConn = withConn $ \conn ->
  traverse_
    (\(pool, limit) -> traverse_ (void . PG.execute_ conn . fromString . T.unpack) (seedConcurrencyPoolSQL schema pool limit))
    smConcSlots

-- | Seed the rate-limit policy. Idempotent.
seedRateLimitPolicies :: Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
seedRateLimitPolicies schema withConn = withConn $ \conn ->
  void $ PG.execute_ conn (fromString (T.unpack (upsertPolicyRowSQL schema (toPolicyRow smBucket))))

-- | Reset the tables, then re-seed both admission policies.
resetSeeded :: IO () -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
resetSeeded reset schema withConn = do
  reset
  seedConcurrencyPools schema withConn
  seedRateLimitPolicies schema withConn

-- | @Insert group delay priority maxAttempts extras@. Insert a job at a varying
-- priority into an optional group, optionally scheduled into the future. A low
-- @maxAttempts@ lets short claim sequences drive jobs to exhaustion.
data Insert (v :: Type -> Type) = Insert (Maybe Text) (Maybe Int) Int (Maybe Int) Extras
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cInsert
  :: forall gen m sm
   . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cInsert run schema table withConn =
  Command
    ( \_ ->
        Just $
          Insert
            <$> Gen.maybe (Gen.element ["g1", "g2", "g3"])
            <*> Gen.maybe (Gen.int (Range.linear 30 120))
            <*> Gen.int (Range.linear 0 5)
            <*> Gen.maybe (Gen.int (Range.linear 1 3))
            <*> genExtras
    )
    ( \(Insert group delay prio maxAtts extras) -> do
        jid <- evalIO (run (mkInsert (applyExtras extras) group delay prio maxAtts))
        checkInvariants schema table withConn
        pure jid
    )
    [Update $ \model (Insert group _ _ _ _) output -> model {mLive = Map.insert output group (mLive model)}]

mkInsert
  :: forall sm
   . (ArbiterC sm)
  => (JobWrite SMPayload -> JobWrite SMPayload)
  -> Maybe Text
  -> Maybe Int
  -> Int
  -> Maybe Int
  -> sm Int64
mkInsert deco group delay prio maxAtts = do
  nvu <- traverse (\secs -> liftIO (addUTCTime (fromIntegral secs) <$> getCurrentTime)) delay
  let job =
        setMaxAttempts (fromIntegral <$> maxAtts)
          $ setPriority (fromIntegral prio)
          $ setNotVisibleUntil nvu
          $ maybe (defaultJob payload) (`defaultGroupedJob` payload) group
  inserted <- HL.insertJob (deco job)
  pure (primaryKey (fromJust inserted))
  where
    payload = smPayload "sm"

data Claim (v :: Type -> Type) = Claim
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cClaim
  :: forall gen m sm
   . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cClaim run schema table withConn =
  Command
    (\_ -> Just (pure Claim))
    ( \Claim -> do
        ids <- evalIO (run (mkClaim @sm))
        -- Each claimed job is now a live in-flight row.
        live <- evalIO (traverse (isLiveInFlight schema table withConn) ids)
        assert (and live)
        checkInvariants schema table withConn
        pure ids
    )
    [ Ensure $ \_ post Claim ids -> do
        -- Never claim more than the batch size.
        assert (length ids <= claimBatchSize)
        -- A claimed id is never a DLQ row. Untracked jobs are claimable and absent
        -- from the model.
        let dlqIds = map concrete (Map.keys (mDlq post))
        nub (filter (`elem` dlqIds) ids) === []
    ]

claimBatchSize :: Int
claimBatchSize = 3

mkClaim :: forall sm. (ArbiterC sm) => sm [Int64]
mkClaim = do
  jobs <- HL.claimNextVisibleJobsAs claimBatchSize 60 smWorker :: sm [JobRead SMPayload]
  pure (map primaryKey jobs)

-- | True if the id names a live row that is currently in flight.
isLiveInFlight :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> Int64 -> IO Bool
isLiveInFlight schema table withConn jid = withConn $ \conn -> do
  rows <-
    PG.query conn (fromString (T.unpack sql)) (Only jid)
  pure $ case rows of
    Only count : _ -> (count :: Int64) > 0
    _ -> False
  where
    sql =
      "SELECT count(*) FROM "
        <> schema
        <> "."
        <> table
        <> " WHERE id = ? AND not_visible_until > NOW() AND NOT suspended AND attempts > 0"

-- | @(leased, suspended, nvu)@ for one row.
leaseState
  :: Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Int64
  -> IO (Bool, Bool, Maybe UTCTime)
leaseState schema table withConn jid = withConn $ \conn -> do
  rows <- PG.query conn (fromString (T.unpack sql)) (Only jid)
  pure $ case rows of
    row : _ -> row
    _ -> (False, False, Nothing)
  where
    sql =
      "SELECT claimed_by IS NOT NULL AND NOT suspended AND not_visible_until IS NOT NULL AND not_visible_until > NOW(), suspended, not_visible_until FROM "
        <> schema
        <> "."
        <> table
        <> " WHERE id = ?"

-- | True if a row with this id is present in the main table.
rowExists :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> Int64 -> IO Bool
rowExists schema table withConn jid = withConn $ \conn -> do
  rows <- PG.query conn (fromString (T.unpack sql)) (Only jid)
  pure $ case rows of
    Only count : _ -> (count :: Int64) > 0
    _ -> False
  where
    sql = "SELECT count(*) FROM " <> schema <> "." <> table <> " WHERE id = ?"

-- | A reference to an existing live job, shared by every id-targeted command.
newtype JobRef (v :: Type -> Type) = JobRef (Var Int64 v)
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

-- | @Retry job backoff@. A zero backoff returns the job to the claim pool at once.
data Retry (v :: Type -> Type) = Retry (Var Int64 v) NominalDiffTime
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

-- | Retry backoffs the model draws from.
retryBackoffs :: [NominalDiffTime]
retryBackoffs = [0, 30]

-- | A command that picks a live job from the model and runs an id-keyed
-- operation, tagging any invariant failure with the label. @removes@ says
-- whether the job leaves the queue. The post-check runs after the operation.
jobRefCommandL'
  :: (MonadGen gen, MonadIO m, MonadTest m)
  => String
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Bool
  -> (Int64 -> m ())
  -> (Int64 -> sm ())
  -> Command gen m Model
jobRefCommandL' lbl run schema table withConn removes postCheck operation =
  Command gen exec callbacks
  where
    gen model
      | Map.null (mLive model) = Nothing
      | otherwise = Just (JobRef <$> Gen.element (Map.keys (mLive model)))
    exec (JobRef ref) = do
      evalIO (run (operation (concrete ref)))
      postCheck (concrete ref)
      checkInvariantsL lbl schema table withConn
    callbacks =
      Require (\model (JobRef ref) -> Map.member ref (mLive model))
        : [Update (\model (JobRef ref) _ -> model {mLive = Map.delete ref (mLive model)}) | removes]

jobRefCommandL
  :: (MonadGen gen, MonadIO m, MonadTest m)
  => String
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Bool
  -> (Int64 -> sm ())
  -> Command gen m Model
jobRefCommandL lbl run schema table withConn removes =
  jobRefCommandL' lbl run schema table withConn removes (\_ -> pure ())

cAck
  , cCancel
  , cSuspend
  , cResume
  , cPromote
  , cRetry
  , cExtend
  , cRelease
    :: forall gen m sm
     . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
    => (forall a. sm a -> IO a)
    -> Text
    -> Text
    -> (forall a. (PG.Connection -> IO a) -> IO a)
    -> Command gen m Model
cAck run schema table withConn =
  jobRefCommandL' "Ack" run schema table withConn True ackedRowGone mkAck
  where
    -- An acked tracked job is gone from the main table. Tracked jobs are never
    -- rollup finalizers.
    ackedRowGone jid = do
      present <- evalIO (rowExists schema table withConn jid)
      present === False
cCancel run schema table withConn = jobRefCommandL "Cancel" run schema table withConn True (void . HL.cancelJob @SMPayload)
cSuspend run schema table withConn = jobRefCommandL "Suspend" run schema table withConn False (void . HL.suspendJob @SMPayload)
cResume run schema table withConn = jobRefCommandL "Resume" run schema table withConn False (void . HL.resumeJob @SMPayload)
cPromote run schema table withConn =
  Command gen exec [Require (\model (JobRef ref) -> Map.member ref (mLive model))]
  where
    gen model
      | Map.null (mLive model) = Nothing
      | otherwise = Just (JobRef <$> Gen.element (Map.keys (mLive model)))
    -- Promote holds no claim. A leased or suspended row comes back untouched.
    exec (JobRef ref) = do
      let jid = concrete ref
      (leased, susp, nvu) <- evalIO (leaseState schema table withConn jid)
      promoted <- evalIO (run (HL.promoteJob @SMPayload jid))
      when (leased || susp) $ do
        annotate ((if leased then "leased" else "suspended") <> " job " <> show jid <> " was promoted")
        promoted === 0
        (_, _, nvu') <- evalIO (leaseState schema table withConn jid)
        nvu' === nvu
      checkInvariantsL "Promote" schema table withConn
cRetry run schema table withConn =
  Command gen exec [Require (\model (Retry ref _) -> Map.member ref (mLive model))]
  where
    gen model
      | Map.null (mLive model) = Nothing
      | otherwise = Just (Retry <$> Gen.element (Map.keys (mLive model)) <*> Gen.element retryBackoffs)
    exec (Retry ref backoff) = do
      evalIO (run (mkRetry @sm (concrete ref) backoff))
      checkInvariantsL "Retry" schema table withConn
cExtend run schema table withConn = jobRefCommandL "Extend" run schema table withConn False mkExtend

-- | Release a job's lease with a visibility timeout of 0. Claim and release
-- cycles drive a job to its @max_attempts@.
cRelease run schema table withConn = jobRefCommandL "Release" run schema table withConn False mkRelease

-- | Read a job by id at this test's payload type.
fetchJob :: forall sm. (ArbiterC sm) => Int64 -> sm (Maybe (JobRead SMPayload))
fetchJob = HL.getJobById

mkAck
  , mkExtend
  , mkRelease
  , mkToDLQ
    :: forall sm
     . (ArbiterC sm)
    => Int64
    -> sm ()
mkAck jid = fetchJob @sm jid >>= traverse_ (void . HL.ackJob)

-- | Park a leased job for @backoff@ seconds.
mkRetry :: forall sm. (ArbiterC sm) => Int64 -> NominalDiffTime -> sm ()
mkRetry jid backoff = fetchJob @sm jid >>= traverse_ (\job -> whenLeased job (void (HL.updateJobForRetry backoff "sm retry" job)))

mkExtend jid = fetchJob @sm jid >>= traverse_ (\job -> whenLeased job (void (HL.setVisibilityTimeout 90 job)))
mkRelease jid = fetchJob @sm jid >>= traverse_ (\job -> whenLeased job (void (HL.setVisibilityTimeout 0 job)))
mkToDLQ jid = fetchJob @sm jid >>= traverse_ (void . HL.moveToDLQ "sm dlq")

-- | Run a lease-management action only when the job is currently in flight.
whenLeased :: (MonadIO m) => JobRead SMPayload -> m () -> m ()
whenLeased job act = do
  now <- liftIO getCurrentTime
  when (attempts job > 0 && not (suspended job) && maybe False (> now) (notVisibleUntil job)) act

-- | Move a live job to the DLQ, then capture the new DLQ row id.
cToDLQ
  :: forall gen m sm
   . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cToDLQ run schema table withConn =
  Command gen exec callbacks
  where
    gen model
      | Map.null (mLive model) = Nothing
      | otherwise = Just (JobRef <$> Gen.element (Map.keys (mLive model)))
    exec (JobRef ref) = do
      let jid = concrete ref
      dlqId <- evalIO (run (mkToDLQ @sm jid) *> lookupDlqId schema table withConn jid)
      checkInvariants schema table withConn
      pure dlqId
    callbacks =
      [ Require $ \model (JobRef ref) -> Map.member ref (mLive model)
      , Update $ \model (JobRef ref) output ->
          model
            { mLive = Map.delete ref (mLive model)
            , mDlq = Map.insert output (Map.findWithDefault Nothing ref (mLive model)) (mDlq model)
            }
      ]

-- | Retry a DLQ job back into the main queue, capturing its new id.
cFromDLQ
  :: forall gen m sm
   . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cFromDLQ run schema table withConn =
  Command gen exec callbacks
  where
    gen model
      | Map.null (mDlq model) = Nothing
      | otherwise = Just (JobRef <$> Gen.element (Map.keys (mDlq model)))
    exec (JobRef ref) = do
      newId <- evalIO (run (mkFromDLQ @sm (concrete ref)))
      checkInvariants schema table withConn
      pure newId
    callbacks =
      [ Require $ \model (JobRef ref) -> Map.member ref (mDlq model)
      , Update $ \model (JobRef ref) output ->
          model
            { mDlq = Map.delete ref (mDlq model)
            , mLive = Map.insert output (Map.findWithDefault Nothing ref (mDlq model)) (mLive model)
            }
      ]

mkFromDLQ :: forall sm. (ArbiterC sm) => Int64 -> sm Int64
mkFromDLQ dlqId = do
  restored <- HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload))
  pure (maybe dlqId primaryKey restored)

-- | The id of the DLQ row holding the given original job id.
lookupDlqId :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> Int64 -> IO Int64
lookupDlqId schema table withConn jid = withConn $ \conn -> do
  rows <- PG.query conn (fromString (T.unpack sql)) (Only jid)
  pure $ case rows of
    Only dlqId : _ -> dlqId
    _ -> jid
  where
    sql =
      "SELECT id FROM "
        <> schema
        <> "."
        <> table
        <> "_dlq WHERE job_id = ? ORDER BY id DESC LIMIT 1"

-- ---------------------------------------------------------------------------
-- Batch and dedup commands (statement-level trigger paths)
-- ---------------------------------------------------------------------------

-- | A subset of live jobs, for the multi-row delete commands.
newtype JobRefs (v :: Type -> Type) = JobRefs [Var Int64 v]
  deriving stock (Eq, Generic, Show)

instance B.FunctorB JobRefs where
  bmap natTrans (JobRefs refs) = JobRefs (map (B.bmap natTrans) refs)

instance B.TraversableB JobRefs where
  btraverse natTrans (JobRefs refs) = JobRefs <$> traverse (B.btraverse natTrans) refs

-- | Insert several jobs in one statement. Left untracked. A batch output cannot
-- be split into per-row 'Var's.
newtype BatchInsert (v :: Type -> Type) = BatchInsert [(Maybe Text, Int)]
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

-- | @InsertTree group childCount@. Insert a suspended rollup finalizer over
-- @childCount@ children sharing the optional group. Left untracked.
data InsertTree (v :: Type -> Type) = InsertTree (Maybe Text) Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cBatchInsert
  :: forall gen m sm
   . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cBatchInsert run schema table withConn =
  Command
    ( \_ ->
        Just $
          BatchInsert
            <$> Gen.list
              (Range.linear 2 4)
              ((,) <$> Gen.maybe (Gen.element ["g1", "g2", "g3"]) <*> Gen.int (Range.linear 0 5))
    )
    ( \(BatchInsert specs) -> do
        evalIO (run (mkBatchInsert @sm specs))
        checkInvariants schema table withConn
    )
    []

mkBatchInsert
  :: forall sm
   . (ArbiterC sm)
  => [(Maybe Text, Int)]
  -> sm ()
mkBatchInsert specs = void (HL.insertJobsBatch_ (map toJob specs))
  where
    toJob (group, prio) =
      setPriority (fromIntegral prio) $ maybe (defaultJob payload) (`defaultGroupedJob` payload) group
    payload = smPayload "sm batch"

cInsertTree
  :: forall gen m sm
   . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cInsertTree run schema table withConn =
  Command
    (\_ -> Just (InsertTree <$> Gen.maybe (Gen.element ["g1", "g2", "g3"]) <*> Gen.int (Range.linear 1 3)))
    ( \(InsertTree group childCount) -> do
        evalIO (run (mkInsertTree @sm group childCount))
        checkInvariants schema table withConn
    )
    []

mkInsertTree
  :: forall sm
   . (ArbiterC sm)
  => Maybe Text
  -> Int
  -> sm ()
mkInsertTree group childCount = do
  let mkJob lbl = maybe (defaultJob (smPayload lbl)) (`defaultGroupedJob` smPayload lbl) group
      children = mkJob "sm tree child 0" :| [mkJob ("sm tree child " <> T.pack (show index)) | index <- [1 .. childCount - 1]]
  void (HL.insertJobTree @SMPayload (mkJob "sm tree parent" <~~ children))

cBatchCancel
  , cBatchDLQ
    :: forall gen m sm
     . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
    => (forall a. sm a -> IO a)
    -> Text
    -> Text
    -> (forall a. (PG.Connection -> IO a) -> IO a)
    -> Command gen m Model
cBatchCancel run schema table withConn =
  Command gen exec callbacks
  where
    gen model
      | Map.null (mLive model) = Nothing
      | otherwise = Just (JobRefs <$> Gen.subsequence (Map.keys (mLive model)))
    exec (JobRefs refs) = do
      evalIO (run (void (HL.cancelJobsBatch @SMPayload (map concrete refs))))
      checkInvariants schema table withConn
    callbacks =
      [ Require $ \model (JobRefs refs) -> all (`Map.member` mLive model) refs
      , Update $ \model (JobRefs refs) _ -> model {mLive = foldl' (flip Map.delete) (mLive model) refs}
      ]
cBatchDLQ run schema table withConn =
  Command gen exec callbacks
  where
    gen model
      | Map.null (mLive model) = Nothing
      | otherwise = Just (JobRefs <$> Gen.subsequence (Map.keys (mLive model)))
    exec (JobRefs refs) = do
      evalIO (run (mkBatchDLQ @sm (map concrete refs)))
      checkInvariants schema table withConn
    callbacks =
      [ Require $ \model (JobRefs refs) -> all (`Map.member` mLive model) refs
      , Update $ \model (JobRefs refs) _ -> model {mLive = foldl' (flip Map.delete) (mLive model) refs}
      ]

mkBatchDLQ :: forall sm. (ArbiterC sm) => [Int64] -> sm ()
mkBatchDLQ ids = do
  jobs <- catMaybes <$> traverse (fetchJob @sm) ids
  void (HL.moveToDLQBatch (map (\job -> (job, "sm batch dlq")) jobs))

-- | Insert a job under one of a few shared dedup keys. Repeated keys exercise
-- the @ON CONFLICT@ path. Left untracked.
data Dedup (v :: Type -> Type) = Dedup Text Bool (Maybe Text) Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cDedup
  :: forall gen m sm
   . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cDedup run schema table withConn =
  Command
    ( \_ ->
        Just $
          Dedup
            <$> Gen.element ["d1", "d2", "d3"]
            <*> Gen.bool
            <*> Gen.maybe (Gen.element ["g1", "g2", "g3"])
            <*> Gen.int (Range.linear 0 5)
    )
    ( \(Dedup key replace group prio) -> do
        evalIO (run (mkDedup @sm key replace group prio))
        checkInvariants schema table withConn
    )
    []

mkDedup
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Bool
  -> Maybe Text
  -> Int
  -> sm ()
mkDedup key replace group prio = void (HL.insertJob job)
  where
    dedupKey = if replace then ReplaceDuplicate key else IgnoreDuplicate key
    job =
      setPriority (fromIntegral prio)
        $ setDedupKey (Just dedupKey)
        $ maybe (defaultJob payload) (`defaultGroupedJob` payload) group
    payload = smPayload "sm dedup"

-- | Run the reaper.
data Refresh (v :: Type -> Type) = Refresh
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

-- | A reaper tick. Drift-correct the groups summary and sweep exhausted jobs to
-- the DLQ.
runReaper
  :: forall sm
   . (MonadArbiter sm, RegistryTables (RegistryOf sm))
  => Text
  -> Text
  -> sm ()
runReaper schema table = do
  void (HL.refreshAllGroupsFully @sm)
  void (Ops.sweepExhaustedJobs schema [table])

cRefresh
  :: forall gen m sm
   . (ArbiterC sm, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cRefresh run schema table withConn =
  Command
    (\_ -> Just (pure Refresh))
    ( \Refresh -> do
        evalIO (run (runReaper @sm schema table))
        checkInvariants schema table withConn
    )
    []

-- ---------------------------------------------------------------------------
-- Concurrency property
-- ---------------------------------------------------------------------------

-- | Groups and dedup keys for the concurrency churn. Fewer groups than workers
-- keeps contention per group high.
concGroups, concDedupKeys :: [Text]
concGroups = ["g" <> T.pack (show index) | index <- [1 .. 3 :: Int]]
concDedupKeys = ["d" <> T.pack (show index) | index <- [1 .. 6 :: Int]]

-- | One self-contained engine action with its parameters baked in. Showable
-- and shrinkable. Every target-based op claims first and acts on what it
-- claimed.
data Act
  = AInsert (Maybe Text) (Maybe Int) Int (Maybe Int) Extras
  | ABatchInsert [(Maybe Text, Int)]
  | AInsertTree (Maybe Text) Int
  | ADedup Text Bool (Maybe Text) Int
  | AClaimAck
  | AClaimRetry
  | AClaimCancel
  | AClaimForceCancel
  | AClaimExtend
  | AClaimRelease
  | AClaimToDLQ
  | ARetryRandomDLQ
  | ASuspendRandom
  | AResumeRandom
  | APromoteRandom
  | ACancelCascade
  | APauseChildren
  | AResumeChildren
  | ADeleteDLQ
  | ADeleteDLQBatch
  | AReaper
  deriving stock (Eq, Show)

genActionData :: Gen Act
genActionData =
  Gen.frequency
    [ (3, AInsert <$> genGroup <*> genDelay <*> genPrio <*> genMaxAtts <*> genExtras)
    , (1, ABatchInsert <$> Gen.list (Range.linear 2 4) ((,) <$> genGroup <*> genPrio))
    , (1, AInsertTree <$> genGroup <*> Gen.int (Range.linear 1 3))
    , (2, ADedup <$> Gen.element concDedupKeys <*> Gen.bool <*> genGroup <*> genPrio)
    , (3, pure AClaimAck)
    , (2, pure AClaimRetry)
    , (2, pure AClaimCancel)
    , (2, pure AClaimForceCancel)
    , (1, pure AClaimExtend)
    , (1, pure AClaimRelease)
    , (2, pure AClaimToDLQ)
    , (1, pure ARetryRandomDLQ)
    , (1, pure ASuspendRandom)
    , (1, pure AResumeRandom)
    , (1, pure APromoteRandom)
    , (1, pure ACancelCascade)
    , (1, pure APauseChildren)
    , (1, pure AResumeChildren)
    , (1, pure ADeleteDLQ)
    , (1, pure ADeleteDLQBatch)
    , (1, pure AReaper)
    ]
  where
    genGroup = Gen.maybe (Gen.element concGroups)
    genDelay = Gen.maybe (Gen.int (Range.linear 30 120))
    genPrio = Gen.int (Range.linear 0 5)
    genMaxAtts = Gen.maybe (Gen.int (Range.linear 1 3))

-- | Interpret an 'Act' into a backend operation.
interpret
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Act
  -> sm ()
interpret schema table withConn act = case act of
  AInsert group delay prio maxAtts extras -> void (mkInsert @sm (applyExtras extras) group delay prio maxAtts)
  ABatchInsert specs -> mkBatchInsert @sm specs
  AInsertTree group childCount -> mkInsertTree @sm group childCount
  ADedup key replace group prio -> mkDedup @sm key replace group prio
  AClaimAck -> claimAck @sm
  AClaimRetry -> claimRetry @sm
  AClaimCancel -> claimCancel @sm
  AClaimForceCancel -> claimForceCancel @sm
  AClaimExtend -> claimExtend @sm
  AClaimRelease -> claimRelease @sm
  AClaimToDLQ -> claimToDLQ @sm
  ARetryRandomDLQ -> retryRandomDLQ @sm schema table withConn
  ASuspendRandom -> suspendRandom @sm schema table withConn
  AResumeRandom -> resumeRandom @sm schema table withConn
  APromoteRandom -> promoteRandom @sm schema table withConn
  ACancelCascade ->
    onRandomRollupParent @sm schema table withConn (void . HL.cancelJobCascade @SMPayload)
  APauseChildren -> onRandomRollupParent @sm schema table withConn (void . HL.pauseChildren @SMPayload)
  AResumeChildren -> onRandomRollupParent @sm schema table withConn (void . HL.resumeChildren @SMPayload)
  ADeleteDLQ -> deleteRandomDLQ @sm schema table withConn
  ADeleteDLQBatch -> deleteRandomDLQBatch @sm schema table withConn
  AReaper -> runReaper @sm schema table

-- | The opaque-action form, for samplers that do not shrink.
genAction
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Gen (sm ())
genAction schema table withConn =
  interpret @sm schema table withConn <$> genActionData

claimThen :: forall sm. (ArbiterC sm) => (JobRead SMPayload -> sm ()) -> sm ()
claimThen handle = do
  jobs <- HL.claimNextVisibleJobsAs 3 30 smWorker :: sm [JobRead SMPayload]
  traverse_ handle jobs

claimAck
  , claimRetry
  , claimCancel
  , claimForceCancel
  , claimExtend
  , claimRelease
  , claimToDLQ
    :: forall sm. (ArbiterC sm) => sm ()
claimAck = claimThen @sm (void . HL.ackJob)
claimRetry = claimThen @sm (void . HL.updateJobForRetry 30 "conc retry")
claimCancel = claimThen @sm (void . HL.cancelJob @SMPayload . primaryKey)
-- Force-cancel while still leased flags the job in place. It stays present and keeps blocking its group head.
claimForceCancel = claimThen @sm (void . HL.forceCancelJob @SMPayload . primaryKey)
claimExtend = claimThen @sm (void . HL.setVisibilityTimeout 60)
claimRelease = claimThen @sm (void . HL.setVisibilityTimeout 0)
claimToDLQ = claimThen @sm (void . HL.moveToDLQ "conc dlq")

-- | Retry one arbitrary DLQ row back into the main queue, if any exists.
retryRandomDLQ
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> sm ()
retryRandomDLQ schema table withConn = do
  mId <- liftIO (firstId withConn sql)
  traverse_ (\dlqId -> void (HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload)))) mId
  where
    sql =
      "SELECT id FROM " <> schema <> "." <> table <> "_dlq ORDER BY random() LIMIT 1"

-- | Apply an id-keyed admin op to a random live job, if any.
onRandomJob
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> (Int64 -> sm ())
  -> sm ()
onRandomJob schema table withConn act = do
  mId <- liftIO (firstId withConn sql)
  traverse_ act mId
  where
    sql = "SELECT id FROM " <> schema <> "." <> table <> " ORDER BY random() LIMIT 1"

suspendRandom
  , resumeRandom
  , promoteRandom
    :: forall sm
     . (ArbiterC sm)
    => Text
    -> Text
    -> (forall a. (PG.Connection -> IO a) -> IO a)
    -> sm ()
suspendRandom schema table withConn = onRandomJob @sm schema table withConn (void . HL.suspendJob @SMPayload)
resumeRandom schema table withConn = onRandomJob @sm schema table withConn (void . HL.resumeJob @SMPayload)
promoteRandom schema table withConn = onRandomJob @sm schema table withConn (void . HL.promoteJob @SMPayload)

-- | Apply an id-keyed op to a random rollup-finalizer parent, if any. A parent
-- is a row with a @parent_state@ snapshot.
onRandomRollupParent
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> (Int64 -> sm ())
  -> sm ()
onRandomRollupParent schema table withConn act = do
  mId <- liftIO (firstId withConn sql)
  traverse_ act mId
  where
    sql =
      "SELECT id FROM " <> schema <> "." <> table <> " WHERE parent_state IS NOT NULL ORDER BY random() LIMIT 1"

-- | Delete a random DLQ row.
deleteRandomDLQ
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> sm ()
deleteRandomDLQ schema table withConn = do
  mId <- liftIO (firstId withConn sql)
  traverse_ (void . HL.deleteDLQJob @SMPayload) mId
  where
    sql = "SELECT id FROM " <> schema <> "." <> table <> "_dlq ORDER BY random() LIMIT 1"

-- | Delete a small random batch of DLQ rows in one statement.
deleteRandomDLQBatch
  :: forall sm
   . (ArbiterC sm)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> sm ()
deleteRandomDLQBatch schema table withConn = do
  ids <- liftIO $ withConn $ \conn -> do
    rows <-
      PG.query_ conn (fromString (T.unpack ("SELECT id FROM " <> schema <> "." <> table <> "_dlq ORDER BY random() LIMIT 3")))
    pure [dlqId | Only dlqId <- rows]
  void (HL.deleteDLQJobsBatch @SMPayload ids)

-- | Run an action, retrying transient serialization and deadlock aborts.
withRetry :: IO () -> IO ()
withRetry act = go (5 :: Int)
  where
    go remaining = do
      outcome <- tryAny act
      case outcome of
        Right () -> pure ()
        Left err
          | remaining > 0 && isRetryableError err -> go (remaining - 1)
          | otherwise -> throwIO err

-- | Detect a transient serialization or deadlock abort by the SQLSTATE in the
-- rendered message.
isRetryableError :: SomeException -> Bool
isRetryableError err = any (`isInfixOf` show err) ["40P01", "40001"]

-- | Install the gap-free HOL detector through the raw-connection accessor.
installHolDetector :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
installHolDetector schema table withConn = withConn $ \conn ->
  traverse_ (void . PG.execute_ conn . fromString . T.unpack) (holInstallSql schema table)

countHolViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
countHolViolations schema table withConn = withConn $ \conn -> do
  rows <- PG.query_ conn (fromString (T.unpack ("SELECT group_key, job_id FROM " <> holViolTbl schema table)))
  pure
    [ "HOL violation: job " <> show (jid :: Int64) <> " claimed while group " <> T.unpack groupName <> " already in-flight"
    | (groupName, jid) <- rows
    ]

removeHolDetector :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
removeHolDetector schema table withConn = withConn $ \conn ->
  traverse_ (void . PG.execute_ conn . fromString . T.unpack) (holRemoveSql schema table)

-- ---------------------------------------------------------------------------
-- Property + hspec wiring
-- ---------------------------------------------------------------------------

prop_engine
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> Property
prop_engine run schema table withConn reset = withTests 300 $ property $ do
  actions <-
    forAll $
      Gen.sequential
        (Range.linear 1 40)
        initialModel
        [ cInsert @_ @_ @sm run schema table withConn
        , cClaim @_ @_ @sm run schema table withConn
        , cAck @_ @_ @sm run schema table withConn
        , cCancel @_ @_ @sm run schema table withConn
        , cSuspend @_ @_ @sm run schema table withConn
        , cResume @_ @_ @sm run schema table withConn
        , cPromote @_ @_ @sm run schema table withConn
        , cRetry @_ @_ @sm run schema table withConn
        , cExtend @_ @_ @sm run schema table withConn
        , cRelease @_ @_ @sm run schema table withConn
        , cToDLQ @_ @_ @sm run schema table withConn
        , cFromDLQ @_ @_ @sm run schema table withConn
        , cBatchInsert @_ @_ @sm run schema table withConn
        , cInsertTree @_ @_ @sm run schema table withConn
        , cBatchCancel @_ @_ @sm run schema table withConn
        , cBatchDLQ @_ @_ @sm run schema table withConn
        , cDedup @_ @_ @sm run schema table withConn
        , cRefresh @_ @_ @sm run schema table withConn
        ]
  evalIO (resetSeeded reset schema withConn)
  executeSequential initialModel actions
  settled <- evalIO (queryViolations schema table withConn)
  settled === []

-- | Concurrent property with up to eight independent action branches. The
-- branches are shrinkable and reproducible from the seed. Run them under the
-- caller-installed HOL detector. Then run one reaper tick and check the detector
-- log and settled summary.
prop_concurrent
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> Property
-- A concurrent failure is nondeterministic and shrink candidates rarely
-- reproduce it. The seed-reproducible counterexample is kept.
prop_concurrent run schema table withConn reset = withTests 100 $ withShrinks 0 $ property $ do
  branches <- forAll $ Gen.list (Range.linear 2 8) (Gen.list (Range.linear 5 25) genActionData)
  (hol, settled) <- evalIO $ do
    resetSeeded reset schema withConn
    truncateHol schema table withConn
    mapConcurrently_
      (traverse_ (\act -> withRetry (run (interpret @sm schema table withConn act))))
      branches
    void (run (runReaper @sm schema table))
    (,) <$> countHolViolations schema table withConn <*> queryViolations schema table withConn
  (hol <> settled) === []

-- | Set a wall-clock time limit for a concurrent test.
withinSecs :: Int -> IO () -> IO ()
withinSecs secs act =
  timeout (secs * 1_000_000) act
    >>= maybe (expectationFailure ("timed out after " <> show secs <> "s")) pure

-- ---------------------------------------------------------------------------
-- Deterministic regression guards
-- ---------------------------------------------------------------------------
-- Saturated, bounded stress targeted at fixed concurrency bugs. These reproduce
-- a regression on nearly every run.

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
      watch act = do
        outcome <- tryAny act
        case outcome of
          Right () -> pure ()
          Left err
            | "40P01" `isInfixOf` show err -> atomicModifyIORef' deadlocks (\count -> (count + 1, ()))
            | "40001" `isInfixOf` show err -> pure ()
            | otherwise -> throwIO err
      actorA = replicateM_ rounds $ watch (run (mkBatchInsert @sm [(Just "g1", 0), (Just "g3", 0)]))
      actorB =
        traverse_
          (\index -> watch (run (mkDedup @sm "dlk" True (Just (if even index then "g1" else "g3")) 0)))
          [1 .. rounds]
  mapConcurrently_ id [actorA, actorB]
  deadlockCount <- readIORef deadlocks
  -- Tolerate the rare residual race. A regression produces hundreds of deadlocks.
  deadlockCount `shouldSatisfy` (<= rounds `div` 100)

-- | Guard for the dedup group-move double-claim. Saturates the hot groups with
-- concurrent dedup moves and claims under the gap-free HOL detector, with the
-- reaper churning. Asserts the detector stayed empty.
serializationGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
serializationGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  installHolDetector schema table withConn
  flip finally (removeHolDetector schema table withConn) $ do
    truncateHol schema table withConn
    let rounds = 800 :: Int
        nActors = 16 :: Int
        actor = replicateM_ rounds $ do
          act <- Gen.sample (genAction @sm schema table withConn)
          withRetry (run act)
        reaper = replicateM_ (rounds * 2) $ withRetry (run (void (HL.refreshAllGroupsFully @sm)))
    mapConcurrently_ id (reaper : replicate nActors actor)
    hol <- countHolViolations schema table withConn
    hol `shouldBe` []

-- | Concurrent churn with no reaper, then assert the group summary matches a fresh recompute.
concurrentDriftGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
concurrentDriftGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  let rounds = 300 :: Int
      nActors = 12 :: Int
      actor = replicateM_ rounds $ do
        act <- Gen.sample (Gen.filter (/= AReaper) genActionData)
        withRetry (run (interpret @sm schema table withConn act))
  mapConcurrently_ id (replicate nActors actor)
  driftViolations schema table withConn >>= (`shouldBe` [])

-- | Deterministic guard for the @max_attempts@ machinery. Inserts ungrouped jobs
-- capped at a single attempt, then claim and release cycles them. The claim guard
-- holds attempts at the limit and the reaper sweep moves every exhausted job to
-- the DLQ.
exhaustionGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
exhaustionGuard run schema table withConn reset = do
  reset
  let jobCount = 20 :: Int
  run (replicateM_ jobCount (void (mkInsert @sm id Nothing Nothing 0 (Just 1))))
  -- Claim everything, then release it back. Only the first cycle claims. Later
  -- cycles are no-ops.
  replicateM_ 3 $
    run $ do
      jobs <- HL.claimNextVisibleJobs 100 60 :: sm [JobRead SMPayload]
      traverse_ (void . HL.setVisibilityTimeout 0) jobs
  -- No live job is past its limit.
  exactViolations schema table withConn >>= (`shouldBe` [])
  -- Every exhausted, claimable job lands in the DLQ.
  void (run (Ops.sweepExhaustedJobs schema [table]))
  let tbl = schema <> "." <> table
  stranded <-
    countQuery withConn $
      "SELECT count(*) FROM "
        <> tbl
        <> " WHERE attempts >= 1 AND NOT suspended"
        <> " AND (not_visible_until IS NULL OR not_visible_until <= NOW())"
  dlqd <- countQuery withConn ("SELECT count(*) FROM " <> tbl <> "_dlq")
  stranded `shouldBe` 0
  dlqd `shouldBe` fromIntegral jobCount

-- | Claim everything visible and ack it, repeatedly, until a claim comes back
-- empty or the round bound is hit.
drainToEmpty
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Int
  -> IO ()
drainToEmpty run = go
  where
    go bound
      | bound <= 0 = pure ()
      | otherwise = do
          jobs <- run (HL.claimNextVisibleJobs 50 60 :: sm [JobRead SMPayload])
          if null jobs
            then pure ()
            else run (traverse_ (void . HL.ackJob) jobs) >> go (bound - 1)

-- | Deterministic liveness guard. A fixed set of jobs across several groups plus
-- ungrouped, at mixed priorities, drains fully under a fair claim and ack loop
-- within a bounded number of rounds.
progressGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
progressGuard run schema table withConn reset = do
  reset
  run $ do
    traverse_
      ( \index ->
          void
            (mkInsert @sm id (Just ("pg-" <> T.pack (show (index `mod` 8)))) Nothing (index `mod` 5) Nothing)
      )
      [1 .. 40 :: Int]
    traverse_
      (\index -> void (mkInsert @sm id Nothing Nothing (index `mod` 5) Nothing))
      [1 .. 20 :: Int]
  drainToEmpty @sm run 300
  remaining <- withConn $ \conn -> do
    [Only count] <- PG.query_ conn (fromString (T.unpack ("SELECT count(*) FROM " <> schema <> "." <> table)))
    pure (count :: Int64)
  remaining `shouldBe` 0

-- | Deterministic guard against group starvation. Ineligible groups in backoff
-- or suspended drop out of the ready ranking. A regression makes the productive
-- claim come back empty.
starvationGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
starvationGuard run reset = do
  let crowders = [1 .. 30 :: Int]
      grp prefix index = prefix <> T.pack (show index)
      ins group lbl = void (run (HL.insertJob (defaultGroupedJob group (smPayload lbl))))
      claim1 = run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
  -- Backoff-blocked crowders. Each fails its head into a long backoff with a
  -- ready successor queued behind it.
  reset
  traverse_ (\index -> ins (grp "boff-" index) (grp "boff-" index <> "-head")) crowders
  replicateM_ (length crowders) $ do
    jobs <- claim1
    run (traverse_ (void . HL.updateJobForRetry 3600 "boom") jobs)
  traverse_ (\index -> ins (grp "boff-" index) (grp "boff-" index <> "-succ")) crowders
  ins "productive-g" "productive"
  behindBackoff <- claim1
  length behindBackoff `shouldBe` 1
  -- Suspended crowders. They are not claimable and leave the ready ranking.
  reset
  traverse_
    ( \index -> do
        inserted <- run (HL.insertJob (defaultGroupedJob (grp "susp-" index) (smPayload (grp "susp-" index))))
        run (traverse_ (void . HL.suspendJob @SMPayload . primaryKey) inserted)
    )
    crowders
  ins "productive-g" "productive"
  behindSuspended <- claim1
  length behindSuspended `shouldBe` 1

-- | Deterministic guard for the rollup-tree lifecycle. Part 1: a finalizer over
-- two children resumes and completes once both children are acked. Part 2:
-- DLQ'ing the parent cascades every tree member to the DLQ.
treeGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
treeGuard run schema table withConn reset = do
  let tbl = schema <> "." <> table
      mainCount = countQuery withConn ("SELECT count(*) FROM " <> tbl)
      dlqCount = countQuery withConn ("SELECT count(*) FROM " <> tbl <> "_dlq")
      mkJob lbl = defaultJob (smPayload lbl)
      twoChildTree = mkJob "tg parent" <~~ (mkJob "tg child 0" :| [mkJob "tg child 1"])
  -- Part 1: acking both children resumes the parent, which then completes.
  reset
  run (mkInsertTree @sm Nothing 2)
  drainToEmpty @sm run 10
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 0)
  -- Part 2: DLQ'ing the suspended parent cascades the whole tree to the DLQ.
  reset
  Right (parent :| _) <- run (HL.insertJobTree @SMPayload twoChildTree)
  void (run (HL.moveToDLQ "tree guard cascade" parent))
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 3)

-- | A dedup-replaced job is fresh and carries no stale claim owner. A grouped
-- job is claimed with a 1s lease and abandoned, then dedup-replaced once the
-- lease expires. The replaced row has no claimant.
dedupReplaceStaleLeaseGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
dedupReplaceStaleLeaseGuard run schema table withConn reset = do
  reset
  let job = setDedupKey (Just (ReplaceDuplicate "drsl-key")) $ defaultGroupedJob "drslg" (smPayload "drsl")
  void (run (HL.insertJob job))
  _ <- run (HL.claimNextVisibleJobsAs 1 1 (UUID.fromWords 0 0 0 1) :: sm [JobRead SMPayload])
  threadDelay 2_000_000
  void (run (HL.insertJob job))
  stale <-
    countQuery withConn $
      "SELECT count(*) FROM " <> schema <> "." <> table <> " WHERE dedup_key = 'drsl-key' AND claimed_by IS NOT NULL"
  stale `shouldBe` 0

-- | Deterministic guard for the rows promote refuses: back in flight on a second
-- attempt, released, and suspended with a schedule.
promoteLeaseGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
promoteLeaseGuard run schema table withConn reset = do
  -- Back in flight on a second attempt.
  reset
  jid <- run (mkInsert @sm id Nothing Nothing 0 Nothing)
  _ <- run (HL.claimNextVisibleJobsAs 1 60 smWorker :: sm [JobRead SMPayload])
  run (mkRetry @sm jid 0)
  attempt2 <- run (HL.claimNextVisibleJobsAs 1 60 smWorker :: sm [JobRead SMPayload])
  map primaryKey attempt2 `shouldBe` [jid]
  run (HL.promoteJob @SMPayload jid) >>= (`shouldBe` 0)
  isLiveInFlight schema table withConn jid >>= (`shouldBe` True)
  -- Released with a zero visibility timeout. Still claimed, no window left.
  reset
  rid <- run (mkInsert @sm id Nothing Nothing 0 Nothing)
  released <- run (HL.claimNextVisibleJobsAs 1 60 smWorker :: sm [JobRead SMPayload])
  map primaryKey released `shouldBe` [rid]
  run (HL.setVisibilityTimeout 0 (head released)) >>= (`shouldBe` 1)
  leaseState schema table withConn rid >>= (`shouldBe` (False, False, Nothing))
  run (HL.promoteJob @SMPayload rid) >>= (`shouldBe` 0)
  -- Suspended with a schedule its resume restores.
  reset
  sid <- run (mkInsert @sm id Nothing (Just 30) 0 Nothing)
  run (HL.suspendJob @SMPayload sid) >>= (`shouldBe` 1)
  (_, _, nvu) <- leaseState schema table withConn sid
  run (HL.promoteJob @SMPayload sid) >>= (`shouldBe` 0)
  (_, _, nvu') <- leaseState schema table withConn sid
  nvu' `shouldBe` nvu

-- | Deterministic guard for crashed-worker recovery. A grouped job claimed with
-- a one-second lease becomes reclaimable once the lease expires, both through the
-- @next_due@ trigger path and after a reaper recompute of @in_flight_until@.
reclaimGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
reclaimGuard run reset = do
  let insClaimExpire group = do
        void (run (mkInsert @sm id (Just group) Nothing 0 Nothing))
        _ <- run (HL.claimNextVisibleJobs 1 1 :: sm [JobRead SMPayload])
        threadDelay 2_000_000
      claim1 = run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
  -- Through the next_due trigger path alone.
  reset
  insClaimExpire "rc-trig"
  viaTrigger <- claim1
  length viaTrigger `shouldBe` 1
  -- After a reaper recompute of in_flight_until.
  reset
  insClaimExpire "rc-reaper"
  void (run (HL.refreshAllGroupsFully @sm))
  viaReaper <- claim1
  length viaReaper `shouldBe` 1

-- | Deterministic checks for 'Ops.runGated'. A second caller within the interval
-- is skipped. Under concurrency exactly one of many callers wins.
runGatedChecks
  :: forall sm
   . (MonadArbiter sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
runGatedChecks run schema withConn = do
  let truncateGates =
        withConn $ \conn ->
          void (PG.execute_ conn (fromString (T.unpack ("TRUNCATE " <> schema <> ".arbiter_gates"))))
  -- A second call within the interval is skipped.
  truncateGates
  firstRun <- run (Ops.runGated schema "sm-gate" 3600 (pure (1 :: Int)))
  secondRun <- run (Ops.runGated schema "sm-gate" 3600 (pure (2 :: Int)))
  firstRun `shouldBe` Just 1
  secondRun `shouldBe` Nothing
  -- Under concurrency exactly one caller wins the gate.
  truncateGates
  results <- mapConcurrently (const (run (Ops.runGated schema "sm-gate2" 3600 (pure ())))) [1 .. 16 :: Int]
  length (filter isJust results) `shouldBe` 1

-- | Deterministic crash-recovery guard. Jobs claimed with a short lease and
-- abandoned are reclaimed and acked exactly once under concurrent workers, with
-- per-group serialization intact.
concurrentReclaimGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
concurrentReclaimGuard run schema table withConn reset = do
  reset
  installHolDetector schema table withConn
  flip finally (removeHolDetector schema table withConn) $ do
    truncateHol schema table withConn
    let groups = [Just ("crg" <> T.pack (show index)) | index <- [1 .. 5 :: Int]]
        seeds = [(Nothing, prio) | prio <- [0 .. 29 :: Int]] <> [(group, prio) | group <- groups, prio <- [0 .. 1 :: Int]]
    ids <- run (traverse (\(group, prio) -> mkInsert @sm id group Nothing prio Nothing) seeds)
    let total = length ids
    -- Claim with a 1s lease and abandon, simulating crashed workers.
    void (run (HL.claimNextVisibleJobs total 1 :: sm [JobRead SMPayload]))
    threadDelay 1_500_000
    -- Race workers to reclaim and ack until drained, recording every acked id.
    acked <- newIORef []
    let drain = do
          jobs <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead SMPayload])
          if null jobs
            then pure ()
            else do
              run (traverse_ (void . HL.ackJob) jobs)
              atomicModifyIORef' acked (\acc -> (map primaryKey jobs <> acc, ()))
              drain
    mapConcurrently_ id (replicate 10 drain)
    got <- readIORef acked
    length got `shouldBe` total
    length (nub got) `shouldBe` total
    hol <- countHolViolations schema table withConn
    hol `shouldBe` []

-- | Deterministic guard that claims honor priority. Ungrouped jobs come out in
-- ascending priority order, and the grouped candidate ranking serves the lowest
-- min-priority group first.
priorityOrderGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
priorityOrderGuard run reset = do
  let ins group prio = void (mkInsert @sm id group Nothing prio Nothing)
      claimPriorities acc = do
        claimed <- run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
        case claimed of
          [] -> pure (reverse acc)
          jobs -> claimPriorities (map priority jobs <> acc)
  -- Ungrouped: claim order is ascending in priority.
  reset
  run (traverse_ (ins Nothing) [3, 1, 4, 1, 5, 0, 2])
  prs <- claimPriorities []
  prs `shouldBe` map fromIntegral [0, 1, 1, 2, 3, 4, 5 :: Int]
  -- Grouped: the lowest min-priority group is served first.
  reset
  run (ins (Just "pg-hi") 5)
  run (ins (Just "pg-lo") 0)
  claimed <- run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
  map priority claimed `shouldBe` [0]

-- | Deterministic guard. A freshly inserted scheduled job becomes claimable when
-- its delay elapses, for both the grouped and ungrouped paths.
scheduledDueGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
scheduledDueGuard run reset = do
  reset
  void (run (mkInsert @sm id (Just "sched-g") (Just 1) 0 Nothing))
  void (run (mkInsert @sm id Nothing (Just 1) 0 Nothing))
  early <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead SMPayload])
  length early `shouldBe` 0
  threadDelay 1_500_000
  due <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead SMPayload])
  length due `shouldBe` 2

-- | Deterministic guard for the recursive @retryFromDLQ@ restore. A rollup tree
-- cascaded to the DLQ and then retried is restored intact and drains to empty.
treeRetryFromDLQGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
treeRetryFromDLQGuard run schema table withConn reset = do
  let tbl = schema <> "." <> table
      mainCount = countQuery withConn ("SELECT count(*) FROM " <> tbl)
      dlqCount = countQuery withConn ("SELECT count(*) FROM " <> tbl <> "_dlq")
      mkJob lbl = defaultJob (smPayload lbl)
      tree = mkJob "trd parent" <~~ (mkJob "trd child 0" :| [mkJob "trd child 1"])
  reset
  Right (parent :| _) <- run (HL.insertJobTree @SMPayload tree)
  void (run (HL.moveToDLQ "tree retry guard" parent))
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 3)
  -- Retry the parent's DLQ row. The recursive restore brings the whole tree back.
  dlqId <- withConn $ \conn -> do
    [Only rowId] <-
      PG.query conn (fromString (T.unpack ("SELECT id FROM " <> tbl <> "_dlq WHERE job_id = ?"))) (Only (primaryKey parent))
    pure (rowId :: Int64)
  void (run (HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload))))
  mainCount >>= (`shouldBe` 3)
  dlqCount >>= (`shouldBe` 0)
  drainToEmpty @sm run 10
  mainCount >>= (`shouldBe` 0)

-- | A job carrying both a concurrency slot and a rate-limit key is admitted only
-- up to the tighter cap. Assumes 'smBucket' capacity 3 and the cap-a\/cap-c pools.
combinedGateGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
combinedGateGuard run schema withConn reset = do
  resetSeeded reset schema withConn
  let insBoth conc rate count =
        run
          ( replicateM_
              count
              ( void
                  (mkInsert @sm (applyExtras (Extras (Just conc) (Just rate))) Nothing Nothing 0 Nothing)
              )
          )
      claimN limit = run (HL.claimNextVisibleJobsAs limit 60 smWorker :: sm [JobRead SMPayload])
  -- Concurrency is the tighter gate. cap-a admits 1 and the bucket holds 3.
  insBoth "cap-a" "rk-1" 4
  concCapped <- claimN 10
  length concCapped `shouldBe` 1
  -- Spend rk-2 to one token under the roomy cap-c pool.
  insBoth "cap-c" "rk-2" 2
  spent <- claimN 10
  length spent `shouldBe` 2
  run (traverse_ (void . HL.ackJob) spent)
  -- Ack frees the slots and leaves the tokens spent. Rate now caps the fresh batch.
  insBoth "cap-c" "rk-2" 3
  rateCapped <- claimN 10
  length rateCapped `shouldBe` 1

-- | Deterministic guard for the claim-trigger lock inversion. A claim's throttle
-- deferral row references a count row the claim never locked. The update trigger
-- leaves that row unlocked.
deferralLockOrderGuard
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
deferralLockOrderGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  let ins conc rate = void (run (mkInsert @sm (applyExtras (Extras conc rate)) Nothing Nothing 0 Nothing))
      tbl = schema <> "." <> table
      concTbl = schema <> ".arbiter_concurrency"
  -- The cap-a job is rate-denied on a drained bucket. The claim defers it without
  -- holding cap-a:s. The cap-b:s row parks the ordered scan between cap-a:s and
  -- cap-c:s. The cap-c job is admitted and the claim holds cap-c:s.
  ins (Just "cap-a") (Just "rk-1")
  ins (Just "cap-c") Nothing
  withConn $ \conn -> do
    execute_
      conn
      ("UPDATE " <> schema <> ".arbiter_rate_limits SET tokens = 0, last_refill = NOW() WHERE rate_limit_key = 'smrl:rk-1'")
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
  threadDelay 300_000
  claimer <- async (run (HL.claimNextVisibleJobsAs 10 60 smWorker :: sm [JobRead SMPayload]))
  threadDelay 300_000
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
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
deferralClaimedByFlipGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  let ins conc rate = void (run (mkInsert @sm (applyExtras (Extras conc rate)) Nothing Nothing 0 Nothing))
      tbl = schema <> "." <> table
      concTbl = schema <> ".arbiter_concurrency"
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
      ("UPDATE " <> schema <> ".arbiter_rate_limits SET tokens = 0, last_refill = NOW() WHERE rate_limit_key = 'smrl:rk-1'")
  held <- newEmptyMVar
  -- Hold cap-a:s like a concurrent claimer, then reach for the cap-c:s row the
  -- claim holds.
  holder <- async $ withConn $ \conn -> do
    PG.begin conn
    lockConcurrencyKey conn concTbl "cap-a:s"
    putMVar held ()
    threadDelay 600_000
    lockConcurrencyKey conn concTbl "cap-c:s"
    PG.commit conn
  takeMVar held
  claimedJobs <- run (HL.claimNextVisibleJobsAs 10 60 smWorker :: sm [JobRead SMPayload])
  wait holder
  length claimedJobs `shouldBe` 1
  countQuery withConn ("SELECT count(*) FROM " <> tbl <> " WHERE throttled_until IS NOT NULL") >>= (`shouldBe` 0)
  countQuery withConn ("SELECT count(*) FROM " <> tbl <> " WHERE claimed_by = '" <> staleWorker <> "'") >>= (`shouldBe` 1)

-- | Parameterized state-machine property suite. The runner executes a backend
-- action against the real database. @withConn@ exposes a raw 'PG.Connection' for
-- the oracle and HOL-detector SQL. @reset@ truncates the test tables.
stateMachineSpec
  :: forall sm
   . (ArbiterC sm)
  => (forall a. sm a -> IO a)
  -- ^ Runner
  -> Text
  -- ^ schemaName
  -> Text
  -- ^ tableName
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -- ^ Raw-connection accessor
  -> IO ()
  -- ^ Reset (truncate) action
  -> Spec
stateMachineSpec run schema table withConn reset = do
  it "core engine invariants hold over random operation sequences" $ do
    passed <- check (prop_engine @sm run schema table withConn reset)
    passed `shouldBe` True
  it "no serialization or summary violation under N concurrent generated streams" $
    withinSecs 180 $ do
      installHolDetector schema table withConn
      passed <-
        check (prop_concurrent @sm run schema table withConn reset)
          `finally` removeHolDetector schema table withConn
      passed `shouldBe` True
  it "concurrent cross-group operations never deadlock" $
    withinSecs 150 (deadlockGuard @sm run reset)
  it "concurrent dedup moves and claims never double-claim a group" $
    withinSecs 120 (serializationGuard @sm run schema table withConn reset)
  it "trigger-maintained group summary stays exact under concurrency without the reaper" $
    withinSecs 120 (concurrentDriftGuard @sm run schema table withConn reset)
  it "exhausted jobs are capped by the claim guard and swept to the DLQ" $
    withinSecs 60 (exhaustionGuard @sm run schema table withConn reset)
  it "rollup tree resumes on child completion and cascades to the DLQ" $
    withinSecs 60 (treeGuard @sm run schema table withConn reset)
  it "ineligible groups do not starve a productive group" $
    withinSecs 60 (starvationGuard @sm run reset)
  it "a fixed workload fully drains under a fair claim loop" $
    withinSecs 60 (progressGuard @sm run schema table withConn reset)
  it "expired-lease grouped job is reclaimable via triggers and after a reaper recompute" $
    withinSecs 60 (reclaimGuard @sm run reset)
  it "a dedup-replaced job does not carry a stale claim owner" $
    withinSecs 60 (dedupReplaceStaleLeaseGuard @sm run schema table withConn reset)
  it "promote leaves a live lease and a paused schedule alone" $
    withinSecs 60 (promoteLeaseGuard @sm run schema table withConn reset)
  it "abandoned jobs are reclaimed and acked exactly once under concurrent workers" $
    withinSecs 90 (concurrentReclaimGuard @sm run schema table withConn reset)
  it "claims honor priority order" $
    withinSecs 60 (priorityOrderGuard @sm run reset)
  it "a freshly scheduled job becomes claimable when its delay elapses" $
    withinSecs 60 (scheduledDueGuard @sm run reset)
  it "a DLQ'd rollup tree is restored intact on retry and drains" $
    withinSecs 60 (treeRetryFromDLQGuard @sm run schema table withConn reset)
  it "runGated skips within the interval and serializes concurrent callers" $
    withinSecs 60 (runGatedChecks run schema withConn)
  it "the combined rate and concurrency gate admits only up to the tighter cap" $
    withinSecs 60 (combinedGateGuard @sm run schema withConn reset)
  it "a claim's throttle deferral does not deadlock with an ordered concurrency scan" $
    withinSecs 60 (deferralLockOrderGuard @sm run schema table withConn reset)
  it "a stale-leased job is not deferred while another claimer holds its count row" $
    withinSecs 60 (deferralClaimedByFlipGuard @sm run schema table withConn reset)

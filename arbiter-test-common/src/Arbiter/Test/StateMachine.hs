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
--   * job_count, ready_count, next_due, in_flight_until match their recompute
--     from the main table, and min_priority\/min_id never exceed theirs (the
--     DELETE trigger lets those drift downward, healed by the reaper)
--
-- Generated inserts also carry rate-limit keys and concurrency slots, so the
-- claim, retry, and DLQ round-trip paths are exercised on limited jobs.
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
import Arbiter.Core.HasArbiterSchema (HasArbiterSchema)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema (inFlightPredicate)
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , JobRead
  , JobWrite
  , attempts
  , dedupKey
  , defaultGroupedJob
  , defaultJob
  , defaultMaxAttempts
  , maxAttempts
  , notVisibleUntil
  , payload
  , primaryKey
  , priority
  , suspended
  )
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.MonadArbiter (MonadArbiter)
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
import Data.Time.Clock (addUTCTime, getCurrentTime)
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
type ArbiterC m registry =
  ( HasArbiterSchema m registry
  , KnownSymbol (TableForPayload SMPayload registry)
  , MonadArbiter m
  , MonadUnliftIO m
  , RegistryTables registry
  )

-- ---------------------------------------------------------------------------
-- Model
-- ---------------------------------------------------------------------------

-- | Live main-queue jobs and DLQ jobs, each keyed by its id and carrying its
-- group. Generates valid command targets. The invariants are checked against
-- the database, not derived from the model.
data Model (v :: Type -> Type) = Model
  { mLive :: Map (Var Int64 v) (Maybe Text)
  , mDlq :: Map (Var Int64 v) (Maybe Text)
  }

initialModel :: Model v
initialModel = Model Map.empty Map.empty

-- ---------------------------------------------------------------------------
-- Raw SQL string builders (schema/table threaded in)
-- ---------------------------------------------------------------------------

holViolTbl, holFn, holTrigger :: Text -> Text -> Text
holViolTbl schema table = schema <> "." <> table <> "_hol_violations"
holFn schema table = schema <> ".detect_hol_" <> table <> "_fn"
holTrigger _ table = "detect_hol_" <> table

-- | DDL to install the gap-free HOL detector: a row trigger that logs a
-- violation the instant a job becomes a lease\/backoff in-flight while another
-- @attempts > 0@ job in its group already is. The @attempts > 0@ filter excludes
-- scheduled jobs, which do not block the group. Single source of truth shared by
-- both test suites.
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

-- Per-step check: serialization only. The full summary oracle is a separate
-- settled-state check (it races under concurrent execution), and the gap-free
-- HOL detector is read after a parallel run.
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
  [Only n] <- PG.query_ conn (fromString (T.unpack sql))
  pure n

-- | Lock one count row @FOR UPDATE@, like a claimer holding the key.
lockConcurrencyKey :: PG.Connection -> Text -> Text -> IO ()
lockConcurrencyKey c concTbl k =
  void
    ( PG.query_ c (fromString (T.unpack ("SELECT 1 FROM " <> concTbl <> " WHERE concurrency_key = '" <> k <> "' FOR UPDATE")))
        :: IO [Only Int64]
    )

truncateHol :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
truncateHol schema table withConn = withConn $ \conn ->
  void $ PG.execute_ conn (fromString (T.unpack ("TRUNCATE " <> holViolTbl schema table)))

-- Arbiter's test schema/table names are plain lowercase identifiers, so the SQL
-- can interpolate them directly without quoting.
queryViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
queryViolations schema table withConn =
  (<>) <$> exactViolations schema table withConn <*> driftViolations schema table withConn

-- | Summary-drift oracle: full-join the stored @_groups@ row against a fresh
-- recompute and report any column that disagrees. Exact single-threaded, so it
-- runs per-step in the sequential property and at settle in the concurrent one.
driftViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
driftViolations schema table withConn = withConn $ \conn -> do
  rows <- PG.query_ conn (q oracleSql)
  pure ["group " <> T.unpack g <> " summary drift: " <> T.unpack cols | (g, cols) <- rows]
  where
    q = fromString . T.unpack
    tbl = schema <> "." <> table
    groupsTbl = schema <> "." <> table <> "_groups"
    -- Full-join the stored summary against a fresh recompute of every column and
    -- report which columns differ. NOW() is evaluated once per statement, so the
    -- stored timestamps compare exactly against the recompute. The recompute
    -- filters mirror the trigger definitions: ready = unblocked-now, next_due =
    -- any parked/leased row, in_flight = leased/backoff/throttled (inFlightPredicate).
    -- min_priority/min_id only ratchet down on delete (the DELETE trigger drops
    -- their maintenance), so they are bounded below, not matched exactly.
    oracleSql =
      "SELECT gk, drift FROM (SELECT COALESCE(g.group_key, e.group_key) AS gk, concat_ws(', '"
        <> ", CASE WHEN g.group_key IS NULL THEN 'missing summary row' END"
        <> ", CASE WHEN e.group_key IS NULL THEN 'orphan summary row' END"
        <> ", CASE WHEN g.min_priority > e.min_priority THEN 'min_priority' END"
        <> ", CASE WHEN g.min_id > e.min_id THEN 'min_id' END"
        <> ", CASE WHEN g.job_count IS DISTINCT FROM e.job_count THEN 'job_count' END"
        <> ", CASE WHEN g.ready_count IS DISTINCT FROM e.ready_count THEN 'ready_count' END"
        <> ", CASE WHEN g.next_due IS DISTINCT FROM e.next_due THEN 'next_due' END"
        <> ", CASE WHEN g.in_flight_until IS DISTINCT FROM e.in_flight_until THEN 'in_flight_until' END"
        <> ") AS drift FROM (SELECT group_key, min_priority, min_id, job_count, ready_count, next_due, in_flight_until FROM "
        <> groupsTbl
        <> " WHERE job_count > 0) g FULL OUTER JOIN (SELECT group_key"
        <> ", MIN(priority)::int AS min_priority"
        <> ", MIN(id)::bigint AS min_id"
        <> ", COUNT(*)::bigint AS job_count"
        <> ", COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended)::bigint AS ready_count"
        <> ", MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended) AS next_due"
        <> ", MAX(not_visible_until) FILTER (WHERE "
        <> inFlightPredicate ""
        <> ") AS in_flight_until FROM "
        <> tbl
        <> " WHERE group_key IS NOT NULL GROUP BY group_key) e ON g.group_key = e.group_key) t WHERE drift <> ''"

-- | Exact, non-racy invariant violations, safe to sample live during concurrent
-- churn (unlike the eventually-settled summary oracle):
--
--   * serialization: more than one in-flight (leased\/backoff) job per group
--   * attempt bound: a live uncancelled job past its limit. The claim guard caps
--     @attempts@ at @max_attempts@, and force-cancel's void-bump is not an execution
--     (like the reaper's exhausted-to-DLQ sweep, cancelled rows are excluded)
--   * dedup uniqueness: two live jobs sharing a @dedup_key@
--   * rate-limit integrity: a bucket's tokens stay within [0, max]. Negative means
--     the gate over-spent, above max means a refill\/top-up\/seed skipped the cap.
exactViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
exactViolations schema table withConn = withConn $ \conn -> do
  serial <- PG.query_ conn (fromString (T.unpack serialSql))
  over <- PG.query_ conn (fromString (T.unpack overSql))
  dups <- PG.query_ conn (fromString (T.unpack dupSql))
  conc <- PG.query_ conn (fromString (T.unpack concSql))
  rl <- PG.query_ conn (fromString (T.unpack rlSql))
  rlMax <- PG.query_ conn (fromString (T.unpack rlMaxSql))
  serialMsgs <- traverse (diagnoseSerial conn) [g | Only g <- serial]
  pure $
    serialMsgs
      <> ["job " <> show (jid :: Int64) <> " exceeded its max_attempts" | Only jid <- over]
      <> ["duplicate live dedup_key " <> T.unpack k | Only k <- dups]
      <> ["concurrency cap exceeded for key " <> T.unpack k | Only k <- conc]
      <> ["rate-limit bucket " <> T.unpack k <> " over-spent (negative tokens)" | Only k <- rl]
      <> ["rate-limit bucket " <> T.unpack k <> " over-credited (tokens above max)" | Only k <- rlMax]
  where
    tbl = schema <> "." <> table
    concPolicies = schema <> ".arbiter_concurrency_policies"
    rlBuckets = schema <> ".arbiter_rate_limits"
    rlPolicies = schema <> ".arbiter_rate_limit_policies"
    groupsTbl = schema <> "." <> table <> "_groups"
    dma = T.pack (show defaultMaxAttempts)
    -- On a serialization violation, dump the offending group's jobs and summary
    -- row (timestamps as ms relative to NOW) so the counterexample carries the
    -- full group state, not just the group key.
    diagnoseSerial conn g = do
      jobs <- PG.query conn (fromString (T.unpack jobsSql)) (Only g)
      summ <- PG.query conn (fromString (T.unpack summSql)) (Only g)
      let jobStr (jid, att, mx, susp, nvu, dk, upd, att_ms) =
            "{id="
              <> show (jid :: Int64)
              <> " att="
              <> show (att :: Int32)
              <> " max="
              <> show (mx :: Maybe Int32)
              <> " susp="
              <> show (susp :: Bool)
              <> " nvu_ms="
              <> show (nvu :: Maybe Int64)
              <> " dk="
              <> show (dk :: Maybe Text)
              <> " upd_ago="
              <> show (upd :: Maybe Int64)
              <> " att_ago="
              <> show (att_ms :: Maybe Int64)
              <> "}"
          summStr (jc, rc, ifu, nd) =
            "jc="
              <> show (jc :: Int64)
              <> " rc="
              <> show (rc :: Int64)
              <> " ifu_ms="
              <> show (ifu :: Maybe Int64)
              <> " nd_ms="
              <> show (nd :: Maybe Int64)
      pure $
        "multiple in-flight in group "
          <> T.unpack g
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
        <> " AND cancel_requested_at IS NULL"
    dupSql =
      "SELECT dedup_key FROM "
        <> tbl
        <> " WHERE dedup_key IS NOT NULL GROUP BY dedup_key HAVING COUNT(*) > 1"
    -- More claimed-in-flight jobs for a key than its effective cap. The gate
    -- enforces this atomically at claim time, so it must hold at every committed
    -- state. eff = COALESCE(pool override, pool default).
    concSql =
      "SELECT j.concurrency_key FROM "
        <> tbl
        <> " j LEFT JOIN "
        <> concPolicies
        <> " p ON p.prefix_id = j.concurrency_prefix"
        <> " WHERE j.concurrency_key IS NOT NULL"
        <> " GROUP BY j.concurrency_key, p.override_limit, p.default_limit"
        <> " HAVING COUNT(*) FILTER (WHERE j.claimed_by IS NOT NULL)"
        <> " > COALESCE(p.override_limit, p.default_limit)"
    -- Negative tokens means the gate over-spent. Epsilon tolerates float rounding.
    rlSql = "SELECT rate_limit_key FROM " <> rlBuckets <> " WHERE tokens < -0.001"
    -- Tokens above the effective max means a refill, top-up, or seed skipped the cap.
    -- Brackets the balance from above as rlSql does from below. eff = COALESCE(override, default).
    rlMaxSql =
      "SELECT b.rate_limit_key FROM "
        <> rlBuckets
        <> " b JOIN "
        <> rlPolicies
        <> " p ON p.prefix_id = b.policy_prefix"
        <> " WHERE b.tokens > COALESCE(p.override_max_tokens, p.default_max_tokens) + 0.001"

-- | A live child whose parent has left the main queue. Single-threaded this
-- never happens (a finalizer completes only after its children, a DLQ'd parent
-- cascades them, and 'retryFromDLQ' refuses to restore a child whose root parent
-- is gone), so it is a per-step check for the sequential property. Under
-- concurrency a parent can be acked between that refuse-check and a child's
-- re-insert, leaving a benign orphan that just processes as a plain job, so the
-- concurrent oracle does not assert it.
orphanViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
orphanViolations schema table withConn = withConn $ \conn -> do
  orphans <- PG.query_ conn (fromString (T.unpack orphanSql))
  pure ["orphaned child " <> show (jid :: Int64) <> " (parent gone)" | Only jid <- orphans]
  where
    tbl = schema <> "." <> table
    orphanSql =
      "SELECT c.id FROM "
        <> tbl
        <> " c WHERE c.parent_id IS NOT NULL AND NOT EXISTS"
        <> " (SELECT 1 FROM "
        <> tbl
        <> " p WHERE p.id = c.parent_id)"

-- ---------------------------------------------------------------------------
-- Commands
-- ---------------------------------------------------------------------------

-- | Worker id for the generated claim paths. Concurrency is counted off
-- @claimed_by@, so generated claims must be attributed for the cap to engage.
smWorker :: UUID.UUID
smWorker = UUID.fromWords 0 0 0 7

-- | Per-job rate-limit key and concurrency pool a generated insert may carry, so
-- the lifecycle (claim, retry, DLQ round-trip) is exercised on limited jobs.
data Extras = Extras (Maybe Text) (Maybe Text)
  deriving stock (Eq, Show)

-- | Seeded pools with a fixed limit, so the cap oracle is exact.
smConcSlots :: [(Text, Int32)]
smConcSlots = [("cap-a", 1), ("cap-b", 2), ("cap-c", 3)]

-- | One suffix per pool, so each pool drives a single count-row key.
smConcSuffix :: Text
smConcSuffix = "s"

smRateKeys :: [Text]
smRateKeys = ["rk-1", "rk-2"]

genExtras :: (MonadGen g) => g Extras
genExtras = Extras <$> Gen.maybe (Gen.element (map fst smConcSlots)) <*> Gen.maybe (Gen.element smRateKeys)

applyExtras :: Extras -> JobWrite SMPayload -> JobWrite SMPayload
applyExtras (Extras mc mr) j = j {payload = (payload j) {smConcSlot = mc, smRateKey = mr}}

data SMPayload = SMPayload
  { smMessage :: Text
  , smConcSlot :: Maybe Text
  , smRateKey :: Maybe Text
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

smPayload :: Text -> SMPayload
smPayload t = SMPayload t Nothing Nothing

data SMSlot = SlotNone | SlotA | SlotB | SlotC
  deriving stock (Bounded, Enum, Eq)

instance HasConcurrency SMPayload where
  concurrencyFor = concurrencyByCase slotTag slotSel
    where
      slotTag p = case smConcSlot p of
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

-- | A binding bucket: capacity 3 with negligible refill over a test run.
smBucket :: Policy
smBucket = tokenBucket "smrl" 3 60

instance HasRateLimit SMPayload where
  rateLimitFor = limitByCase rateTag rateSel
    where
      rateTag p = case smRateKey p of
        Just "rk-1" -> RateK1
        Just "rk-2" -> RateK2
        _ -> RateNone
      rateSel RateNone = noLimit
      rateSel RateK1 = limitBy smBucket (const "rk-1")
      rateSel RateK2 = limitBy smBucket (const "rk-2")

-- | Seed the concurrency pools (idempotent), so a fresh reset re-establishes them.
seedConcurrencyPools :: Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
seedConcurrencyPools schema withConn = withConn $ \c ->
  traverse_
    (\(p, l) -> traverse_ (void . PG.execute_ c . fromString . T.unpack) (seedConcurrencyPoolSQL schema p l))
    smConcSlots

-- | Seed the rate-limit policy (idempotent), so generated limited jobs run the claim gate against a real bucket.
seedRateLimitPolicies :: Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
seedRateLimitPolicies schema withConn = withConn $ \c ->
  void $ PG.execute_ c (fromString (T.unpack (upsertPolicyRowSQL schema (toPolicyRow smBucket))))

-- | Reset the tables, then re-seed both admission policies (the guard preamble).
resetSeeded :: IO () -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
resetSeeded reset schema withConn = do
  reset
  seedConcurrencyPools schema withConn
  seedRateLimitPolicies schema withConn

-- | Insert a job at a varying priority into an optional group, optionally
-- scheduled into the future. Priority variation is what lets @min_priority@
-- drift be observed.
-- | @Insert group delay priority maxAttempts@. A low @maxAttempts@ lets short
-- claim sequences drive jobs to exhaustion, exercising the claim guard + sweep.
data Insert (v :: Type -> Type) = Insert (Maybe Text) (Maybe Int) Int (Maybe Int) Extras
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cInsert
  :: forall gen m sm registry
   . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
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
    ( \(Insert g d p ma ext) -> do
        jid <- evalIO (run (mkInsert (applyExtras ext) g d p ma))
        checkInvariants schema table withConn
        pure jid
    )
    [Update $ \m (Insert g _ _ _ _) o -> m {mLive = Map.insert o g (mLive m)}]

mkInsert
  :: forall sm registry
   . (ArbiterC sm registry)
  => (JobWrite SMPayload -> JobWrite SMPayload)
  -> Maybe Text
  -> Maybe Int
  -> Int
  -> Maybe Int
  -> sm Int64
mkInsert deco g d p ma = do
  nvu <- traverse (\s -> liftIO (addUTCTime (fromIntegral s) <$> getCurrentTime)) d
  let job =
        (maybe (defaultJob payload) (`defaultGroupedJob` payload) g)
          { notVisibleUntil = nvu
          , priority = fromIntegral p
          , maxAttempts = fromIntegral <$> ma
          }
  mj <- HL.insertJob (deco job)
  pure (primaryKey (fromJust mj))
  where
    payload = smPayload "sm"

data Claim (v :: Type -> Type) = Claim
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cClaim
  :: forall gen m sm registry
   . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cClaim run schema table withConn =
  Command
    (\_ -> Just (pure Claim))
    ( \Claim -> do
        ids <- evalIO (run (mkClaim @sm @registry))
        -- Each claimed job is now a live, in-flight (leased) row in the database.
        live <- evalIO (traverse (isLiveInFlight schema table withConn) ids)
        assert (and live)
        checkInvariants schema table withConn
        pure ids
    )
    [ Ensure $ \_ post Claim ids -> do
        -- Never claim more than the batch size.
        assert (length ids <= claimBatchSize)
        -- A claimed id must not be a DLQ row (those never re-enter the main queue
        -- by claim). Untracked jobs (batch\/tree\/dedup) are legitimately claimable
        -- and absent from the model, so only the DLQ membership is asserted.
        let dlqIds = map concrete (Map.keys (mDlq post))
        nub (filter (`elem` dlqIds) ids) === []
    ]

claimBatchSize :: Int
claimBatchSize = 3

mkClaim :: forall sm registry. (ArbiterC sm registry) => sm [Int64]
mkClaim = do
  js <- HL.claimNextVisibleJobsAs claimBatchSize 60 smWorker :: sm [JobRead SMPayload]
  pure (map primaryKey js)

-- | True if the id names a live row that is currently leased (in-flight).
isLiveInFlight :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> Int64 -> IO Bool
isLiveInFlight schema table withConn jid = withConn $ \conn -> do
  rows <-
    PG.query conn (fromString (T.unpack sql)) (Only jid)
  pure $ case rows of
    Only n : _ -> (n :: Int64) > 0
    _ -> False
  where
    sql =
      "SELECT count(*) FROM "
        <> schema
        <> "."
        <> table
        <> " WHERE id = ? AND not_visible_until > NOW() AND NOT suspended AND attempts > 0"

-- | True if a row with this id is present in the main table.
rowExists :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> Int64 -> IO Bool
rowExists schema table withConn jid = withConn $ \conn -> do
  rows <- PG.query conn (fromString (T.unpack sql)) (Only jid)
  pure $ case rows of
    Only n : _ -> (n :: Int64) > 0
    _ -> False
  where
    sql = "SELECT count(*) FROM " <> schema <> "." <> table <> " WHERE id = ?"

-- | A reference to an existing live job, shared by every id-targeted command.
newtype JobRef (v :: Type -> Type) = JobRef (Var Int64 v)
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

-- | A command that picks a live job from the model and runs an id-keyed
-- operation, tagging any invariant failure with @lbl@. @removes@ says whether
-- the job leaves the queue. @extra@ adds command-specific callbacks (e.g. an
-- ack-removed Ensure).
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
jobRefCommandL' lbl run schema table withConn removes postCheck op =
  Command gen exec callbacks
  where
    gen m
      | Map.null (mLive m) = Nothing
      | otherwise = Just (JobRef <$> Gen.element (Map.keys (mLive m)))
    exec (JobRef v) = do
      evalIO (run (op (concrete v)))
      postCheck (concrete v)
      checkInvariantsL lbl schema table withConn
    callbacks =
      Require (\m (JobRef v) -> Map.member v (mLive m))
        : [Update (\m (JobRef v) _ -> m {mLive = Map.delete v (mLive m)}) | removes]

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
    :: forall gen m sm registry
     . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
    => (forall a. sm a -> IO a)
    -> Text
    -> Text
    -> (forall a. (PG.Connection -> IO a) -> IO a)
    -> Command gen m Model
cAck run schema table withConn =
  jobRefCommandL' "Ack" run schema table withConn True ackedRowGone mkAck
  where
    -- A plain tracked job that was acked must no longer be present in the main
    -- table. Tracked jobs are never rollup finalizers (those are untracked), so
    -- ack deletes the row outright rather than suspending it.
    ackedRowGone jid = do
      present <- evalIO (rowExists schema table withConn jid)
      present === False
cCancel run schema table withConn = jobRefCommandL "Cancel" run schema table withConn True (void . HL.cancelJob @sm @registry @SMPayload)
cSuspend run schema table withConn = jobRefCommandL "Suspend" run schema table withConn False (void . HL.suspendJob @sm @registry @SMPayload)
cResume run schema table withConn = jobRefCommandL "Resume" run schema table withConn False (void . HL.resumeJob @sm @registry @SMPayload)
cPromote run schema table withConn = jobRefCommandL "Promote" run schema table withConn False (void . HL.promoteJob @sm @registry @SMPayload)
cRetry run schema table withConn = jobRefCommandL "Retry" run schema table withConn False mkRetry
cExtend run schema table withConn = jobRefCommandL "Extend" run schema table withConn False mkExtend

-- | Release a job's lease (visibility timeout 0), making it immediately
-- re-claimable. Claim/release cycles drive a job to its @max_attempts@, which is
-- what exercises the claim guard, the reaper sweep, and the over-execution bound.
cRelease run schema table withConn = jobRefCommandL "Release" run schema table withConn False mkRelease

-- | Read a job by id at this test's payload type.
fetchJob :: forall sm registry. (ArbiterC sm registry) => Int64 -> sm (Maybe (JobRead SMPayload))
fetchJob = HL.getJobById

mkAck
  , mkRetry
  , mkExtend
  , mkRelease
  , mkToDLQ
    :: forall sm registry
     . (ArbiterC sm registry)
    => Int64
    -> sm ()
mkAck jid = fetchJob @sm @registry jid >>= traverse_ (void . HL.ackJob)
mkRetry jid = fetchJob @sm @registry jid >>= traverse_ (\j -> whenLeased j (void (HL.updateJobForRetry 30 "sm retry" j)))
mkExtend jid = fetchJob @sm @registry jid >>= traverse_ (\j -> whenLeased j (void (HL.setVisibilityTimeout 90 j)))
mkRelease jid = fetchJob @sm @registry jid >>= traverse_ (\j -> whenLeased j (void (HL.setVisibilityTimeout 0 j)))
mkToDLQ jid = fetchJob @sm @registry jid >>= traverse_ (void . HL.moveToDLQ "sm dlq")

-- | Run a lease-management action only when the job is currently in-flight,
-- matching a worker that only extends\/retries\/releases a job it holds. On a
-- non-leased job it would fabricate an in-flight row no claim handed out.
whenLeased :: (MonadIO m) => JobRead SMPayload -> m () -> m ()
whenLeased j act = do
  now <- liftIO getCurrentTime
  when (attempts j > 0 && not (suspended j) && maybe False (> now) (notVisibleUntil j)) act

-- | Move a live job to the DLQ, then capture the new DLQ row id so a later
-- 'cFromDLQ' can target it.
cToDLQ
  :: forall gen m sm registry
   . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cToDLQ run schema table withConn =
  Command gen exec callbacks
  where
    gen m
      | Map.null (mLive m) = Nothing
      | otherwise = Just (JobRef <$> Gen.element (Map.keys (mLive m)))
    exec (JobRef v) = do
      let jid = concrete v
      dlqId <- evalIO (run (mkToDLQ @sm @registry jid) *> lookupDlqId schema table withConn jid)
      checkInvariants schema table withConn
      pure dlqId
    callbacks =
      [ Require $ \m (JobRef v) -> Map.member v (mLive m)
      , Update $ \m (JobRef v) o ->
          m
            { mLive = Map.delete v (mLive m)
            , mDlq = Map.insert o (Map.findWithDefault Nothing v (mLive m)) (mDlq m)
            }
      ]

-- | Retry a DLQ job back into the main queue, capturing its new id.
cFromDLQ
  :: forall gen m sm registry
   . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cFromDLQ run schema table withConn =
  Command gen exec callbacks
  where
    gen m
      | Map.null (mDlq m) = Nothing
      | otherwise = Just (JobRef <$> Gen.element (Map.keys (mDlq m)))
    exec (JobRef v) = do
      newId <- evalIO (run (mkFromDLQ @sm @registry (concrete v)))
      checkInvariants schema table withConn
      pure newId
    callbacks =
      [ Require $ \m (JobRef v) -> Map.member v (mDlq m)
      , Update $ \m (JobRef v) o ->
          m
            { mDlq = Map.delete v (mDlq m)
            , mLive = Map.insert o (Map.findWithDefault Nothing v (mDlq m)) (mLive m)
            }
      ]

mkFromDLQ :: forall sm registry. (ArbiterC sm registry) => Int64 -> sm Int64
mkFromDLQ dlqId = do
  mj <- HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload))
  pure (maybe dlqId primaryKey mj)

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
  bmap f (JobRefs vs) = JobRefs (map (B.bmap f) vs)

instance B.TraversableB JobRefs where
  btraverse f (JobRefs vs) = JobRefs <$> traverse (B.btraverse f) vs

-- | Insert several jobs in one statement. Left untracked: a batch output can't
-- be split into per-row 'Var's, so the rows are reachable only via claim, the
-- reaper, or the batch deletes. Exercises the multi-row insert trigger.
newtype BatchInsert (v :: Type -> Type) = BatchInsert [(Maybe Text, Int)]
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

-- | @InsertTree group childCount@: insert a suspended rollup finalizer over
-- @childCount@ children sharing the optional group. Untracked, like 'BatchInsert'
-- (the multi-row output can't be split into per-row 'Var's). Exercises the tree
-- insert, parent-resume, and cascade-to-DLQ paths.
data InsertTree (v :: Type -> Type) = InsertTree (Maybe Text) Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cBatchInsert
  :: forall gen m sm registry
   . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
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
        evalIO (run (mkBatchInsert @sm @registry specs))
        checkInvariants schema table withConn
    )
    []

mkBatchInsert
  :: forall sm registry
   . (ArbiterC sm registry)
  => [(Maybe Text, Int)]
  -> sm ()
mkBatchInsert specs = void (HL.insertJobsBatch_ (map toJob specs))
  where
    toJob (g, p) =
      (maybe (defaultJob payload) (`defaultGroupedJob` payload) g) {priority = fromIntegral p}
    payload = smPayload "sm batch"

cInsertTree
  :: forall gen m sm registry
   . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cInsertTree run schema table withConn =
  Command
    (\_ -> Just (InsertTree <$> Gen.maybe (Gen.element ["g1", "g2", "g3"]) <*> Gen.int (Range.linear 1 3)))
    ( \(InsertTree g n) -> do
        evalIO (run (mkInsertTree @sm @registry g n))
        checkInvariants schema table withConn
    )
    []

mkInsertTree
  :: forall sm registry
   . (ArbiterC sm registry)
  => Maybe Text
  -> Int
  -> sm ()
mkInsertTree g n = do
  let mk lbl = maybe (defaultJob (smPayload lbl)) (`defaultGroupedJob` smPayload lbl) g
      children = mk "sm tree child 0" :| [mk ("sm tree child " <> T.pack (show i)) | i <- [1 .. n - 1]]
  void (HL.insertJobTree @sm @registry @SMPayload (mk "sm tree parent" <~~ children))

cBatchCancel
  , cBatchDLQ
    :: forall gen m sm registry
     . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
    => (forall a. sm a -> IO a)
    -> Text
    -> Text
    -> (forall a. (PG.Connection -> IO a) -> IO a)
    -> Command gen m Model
cBatchCancel run schema table withConn =
  Command gen exec callbacks
  where
    gen m
      | Map.null (mLive m) = Nothing
      | otherwise = Just (JobRefs <$> Gen.subsequence (Map.keys (mLive m)))
    exec (JobRefs vs) = do
      evalIO (run (void (HL.cancelJobsBatch @sm @registry @SMPayload (map concrete vs))))
      checkInvariants schema table withConn
    callbacks =
      [ Require $ \m (JobRefs vs) -> all (`Map.member` mLive m) vs
      , Update $ \m (JobRefs vs) _ -> m {mLive = foldl' (flip Map.delete) (mLive m) vs}
      ]
cBatchDLQ run schema table withConn =
  Command gen exec callbacks
  where
    gen m
      | Map.null (mLive m) = Nothing
      | otherwise = Just (JobRefs <$> Gen.subsequence (Map.keys (mLive m)))
    exec (JobRefs vs) = do
      evalIO (run (mkBatchDLQ @sm @registry (map concrete vs)))
      checkInvariants schema table withConn
    callbacks =
      [ Require $ \m (JobRefs vs) -> all (`Map.member` mLive m) vs
      , Update $ \m (JobRefs vs) _ -> m {mLive = foldl' (flip Map.delete) (mLive m) vs}
      ]

mkBatchDLQ :: forall sm registry. (ArbiterC sm registry) => [Int64] -> sm ()
mkBatchDLQ ids = do
  jobs <- catMaybes <$> traverse (fetchJob @sm @registry) ids
  void (HL.moveToDLQBatch (map (\j -> (j, "sm batch dlq")) jobs))

-- | Insert a job under one of a few shared dedup keys. Repeated keys exercise
-- the @ON CONFLICT@ path, including replace-with-group-change (old group
-- decremented) and replace-with-priority-change. Left untracked.
data Dedup (v :: Type -> Type) = Dedup Text Bool (Maybe Text) Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cDedup
  :: forall gen m sm registry
   . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
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
    ( \(Dedup key replace g p) -> do
        evalIO (run (mkDedup @sm @registry key replace g p))
        checkInvariants schema table withConn
    )
    []

mkDedup
  :: forall sm registry
   . (ArbiterC sm registry)
  => Text
  -> Bool
  -> Maybe Text
  -> Int
  -> sm ()
mkDedup key replace g p = void (HL.insertJob job)
  where
    dk = if replace then ReplaceDuplicate key else IgnoreDuplicate key
    job =
      (maybe (defaultJob payload) (`defaultGroupedJob` payload) g)
        { dedupKey = Just dk
        , priority = fromIntegral p
        }
    payload = smPayload "sm dedup"

-- | Run the reaper. Drift-correcting, so it must never break an invariant.
data Refresh (v :: Type -> Type) = Refresh
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

-- | A reaper tick: drift-correct the groups summary and sweep exhausted jobs to
-- the DLQ. Both are backstops, so neither may break an invariant.
runReaper
  :: forall sm registry
   . (HasArbiterSchema sm registry, MonadArbiter sm, MonadUnliftIO sm, RegistryTables registry)
  => Text
  -> Text
  -> sm ()
runReaper schema table = do
  void (HL.refreshAllGroups @sm @registry)
  void (Ops.sweepExhaustedJobs schema [table])

cRefresh
  :: forall gen m sm registry
   . (ArbiterC sm registry, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cRefresh run schema table withConn =
  Command
    (\_ -> Just (pure Refresh))
    ( \Refresh -> do
        evalIO (run (runReaper @sm @registry schema table))
        checkInvariants schema table withConn
    )
    []

-- ---------------------------------------------------------------------------
-- Concurrency property
-- ---------------------------------------------------------------------------

-- | Groups and dedup keys for the concurrency churn. More groups widens the set
-- of summary rows updated at once. Fewer than 'nWorkers', so contention per
-- group stays high.
concGroups, concDedupKeys :: [Text]
concGroups = ["g" <> T.pack (show i) | i <- [1 .. 3 :: Int]]
concDedupKeys = ["d" <> T.pack (show i) | i <- [1 .. 6 :: Int]]

-- | One self-contained engine action with its parameters baked in. Showable
-- and shrinkable, so the N-way concurrent property can generate, display, and
-- minimise whole branches of these. Every target-based op claims first and
-- acts on what it claimed, so no action needs to coordinate ids with another
-- branch.
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
  :: forall sm registry
   . (ArbiterC sm registry)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Act
  -> sm ()
interpret schema table withConn act = case act of
  AInsert g d p ma ext -> void (mkInsert @sm @registry (applyExtras ext) g d p ma)
  ABatchInsert specs -> mkBatchInsert @sm @registry specs
  AInsertTree g n -> mkInsertTree @sm @registry g n
  ADedup k r g p -> mkDedup @sm @registry k r g p
  AClaimAck -> claimAck @sm @registry
  AClaimRetry -> claimRetry @sm @registry
  AClaimCancel -> claimCancel @sm @registry
  AClaimForceCancel -> claimForceCancel @sm @registry
  AClaimExtend -> claimExtend @sm @registry
  AClaimRelease -> claimRelease @sm @registry
  AClaimToDLQ -> claimToDLQ @sm @registry
  ARetryRandomDLQ -> retryRandomDLQ @sm @registry schema table withConn
  ASuspendRandom -> suspendRandom @sm @registry schema table withConn
  AResumeRandom -> resumeRandom @sm @registry schema table withConn
  APromoteRandom -> promoteRandom @sm @registry schema table withConn
  ACancelCascade ->
    onRandomRollupParent @sm @registry schema table withConn (void . HL.cancelJobCascade @sm @registry @SMPayload)
  APauseChildren -> onRandomRollupParent @sm @registry schema table withConn (void . HL.pauseChildren @sm @registry @SMPayload)
  AResumeChildren -> onRandomRollupParent @sm @registry schema table withConn (void . HL.resumeChildren @sm @registry @SMPayload)
  ADeleteDLQ -> deleteRandomDLQ @sm @registry schema table withConn
  ADeleteDLQBatch -> deleteRandomDLQBatch @sm @registry schema table withConn
  AReaper -> runReaper @sm @registry schema table

-- | The opaque-action form, for samplers ('serializationGuard') that don't shrink.
genAction
  :: forall sm registry
   . (ArbiterC sm registry)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Gen (sm ())
genAction schema table withConn =
  interpret @sm @registry schema table withConn <$> genActionData

claimThen :: forall sm registry. (ArbiterC sm registry) => (JobRead SMPayload -> sm ()) -> sm ()
claimThen f = do
  js <- HL.claimNextVisibleJobsAs 3 30 smWorker :: sm [JobRead SMPayload]
  traverse_ f js

claimAck
  , claimRetry
  , claimCancel
  , claimForceCancel
  , claimExtend
  , claimRelease
  , claimToDLQ
    :: forall sm registry. (ArbiterC sm registry) => sm ()
claimAck = claimThen @sm @registry (void . HL.ackJob)
claimRetry = claimThen @sm @registry (void . HL.updateJobForRetry 30 "conc retry")
claimCancel = claimThen @sm @registry (void . HL.cancelJob @sm @registry @SMPayload . primaryKey)
-- Force-cancel while still leased hits the flag-claimed-live branch, so the job stays present and must keep blocking its group head.
claimForceCancel = claimThen @sm @registry (void . HL.forceCancelJob @sm @registry @SMPayload . primaryKey)
claimExtend = claimThen @sm @registry (void . HL.setVisibilityTimeout 60)
claimRelease = claimThen @sm @registry (void . HL.setVisibilityTimeout 0)
claimToDLQ = claimThen @sm @registry (void . HL.moveToDLQ "conc dlq")

-- | Retry one arbitrary DLQ row back into the main queue, if any exists.
retryRandomDLQ
  :: forall sm registry
   . (ArbiterC sm registry)
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

-- | Apply an id-keyed admin op to a random live job, if any. Lets stateless
-- churn exercise readiness-changing ops (suspend\/resume\/promote) concurrently.
onRandomJob
  :: forall sm registry
   . (ArbiterC sm registry)
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
    :: forall sm registry
     . (ArbiterC sm registry)
    => Text
    -> Text
    -> (forall a. (PG.Connection -> IO a) -> IO a)
    -> sm ()
suspendRandom schema table withConn = onRandomJob @sm @registry schema table withConn (void . HL.suspendJob @sm @registry @SMPayload)
resumeRandom schema table withConn = onRandomJob @sm @registry schema table withConn (void . HL.resumeJob @sm @registry @SMPayload)
promoteRandom schema table withConn = onRandomJob @sm @registry schema table withConn (void . HL.promoteJob @sm @registry @SMPayload)

-- | Apply an id-keyed op to a random rollup-finalizer parent (a row with a
-- @parent_state@ snapshot), if any. Drives the tree-mutation ops the flat random
-- churn would otherwise never target.
onRandomRollupParent
  :: forall sm registry
   . (ArbiterC sm registry)
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

-- | Delete a random DLQ row (with parent-resume side effects).
deleteRandomDLQ
  :: forall sm registry
   . (ArbiterC sm registry)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> sm ()
deleteRandomDLQ schema table withConn = do
  mId <- liftIO (firstId withConn sql)
  traverse_ (void . HL.deleteDLQJob @sm @registry @SMPayload) mId
  where
    sql = "SELECT id FROM " <> schema <> "." <> table <> "_dlq ORDER BY random() LIMIT 1"

-- | Delete a small random batch of DLQ rows in one statement.
deleteRandomDLQBatch
  :: forall sm registry
   . (ArbiterC sm registry)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> sm ()
deleteRandomDLQBatch schema table withConn = do
  ids <- liftIO $ withConn $ \conn -> do
    rows <-
      PG.query_ conn (fromString (T.unpack ("SELECT id FROM " <> schema <> "." <> table <> "_dlq ORDER BY random() LIMIT 3")))
    pure [dlqId | Only dlqId <- rows]
  void (HL.deleteDLQJobsBatch @sm @registry @SMPayload ids)

-- | Run an action, retrying transient serialization/deadlock aborts the way a
-- real worker would. These are expected under contention and are not invariant
-- violations.
withRetry :: IO () -> IO ()
withRetry act = go (5 :: Int)
  where
    go n = do
      r <- tryAny act
      case r of
        Right () -> pure ()
        Left e
          | n > 0 && isRetryableError e -> go (n - 1)
          | otherwise -> throwIO e

-- | A transient serialization or deadlock abort. postgresql-simple and hasql
-- surface 40P01\/40001 with different exception types, so match the SQLSTATE in
-- the rendered message rather than a single backend's error constructor.
isRetryableError :: SomeException -> Bool
isRetryableError e = any (`isInfixOf` show e) ["40P01", "40001"]

-- | Install the gap-free HOL detector through the raw-connection accessor.
installHolDetector :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
installHolDetector schema table withConn = withConn $ \conn ->
  traverse_ (void . PG.execute_ conn . fromString . T.unpack) (holInstallSql schema table)

countHolViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
countHolViolations schema table withConn = withConn $ \conn -> do
  rows <- PG.query_ conn (fromString (T.unpack ("SELECT group_key, job_id FROM " <> holViolTbl schema table)))
  pure
    [ "HOL violation: job " <> show (jid :: Int64) <> " claimed while group " <> T.unpack g <> " already in-flight"
    | (g, jid) <- rows
    ]

removeHolDetector :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO ()
removeHolDetector schema table withConn = withConn $ \conn ->
  traverse_ (void . PG.execute_ conn . fromString . T.unpack) (holRemoveSql schema table)

-- ---------------------------------------------------------------------------
-- Property + hspec wiring
-- ---------------------------------------------------------------------------

prop_engine
  :: forall sm registry
   . (ArbiterC sm registry)
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
        [ cInsert @_ @_ @sm @registry run schema table withConn
        , cClaim @_ @_ @sm @registry run schema table withConn
        , cAck @_ @_ @sm @registry run schema table withConn
        , cCancel @_ @_ @sm @registry run schema table withConn
        , cSuspend @_ @_ @sm @registry run schema table withConn
        , cResume @_ @_ @sm @registry run schema table withConn
        , cPromote @_ @_ @sm @registry run schema table withConn
        , cRetry @_ @_ @sm @registry run schema table withConn
        , cExtend @_ @_ @sm @registry run schema table withConn
        , cRelease @_ @_ @sm @registry run schema table withConn
        , cToDLQ @_ @_ @sm @registry run schema table withConn
        , cFromDLQ @_ @_ @sm @registry run schema table withConn
        , cBatchInsert @_ @_ @sm @registry run schema table withConn
        , cInsertTree @_ @_ @sm @registry run schema table withConn
        , cBatchCancel @_ @_ @sm @registry run schema table withConn
        , cBatchDLQ @_ @_ @sm @registry run schema table withConn
        , cDedup @_ @_ @sm @registry run schema table withConn
        , cRefresh @_ @_ @sm @registry run schema table withConn
        ]
  evalIO (resetSeeded reset schema withConn)
  executeSequential initialModel actions
  settled <- evalIO (queryViolations schema table withConn)
  settled === []

-- | N-way concurrent property. Generates up to 8 independent branches of
-- self-contained actions -- shrinkable and seed-reproducible -- and runs them
-- concurrently under the gap-free HOL detector (installed by the caller), then
-- quiesces with a reaper tick and asserts both the detector log and the settled
-- summary oracle are clean.
prop_concurrent
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> Property
-- 'withShrinks 0': a concurrent failure is nondeterministic, so most shrink
-- candidates do not reproduce it and hedgehog thrashes for minutes. Keep the
-- original seed-reproducible counterexample instead and minimise deterministically.
prop_concurrent run schema table withConn reset = withTests 100 $ withShrinks 0 $ property $ do
  branches <- forAll $ Gen.list (Range.linear 2 8) (Gen.list (Range.linear 5 25) genActionData)
  (hol, settled) <- evalIO $ do
    resetSeeded reset schema withConn
    truncateHol schema table withConn
    mapConcurrently_
      (traverse_ (\a -> withRetry (run (interpret @sm @registry schema table withConn a))))
      branches
    void (run (runReaper @sm @registry schema table))
    (,) <$> countHolViolations schema table withConn <*> queryViolations schema table withConn
  (hol <> settled) === []

-- | Bound a concurrent test's wall time so a regression can never hang CI.
withinSecs :: Int -> IO () -> IO ()
withinSecs s act =
  timeout (s * 1_000_000) act
    >>= maybe (expectationFailure ("timed out after " <> show s <> "s")) pure

-- ---------------------------------------------------------------------------
-- Deterministic regression guards
-- ---------------------------------------------------------------------------
-- Saturated, bounded stress targeted at the two fixed concurrency bugs. Unlike
-- the diffuse fuzz, these reproduce a regression on nearly every run, so they
-- are reliable CI guards.

-- | Guard for the cross-group trigger deadlock. Two actors hammer opposing
-- multi-group operations: a batch insert spanning g1+g3 versus a dedup move
-- toggling a key between them. The canonical @FOR UPDATE@ in the group triggers
-- must lock group rows in a consistent order. Without it these deadlock within a
-- few hundred operations. We count @40P01@ rather than retrying, since detecting
-- a deadlock is the whole point, and assert none occurred.
deadlockGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
deadlockGuard run reset = do
  reset
  deadlocks <- newIORef (0 :: Int)
  let rounds = 600 :: Int
      watch act = do
        r <- tryAny act
        case r of
          Right () -> pure ()
          Left e
            | "40P01" `isInfixOf` show e -> atomicModifyIORef' deadlocks (\n -> (n + 1, ()))
            | "40001" `isInfixOf` show e -> pure ()
            | otherwise -> throwIO e
      actorA = replicateM_ rounds $ watch (run (mkBatchInsert @sm @registry [(Just "g1", 0), (Just "g3", 0)]))
      actorB =
        traverse_
          (\i -> watch (run (mkDedup @sm @registry "dlk" True (Just (if even i then "g1" else "g3")) 0)))
          [1 .. rounds]
  mapConcurrently_ id [actorA, actorB]
  n <- readIORef deadlocks
  -- Tolerate the rare residual race but still catch a regression (hundreds of deadlocks).
  n `shouldSatisfy` (<= rounds `div` 100)

-- | Guard for the dedup group-move double-claim. Saturates the hot groups with
-- concurrent dedup moves and claims under the gap-free HOL detector, with the
-- reaper churning. If the claim's @expected_group@ re-check or the reaper fix
-- regresses, a job moved between groups gets double-claimed and the detector
-- logs it. Asserts the detector stayed empty.
serializationGuard
  :: forall sm registry
   . (ArbiterC sm registry)
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
          act <- Gen.sample (genAction @sm @registry schema table withConn)
          withRetry (run act)
        reaper = replicateM_ (rounds * 2) $ withRetry (run (void (HL.refreshAllGroups @sm @registry)))
    mapConcurrently_ id (reaper : replicate nActors actor)
    hol <- countHolViolations schema table withConn
    hol `shouldBe` []

-- | Concurrent churn with no reaper, then assert the group summary matches a fresh recompute.
concurrentDriftGuard
  :: forall sm registry
   . (ArbiterC sm registry)
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
        withRetry (run (interpret @sm @registry schema table withConn act))
  mapConcurrently_ id (replicate nActors actor)
  driftViolations schema table withConn >>= (`shouldBe` [])

-- | Deterministic guard for the @max_attempts@ machinery. Inserts ungrouped jobs
-- capped at a single attempt, then claim\/release-cycles them: the claim guard
-- must hold attempts at the limit (so nothing over-executes), and the reaper
-- sweep must then move every exhausted job to the DLQ. The random fuzzer reaches
-- this state only by luck (leases rarely expire in a fast run). This guarantees
-- it, so a regressed claim guard or a broken sweep is caught every run.
exhaustionGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
exhaustionGuard run schema table withConn reset = do
  reset
  let n = 20 :: Int
  run (replicateM_ n (void (mkInsert @sm @registry id Nothing Nothing 0 (Just 1))))
  -- Claim everything, then release it back. With the guard intact, only the
  -- first cycle claims (attempts -> 1 = limit). Later cycles are no-ops.
  replicateM_ 3 $
    run $ do
      js <- HL.claimNextVisibleJobs 100 60 :: sm [JobRead SMPayload]
      traverse_ (void . HL.setVisibilityTimeout 0) js
  -- Claim guard: no live job is past its limit.
  exactViolations schema table withConn >>= (`shouldBe` [])
  -- Sweep: every exhausted, claimable job lands in the DLQ.
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
  dlqd `shouldBe` fromIntegral n

-- | Claim everything visible and ack it, repeatedly, until a claim comes back
-- empty or the round bound is hit. Acking children resumes their finalizers, so
-- a healthy queue with no suspended\/backoff jobs drains fully.
drainToEmpty
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> Int
  -> IO ()
drainToEmpty run = go
  where
    go bound
      | bound <= 0 = pure ()
      | otherwise = do
          js <- run (HL.claimNextVisibleJobs 50 60 :: sm [JobRead SMPayload])
          if null js
            then pure ()
            else run (traverse_ (void . HL.ackJob) js) >> go (bound - 1)

-- | Deterministic liveness guard: a fixed set of jobs across several groups plus
-- ungrouped, at mixed priorities, must fully drain under a fair claim+ack loop
-- within a bounded number of rounds. A job the claim can never surface -- a stuck
-- @in_flight_until@, a lost wakeup, a @ready_count@ undercount -- strands the
-- queue, so the bound (or 'withinSecs') trips. Liveness, expressed as bounded
-- progress.
progressGuard
  :: forall sm registry
   . (ArbiterC sm registry)
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
      ( \i ->
          void
            (mkInsert @sm @registry id (Just ("pg-" <> T.pack (show (i `mod` 8)))) Nothing (i `mod` 5) Nothing)
      )
      [1 .. 40 :: Int]
    traverse_
      (\i -> void (mkInsert @sm @registry id Nothing Nothing (i `mod` 5) Nothing))
      [1 .. 20 :: Int]
  drainToEmpty @sm @registry run 300
  remaining <- withConn $ \conn -> do
    [Only x] <- PG.query_ conn (fromString (T.unpack ("SELECT count(*) FROM " <> schema <> "." <> table)))
    pure (x :: Int64)
  remaining `shouldBe` 0

-- | Deterministic guard against group starvation: ineligible groups (in backoff
-- or suspended) must drop out of the ready ranking, or many low-id ineligible
-- groups crowd the bounded claim window and starve a productive higher-id group.
-- Re-adds the scenarios that previously guarded that ranking bug. A regression
-- makes the productive claim come back empty.
starvationGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
starvationGuard run reset = do
  let crowders = [1 .. 30 :: Int]
      grp prefix i = prefix <> T.pack (show i)
      ins g lbl = void (run (HL.insertJob (defaultGroupedJob g (smPayload lbl))))
      claim1 = run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
  -- Backoff-blocked crowders: each fails its head into a long backoff with a
  -- ready successor queued behind it.
  reset
  traverse_ (\i -> ins (grp "boff-" i) (grp "boff-" i <> "-head")) crowders
  replicateM_ (length crowders) $ do
    js <- claim1
    run (traverse_ (void . HL.updateJobForRetry 3600 "boom") js)
  traverse_ (\i -> ins (grp "boff-" i) (grp "boff-" i <> "-succ")) crowders
  ins "productive-g" "productive"
  c1 <- claim1
  length c1 `shouldBe` 1
  -- Suspended crowders: not claimable, but must still leave the ready ranking.
  reset
  traverse_
    ( \i -> do
        mj <- run (HL.insertJob (defaultGroupedJob (grp "susp-" i) (smPayload (grp "susp-" i))))
        run (traverse_ (void . HL.suspendJob @sm @registry @SMPayload . primaryKey) mj)
    )
    crowders
  ins "productive-g" "productive"
  c2 <- claim1
  length c2 `shouldBe` 1

-- | Deterministic guard for the rollup-tree lifecycle, which the fuzzer hits
-- only sparsely. Part 1: a finalizer over two children resumes and completes
-- once both children are acked, leaving the queue empty. Part 2: DLQ'ing the
-- parent cascades every tree member to the DLQ. Catches a broken parent-resume
-- or a broken cascade on every run.
treeGuard
  :: forall sm registry
   . (ArbiterC sm registry)
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
      mk lbl = defaultJob (smPayload lbl)
      twoChildTree = mk "tg parent" <~~ (mk "tg child 0" :| [mk "tg child 1"])
  -- Part 1: acking both children resumes the parent, which then completes.
  reset
  run (mkInsertTree @sm @registry Nothing 2)
  drainToEmpty @sm @registry run 10
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 0)
  -- Part 2: DLQ'ing the suspended parent cascades the whole tree to the DLQ.
  reset
  Right (parent :| _) <- run (HL.insertJobTree @sm @registry @SMPayload twoChildTree)
  void (run (HL.moveToDLQ "tree guard cascade" parent))
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 3)

-- | Regression guard: a dedup-replaced job is fresh, so it must not carry a stale
-- claim owner. A grouped job is claimed as a worker with a 1s lease and abandoned
-- (claimed_by set), then dedup-replaced once the lease expires. The replaced row's
-- claimed_by must be NULL.
dedupReplaceStaleLeaseGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
dedupReplaceStaleLeaseGuard run schema table withConn reset = do
  reset
  let job = (defaultGroupedJob "drslg" (smPayload "drsl")) {dedupKey = Just (ReplaceDuplicate "drsl-key")}
  void (run (HL.insertJob job))
  _ <- run (HL.claimNextVisibleJobsAs 1 1 (UUID.fromWords 0 0 0 1) :: sm [JobRead SMPayload])
  threadDelay 2_000_000
  void (run (HL.insertJob job))
  stale <-
    countQuery withConn $
      "SELECT count(*) FROM " <> schema <> "." <> table <> " WHERE dedup_key = 'drsl-key' AND claimed_by IS NOT NULL"
  stale `shouldBe` 0

-- | Deterministic guard for crashed-worker recovery: a grouped job claimed with
-- a one-second lease must become reclaimable once the lease expires, both via the
-- @next_due@ trigger path alone and after a reaper recompute of @in_flight_until@.
-- The property's millisecond sequences never let a lease expire, so this is its
-- own guard.
reclaimGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
reclaimGuard run reset = do
  let insClaimExpire g = do
        void (run (mkInsert @sm @registry id (Just g) Nothing 0 Nothing))
        _ <- run (HL.claimNextVisibleJobs 1 1 :: sm [JobRead SMPayload])
        threadDelay 2_000_000
      claim1 = run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
  -- Via the next_due trigger path alone (no reaper).
  reset
  insClaimExpire "rc-trig"
  c1 <- claim1
  length c1 `shouldBe` 1
  -- After a reaper recompute of in_flight_until.
  reset
  insClaimExpire "rc-reaper"
  void (run (HL.refreshAllGroups @sm @registry))
  c2 <- claim1
  length c2 `shouldBe` 1

-- | Deterministic checks for the 'Ops.runGated' gate the reaper relies on: a
-- second caller within the interval is skipped, and under concurrency exactly
-- one of many callers wins. Recovers the gating-mechanism coverage dropped with
-- the old groups-invariant suite.
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
  r1 <- run (Ops.runGated schema "sm-gate" 3600 (pure (1 :: Int)))
  r2 <- run (Ops.runGated schema "sm-gate" 3600 (pure (2 :: Int)))
  r1 `shouldBe` Just 1
  r2 `shouldBe` Nothing
  -- Under concurrency exactly one caller wins the gate.
  truncateGates
  results <- mapConcurrently (const (run (Ops.runGated schema "sm-gate2" 3600 (pure ())))) [1 .. 16 :: Int]
  length (filter isJust results) `shouldBe` 1

-- | Deterministic crash-recovery guard. Jobs claimed with a short lease and then
-- abandoned (the worker died) must, once the lease expires, be reclaimed and acked
-- exactly once under concurrent workers with no double-claim, no lost job, and
-- per-group serialization intact. The fuzzer never reaches this because its leases
-- are 60s and never expire mid-run.
concurrentReclaimGuard
  :: forall sm registry
   . (ArbiterC sm registry)
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
    let groups = [Just ("crg" <> T.pack (show i)) | i <- [1 .. 5 :: Int]]
        seeds = [(Nothing, p) | p <- [0 .. 29 :: Int]] <> [(g, p) | g <- groups, p <- [0 .. 1 :: Int]]
    ids <- run (traverse (\(g, p) -> mkInsert @sm @registry id g Nothing p Nothing) seeds)
    let total = length ids
    -- Claim with a 1s lease and abandon, simulating crashed workers.
    void (run (HL.claimNextVisibleJobs total 1 :: sm [JobRead SMPayload]))
    threadDelay 1_500_000
    -- Race workers to reclaim and ack until drained, recording every acked id.
    acked <- newIORef []
    let drain = do
          js <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead SMPayload])
          if null js
            then pure ()
            else do
              run (traverse_ (void . HL.ackJob) js)
              atomicModifyIORef' acked (\xs -> (map primaryKey js <> xs, ()))
              drain
    mapConcurrently_ id (replicate 10 drain)
    got <- readIORef acked
    length got `shouldBe` total
    length (nub got) `shouldBe` total
    hol <- countHolViolations schema table withConn
    hol `shouldBe` []

-- | Deterministic guard that claims honor priority. Ungrouped jobs come out in
-- ascending priority order, and the grouped candidate ranking serves the lowest
-- min-priority group first. No other test asserts priority ordering.
priorityOrderGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
priorityOrderGuard run reset = do
  let ins g p = void (mkInsert @sm @registry id g Nothing p Nothing)
      claimPriorities acc = do
        c <- run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
        case c of
          [] -> pure (reverse acc)
          js -> claimPriorities (map priority js <> acc)
  -- Ungrouped: claim order is ascending in priority.
  reset
  run (traverse_ (ins Nothing) [3, 1, 4, 1, 5, 0, 2])
  prs <- claimPriorities []
  prs `shouldBe` map fromIntegral [0, 1, 1, 2, 3, 4, 5 :: Int]
  -- Grouped: the lowest min-priority group is served first.
  reset
  run (ins (Just "pg-hi") 5)
  run (ins (Just "pg-lo") 0)
  c <- run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead SMPayload])
  map priority c `shouldBe` [0]

-- | Deterministic guard: a freshly inserted scheduled job (never leased) becomes
-- claimable exactly when its delay elapses, for both the grouped (@next_due@) and
-- ungrouped paths. The fuzzer schedules 30-120s out, so it never sees the due
-- transition.
scheduledDueGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
scheduledDueGuard run reset = do
  reset
  void (run (mkInsert @sm @registry id (Just "sched-g") (Just 1) 0 Nothing))
  void (run (mkInsert @sm @registry id Nothing (Just 1) 0 Nothing))
  c0 <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead SMPayload])
  length c0 `shouldBe` 0
  threadDelay 1_500_000
  c1 <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead SMPayload])
  length c1 `shouldBe` 2

-- | Deterministic guard for the recursive @retryFromDLQ@ restore. A rollup tree
-- cascaded to the DLQ and then retried must be restored intact and drain to empty,
-- exercising the ancestor-walk SQL the fuzzer only hits by chance.
treeRetryFromDLQGuard
  :: forall sm registry
   . (ArbiterC sm registry)
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
      mk lbl = defaultJob (smPayload lbl)
      tree = mk "trd parent" <~~ (mk "trd child 0" :| [mk "trd child 1"])
  reset
  Right (parent :| _) <- run (HL.insertJobTree @sm @registry @SMPayload tree)
  void (run (HL.moveToDLQ "tree retry guard" parent))
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 3)
  -- Retry the parent's DLQ row. The recursive restore brings the whole tree back.
  dlqId <- withConn $ \conn -> do
    [Only i] <-
      PG.query conn (fromString (T.unpack ("SELECT id FROM " <> tbl <> "_dlq WHERE job_id = ?"))) (Only (primaryKey parent))
    pure (i :: Int64)
  void (run (HL.retryFromDLQ dlqId :: sm (Maybe (JobRead SMPayload))))
  mainCount >>= (`shouldBe` 3)
  dlqCount >>= (`shouldBe` 0)
  drainToEmpty @sm @registry run 10
  mainCount >>= (`shouldBe` 0)

-- | A job carrying both a concurrency slot and a rate-limit key is admitted only
-- up to the tighter cap. Assumes 'smBucket' capacity 3 and the cap-a\/cap-c pools.
combinedGateGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
combinedGateGuard run schema withConn reset = do
  resetSeeded reset schema withConn
  let insBoth conc rate n =
        run
          ( replicateM_
              n
              ( void
                  (mkInsert @sm @registry (applyExtras (Extras (Just conc) (Just rate))) Nothing Nothing 0 Nothing)
              )
          )
      claimN n = run (HL.claimNextVisibleJobsAs n 60 smWorker :: sm [JobRead SMPayload])
  -- Concurrency is the tighter gate: cap-a admits 1 though the bucket holds 3.
  insBoth "cap-a" "rk-1" 4
  c1 <- claimN 10
  length c1 `shouldBe` 1
  -- Spend rk-2 to one token under the roomy cap-c pool.
  insBoth "cap-c" "rk-2" 2
  c2 <- claimN 10
  length c2 `shouldBe` 2
  run (traverse_ (void . HL.ackJob) c2)
  -- Ack frees the slots but not the tokens, so rate now caps the fresh batch.
  insBoth "cap-c" "rk-2" 3
  c3 <- claimN 10
  length c3 `shouldBe` 1

-- | Deterministic guard for the claim-trigger lock inversion. A claim's throttle
-- deferral row references a count row the claim never locked (SKIP LOCKED passed
-- over it). The update trigger must not lock that row, or the claim deadlocks
-- with an ordered scan that holds it while waiting behind a key the claim holds.
deferralLockOrderGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
deferralLockOrderGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  let ins conc rate = void (run (mkInsert @sm @registry (applyExtras (Extras conc rate)) Nothing Nothing 0 Nothing))
      tbl = schema <> "." <> table
      concTbl = schema <> ".arbiter_concurrency"
  -- The cap-a job is rate-denied (drained bucket) so the claim defers it without
  -- holding cap-a:s. The cap-b:s row exists only to park the ordered scan between
  -- cap-a:s and cap-c:s. The cap-c job is admitted, so the claim holds cap-c:s.
  ins (Just "cap-a") (Just "rk-1")
  ins (Just "cap-c") Nothing
  withConn $ \c -> do
    execute_
      c
      ("UPDATE " <> schema <> ".arbiter_rate_limits SET tokens = 0, last_refill = NOW() WHERE rate_limit_key = 'smrl:rk-1'")
    execute_
      c
      ( "INSERT INTO "
          <> concTbl
          <> " (concurrency_key, concurrency_prefix, in_flight) VALUES ('cap-b:s', 'cap-b', 0) ON CONFLICT (concurrency_key) DO NOTHING"
      )
  held <- newEmptyMVar
  release <- newEmptyMVar
  h <- async $ withConn $ \c -> do
    PG.begin c
    lockConcurrencyKey c concTbl "cap-b:s"
    putMVar held ()
    takeMVar release
    PG.commit c
  takeMVar held
  -- The ordered scan locks cap-a:s, then parks on the held cap-b:s.
  r <- async (void (run (HL.reconcileConcurrencyCounts :: sm Int64)))
  threadDelay 300_000
  a <- async (run (HL.claimNextVisibleJobsAs 10 60 smWorker :: sm [JobRead SMPayload]))
  threadDelay 300_000
  putMVar release ()
  wait h
  claimedJobs <- wait a
  wait r
  length claimedJobs `shouldBe` 1
  countQuery withConn ("SELECT count(*) FROM " <> tbl <> " WHERE throttled_until IS NOT NULL") >>= (`shouldBe` 1)

-- | The claimed_by-flip variant of 'deferralLockOrderGuard'. Deferring a stale-leased
-- job flips claimed_by, so the trigger must lock its count row. The claim must not
-- defer it while another claimer holds that row, or two claims can form a lock cycle.
deferralClaimedByFlipGuard
  :: forall sm registry
   . (ArbiterC sm registry)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
deferralClaimedByFlipGuard run schema table withConn reset = do
  resetSeeded reset schema withConn
  let ins conc rate = void (run (mkInsert @sm @registry (applyExtras (Extras conc rate)) Nothing Nothing 0 Nothing))
      tbl = schema <> "." <> table
      concTbl = schema <> ".arbiter_concurrency"
      staleWorker = "00000000-0000-0000-0000-000000000009" :: Text
  -- The cap-a job is a stale lease (claimed_by set, lease expired) with a drained
  -- bucket, so a claim would rate-defer it with a claimed_by flip. The cap-c job is
  -- admissible, so the claim holds cap-c:s.
  ins (Just "cap-a") (Just "rk-1")
  ins (Just "cap-c") Nothing
  withConn $ \c -> do
    execute_
      c
      ( "UPDATE "
          <> tbl
          <> " SET claimed_by = '"
          <> staleWorker
          <> "', attempts = 1, not_visible_until = NOW() - interval '1 second' WHERE concurrency_key = 'cap-a:s'"
      )
    execute_
      c
      ("UPDATE " <> schema <> ".arbiter_rate_limits SET tokens = 0, last_refill = NOW() WHERE rate_limit_key = 'smrl:rk-1'")
  held <- newEmptyMVar
  -- Hold cap-a:s like a concurrent claimer, then reach for the cap-c:s row the claim
  -- holds, closing the cycle if the claim's deferral waits on cap-a:s.
  h <- async $ withConn $ \c -> do
    PG.begin c
    lockConcurrencyKey c concTbl "cap-a:s"
    putMVar held ()
    threadDelay 600_000
    lockConcurrencyKey c concTbl "cap-c:s"
    PG.commit c
  takeMVar held
  claimedJobs <- run (HL.claimNextVisibleJobsAs 10 60 smWorker :: sm [JobRead SMPayload])
  wait h
  length claimedJobs `shouldBe` 1
  countQuery withConn ("SELECT count(*) FROM " <> tbl <> " WHERE throttled_until IS NOT NULL") >>= (`shouldBe` 0)
  countQuery withConn ("SELECT count(*) FROM " <> tbl <> " WHERE claimed_by = '" <> staleWorker <> "'") >>= (`shouldBe` 1)

-- | Parameterized state-machine property suite. The runner executes a backend
-- action against the real database. @withConn@ exposes a raw 'PG.Connection' for
-- the oracle and HOL-detector SQL. @reset@ truncates the test tables.
stateMachineSpec
  :: forall sm registry
   . (ArbiterC sm registry)
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
    ok <- check (prop_engine @sm @registry run schema table withConn reset)
    ok `shouldBe` True
  it "no serialization or summary violation under N concurrent generated streams" $
    withinSecs 180 $ do
      installHolDetector schema table withConn
      ok <-
        check (prop_concurrent @sm @registry run schema table withConn reset)
          `finally` removeHolDetector schema table withConn
      ok `shouldBe` True
  it "concurrent cross-group operations never deadlock" $
    withinSecs 150 (deadlockGuard @sm @registry run reset)
  it "concurrent dedup moves and claims never double-claim a group" $
    withinSecs 120 (serializationGuard @sm @registry run schema table withConn reset)
  it "trigger-maintained group summary stays exact under concurrency without the reaper" $
    withinSecs 120 (concurrentDriftGuard @sm @registry run schema table withConn reset)
  it "exhausted jobs are capped by the claim guard and swept to the DLQ" $
    withinSecs 60 (exhaustionGuard @sm @registry run schema table withConn reset)
  it "rollup tree resumes on child completion and cascades to the DLQ" $
    withinSecs 60 (treeGuard @sm @registry run schema table withConn reset)
  it "ineligible groups do not starve a productive group" $
    withinSecs 60 (starvationGuard @sm @registry run reset)
  it "a fixed workload fully drains under a fair claim loop" $
    withinSecs 60 (progressGuard @sm @registry run schema table withConn reset)
  it "expired-lease grouped job is reclaimable via triggers and after a reaper recompute" $
    withinSecs 60 (reclaimGuard @sm @registry run reset)
  it "a dedup-replaced job does not carry a stale claim owner" $
    withinSecs 60 (dedupReplaceStaleLeaseGuard @sm @registry run schema table withConn reset)
  it "abandoned jobs are reclaimed and acked exactly once under concurrent workers" $
    withinSecs 90 (concurrentReclaimGuard @sm @registry run schema table withConn reset)
  it "claims honor priority order" $
    withinSecs 60 (priorityOrderGuard @sm @registry run reset)
  it "a freshly scheduled job becomes claimable when its delay elapses" $
    withinSecs 60 (scheduledDueGuard @sm @registry run reset)
  it "a DLQ'd rollup tree is restored intact on retry and drains" $
    withinSecs 60 (treeRetryFromDLQGuard @sm @registry run schema table withConn reset)
  it "runGated skips within the interval and serializes concurrent callers" $
    withinSecs 60 (runGatedChecks run schema withConn)
  it "the combined rate and concurrency gate admits only up to the tighter cap" $
    withinSecs 60 (combinedGateGuard @sm @registry run schema withConn reset)
  it "a claim's throttle deferral does not deadlock with an ordered concurrency scan" $
    withinSecs 60 (deferralLockOrderGuard @sm @registry run schema table withConn reset)
  it "a stale-leased job is not deferred while another claimer holds its count row" $
    withinSecs 60 (deferralClaimedByFlipGuard @sm @registry run schema table withConn reset)

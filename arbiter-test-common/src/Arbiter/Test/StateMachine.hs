{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE KindSignatures #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

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
--   * job_count, ready_count, next_due, in_flight_until match their recompute
--     from the main table, and min_priority\/min_id never exceed theirs (the
--     DELETE trigger lets those drift downward, healed by the reaper)
--
-- A second property ('prop_concurrent') generates N independent branches of
-- self-contained actions, runs them concurrently under a gap-free serialization
-- detector (a row trigger that fires inside every claim), then quiesces with a
-- reaper tick and asserts both the detector log and the full settled oracle are
-- clean. All contention is in the generated, shrinkable model.
-- Deterministic guards back the known-critical races.
module Arbiter.Test.StateMachine
  ( stateMachineSpec
  , holViolTbl
  , holInstallSql
  , holRemoveSql
  ) where

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , JobPayload
  , JobRead
  , attempts
  , dedupKey
  , defaultGroupedJob
  , defaultJob
  , defaultMaxAttempts
  , maxAttempts
  , notVisibleUntil
  , primaryKey
  , priority
  , suspended
  )
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables, TableForPayload)
import Barbies qualified as B
import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, finally, throwIO)
import Control.Monad (replicateM_, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
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
import UnliftIO.Async (mapConcurrently, mapConcurrently_)

-- | Constraints every HL-issuing helper in this module needs.
type ArbiterC m registry payload =
  ( HasArbiterSchema m registry
  , JobPayload payload
  , KnownSymbol (TableForPayload payload registry)
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
    -- any parked/leased row, in_flight = leased/backoff only (attempts > 0).
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
        <> ", MAX(not_visible_until) FILTER (WHERE not_visible_until > NOW() AND NOT suspended AND attempts > 0) AS in_flight_until FROM "
        <> tbl
        <> " WHERE group_key IS NOT NULL GROUP BY group_key) e ON g.group_key = e.group_key) t WHERE drift <> ''"

-- | Exact, non-racy invariant violations, safe to sample live during concurrent
-- churn (unlike the eventually-settled summary oracle):
--
--   * serialization: more than one in-flight (leased\/backoff) job per group
--   * attempt bound: a live job past its limit (the claim guard must cap
--     @attempts@ at @max_attempts@, so over-execution can never happen)
--   * dedup uniqueness: two live jobs sharing a @dedup_key@
exactViolations :: Text -> Text -> (forall a. (PG.Connection -> IO a) -> IO a) -> IO [String]
exactViolations schema table withConn = withConn $ \conn -> do
  serial <- PG.query_ conn (fromString (T.unpack serialSql))
  over <- PG.query_ conn (fromString (T.unpack overSql))
  dups <- PG.query_ conn (fromString (T.unpack dupSql))
  serialMsgs <- traverse (diagnoseSerial conn) [g | Only g <- serial]
  pure $
    serialMsgs
      <> ["job " <> show (jid :: Int64) <> " exceeded its max_attempts" | Only jid <- over]
      <> ["duplicate live dedup_key " <> T.unpack k | Only k <- dups]
  where
    tbl = schema <> "." <> table
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
      "SELECT id FROM " <> tbl <> " WHERE attempts > COALESCE(max_attempts, " <> dma <> ")"
    dupSql =
      "SELECT dedup_key FROM "
        <> tbl
        <> " WHERE dedup_key IS NOT NULL GROUP BY dedup_key HAVING COUNT(*) > 1"

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

-- | Insert a job at a varying priority into an optional group, optionally
-- scheduled into the future. Priority variation is what lets @min_priority@
-- drift be observed.
-- | @Insert group delay priority maxAttempts@. A low @maxAttempts@ lets short
-- claim sequences drive jobs to exhaustion, exercising the claim guard + sweep.
data Insert (v :: Type -> Type) = Insert (Maybe Text) (Maybe Int) Int (Maybe Int)
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cInsert
  :: forall gen m sm registry payload
   . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cInsert mkPayload run schema table withConn =
  Command
    ( \_ ->
        Just $
          Insert
            <$> Gen.maybe (Gen.element ["g1", "g2", "g3"])
            <*> Gen.maybe (Gen.int (Range.linear 30 120))
            <*> Gen.int (Range.linear 0 5)
            <*> Gen.maybe (Gen.int (Range.linear 1 3))
    )
    ( \(Insert g d p ma) -> do
        jid <- evalIO (run (mkInsert mkPayload g d p ma))
        checkInvariants schema table withConn
        pure jid
    )
    [Update $ \m (Insert g _ _ _) o -> m {mLive = Map.insert o g (mLive m)}]

mkInsert
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> Maybe Text
  -> Maybe Int
  -> Int
  -> Maybe Int
  -> sm Int64
mkInsert mkPayload g d p ma = do
  nvu <- traverse (\s -> liftIO (addUTCTime (fromIntegral s) <$> getCurrentTime)) d
  let job =
        (maybe (defaultJob payload) (`defaultGroupedJob` payload) g)
          { notVisibleUntil = nvu
          , priority = fromIntegral p
          , maxAttempts = fromIntegral <$> ma
          }
  mj <- HL.insertJob job
  pure (primaryKey (fromJust mj))
  where
    payload = mkPayload "sm"

data Claim (v :: Type -> Type) = Claim
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cClaim
  :: forall gen m sm registry payload
   . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
  => (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cClaim run schema table withConn =
  Command
    (\_ -> Just (pure Claim))
    ( \Claim -> do
        ids <- evalIO (run (mkClaim @sm @registry @payload))
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

mkClaim :: forall sm registry payload. (ArbiterC sm registry payload) => sm [Int64]
mkClaim = do
  js <- HL.claimNextVisibleJobs claimBatchSize 60 :: sm [JobRead payload]
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
    :: forall gen m sm registry payload
     . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
    => (Text -> payload)
    -> (forall a. sm a -> IO a)
    -> Text
    -> Text
    -> (forall a. (PG.Connection -> IO a) -> IO a)
    -> Command gen m Model
cAck mkPayload run schema table withConn =
  jobRefCommandL' "Ack" run schema table withConn True ackedRowGone (mkAck mkPayload)
  where
    -- A plain tracked job that was acked must no longer be present in the main
    -- table. Tracked jobs are never rollup finalizers (those are untracked), so
    -- ack deletes the row outright rather than suspending it.
    ackedRowGone jid = do
      present <- evalIO (rowExists schema table withConn jid)
      present === False
cCancel _ run schema table withConn = jobRefCommandL "Cancel" run schema table withConn True (void . HL.cancelJob @sm @registry @payload)
cSuspend _ run schema table withConn = jobRefCommandL "Suspend" run schema table withConn False (void . HL.suspendJob @sm @registry @payload)
cResume _ run schema table withConn = jobRefCommandL "Resume" run schema table withConn False (void . HL.resumeJob @sm @registry @payload)
cPromote _ run schema table withConn = jobRefCommandL "Promote" run schema table withConn False (void . HL.promoteJob @sm @registry @payload)
cRetry mkPayload run schema table withConn = jobRefCommandL "Retry" run schema table withConn False (mkRetry mkPayload)
cExtend mkPayload run schema table withConn = jobRefCommandL "Extend" run schema table withConn False (mkExtend mkPayload)

-- | Release a job's lease (visibility timeout 0), making it immediately
-- re-claimable. Claim/release cycles drive a job to its @max_attempts@, which is
-- what exercises the claim guard, the reaper sweep, and the over-execution bound.
cRelease mkPayload run schema table withConn = jobRefCommandL "Release" run schema table withConn False (mkRelease mkPayload)

-- | Read a job by id at this test's payload type.
fetchJob :: forall sm registry payload. (ArbiterC sm registry payload) => Int64 -> sm (Maybe (JobRead payload))
fetchJob = HL.getJobById

mkAck
  , mkRetry
  , mkExtend
  , mkRelease
  , mkToDLQ
    :: forall sm registry payload
     . (ArbiterC sm registry payload)
    => (Text -> payload)
    -> Int64
    -> sm ()
mkAck _ jid = fetchJob @sm @registry @payload jid >>= traverse_ (void . HL.ackJob)
mkRetry _ jid = fetchJob @sm @registry @payload jid >>= traverse_ (\j -> whenLeased j (void (HL.updateJobForRetry 30 "sm retry" j)))
mkExtend _ jid = fetchJob @sm @registry @payload jid >>= traverse_ (\j -> whenLeased j (void (HL.setVisibilityTimeout 90 j)))
mkRelease _ jid = fetchJob @sm @registry @payload jid >>= traverse_ (\j -> whenLeased j (void (HL.setVisibilityTimeout 0 j)))
mkToDLQ _ jid = fetchJob @sm @registry @payload jid >>= traverse_ (void . HL.moveToDLQ "sm dlq")

-- | Run a lease-management action only when the job is currently in-flight,
-- matching a worker that only extends\/retries\/releases a job it holds. On a
-- non-leased job it would fabricate an in-flight row no claim handed out.
whenLeased :: (MonadIO m) => JobRead payload -> m () -> m ()
whenLeased j act = do
  now <- liftIO getCurrentTime
  when (attempts j > 0 && not (suspended j) && maybe False (> now) (notVisibleUntil j)) act

-- | Move a live job to the DLQ, then capture the new DLQ row id so a later
-- 'cFromDLQ' can target it.
cToDLQ
  :: forall gen m sm registry payload
   . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cToDLQ mkPayload run schema table withConn =
  Command gen exec callbacks
  where
    gen m
      | Map.null (mLive m) = Nothing
      | otherwise = Just (JobRef <$> Gen.element (Map.keys (mLive m)))
    exec (JobRef v) = do
      let jid = concrete v
      dlqId <- evalIO (run (mkToDLQ @sm @registry @payload mkPayload jid) *> lookupDlqId schema table withConn jid)
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
  :: forall gen m sm registry payload
   . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
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
      newId <- evalIO (run (mkFromDLQ @sm @registry @payload (concrete v)))
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

mkFromDLQ :: forall sm registry payload. (ArbiterC sm registry payload) => Int64 -> sm Int64
mkFromDLQ dlqId = do
  mj <- HL.retryFromDLQ dlqId :: sm (Maybe (JobRead payload))
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
  :: forall gen m sm registry payload
   . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cBatchInsert mkPayload run schema table withConn =
  Command
    ( \_ ->
        Just $
          BatchInsert
            <$> Gen.list
              (Range.linear 2 4)
              ((,) <$> Gen.maybe (Gen.element ["g1", "g2", "g3"]) <*> Gen.int (Range.linear 0 5))
    )
    ( \(BatchInsert specs) -> do
        evalIO (run (mkBatchInsert @sm @registry @payload mkPayload specs))
        checkInvariants schema table withConn
    )
    []

mkBatchInsert
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> [(Maybe Text, Int)]
  -> sm ()
mkBatchInsert mkPayload specs = void (HL.insertJobsBatch_ (map toJob specs))
  where
    toJob (g, p) =
      (maybe (defaultJob payload) (`defaultGroupedJob` payload) g) {priority = fromIntegral p}
    payload = mkPayload "sm batch"

cInsertTree
  :: forall gen m sm registry payload
   . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cInsertTree mkPayload run schema table withConn =
  Command
    (\_ -> Just (InsertTree <$> Gen.maybe (Gen.element ["g1", "g2", "g3"]) <*> Gen.int (Range.linear 1 3)))
    ( \(InsertTree g n) -> do
        evalIO (run (mkInsertTree @sm @registry @payload mkPayload g n))
        checkInvariants schema table withConn
    )
    []

mkInsertTree
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> Maybe Text
  -> Int
  -> sm ()
mkInsertTree mkPayload g n = do
  let mk lbl = maybe (defaultJob (mkPayload lbl)) (`defaultGroupedJob` mkPayload lbl) g
      children = mk "sm tree child 0" :| [mk ("sm tree child " <> T.pack (show i)) | i <- [1 .. n - 1]]
  void (HL.insertJobTree @sm @registry @payload (mk "sm tree parent" <~~ children))

cBatchCancel
  , cBatchDLQ
    :: forall gen m sm registry payload
     . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
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
      evalIO (run (void (HL.cancelJobsBatch @sm @registry @payload (map concrete vs))))
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
      evalIO (run (mkBatchDLQ @sm @registry @payload (map concrete vs)))
      checkInvariants schema table withConn
    callbacks =
      [ Require $ \m (JobRefs vs) -> all (`Map.member` mLive m) vs
      , Update $ \m (JobRefs vs) _ -> m {mLive = foldl' (flip Map.delete) (mLive m) vs}
      ]

mkBatchDLQ :: forall sm registry payload. (ArbiterC sm registry payload) => [Int64] -> sm ()
mkBatchDLQ ids = do
  jobs <- catMaybes <$> traverse (fetchJob @sm @registry @payload) ids
  void (HL.moveToDLQBatch (map (\j -> (j, "sm batch dlq")) jobs))

-- | Insert a job under one of a few shared dedup keys. Repeated keys exercise
-- the @ON CONFLICT@ path, including replace-with-group-change (old group
-- decremented) and replace-with-priority-change. Left untracked.
data Dedup (v :: Type -> Type) = Dedup Text Bool (Maybe Text) Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (B.FunctorB, B.TraversableB)

cDedup
  :: forall gen m sm registry payload
   . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Command gen m Model
cDedup mkPayload run schema table withConn =
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
        evalIO (run (mkDedup @sm @registry @payload mkPayload key replace g p))
        checkInvariants schema table withConn
    )
    []

mkDedup
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> Text
  -> Bool
  -> Maybe Text
  -> Int
  -> sm ()
mkDedup mkPayload key replace g p = void (HL.insertJob job)
  where
    dk = if replace then ReplaceDuplicate key else IgnoreDuplicate key
    job =
      (maybe (defaultJob payload) (`defaultGroupedJob` payload) g)
        { dedupKey = Just dk
        , priority = fromIntegral p
        }
    payload = mkPayload "sm dedup"

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
  :: forall gen m sm registry payload
   . (ArbiterC sm registry payload, MonadGen gen, MonadIO m, MonadTest m)
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
  = AInsert (Maybe Text) (Maybe Int) Int (Maybe Int)
  | ABatchInsert [(Maybe Text, Int)]
  | AInsertTree (Maybe Text) Int
  | ADedup Text Bool (Maybe Text) Int
  | AClaimAck
  | AClaimRetry
  | AClaimCancel
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
    [ (3, AInsert <$> genGroup <*> genDelay <*> genPrio <*> genMaxAtts)
    , (1, ABatchInsert <$> Gen.list (Range.linear 2 4) ((,) <$> genGroup <*> genPrio))
    , (1, AInsertTree <$> genGroup <*> Gen.int (Range.linear 1 3))
    , (2, ADedup <$> Gen.element concDedupKeys <*> Gen.bool <*> genGroup <*> genPrio)
    , (3, pure AClaimAck)
    , (2, pure AClaimRetry)
    , (2, pure AClaimCancel)
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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Act
  -> sm ()
interpret mkPayload schema table withConn act = case act of
  AInsert g d p ma -> void (mkInsert @sm @registry @payload mkPayload g d p ma)
  ABatchInsert specs -> mkBatchInsert @sm @registry @payload mkPayload specs
  AInsertTree g n -> mkInsertTree @sm @registry @payload mkPayload g n
  ADedup k r g p -> mkDedup @sm @registry @payload mkPayload k r g p
  AClaimAck -> claimAck @sm @registry @payload
  AClaimRetry -> claimRetry @sm @registry @payload
  AClaimCancel -> claimCancel @sm @registry @payload
  AClaimExtend -> claimExtend @sm @registry @payload
  AClaimRelease -> claimRelease @sm @registry @payload
  AClaimToDLQ -> claimToDLQ @sm @registry @payload
  ARetryRandomDLQ -> retryRandomDLQ @sm @registry @payload schema table withConn
  ASuspendRandom -> suspendRandom @sm @registry @payload schema table withConn
  AResumeRandom -> resumeRandom @sm @registry @payload schema table withConn
  APromoteRandom -> promoteRandom @sm @registry @payload schema table withConn
  ACancelCascade ->
    onRandomRollupParent @sm @registry @payload schema table withConn (void . HL.cancelJobCascade @sm @registry @payload)
  APauseChildren -> onRandomRollupParent @sm @registry @payload schema table withConn (void . HL.pauseChildren @sm @registry @payload)
  AResumeChildren -> onRandomRollupParent @sm @registry @payload schema table withConn (void . HL.resumeChildren @sm @registry @payload)
  ADeleteDLQ -> deleteRandomDLQ @sm @registry @payload schema table withConn
  ADeleteDLQBatch -> deleteRandomDLQBatch @sm @registry @payload schema table withConn
  AReaper -> runReaper @sm @registry schema table

-- | The opaque-action form, for samplers ('serializationGuard') that don't shrink.
genAction
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> Gen (sm ())
genAction mkPayload schema table withConn =
  interpret @sm @registry @payload mkPayload schema table withConn <$> genActionData

claimThen :: forall sm registry payload. (ArbiterC sm registry payload) => (JobRead payload -> sm ()) -> sm ()
claimThen f = do
  js <- HL.claimNextVisibleJobs 3 30 :: sm [JobRead payload]
  traverse_ f js

claimAck
  , claimRetry
  , claimCancel
  , claimExtend
  , claimRelease
  , claimToDLQ
    :: forall sm registry payload. (ArbiterC sm registry payload) => sm ()
claimAck = claimThen @sm @registry @payload (void . HL.ackJob)
claimRetry = claimThen @sm @registry @payload (void . HL.updateJobForRetry 30 "conc retry")
claimCancel = claimThen @sm @registry @payload (void . HL.cancelJob @sm @registry @payload . primaryKey)
claimExtend = claimThen @sm @registry @payload (void . HL.setVisibilityTimeout 60)
claimRelease = claimThen @sm @registry @payload (void . HL.setVisibilityTimeout 0)
claimToDLQ = claimThen @sm @registry @payload (void . HL.moveToDLQ "conc dlq")

-- | Retry one arbitrary DLQ row back into the main queue, if any exists.
retryRandomDLQ
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> sm ()
retryRandomDLQ schema table withConn = do
  mId <- liftIO (firstId withConn sql)
  traverse_ (\dlqId -> void (HL.retryFromDLQ dlqId :: sm (Maybe (JobRead payload)))) mId
  where
    sql =
      "SELECT id FROM " <> schema <> "." <> table <> "_dlq ORDER BY random() LIMIT 1"

-- | Apply an id-keyed admin op to a random live job, if any. Lets stateless
-- churn exercise readiness-changing ops (suspend\/resume\/promote) concurrently.
onRandomJob
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
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
    :: forall sm registry payload
     . (ArbiterC sm registry payload)
    => Text
    -> Text
    -> (forall a. (PG.Connection -> IO a) -> IO a)
    -> sm ()
suspendRandom schema table withConn = onRandomJob @sm @registry @payload schema table withConn (void . HL.suspendJob @sm @registry @payload)
resumeRandom schema table withConn = onRandomJob @sm @registry @payload schema table withConn (void . HL.resumeJob @sm @registry @payload)
promoteRandom schema table withConn = onRandomJob @sm @registry @payload schema table withConn (void . HL.promoteJob @sm @registry @payload)

-- | Apply an id-keyed op to a random rollup-finalizer parent (a row with a
-- @parent_state@ snapshot), if any. Drives the tree-mutation ops the flat random
-- churn would otherwise never target.
onRandomRollupParent
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> sm ()
deleteRandomDLQ schema table withConn = do
  mId <- liftIO (firstId withConn sql)
  traverse_ (void . HL.deleteDLQJob @sm @registry @payload) mId
  where
    sql = "SELECT id FROM " <> schema <> "." <> table <> "_dlq ORDER BY random() LIMIT 1"

-- | Delete a small random batch of DLQ rows in one statement.
deleteRandomDLQBatch
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> sm ()
deleteRandomDLQBatch schema table withConn = do
  ids <- liftIO $ withConn $ \conn -> do
    rows <-
      PG.query_ conn (fromString (T.unpack ("SELECT id FROM " <> schema <> "." <> table <> "_dlq ORDER BY random() LIMIT 3")))
    pure [dlqId | Only dlqId <- rows]
  void (HL.deleteDLQJobsBatch @sm @registry @payload ids)

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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> Property
prop_engine mkPayload run schema table withConn reset = withTests 300 $ property $ do
  actions <-
    forAll $
      Gen.sequential
        (Range.linear 1 40)
        initialModel
        [ cInsert @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cClaim @_ @_ @sm @registry @payload run schema table withConn
        , cAck @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cCancel @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cSuspend @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cResume @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cPromote @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cRetry @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cExtend @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cRelease @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cToDLQ @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cFromDLQ @_ @_ @sm @registry @payload run schema table withConn
        , cBatchInsert @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cInsertTree @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cBatchCancel @_ @_ @sm @registry @payload run schema table withConn
        , cBatchDLQ @_ @_ @sm @registry @payload run schema table withConn
        , cDedup @_ @_ @sm @registry @payload mkPayload run schema table withConn
        , cRefresh @_ @_ @sm @registry @payload run schema table withConn
        ]
  evalIO reset
  executeSequential initialModel actions
  settled <- evalIO (queryViolations schema table withConn)
  settled === []

-- | N-way concurrent property. Generates up to 8 independent branches of
-- self-contained actions -- shrinkable and seed-reproducible -- and runs them
-- concurrently under the gap-free HOL detector (installed by the caller), then
-- quiesces with a reaper tick and asserts both the detector log and the settled
-- summary oracle are clean.
prop_concurrent
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> Property
-- 'withShrinks 0': a concurrent failure is nondeterministic, so most shrink
-- candidates do not reproduce it and hedgehog thrashes for minutes. Keep the
-- original seed-reproducible counterexample instead and minimise deterministically.
prop_concurrent mkPayload run schema table withConn reset = withTests 100 $ withShrinks 0 $ property $ do
  branches <- forAll $ Gen.list (Range.linear 2 8) (Gen.list (Range.linear 5 25) genActionData)
  (hol, settled) <- evalIO $ do
    reset
    truncateHol schema table withConn
    mapConcurrently_
      (traverse_ (\a -> withRetry (run (interpret @sm @registry @payload mkPayload schema table withConn a))))
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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
deadlockGuard mkPayload run reset = do
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
      actorA = replicateM_ rounds $ watch (run (mkBatchInsert @sm @registry @payload mkPayload [(Just "g1", 0), (Just "g3", 0)]))
      actorB =
        traverse_
          (\i -> watch (run (mkDedup @sm @registry @payload mkPayload "dlk" True (Just (if even i then "g1" else "g3")) 0)))
          [1 .. rounds]
  mapConcurrently_ id [actorA, actorB]
  n <- readIORef deadlocks
  -- A non-canonical lock order regresses to deadlocking every few hundred ops, so
  -- hundreds here. The canonical order leaves only a sub-0.01% Postgres-internal
  -- race, so tolerate a small handful while still catching a regression.
  n `shouldSatisfy` (<= rounds `div` 100)

-- | Guard for the dedup group-move double-claim. Saturates the hot groups with
-- concurrent dedup moves and claims under the gap-free HOL detector, with the
-- reaper churning. If the claim's @expected_group@ re-check or the reaper fix
-- regresses, a job moved between groups gets double-claimed and the detector
-- logs it. Asserts the detector stayed empty.
serializationGuard
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
serializationGuard mkPayload run schema table withConn reset = do
  reset
  installHolDetector schema table withConn
  flip finally (removeHolDetector schema table withConn) $ do
    truncateHol schema table withConn
    let rounds = 800 :: Int
        nActors = 16 :: Int
        actor = replicateM_ rounds $ do
          act <- Gen.sample (genAction @sm @registry @payload mkPayload schema table withConn)
          withRetry (run act)
        reaper = replicateM_ (rounds * 2) $ withRetry (run (void (HL.refreshAllGroups @sm @registry)))
    mapConcurrently_ id (reaper : replicate nActors actor)
    hol <- countHolViolations schema table withConn
    hol `shouldBe` []

-- | Deterministic guard that the trigger-maintained summary stays exact under
-- concurrency with no reaper. 'prop_concurrent' and 'serializationGuard' read the
-- summary oracle only after a healing 'runReaper' tick, so a delta (notably the
-- commutative @ready_count@) that drifts only under concurrent interleaving is
-- masked there. Here many actors churn the hot groups with the reaper excluded
-- everywhere, then the oracle is read directly. A job write and its group-summary
-- delta commit in one transaction, and same-group triggers serialize on the group
-- row's @FOR UPDATE@, so once the churn has joined, correct deltas must already
-- match the live recompute. Any mismatch is a real maintenance bug, not transient.
concurrentDriftGuard
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
concurrentDriftGuard mkPayload run schema table withConn reset = do
  reset
  let rounds = 300 :: Int
      nActors = 12 :: Int
      actor = replicateM_ rounds $ do
        act <- Gen.sample (Gen.filter (/= AReaper) genActionData)
        withRetry (run (interpret @sm @registry @payload mkPayload schema table withConn act))
  mapConcurrently_ id (replicate nActors actor)
  driftViolations schema table withConn >>= (`shouldBe` [])

-- | Deterministic guard for the @max_attempts@ machinery. Inserts ungrouped jobs
-- capped at a single attempt, then claim\/release-cycles them: the claim guard
-- must hold attempts at the limit (so nothing over-executes), and the reaper
-- sweep must then move every exhausted job to the DLQ. The random fuzzer reaches
-- this state only by luck (leases rarely expire in a fast run). This guarantees
-- it, so a regressed claim guard or a broken sweep is caught every run.
exhaustionGuard
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
exhaustionGuard mkPayload run schema table withConn reset = do
  reset
  let n = 20 :: Int
  run (replicateM_ n (void (mkInsert @sm @registry @payload mkPayload Nothing Nothing 0 (Just 1))))
  -- Claim everything, then release it back. With the guard intact, only the
  -- first cycle claims (attempts -> 1 = limit). Later cycles are no-ops.
  replicateM_ 3 $
    run $ do
      js <- HL.claimNextVisibleJobs 100 60 :: sm [JobRead payload]
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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (forall a. sm a -> IO a)
  -> Int
  -> IO ()
drainToEmpty run = go
  where
    go bound
      | bound <= 0 = pure ()
      | otherwise = do
          js <- run (HL.claimNextVisibleJobs 50 60 :: sm [JobRead payload])
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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
progressGuard mkPayload run schema table withConn reset = do
  reset
  run $ do
    traverse_
      ( \i ->
          void (mkInsert @sm @registry @payload mkPayload (Just ("pg-" <> T.pack (show (i `mod` 8)))) Nothing (i `mod` 5) Nothing)
      )
      [1 .. 40 :: Int]
    traverse_
      (\i -> void (mkInsert @sm @registry @payload mkPayload Nothing Nothing (i `mod` 5) Nothing))
      [1 .. 20 :: Int]
  drainToEmpty @sm @registry @payload run 300
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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
starvationGuard mkPayload run reset = do
  let crowders = [1 .. 30 :: Int]
      grp prefix i = prefix <> T.pack (show i)
      ins g lbl = void (run (HL.insertJob (defaultGroupedJob g (mkPayload lbl))))
      claim1 = run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead payload])
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
        mj <- run (HL.insertJob (defaultGroupedJob (grp "susp-" i) (mkPayload (grp "susp-" i))))
        run (traverse_ (void . HL.suspendJob @sm @registry @payload . primaryKey) mj)
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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
treeGuard mkPayload run schema table withConn reset = do
  let tbl = schema <> "." <> table
      mainCount = countQuery withConn ("SELECT count(*) FROM " <> tbl)
      dlqCount = countQuery withConn ("SELECT count(*) FROM " <> tbl <> "_dlq")
      mk lbl = defaultJob (mkPayload lbl)
      twoChildTree = mk "tg parent" <~~ (mk "tg child 0" :| [mk "tg child 1"])
  -- Part 1: acking both children resumes the parent, which then completes.
  reset
  run (mkInsertTree @sm @registry @payload mkPayload Nothing 2)
  drainToEmpty @sm @registry @payload run 10
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 0)
  -- Part 2: DLQ'ing the suspended parent cascades the whole tree to the DLQ.
  reset
  Right (parent :| _) <- run (HL.insertJobTree @sm @registry @payload twoChildTree)
  void (run (HL.moveToDLQ "tree guard cascade" parent))
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 3)

-- | Deterministic guard for crashed-worker recovery: a grouped job claimed with
-- a one-second lease must become reclaimable once the lease expires, both via the
-- @next_due@ trigger path alone and after a reaper recompute of @in_flight_until@.
-- The property's millisecond sequences never let a lease expire, so this is its
-- own guard.
reclaimGuard
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
reclaimGuard mkPayload run reset = do
  let insClaimExpire g = do
        void (run (mkInsert @sm @registry @payload mkPayload (Just g) Nothing 0 Nothing))
        _ <- run (HL.claimNextVisibleJobs 1 1 :: sm [JobRead payload])
        threadDelay 2_000_000
      claim1 = run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead payload])
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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
concurrentReclaimGuard mkPayload run schema table withConn reset = do
  reset
  installHolDetector schema table withConn
  flip finally (removeHolDetector schema table withConn) $ do
    truncateHol schema table withConn
    let groups = [Just ("crg" <> T.pack (show i)) | i <- [1 .. 5 :: Int]]
        seeds = [(Nothing, p) | p <- [0 .. 29 :: Int]] <> [(g, p) | g <- groups, p <- [0 .. 1 :: Int]]
    ids <- run (traverse (\(g, p) -> mkInsert @sm @registry @payload mkPayload g Nothing p Nothing) seeds)
    let total = length ids
    -- Claim with a 1s lease and abandon, simulating crashed workers.
    void (run (HL.claimNextVisibleJobs total 1 :: sm [JobRead payload]))
    threadDelay 1_500_000
    -- Race workers to reclaim and ack until drained, recording every acked id.
    acked <- newIORef []
    let drain = do
          js <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead payload])
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
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
priorityOrderGuard mkPayload run reset = do
  let ins g p = void (mkInsert @sm @registry @payload mkPayload g Nothing p Nothing)
      claimPriorities acc = do
        c <- run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead payload])
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
  c <- run (HL.claimNextVisibleJobs 1 60 :: sm [JobRead payload])
  map priority c `shouldBe` [0]

-- | Deterministic guard: a freshly inserted scheduled job (never leased) becomes
-- claimable exactly when its delay elapses, for both the grouped (@next_due@) and
-- ungrouped paths. The fuzzer schedules 30-120s out, so it never sees the due
-- transition.
scheduledDueGuard
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> IO ()
  -> IO ()
scheduledDueGuard mkPayload run reset = do
  reset
  void (run (mkInsert @sm @registry @payload mkPayload (Just "sched-g") (Just 1) 0 Nothing))
  void (run (mkInsert @sm @registry @payload mkPayload Nothing (Just 1) 0 Nothing))
  c0 <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead payload])
  length c0 `shouldBe` 0
  threadDelay 1_500_000
  c1 <- run (HL.claimNextVisibleJobs 5 60 :: sm [JobRead payload])
  length c1 `shouldBe` 2

-- | Deterministic guard for the recursive @retryFromDLQ@ restore. A rollup tree
-- cascaded to the DLQ and then retried must be restored intact and drain to empty,
-- exercising the ancestor-walk SQL the fuzzer only hits by chance.
treeRetryFromDLQGuard
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -> (forall a. sm a -> IO a)
  -> Text
  -> Text
  -> (forall a. (PG.Connection -> IO a) -> IO a)
  -> IO ()
  -> IO ()
treeRetryFromDLQGuard mkPayload run schema table withConn reset = do
  let tbl = schema <> "." <> table
      mainCount = countQuery withConn ("SELECT count(*) FROM " <> tbl)
      dlqCount = countQuery withConn ("SELECT count(*) FROM " <> tbl <> "_dlq")
      mk lbl = defaultJob (mkPayload lbl)
      tree = mk "trd parent" <~~ (mk "trd child 0" :| [mk "trd child 1"])
  reset
  Right (parent :| _) <- run (HL.insertJobTree @sm @registry @payload tree)
  void (run (HL.moveToDLQ "tree retry guard" parent))
  mainCount >>= (`shouldBe` 0)
  dlqCount >>= (`shouldBe` 3)
  -- Retry the parent's DLQ row. The recursive restore brings the whole tree back.
  dlqId <- withConn $ \conn -> do
    [Only i] <-
      PG.query conn (fromString (T.unpack ("SELECT id FROM " <> tbl <> "_dlq WHERE job_id = ?"))) (Only (primaryKey parent))
    pure (i :: Int64)
  void (run (HL.retryFromDLQ dlqId :: sm (Maybe (JobRead payload))))
  mainCount >>= (`shouldBe` 3)
  dlqCount >>= (`shouldBe` 0)
  drainToEmpty @sm @registry @payload run 10
  mainCount >>= (`shouldBe` 0)

-- | Parameterized state-machine property suite. The runner executes a backend
-- action against the real database. @withConn@ exposes a raw 'PG.Connection' for
-- the oracle and HOL-detector SQL. @reset@ truncates the test tables.
stateMachineSpec
  :: forall sm registry payload
   . (ArbiterC sm registry payload)
  => (Text -> payload)
  -- ^ Build a test payload from a label
  -> (forall a. sm a -> IO a)
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
stateMachineSpec mkPayload run schema table withConn reset = do
  it "core engine invariants hold over random operation sequences" $ do
    ok <- check (prop_engine @sm @registry @payload mkPayload run schema table withConn reset)
    ok `shouldBe` True
  it "no serialization or summary violation under N concurrent generated streams" $
    withinSecs 180 $ do
      installHolDetector schema table withConn
      ok <-
        check (prop_concurrent @sm @registry @payload mkPayload run schema table withConn reset)
          `finally` removeHolDetector schema table withConn
      ok `shouldBe` True
  it "concurrent cross-group operations never deadlock" $
    withinSecs 150 (deadlockGuard @sm @registry @payload mkPayload run reset)
  it "concurrent dedup moves and claims never double-claim a group" $
    withinSecs 120 (serializationGuard @sm @registry @payload mkPayload run schema table withConn reset)
  it "trigger-maintained group summary stays exact under concurrency without the reaper" $
    withinSecs 120 (concurrentDriftGuard @sm @registry @payload mkPayload run schema table withConn reset)
  it "exhausted jobs are capped by the claim guard and swept to the DLQ" $
    withinSecs 60 (exhaustionGuard @sm @registry @payload mkPayload run schema table withConn reset)
  it "rollup tree resumes on child completion and cascades to the DLQ" $
    withinSecs 60 (treeGuard @sm @registry @payload mkPayload run schema table withConn reset)
  it "ineligible groups do not starve a productive group" $
    withinSecs 60 (starvationGuard @sm @registry @payload mkPayload run reset)
  it "a fixed workload fully drains under a fair claim loop" $
    withinSecs 60 (progressGuard @sm @registry @payload mkPayload run schema table withConn reset)
  it "expired-lease grouped job is reclaimable via triggers and after a reaper recompute" $
    withinSecs 60 (reclaimGuard @sm @registry @payload mkPayload run reset)
  it "abandoned jobs are reclaimed and acked exactly once under concurrent workers" $
    withinSecs 90 (concurrentReclaimGuard @sm @registry @payload mkPayload run schema table withConn reset)
  it "claims honor priority order" $
    withinSecs 60 (priorityOrderGuard @sm @registry @payload mkPayload run reset)
  it "a freshly scheduled job becomes claimable when its delay elapses" $
    withinSecs 60 (scheduledDueGuard @sm @registry @payload mkPayload run reset)
  it "a DLQ'd rollup tree is restored intact on retry and drains" $
    withinSecs 60 (treeRetryFromDLQGuard @sm @registry @payload mkPayload run schema table withConn reset)
  it "runGated skips within the interval and serializes concurrent callers" $
    withinSecs 60 (runGatedChecks run schema withConn)

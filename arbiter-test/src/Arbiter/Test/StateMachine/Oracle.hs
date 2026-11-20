{-# LANGUAGE OverloadedStrings #-}

-- | Database invariant oracles for the state-machine suites, and the
-- head-of-line detector the concurrent suites install.
module Arbiter.Test.StateMachine.Oracle
  ( WithConn
  , firstId
  , countQuery
  , stepViolations
  , settledViolations
  , exactViolations
  , driftViolations
  , truncateHol
  , installHolDetector
  , holViolations
  , removeHolDetector
  ) where

import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyPoliciesTable, arbiterConcurrencyTable)
import Arbiter.Core.Job.Schema (jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Job.Schema.Groups (groupAggregates, inFlightPredicate)
import Arbiter.Core.Job.Types (defaultMaxAttempts)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTable, arbiterRateLimitsTable)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.Maybe (listToMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple (Only (..))
import Database.PostgreSQL.Simple qualified as PG

import Arbiter.Test.Setup (execute_, toQuery)

-- | A raw-connection accessor.
type WithConn = forall a. (PG.Connection -> IO a) -> IO a

-- ---------------------------------------------------------------------------
-- Raw SQL string builders (schema/table threaded in)
-- ---------------------------------------------------------------------------

-- | Qualified name of the head-of-line detector's violations table.
holViolTbl :: Text -> Text -> Text
holViolTbl schema table = schema <> "." <> table <> "_hol_violations"

-- | Names of the head-of-line detector's function and trigger.
holFn, holTrigger :: Text -> Text -> Text
holFn schema table = schema <> ".detect_hol_" <> table <> "_fn"
holTrigger _ table = "detect_hol_" <> table

-- | DDL to install the gap-free HOL detector. A row trigger logs a violation
-- when a claim makes a job in-flight while another @attempts > 0@ job in its
-- group is still in flight at the trigger's clock time. Only a claim raises
-- @attempts@. The @attempts > 0@ filter excludes scheduled jobs.
holInstallSql :: Text -> Text -> [Text]
holInstallSql schema table =
  [ "SET client_min_messages TO warning"
  , "CREATE TABLE IF NOT EXISTS " <> holViolTbl schema table <> " (group_key TEXT NOT NULL, job_id BIGINT NOT NULL)"
  , "TRUNCATE " <> holViolTbl schema table
  , "CREATE OR REPLACE FUNCTION "
      <> holFn schema table
      <> "() RETURNS TRIGGER AS $t$ BEGIN IF NEW.not_visible_until > NOW()"
      <> " AND NOT NEW.suspended AND NEW.attempts > OLD.attempts AND NEW.group_key IS NOT NULL"
      <> " AND EXISTS (SELECT 1 FROM "
      <> tbl
      <> " WHERE group_key = NEW.group_key AND id <> NEW.id"
      <> " AND not_visible_until > clock_timestamp() AND NOT suspended AND attempts > 0)"
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
    tbl = jobQueueTable schema table

-- | DDL to remove the HOL detector trigger, function, and violations table.
holRemoveSql :: Text -> Text -> [Text]
holRemoveSql schema table =
  [ "SET client_min_messages TO warning"
  , "DROP TRIGGER IF EXISTS " <> holTrigger schema table <> " ON " <> jobQueueTable schema table
  , "DROP FUNCTION IF EXISTS " <> holFn schema table <> "()"
  , "DROP TABLE IF EXISTS " <> holViolTbl schema table
  ]

-- ---------------------------------------------------------------------------
-- Invariant checks (queried from the database after each command)
-- ---------------------------------------------------------------------------

-- | The invariants checked after every sequential step.
stepViolations :: Text -> Text -> WithConn -> IO [String]
stepViolations schema table withConn =
  mconcat
    <$> sequence
      [ exactViolations schema table withConn
      , orphanViolations schema table withConn
      , driftViolations schema table withConn
      , claimViolations schema table withConn
      ]

-- | First @id@ of a query, or Nothing if it returns no rows.
firstId :: WithConn -> Text -> IO (Maybe Int64)
firstId withConn sql = withConn $ \conn ->
  listToMaybe . map fromOnly <$> PG.query_ conn (toQuery sql)

-- | A scalar @count(*)@ query through the raw-connection accessor.
countQuery :: WithConn -> Text -> IO Int64
countQuery withConn sql = withConn $ \conn -> do
  [Only count] <- PG.query_ conn (toQuery sql)
  pure count

truncateHol :: Text -> Text -> WithConn -> IO ()
truncateHol schema table withConn = withConn $ \conn ->
  execute_ conn ("TRUNCATE " <> holViolTbl schema table)

-- | The invariants checked once a run settles.
settledViolations :: Text -> Text -> WithConn -> IO [String]
settledViolations schema table withConn =
  mconcat
    <$> sequence
      [ exactViolations schema table withConn
      , driftViolations schema table withConn
      , claimViolations schema table withConn
      ]

-- | Claim bookkeeping that must hold between statements:
--
--   * a suspended job names no holder
--   * each concurrency key's stored @in_flight@ equals a recount of claimed rows
claimViolations :: Text -> Text -> WithConn -> IO [String]
claimViolations schema table withConn = withConn $ \conn -> do
  held <- PG.query_ conn (toQuery heldSql)
  counts <- PG.query_ conn (toQuery countSql)
  pure $
    ["suspended job " <> show (jid :: Int64) <> " still names a holder" | Only jid <- held]
      <> [ "concurrency key " <> T.unpack key <> " stores in_flight " <> show stored <> ", recount " <> show live
         | (key, stored, live) <- counts :: [(Text, Int64, Int64)]
         ]
  where
    tbl = jobQueueTable schema table
    concTbl = arbiterConcurrencyTable schema
    heldSql = "SELECT id FROM " <> tbl <> " WHERE suspended AND claimed_by IS NOT NULL"
    countSql =
      "SELECT COALESCE(stored.concurrency_key, live.concurrency_key), COALESCE(stored.in_flight, 0)::bigint, COALESCE(live.claimed, 0)::bigint"
        <> " FROM (SELECT concurrency_key, in_flight FROM "
        <> concTbl
        <> " WHERE in_flight > 0) stored FULL OUTER JOIN (SELECT concurrency_key, COUNT(*) AS claimed FROM "
        <> tbl
        <> " WHERE concurrency_key IS NOT NULL AND claimed_by IS NOT NULL GROUP BY concurrency_key) live"
        <> " ON stored.concurrency_key = live.concurrency_key"
        <> " WHERE COALESCE(stored.in_flight, 0) <> COALESCE(live.claimed, 0)"

-- | Summary-drift oracle. Full-join the stored @_groups@ row against a fresh
-- recompute and report any column that disagrees.
driftViolations :: Text -> Text -> WithConn -> IO [String]
driftViolations schema table withConn = withConn $ \conn -> do
  rows <- PG.query_ conn (toQuery oracleSql)
  pure ["group " <> T.unpack groupName <> " summary drift: " <> T.unpack cols | (groupName, cols) <- rows]
  where
    tbl = jobQueueTable schema table
    groupsTbl = jobQueueGroupsTable schema table
    -- NOW() is evaluated once per statement. A past stored in_flight_until reads
    -- as free, as in the claim.
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
        <> ") AS drift FROM (SELECT group_key, min_priority, min_id, job_count, ready_count, next_due"
        <> ", CASE WHEN in_flight_until > NOW() THEN in_flight_until END AS in_flight_until FROM "
        <> groupsTbl
        <> " WHERE job_count > 0) summary FULL OUTER JOIN (SELECT group_key, "
        <> groupAggregates ""
        <> ", MAX(not_visible_until) FILTER (WHERE "
        <> inFlightPredicate ""
        <> ") AS in_flight_until FROM "
        <> tbl
        <> " WHERE group_key IS NOT NULL GROUP BY group_key) expected ON summary.group_key = expected.group_key) compared WHERE drift <> ''"

-- | Exact invariant violations, safe to sample live during concurrent churn:
--
--   * serialization: more than one claimed in-flight job per group
--   * attempt bound: a live job past its limit
--   * dedup uniqueness: two live jobs sharing a @dedup_key@
--   * concurrency cap: more claimed jobs on a key than its effective cap
--   * rate-limit integrity: a bucket's tokens outside [0, max]
exactViolations :: Text -> Text -> WithConn -> IO [String]
exactViolations schema table withConn = withConn $ \conn -> do
  serial <- PG.query_ conn (toQuery serialSql)
  over <- PG.query_ conn (toQuery overSql)
  dups <- PG.query_ conn (toQuery dupSql)
  conc <- PG.query_ conn (toQuery concSql)
  overSpent <- PG.query_ conn (toQuery rlSql)
  rlMax <- PG.query_ conn (toQuery rlMaxSql)
  serialMsgs <- traverse (diagnoseSerial conn) [groupName | Only groupName <- serial]
  pure $
    serialMsgs
      <> ["job " <> show (jid :: Int64) <> " exceeded its max_attempts" | Only jid <- over]
      <> ["duplicate live dedup_key " <> T.unpack key | Only key <- dups]
      <> ["concurrency cap exceeded for key " <> T.unpack key | Only key <- conc]
      <> ["rate-limit bucket " <> T.unpack key <> " over-spent (negative tokens)" | Only key <- overSpent]
      <> ["rate-limit bucket " <> T.unpack key <> " over-credited (tokens above max)" | Only key <- rlMax]
  where
    tbl = jobQueueTable schema table
    concPolicies = arbiterConcurrencyPoliciesTable schema
    rlBuckets = arbiterRateLimitsTable schema
    rlPolicies = arbiterRateLimitPoliciesTable schema
    groupsTbl = jobQueueGroupsTable schema table
    dma = T.pack (show defaultMaxAttempts)
    -- On a serialization violation, dump the offending group's jobs and summary
    -- row. Timestamps are ms relative to NOW.
    diagnoseSerial conn groupName = do
      jobs <- PG.query conn (toQuery jobsSql) (Only groupName)
      summ <- PG.query conn (toQuery summSql) (Only groupName)
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
        <> " AND NOT suspended AND claimed_by IS NOT NULL"
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
    -- policy override, else the policy default.
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
orphanViolations :: Text -> Text -> WithConn -> IO [String]
orphanViolations schema table withConn = withConn $ \conn -> do
  orphans <- PG.query_ conn (toQuery orphanSql)
  pure ["orphaned child " <> show (jid :: Int64) <> " (parent gone)" | Only jid <- orphans]
  where
    tbl = jobQueueTable schema table
    orphanSql =
      "SELECT child.id FROM "
        <> tbl
        <> " child WHERE child.parent_id IS NOT NULL AND NOT EXISTS"
        <> " (SELECT 1 FROM "
        <> tbl
        <> " parent WHERE parent.id = child.parent_id)"

-- | Install the gap-free HOL violation detector on a raw connection.
installHolDetector :: PG.Connection -> Text -> Text -> IO ()
installHolDetector conn schema table =
  traverse_ (execute_ conn) (holInstallSql schema table)

-- | One message per violation the HOL detector trigger recorded.
holViolations :: PG.Connection -> Text -> Text -> IO [String]
holViolations conn schema table = do
  rows <- PG.query_ conn (toQuery ("SELECT group_key, job_id FROM " <> holViolTbl schema table))
  pure
    [ "HOL violation: job " <> show (jid :: Int64) <> " claimed while group " <> T.unpack groupName <> " already in-flight"
    | (groupName, jid) <- rows
    ]

-- | Remove the HOL detector trigger, function, and violations table.
removeHolDetector :: PG.Connection -> Text -> Text -> IO ()
removeHolDetector conn schema table =
  traverse_ (execute_ conn) (holRemoveSql schema table)

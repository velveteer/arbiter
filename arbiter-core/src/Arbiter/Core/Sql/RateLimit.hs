{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | RateLimit SQL templates.
module Arbiter.Core.Sql.RateLimit
  ( defaultThrottleWaitSeconds
  , addRateLimitTokensSQL
  , pruneRateLimitBucketsSQL
  , resetRateLimitBucketsSQL
  , wakeThrottledJobsSQL
  , wakeThrottledJobsForKeySQL
  , wakeThrottledAcrossSQL
  , wakeThrottledBody
  , accruedTokensExpr
  , refilledExpr
  , refilledTokensExpr
  , listRateLimitPoliciesSQL
  , getRateLimitPolicySQL
  , rateLimitPolicyExistsSQL
  , rateLimitPoliciesSQL
  , listRateLimitBucketsSQL
  , updateRateLimitOverridesSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Admission (effectivePolicyCol, policyViewScope, touchColumn)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueTable)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTable, arbiterRateLimitsTable)
import Arbiter.Core.Sql.Jobs (throttledPredicateSQL, unionAllOverQueueTables)

-- | Deny-path wait (seconds) when no refill interval yields a real wait.
defaultThrottleWaitSeconds :: Double
defaultThrottleWaitSeconds = 0.5

-- | Add @amount@ to a key's bucket, clamped to @[0, max]@, seeding an absent bucket at
-- full. A no-op without a policy. Parameters: rate_limit_key, rate_limit_prefix, amount.
addRateLimitTokensSQL :: SchemaName -> Text
addRateLimitTokensSQL schema =
  let buckets = arbiterRateLimitsTable schema
      policies = arbiterRateLimitPoliciesTable schema
      mx = effectivePolicyCol "p" "max_tokens"
      rf = effectivePolicyCol "p" "refill_amount"
      iv = effectivePolicyCol "p" "interval"
      toppedUp =
        "GREATEST(0, "
          <> refilledExpr
            "(SELECT mx FROM pol)"
            "a.tokens + (SELECT amt FROM k)"
            "a.last_refill"
            "(SELECT rf FROM pol)"
            "(SELECT iv FROM pol)"
          <> ")"
   in [text|
        WITH k AS (SELECT ?::text AS key, ?::text AS prefix, ?::float8 AS amt),
        pol AS (
          SELECT ${mx} AS mx, ${rf} AS rf, ${iv} AS iv
          FROM ${policies} p WHERE p.prefix_id = (SELECT prefix FROM k)
        )
        INSERT INTO ${buckets} AS a (rate_limit_key, policy_prefix, tokens, last_refill)
        SELECT (SELECT key FROM k), (SELECT prefix FROM k), (SELECT mx FROM pol), NOW()
        FROM pol
        ON CONFLICT (rate_limit_key) DO UPDATE
          SET tokens = ${toppedUp},
              last_refill = NOW()
      |]

-- | Delete idle buckets that have refilled to max, since a full bucket re-seeds
-- identically on next use. Parameter: idle_seconds.
pruneRateLimitBucketsSQL :: SchemaName -> Text
pruneRateLimitBucketsSQL schema =
  let buckets = arbiterRateLimitsTable schema
      policies = arbiterRateLimitPoliciesTable schema
      accrued = accruedTokensExpr "b.last_refill" (effectivePolicyCol "p" "refill_amount") (effectivePolicyCol "p" "interval")
      effMax = effectivePolicyCol "p" "max_tokens"
   in [text|
        DELETE FROM ${buckets} b
        USING ${policies} p
        WHERE p.prefix_id = b.policy_prefix
          AND b.last_refill < NOW() - (?::float8 * interval '1 second')
          AND b.tokens + ${accrued} >= ${effMax}
      |]

-- | Refill a prefix's buckets to full. The fixed-window reset, which also wakes jobs, is the HighLevel resetRateLimitBuckets. Parameter: policy_prefix.
resetRateLimitBucketsSQL :: SchemaName -> Text
resetRateLimitBucketsSQL schema =
  let buckets = arbiterRateLimitsTable schema
      policies = arbiterRateLimitPoliciesTable schema
      effMax = effectivePolicyCol "p" "max_tokens"
   in [text|
        UPDATE ${buckets} b
        SET tokens = ${effMax}, last_refill = NOW()
        FROM ${policies} p
        WHERE p.prefix_id = b.policy_prefix AND b.policy_prefix = ?
      |]

-- | Clear the rate-limit deferral on a prefix's throttled jobs so a window reset
-- releases work parked mid-window instead of leaving it until a full interval elapses.
-- One statement over all given queue tables. Parameter: policy_prefix, once per table.
wakeThrottledJobsSQL :: SchemaName -> [TableName] -> Text
wakeThrottledJobsSQL schema tableNames = wakeThrottledAcrossSQL schema tableNames ""

-- | Like 'wakeThrottledJobsSQL' but for one key, after a top-up. Params per table: prefix, key.
wakeThrottledJobsForKeySQL :: SchemaName -> [TableName] -> Text
wakeThrottledJobsForKeySQL schema tableNames = wakeThrottledAcrossSQL schema tableNames " AND rate_limit_key = ?"

-- | One wake UPDATE CTE per queue table, summed, so a wake is a single round trip.
wakeThrottledAcrossSQL :: SchemaName -> [TableName] -> Text -> Text
wakeThrottledAcrossSQL schema tableNames keyClause =
  let wakes = [("w" <> T.pack (show i), wakeThrottledBody schema t keyClause) | (i, t) <- zip [0 :: Int ..] tableNames]
      ctes = T.intercalate ", " [name <> " AS (" <> body <> " RETURNING 1)" | (name, body) <- wakes]
      total = T.intercalate " + " ["(SELECT COUNT(*) FROM " <> name <> ")" | (name, _) <- wakes]
   in "WITH " <> ctes <> " SELECT (" <> total <> ")::int8 AS count"

-- | Shared wake UPDATE. @keyClause@ injects an optional @AND rate_limit_key = ?@.
wakeThrottledBody :: SchemaName -> TableName -> Text -> Text
wakeThrottledBody schema tableName keyClause =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET not_visible_until = NOW(), throttled_until = NULL, updated_at = NOW()
        WHERE rate_limit_prefix = ?${keyClause} AND ${throttledPredicateSQL} AND NOT suspended
      |]

-- | Tokens accrued since @lastRefill@ at @refill@ per @interval@ seconds. Arguments
-- are SQL expressions.
accruedTokensExpr :: Text -> Text -> Text -> Text
accruedTokensExpr lastRefill refill interval =
  "COALESCE(EXTRACT(EPOCH FROM (NOW() - "
    <> lastRefill
    <> ")) / NULLIF("
    <> interval
    <> ", 0) * "
    <> refill
    <> ", 0)"

-- | A bucket's lazily-refilled token count, capped at @maxTokens@. Arguments are SQL
-- expressions, so callers pass either CTE references or effective policy columns.
refilledExpr :: Text -> Text -> Text -> Text -> Text -> Text
refilledExpr maxTokens tokens lastRefill refill interval =
  "LEAST(" <> maxTokens <> ", " <> tokens <> " + " <> accruedTokensExpr lastRefill refill interval <> ")"

-- | A bucket's lazily-refilled current token count, for bucket alias @b@ and policy
-- alias @p@. Mirrors the gate's accrual so observability reflects tokens available now.
refilledTokensExpr :: Text -> Text -> Text
refilledTokensExpr b p =
  refilledExpr
    (effectivePolicyCol p "max_tokens")
    (b <> ".tokens")
    (b <> ".last_refill")
    (effectivePolicyCol p "refill_amount")
    (effectivePolicyCol p "interval")

-- | List every policy with its default/override params and per-prefix bucket
-- aggregates (count, min and average of lazily-refilled tokens, live throttled
-- count over the given queue tables). No parameters.
listRateLimitPoliciesSQL :: SchemaName -> [TableName] -> Text
listRateLimitPoliciesSQL schema tableNames = rateLimitPoliciesSQL schema tableNames False

-- | Single-prefix variant of 'listRateLimitPoliciesSQL'. Parameter: prefix.
getRateLimitPolicySQL :: SchemaName -> [TableName] -> Text
getRateLimitPolicySQL schema tableNames = rateLimitPoliciesSQL schema tableNames True

-- | Whether a rate-limit policy exists for a prefix. Parameter: prefix.
rateLimitPolicyExistsSQL :: SchemaName -> Text
rateLimitPolicyExistsSQL schema =
  "SELECT EXISTS (SELECT 1 FROM " <> arbiterRateLimitPoliciesTable schema <> " WHERE prefix_id = ?) AS result"

rateLimitPoliciesSQL :: SchemaName -> [TableName] -> Bool -> Text
rateLimitPoliciesSQL schema tableNames single =
  let policies = arbiterRateLimitPoliciesTable schema
      buckets = arbiterRateLimitsTable schema
      refilled = refilledTokensExpr "b" "pp"
      (kCte, aggWhere, scope) = policyViewScope single "b.policy_prefix"
      thrScope = if single then " AND rate_limit_prefix = (SELECT prefix FROM k)" else "" :: Text
      thrUnion = unionAllOverQueueTables schema tableNames $ \t ->
        "SELECT rate_limit_prefix AS prefix, COUNT(*)::int8 AS c FROM "
          <> t
          <> " WHERE "
          <> throttledPredicateSQL
          <> " AND NOT suspended AND rate_limit_prefix IS NOT NULL"
          <> thrScope
          <> " GROUP BY rate_limit_prefix"
      (thrCol, thrJoin)
        | null tableNames = ("0::int8 AS throttled_count", "")
        | otherwise =
            ( "COALESCE(thr.cnt, 0) AS throttled_count"
            , "LEFT JOIN (SELECT prefix, SUM(c)::int8 AS cnt FROM ("
                <> thrUnion
                <> ") u GROUP BY prefix) thr ON thr.prefix = p.prefix_id"
            )
   in [text|
        ${kCte}
        SELECT p.prefix_id,
               p.default_max_tokens, p.default_refill_amount, p.default_interval,
               p.override_max_tokens, p.override_refill_amount, p.override_interval,
               COALESCE(agg.bucket_count, 0) AS bucket_count,
               ${thrCol},
               agg.min_tokens, agg.avg_tokens
        FROM ${policies} p
        LEFT JOIN (
          SELECT b.policy_prefix, COUNT(*) AS bucket_count,
                 MIN(${refilled}) AS min_tokens, AVG(${refilled}) AS avg_tokens
          FROM ${buckets} b JOIN ${policies} pp ON pp.prefix_id = b.policy_prefix
          ${aggWhere}
          GROUP BY b.policy_prefix
        ) agg ON agg.policy_prefix = p.prefix_id
        ${thrJoin}
        ${scope}
      |]

-- | List a prefix's buckets with effective max and lazily-refilled fill fraction,
-- paginated. Parameters: policy_prefix, limit, offset.
listRateLimitBucketsSQL :: SchemaName -> Text
listRateLimitBucketsSQL schema =
  let policies = arbiterRateLimitPoliciesTable schema
      buckets = arbiterRateLimitsTable schema
      refilled = refilledTokensExpr "b" "p"
      effMax = effectivePolicyCol "p" "max_tokens"
   in [text|
        SELECT b.rate_limit_key, b.policy_prefix,
               ${refilled} AS tokens,
               ${effMax} AS max_tokens,
               ${refilled} / NULLIF(${effMax}, 0) AS fill_fraction,
               b.last_refill
        FROM ${buckets} b
        JOIN ${policies} p ON p.prefix_id = b.policy_prefix
        WHERE b.policy_prefix = ?
        ORDER BY fill_fraction ASC NULLS LAST, b.rate_limit_key
        LIMIT ? OFFSET ?
      |]

-- | Set or clear a policy's override params. Each field carries a boolean "touch"
-- flag and a nullable value, so an untouched field keeps its current column value.
-- Parameters: touch_max, max, touch_refill, refill, touch_interval, interval, prefix.
updateRateLimitOverridesSQL :: SchemaName -> Text
updateRateLimitOverridesSQL schema =
  let policies = arbiterRateLimitPoliciesTable schema
      touches =
        T.intercalate
          ", "
          [touchColumn c "float8" | c <- ["override_max_tokens", "override_refill_amount", "override_interval"]]
   in [text| UPDATE ${policies} SET ${touches} WHERE prefix_id = ? |]

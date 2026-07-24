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
  , refilledExpr
  , listRateLimitPoliciesSQL
  , getRateLimitPolicySQL
  , rateLimitPolicyExistsSQL
  , listRateLimitBucketsSQL
  , updateRateLimitOverridesSQL
  ) where

import Control.Monad (join)
import Data.Int (Int64)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Core.Admission (effectivePolicyCol, policyViewScope)
import Arbiter.Core.Codec (Col (..), col, rateLimitBucketCodec, rateLimitPolicyViewCodec)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueTable)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTable, arbiterRateLimitsTable)
import Arbiter.Core.RateLimit.Stats (RateLimitBucketView, RateLimitPolicyView)
import Arbiter.Core.Sql.Jobs (throttledPredicateSQL, unionAllOverQueueTables)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, mwhen, raw, rows, sepBy)

-- | Deny-path wait (seconds) when no refill interval yields a real wait.
defaultThrottleWaitSeconds :: Double
defaultThrottleWaitSeconds = 0.5

-- | Add @amount@ to a key's bucket, clamped to @[0, max]@, seeding an absent bucket at
-- full. A no-op without a policy.
addRateLimitTokensSQL :: SchemaName -> Text -> Text -> Double -> Query ()
addRateLimitTokensSQL schema key prefix amount =
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
   in [sql|
        WITH k AS (SELECT #{key :: CText}::text AS key, #{prefix :: CText}::text AS prefix, #{amount :: CFloat8}::float8 AS amt),
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
-- identically on next use.
pruneRateLimitBucketsSQL :: SchemaName -> Double -> Query ()
pruneRateLimitBucketsSQL schema idleSeconds =
  let buckets = arbiterRateLimitsTable schema
      policies = arbiterRateLimitPoliciesTable schema
      accrued = accruedTokensExpr "b.last_refill" (effectivePolicyCol "p" "refill_amount") (effectivePolicyCol "p" "interval")
      effMax = effectivePolicyCol "p" "max_tokens"
   in [sql|
        DELETE FROM ${buckets} b
        USING ${policies} p
        WHERE p.prefix_id = b.policy_prefix
          AND b.last_refill < NOW() - (#{idleSeconds :: CFloat8}::float8 * interval '1 second')
          AND b.tokens + ${accrued} >= ${effMax}
      |]

-- | Refill a prefix's buckets to full. The fixed-window reset, which also wakes jobs, is the HighLevel resetRateLimitBuckets.
resetRateLimitBucketsSQL :: SchemaName -> Text -> Query ()
resetRateLimitBucketsSQL schema prefix =
  let buckets = arbiterRateLimitsTable schema
      policies = arbiterRateLimitPoliciesTable schema
      effMax = effectivePolicyCol "p" "max_tokens"
   in [sql|
        UPDATE ${buckets} b
        SET tokens = ${effMax}, last_refill = NOW()
        FROM ${policies} p
        WHERE p.prefix_id = b.policy_prefix AND b.policy_prefix = #{prefix :: CText}
      |]

-- | Clear the rate-limit deferral on a prefix's throttled jobs so a window reset
-- releases work parked mid-window instead of leaving it until a full interval elapses.
-- One statement over all given queue tables.
wakeThrottledJobsSQL :: SchemaName -> [TableName] -> Text -> Query Int64
wakeThrottledJobsSQL schema tableNames prefix =
  wakeThrottledAcrossSQL [wakeThrottledBody schema t prefix mempty | t <- tableNames]

-- | Like 'wakeThrottledJobsSQL' but for one key, after a top-up.
wakeThrottledJobsForKeySQL :: SchemaName -> [TableName] -> Text -> Text -> Query Int64
wakeThrottledJobsForKeySQL schema tableNames prefix key =
  wakeThrottledAcrossSQL
    [wakeThrottledBody schema t prefix [sql|AND rate_limit_key = #{key :: CText}|] | t <- tableNames]

-- | One wake UPDATE CTE per queue table, summed, so a wake is a single round trip.
wakeThrottledAcrossSQL :: [Query ()] -> Query Int64
wakeThrottledAcrossSQL bodies =
  let named = zip [0 :: Int ..] bodies
      cteFrag = sepBy ", " [raw ("w" <> T.pack (show i) <> " AS (") <> b <> raw " RETURNING 1)" | (i, b) <- named]
      total = T.intercalate " + " ["(SELECT COUNT(*) FROM w" <> T.pack (show i) <> ")" | (i, _) <- named]
   in rows (col "count" CInt8) (raw "WITH " <> cteFrag <> raw (" SELECT (" <> total <> ")::int8 AS count"))

-- | Shared wake UPDATE for a prefix, optionally narrowed by @keyFrag@ (e.g. one key).
wakeThrottledBody :: SchemaName -> TableName -> Text -> Query () -> Query ()
wakeThrottledBody schema tableName prefix keyFrag =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET not_visible_until = NOW(), throttled_until = NULL, updated_at = NOW()
        WHERE rate_limit_prefix = #{prefix :: CText}
          ${keyFrag}
          AND ${throttledPredicateSQL} AND NOT suspended
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
-- count over the given queue tables).
listRateLimitPoliciesSQL :: SchemaName -> [TableName] -> Query RateLimitPolicyView
listRateLimitPoliciesSQL schema tableNames = rateLimitPoliciesSQL schema tableNames Nothing

-- | Single-prefix variant of 'listRateLimitPoliciesSQL'.
getRateLimitPolicySQL :: SchemaName -> [TableName] -> Text -> Query RateLimitPolicyView
getRateLimitPolicySQL schema tableNames prefix = rateLimitPoliciesSQL schema tableNames (Just prefix)

-- | Whether a rate-limit policy exists for a prefix.
rateLimitPolicyExistsSQL :: SchemaName -> Text -> Query Bool
rateLimitPolicyExistsSQL schema prefix =
  let tbl = arbiterRateLimitPoliciesTable schema
   in [sql|SELECT EXISTS (SELECT 1 FROM ${tbl} WHERE prefix_id = #{prefix :: CText}) AS @{result :: CBool}|]

rateLimitPoliciesSQL :: SchemaName -> [TableName] -> Maybe Text -> Query RateLimitPolicyView
rateLimitPoliciesSQL schema tableNames mPrefix =
  let policies = arbiterRateLimitPoliciesTable schema
      buckets = arbiterRateLimitsTable schema
      refilled = refilledTokensExpr "b" "pp"
      single = isJust mPrefix
      kCte = foldMap (\p -> [sql|WITH k AS (SELECT #{p :: CText}::text AS prefix)|]) mPrefix
      (aggWhere, scope) = policyViewScope single "b.policy_prefix"
      thrScope = mwhen single " AND rate_limit_prefix = (SELECT prefix FROM k)" :: Text
      thrUnion = unionAllOverQueueTables schema tableNames $ \_ t ->
        "SELECT rate_limit_prefix AS prefix, COUNT(*)::int8 AS c FROM "
          <> t
          <> " WHERE "
          <> throttledPredicateSQL
          <> " AND NOT suspended AND rate_limit_prefix IS NOT NULL"
          <> thrScope
          <> " GROUP BY rate_limit_prefix"
      (thrCol, thrJoin)
        | null tableNames = ("0::int8 AS throttled_count" :: Text, "" :: Text)
        | otherwise =
            ( "COALESCE(thr.cnt, 0) AS throttled_count"
            , "LEFT JOIN (SELECT prefix, SUM(c)::int8 AS cnt FROM ("
                <> thrUnion
                <> ") u GROUP BY prefix) thr ON thr.prefix = p.prefix_id"
            )
   in rows
        rateLimitPolicyViewCodec
        [sql|
        ${kCte}
        SELECT p.prefix_id,
               p.default_max_tokens, p.default_refill_amount, p.default_interval,
               p.override_max_tokens, p.override_refill_amount, p.override_interval,
               COALESCE(agg.bucket_count, 0) AS bucket_count,
               ${thrCol},
               agg.min_tokens, agg.avg_tokens
        FROM ${policies} p
        LEFT JOIN (
          SELECT policy_prefix, COUNT(*) AS bucket_count,
                 MIN(tok) AS min_tokens, AVG(tok) AS avg_tokens
          FROM (
            SELECT b.policy_prefix, ${refilled} AS tok
            FROM ${buckets} b JOIN ${policies} pp ON pp.prefix_id = b.policy_prefix
            ${aggWhere}
          ) r
          GROUP BY policy_prefix
        ) agg ON agg.policy_prefix = p.prefix_id
        ${thrJoin}
        ${scope}
      |]

-- | List a prefix's buckets with effective max and lazily-refilled fill fraction,
-- paginated.
listRateLimitBucketsSQL :: SchemaName -> Text -> Int64 -> Int64 -> Query RateLimitBucketView
listRateLimitBucketsSQL schema prefix limit offset =
  let policies = arbiterRateLimitPoliciesTable schema
      buckets = arbiterRateLimitsTable schema
      refilled = refilledTokensExpr "b" "p"
      effMax = effectivePolicyCol "p" "max_tokens"
   in rows
        rateLimitBucketCodec
        [sql|
          SELECT rate_limit_key, policy_prefix, tokens, max_tokens,
                 tokens / NULLIF(max_tokens, 0) AS fill_fraction,
                 last_refill
          FROM (
            SELECT b.rate_limit_key, b.policy_prefix,
                   ${refilled} AS tokens,
                   ${effMax} AS max_tokens,
                   b.last_refill
            FROM ${buckets} b
            JOIN ${policies} p ON p.prefix_id = b.policy_prefix
            WHERE b.policy_prefix = #{prefix :: CText}
          ) r
          ORDER BY fill_fraction ASC NULLS LAST, rate_limit_key
          LIMIT #{limit :: CInt8} OFFSET #{offset :: CInt8}
        |]

-- | Set or clear a policy's override params. @Nothing@ leaves a field untouched.
-- @Just v@ writes @v@ (a null clears the override back to the default).
updateRateLimitOverridesSQL
  :: SchemaName -> Maybe (Maybe Double) -> Maybe (Maybe Double) -> Maybe (Maybe Double) -> Text -> Query ()
updateRateLimitOverridesSQL schema mMax mRefill mIv prefix =
  let policies = arbiterRateLimitPoliciesTable schema
      touch :: Text -> Maybe (Maybe Double) -> Query ()
      touch name field =
        let flag = isJust field
            val = join field
         in [sql|${name} = CASE WHEN #{flag :: CBool}::boolean THEN #{val :: Maybe CFloat8}::float8 ELSE ${name} END|]
      setFrag =
        sepBy
          ", "
          [ touch "override_max_tokens" mMax
          , touch "override_refill_amount" mRefill
          , touch "override_interval" mIv
          ]
   in [sql| UPDATE ${policies} SET ${setFrag} WHERE prefix_id = #{prefix :: CText} |]

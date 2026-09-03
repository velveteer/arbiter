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
import NeatInterpolation (text)

import Arbiter.Core.Admission (effectivePolicyCol)
import Arbiter.Core.Codec (rateLimitBucketCodec, rateLimitPolicyViewCodec)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueTable)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTable, arbiterRateLimitsTable)
import Arbiter.Core.RateLimit.Stats (RateLimitBucketView, RateLimitPolicyView)
import Arbiter.Core.Sql.Jobs (throttledPredicateSQL, unionAllOverQueueTables)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows, sepBy)

-- | Deny-path wait (seconds) when no refill interval yields a real wait.
defaultThrottleWaitSeconds :: Double
defaultThrottleWaitSeconds = 0.5

-- | Add @amount@ to a key's bucket, clamped to @[0, max]@, seeding an absent bucket at
-- full. A no-op without a policy.
addRateLimitTokensSQL :: SchemaName -> Text -> Text -> Double -> Query ()
addRateLimitTokensSQL schema key prefix amount =
  let buckets = arbiterRateLimitsTable schema
      policies = arbiterRateLimitPoliciesTable schema
      effMax = effectivePolicyCol "policy" "max_tokens"
      effRefill = effectivePolicyCol "policy" "refill_amount"
      effInterval = effectivePolicyCol "policy" "interval"
      refilled =
        refilledExpr
          "(SELECT max_tokens FROM pol)"
          "bucket.tokens + (SELECT amt FROM input)"
          "bucket.last_refill"
          "(SELECT refill_amount FROM pol)"
          "(SELECT refill_interval FROM pol)"
   in [sql|
        WITH input AS (
          SELECT #{key :: CText}::text AS key, #{prefix :: CText}::text AS prefix, #{amount :: CFloat8}::float8 AS amt
        ),
        pol AS (
          SELECT ${effMax} AS max_tokens, ${effRefill} AS refill_amount, ${effInterval} AS refill_interval
          FROM ${policies} policy WHERE policy.prefix_id = (SELECT prefix FROM input)
        )
        INSERT INTO ${buckets} AS bucket (rate_limit_key, policy_prefix, tokens, last_refill)
        SELECT (SELECT key FROM input), (SELECT prefix FROM input), (SELECT max_tokens FROM pol), NOW()
        FROM pol
        ON CONFLICT (rate_limit_key) DO UPDATE
          SET tokens = GREATEST(0, ${refilled}),
              last_refill = NOW()
      |]

-- | Delete idle buckets that have refilled to max.
pruneRateLimitBucketsSQL :: SchemaName -> Double -> Query ()
pruneRateLimitBucketsSQL schema idleSeconds =
  let buckets = arbiterRateLimitsTable schema
      policies = arbiterRateLimitPoliciesTable schema
      effMax = effectivePolicyCol "policy" "max_tokens"
   in [sql|
        DELETE FROM ${buckets} bucket
        USING ${policies} policy
        WHERE policy.prefix_id = bucket.policy_prefix
          AND bucket.last_refill < NOW() - (#{idleSeconds :: CFloat8}::float8 * interval '1 second')
          AND ${refilledBucketTokens} >= ${effMax}
      |]

-- | Refill a prefix's buckets to full. The fixed-window reset, which also wakes jobs,
-- is the HighLevel resetRateLimitBuckets.
resetRateLimitBucketsSQL :: SchemaName -> Text -> Query ()
resetRateLimitBucketsSQL schema prefix =
  let buckets = arbiterRateLimitsTable schema
      policies = arbiterRateLimitPoliciesTable schema
      effMax = effectivePolicyCol "policy" "max_tokens"
   in [sql|
        UPDATE ${buckets} bucket
        SET tokens = ${effMax}, last_refill = NOW()
        FROM ${policies} policy
        WHERE policy.prefix_id = bucket.policy_prefix AND bucket.policy_prefix = #{prefix :: CText}
      |]

-- | Clear the rate-limit deferral on a prefix's throttled jobs. One statement over
-- all given queue tables.
wakeThrottledJobsSQL :: SchemaName -> [TableName] -> Text -> Query Int64
wakeThrottledJobsSQL schema tableNames prefix =
  wakeThrottledAcrossSQL [wakeThrottledBody schema tableName prefix mempty | tableName <- tableNames]

-- | Like 'wakeThrottledJobsSQL' but for one key, after a top-up.
wakeThrottledJobsForKeySQL :: SchemaName -> [TableName] -> Text -> Text -> Query Int64
wakeThrottledJobsForKeySQL schema tableNames prefix key =
  wakeThrottledAcrossSQL
    [wakeThrottledBody schema tableName prefix [sql|AND rate_limit_key = #{key :: CText}|] | tableName <- tableNames]

-- | One wake UPDATE CTE per queue table, summed.
wakeThrottledAcrossSQL :: [Query ()] -> Query Int64
wakeThrottledAcrossSQL bodies =
  let named = [(T.pack ("wake_" <> show index), body) | (index, body) <- zip [0 :: Int ..] bodies]
      wakeCtes = sepBy ", " [[sql|${cteName} AS (${body} RETURNING 1)|] | (cteName, body) <- named]
      total = T.intercalate " + " [[text|(SELECT COUNT(*) FROM ${cteName})|] | (cteName, _) <- named]
   in [sql|WITH ${wakeCtes} SELECT (${total})::int8 AS @{count :: CInt8}|]

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
  [text|COALESCE(EXTRACT(EPOCH FROM (NOW() - ${lastRefill})) / NULLIF(${interval}, 0) * ${refill}, 0)|]

-- | A bucket's lazily-refilled token count, capped at @maxTokens@. Arguments are SQL
-- expressions.
refilledExpr :: Text -> Text -> Text -> Text -> Text -> Text
refilledExpr maxTokens tokens lastRefill refill interval =
  let accrued = accruedTokensExpr lastRefill refill interval
   in [text|LEAST(${maxTokens}, ${tokens} + ${accrued})|]

-- | The lazily-refilled token count of the bucket at alias @bucket@ under the policy
-- at alias @policy@. Mirrors the gate's accrual.
refilledBucketTokens :: Text
refilledBucketTokens =
  refilledExpr
    (effectivePolicyCol "policy" "max_tokens")
    "bucket.tokens"
    "bucket.last_refill"
    (effectivePolicyCol "policy" "refill_amount")
    (effectivePolicyCol "policy" "interval")

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

-- | The policy views, every policy or the one a prefix names.
rateLimitPoliciesSQL :: SchemaName -> [TableName] -> Maybe Text -> Query RateLimitPolicyView
rateLimitPoliciesSQL schema tableNames mPrefix =
  let policies = arbiterRateLimitPoliciesTable schema
      buckets = arbiterRateLimitsTable schema
      throttledPerTable = unionAllOverQueueTables schema tableNames $ \_ table ->
        [text|
          SELECT rate_limit_prefix AS prefix, COUNT(*)::int8 AS throttled
          FROM ${table}
          WHERE ${throttledPredicateSQL} AND NOT suspended AND rate_limit_prefix IS NOT NULL
            AND ((SELECT prefix FROM target) IS NULL OR rate_limit_prefix = (SELECT prefix FROM target))
          GROUP BY rate_limit_prefix
        |]
      throttledJoin =
        [text|
          LEFT JOIN (
            SELECT prefix, SUM(throttled)::int8 AS throttled
            FROM (${throttledPerTable}) per_table
            GROUP BY prefix
          ) throttled ON throttled.prefix = policy.prefix_id
        |]
      throttledCol, throttledJoinClause :: Text
      (throttledCol, throttledJoinClause) = case tableNames of
        [] -> ("0::int8 AS throttled_count", "")
        _ -> ("COALESCE(throttled.throttled, 0) AS throttled_count", throttledJoin)
   in rows
        rateLimitPolicyViewCodec
        [sql|
        WITH target AS (SELECT #{mPrefix :: Maybe CText}::text AS prefix)
        SELECT policy.prefix_id,
               policy.default_max_tokens, policy.default_refill_amount, policy.default_interval,
               policy.override_max_tokens, policy.override_refill_amount, policy.override_interval,
               COALESCE(agg.bucket_count, 0) AS bucket_count,
               ${throttledCol},
               agg.min_tokens, agg.avg_tokens
        FROM ${policies} policy
        LEFT JOIN (
          SELECT policy_prefix, COUNT(*) AS bucket_count,
                 MIN(tokens) AS min_tokens, AVG(tokens) AS avg_tokens
          FROM (
            SELECT bucket.policy_prefix, ${refilledBucketTokens} AS tokens
            FROM ${buckets} bucket JOIN ${policies} policy ON policy.prefix_id = bucket.policy_prefix
            WHERE (SELECT prefix FROM target) IS NULL OR bucket.policy_prefix = (SELECT prefix FROM target)
          ) refilled_bucket
          GROUP BY policy_prefix
        ) agg ON agg.policy_prefix = policy.prefix_id
        ${throttledJoinClause}
        WHERE (SELECT prefix FROM target) IS NULL OR policy.prefix_id = (SELECT prefix FROM target)
        ORDER BY policy.prefix_id
      |]

-- | List a prefix's buckets with effective max and lazily-refilled fill fraction,
-- paginated.
listRateLimitBucketsSQL :: SchemaName -> Text -> Int64 -> Int64 -> Query RateLimitBucketView
listRateLimitBucketsSQL schema prefix limit offset =
  let policies = arbiterRateLimitPoliciesTable schema
      buckets = arbiterRateLimitsTable schema
      effMax = effectivePolicyCol "policy" "max_tokens"
   in rows
        rateLimitBucketCodec
        [sql|
          SELECT rate_limit_key, policy_prefix, tokens, max_tokens,
                 tokens / NULLIF(max_tokens, 0) AS fill_fraction,
                 last_refill
          FROM (
            SELECT bucket.rate_limit_key, bucket.policy_prefix,
                   ${refilledBucketTokens} AS tokens,
                   ${effMax} AS max_tokens,
                   bucket.last_refill
            FROM ${buckets} bucket
            JOIN ${policies} policy ON policy.prefix_id = bucket.policy_prefix
            WHERE bucket.policy_prefix = #{prefix :: CText}
          ) refilled_bucket
          ORDER BY fill_fraction ASC NULLS LAST, rate_limit_key
          LIMIT #{limit :: CInt8} OFFSET #{offset :: CInt8}
        |]

-- | Set or clear a policy's override params. @Nothing@ leaves a field untouched.
-- @Just v@ writes @v@ (a null clears the override back to the default).
updateRateLimitOverridesSQL
  :: SchemaName -> Maybe (Maybe Double) -> Maybe (Maybe Double) -> Maybe (Maybe Double) -> Text -> Query ()
updateRateLimitOverridesSQL schema mMax mRefill mInterval prefix =
  let policies = arbiterRateLimitPoliciesTable schema
      setMax = isJust mMax
      maxTokens = join mMax
      setRefill = isJust mRefill
      refill = join mRefill
      setInterval = isJust mInterval
      interval = join mInterval
   in [sql|
        UPDATE ${policies}
        SET override_max_tokens = CASE WHEN #{setMax :: CBool}::boolean
                                       THEN #{maxTokens :: Maybe CFloat8}::float8
                                       ELSE override_max_tokens END,
            override_refill_amount = CASE WHEN #{setRefill :: CBool}::boolean
                                          THEN #{refill :: Maybe CFloat8}::float8
                                          ELSE override_refill_amount END,
            override_interval = CASE WHEN #{setInterval :: CBool}::boolean
                                     THEN #{interval :: Maybe CFloat8}::float8
                                     ELSE override_interval END
        WHERE prefix_id = #{prefix :: CText}
      |]

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Conversion of declared 'Policy' values to upsertable rows, plus DDL for the
-- policies table, bucket table, and job columns. No database execution here.
module Arbiter.Core.RateLimit.Schema
  ( -- * Policy rows
    PolicyRow (..)
  , toPolicyRow

    -- * Table name helpers
  , arbiterRateLimitPoliciesTable
  , arbiterRateLimitPoliciesTableName
  , arbiterRateLimitsTable
  , arbiterRateLimitsTableName

    -- * DDL
  , createRateLimitPoliciesTableSQL
  , createRateLimitsTableSQL
  , alterRateLimitsDurabilitySQL
  , upsertPolicyRowSQL
  , addRateLimitColumnsSQL
  , addRateLimitCostColumnSQL
  , createThrottledIndexSQL
  , createRateLimitBucketTriggerFunctionsSQL
  , createRateLimitBucketTriggersSQL
  , bucketSeedInsert
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Admission (effectivePolicyCol, policyUpsertSQL)
import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , jobQueueDLQTable
  , jobQueueTable
  , maintenanceFunctionNames
  , statementTriggerSQL
  )
import Arbiter.Core.RateLimit.Spec (Durability (..), Policy (..))
import Arbiter.Core.SqlLiterals (doubleLiteral, quoteIdentifier, textLiteral)

-- | A token-bucket policy as upsertable row fields.
data PolicyRow = PolicyRow
  { prefixId :: Text
  , maxTokens :: Double
  , refillAmt :: Double
  , interval :: Double
  }
  deriving stock (Eq, Show)

-- | A policy in its stored form.
toPolicyRow :: Policy -> PolicyRow
toPolicyRow (Policy prefix burst refill period) =
  PolicyRow {prefixId = prefix, maxTokens = burst, refillAmt = refill, interval = realToFrac period}

-- | Qualified name of the app-global policies table.
arbiterRateLimitPoliciesTable :: SchemaName -> Text
arbiterRateLimitPoliciesTable schemaName =
  quoteIdentifier schemaName <> "." <> arbiterRateLimitPoliciesTableName

-- | Bare name of the policies table, for catalog lookups by relname.
arbiterRateLimitPoliciesTableName :: Text
arbiterRateLimitPoliciesTableName = "arbiter_rate_limit_policies"

-- | Bare (unqualified) name of the bucket table, for catalog lookups by relname.
arbiterRateLimitsTableName :: Text
arbiterRateLimitsTableName = "arbiter_rate_limits"

-- | Qualified name of the single bucket table. Its WAL durability is a table-level
-- property reconciled by the migration.
arbiterRateLimitsTable :: SchemaName -> Text
arbiterRateLimitsTable schemaName =
  quoteIdentifier schemaName <> "." <> arbiterRateLimitsTableName

-- | DDL for the policies table. @default_*@ is migration-owned. @override_*@ is
-- management-owned. The effective params are @COALESCE(override, default)@.
createRateLimitPoliciesTableSQL :: SchemaName -> Text
createRateLimitPoliciesTableSQL schemaName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> arbiterRateLimitPoliciesTable schemaName <> " ("
    , "  prefix_id TEXT PRIMARY KEY,"
    , "  default_max_tokens DOUBLE PRECISION NOT NULL CHECK (default_max_tokens >= 0),"
    , "  default_refill_amount DOUBLE PRECISION NOT NULL CHECK (default_refill_amount >= 0),"
    , "  default_interval DOUBLE PRECISION NOT NULL CHECK (default_interval > 0),"
    , "  override_max_tokens DOUBLE PRECISION CHECK (override_max_tokens >= 0),"
    , "  override_refill_amount DOUBLE PRECISION CHECK (override_refill_amount >= 0),"
    , "  override_interval DOUBLE PRECISION CHECK (override_interval > 0)"
    , ");"
    ]

-- | DDL for the bucket table, always created @UNLOGGED@.
createRateLimitsTableSQL :: SchemaName -> Text
createRateLimitsTableSQL schemaName =
  T.unlines
    [ "CREATE UNLOGGED TABLE IF NOT EXISTS " <> arbiterRateLimitsTable schemaName <> " ("
    , "  rate_limit_key TEXT PRIMARY KEY,"
    , "  policy_prefix TEXT NOT NULL,"
    , "  tokens DOUBLE PRECISION NOT NULL,"
    , "  last_refill TIMESTAMPTZ NOT NULL"
    , ") WITH (fillfactor = 80);"
    ]

-- | Converge the bucket table's WAL persistence to a durability. Rewrites the
-- table under @ACCESS EXCLUSIVE@. Callers issue it when the durability differs
-- from the current state.
alterRateLimitsDurabilitySQL :: Durability -> SchemaName -> Text
alterRateLimitsDurabilitySQL dur schemaName =
  "ALTER TABLE " <> arbiterRateLimitsTable schemaName <> set <> ";"
  where
    set = case dur of
      Durable -> " SET LOGGED"
      Unlogged -> " SET UNLOGGED"

-- | Upsert a policy's @default_*@ params. Any operator @override_*@ is left untouched.
upsertPolicyRowSQL :: SchemaName -> PolicyRow -> Text
upsertPolicyRowSQL schemaName row =
  policyUpsertSQL
    (arbiterRateLimitPoliciesTable schemaName)
    (textLiteral (prefixId row))
    [ ("default_max_tokens", doubleLiteral (maxTokens row))
    , ("default_refill_amount", doubleLiteral (refillAmt row))
    , ("default_interval", doubleLiteral (interval row))
    ]

-- | Migration adding the rate-limit columns to a queue's job and DLQ tables. All
-- nullable. @throttled_until@ (job table only) marks a throttle-deferred grouped
-- head in-flight. Its group stays stalled and spends no attempt.
addRateLimitColumnsSQL :: SchemaName -> TableName -> Text
addRateLimitColumnsSQL schemaName tableName =
  T.unlines
    [ "ALTER TABLE " <> jobQueueTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS rate_limit_key TEXT;"
    , "ALTER TABLE " <> jobQueueTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS rate_limit_prefix TEXT;"
    , "ALTER TABLE " <> jobQueueTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS throttled_until TIMESTAMPTZ;"
    , "ALTER TABLE " <> jobQueueDLQTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS rate_limit_key TEXT;"
    , "ALTER TABLE " <> jobQueueDLQTable schemaName tableName <> " ADD COLUMN IF NOT EXISTS rate_limit_prefix TEXT;"
    ]

-- | Add the per-job token cost column to a queue's job and DLQ tables. Defaulted to
-- a unit cost. Existing rows backfill to it. The DLQ stores it and a retried job
-- retains its cost.
addRateLimitCostColumnSQL :: SchemaName -> TableName -> Text
addRateLimitCostColumnSQL schemaName tableName =
  T.unlines
    [ "ALTER TABLE "
        <> jobQueueTable schemaName tableName
        <> " ADD COLUMN IF NOT EXISTS rate_limit_cost DOUBLE PRECISION NOT NULL DEFAULT 1;"
    , "ALTER TABLE "
        <> jobQueueDLQTable schemaName tableName
        <> " ADD COLUMN IF NOT EXISTS rate_limit_cost DOUBLE PRECISION NOT NULL DEFAULT 1;"
    ]

-- | Statement-level triggers that ensure a full token bucket row exists for every
-- rate-limited job's key whose prefix has a policy. Tokens are spent at claim and
-- refill over time. There is no delete trigger. Key creation or a dedup-replace key
-- move seeds a row.
createRateLimitBucketTriggerFunctionsSQL :: SchemaName -> TableName -> Text
createRateLimitBucketTriggerFunctionsSQL schemaName tableName =
  let buckets = arbiterRateLimitsTable schemaName
      policies = arbiterRateLimitPoliciesTable schemaName
      baseName = "ensure_" <> tableName <> "_rate_limit_buckets"
      (funcInsert, _funcDelete, funcUpdate) = maintenanceFunctionNames schemaName baseName
      dollarQuote = "$$"
   in T.unlines
        [ bucketInsertFunction funcInsert buckets policies dollarQuote
        , bucketUpdateFunction funcUpdate buckets policies dollarQuote
        ]

-- | The seed INSERT shared by the trigger functions and the claim's @rl_seed@ CTE,
-- parameterized over the source rows (aliased @n@) and key filter.
bucketSeedInsert :: Text -> Text -> Text -> Text -> Text
bucketSeedInsert buckets policies source match =
  let effMax = effectivePolicyCol "p" "max_tokens"
   in [text|
        INSERT INTO ${buckets} (rate_limit_key, policy_prefix, tokens, last_refill)
        SELECT n.rate_limit_key, MAX(n.rate_limit_prefix),
               MAX(${effMax}), NOW()
        FROM ${source}
        JOIN ${policies} p ON p.prefix_id = n.rate_limit_prefix
        WHERE ${match}
          AND NOT EXISTS (SELECT 1 FROM ${buckets} b WHERE b.rate_limit_key = n.rate_limit_key)
        GROUP BY n.rate_limit_key
        ORDER BY n.rate_limit_key
        ON CONFLICT (rate_limit_key) DO NOTHING
      |]

-- | Seed a full bucket for each fresh inserted key whose prefix has a policy. The NOT
-- EXISTS guard skips the insert for an already-present key.
bucketInsertFunction :: Text -> Text -> Text -> Text -> Text
bucketInsertFunction funcName buckets policies dollarQuote =
  let seed = bucketSeedInsert buckets policies "new_table n" "n.rate_limit_key IS NOT NULL"
   in [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dollarQuote}
    BEGIN
      IF NOT EXISTS (SELECT 1 FROM new_table WHERE rate_limit_key IS NOT NULL LIMIT 1) THEN
        RETURN NULL;
      END IF;

      ${seed};

      RETURN NULL;
    END;
    ${dollarQuote} LANGUAGE plpgsql;
  |]

-- | Seed a bucket for a key a dedup-replace moved onto. A claim or heartbeat
-- leaves the key unchanged and returns early.
bucketUpdateFunction :: Text -> Text -> Text -> Text -> Text
bucketUpdateFunction funcName buckets policies dollarQuote =
  let seed =
        bucketSeedInsert
          buckets
          policies
          "new_table n JOIN old_table o ON o.id = n.id"
          "n.rate_limit_key IS NOT NULL AND n.rate_limit_key IS DISTINCT FROM o.rate_limit_key"
   in [text|
    CREATE OR REPLACE FUNCTION ${funcName}()
    RETURNS TRIGGER AS ${dollarQuote}
    BEGIN
      IF NOT EXISTS (
        SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE n.rate_limit_key IS NOT NULL AND n.rate_limit_key IS DISTINCT FROM o.rate_limit_key
        LIMIT 1
      ) THEN
        RETURN NULL;
      END IF;

      ${seed};

      RETURN NULL;
    END;
    ${dollarQuote} LANGUAGE plpgsql;
  |]

-- | The statement-level AFTER INSERT and AFTER UPDATE triggers backing
-- 'createRateLimitBucketTriggerFunctionsSQL'. There is no delete trigger.
createRateLimitBucketTriggersSQL :: SchemaName -> TableName -> Text
createRateLimitBucketTriggersSQL schemaName tableName =
  let tbl = jobQueueTable schemaName tableName
      baseName = "ensure_" <> tableName <> "_rate_limit_buckets"
   in T.intercalate
        "\n\n"
        [ statementTriggerSQL schemaName tbl baseName "_insert" "INSERT" "NEW TABLE AS new_table"
        , statementTriggerSQL schemaName tbl baseName "_update" "UPDATE" "OLD TABLE AS old_table NEW TABLE AS new_table"
        ]
        <> "\n"

-- | Index backing the throttle wake and per-prefix count, in its own migration. The
-- prefix leads and @rate_limit_key@ trails.
createThrottledIndexSQL :: SchemaName -> TableName -> Text
createThrottledIndexSQL schemaName tableName =
  T.unlines
    [ "CREATE INDEX IF NOT EXISTS " <> quoteIdentifier ("idx_" <> tableName <> "_throttled")
    , "ON " <> jobQueueTable schemaName tableName <> " (rate_limit_prefix, rate_limit_key)"
    , "WHERE throttled_until IS NOT NULL;"
    ]

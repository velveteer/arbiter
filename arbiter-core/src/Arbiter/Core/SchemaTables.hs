{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Table-name lists for an arbiter schema.
module Arbiter.Core.SchemaTables
  ( allSchemaTables
  , sharedArbiterTables
  ) where

import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyPoliciesTableName, arbiterConcurrencyTableName)
import Arbiter.Core.CronSchedule (cronSchedulesTableName)
import Arbiter.Core.Gates (arbiterGatesTableName)
import Arbiter.Core.Job.Schema (TableName, queueTableNames)
import Arbiter.Core.Queues (arbiterQueuesTableName)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTableName, arbiterRateLimitsTableName)
import Arbiter.Core.Worker (arbiterWorkersTableName)

-- | Every schema-wide arbiter table, unqualified and unquoted. See also
-- 'Arbiter.Core.Job.Schema.queueTableNames'.
sharedArbiterTables :: [TableName]
sharedArbiterTables =
  [ arbiterGatesTableName
  , arbiterWorkersTableName
  , arbiterQueuesTableName
  , arbiterConcurrencyTableName
  , arbiterConcurrencyPoliciesTableName
  , arbiterRateLimitsTableName
  , arbiterRateLimitPoliciesTableName
  , cronSchedulesTableName
  ]

-- | Every table an arbiter schema holds, for the given queues.
allSchemaTables :: [TableName] -> [TableName]
allSchemaTables queueTables = concatMap queueTableNames queueTables <> sharedArbiterTables

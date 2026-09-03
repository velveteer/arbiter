-- | The tables an arbiter schema contains that no single queue owns.
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

-- | Unqualified and unquoted, alongside 'Arbiter.Core.Job.Schema.queueTableNames'.
-- Every schema-wide table belongs here.
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

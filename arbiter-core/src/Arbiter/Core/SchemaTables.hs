-- | The tables an arbiter schema contains that no single queue owns.
module Arbiter.Core.SchemaTables
  ( sharedArbiterTables
  ) where

import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyPoliciesTableName, arbiterConcurrencyTableName)
import Arbiter.Core.CronSchedule (cronSchedulesTableName)
import Arbiter.Core.Gates (arbiterGatesTableName)
import Arbiter.Core.Job.Schema (TableName)
import Arbiter.Core.Queues (arbiterQueuesTableName)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTableName, arbiterRateLimitsTableName)
import Arbiter.Core.Worker (arbiterWorkersTableName)

-- | Unqualified and unquoted, alongside 'Arbiter.Core.Job.Schema.queueTableNames'.
-- Every schema-wide table belongs here, so a caller that sweeps, scans or resets the
-- schema covers all of them.
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

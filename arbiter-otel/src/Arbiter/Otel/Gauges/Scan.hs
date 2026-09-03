{-# LANGUAGE ScopedTypeVariables #-}

-- | Database scan for one gauge snapshot.
module Arbiter.Otel.Gauges.Scan
  ( scanSnapshot
  ) where

import Arbiter.Core.Health qualified as Health
import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.MonadArbiter (MonadArbiter, withDbTransaction)
import Arbiter.Core.Operations
  ( QueueOverview (..)
  , QueueStats (..)
  , getAllQueueStats
  , listConcurrencyPolicies
  , listRateLimitPolicies
  , setLocalStatementTimeout
  )
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Time (NominalDiffTime)

import Arbiter.Otel.Gauges.Cache (Snapshot (..))

-- | Read all database-global values used by gauge instruments.
scanSnapshot
  :: forall m
   . (MonadArbiter m)
  => NominalDiffTime
  -> SchemaName
  -> [(TableName, [Text])]
  -> m Snapshot
scanSnapshot statementTimeout schema queueKinds = do
  overviews <- map zeroFilled <$> boundedRead (getAllQueueStats schema queueKinds)
  (dbHealth, tableHealth) <- boundedRead (Health.getPgHealth schema queueTables)
  concurrencyPolicies <- boundedRead (listConcurrencyPolicies schema)
  rateLimitPolicies <- boundedRead (listRateLimitPolicies schema [])
  pure
    Snapshot
      { queues = overviews
      , db = dbHealth
      , tables = tableHealth
      , concurrency = concurrencyPolicies
      , rateLimits = rateLimitPolicies
      }
  where
    boundedRead :: forall a. m a -> m a
    boundedRead query =
      withDbTransaction (setLocalStatementTimeout statementTimeout >> query)
    queueTables = map fst queueKinds
    declaredKinds = Map.fromList queueKinds
    zeroFilled overview =
      let zeros = Map.fromList [(kind, 0) | kind <- Map.findWithDefault [] (overviewQueue overview) declaredKinds]
          stats = overviewStats overview
       in overview {overviewStats = stats {kindCounts = Map.union (kindCounts stats) zeros}}

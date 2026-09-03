{-# LANGUAGE DeriveAnyClass #-}

-- | Shared state for gauge instruments and the refresh loop.
module Arbiter.Otel.Gauges.Cache
  ( Snapshot (..)
  , Cached (..)
  , GaugeState (..)
  , GaugeCache (..)
  , newGaugeCache
  , publishSnapshot
  , setReachable
  , retireCache
  ) where

import Arbiter.Core.Concurrency.Stats qualified as Conc (ConcurrencyPolicyView)
import Arbiter.Core.Health qualified as Health
import Arbiter.Core.Operations (QueueOverview)
import Arbiter.Core.RateLimit.Stats qualified as RL (RateLimitPolicyView)
import Control.Concurrent.STM (STM, TVar, newTVarIO, readTVar, writeTVar)
import Data.Aeson (FromJSON, ToJSON)
import GHC.Generics (Generic)

-- | Values from one database scan.
data Snapshot = Snapshot
  { queues :: [QueueOverview]
  , db :: Maybe Health.PgDbHealth
  , tables :: [Health.PgTableHealth]
  , concurrency :: [Conc.ConcurrencyPolicyView]
  , rateLimits :: [RL.RateLimitPolicyView]
  }
  deriving stock (Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | A snapshot and the monotonic time at which its scan started.
data Cached = Cached
  { takenAt :: Double
  , reading :: Snapshot
  }

-- | Values read by observable instruments.
data GaugeState = GaugeState
  { currentSnapshot :: Maybe Cached
  , lastScanTime :: Maybe Double
  , databaseReachable :: Maybe Bool
  }

-- | Mutable gauge state and its registration time.
data GaugeCache = GaugeCache
  { gaugeState :: TVar GaugeState
  , registeredAt :: Double
  }

-- | Create an empty gauge cache.
newGaugeCache :: Double -> IO GaugeCache
newGaugeCache now =
  GaugeCache
    <$> newTVarIO
      GaugeState
        { currentSnapshot = Nothing
        , lastScanTime = Nothing
        , databaseReachable = Nothing
        }
    <*> pure now

-- | Publish a snapshot to the observable instruments.
publishSnapshot :: GaugeCache -> Cached -> STM ()
publishSnapshot cache snapshot = do
  state <- readTVar (gaugeState cache)
  writeTVar
    (gaugeState cache)
    state
      { currentSnapshot = Just snapshot
      , lastScanTime = Just (takenAt snapshot)
      }

-- | Update the result of the last database operation.
setReachable :: GaugeCache -> Bool -> STM ()
setReachable cache reachable = do
  state <- readTVar (gaugeState cache)
  writeTVar (gaugeState cache) state {databaseReachable = Just reachable}

-- | Stop exporting the cached snapshot.
retireCache :: GaugeCache -> STM ()
retireCache cache = do
  state <- readTVar (gaugeState cache)
  writeTVar (gaugeState cache) state {currentSnapshot = Nothing}

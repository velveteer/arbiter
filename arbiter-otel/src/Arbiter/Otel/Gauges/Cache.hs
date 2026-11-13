{-# LANGUAGE DeriveAnyClass #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Shared state for gauge instruments and the refresh loop.
module Arbiter.Otel.Gauges.Cache
  ( Snapshot (..)
  , Cached (..)
  , Export (..)
  , live
  , lastScan
  , retire
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
import Control.Concurrent.STM (STM, TVar, modifyTVar', newTVarIO, writeTVar)
import Data.Aeson (FromJSON, ToJSON)
import GHC.Generics (Generic)

-- | Values from one database scan.
data Snapshot = Snapshot
  { queues :: [QueueOverview]
  -- ^ One overview per registry queue.
  , tables :: [Health.PgTableHealth]
  -- ^ Health counters per arbiter table.
  , concurrency :: [Conc.ConcurrencyPolicyView]
  -- ^ One view per concurrency policy.
  , rateLimits :: [RL.RateLimitPolicyView]
  -- ^ One view per rate-limit policy.
  }
  deriving stock (Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | A snapshot and the monotonic time at which its scan started.
data Cached = Cached
  { takenAt :: Double
  -- ^ Monotonic time at which the scan started.
  , reading :: Snapshot
  }

-- | What the instruments observe. 'Idle' retains the last scan time, if available.
-- The SDK keeps exporting a series it stops observing, under its last timestamp.
data Export
  = -- | Observe the readings of this scan.
    Live Cached
  | -- | Observe nothing. Keep the last scan time, if any.
    Idle (Maybe Double)

-- | The scan behind an export, if it has one.
live :: Export -> Maybe Cached
live = \case
  Live cached -> Just cached
  Idle _ -> Nothing

-- | When the export was last scanned, if it ever was.
lastScan :: Export -> Maybe Double
lastScan = \case
  Live cached -> Just (takenAt cached)
  Idle lastAt -> lastAt

-- | Stop observing readings, keeping when the last one was scanned.
retire :: Export -> Export
retire = Idle . lastScan

-- | Mutable gauge state and its registration time.
data GaugeCache = GaugeCache
  { export :: TVar Export
  -- ^ What the instruments observe now.
  , databaseReachable :: TVar (Maybe Bool)
  -- ^ Whether the last gauge scan reached the database. 'Nothing' before the first scan
  -- and after retirement.
  , registeredAt :: Double
  -- ^ Monotonic time of the registration.
  }

-- | Create an empty gauge cache for a registration starting at @now@.
newGaugeCache :: Double -> IO GaugeCache
newGaugeCache now =
  GaugeCache
    <$> newTVarIO (Idle Nothing)
    <*> newTVarIO Nothing
    <*> pure now

-- | Publish a snapshot to the observable instruments.
publishSnapshot :: GaugeCache -> Cached -> STM ()
publishSnapshot cache = writeTVar (export cache) . Live

-- | Record whether the last gauge scan reached the database.
setReachable :: GaugeCache -> Bool -> STM ()
setReachable cache = writeTVar (databaseReachable cache) . Just

-- | Stop observing the cached snapshot and the reachability reading.
retireCache :: GaugeCache -> STM ()
retireCache cache = modifyTVar' (export cache) retire >> writeTVar (databaseReachable cache) Nothing

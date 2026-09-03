{-# LANGUAGE DeriveAnyClass #-}

-- | Shared state for gauge instruments and the refresh loop.
module Arbiter.Otel.Gauges.Cache
  ( Snapshot (..)
  , Cached (..)
  , Export (..)
  , live
  , lastScan
  , retire
  , GaugeCache (..)
  , Baseline
  , SeriesKey
  , newGaugeCache
  , publishSnapshot
  , setReachable
  , retireCache
  , riseSince
  ) where

import Arbiter.Core.Concurrency.Stats qualified as Conc (ConcurrencyPolicyView)
import Arbiter.Core.Health qualified as Health
import Arbiter.Core.Operations (QueueOverview)
import Arbiter.Core.RateLimit.Stats qualified as RL (RateLimitPolicyView)
import Control.Concurrent.STM (STM, TVar, modifyTVar', newTVarIO, writeTVar)
import Data.Aeson (FromJSON, ToJSON)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HM
import Data.IORef (IORef, newIORef)
import Data.Text (Text)
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

-- | What the instruments export. 'Idle' keeps the last reading's scan time for the
-- staleness series. Before the first scan it has none.
data Export = Live Cached | Idle (Maybe Double)

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

-- | Stop exporting readings, keeping when the last one was scanned.
retire :: Export -> Export
retire = Idle . lastScan

-- | One counter series: its instrument and attributes.
type SeriesKey = (Text, [(Text, Text)])

-- | The scan a counter series was last counted from, and the total it stood at.
data Baseline = Baseline
  { countedFrom :: !Double
  , countedTotal :: !Double
  }

-- | Mutable gauge state and its registration time.
data GaugeCache = GaugeCache
  { export :: TVar Export
  , databaseReachable :: TVar (Maybe Bool)
  , counterBaselines :: IORef (HashMap SeriesKey Baseline)
  , registeredAt :: Double
  }

-- | Create an empty gauge cache for a registration starting at @now@.
newGaugeCache :: Double -> IO GaugeCache
newGaugeCache now =
  GaugeCache
    <$> newTVarIO (Idle Nothing)
    <*> newTVarIO Nothing
    <*> newIORef HM.empty
    <*> pure now

-- | Publish a snapshot to the observable instruments.
publishSnapshot :: GaugeCache -> Cached -> STM ()
publishSnapshot cache = writeTVar (export cache) . Live

-- | Update the result of the last database operation.
setReachable :: GaugeCache -> Bool -> STM ()
setReachable cache = writeTVar (databaseReachable cache) . Just

-- | Stop exporting the cached snapshot.
retireCache :: GaugeCache -> STM ()
retireCache cache = modifyTVar' (export cache) retire

-- | What a total scanned at @scannedAt@ adds to its series. The first reading and an
-- already counted reading add nothing. A reset counter adds the whole total. Any other
-- reading adds the difference.
riseSince
  :: SeriesKey
  -> Double
  -> Double
  -> HashMap SeriesKey Baseline
  -> (HashMap SeriesKey Baseline, Double)
riseSince key scannedAt total seen = case HM.lookup key seen of
  Just base | countedFrom base >= scannedAt -> (seen, 0)
  Just base -> (counted, if total < countedTotal base then total else total - countedTotal base)
  Nothing -> (counted, 0)
  where
    counted = HM.insert key (Baseline scannedAt total) seen

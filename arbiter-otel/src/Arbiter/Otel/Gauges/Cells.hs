{-# LANGUAGE DeriveAnyClass #-}

-- | The state the gauge instruments read, one set per registration.
module Arbiter.Otel.Gauges.Cells
  ( Snapshot (..)
  , Cached (..)
  , Export (..)
  , live
  , lastScan
  , retire
  , GaugeCells (..)
  , Baseline
  , SeriesKey
  , newGaugeCells
  , riseSince
  ) where

import Arbiter.Core.Concurrency.Stats qualified as Conc (ConcurrencyPolicyView (..))
import Arbiter.Core.Health qualified as Health
import Arbiter.Core.Operations (QueueOverview (..))
import Arbiter.Core.RateLimit.Stats qualified as RL (RateLimitPolicyView (..))
import Control.Concurrent.STM (TVar, newTVarIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HM
import Data.IORef (IORef, newIORef)
import Data.Text (Text)
import GHC.Generics (Generic)

-- | One gauge scan's readings.
data Snapshot = Snapshot
  { queues :: [QueueOverview]
  , db :: Maybe Health.PgDbHealth
  , tables :: [Health.PgTableHealth]
  , concurrency :: [Conc.ConcurrencyPolicyView]
  , rateLimits :: [RL.RateLimitPolicyView]
  }
  deriving stock (Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | A snapshot and the monotonic time its scan started.
data Cached = Cached
  { takenAt :: Double
  , reading :: Snapshot
  }

-- | What the instruments export. 'Idle' keeps the last reading's scan time, which the
-- staleness series goes on growing from, and has none before the first scan.
data Export = Live Cached | Idle (Maybe Double)

-- | The scan behind an export, if it has one.
live :: Export -> Maybe Cached
live = \case
  Live c -> Just c
  Idle _ -> Nothing

-- | When the export was last scanned, if it ever was.
lastScan :: Export -> Maybe Double
lastScan = \case
  Live c -> Just (takenAt c)
  Idle at -> at

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

-- | What the registered callbacks read.
data GaugeCells = GaugeCells
  { cache :: TVar Export
  , counterBaselines :: IORef (HashMap SeriesKey Baseline)
  , registeredAt :: Double
  }

-- | Cells for a registration starting at @now@.
newGaugeCells :: Double -> IO GaugeCells
newGaugeCells now = GaugeCells <$> newTVarIO (Idle Nothing) <*> newIORef HM.empty <*> pure now

-- | What a total scanned at @scannedAt@ adds to its series: nothing for the first
-- reading or one already counted, the whole total for a counter that was reset,
-- otherwise the difference.
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

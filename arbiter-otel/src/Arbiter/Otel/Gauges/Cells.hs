{-# LANGUAGE DeriveAnyClass #-}

-- | The state the gauge instruments read, reusable by a later registration.
module Arbiter.Otel.Gauges.Cells
  ( Snapshot (..)
  , Cached (..)
  , GaugeCells (..)
  , Baseline
  , SeriesKey
  , newGaugeCells
  , resetGaugeCells
  , riseSince
  ) where

import Arbiter.Core.Concurrency.Stats qualified as Conc (ConcurrencyPolicyView (..))
import Arbiter.Core.Health qualified as Health
import Arbiter.Core.Operations (QueueOverview (..))
import Arbiter.Core.RateLimit.Stats qualified as RL (RateLimitPolicyView (..))
import Control.Concurrent.STM (TVar, atomically, newTVarIO, writeTVar)
import Data.Aeson (FromJSON, ToJSON)
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HM
import Data.IORef (IORef, newIORef, writeIORef)
import Data.Text (Text)
import GHC.Generics (Generic)

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

-- | One counter series: its instrument and attributes.
type SeriesKey = (Text, [(Text, Text)])

-- | The scan a counter series was last counted from, and the total it stood at.
data Baseline = Baseline
  { countedFrom :: !Double
  , countedTotal :: !Double
  }

-- | What the registered callbacks read.
data GaugeCells = GaugeCells
  { cache :: TVar (Maybe Cached)
  , counterBaselines :: IORef (HashMap SeriesKey Baseline)
  , registeredAt :: TVar Double
  }

newGaugeCells :: IO GaugeCells
newGaugeCells = GaugeCells <$> newTVarIO Nothing <*> newIORef HM.empty <*> newTVarIO 0

-- | Reset the cells for a registration starting at @now@.
resetGaugeCells :: Double -> GaugeCells -> IO ()
resetGaugeCells now cells = do
  writeIORef (counterBaselines cells) HM.empty
  atomically $ do
    writeTVar (cache cells) Nothing
    writeTVar (registeredAt cells) now

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

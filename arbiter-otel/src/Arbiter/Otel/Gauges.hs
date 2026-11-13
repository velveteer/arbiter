{-# LANGUAGE OverloadedStrings #-}

-- | Registration and refresh lifecycle for database gauges.
module Arbiter.Otel.Gauges
  ( startGauges
  , withGaugeLoop
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.FailureGate (clearFailure, holdFailure)
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations (Shared (..), micros)
import Arbiter.Worker.Logger
  ( FailureGate
  , LogConfig (..)
  , LogLevel (Warning)
  , newFailureGate
  , reportOutcome
  , tryLog
  )
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically, readTVarIO)
import Control.Exception (SomeException, fromException)
import Control.Monad (forever, guard, void, when)
import Data.Foldable (for_, traverse_)
import Data.Maybe (isJust, isNothing)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import GHC.Clock (getMonotonicTime)
import UnliftIO.Exception (bracket, finally)

import Arbiter.Otel.Gauges.Cache
  ( Cached (..)
  , GaugeCache (..)
  , live
  , newGaugeCache
  , publishSnapshot
  , retireCache
  , setReachable
  )
import Arbiter.Otel.Gauges.Coordination (RefreshSource (..), ScanAbandoned, prepareRefreshSource)
import Arbiter.Otel.Gauges.Instruments (registerInstruments)
import Arbiter.Otel.Metrics (arbiterMeter)
import Arbiter.Otel.Telemetry qualified as Tel

-- | Register gauge instruments and return their refresh loop. Run the loop on its own
-- thread. It runs until it is stopped. A stopped loop stops observing. The SDK keeps
-- exporting each series' last observation under its original timestamp, and
-- @arbiter.gauges.age@ keeps rising. With metrics off, the loop returns at once.
startGauges
  :: (MonadArbiter m)
  => Tel.Telemetry
  -> LogConfig
  -- ^ Base log config for the loop.
  -> (forall a. m a -> IO a)
  -- ^ Runs a database action.
  -> Text
  -> [(Text, [Text])]
  -- ^ Each queue table and its payload kind labels.
  -> NominalDiffTime
  -- ^ Refresh interval. The minimum is one second.
  -> IO (IO ())
startGauges tel baseLog runDb schema queueKinds refreshInterval = do
  (loop, stop) <- prepareGauges tel baseLog runDb schema queueKinds refreshInterval
  pure (loop `finally` stop)

-- | Run an action with registered gauge instruments and a refresh loop.
withGaugeLoop
  :: (MonadArbiter m)
  => Tel.Telemetry
  -> LogConfig
  -> (forall a. m a -> IO a)
  -> Text
  -> [(Text, [Text])]
  -> (IO () -> IO b)
  -> IO b
withGaugeLoop tel baseLog runDb schema queueKinds use =
  bracket (prepareGauges tel baseLog runDb schema queueKinds (Tel.gaugeRefresh tel)) snd (use . fst)

prepareGauges
  :: (MonadArbiter m)
  => Tel.Telemetry
  -> LogConfig
  -> (forall a. m a -> IO a)
  -> Text
  -> [(Text, [Text])]
  -> NominalDiffTime
  -> IO (IO (), IO ())
prepareGauges tel baseLog runDb schema queueKinds requestedInterval
  | isNothing (Tel.meters tel) = pure (pure (), pure ())
  | otherwise = do
      cache <- newGaugeCache =<< getMonotonicTime
      arbiterMeter (Tel.provider tel) >>= \meter -> registerInstruments meter cache
      source <-
        prepareRefreshSource
          logCfg
          runDb
          schema
          queueKinds
          refreshInterval
          freshnessWindow
          (cacheIsStale cache freshnessWindow)
      gates <- RefreshGates <$> newFailureGate <*> newFailureGate
      pure (refreshLoop logCfg gates source refreshInterval cache, atomically (retireCache cache))
  where
    refreshInterval = max minimumRefreshInterval requestedInterval
    freshnessWindow = freshnessWindowFactor * refreshInterval
    -- Gauge work has no worker-pool identity.
    logCfg = (Tel.telemetryLogConfig tel baseLog) {identityContext = []}

-- | The fastest refresh the loop runs at.
minimumRefreshInterval :: NominalDiffTime
minimumRefreshInterval = 1

-- | The freshness window over the refresh interval. Covers a slow scan plus one
-- missed refresh.
freshnessWindowFactor :: NominalDiffTime
freshnessWindowFactor = 3

-- | One failure gate for the scan and one for abandoning a slow scan.
data RefreshGates = RefreshGates
  { scanGate :: FailureGate
  , abandonGate :: FailureGate
  }

refreshLoop :: LogConfig -> RefreshGates -> RefreshSource -> NominalDiffTime -> GaugeCache -> IO ()
refreshLoop logCfg gates source refreshInterval cache = forever $ do
  started <- getMonotonicTime
  refreshed <- runRefresh source
  now <- getMonotonicTime
  reportScan refreshed
  for_ (reachabilityOf refreshed) (atomically . setReachable cache)
  traverse_ (traverse_ (atomically . publishSnapshot cache) . (>>= stamp started now)) refreshed
  finished <- getMonotonicTime
  threadDelay (max (micros (minimumDelay source)) (micros (refreshInterval - realToFrac (finished - started))))
  where
    -- A failed scan keeps the last reading. A state change gets a log line.
    reportScan = \case
      Left failure | isJust (fromException @ScanAbandoned failure) -> do
        worth <- holdFailure (abandonGate gates) (failureRepeatInterval logCfg) (displayEx failure)
        when worth (tryLog logCfg Warning (displayEx failure))
      refreshed -> do
        void (clearFailure (abandonGate gates))
        reportOutcome logCfg Warning (scanGate gates) "Gauge refresh" refreshed
    stamp started now = \case
      Ran snap -> Just Cached {takenAt = started, reading = snap}
      Published age snap -> Just Cached {takenAt = now - age, reading = snap}
      Unreadable _ -> Nothing

-- | Whether a refresh reached the database. A slow scan abandoned for a fresher
-- reading says nothing either way.
reachabilityOf :: Either SomeException a -> Maybe Bool
reachabilityOf = either (\failure -> False <$ guard (isNothing (fromException @ScanAbandoned failure))) (const (Just True))

cacheIsStale :: GaugeCache -> NominalDiffTime -> IO Bool
cacheIsStale cache maxAge = do
  now <- getMonotonicTime
  cached <- live <$> readTVarIO (export cache)
  pure (maybe True (\lastCached -> now - takenAt lastCached > realToFrac maxAge) cached)

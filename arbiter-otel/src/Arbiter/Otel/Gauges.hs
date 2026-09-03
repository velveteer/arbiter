{-# LANGUAGE OverloadedStrings #-}

-- | Registration and refresh lifecycle for database gauges.
module Arbiter.Otel.Gauges
  ( startGauges
  , withGaugeLoop
  , reachabilityOf
  ) where

import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations (Shared (..), micros)
import Arbiter.Worker (FailureGate, LogConfig (..), LogLevel (Warning), newFailureGate, reportOutcome)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically, readTVarIO)
import Control.Exception (SomeException)
import Control.Monad (forever)
import Data.Foldable (for_, traverse_)
import Data.Maybe (isNothing)
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
import Arbiter.Otel.Gauges.Coordination (RefreshSource (..), prepareRefreshSource)
import Arbiter.Otel.Gauges.Instruments (registerInstruments)
import Arbiter.Otel.Metrics (arbiterMeter)
import Arbiter.Otel.Telemetry qualified as Tel

-- | Register gauge instruments and return their refresh loop.
startGauges
  :: (MonadArbiter m)
  => Tel.Telemetry
  -> LogConfig
  -> (forall a. m a -> IO a)
  -> SchemaName
  -> [(TableName, [Text])]
  -> NominalDiffTime
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
  -> SchemaName
  -> [(TableName, [Text])]
  -> NominalDiffTime
  -> (IO () -> IO b)
  -> IO b
withGaugeLoop tel baseLog runDb schema queueKinds refreshInterval use =
  bracket (prepareGauges tel baseLog runDb schema queueKinds refreshInterval) snd (use . fst)

prepareGauges
  :: (MonadArbiter m)
  => Tel.Telemetry
  -> LogConfig
  -> (forall a. m a -> IO a)
  -> SchemaName
  -> [(TableName, [Text])]
  -> NominalDiffTime
  -> IO (IO (), IO ())
prepareGauges tel baseLog runDb schema queueKinds requestedInterval
  | isNothing (Tel.meters tel) = pure (pure (), pure ())
  | otherwise = do
      cache <- newGaugeCache =<< getMonotonicTime
      advance <- arbiterMeter (Tel.provider tel) >>= \meter -> registerInstruments meter cache
      source <-
        prepareRefreshSource
          logCfg
          runDb
          schema
          queueKinds
          refreshInterval
          freshnessWindow
          (cacheIsStale cache freshnessWindow)
      refreshGate <- newFailureGate
      pure (refreshLoop logCfg refreshGate source refreshInterval cache advance, atomically (retireCache cache))
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

refreshLoop :: LogConfig -> FailureGate -> RefreshSource -> NominalDiffTime -> GaugeCache -> (Cached -> IO ()) -> IO ()
refreshLoop logCfg refreshGate source refreshInterval cache advance = forever $ do
  started <- getMonotonicTime
  refreshed <- runRefresh source
  now <- getMonotonicTime
  -- A failed scan keeps the last reading. A state change gets a log line.
  for_ (reachabilityOf refreshed) $ \reachable -> do
    reportOutcome logCfg Warning refreshGate "Gauge refresh" refreshed
    atomically (setReachable cache reachable)
  traverse_ (traverse_ publish . (>>= stamp started now)) refreshed
  threadDelay (max (micros (minimumDelay source)) (micros (refreshInterval - realToFrac (now - started))))
  where
    publish cached = atomically (publishSnapshot cache cached) >> advance cached
    stamp started now = \case
      Ran snap -> Just (Cached started snap)
      Published age snap -> Just (Cached (now - age) snap)
      Unreadable _ -> Nothing

-- | What a scan says about the database. An abandoned scan says nothing.
reachabilityOf :: Either SomeException (Maybe a) -> Maybe Bool
reachabilityOf = either (const (Just False)) (True <$)

cacheIsStale :: GaugeCache -> NominalDiffTime -> IO Bool
cacheIsStale cache maxAge = do
  now <- getMonotonicTime
  cached <- live <$> readTVarIO (export cache)
  pure (maybe True (\lastCached -> now - takenAt lastCached > realToFrac maxAge) cached)

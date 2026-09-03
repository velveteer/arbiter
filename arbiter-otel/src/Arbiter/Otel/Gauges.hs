{-# LANGUAGE OverloadedStrings #-}

-- | Registration and refresh lifecycle for database gauges.
module Arbiter.Otel.Gauges
  ( startGauges
  , withGaugeLoop
  ) where

import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations (micros)
import Arbiter.Worker (FailureGate, LogConfig (..), LogLevel (Warning), newFailureGate, reportOutcome)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically, readTVarIO)
import Control.Exception (SomeException)
import Control.Monad (forever)
import Data.Foldable (for_)
import Data.Maybe (isNothing)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import GHC.Clock (getMonotonicTime)
import UnliftIO.Exception (bracket, finally)

import Arbiter.Otel.Gauges.Cache
  ( Cached (..)
  , GaugeCache (..)
  , GaugeState (..)
  , newGaugeCache
  , publishSnapshot
  , retireCache
  , setReachable
  )
import Arbiter.Otel.Gauges.Coordination
  ( RefreshResult (..)
  , RefreshSource (..)
  , RefreshedSnapshot (..)
  , prepareRefreshSource
  )
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
      arbiterMeter (Tel.provider tel) >>= \meter -> registerInstruments meter cache
      source <-
        prepareRefreshSource
          logCfg
          runDb
          schema
          queueKinds
          refreshInterval
          (cacheIsStale cache freshnessWindow)
      refreshGate <- newFailureGate
      let loop = refreshLoop logCfg refreshGate source cache
      pure (loop, atomically (retireCache cache))
  where
    refreshInterval = max 1 requestedInterval
    freshnessWindow = 3 * refreshInterval
    -- Gauge work has no worker-pool identity.
    logCfg = (Tel.telemetryLogConfig tel baseLog) {identityContext = []}

refreshLoop :: LogConfig -> FailureGate -> RefreshSource -> GaugeCache -> IO ()
refreshLoop logCfg refreshGate source cache = forever $ do
  started <- getMonotonicTime
  result <- runRefresh source
  finished <- getMonotonicTime

  for_ (reachability result) $ \reachable -> do
    reportOutcome logCfg Warning refreshGate "Gauge refresh" (outcome result)
    atomically (setReachable cache reachable)

  case result of
    RefreshReady snapshot -> atomically (publishSnapshot cache (toCached started finished snapshot))
    RefreshSkipped -> pure ()
    RefreshFailed _ -> pure ()

  let elapsed = finished - started
      remaining = micros (refreshInterval source) - round (elapsed * 1_000_000)
  threadDelay (max (micros (minimumDelay source)) remaining)

reachability :: RefreshResult -> Maybe Bool
reachability = \case
  RefreshFailed _ -> Just False
  RefreshSkipped -> Nothing
  RefreshReady _ -> Just True

outcome :: RefreshResult -> Either SomeException ()
outcome = \case
  RefreshFailed err -> Left err
  _ -> Right ()

toCached :: Double -> Double -> RefreshedSnapshot -> Cached
toCached started finished = \case
  Scanned snapshot -> Cached started snapshot
  ReadShared age snapshot -> Cached (finished - age) snapshot

cacheIsStale :: GaugeCache -> NominalDiffTime -> IO Bool
cacheIsStale cache maxAge = do
  now <- getMonotonicTime
  snapshot <- currentSnapshot <$> readTVarIO (gaugeState cache)
  pure (maybe True (\cached -> now - takenAt cached > realToFrac maxAge) snapshot)

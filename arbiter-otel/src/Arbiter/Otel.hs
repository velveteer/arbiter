{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | OpenTelemetry for arbiter: traces, metrics and logs.
--
-- Spans and trace-context propagation are built in, so this module is the SDK side:
-- install the exporters, then hand the handle to the helpers.
--
-- @
-- Otel.withTelemetryFromEnv $ \\tel -> do
--   putStrLn (unpack (Otel.telemetrySummary tel))
--   env <- createHasqlEnv ...
--   let pools = Otel.instrumentPools tel [namedWorkerPool emailCfg]
--   Otel.withMetricsEndpoint tel defaultLogConfig 9464
--     $ runHasqlDb env
--     $ Otel.withGauges tel defaultLogConfig 15 (runWorkerPools pools)
-- @
module Arbiter.Otel
  ( -- * Setup
    Telemetry (..)
  , withTelemetry
  , withTelemetryIf
  , withTelemetryFromEnv
  , withExternalTelemetry
  , withMetricsEndpoint

    -- * Instrumenting a pool
  , instrumentPool
  , instrumentPools
  , instrumentConfig

    -- * Gauges
  , withGauges
  , startGauges

    -- * Metrics
  , ArbiterMeters
  , arbiterMeter
  , newArbiterMeters
  , otelHooks
  , otelMaintenance
  , otelLogs
  , otelLogDestination
  , attrs
  ) where

import Arbiter.Core.Job.Types (andThen)
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf, getSchema)
import Arbiter.Core.QueueRegistry (RegistryTables, registryTableNames)
import Arbiter.Core.Threads (labelArbiterThread)
import Arbiter.Worker (NamedWorkerPool (..))
import Arbiter.Worker.Config (WorkerConfig (..), withHooks)
import Arbiter.Worker.Logger (LogConfig, LogDestination)
import Control.Monad (guard)
import Data.Foldable (traverse_)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import UnliftIO (MonadUnliftIO, withRunInIO)
import UnliftIO.Async (withAsync)

import Arbiter.Otel.Gauges (startGauges, withGaugeLoop)
import Arbiter.Otel.Metrics
  ( ArbiterMeters
  , arbiterMeter
  , attrs
  , newArbiterMeters
  , otelHooks
  , otelLogDestination
  , otelLogs
  , otelMaintenance
  )
import Arbiter.Otel.Telemetry
  ( Telemetry (..)
  , withExternalTelemetry
  , withMetricsEndpoint
  , withTelemetry
  , withTelemetryFromEnv
  , withTelemetryIf
  )

-- | Give a pool consumer spans, lifecycle metrics, and the telemetry log destination.
-- The pool's registry queue name labels its metrics, which is what the gauges are
-- labelled by. Apply it once per pool.
instrumentPool :: (MonadUnliftIO m) => Telemetry -> NamedWorkerPool m -> NamedWorkerPool m
instrumentPool tel pool@(NamedWorkerPool queue cfg)
  | not (enabled tel) = pool
  | otherwise =
      NamedWorkerPool queue (instrumentConfig (meters tel <$ guard (metricsEnabled tel)) (logDestination tel) queue cfg)

-- | 'instrumentPool' over a pool list.
instrumentPools :: (MonadUnliftIO m) => Telemetry -> [NamedWorkerPool m] -> [NamedWorkerPool m]
instrumentPools = map . instrumentPool

-- | Run the registry's depth and health gauges alongside @action@. Nothing is scanned
-- when the handle has metrics off, and one handle registers one set of gauges.
withGauges
  :: forall m b
   . (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => Telemetry
  -> LogConfig
  -> NominalDiffTime
  -- ^ How often to refresh the readings.
  -> m b
  -> m b
withGauges tel baseLog refreshInterval action = do
  schema <- getSchema
  withRunInIO $ \runDb ->
    withGaugeLoop tel baseLog runDb schema (registryTableNames (Proxy @(RegistryOf m))) refreshInterval $
      \loop -> withAsync (labelArbiterThread "gauges" Nothing >> loop) (const (runDb action))

-- | 'instrumentPool' against meters and a log destination the caller resolved itself.
instrumentConfig
  :: (MonadUnliftIO m)
  => Maybe ArbiterMeters
  -> Maybe LogDestination
  -> Text
  -> WorkerConfig m payload
  -> WorkerConfig m payload
instrumentConfig ms dest queue cfg =
  withHooks metricHooks $
    cfg
      { logConfig = otelLogs dest (logConfig cfg)
      , onMaintenance = \op n -> traverse_ (\ms' -> otelMaintenance ms' op n) ms `andThen` onMaintenance cfg op n
      }
  where
    metricHooks hooks = maybe hooks (\ms' -> otelHooks ms' queue <> hooks) ms

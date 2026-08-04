{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}

-- | OpenTelemetry for arbiter: traces, metrics and logs.
--
-- Spans and trace-context propagation are built in, so this module is the SDK side:
-- 'runWorkerPools' installs the exporters and instruments the pools. The @With@ variants
-- take a handle the caller installed itself.
--
-- @
-- env <- createHasqlEnv ...
-- runHasqlDb env $ Otel.runWorkerPools [namedWorkerPool emailCfg]
-- @
module Arbiter.Otel
  ( -- * Setup
    Telemetry (..)
  , withTelemetry
  , withTelemetryIf
  , withTelemetryFromEnv
  , withExternalTelemetry

    -- * Instrumenting a pool
  , instrumentPool
  , instrumentPools
  , instrumentConfig

    -- * Running pools
  , runWorkerPools
  , runSelectedWorkerPools
  , runWorkerPoolsWith
  , runSelectedWorkerPoolsWith

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

import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf, getSchema)
import Arbiter.Core.QueueRegistry (RegistryTables, registryTableNames)
import Arbiter.Core.Threads (labelArbiterThread)
import Arbiter.Worker (NamedWorkerPool (..))
import Arbiter.Worker qualified as Worker
import Arbiter.Worker.Config (WorkerConfig (..), withHooks, withMaintenance)
import Arbiter.Worker.Logger (LogConfig, defaultLogConfig)
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
  , withTelemetry
  , withTelemetryFromEnv
  , withTelemetryIf
  )

-- | Give a pool consumer spans, lifecycle metrics, and the telemetry log destination.
-- The pool's registry queue name labels its metrics, which is what the gauges are
-- labelled by. Apply it once per pool.
instrumentPool :: (MonadUnliftIO m) => Telemetry -> NamedWorkerPool m -> NamedWorkerPool m
instrumentPool tel (NamedWorkerPool queue cfg) =
  NamedWorkerPool queue (instrumentConfig tel queue cfg)

-- | 'instrumentPool' over a pool list.
instrumentPools :: (MonadUnliftIO m) => Telemetry -> [NamedWorkerPool m] -> [NamedWorkerPool m]
instrumentPools = map . instrumentPool

-- | Run the registry's depth and health gauges alongside @action@. Nothing is scanned
-- when the handle has metrics off.
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

-- | 'Arbiter.Worker.runWorkerPools' with the SDK installed from the environment, the
-- pools instrumented and the gauges running. Take the handle yourself with
-- 'runWorkerPoolsWith' when something else needs it.
runWorkerPools
  :: forall m
   . (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => [NamedWorkerPool m]
  -> m ()
runWorkerPools pools =
  withTelemetryHere $ \tel ->
    poolRun tel defaultLogConfig defaultGaugeRefresh pools Worker.runWorkerPools

-- | 'runWorkerPools' over an explicit queue list.
runSelectedWorkerPools
  :: forall m
   . (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPools enabled pools =
  withTelemetryHere $ \tel ->
    poolRun tel defaultLogConfig defaultGaugeRefresh pools (Worker.runSelectedWorkerPools enabled)

-- | 'runWorkerPools' over a handle the caller installed itself, with the gauge loop's
-- base log config and refresh interval.
runWorkerPoolsWith
  :: forall m
   . (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => Telemetry
  -> LogConfig
  -> NominalDiffTime
  -- ^ How often to refresh the gauge readings.
  -> [NamedWorkerPool m]
  -> m ()
runWorkerPoolsWith tel baseLog refreshInterval pools =
  poolRun tel baseLog refreshInterval pools Worker.runWorkerPools

-- | 'runSelectedWorkerPools' over a handle the caller installed itself.
runSelectedWorkerPoolsWith
  :: forall m
   . (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => Telemetry
  -> LogConfig
  -> NominalDiffTime
  -> [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPoolsWith tel baseLog refreshInterval enabled pools =
  poolRun tel baseLog refreshInterval pools (Worker.runSelectedWorkerPools enabled)

-- | Install the SDK around an action in the database monad.
withTelemetryHere :: (MonadUnliftIO m) => (Telemetry -> m a) -> m a
withTelemetryHere use = withRunInIO $ \runDb -> withTelemetryFromEnv (runDb . use)

-- | Run @run@ over the instrumented pools with the gauges alongside.
poolRun
  :: (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => Telemetry
  -> LogConfig
  -> NominalDiffTime
  -> [NamedWorkerPool m]
  -> ([NamedWorkerPool m] -> m ())
  -> m ()
poolRun tel baseLog refreshInterval pools run =
  withGauges tel baseLog refreshInterval (run (instrumentPools tel pools))

-- | Gauge refresh interval the handle-less entry points use.
defaultGaugeRefresh :: NominalDiffTime
defaultGaugeRefresh = 15

-- | 'instrumentPool' over a bare config, labelled by @queue@.
instrumentConfig
  :: (MonadUnliftIO m)
  => Telemetry
  -> Text
  -> WorkerConfig m payload
  -> WorkerConfig m payload
instrumentConfig tel queue cfg =
  withHooks metricHooks $
    withMaintenance (\op n -> traverse_ (\ms -> otelMaintenance ms op n) (meters tel)) $
      cfg {logConfig = otelLogs (logDestination tel) (logConfig cfg)}
  where
    metricHooks hooks = maybe hooks (\ms -> otelHooks ms queue <> hooks) (meters tel)

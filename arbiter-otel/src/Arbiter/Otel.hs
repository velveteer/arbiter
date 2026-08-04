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

import Arbiter.Core.Job.Types (andThen)
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf, getSchema)
import Arbiter.Core.QueueRegistry (RegistryTables, registryTableNames)
import Arbiter.Core.Threads (labelArbiterThread)
import Arbiter.Worker (NamedWorkerPool (..))
import Arbiter.Worker qualified as Worker
import Arbiter.Worker.Config (WorkerConfig (..), withHooks)
import Arbiter.Worker.Logger (LogConfig, LogDestination, defaultLogConfig)
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
  NamedWorkerPool queue (instrumentConfig (meters tel) (logDestination tel) queue cfg)

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
runWorkerPools pools = withTelemetryHere (\tel -> defaultPoolRun tel pools Worker.runWorkerPools)

-- | 'runWorkerPools' over an explicit queue list.
runSelectedWorkerPools
  :: forall m
   . (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPools enabled pools =
  withTelemetryHere (\tel -> defaultPoolRun tel pools (Worker.runSelectedWorkerPools enabled))

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
  withGauges tel baseLog refreshInterval (Worker.runWorkerPools (instrumentPools tel pools))

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
  withGauges tel baseLog refreshInterval $
    Worker.runSelectedWorkerPools enabled (instrumentPools tel pools)

-- | Install the SDK around an action in the database monad.
withTelemetryHere :: (MonadUnliftIO m) => (Telemetry -> m a) -> m a
withTelemetryHere use = withRunInIO $ \runDb -> withTelemetryFromEnv (runDb . use)

-- | Instrumented pools and gauges on the defaults, for the handle-less entry points.
defaultPoolRun
  :: (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => Telemetry
  -> [NamedWorkerPool m]
  -> ([NamedWorkerPool m] -> m ())
  -> m ()
defaultPoolRun tel pools run =
  withGauges tel defaultLogConfig defaultGaugeRefresh (run (instrumentPools tel pools))

-- | Gauge refresh interval the handle-less entry points use.
defaultGaugeRefresh :: NominalDiffTime
defaultGaugeRefresh = 15

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
      , onMaintenance = \op n -> traverse_ (\ms' -> otelMaintenance ms' op n) ms `andThen` baseMaintenance op n
      }
  where
    baseMaintenance = onMaintenance cfg
    metricHooks hooks = maybe hooks (\ms' -> otelHooks ms' queue <> hooks) ms

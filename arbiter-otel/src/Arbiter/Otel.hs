{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

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
  , telemetryLogConfig

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
  , loggerDestination
  , attrs
  , arbiterMetricNames
  ) where

import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf, getSchema)
import Arbiter.Core.QueueRegistry (RegistryTables, TableForPayload, registryTableNames)
import Arbiter.Core.Threads (labelArbiterThread)
import Arbiter.Worker (NamedWorkerPool (..))
import Arbiter.Worker qualified as Worker
import Arbiter.Worker.Config (WorkerConfig (..), withHooks, withMaintenance)
import Arbiter.Worker.Logger (LogConfig, defaultLogConfig)
import Data.Foldable (traverse_)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import GHC.TypeLits (KnownSymbol)
import UnliftIO (MonadUnliftIO, withRunInIO)
import UnliftIO.Async (withAsync)

import Arbiter.Otel.Gauges (startGauges, withGaugeLoop)
import Arbiter.Otel.Logs (loggerDestination, otelLogs)
import Arbiter.Otel.MetricNames (arbiterMetricNames)
import Arbiter.Otel.Metrics
  ( ArbiterMeters
  , arbiterMeter
  , attrs
  , newArbiterMeters
  , otelHooks
  , otelMaintenance
  )
import Arbiter.Otel.Telemetry
  ( Telemetry (..)
  , telemetryLogConfig
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
  NamedWorkerPool queue (labelledConfig tel queue cfg)

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
  -> m b
  -> m b
withGauges tel baseLog action = do
  schema <- getSchema
  withRunInIO $ \runDb ->
    withGaugeLoop tel baseLog runDb schema (registryTableNames (Proxy @(RegistryOf m))) (gaugeRefresh tel) $
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
  withTelemetryHere $ \tel -> runWorkerPoolsWith tel defaultLogConfig pools

-- | 'runWorkerPools' over an explicit queue list.
runSelectedWorkerPools
  :: forall m
   . (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPools enabled pools =
  withTelemetryHere $ \tel -> runSelectedWorkerPoolsWith tel defaultLogConfig enabled pools

-- | 'runWorkerPools' over a handle the caller installed itself, with the gauge loop's
-- base log config.
runWorkerPoolsWith
  :: forall m
   . (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => Telemetry
  -> LogConfig
  -> [NamedWorkerPool m]
  -> m ()
runWorkerPoolsWith tel baseLog pools =
  withGauges tel baseLog (Worker.runWorkerPools (instrumentPools tel pools))

-- | 'runSelectedWorkerPools' over a handle the caller installed itself.
runSelectedWorkerPoolsWith
  :: forall m
   . (MonadArbiter m, MonadUnliftIO m, RegistryTables (RegistryOf m))
  => Telemetry
  -> LogConfig
  -> [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPoolsWith tel baseLog enabled pools =
  withGauges tel baseLog (Worker.runSelectedWorkerPools enabled (instrumentPools tel pools))

-- | Install the SDK around an action in the database monad.
withTelemetryHere :: (MonadUnliftIO m) => (Telemetry -> m a) -> m a
withTelemetryHere use = withRunInIO $ \runDb -> withTelemetryFromEnv (runDb . use)

-- | 'instrumentPool' over a bare config, labelled by the payload's registry queue.
instrumentConfig
  :: forall m payload
   . (KnownSymbol (TableForPayload payload (RegistryOf m)), MonadUnliftIO m)
  => Telemetry
  -> WorkerConfig m payload
  -> WorkerConfig m payload
instrumentConfig tel = labelledConfig tel (Arb.queueTable @payload @m)

labelledConfig
  :: (MonadUnliftIO m)
  => Telemetry
  -> Text
  -> WorkerConfig m payload
  -> WorkerConfig m payload
labelledConfig tel queue cfg =
  withHooks metricHooks $
    withMaintenance (\op n -> traverse_ (\ms -> otelMaintenance ms op n) (meters tel)) $
      cfg {logConfig = telemetryLogConfig tel (logConfig cfg)}
  where
    metricHooks hooks = maybe hooks (\ms -> otelHooks ms queue <> hooks) (meters tel)

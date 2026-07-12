{-# LANGUAGE OverloadedStrings #-}

-- | OpenTelemetry setup: one meter provider feeding a Prometheus scrape and OTLP push.
-- The SDK resolves each signal's endpoint and exporter from the standard @OTEL_@ variables.
module Arbiter.Serve.Telemetry
  ( Telemetry (..)
  , withTelemetry
  , withExternalTelemetry
  ) where

import Arbiter.Worker.Logger (LogDestination)
import Arbiter.Worker.Telemetry (ArbiterMeters, globalArbiterMeters, newArbiterMeters, otelLogDestination)
import Control.Exception (bracket)
import Control.Monad (unless, void)
import Data.Functor ((<&>))
import Data.Maybe (isJust)
import Data.Text qualified as T
import Data.Vector qualified as Vector
import Network.HTTP.Types (status404)
import Network.Wai (Application, Middleware, responseLBS)
import OpenTelemetry.Environment
  ( LogsExporterSelection (..)
  , MetricsExporterSelection (..)
  , lookupLogsExporterSelection
  , lookupMetricsExporterSelection
  )
import OpenTelemetry.Exporter.OTLP.Internal.Config
  ( loadExporterEnvironmentVariables
  , otlpEndpoint
  , otlpLogsEndpoint
  , otlpMetricsEndpoint
  , otlpTracesEndpoint
  )
import OpenTelemetry.Exporter.Prometheus.WAI (prometheusApplication)
import OpenTelemetry.Instrumentation.Wai (newOpenTelemetryWaiMiddleware)
import OpenTelemetry.Log (initializeGlobalLoggerProvider, shutdownLoggerProvider)
import OpenTelemetry.MeterProvider (collectResourceMetrics)
import OpenTelemetry.Metric
  ( createMeterProvider
  , defaultSdkMeterProviderOptions
  , forkPeriodicMetricReader
  , periodicMetricReaderOptionsFromEnv
  , resolveMetricExporter
  , shutdownMeterProvider
  , stopPeriodicMetricReader
  )
import OpenTelemetry.Metric.Core (MeterProvider, getGlobalMeterProvider, setGlobalMeterProvider)
import OpenTelemetry.Resource
  ( MaterializedResources
  , materializeResources
  , mergeResources
  , mkResource
  , (.=)
  )
import OpenTelemetry.Resource.Detect (detectBuiltInResources, detectResourceAttributes)
import OpenTelemetry.Trace (initializeGlobalTracerProvider, shutdownTracerProvider)
import System.Environment (lookupEnv, setEnv)

data Telemetry = Telemetry
  { meters :: ArbiterMeters
  , provider :: MeterProvider
  , waiMiddleware :: Middleware
  , metricsApp :: Application
  , logDestination :: Maybe LogDestination
  -- ^ Where the pools' logs go. 'Nothing' leaves the caller's own destination.
  }

-- | Bracketed OpenTelemetry init/shutdown (nested so partial setup unwinds).
withTelemetry :: (Telemetry -> IO a) -> IO a
withTelemetry action = do
  -- Prometheus text cannot parse exemplars, which the WAI gauge emits.
  exemplarFilter <- lookupEnv "OTEL_METRICS_EXEMPLAR_FILTER"
  unless (isJust exemplarFilter) $ setEnv "OTEL_METRICS_EXEMPLAR_FILTER" "always_off"
  configured <- otlpConfigured
  -- The SDK offers no hook to decline its default exporter, so the variable is the only lever.
  unless configured $ setEnv "OTEL_TRACES_EXPORTER" "none"
  resources <- detectedResources
  bracket
    (createMeterProvider resources defaultSdkMeterProviderOptions)
    (\(provider, _) -> void (shutdownMeterProvider provider Nothing))
    $ \(provider, env) -> do
      setGlobalMeterProvider provider
      withOtlpReader configured env $
        bracket initializeGlobalTracerProvider (\tp -> void (shutdownTracerProvider tp Nothing)) $ \_ ->
          withLogs configured $ \logs -> do
            waiMiddleware <- newOpenTelemetryWaiMiddleware
            meters <- newArbiterMeters provider
            action
              Telemetry
                { meters = meters
                , provider = provider
                , waiMiddleware = waiMiddleware
                , metricsApp = prometheusApplication (Vector.fromList <$> collectResourceMetrics env)
                , logDestination = logs
                }
  where
    withOtlpReader configured env inner = do
      push <- wantsOtlpPush configured
      if push
        then do
          exporter <- resolveMetricExporter
          opts <- periodicMetricReaderOptionsFromEnv
          bracket (forkPeriodicMetricReader env exporter opts) stopPeriodicMetricReader (const inner)
        else inner
    withLogs configured inner = do
      push <- wantsOtlpLogs configured
      if push
        then bracket initializeGlobalLoggerProvider (\lp -> void (shutdownLoggerProvider lp Nothing)) $
          \_ -> otelLogDestination >>= inner . Just
        else inner Nothing

-- | The resource the SDK detects for traces, so pushed metrics carry the same @service.name@.
detectedResources :: IO MaterializedResources
detectedResources = do
  builtIn <- detectBuiltInResources
  fromEnv <- mkResource . map Just <$> detectResourceAttributes
  svcName <- fmap T.pack <$> lookupEnv "OTEL_SERVICE_NAME"
  let base = mergeResources fromEnv builtIn
  pure . materializeResources $
    maybe base (\n -> mergeResources (mkResource ["service.name" .= n]) base) svcName

-- | Build 'Telemetry' from the caller's global meter provider (no scrape app).
withExternalTelemetry :: (Telemetry -> IO a) -> IO a
withExternalTelemetry action = do
  mp <- getGlobalMeterProvider
  waiMiddleware <- newOpenTelemetryWaiMiddleware
  meters <- globalArbiterMeters
  action
    Telemetry
      { meters = meters
      , provider = mp
      , waiMiddleware = waiMiddleware
      , metricsApp = \_ respond -> respond (responseLBS status404 [("Content-Type", "text/plain")] "metrics via external provider")
      , logDestination = Nothing
      }

-- | Whether the operator pointed a signal at a collector. Unset exports nothing, rather than
-- the spec's default localhost collector, which a linked-in library must not push to uninvited.
otlpConfigured :: IO Bool
otlpConfigured = do
  cfg <- loadExporterEnvironmentVariables
  pure . any isJust $
    [otlpEndpoint cfg, otlpTracesEndpoint cfg, otlpMetricsEndpoint cfg, otlpLogsEndpoint cfg]

-- | Prometheus is pull-based, so it pushes nothing: the scrape endpoint serves it.
wantsOtlpPush :: Bool -> IO Bool
wantsOtlpPush configured =
  lookupMetricsExporterSelection <&> \case
    Just MetricsExporterNone -> False
    Just MetricsExporterPrometheus -> False
    Just _ -> True
    Nothing -> configured

wantsOtlpLogs :: Bool -> IO Bool
wantsOtlpLogs configured =
  lookupLogsExporterSelection <&> \case
    Just LogsExporterNone -> False
    Just _ -> True
    Nothing -> configured

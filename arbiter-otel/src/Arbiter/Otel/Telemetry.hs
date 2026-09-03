{-# LANGUAGE OverloadedStrings #-}

-- | OpenTelemetry setup: traces, metrics and logs pushed over OTLP. The SDK resolves
-- each signal's endpoint and exporter from the standard @OTEL_@ variables.
module Arbiter.Otel.Telemetry
  ( Telemetry (..)
  , withTelemetry
  , withTelemetryIf
  , withTelemetryFromEnv
  , withExternalTelemetry
  , telemetryLogConfig
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Trace (resolveTracer)
import Arbiter.Worker.Logger (LogConfig, LogDestination)
import Control.Exception (SomeException, bracket)
import Control.Monad (void)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Bifunctor (first)
import Data.Either (fromRight)
import Data.Foldable (traverse_)
import Data.Maybe (catMaybes, fromMaybe, isJust)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import OpenTelemetry.Attributes (lookupAttributeByKey)
import OpenTelemetry.Attributes.Key (AttributeKey)
import OpenTelemetry.Environment (MetricsExporterSelection (..), lookupBooleanEnv, lookupMetricsExporterSelection)
import OpenTelemetry.Log
  ( LoggerProvider
  , getGlobalLoggerProvider
  , initializeGlobalLoggerProvider
  , setGlobalLoggerProvider
  , shutdownLoggerProvider
  )
import OpenTelemetry.Metric
  ( PeriodicMetricReaderOptions (..)
  , createMeterProvider
  , defaultPeriodicMetricReaderOptions
  , defaultSdkMeterProviderOptions
  , forkPeriodicMetricReader
  , periodicMetricReaderOptionsFromEnv
  , resolveMetricExporter
  , shutdownMeterProvider
  , stopPeriodicMetricReader
  )
import OpenTelemetry.Metric.Core (MeterProvider, getGlobalMeterProvider, noopMeterProvider, setGlobalMeterProvider)
import OpenTelemetry.Processor.Span (SpanProcessor)
import OpenTelemetry.Propagator (getGlobalTextMapPropagator, setGlobalTextMapPropagator)
import OpenTelemetry.Resource
  ( MaterializedResources
  , emptyMaterializedResources
  , getMaterializedResourcesAttributes
  , materializeResources
  , mergeResources
  , mkResource
  , (.=)
  )
import OpenTelemetry.Resource.Detect (detectBuiltInResources, detectResourceAttributes)
import OpenTelemetry.Trace
  ( TracerProvider
  , TracerProviderOptions (..)
  , createTracerProvider
  , emptyTracerProviderOptions
  , getGlobalTracerProvider
  , getTracerProviderInitializationOptions
  , setGlobalTracerProvider
  , shutdownTracerProvider
  )
import System.Environment (lookupEnv)
import UnliftIO (liftIO, tryAny)

import Arbiter.Otel.Logs (loggerDestination, otelLogs)
import Arbiter.Otel.Metrics (ArbiterMeters, newArbiterMeters)

-- | A running telemetry handle: instruments, provider, and resolved settings.
data Telemetry = Telemetry
  { meters :: Maybe ArbiterMeters
  -- ^ 'Nothing' when nothing is exporting metrics. Then there are no job instruments and no gauge scan.
  , provider :: MeterProvider
  , logDestination :: Maybe LogDestination
  -- ^ Where the pools' logs go. 'Nothing' leaves the caller's own destination.
  , gaugeRefresh :: NominalDiffTime
  -- ^ The metric export interval this handle resolved.
  , telemetrySummary :: Text
  -- ^ What this handle exports and where, for the caller to log at startup.
  }

-- | Bracketed OpenTelemetry init/shutdown. Nested brackets unwind partial setup. Every
-- signal is the SDK's, resolved from its own @OTEL_@ variables.
-- 'withTelemetryFromEnv' is the gated form.
withTelemetry :: (Telemetry -> IO a) -> IO a
withTelemetry action = do
  previousMeters <- getGlobalMeterProvider
  readerOpts <- periodicMetricReaderOptionsFromEnv
  detected <- tryAny getTracerProviderInitializationOptions
  let (processors, traceOpts) = fromRight ([], emptyTracerProviderOptions) detected
      detectNote = either (Just . signalFailed "traces") (const Nothing) detected
  resources <- either (const detectResources) (pure . tracerProviderOptionsResources . snd) detected
  evalContT $ do
    traces <- ContT (withTraces processors traceOpts)
    metrics <- ContT (withMeterProvider resources previousMeters)
    reader <- ContT (withReader readerOpts (snd <$> metrics))
    logs <- ContT withLogs
    liftIO $ do
      let meterProvider = either (const noopMeterProvider) fst metrics
      instruments <- either (pure . Left) (const (arbiterInstruments meterProvider)) reader
      action
        (baseTelemetry meterProvider)
          { meters = either (const Nothing) Just instruments
          , logDestination = either (const Nothing) (Just . loggerDestination) logs
          , gaugeRefresh = refreshFor readerOpts
          , telemetrySummary =
              summarize (serviceName resources) (catMaybes [detectNote, noteOf traces, noteOf instruments, noteOf logs])
          }
  where
    withTraces :: [SpanProcessor] -> TracerProviderOptions -> (Either Text TracerProvider -> IO a) -> IO a
    withTraces processors opts inner =
      bracket getGlobalTextMapPropagator setGlobalTextMapPropagator $ \_ ->
        withGlobalProvider
          "traces"
          getGlobalTracerProvider
          setGlobalTracerProvider
          initialize
          (\tracerProvider -> void (shutdownTracerProvider tracerProvider Nothing))
          inner
      where
        initialize = do
          setGlobalTextMapPropagator (tracerProviderOptionsPropagators opts)
          createTracerProvider processors opts
    withMeterProvider resources previous =
      bracketSignal
        "metrics"
        ( createMeterProvider resources defaultSdkMeterProviderOptions >>= \created -> created <$ setGlobalMeterProvider (fst created)
        )
        (\(meterProvider, _) -> setGlobalMeterProvider previous >> void (shutdownMeterProvider meterProvider Nothing))
    withReader readerOpts env inner = do
      selection <- lookupMetricsExporterSelection
      maybe (either (inner . Left) forked env) (inner . Left) (metricsOffNote selection)
      where
        forked meterEnv = bracketSignal "metrics" (forkReader meterEnv) stopPeriodicMetricReader inner
        forkReader meterEnv = resolveMetricExporter >>= \exporter -> forkPeriodicMetricReader meterEnv exporter readerOpts
    withLogs =
      withGlobalProvider
        "logs"
        getGlobalLoggerProvider
        setGlobalLoggerProvider
        initializeGlobalLoggerProvider
        (\loggerProvider -> void (shutdownLoggerProvider loggerProvider Nothing))

-- | Install a signal's SDK provider as the global one for the duration, restoring the
-- previous provider and shutting the new one down afterwards.
withGlobalProvider
  :: Text -> IO p -> (p -> IO ()) -> IO p -> (p -> IO ()) -> (Either Text p -> IO a) -> IO a
withGlobalProvider signal getGlobal setGlobal initialize shutdown inner = do
  previous <- getGlobal
  bracketSignal
    signal
    (initialize >>= \installed -> installed <$ setGlobal installed)
    (\installed -> setGlobal previous >> shutdown installed)
    inner

-- | Set a signal up and run @inner@ over it, or over the failure that left it off.
bracketSignal :: Text -> IO r -> (r -> IO ()) -> (Either Text r -> IO a) -> IO a
bracketSignal signal acquire release inner =
  bracket (tryAny acquire) (traverse_ release) (inner . first (signalFailed signal))

-- | Why a selection leaves metrics off, for the selections that do.
metricsOffNote :: Maybe MetricsExporterSelection -> Maybe Text
metricsOffNote = \case
  Just MetricsExporterNone -> Just "metrics off, OTEL_METRICS_EXPORTER=none"
  Just MetricsExporterPrometheus -> Just "metrics off, no Prometheus endpoint is served, point OTEL_EXPORTER_OTLP_ENDPOINT at a collector"
  -- The SDK's exporter resolution has no case for this one.
  Just (MetricsExporterCustom name) -> Just ("metrics off, unrecognized OTEL_METRICS_EXPORTER=" <> T.pack name)
  _ -> Nothing

-- | The resource every signal exports under, detected the way "OpenTelemetry.Trace" does.
detectResources :: IO MaterializedResources
detectResources = fromRight emptyMaterializedResources <$> tryAny detect
  where
    detect = do
      builtIn <- detectBuiltInResources
      fromEnv <- mkResource . map Just <$> detectResourceAttributes
      service <- fmap (mkResource . foldMap (\name -> ["service.name" .= T.pack name])) (lookupEnv "OTEL_SERVICE_NAME")
      pure (materializeResources (mergeResources service (mergeResources fromEnv builtIn)))

-- | The note a signal that could not start leaves in the summary.
signalFailed :: Text -> SomeException -> Text
signalFailed signal exception = signal <> " exporter did not start: " <> displayEx exception

-- | Why a signal is off, for the ones that are.
noteOf :: Either Text r -> Maybe Text
noteOf = either Just (const Nothing)

-- | The lifecycle instruments over a provider, or why they could not be built.
arbiterInstruments :: MeterProvider -> IO (Either Text ArbiterMeters)
arbiterInstruments meterProvider = first (signalFailed "metrics") <$> tryAny (newArbiterMeters meterProvider)

-- | One line for the caller to log at startup, with whatever could not be started.
summarize :: Maybe Text -> [Text] -> Text
summarize service notes =
  T.intercalate ", " (("telemetry on, service.name=" <> fromMaybe "unset" service) : notes)

-- | Service name in the detected resource. All signals use this name.
serviceName :: MaterializedResources -> Maybe Text
serviceName res = lookupAttributeByKey (getMaterializedResourcesAttributes res) ("service.name" :: AttributeKey Text)

-- | 'withTelemetry' when the flag is set. An inert handle when the flag is off.
withTelemetryIf :: Bool -> (Telemetry -> IO a) -> IO a
withTelemetryIf True action = withTelemetry action
withTelemetryIf False action = action inertTelemetry

-- | A handle that uses the API no-op providers.
inertTelemetry :: Telemetry
inertTelemetry = baseTelemetry noopMeterProvider

-- | A non-exporting handle over @meterProvider@. The installers update the fields they set.
baseTelemetry :: MeterProvider -> Telemetry
baseTelemetry meterProvider =
  Telemetry
    { meters = Nothing
    , provider = meterProvider
    , logDestination = Nothing
    , gaugeRefresh = refreshFor defaultPeriodicMetricReaderOptions
    , telemetrySummary = "telemetry off"
    }

-- | A metric reader's export interval.
refreshFor :: PeriodicMetricReaderOptions -> NominalDiffTime
refreshFor opts = fromIntegral (periodicIntervalMicros opts) / 1_000_000

-- | 'withTelemetryIf' on @OTEL_SDK_DISABLED@, the spec's own switch.
withTelemetryFromEnv :: (Telemetry -> IO a) -> IO a
withTelemetryFromEnv action = do
  disabled <- lookupBooleanEnv "OTEL_SDK_DISABLED"
  withTelemetryIf (not disabled) action

-- | Send log output to the configured destination and this handle's destination.
telemetryLogConfig :: Telemetry -> LogConfig -> LogConfig
telemetryLogConfig = otelLogs . logDestination

-- | Run an action with application-owned providers. Missing providers disable
-- their applicable signals.
withExternalTelemetry :: Maybe MeterProvider -> Maybe LoggerProvider -> (Telemetry -> IO a) -> IO a
withExternalTelemetry mmp mlp action = do
  tracing <- isJust <$> resolveTracer
  instruments <- traverse arbiterInstruments mmp
  readerOpts <- periodicMetricReaderOptionsFromEnv
  action
    (baseTelemetry (fromMaybe noopMeterProvider mmp))
      { meters = either (const Nothing) Just =<< instruments
      , logDestination = loggerDestination <$> mlp
      , gaugeRefresh = refreshFor readerOpts
      , telemetrySummary =
          T.intercalate ", " $
            "telemetry on, caller's providers"
              : catMaybes
                [ if tracing then Nothing else Just "no global tracer provider installed"
                , noteOf =<< instruments
                ]
      }

{-# LANGUAGE OverloadedStrings #-}

-- | OpenTelemetry setup: traces, metrics and logs pushed over OTLP. The SDK resolves
-- each signal's endpoint and exporter from the standard @OTEL_@ variables.
module Arbiter.Otel.Telemetry
  ( Telemetry (..)
  , withTelemetry
  , withTelemetryIf
  , withTelemetryFromEnv
  , withExternalTelemetry
  , ExternalProviders (..)
  , defaultExternalProviders
  , telemetryLogConfig
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Threads (microsPerSecond)
import Arbiter.Core.Trace (resolveTracer)
import Arbiter.Worker.Logger (LogConfig, LogDestination)
import Control.Exception (SomeException, bracket)
import Control.Monad (guard, mfilter, void)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Bifunctor (first)
import Data.Either (fromRight, isRight)
import Data.Foldable (traverse_)
import Data.Maybe (catMaybes, fromMaybe, isJust, isNothing)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import OpenTelemetry.Attributes (lookupAttributeByKey)
import OpenTelemetry.Attributes.Key (AttributeKey)
import OpenTelemetry.Environment (MetricsExporterSelection (..), lookupBooleanEnv, lookupMetricsExporterSelection)
import OpenTelemetry.Log
  ( LoggerProvider (..)
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
  -- ^ 'Nothing' when metrics are off. Then no job instruments and no gauge scan.
  , provider :: MeterProvider
  -- ^ The meter provider the gauge instruments register on.
  , telemetryLogDestination :: Maybe LogDestination
  -- ^ An extra destination the logs are copied to. 'Nothing' adds none.
  , gaugeRefresh :: NominalDiffTime
  -- ^ How often the gauges rescan the database: the metric export interval this
  -- handle resolved.
  , telemetrySummary :: Text
  -- ^ A startup line for the caller to log, with a note for each signal that failed to start.
  }

-- | Bracketed OpenTelemetry init and shutdown. Every signal is the SDK's, resolved from
-- its own @OTEL_@ variables.
-- 'withTelemetryFromEnv' is the gated form.
withTelemetry :: (Telemetry -> IO a) -> IO a
withTelemetry action = do
  previousMeters <- getGlobalMeterProvider
  readerOpts <- periodicMetricReaderOptionsFromEnv
  detected <- tryAny getTracerProviderInitializationOptions
  resources <- either (const detectResources) (pure . tracerProviderOptionsResources . snd) detected
  let tracesExport = either (const False) (not . null . fst) detected
  evalContT $ do
    traces <- ContT (withTraces (first detectionFailed detected))
    metrics <- ContT (withMeterProvider resources previousMeters)
    reader <- ContT (withReader readerOpts (snd <$> metrics))
    logs <- ContT withLogs
    liftIO $ do
      let meterProvider = either (const noopMeterProvider) fst metrics
          exporting = either (const Nothing) (mfilter exportsLogs . Just) logs
      instruments <- either (pure . Left) (const (arbiterInstruments meterProvider)) reader
      action
        (baseTelemetry meterProvider)
          { meters = either (const Nothing) Just instruments
          , telemetryLogDestination = loggerDestination <$> exporting
          , gaugeRefresh = refreshFor readerOpts
          , telemetrySummary =
              summarize
                ((isRight traces && tracesExport) || isRight instruments || isJust exporting)
                ("service.name=" <> fromMaybe "unset" (serviceName resources))
                [ noteOf traces
                , "traces off, no span exporter configured" <$ guard (isRight traces && not tracesExport)
                , noteOf instruments
                , noteOf logs
                , "logs off, no log exporter configured" <$ guard (isRight logs && isNothing exporting)
                ]
          }
  where
    withTraces
      :: Either Text ([SpanProcessor], TracerProviderOptions) -> (Either Text TracerProvider -> IO a) -> IO a
    withTraces detected inner = either (inner . Left) (\(processors, opts) -> installTraces processors opts inner) detected
    installTraces processors opts inner =
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

-- | Reason metrics are disabled, when applicable.
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

-- | The note a trace setup that could not resolve its options leaves in the summary.
detectionFailed :: SomeException -> Text
detectionFailed exception =
  "traces not installed, the previous global tracer provider stays, its options did not resolve: "
    <> displayEx exception

-- | Reason a signal is disabled, when applicable.
noteOf :: Either Text r -> Maybe Text
noteOf = either Just (const Nothing)

-- | The job, admission and maintenance instruments over a provider, or why they could not be built.
arbiterInstruments :: MeterProvider -> IO (Either Text ArbiterMeters)
arbiterInstruments meterProvider = first (signalFailed "metrics") <$> tryAny (newArbiterMeters meterProvider)

-- | Startup log line naming the provider source and listing unavailable telemetry components.
-- It reads off when no signal is on.
summarize :: Bool -> Text -> [Maybe Text] -> Text
summarize anyOn source notes = T.intercalate ", " ((state <> source) : catMaybes notes)
  where
    state = if anyOn then "telemetry on, " else "telemetry off, "

-- | Service name in the detected resource. All signals use this name.
serviceName :: MaterializedResources -> Maybe Text
serviceName res = lookupAttributeByKey (getMaterializedResourcesAttributes res) ("service.name" :: AttributeKey Text)

-- | Whether a logger provider sends its records anywhere.
exportsLogs :: LoggerProvider -> Bool
exportsLogs = not . null . loggerProviderProcessors

-- | A handle that uses the API no-op providers.
inertTelemetry :: Telemetry
inertTelemetry = baseTelemetry noopMeterProvider

-- | A non-exporting handle over @meterProvider@. The installers update the fields they set.
baseTelemetry :: MeterProvider -> Telemetry
baseTelemetry meterProvider =
  Telemetry
    { meters = Nothing
    , provider = meterProvider
    , telemetryLogDestination = Nothing
    , gaugeRefresh = refreshFor defaultPeriodicMetricReaderOptions
    , telemetrySummary = "telemetry off"
    }

-- | A metric reader's export interval.
refreshFor :: PeriodicMetricReaderOptions -> NominalDiffTime
refreshFor opts = fromIntegral (periodicIntervalMicros opts) / microsPerSecond

-- | 'withTelemetry', or an inert handle when @OTEL_SDK_DISABLED@ is @true@.
withTelemetryFromEnv :: (Telemetry -> IO a) -> IO a
withTelemetryFromEnv action = lookupBooleanEnv "OTEL_SDK_DISABLED" >>= \disabled -> withTelemetryIf (not disabled) action

-- | 'withTelemetry' when the flag is set. An inert handle when the flag is off.
withTelemetryIf :: Bool -> (Telemetry -> IO a) -> IO a
withTelemetryIf True action = withTelemetry action
withTelemetryIf False action = action inertTelemetry

-- | Also send the config's logs to this handle's destination. A config that discards
-- its logs still exports them.
telemetryLogConfig :: Telemetry -> LogConfig -> LogConfig
telemetryLogConfig = otelLogs . telemetryLogDestination

-- | Providers the application owns, for 'withExternalTelemetry'.
data ExternalProviders = ExternalProviders
  { externalMeterProvider :: Maybe MeterProvider
  -- ^ 'Nothing' turns metrics off.
  , externalLoggerProvider :: Maybe LoggerProvider
  -- ^ 'Nothing' turns the log copy off.
  , externalGaugeRefresh :: NominalDiffTime
  -- ^ How often the gauges rescan the database. Match the meter provider's export
  -- interval.
  }

-- | No providers, and the default 60s gauge refresh.
defaultExternalProviders :: ExternalProviders
defaultExternalProviders =
  ExternalProviders
    { externalMeterProvider = Nothing
    , externalLoggerProvider = Nothing
    , externalGaugeRefresh = refreshFor defaultPeriodicMetricReaderOptions
    }

-- | Run an action with application-owned providers. Missing providers disable
-- their applicable signals.
withExternalTelemetry :: ExternalProviders -> (Telemetry -> IO a) -> IO a
withExternalTelemetry external action = do
  tracing <- isJust <$> resolveTracer
  instruments <- traverse arbiterInstruments (externalMeterProvider external)
  let mlp = externalLoggerProvider external
      exporting = mfilter exportsLogs mlp
  action
    (baseTelemetry (fromMaybe noopMeterProvider (externalMeterProvider external)))
      { meters = either (const Nothing) Just =<< instruments
      , telemetryLogDestination = loggerDestination <$> exporting
      , gaugeRefresh = externalGaugeRefresh external
      , telemetrySummary =
          summarize
            (tracing || any isRight instruments || isJust exporting)
            "caller's providers"
            [ "no global tracer provider installed" <$ guard (not tracing)
            , maybe (Just "metrics off, no meter provider") noteOf instruments
            , "logs off, no logger provider" <$ guard (isNothing mlp)
            , "logs off, the logger provider has no processor" <$ guard (isJust mlp && isNothing exporting)
            ]
      }

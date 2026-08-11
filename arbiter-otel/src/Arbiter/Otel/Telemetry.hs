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

import Arbiter.Core.Trace (resolveTracer)
import Arbiter.Worker.Logger (LogConfig, LogDestination)
import Control.Exception (SomeException, bracket, displayException)
import Control.Monad (void)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Bifunctor (first)
import Data.Foldable (traverse_)
import Data.Maybe (catMaybes, fromMaybe, isJust, isNothing)
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

data Telemetry = Telemetry
  { meters :: Maybe ArbiterMeters
  -- ^ 'Nothing' when nothing is exporting metrics: no job instruments, no gauge scan.
  , provider :: MeterProvider
  , logDestination :: Maybe LogDestination
  -- ^ Where the pools' logs go. 'Nothing' leaves the caller's own destination.
  , gaugeRefresh :: NominalDiffTime
  -- ^ The metric export interval this handle resolved.
  , telemetrySummary :: Text
  -- ^ What this handle exports and where, for the caller to log at startup.
  }

-- | Bracketed OpenTelemetry init/shutdown, nested so partial setup unwinds. Every
-- signal is the SDK's, resolved from its own @OTEL_@ variables.
-- 'withTelemetryFromEnv' is the gated form.
withTelemetry :: (Telemetry -> IO a) -> IO a
withTelemetry action = do
  detected <- tryAny getTracerProviderInitializationOptions
  let (processors, traceOpts) = either (const ([], emptyTracerProviderOptions)) id detected
      detectNote = either (Just . signalFailed "traces") (const Nothing) detected
  resources <- either (const detectResources) (pure . tracerProviderOptionsResources . snd) detected
  previousMeters <- getGlobalMeterProvider
  readerOpts <- periodicMetricReaderOptionsFromEnv
  evalContT $ do
    (mp, env) <- ContT (withMeterProvider resources previousMeters)
    reader <- ContT (withReader readerOpts env)
    traces <- ContT (withTraces processors traceOpts)
    logs <- ContT withLogs
    liftIO $ do
      ms <- ifStarted (noteOf reader) (newArbiterMeters mp)
      action
        (baseTelemetry mp)
          { meters = ms
          , logDestination = either (const Nothing) (Just . loggerDestination) logs
          , gaugeRefresh = refreshFor readerOpts
          , telemetrySummary =
              summarize (serviceName resources) (catMaybes [detectNote, noteOf traces, noteOf reader, noteOf logs])
          }
  where
    -- Why a signal is off, for the ones that are.
    noteOf :: Either Text r -> Maybe Text
    noteOf = either Just (const Nothing)
    -- Bind a signal's handle only when nothing went wrong starting it.
    ifStarted :: Maybe Text -> IO b -> IO (Maybe b)
    ifStarted note act = if isNothing note then Just <$> act else pure Nothing
    withTraces :: [SpanProcessor] -> TracerProviderOptions -> (Either Text TracerProvider -> IO a) -> IO a
    withTraces processors opts inner =
      bracket getGlobalTextMapPropagator setGlobalTextMapPropagator $ \_ ->
        withGlobalProvider
          "traces"
          getGlobalTracerProvider
          setGlobalTracerProvider
          initialize
          (\tp -> void (shutdownTracerProvider tp Nothing))
          inner
      where
        initialize = do
          setGlobalTextMapPropagator (tracerProviderOptionsPropagators opts)
          createTracerProvider processors opts
    withMeterProvider resources previous inner =
      bracket
        (createMeterProvider resources defaultSdkMeterProviderOptions)
        (\(m, _) -> setGlobalMeterProvider previous >> void (shutdownMeterProvider m Nothing))
        (\(m, env) -> setGlobalMeterProvider m >> inner (m, env))
    withReader readerOpts env inner = do
      selection <- lookupMetricsExporterSelection
      maybe (bracketSignal "metrics" forkReader stopPeriodicMetricReader inner) (inner . Left) (metricsOffNote selection)
      where
        forkReader = resolveMetricExporter >>= \e -> forkPeriodicMetricReader env e readerOpts
    withLogs =
      withGlobalProvider
        "logs"
        getGlobalLoggerProvider
        setGlobalLoggerProvider
        initializeGlobalLoggerProvider
        (\lp -> void (shutdownLoggerProvider lp Nothing))

-- | Install a signal's SDK provider as the global one for the duration, restoring the
-- previous provider and shutting the new one down afterwards.
withGlobalProvider
  :: Text -> IO p -> (p -> IO ()) -> IO p -> (p -> IO ()) -> (Either Text p -> IO a) -> IO a
withGlobalProvider signal getGlobal setGlobal initialize shutdown inner = do
  previous <- getGlobal
  bracketSignal signal (initialize >>= \p -> p <$ setGlobal p) (\p -> setGlobal previous >> shutdown p) inner

-- | Set a signal up and run @inner@ over it, or over the failure that left it off.
bracketSignal :: Text -> IO r -> (r -> IO ()) -> (Either Text r -> IO a) -> IO a
bracketSignal signal acquire release inner =
  bracket (tryAny acquire) (traverse_ release) (inner . first (signalFailed signal))

-- | Why a selection leaves metrics off, for the selections that do.
metricsOffNote :: Maybe MetricsExporterSelection -> Maybe Text
metricsOffNote = \case
  Just MetricsExporterNone -> Just "metrics off, OTEL_METRICS_EXPORTER=none"
  Just MetricsExporterPrometheus -> Just "metrics off, no Prometheus endpoint is served, point OTEL_EXPORTER_OTLP_ENDPOINT at a collector"
  _ -> Nothing

-- | The resource every signal exports under, detected the way "OpenTelemetry.Trace" does.
detectResources :: IO MaterializedResources
detectResources = either (const emptyMaterializedResources) id <$> tryAny detect
  where
    detect = do
      builtIn <- detectBuiltInResources
      fromEnv <- mkResource . map Just <$> detectResourceAttributes
      service <- fmap (mkResource . foldMap (\n -> ["service.name" .= T.pack n])) (lookupEnv "OTEL_SERVICE_NAME")
      pure (materializeResources (mergeResources service (mergeResources fromEnv builtIn)))

-- | The note a signal that could not start leaves in the summary.
signalFailed :: Text -> SomeException -> Text
signalFailed signal e = signal <> " exporter did not start: " <> T.pack (displayException e)

-- | One line for the caller to log at startup, with whatever could not be started.
summarize :: Maybe Text -> [Text] -> Text
summarize service notes =
  T.intercalate ", " (("telemetry on, service.name=" <> fromMaybe "unset" service) : notes)

-- | The service name the detected resource carries, which every signal is exported under.
serviceName :: MaterializedResources -> Maybe Text
serviceName res = lookupAttributeByKey (getMaterializedResourcesAttributes res) ("service.name" :: AttributeKey Text)

-- | 'withTelemetry' when the flag is set, an inert handle when it is not.
withTelemetryIf :: Bool -> (Telemetry -> IO a) -> IO a
withTelemetryIf True action = withTelemetry action
withTelemetryIf False action = action inertTelemetry

-- | A handle over the API's no-op providers, installing nothing.
inertTelemetry :: Telemetry
inertTelemetry = baseTelemetry noopMeterProvider

-- | An exporting-nothing handle over @mp@. The installers record-update the fields
-- they actually set.
baseTelemetry :: MeterProvider -> Telemetry
baseTelemetry mp =
  Telemetry
    { meters = Nothing
    , provider = mp
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

-- | Send a log config's output to this handle's destination as well as its own.
telemetryLogConfig :: Telemetry -> LogConfig -> LogConfig
telemetryLogConfig = otelLogs . logDestination

-- | Build 'Telemetry' over providers the caller installed itself, which the meters and
-- the log destination bind to here. A 'Nothing' provider leaves that signal off.
withExternalTelemetry :: Maybe MeterProvider -> Maybe LoggerProvider -> (Telemetry -> IO a) -> IO a
withExternalTelemetry mmp mlp action = do
  tracing <- isJust <$> resolveTracer
  ms <- traverse newArbiterMeters mmp
  readerOpts <- periodicMetricReaderOptionsFromEnv
  action
    (baseTelemetry (fromMaybe noopMeterProvider mmp))
      { meters = ms
      , logDestination = loggerDestination <$> mlp
      , gaugeRefresh = refreshFor readerOpts
      , telemetrySummary =
          "telemetry on, caller's providers" <> if tracing then mempty else ", no global tracer provider installed"
      }

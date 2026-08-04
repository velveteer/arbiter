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
import Control.Exception (bracket, displayException)
import Control.Monad (guard, void)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Foldable (traverse_)
import Data.Maybe (catMaybes, fromMaybe, isJust, isNothing)
import Data.Text (Text)
import Data.Text qualified as T
import OpenTelemetry.Attributes (lookupAttributeByKey)
import OpenTelemetry.Attributes.Key (AttributeKey)
import OpenTelemetry.Environment (lookupBooleanEnv)
import OpenTelemetry.Log
  ( LoggerProvider
  , getGlobalLoggerProvider
  , initializeGlobalLoggerProvider
  , setGlobalLoggerProvider
  , shutdownLoggerProvider
  )
import OpenTelemetry.Metric
  ( createMeterProvider
  , defaultSdkMeterProviderOptions
  , forkPeriodicMetricReader
  , periodicMetricReaderOptionsFromEnv
  , resolveMetricExporter
  , shutdownMeterProvider
  , stopPeriodicMetricReader
  )
import OpenTelemetry.Metric.Core (MeterProvider, getGlobalMeterProvider, noopMeterProvider, setGlobalMeterProvider)
import OpenTelemetry.Resource
  ( MaterializedResources
  , getMaterializedResourcesAttributes
  , materializeResources
  , mergeResources
  , mkResource
  , (.=)
  )
import OpenTelemetry.Resource.Detect (detectBuiltInResources, detectResourceAttributes)
import OpenTelemetry.Trace
  ( getGlobalTracerProvider
  , initializeGlobalTracerProvider
  , setGlobalTracerProvider
  , shutdownTracerProvider
  )
import System.Environment (lookupEnv)
import UnliftIO (liftIO, tryAny)

import Arbiter.Otel.Metrics (ArbiterMeters, loggerDestination, newArbiterMeters, otelLogs)

data Telemetry = Telemetry
  { meters :: Maybe ArbiterMeters
  -- ^ 'Nothing' when nothing is exporting metrics: no job instruments, no gauge scan.
  , provider :: MeterProvider
  , logDestination :: Maybe LogDestination
  -- ^ Where the pools' logs go. 'Nothing' leaves the caller's own destination.
  , telemetrySummary :: Text
  -- ^ What this handle exports and where, for the caller to log at startup.
  }

-- | Bracketed OpenTelemetry init/shutdown, nested so partial setup unwinds. Every
-- signal is the SDK's, resolved from its own @OTEL_@ variables.
-- 'withTelemetryFromEnv' is the gated form.
withTelemetry :: (Telemetry -> IO a) -> IO a
withTelemetry action = do
  resources <- detectedResources
  previousMeters <- getGlobalMeterProvider
  evalContT $ do
    (mp, env) <- ContT (withMeterProvider resources previousMeters)
    metricsNote <- ContT (withReader env)
    tracesNote <- ContT withTraces
    logsNote <- ContT withLogs
    liftIO $ do
      ms <- traverse (const (newArbiterMeters mp)) (guard (isNothing metricsNote))
      dest <- loggerDestination <$> getGlobalLoggerProvider
      action
        (baseTelemetry mp)
          { meters = ms
          , logDestination = Just dest
          , telemetrySummary = summarize (serviceName resources) (catMaybes [tracesNote, metricsNote, logsNote])
          }
  where
    withTraces =
      withGlobalProvider "traces" getGlobalTracerProvider setGlobalTracerProvider initializeGlobalTracerProvider $
        \tp -> void (shutdownTracerProvider tp Nothing)
    withMeterProvider resources previous inner =
      bracket
        (createMeterProvider resources defaultSdkMeterProviderOptions)
        (\(m, _) -> setGlobalMeterProvider previous >> void (shutdownMeterProvider m Nothing))
        (\(m, env) -> setGlobalMeterProvider m >> inner (m, env))
    withReader env =
      bracketSignal
        "metrics"
        (resolveMetricExporter >>= \e -> periodicMetricReaderOptionsFromEnv >>= forkPeriodicMetricReader env e)
        stopPeriodicMetricReader
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
  :: Text -> IO p -> (p -> IO ()) -> IO p -> (p -> IO ()) -> (Maybe Text -> IO a) -> IO a
withGlobalProvider signal getGlobal setGlobal initialize shutdown inner = do
  previous <- getGlobal
  bracketSignal signal initialize (\p -> setGlobal previous >> shutdown p) inner

-- | Set a signal up and run @inner@ under it, or under the failure that left it off.
bracketSignal :: Text -> IO r -> (r -> IO ()) -> (Maybe Text -> IO a) -> IO a
bracketSignal signal acquire release inner =
  bracket (tryAny acquire) (traverse_ release) (inner . either (Just . failed) (const Nothing))
  where
    failed e = signal <> " exporter did not start: " <> T.pack (displayException e)

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
    , telemetrySummary = "telemetry off"
    }

-- | 'withTelemetryIf' on @OTEL_SDK_DISABLED@, the spec's own switch.
withTelemetryFromEnv :: (Telemetry -> IO a) -> IO a
withTelemetryFromEnv action = do
  disabled <- lookupBooleanEnv "OTEL_SDK_DISABLED"
  withTelemetryIf (not disabled) action

-- | Send a log config's output to this handle's destination as well as its own.
telemetryLogConfig :: Telemetry -> LogConfig -> LogConfig
telemetryLogConfig = otelLogs . logDestination

-- | The resource the SDK detects for traces, so pushed metrics carry the same @service.name@.
detectedResources :: IO MaterializedResources
detectedResources = do
  builtIn <- detectBuiltInResources
  fromEnv <- mkResource . map Just <$> detectResourceAttributes
  svcName <- fmap T.pack <$> lookupEnv "OTEL_SERVICE_NAME"
  let base = mergeResources fromEnv builtIn
  pure . materializeResources $
    maybe base (\n -> mergeResources (mkResource ["service.name" .= n]) base) svcName

-- | Build 'Telemetry' over providers the caller installed itself, which the meters and
-- the log destination bind to here. 'Nothing' exports no logs.
withExternalTelemetry :: MeterProvider -> Maybe LoggerProvider -> (Telemetry -> IO a) -> IO a
withExternalTelemetry mp mlp action = do
  tracing <- isJust <$> resolveTracer
  ms <- newArbiterMeters mp
  action
    (baseTelemetry mp)
      { meters = Just ms
      , logDestination = loggerDestination <$> mlp
      , telemetrySummary =
          "telemetry on, caller's providers" <> if tracing then mempty else ", no global tracer provider installed"
      }

{-# LANGUAGE OverloadedStrings #-}

-- | OpenTelemetry setup: one meter provider feeding a Prometheus scrape and OTLP push.
-- The SDK resolves each signal's endpoint and exporter from the standard @OTEL_@ variables.
module Arbiter.Otel.Telemetry
  ( Telemetry (..)
  , GaugeClaim (..)
  , withTelemetry
  , withTelemetryIf
  , withTelemetryFromEnv
  , withExternalTelemetry
  , withMetricsEndpoint
  , claimGaugeSlot
  , telemetryLogConfig
  ) where

import Arbiter.Core.Threads (labelArbiterThread)
import Arbiter.Core.Trace (resolveTracer)
import Arbiter.Worker (tryLog)
import Arbiter.Worker.Logger (LogConfig, LogDestination, LogLevel (..))
import Control.Applicative ((<|>))
import Control.Concurrent.MVar (MVar, modifyMVar, modifyMVar_, newMVar)
import Control.Exception (bracket, displayException)
import Control.Monad (guard, mfilter, void)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Foldable (traverse_)
import Data.List (dropWhileEnd)
import Data.Maybe (fromMaybe, isJust)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Vector qualified as Vector
import Network.HTTP.Types (status404)
import Network.Wai (Application, pathInfo, responseLBS)
import Network.Wai.Handler.Warp (defaultSettings, runSettings, setPort)
import OpenTelemetry.Attributes (lookupAttributeByKey)
import OpenTelemetry.Attributes.Key (AttributeKey)
import OpenTelemetry.Environment
  ( LogsExporterSelection (..)
  , MetricsExporterSelection (..)
  , lookupBooleanEnv
  , lookupLogsExporterSelection
  , lookupMetricsExporterSelection
  )
import OpenTelemetry.Exporter.Prometheus.WAI (prometheusApplication)
import OpenTelemetry.Log
  ( LoggerProvider
  , getGlobalLoggerProvider
  , initializeGlobalLoggerProvider
  , setGlobalLoggerProvider
  , shutdownLoggerProvider
  )
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
import System.IO.Unsafe (unsafePerformIO)
import System.Mem.StableName (StableName, makeStableName)
import UnliftIO (MonadUnliftIO, liftIO, tryAny)
import UnliftIO.Async (link, withAsync)

import Arbiter.Otel.Gauges.Cells (GaugeCells, newGaugeCells)
import Arbiter.Otel.Metrics (ArbiterMeters, loggerDestination, newArbiterMeters, otelLogDestination, otelLogs)

data Telemetry = Telemetry
  { enabled :: Bool
  -- ^ False for the inert handle: no SDK installed, no gauge scan, no scrape port.
  , metricsEnabled :: Bool
  -- ^ False under @OTEL_METRICS_EXPORTER=none@: no gauge scan, no scrape port.
  , meters :: ArbiterMeters
  , provider :: MeterProvider
  , metricsApp :: Maybe Application
  -- ^ The Prometheus scrape app. 'Nothing' when this handle serves no scrape endpoint.
  , logDestination :: Maybe LogDestination
  -- ^ Where the pools' logs go. 'Nothing' leaves the caller's own destination.
  , telemetrySummary :: Text
  -- ^ What this handle exports and where, for the caller to log at startup.
  }

-- | Bracketed OpenTelemetry init/shutdown on the SDK's own defaults, nested so partial
-- setup unwinds. 'withTelemetryFromEnv' is the gated form.
withTelemetry :: (Telemetry -> IO a) -> IO a
withTelemetry action = do
  resources <- detectedResources
  (wantMetrics, push) <- metricsExporter
  traces <- tracesExporter
  logs <- logsExporter
  previousMeters <- getGlobalMeterProvider
  evalContT $ do
    (mp, mEnv) <- ContT (withMeterProvider wantMetrics resources previousMeters)
    metricsSignal <- ContT (withOtlpReader push mEnv)
    tracesSignal <- ContT (withTraces (isJust traces))
    (logsSignal, dest) <- ContT (withLogs (isJust logs))
    liftIO $ do
      base <- baseTelemetry mp
      action
        base
          { enabled = True
          , metricsEnabled = wantMetrics
          , metricsApp =
              (\env -> scrapeOnly (prometheusApplication (Vector.fromList <$> collectResourceMetrics env))) <$> mEnv
          , logDestination = dest
          , telemetrySummary =
              summarize
                (signalLabel tracesSignal traces)
                (metricsMode wantMetrics (exporting metricsSignal))
                (signalLabel logsSignal logs)
                (serviceName resources)
                [note | ExporterFailed note <- [tracesSignal, metricsSignal, logsSignal]]
          }
  where
    withTraces wanted =
      withGlobalProvider wanted "traces" getGlobalTracerProvider setGlobalTracerProvider initializeGlobalTracerProvider $
        \tp -> void (shutdownTracerProvider tp Nothing)
    -- Metrics off installs no provider at all, so instruments the process resolves
    -- globally stay no-ops.
    withMeterProvider wanted resources previous inner
      | not wanted = inner (noopMeterProvider, Nothing)
      | otherwise =
          bracket
            (createMeterProvider resources defaultSdkMeterProviderOptions)
            (\(m, _) -> setGlobalMeterProvider previous >> void (shutdownMeterProvider m Nothing))
            (\(m, env) -> setGlobalMeterProvider m >> inner (m, Just env))
    withOtlpReader wanted mEnv inner = case guard wanted >> mEnv of
      Nothing -> inner ExporterOff
      Just env ->
        bracketSignal
          "metrics"
          (resolveMetricExporter >>= \e -> periodicMetricReaderOptionsFromEnv >>= forkPeriodicMetricReader env e)
          stopPeriodicMetricReader
          inner
    withLogs wanted inner =
      withGlobalProvider
        wanted
        "logs"
        getGlobalLoggerProvider
        setGlobalLoggerProvider
        initializeGlobalLoggerProvider
        (\lp -> void (shutdownLoggerProvider lp Nothing))
        (\signal -> inner (signal, otelLogDestination <$ guard (exporting signal)))

-- | What became of one signal's exporter.
data Signal
  = Exporting
  | -- | Off by configuration.
    ExporterOff
  | -- | Off because the SDK could not build it, with what to tell the operator.
    ExporterFailed Text

exporting :: Signal -> Bool
exporting Exporting = True
exporting _ = False

-- | A signal that did not start is off, whatever it was pointed at.
signalLabel :: Signal -> Maybe Text -> Text
signalLabel Exporting = fromMaybe "off"
signalLabel _ = const "off"

-- | Install a signal's SDK provider as the global one for the duration, restoring the
-- previous provider and shutting the new one down afterwards.
withGlobalProvider
  :: Bool -> Text -> IO p -> (p -> IO ()) -> IO p -> (p -> IO ()) -> (Signal -> IO a) -> IO a
withGlobalProvider wanted signal getGlobal setGlobal initialize shutdown inner
  | not wanted = inner ExporterOff
  | otherwise = do
      previous <- getGlobal
      bracketSignal signal initialize (\p -> setGlobal previous >> shutdown p) inner

-- | Set a signal up and run @inner@ under it, or under the failure that left it off.
bracketSignal :: Text -> IO r -> (r -> IO ()) -> (Signal -> IO a) -> IO a
bracketSignal signal acquire release inner =
  bracket (tryAny acquire) (traverse_ release) (inner . either failed (const Exporting))
  where
    failed e = ExporterFailed (signal <> " exporter did not start: " <> T.pack (displayException e))

-- | One line naming where each signal goes, for the caller to log at startup, with
-- whatever could not be started.
summarize :: Text -> Text -> Text -> Maybe Text -> [Text] -> Text
summarize traces metrics logs service notes =
  T.intercalate ", " $
    [ "traces=" <> traces
    , "metrics=" <> metrics
    , "logs=" <> logs
    , "service.name=" <> fromMaybe "unset" service
    ]
      <> notes

-- | The service name the detected resource carries, which every signal is exported under.
serviceName :: MaterializedResources -> Maybe Text
serviceName res = lookupAttributeByKey (getMaterializedResourcesAttributes res) ("service.name" :: AttributeKey Text)

-- | A scrape endpoint this handle can serve, an OTLP push, or both.
metricsMode :: Bool -> Bool -> Text
metricsMode False _ = "off"
metricsMode True push = if push then "scrape+otlp" else "scrape"

-- | 'withTelemetry' when the flag is set, an inert handle when it is not.
withTelemetryIf :: Bool -> (Telemetry -> IO a) -> IO a
withTelemetryIf True action = withTelemetry action
withTelemetryIf False action = action =<< inertTelemetry

-- | A handle over the API's no-op providers, installing nothing.
inertTelemetry :: IO Telemetry
inertTelemetry = baseTelemetry noopMeterProvider

-- | An exporting-nothing handle over @mp@. The installers record-update the fields
-- they actually set.
baseTelemetry :: MeterProvider -> IO Telemetry
baseTelemetry mp = do
  ms <- newArbiterMeters mp
  pure
    Telemetry
      { enabled = False
      , metricsEnabled = False
      , meters = ms
      , provider = mp
      , metricsApp = Nothing
      , logDestination = Nothing
      , telemetrySummary = "telemetry off, no exporter configured"
      }

-- | 'withTelemetryIf' on the standard variables: on once a signal names an exporter,
-- off under @OTEL_SDK_DISABLED@.
withTelemetryFromEnv :: (Telemetry -> IO a) -> IO a
withTelemetryFromEnv action = do
  disabled <- lookupBooleanEnv "OTEL_SDK_DISABLED"
  configured <- exporterConfigured
  withTelemetryIf (not disabled && configured) action

-- | Whether the operator pointed a signal at anything, by collector endpoint or by named
-- exporter. Unset exports nothing, rather than the spec's default localhost collector.
-- Asked of the same functions the setup installs from, so the gate cannot disagree with
-- what is then installed.
exporterConfigured :: IO Bool
exporterConfigured = do
  traces <- tracesExporter
  logs <- logsExporter
  -- Metrics collect by default, so only a named exporter or an endpoint points them
  -- anywhere: Prometheus is pull-based and never pushes.
  named <- lookupMetricsExporterSelection
  (_, push) <- metricsExporter
  pure . or $
    [ isJust traces
    , isJust logs
    , any (/= MetricsExporterNone) named
    , push
    ]

-- | The exporter name an operator gave, normalized the way the SDK reads it: the first
-- entry of the list, lowercased and trimmed. An empty value counts as unset, a
-- deployment spec leaving a variable blank meaning the same.
exporterSelection :: String -> IO (Maybe Text)
exporterSelection name = mfilter (not . T.null) . fmap (normalize . T.pack) <$> lookupEnv name
  where
    normalize = T.toLower . T.strip . T.takeWhile (/= ',')

-- | Answer the scrape at the mount root and at @\/metrics@, trailing slash or not,
-- nothing else. Collecting the whole snapshot for an unrelated path under the mount
-- is wasted work.
scrapeOnly :: Application -> Application
scrapeOnly app req respond
  | dropWhileEnd T.null (pathInfo req) `elem` [[], ["metrics"]] = app req respond
  | otherwise = respond (responseLBS status404 [] mempty)

-- | Serve the Prometheus scrape endpoint on its own port for the duration of the action.
-- A handle with no scrape app of its own binds nothing.
withMetricsEndpoint :: (MonadUnliftIO m) => Telemetry -> LogConfig -> Int -> m a -> m a
withMetricsEndpoint tel baseLog port action = maybe (unserved >> action) serve (metricsApp tel)
  where
    -- An explicit request that binds nothing would otherwise look like a scrape target
    -- that is up but empty.
    unserved = tryLog (telemetryLogConfig tel baseLog) Warning ("Binding no metrics endpoint: " <> reason)
    reason
      | not (metricsEnabled tel) = "metrics are off"
      | otherwise = "this handle serves no scrape endpoint of its own, the caller's reader does"

    -- Linked, so a port it cannot bind fails the process.
    serve app =
      withAsync (liftIO (labelArbiterThread "metrics-server" Nothing >> runSettings (setPort port defaultSettings) app)) $
        \server -> link server >> action

-- | Send a log config's output to this handle's destination as well as its own.
telemetryLogConfig :: Telemetry -> LogConfig -> LogConfig
telemetryLogConfig = otelLogs . logDestination

-- | One meter provider's gauge registration, and whether a caller holds it. The cells
-- outlive the claim: the SDK cannot unregister an observable callback.
data GaugeSlot = GaugeSlot
  { slotCells :: GaugeCells
  , slotHeld :: Bool
  }

-- | Gauge registrations by the meter provider they were made against, rather than by
-- handle: two handles over one provider would otherwise each register the instruments,
-- and every observation would be counted twice.
gaugeSlots :: MVar [(StableName MeterProvider, GaugeSlot)]
gaugeSlots = unsafePerformIO (newMVar [])
{-# NOINLINE gaugeSlots #-}

-- | A held gauge slot.
data GaugeClaim = GaugeClaim
  { claimCells :: GaugeCells
  , claimRegistered :: Bool
  -- ^ True when this provider's instruments already exist and only need their cells reset.
  , releaseGaugeSlot :: IO ()
  }

-- | Take the gauge registration for this handle's meter provider. 'Nothing' once
-- another caller holds it.
claimGaugeSlot :: Telemetry -> IO (Maybe GaugeClaim)
claimGaugeSlot tel = do
  -- Forced, so the name is the provider's and not a fresh selector thunk's.
  key <- makeStableName $! provider tel
  modifyMVar gaugeSlots $ \slots -> case lookup key slots of
    Just held | slotHeld held -> pure (slots, Nothing)
    prior -> do
      cells <- maybe newGaugeCells (pure . slotCells) prior
      let claim = GaugeClaim cells (isJust prior) (release key)
      pure (setSlot key (GaugeSlot cells True) slots, Just claim)
  where
    setSlot key slot = ((key, slot) :) . filter ((/= key) . fst)
    release key = modifyMVar_ gaugeSlots (pure . map (\(k, v) -> (k, if k == key then v {slotHeld = False} else v)))

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
-- the log destination bind to here (no scrape app). 'Nothing' exports no logs.
withExternalTelemetry :: MeterProvider -> Maybe LoggerProvider -> (Telemetry -> IO a) -> IO a
withExternalTelemetry mp mlp action = do
  tracing <- isJust <$> resolveTracer
  base <- baseTelemetry mp
  action
    base
      { enabled = True
      , metricsEnabled = True
      , logDestination = loggerDestination <$> mlp
      , telemetrySummary =
          summarize
            (if tracing then "caller's provider" else "no global provider installed")
            "caller's provider"
            (maybe "off" (const "caller's provider") mlp)
            Nothing
            []
      }

-- | The trace exporter to install, 'Nothing' when the operator pointed the signal
-- nowhere. Unlike metrics and logs, the SDK exposes no selection type for this signal.
tracesExporter :: IO (Maybe Text)
tracesExporter = do
  named <- exporterSelection "OTEL_TRACES_EXPORTER"
  fallback <- otlpDefault "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
  pure (mfilter (/= "none") (named <|> fallback))

-- | Whether metrics are collected at all, and whether they push over OTLP. Only an
-- explicit @none@ turns collection off, and Prometheus is pull-based so it does not push.
metricsExporter :: IO (Bool, Bool)
metricsExporter = do
  selection <- lookupMetricsExporterSelection
  push <- otlpConfigured "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"
  pure $ case selection of
    Just MetricsExporterNone -> (False, False)
    Just MetricsExporterPrometheus -> (True, False)
    Just _ -> (True, True)
    Nothing -> (True, push)

-- | The log exporter to install, 'Nothing' when the operator pointed the signal nowhere.
logsExporter :: IO (Maybe Text)
logsExporter =
  lookupLogsExporterSelection >>= \case
    Nothing -> otlpDefault "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT"
    Just LogsExporterNone -> pure Nothing
    Just LogsExporterOtlp -> pure (Just "otlp")
    Just LogsExporterConsole -> pure (Just "console")
    Just (LogsExporterCustom name) -> pure (Just (T.pack name))

-- | What a signal nobody named exports: OTLP once an endpoint was configured for it,
-- and nothing at all otherwise, rather than the spec's default localhost collector.
otlpDefault :: String -> IO (Maybe Text)
otlpDefault signalEndpointVar = (\ok -> "otlp" <$ guard ok) <$> otlpConfigured signalEndpointVar

-- | Whether an endpoint was configured for a signal, by its own variable or the shared one.
otlpConfigured :: String -> IO Bool
otlpConfigured signalEndpointVar =
  any isJust <$> traverse lookupEnv ["OTEL_EXPORTER_OTLP_ENDPOINT", signalEndpointVar]

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | The three signals against a real Postgres (ARBITER_TEST_CONN_STRING).
module Main (main) where

import Arbiter.Concurrency (HasConcurrency)
import Arbiter.Core.Job.Archive (ArchiveJob (..))
import Arbiter.Core.Job.DLQ (DLQJob (dlqPrimaryKey))
import Arbiter.Core.Job.Types
  ( HasKind (..)
  , ObservabilityHooks (..)
  , TraceContext (..)
  , defaultJob
  , defaultObservabilityHooks
  , payload
  , primaryKey
  , setArchiveFor
  , setTraceContext
  , traceContext
  )
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Core.SqlLiterals (quoteIdentifier)
import Arbiter.Core.Trace
  ( ConsumeShape (..)
  , capturingContext
  , consumeSpanFor
  , currentTraceContext
  , markSpanError
  , recordJobFailure
  , resolveTracer
  , withConsumeSpan
  , withPublishSpan
  )
import Arbiter.Migrations (MigrationResult (..), defaultMigrationConfig, runMigrationsForRegistry)
import Arbiter.RateLimit (HasRateLimit)
import Arbiter.Simple (SimpleDb, createSimpleEnv, runSimpleDb)
import Arbiter.Test.Config (getTestConnectionString)
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (execute_)
import Arbiter.Worker
  ( MaintenanceOp (..)
  , WorkerConfig (..)
  , defaultLogConfig
  , runWorkerPool
  , transactionalWorkerConfig
  )
import Control.Exception (bracket)
import Control.Monad (foldM_, join, void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Char (isAsciiLower)
import Data.Foldable (toList, traverse_)
import Data.IORef (IORef, modifyIORef', newIORef, readIORef)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (listToMaybe)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Data.Text.IO qualified as TIO
import Data.Time (getCurrentTime)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import GHC.Generics (Generic)
import OpenTelemetry.Attributes (Attributes, emptyAttributes, lookupAttributeByKey)
import OpenTelemetry.Attributes.Key (AttributeKey)
import OpenTelemetry.Context (empty, insertSpan)
import OpenTelemetry.Context.ThreadLocal (attachContext, detachContext)
import OpenTelemetry.Exporter.Metric
  ( MetricExport (..)
  , NumberValue (..)
  , ResourceMetricsExport (..)
  , ScopeMetricsExport (..)
  , exponentialHistogramDataPointAttributes
  , gaugeDataPointAttributes
  , histogramDataPointAttributes
  , sumDataPointAttributes
  , sumDataPointValue
  )
import OpenTelemetry.MeterProvider (collectResourceMetrics)
import OpenTelemetry.Metric (SdkMeterEnv, createMeterProvider, defaultSdkMeterProviderOptions)
import OpenTelemetry.Metric.Core
  ( counterAdd
  , defaultAdvisoryParameters
  , getMeter
  , meterCreateCounterDouble
  )
import OpenTelemetry.Processor.Span (SpanProcessor (..))
import OpenTelemetry.Propagator.W3CTraceContext (decodeSpanContext)
import OpenTelemetry.Resource (materializeResources, mkResource)
import OpenTelemetry.Trace.Core
  ( Event (..)
  , FlushResult (..)
  , ImmutableSpan (..)
  , Link (..)
  , ShutdownResult (..)
  , SpanContext (..)
  , SpanHot (..)
  , SpanKind (..)
  , SpanStatus (..)
  , TracerProvider
  , createTracerProvider
  , emptyTracerProviderOptions
  , setGlobalTracerProvider
  , wrapSpanContext
  )
import OpenTelemetry.Util (appendOnlyBoundedCollectionValues)
import System.Directory (doesFileExist)
import Test.Hspec
import UnliftIO.Async (withAsync)

import Arbiter.Otel qualified as Otel
import Arbiter.Otel.Gauges.Cache qualified as Cache

newtype Greeting = Greeting Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasConcurrency, HasKind, HasRateLimit, ToJSON)

type Reg = '[Queue "greetings" Greeting]

schema :: Text
schema = "arbiter_otel_test"

queue :: Text
queue = "greetings"

-- | A known-good W3C header.
sampleTraceparent :: ByteString
sampleTraceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"

sampleTraceId :: Text
sampleTraceId = "4bf92f3577b34da6a3ce929d0e0e4736"

-- | Unwrap, failing the example when there is nothing to unwrap.
orFail :: String -> Maybe a -> IO a
orFail msg = maybe (expectationFailure msg >> error "unreachable") pure

-- | Run an action with a frozen span attached to this thread, as an enclosing @inSpan@
-- would leave it.
withAttachedSpan :: ByteString -> IO a -> IO a
withAttachedSpan traceparent action = do
  sampleContext <- orFail "sample traceparent did not decode" (decodeSpanContext (Just traceparent) Nothing)
  bracket (attachContext (insertSpan (wrapSpanContext sampleContext) empty)) detachContext (const action)

-- | Drop and re-migrate the test schema.
freshSchema :: ByteString -> IO ()
freshSchema connStr = do
  bracket (connectPostgreSQL connStr) close $ \conn -> do
    execute_ conn "SET client_min_messages = warning"
    execute_ conn ("DROP SCHEMA IF EXISTS " <> quoteIdentifier schema <> " CASCADE")
  migrated <- runMigrationsForRegistry (Proxy @Reg) connStr schema defaultMigrationConfig
  migrated `shouldBe` MigrationSuccess

-- | A tracer provider whose processor keeps every span it ends.
recordingTracerProvider :: IO (IORef [ImmutableSpan], TracerProvider)
recordingTracerProvider = do
  ref <- newIORef []
  let processor =
        SpanProcessor
          { spanProcessorOnStart = \_ _ -> pure ()
          , spanProcessorOnEnd = \endedSpan -> modifyIORef' ref (endedSpan :)
          , spanProcessorShutdown = pure ShutdownSuccess
          , spanProcessorForceFlush = pure FlushSuccess
          }
  (,) ref <$> createTracerProvider [processor] emptyTracerProviderOptions

-- | Paths are package-relative. Cabal runs a test suite from the package directory.
dashboardPath :: FilePath
dashboardPath = "deploy/observability/grafana/dashboards/arbiter.json"

alertingPath :: FilePath
alertingPath = "deploy/observability/grafana/provisioning/alerting/arbiter.yaml"

-- | The provisioned files live in the repo. An sdist run has none.
withProvisioned :: FilePath -> ([Text] -> Spec) -> Spec
withProvisioned path checks = do
  present <- runIO (doesFileExist path)
  if present
    then runIO (referencedMetrics <$> TIO.readFile path) >>= checks
    else it "ships outside the package" $ pendingWith (path <> " is not in this tree")

-- | Every @arbiter_*@ family a provisioned file names, with Prometheus-flattened dots.
-- Reads the whole file, including panel descriptions.
referencedMetrics :: Text -> [Text]
referencedMetrics = map (T.takeWhile nameChar . snd) . T.breakOnAll "arbiter_"
  where
    nameChar char = isAsciiLower char || char == '_'

-- | Every instrument the library registers, as Prometheus renders it.
declaredMetrics :: [Text]
declaredMetrics = map (T.replace "." "_") Otel.arbiterMetricNames

-- | A provisioned file names at least one metric. Every named metric is one the library
-- registers.
queriesOnlyDeclared :: [Text] -> Expectation
queriesOnlyDeclared referenced = do
  referenced `shouldSatisfy` (not . null)
  filter (\metric -> not (any (`T.isPrefixOf` metric) declaredMetrics)) referenced `shouldBe` []

-- | Every point a collection produced, as its metric name and attributes.
collected :: SdkMeterEnv -> IO [(Text, Attributes)]
collected env = concatMap resourcePoints <$> collectResourceMetrics env
  where
    resourcePoints = foldMap scopePoints . resourceMetricsScopes
    scopePoints = foldMap metricPoints . scopeMetricsExports
    metricPoints = \case
      MetricExportSum {mesName, mesSumPoints} -> withName mesName sumDataPointAttributes mesSumPoints
      MetricExportGauge {megName, megGaugePoints} -> withName megName gaugeDataPointAttributes megGaugePoints
      MetricExportHistogram {mehName, mehPoints} -> withName mehName histogramDataPointAttributes mehPoints
      MetricExportExponentialHistogram {meehName, meehPoints} ->
        withName meehName exponentialHistogramDataPointAttributes meehPoints
    withName name pointAttrs = foldMap (\point -> [(name, pointAttrs point)])

-- | The monotonic sums a collection produced, as metric name and point value.
collectedSums :: SdkMeterEnv -> IO [(Text, Double)]
collectedSums env = concatMap resourceSums <$> collectResourceMetrics env
  where
    resourceSums = foldMap scopeSums . resourceMetricsScopes
    scopeSums = foldMap metricSums . scopeMetricsExports
    metricSums = \case
      MetricExportSum {mesName, mesMonotonic = True, mesSumPoints} ->
        foldMap (\point -> [(mesName, asDouble (sumDataPointValue point))]) mesSumPoints
      _ -> []
    asDouble = \case
      DoubleNumber double -> double
      IntNumber int -> fromIntegral int

-- | Whether a metric was recorded carrying every one of @kvs@ on one point.
recordedWith :: Text -> [(Text, Text)] -> [(Text, Attributes)] -> Bool
recordedWith name kvs = any (\(recordedName, attributes) -> recordedName == name && all (carries attributes) kvs)
  where
    carries attributes (key, value) = lookupAttributeByKey attributes (fromString (T.unpack key) :: AttributeKey Text) == Just value

main :: IO ()
main = hspec spec

spec :: Spec
spec = do
  connStr <- runIO getTestConnectionString
  runIO (freshSchema connStr)
  plainEnv <- runIO (createSimpleEnv (Proxy @Reg) connStr schema)

  describe "trace context" $ do
    it "stamps the ambient span onto an enqueued job" $ do
      stored <- withAttachedSpan sampleTraceparent $ enqueue plainEnv (Greeting "traced")
      fmap (T.isInfixOf sampleTraceId . traceparent) (traceContext stored) `shouldBe` Just True

    it "leaves the columns null when no span is active" $ do
      stored <- enqueue plainEnv (Greeting "no-span")
      traceContext stored `shouldBe` Nothing

    it "does not overwrite a trace context the caller set" $ do
      let job = setTraceContext (Just (TraceContext "caller-set" Nothing)) $ defaultJob (Greeting "preset")
      stored <- withAttachedSpan sampleTraceparent $ insertRaw plainEnv job
      fmap traceparent (traceContext stored) `shouldBe` Just "caller-set"

    it "survives the queue-to-archive copy" $ do
      let job = setArchiveFor (Just 3600) $ defaultJob (Greeting "archived")
      stored <- withAttachedSpan sampleTraceparent $ insertRaw plainEnv job
      void $ runSimpleDb plainEnv (Ops.ackJob schema queue stored)
      archived <- runSimpleDb plainEnv (Ops.getArchivedJobById @_ @Greeting schema queue (primaryKey stored))
      fmap (traceContext . jobSnapshot) archived `shouldBe` Just (traceContext stored)

    it "survives the DLQ round trip" $ do
      stored <- withAttachedSpan sampleTraceparent $ insertRaw plainEnv (defaultJob (Greeting "dlq'd"))
      entry <- runSimpleDb plainEnv $ do
        void $ Ops.moveToDLQ Ops.TakeLocks schema queue "boom" stored
        listToMaybe <$> Ops.listDLQJobs @_ @Greeting schema queue 1 0
      retried <- traverse (runSimpleDb plainEnv . Ops.retryFromDLQ @_ @Greeting schema queue . dlqPrimaryKey) entry
      fmap (fmap traceContext) retried `shouldBe` Just (Just (traceContext stored))

  describe "telemetry" $
    it "records job metrics and queue-depth gauges" $ do
      (meterProvider, env) <- createMeterProvider (materializeResources (mkResource [])) defaultSdkMeterProviderOptions
      Otel.withExternalTelemetry (Just meterProvider) Nothing $ \tel -> do
        job <- enqueue plainEnv (Greeting "measured")
        now <- getCurrentTime
        let noopHandler :: JobHandler (SimpleDb Reg IO) Greeting ()
            noopHandler _conn _job = pure ()
        -- Through the pool's own instrumentation.
        instrumented <- Otel.instrumentConfig tel <$> transactionalWorkerConfig 1 noopHandler
        let hooks = observabilityHooks instrumented
        runSimpleDb plainEnv $ do
          onJobClaimed hooks job now
          onJobSuccess hooks job now now
          onJobFailedAndMovedToDLQ hooks "boom" job
          onJobCancelled hooks job "cancelled"
          onJobUnavailable hooks job "no longer available"
        runSimpleDb plainEnv $ onMaintenance instrumented SweepExhaustedJobs 3

        loop <- Otel.startGauges tel defaultLogConfig (runSimpleDb plainEnv) schema [(queue, kindsFor @Greeting)] 1
        withAsync loop $ \_ -> do
          waitUntil 30_000 $ recordedWith "arbiter.queue.depth" [("queue", queue)] <$> collected env
          points <- collected env
          traverse_
            (\(name, kvs) -> points `shouldSatisfy` recordedWith name kvs)
            [ ("arbiter.jobs.processed", [("outcome", "success"), ("kind", "Greeting")])
            , ("arbiter.jobs.processed", [("outcome", "dlq")])
            , ("arbiter.jobs.processed", [("outcome", "cancelled")])
            , ("arbiter.jobs.processed", [("outcome", "unavailable")])
            , ("arbiter.maintenance.rows", [("op", "sweep-exhausted-jobs")])
            , ("arbiter.queue.depth", [("queue", queue)])
            , ("arbiter.queue.depth_by_kind", [("queue", queue), ("kind", "Greeting")])
            , ("arbiter.pg.database.backends", [])
            , ("arbiter.pg.table.blocks", [("table", queue), ("source", "hit")])
            , ("arbiter.pg.table.xid_age", [("table", queue)])
            ]

  -- The one series that tells a stopped refresh loop from a fresh reading.
  describe "reading staleness" $ do
    let scanned scanAt = Cache.Live (Cache.Cached scanAt (Cache.Snapshot [] Nothing [] [] []))

    it "keeps the scan time a retired reading was taken at" $
      Cache.lastScan (Cache.retire (scanned 100)) `shouldBe` Just 100

    it "keeps it across a second retire" $
      Cache.lastScan (Cache.retire (Cache.retire (scanned 100))) `shouldBe` Just 100

    it "stops exporting the reading it retired" $
      fmap Cache.takenAt (Cache.live (Cache.retire (scanned 100))) `shouldBe` Nothing

    it "has no scan time before the first reading" $
      Cache.lastScan (Cache.Idle Nothing) `shouldBe` Nothing

  describe "counter baselines" $ do
    let rise = Cache.riseSince ("arbiter.jobs.processed", [("outcome", "success")])
        counted scanAt total = fst (rise scanAt total mempty)

    it "counts nothing from the first scan" $
      snd (rise 10 7 mempty) `shouldBe` 0

    it "counts the rise between consecutive scans" $
      snd (rise 20 11 (counted 10 7)) `shouldBe` 4

    it "counts the whole total when the counter reset" $
      snd (rise 20 3 (counted 10 7)) `shouldBe` 3

    it "counts nothing from a scan already counted" $
      snd (rise 10 9 (counted 10 7)) `shouldBe` 0

  describe "counter export" $ do
    let name = "arbiter.test.total"
        key = (name, [])
        scans totals = do
          (meterProvider, env) <- createMeterProvider (materializeResources (mkResource [])) defaultSdkMeterProviderOptions
          meter <- getMeter meterProvider "arbiter-otel-test"
          counter <- meterCreateCounterDouble meter name Nothing Nothing defaultAdvisoryParameters
          let count seen (scanAt, total) = do
                let (seen', rise) = Cache.riseSince key scanAt total seen
                counterAdd counter rise emptyAttributes
                pure seen'
          foldM_ count mempty totals
          lookup name <$> collectedSums env

    it "sums the rises across three scans" $
      scans [(10, 100), (20, 130), (30, 190)] `shouldReturn` Just 90

    it "counts nothing from a single scan" $
      scans [(10, 100)] `shouldReturn` Just 0

    it "counts the whole total across a reset" $
      scans [(10, 100), (20, 130), (30, 5)] `shouldReturn` Just 35

  -- Nothing else reads the dashboard.
  describe "provisioned dashboard" $ withProvisioned dashboardPath $ \referenced -> do
    it "queries only metrics the library registers" $ queriesOnlyDeclared referenced

    it "has a panel for every metric the library registers" $
      filter (\declared -> not (any (declared `T.isPrefixOf`) referenced)) declaredMetrics `shouldBe` []

  describe "provisioned alerts" $ withProvisioned alertingPath $ \referenced ->
    it "query only metrics the library registers" $ queriesOnlyDeclared referenced

  describe "consumer spans" $ do
    (recorded, provider) <- runIO recordingTracerProvider

    it "links the job's producer, records its failure, and reattaches across a fork" $ do
      setGlobalTracerProvider provider
      job <- withAttachedSpan sampleTraceparent $ enqueue plainEnv (Greeting "spanned")
      tracer <- resolveTracer
      inherited <- runSimpleDb plainEnv . withConsumeSpan tracer (consumeSpanFor queue PerJob) (job :| []) $ do
        reattach <- capturingContext
        recordJobFailure job "boom"
        markSpanError "boom"
        reattach (liftIO (fmap traceparent <$> currentTraceContext))

      spans <- readIORef recorded
      consumerSpan <- orFail "expected exactly one consumer span" $ case spans of
        [one] -> Just one
        _ -> Nothing
      hot <- readIORef (spanHot consumerSpan)
      hotName hot `shouldBe` "process " <> queue
      spanKind consumerSpan `shouldBe` Consumer
      hotStatus hot `shouldBe` Error "boom"
      map eventName (values (hotEvents hot)) `shouldBe` ["job.failed"]
      -- Linked to the enqueue's trace, and running under a trace of its own.
      map (traceId . frozenLinkContext) (values (hotLinks hot))
        `shouldBe` map traceId (toList (spanContextOf . traceparent =<< traceContext job))
      (traceId <$> (spanContextOf =<< inherited)) `shouldBe` Just (traceId (spanContext consumerSpan))
      kindAttrOf hot `shouldBe` Just "Greeting"

    it "labels a publish span with the variant the payload derives" $ do
      (published, publishProvider) <- recordingTracerProvider
      setGlobalTracerProvider publishProvider
      runSimpleDb plainEnv $ withPublishSpan queue [defaultJob (Greeting "published")] (pure ())
      hot <- traverse (readIORef . spanHot) =<< readIORef published
      map kindAttrOf hot `shouldBe` [Just "Greeting"]

  describe "lifecycle hooks" $
    it "fires onJobClaimed under the job's consumer span" $ do
      (recorded, provider) <- recordingTracerProvider
      setGlobalTracerProvider provider
      seen <- newIORef []
      let handler :: JobHandler (SimpleDb Reg IO) Greeting ()
          handler _conn _job = pure ()
          hooks =
            defaultObservabilityHooks
              { onJobClaimed = \claimedJob _ -> liftIO $ do
                  ctx <- currentTraceContext
                  modifyIORef' seen ((payload claimedJob, traceparent <$> ctx) :)
              }
      config <- transactionalWorkerConfig 1 handler
      void $ enqueue plainEnv (Greeting "claim-hook")
      withAsync (runSimpleDb plainEnv (runWorkerPool config {observabilityHooks = hooks, pollInterval = 0.05})) $ \_ -> do
        waitUntil 20_000 $ any ((== Greeting "claim-hook") . fst) <$> readIORef seen
        claimed <- lookup (Greeting "claim-hook") <$> readIORef seen
        hookTrace <- orFail "claim hook ran under no span" (join claimed)
        hookTraceId <- orFail "claim hook's traceparent did not decode" (traceId <$> spanContextOf hookTrace)
        waitUntil 20_000 $ elem hookTraceId . map (traceId . spanContext) <$> readIORef recorded
  where
    values = toList . appendOnlyBoundedCollectionValues
    kindAttrOf hot = lookupAttributeByKey (hotAttributes hot) ("arbiter.kind" :: AttributeKey Text)
    spanContextOf header = decodeSpanContext (Just (encodeUtf8 header)) Nothing
    enqueue env greeting = insertRaw env (defaultJob greeting)
    insertRaw env job = do
      inserted <- runSimpleDb env (Ops.insertJob schema queue job)
      orFail "insert returned no row" inserted

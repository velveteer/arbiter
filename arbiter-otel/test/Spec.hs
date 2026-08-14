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
  ( Job (..)
  , ObservabilityHooks (..)
  , TraceContext (..)
  , defaultJob
  , defaultObservabilityHooks
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
import Control.Monad (join, void)
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
import OpenTelemetry.Attributes (Attributes, lookupAttributeByKey)
import OpenTelemetry.Attributes.Key (AttributeKey)
import OpenTelemetry.Context (empty, insertSpan)
import OpenTelemetry.Context.ThreadLocal (attachContext, detachContext)
import OpenTelemetry.Exporter.Metric
  ( MetricExport (..)
  , ResourceMetricsExport (..)
  , ScopeMetricsExport (..)
  , exponentialHistogramDataPointAttributes
  , gaugeDataPointAttributes
  , histogramDataPointAttributes
  , sumDataPointAttributes
  )
import OpenTelemetry.MeterProvider (collectResourceMetrics)
import OpenTelemetry.Metric (SdkMeterEnv, createMeterProvider, defaultSdkMeterProviderOptions)
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
import Arbiter.Otel.Gauges.Cells qualified as Cells

newtype Greeting = Greeting Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasConcurrency, HasRateLimit, ToJSON)

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
  sc <- orFail "sample traceparent did not decode" (decodeSpanContext (Just traceparent) Nothing)
  bracket (attachContext (insertSpan (wrapSpanContext sc) empty)) detachContext (const action)

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
          , spanProcessorOnEnd = \sp -> modifyIORef' ref (sp :)
          , spanProcessorShutdown = pure ShutdownSuccess
          , spanProcessorForceFlush = pure FlushSuccess
          }
  (,) ref <$> createTracerProvider [processor] emptyTracerProviderOptions

-- | Paths are package-relative, which is where cabal runs a test suite from.
dashboardPath :: FilePath
dashboardPath = "deploy/observability/grafana/dashboards/arbiter.json"

alertingPath :: FilePath
alertingPath = "deploy/observability/grafana/provisioning/alerting/arbiter.yaml"

-- | The provisioned files live in the repo, not the package, so an sdist run has none.
withProvisioned :: FilePath -> ([Text] -> Spec) -> Spec
withProvisioned path checks = do
  present <- runIO (doesFileExist path)
  if present
    then runIO (referencedMetrics <$> TIO.readFile path) >>= checks
    else it "ships outside the package" $ pendingWith (path <> " is not in this tree")

-- | Every @arbiter_*@ family a provisioned file names, Prometheus having flattened the
-- dots. Read from the whole file rather than the queries alone: a metric named in a
-- panel description should be just as real as one in a query.
referencedMetrics :: Text -> [Text]
referencedMetrics = map (T.takeWhile nameChar . snd) . T.breakOnAll "arbiter_"
  where
    nameChar c = isAsciiLower c || c == '_'

-- | Every instrument the library registers, as Prometheus renders it.
declaredMetrics :: [Text]
declaredMetrics = map (T.replace "." "_") Otel.arbiterMetricNames

-- | No metric a provisioned file names is one the library never registers.
queriesOnlyDeclared :: [Text] -> Expectation
queriesOnlyDeclared referenced =
  filter (\r -> not (any (`T.isPrefixOf` r) declaredMetrics)) referenced `shouldBe` []

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
    withName name pointAttrs = foldMap (\p -> [(name, pointAttrs p)])

-- | Whether a metric was recorded carrying every one of @kvs@ on one point.
recordedWith :: Text -> [(Text, Text)] -> [(Text, Attributes)] -> Bool
recordedWith name kvs = any (\(n, as) -> n == name && all (carries as) kvs)
  where
    carries as (k, v) = lookupAttributeByKey as (fromString (T.unpack k) :: AttributeKey Text) == Just v

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
      let job = (defaultJob (Greeting "preset")) {traceContext = Just (TraceContext "caller-set" Nothing)}
      stored <- withAttachedSpan sampleTraceparent $ insertRaw plainEnv job
      fmap traceparent (traceContext stored) `shouldBe` Just "caller-set"

    it "survives the queue-to-archive copy" $ do
      let job = (defaultJob (Greeting "archived")) {archiveFor = Just 3600}
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
      (mp, env) <- createMeterProvider (materializeResources (mkResource [])) defaultSdkMeterProviderOptions
      Otel.withExternalTelemetry (Just mp) Nothing $ \tel -> do
        job <- enqueue plainEnv (Greeting "measured")
        now <- getCurrentTime
        ms <- orFail "expected metrics on" (Otel.meters tel)
        let hooks = Otel.otelHooks ms queue
        onJobClaimed hooks job now
        onJobSuccess hooks job now now
        onJobFailedAndMovedToDLQ hooks "boom" job
        onJobCancelled hooks job "cancelled"
        onJobUnavailable hooks job "no longer available"
        Otel.otelMaintenance ms SweepExhaustedJobs 3

        loop <- Otel.startGauges tel defaultLogConfig (runSimpleDb plainEnv) schema [queue] 1
        withAsync loop $ \_ -> do
          waitUntil 30_000 $ recordedWith "arbiter.queue.depth" [("queue", queue)] <$> collected env
          points <- collected env
          traverse_
            (\(name, kvs) -> points `shouldSatisfy` recordedWith name kvs)
            [ ("arbiter.jobs.processed", [("outcome", "success")])
            , ("arbiter.jobs.processed", [("outcome", "dlq")])
            , ("arbiter.jobs.processed", [("outcome", "cancelled")])
            , ("arbiter.jobs.processed", [("outcome", "unavailable")])
            , ("arbiter.maintenance.rows", [("op", "sweep-exhausted-jobs")])
            , ("arbiter.queue.depth", [("queue", queue)])
            , ("arbiter.pg.backends", [])
            ]

  -- The one series that tells a stopped refresh loop from a fresh reading.
  describe "reading staleness" $ do
    let scanned at = Cells.Live (Cells.Cached at (Cells.Snapshot [] Nothing [] [] []))

    it "keeps the scan time a retired reading was taken at" $
      Cells.lastScan (Cells.retire (scanned 100)) `shouldBe` Just 100

    it "keeps it across a second retire" $
      Cells.lastScan (Cells.retire (Cells.retire (scanned 100))) `shouldBe` Just 100

    it "stops exporting the reading it retired" $
      fmap Cells.takenAt (Cells.live (Cells.retire (scanned 100))) `shouldBe` Nothing

    it "has no scan time before the first reading" $
      Cells.lastScan (Cells.Idle Nothing) `shouldBe` Nothing

  describe "counter baselines" $ do
    let rise = Cells.riseSince ("arbiter.jobs.processed", [("outcome", "success")])
        counted at total = fst (rise at total mempty)

    it "counts nothing from the first scan" $
      snd (rise 10 7 mempty) `shouldBe` 0

    it "counts the rise between consecutive scans" $
      snd (rise 20 11 (counted 10 7)) `shouldBe` 4

    it "counts the whole total when the counter reset" $
      snd (rise 20 3 (counted 10 7)) `shouldBe` 3

    it "counts nothing from a scan already counted" $
      snd (rise 10 9 (counted 10 7)) `shouldBe` 0

  -- Nothing else reads the dashboard, so a renamed metric would blank a panel silently.
  describe "provisioned dashboard" $ withProvisioned dashboardPath $ \referenced -> do
    it "queries only metrics the library registers" $ queriesOnlyDeclared referenced

    it "has a panel for every metric the library registers" $
      filter (\d -> not (any (d `T.isPrefixOf`) referenced)) declaredMetrics `shouldBe` []

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
      sp <- orFail "expected exactly one consumer span" $ case spans of
        [one] -> Just one
        _ -> Nothing
      hot <- readIORef (spanHot sp)
      hotName hot `shouldBe` "process " <> queue
      spanKind sp `shouldBe` Consumer
      hotStatus hot `shouldBe` Error "boom"
      map eventName (values (hotEvents hot)) `shouldBe` ["job.failed"]
      -- Linked to the enqueue's trace, and running under a trace of its own.
      map (traceId . frozenLinkContext) (values (hotLinks hot))
        `shouldBe` map traceId (toList (spanContextOf . traceparent =<< traceContext job))
      (traceId <$> (spanContextOf =<< inherited)) `shouldBe` Just (traceId (spanContext sp))

  describe "lifecycle hooks" $
    it "fires onJobClaimed under the job's consumer span" $ do
      (recorded, provider) <- recordingTracerProvider
      setGlobalTracerProvider provider
      seen <- newIORef []
      let handler :: JobHandler (SimpleDb Reg IO) Greeting ()
          handler _conn _job = pure ()
          hooks =
            defaultObservabilityHooks
              { onJobClaimed = \j _ -> liftIO $ do
                  ctx <- currentTraceContext
                  modifyIORef' seen ((payload j, traceparent <$> ctx) :)
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
    spanContextOf tp = decodeSpanContext (Just (encodeUtf8 tp)) Nothing
    enqueue env p = insertRaw env (defaultJob p)
    insertRaw env job = do
      inserted <- runSimpleDb env (Ops.insertJob schema queue job)
      orFail "insert returned no row" inserted

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Main (main) where

import Arbiter.Concurrency (HasConcurrency (..), concurrencyBy, concurrencyPool)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
  ( HasKind
  , defaultJob
  , payload
  , primaryKey
  , setArchiveFor
  , setGroupKey
  , setMaxAttempts
  , setNotVisibleUntil
  , setPriority
  )
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Migrations (MigrationConfig (..), MigrationResult (..), defaultMigrationConfig, runMigrationsForRegistry)
import Arbiter.Otel qualified as Otel
import Arbiter.RateLimit (HasRateLimit (..), globalLimit, limitBy, limitByCase, tokenBucket)
import Arbiter.Servant (initArbiterServer)
import Arbiter.Servant.API (ArbiterAPI)
import Arbiter.Servant.OpenApi (openApiSpec)
import Arbiter.Servant.Server (ArbiterServerConfig, arbiterServer)
import Arbiter.Servant.UI (AdminUI, adminUIServer, adminUIServerDev)
import Arbiter.Simple
import Arbiter.Worker
  ( BatchCallbacks (..)
  , WorkerConfig (..)
  , defaultBatchedWorkerConfig
  , defaultLogConfig
  , mergedChildResults
  , namedWorkerPool
  , poolConfigForWorkers
  , shutdownPools
  , transactionalWorkerConfig
  )
import Arbiter.Worker.Cron (OverlapPolicy (..), cronJob)
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.Async (race_)
import Control.Monad (forM_, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString.Char8 qualified as BS
import Data.Foldable (traverse_)
import Data.HashMap.Strict qualified as HM
import Data.List (unfoldr)
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Maybe (fromMaybe)
import Data.OpenApi (ToSchema)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (addUTCTime, diffTimeToPicoseconds, getCurrentTime, utctDayTime)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Network.Wai.Handler.Warp
  ( defaultSettings
  , runSettings
  , setPort
  )
import Network.Wai.Middleware.Cors
  ( CorsResourcePolicy (..)
  , cors
  , simpleCorsResourcePolicy
  )
import Network.Wai.Middleware.RequestLogger (logStdout)
import OpenTelemetry.Attributes qualified as Attr
import OpenTelemetry.Instrumentation.Wai (newOpenTelemetryWaiMiddleware)
import OpenTelemetry.Trace.Core qualified as Trace
import Servant (Application, serve, (:<|>) (..))
import Servant.Swagger.UI (SwaggerSchemaUI, swaggerSchemaUIServer)
import System.Environment (lookupEnv)
import System.Exit (die)
import System.Posix.Signals qualified as Signals
import System.Random (randomRIO)

-- ---------------------------------------------------------------------------
-- Payload types
-- ---------------------------------------------------------------------------

data DemoPayload
  = TestMessage Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasKind, ToJSON, ToSchema)

data EmailPayload
  = SendEmail Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasKind, ToJSON, ToSchema)

data NotificationPayload
  = PushNotification Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasKind, ToJSON, ToSchema)

-- | Cheap throwaway work the burst generator floods its queue with.
newtype BulkPayload = BulkTask Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasKind, ToJSON, ToSchema)

-- | Pipeline payload for the rollup demo.
--
-- Leaf jobs carry a chunk name and produce @[Text]@ results.
-- Finalizer jobs merge child results and propagate them upward.
data PipelinePayload
  = ProcessChunk Text
  | AggregateResults Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasKind, ToJSON, ToSchema)

-- | The demo's routes: the queue API, an OpenAPI description of it with a Swagger UI
-- to read it in, and the dashboard. The dashboard is a catch-all and comes last.
type DemoAPI =
  ArbiterAPI DemoRegistry
    :<|> SwaggerSchemaUI "docs" "openapi.json"
    :<|> AdminUI

-- | The demo application. @mDevDir@ serves the dashboard from disk when set.
demoApp :: Maybe FilePath -> ArbiterServerConfig DemoRegistry -> Application
demoApp mDevDir config =
  serve (Proxy @DemoAPI) $
    arbiterServer config
      :<|> swaggerSchemaUIServer (openApiSpec @DemoRegistry)
      :<|> maybe adminUIServer adminUIServerDev mDevDir

-- | Demo registry with multiple queues
type DemoRegistry =
  '[ Queue "demo_queue" DemoPayload
   , Queue "email_queue" EmailPayload
   , Queue "notifications" NotificationPayload
   , Queue "bulk_queue" BulkPayload
   , QueueWithResult "pipeline" PipelinePayload [Text]
   ]

-- | Email recipient tiers, each rate-limited separately.
data EmailTier = Internal | External | System
  deriving stock (Bounded, Enum, Eq, Show)

-- | The recipient domain, or 'Nothing' for a non-address (e.g. a digest name).
emailDomain :: EmailPayload -> Maybe Text
emailDomain mail = case T.splitOn "@" (emailAddress mail) of
  [_, dom] | not (T.null dom) -> Just dom
  _ -> Nothing

-- | Classify an email by recipient domain: in-house, external, or system digest.
emailTier :: EmailPayload -> EmailTier
emailTier mail = case emailDomain mail of
  Just "acme.test" -> Internal
  Just _ -> External
  Nothing -> System

-- | Rate-limit selection for the demo. Emails are limited per recipient tier via an
-- N-way 'limitByCase', each tier its own bucket. Notifications share one global
-- bucket. Other queues stay unlimited. The migration seeds every branch's policy.
instance HasRateLimit EmailPayload where
  rateLimitFor = limitByCase emailTier $ \case
    Internal -> limitBy (tokenBucket "email-internal" 5 30) emailKeySuffix
    External -> limitBy (tokenBucket "email-external" 2 30) emailKeySuffix
    System -> globalLimit (tokenBucket "email-system" 1 30) "digests"

instance HasRateLimit NotificationPayload where
  rateLimitFor = globalLimit (tokenBucket "notify" 2 20) "all"

-- | At most 2 emails per recipient domain in flight at once, from the seeded
-- "email-domain" pool. Orthogonal to the rate limit on the same payload.
instance HasConcurrency EmailPayload where
  concurrencyFor = concurrencyBy (concurrencyPool "email-domain" 2) emailKeySuffix

-- | The recipient address of an email job.
emailAddress :: EmailPayload -> Text
emailAddress (SendEmail addr) = addr

-- | Key an email bucket by recipient domain, or @"system"@ for non-addresses.
emailKeySuffix :: EmailPayload -> Text
emailKeySuffix = fromMaybe "system" . emailDomain

main :: IO ()
main = Otel.withTelemetryFromEnv runDemo

-- | The demo proper.
runDemo :: Otel.Telemetry -> IO ()
runDemo tel = do
  -- Get config from environment or use defaults
  connStr <-
    maybe "host=localhost port=5432 user=postgres password=master dbname=postgres" BS.pack
      <$> lookupEnv "DATABASE_URL"
  schemaStr <- fromMaybe "arbiter_demo" <$> lookupEnv "SCHEMA"
  portStr <- fromMaybe "8080" <$> lookupEnv "PORT"
  let schema = T.pack schemaStr
      port = read portStr :: Int
  -- Drop and recreate schema for a clean demo
  putStrLn "Resetting schema..."
  conn <- PG.connectPostgreSQL connStr
  void $ PG.execute_ conn $ "DROP SCHEMA IF EXISTS " <> fromString schemaStr <> " CASCADE"
  PG.close conn

  putStrLn "Running migrations..."
  migrationResult <-
    runMigrationsForRegistry
      (Proxy @DemoRegistry)
      connStr
      schema
      defaultMigrationConfig {enableEventStreaming = True}
  case migrationResult of
    MigrationSuccess -> putStrLn "Migrations complete"
    MigrationError err -> die $ "Migration failed: " <> err

  -- Producer env for seeding and the background load generator. The worker
  -- pools get their own pool from the managed runner below.
  producerEnv <- createSimpleEnv (Proxy @DemoRegistry) connStr schema

  -- Seed demo data
  putStrLn "Seeding demo data..."
  seedDemoData producerEnv schema

  -- Create server config (own connection pool for admin API)
  putStrLn ""
  putStrLn "Setting up server..."
  serverConfig <- initArbiterServer (Proxy @DemoRegistry) connStr schema
  putStrLn "Server ready"

  -- Create worker configs with cron jobs
  putStrLn "Creating worker configs..."
  demoWorkerCfg <- mkDemoWorker
  emailWorkerCfg <- mkEmailWorker
  notifWorkerCfg <- mkNotifWorker
  bulkWorkerCfg <- mkBulkWorker
  pipelineWorkerCfg <- mkPipelineWorker
  putStrLn "Workers configured"

  let policy =
        simpleCorsResourcePolicy
          { corsRequestHeaders = ["Content-Type", "Accept"]
          , corsMethods = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
          }

  -- Dev mode: serve static files from disk when ADMIN_DEV_DIR is set
  mDevDir <- lookupEnv "ADMIN_DEV_DIR"
  traceHttp <- newOpenTelemetryWaiMiddleware
  let app = traceHttp $ cors (const $ Just policy) $ logStdout $ demoApp mDevDir serverConfig

  -- Start server
  putStrLn ""
  putStrLn "=== Server Starting ==="
  putStrLn $ "API:     http://localhost:" <> show port <> "/api/v1"
  putStrLn $ "Admin:   http://localhost:" <> show port <> "/"
  putStrLn $ "Docs:    http://localhost:" <> show port <> "/docs"
  putStrLn "Workers and cron schedules running across all queues"
  case mDevDir of
    Just dir -> putStrLn $ "Dev: serving static files from " <> dir
    Nothing -> putStrLn "Set ADMIN_DEV_DIR to serve static files from disk"
  putStrLn ""
  putStrLn "Press Ctrl+C to stop"
  putStrLn ""

  -- Build worker pool list
  let workers =
        [ namedWorkerPool demoWorkerCfg
        , namedWorkerPool emailWorkerCfg
        , namedWorkerPool notifWorkerCfg
        , namedWorkerPool bulkWorkerCfg
        , namedWorkerPool pipelineWorkerCfg
        ]
  let handler = Signals.Catch $ shutdownPools workers
  void $ Signals.installHandler Signals.sigTERM handler Nothing
  void $ Signals.installHandler Signals.sigINT handler Nothing

  -- Self-restart watchdog. After RESET_INTERVAL_MINUTES, raise SIGTERM. The
  -- handler above exits the process and the container's restart policy reseeds
  -- a clean demo. A value of 0 disables it.
  resetMin <- maybe 20 read <$> lookupEnv "RESET_INTERVAL_MINUTES"
  when (resetMin > 0) $ void $ forkIO $ do
    threadDelay (resetMin * 60 * 1_000_000)
    putStrLn $ "[reset] " <> show resetMin <> "m elapsed, restarting for a clean demo"
    Signals.raiseSignal Signals.sigTERM

  -- Background load generator. Pulse a few jobs on an interval. A value of 0
  -- disables it.
  pulseSec <- maybe 5 read <$> lookupEnv "LOAD_PULSE_SECONDS"
  when (pulseSec > 0) $ void $ forkIO $ loadPulse producerEnv pulseSec

  -- Emit a small archivable pipeline on an interval. A value of 0 disables it.
  pipeSec <- maybe 2 read <$> lookupEnv "PIPELINE_PULSE_SECONDS"
  when (pipeSec > 0) $ void $ forkIO $ pipelinePulse producerEnv schema pipeSec

  -- Bursty batch inserts into bulk_queue. A spike of multi-row inserts on a
  -- jittered interval. Either value at 0 disables it.
  burstSec <- maybe 10 read <$> lookupEnv "BURST_INTERVAL_SECONDS"
  burstSize <- maybe 1000 read <$> lookupEnv "BURST_SIZE"
  burstGroups <- maybe 8 read <$> lookupEnv "BURST_GROUPS"
  when (burstSec > 0 && burstSize > 0) $ do
    putStrLn $ "Burst load: ~" <> show burstSize <> " jobs every ~" <> show burstSec <> "s into bulk_queue"
    void $ forkIO $ burstPulse producerEnv burstSec burstSize burstGroups

  poolCfg <- poolConfigForWorkers workers
  workerEnv <- createSimpleEnvWithConfig (Proxy @DemoRegistry) connStr schema poolCfg

  putStrLn $ "Telemetry: " <> T.unpack (Otel.telemetrySummary tel)
  race_
    (runSimpleDb workerEnv $ Otel.runWorkerPoolsWith tel defaultLogConfig workers)
    (runSettings (setPort port defaultSettings) app)

-- ---------------------------------------------------------------------------
-- Worker configs
-- ---------------------------------------------------------------------------

type DemoM = SimpleDb DemoRegistry IO

simulateWork :: Double -> IO ()
simulateWork mean = do
  uniform <- randomRIO (1.0e-6, 1.0)
  threadDelay (round (mean * negate (log uniform) * 1e6))

mkDemoWorker :: IO (WorkerConfig DemoM DemoPayload)
mkDemoWorker = do
  cfg <- transactionalWorkerConfig 5 handler
  pure
    cfg
      { cronJobs = demoCrons
      , pollInterval = 10
      , livenessFile = Nothing
      }
  where
    handler _conn _job = liftIO $ simulateWork 2
    demoCrons =
      [ either error id $
          cronJob
            "demo-ticker"
            "* * * * *" -- every minute
            AllowOverlap
            (\_ tickTime -> defaultJob (TestMessage $ "tick:" <> tshow tickTime))
      ]

mkEmailWorker :: IO (WorkerConfig DemoM EmailPayload)
mkEmailWorker = do
  cfg <- transactionalWorkerConfig 3 handler
  pure
    cfg
      { cronJobs = emailCrons
      , pollInterval = 10
      , livenessFile = Nothing
      }
  where
    handler _conn _job = liftIO $ simulateWork 2
    emailCrons =
      [ either error id $
          cronJob
            "email-digest"
            "*/2 * * * *" -- every 2 minutes
            SkipOverlap
            (\_ _ -> defaultJob (SendEmail "scheduled-digest"))
      ]

mkNotifWorker :: IO (WorkerConfig DemoM NotificationPayload)
mkNotifWorker = do
  cfg <- transactionalWorkerConfig 1 handler
  pure
    cfg
      { cronJobs = notifCrons
      , pollInterval = 2
      , livenessFile = Nothing
      }
  where
    handler _conn _job = liftIO $ simulateWork 0.05
    notifCrons =
      [ either error id $
          cronJob
            "notif-broadcast"
            "*/3 * * * *" -- every 3 minutes
            AllowOverlap
            (\_ tickTime -> defaultJob (PushNotification $ "broadcast:" <> tshow tickTime))
      ]

-- | Drains the burst queue. Many workers claim large batches with a near-zero
-- handler. Sized by BURST_WORKERS and BURST_BATCH.
mkBulkWorker :: IO (WorkerConfig DemoM BulkPayload)
mkBulkWorker = do
  count <- maybe 6 read <$> lookupEnv "BURST_WORKERS"
  batch <- maybe 25 read <$> lookupEnv "BURST_BATCH"
  cfg <- defaultBatchedWorkerConfig count batch handler
  pure cfg {pollInterval = 1, livenessFile = Nothing}
  where
    -- Roughly 300 jobs a second across the pool. A burst drains slowly enough
    -- to be sampled.
    handler jobs cbs = do
      liftIO $ simulateWork 0.5
      ackAll cbs (NE.toList jobs)

-- ---------------------------------------------------------------------------
-- Pipeline worker - rollup demo
-- ---------------------------------------------------------------------------

mkPipelineWorker :: IO (WorkerConfig DemoM PipelinePayload)
mkPipelineWorker = do
  cfg <- transactionalWorkerConfig 3 handler
  pure cfg {pollInterval = 2, livenessFile = Nothing}
  where
    handler _conn job = case payload job of
      ProcessChunk chunkName -> do
        liftIO $ simulateWork 1
        pure (chunkFindings chunkName)
      AggregateResults _ -> fst <$> mergedChildResults job

-- | Deterministic fake findings for each chunk name.
chunkFindings :: Text -> [Text]
chunkFindings name
  | "revenue" `T.isInfixOf` name = ["revenue:$1.2M", "growth:15%"]
  | "expense" `T.isInfixOf` name = ["costs:$800K", "savings:$50K"]
  | "forecast" `T.isInfixOf` name = ["forecast:$1.5M", "confidence:high"]
  | "inventory" `T.isInfixOf` name = ["stock:12000", "turnover:4.2x"]
  | "shipping" `T.isInfixOf` name = ["deliveries:3400", "on-time:98%"]
  | "support" `T.isInfixOf` name = ["tickets:890", "resolution:4.2h"]
  | "churn" `T.isInfixOf` name = ["churn:2.1%", "at-risk:340"]
  | otherwise = ["processed:" <> name]

-- ---------------------------------------------------------------------------
-- Seed data
-- ---------------------------------------------------------------------------

-- | Seed a varied initial dataset across every queue and every status the
-- admin UI can show: ready, scheduled, suspended, backoff, in-flight (as
-- workers claim jobs), and dead-letter.
seedDemoData :: SimpleEnv DemoRegistry -> Text -> IO ()
seedDemoData env schemaName = runSimpleDb env $ do
  seedPipeline schemaName
  seedQueues
  liftIO $ putStrLn "  Seeded jobs across demo_queue, email_queue, notifications, and pipeline"

-- | A 3-level rollup pipeline. Results flow up from leaves through finalizers.
-- Priority 10 keeps this long run behind the quick pipelines from 'pipelinePulse'.
seedPipeline :: Text -> DemoM ()
seedPipeline schemaName = do
  let background job = setPriority 10 $ defaultJob job
      chunk = JT.leaf . background . ProcessChunk
      agg name = JT.rollup (background (AggregateResults name))
  tracer <- demoTracer
  result <- Trace.inSpan' tracer "seed pipeline" Trace.defaultSpanArguments $ \seedSpan -> do
    tree <-
      JT.insertJobTree schemaName "pipeline" $
        agg
          "final-report"
          ( NE.fromList
              $ NE.take 300
              $ NE.cycle
                [ agg "financials" [chunk "revenue-data", chunk "expense-data", chunk "forecast-data"]
                , agg "operations" [chunk "inventory-data", chunk "shipping-data", chunk "support-data"]
                ]
          )
    traverse_
      ( \(root :| rest) ->
          Trace.addAttributes seedSpan $
            HM.fromList
              [ ("demo.pipeline.node_count", Attr.toAttribute (1 + length rest :: Int))
              , ("demo.pipeline.root_id", Attr.toAttribute (tshow (primaryKey root)))
              ]
      )
      tree
    pure tree
  case result of
    Left err -> liftIO $ die $ "pipeline seed failed: " <> show err
    Right (root :| rest) ->
      liftIO $ putStrLn $ "  pipeline: root #" <> show (primaryKey root) <> " with " <> show (length rest) <> " descendants"

-- | A spread across the flat queues covering ready, scheduled, suspended,
-- backoff, and dead-letter, with varied priorities and group keys.
seedQueues :: DemoM ()
seedQueues = do
  now <- liftIO getCurrentTime

  -- Backoff and DLQ examples are produced through the normal lifecycle.
  void $
    HL.insertJob
      (setGroupKey (Just "integrations") $ setMaxAttempts (Just 5) $ defaultJob (TestMessage "flaky-webhook"))
  [flaky1] <- HL.claimNextVisibleJobs @DemoPayload 1 60
  void $ HL.updateJobForRetry 0 "connection reset by upstream" flaky1
  [flaky2] <- HL.claimNextVisibleJobs @DemoPayload 1 60
  void $ HL.updateJobForRetry 180 "connection reset by upstream" flaky2

  void $
    HL.insertJob
      (setGroupKey (Just "ingest") $ setMaxAttempts (Just 5) $ defaultJob (TestMessage "corrupt-record"))
  forM_ ([1 .. 4] :: [Int]) $ \_ -> do
    [failed] <- HL.claimNextVisibleJobs @DemoPayload 1 60
    void $ HL.updateJobForRetry 0 "unparseable payload" failed
  [doomed] <- HL.claimNextVisibleJobs @DemoPayload 1 60
  void $ HL.moveToDLQ "unparseable payload after 5 attempts" doomed

  -- demo_queue: ready jobs with varied priority and group keys
  forM_ (zip [0 :: Int ..] demoTasks) $ \(index, (grp, msg)) ->
    void $ HL.insertJob (setGroupKey (Just grp) $ setPriority (fromIntegral (index `mod` 5)) $ defaultJob (TestMessage msg))
  -- scheduled (not visible yet) and suspended (paused)
  void $
    HL.insertJob
      ( setGroupKey (Just "maintenance")
          $ setNotVisibleUntil (Just (addUTCTime 3600 now))
          $ defaultJob (TestMessage "nightly-reindex")
      )
  paused <- need =<< HL.insertJob (setGroupKey (Just "maintenance") $ defaultJob (TestMessage "paused-migration"))
  void $ HL.suspendJob @DemoPayload (primaryKey paused)

  -- email_queue: a few ready, one scheduled, one bounced to the DLQ
  void $ HL.insertJob (setMaxAttempts (Just 2) $ defaultJob (SendEmail "nobody@invalid.test"))
  [rejected] <- HL.claimNextVisibleJobs @EmailPayload 1 60
  void $ HL.updateJobForRetry 0 "recipient rejected (550)" rejected
  forM_ (["welcome@acme.test", "receipt@acme.test", "reset@acme.test"] :: [Text]) $ \addr ->
    void $ HL.insertJob (defaultJob (SendEmail addr))
  void $ HL.insertJob (setNotVisibleUntil (Just (addUTCTime 1800 now)) $ defaultJob (SendEmail "weekly-digest"))

  -- notifications: a few ready
  forM_ (["deploy finished", "build green", "nightly backup ok"] :: [Text]) $ \msg ->
    void $ HL.insertJob (defaultJob (PushNotification msg))
  where
    need :: Maybe a -> DemoM a
    need = maybe (liftIO (die "demo seed: insert returned Nothing")) pure

-- | (group key, message) pairs for the demo_queue ready jobs.
demoTasks :: [(Text, Text)]
demoTasks =
  [ ("imports", "import customers.csv")
  , ("imports", "import orders.csv")
  , ("reports", "generate Q3 summary")
  , ("reports", "refresh dashboard")
  , ("billing", "reconcile invoices")
  , ("billing", "charge subscriptions")
  , ("maintenance", "vacuum analyze")
  , ("integrations", "sync CRM contacts")
  ]

-- | Background load generator. A small pulse of jobs every @sec@ seconds.
loadPulse :: SimpleEnv DemoRegistry -> Int -> IO ()
loadPulse env sec = go 0
  where
    -- Archive one job in @every@.
    keepEvery every tick = if tick `mod` every == 0 then Just 3600 else Nothing
    go :: Int -> IO ()
    go tick = do
      threadDelay (sec * 1_000_000)
      runSimpleDb env $ do
        void $
          HL.insertJob
            ( setArchiveFor (keepEvery 4 tick)
                $ setPriority (fromIntegral (tick `mod` 5))
                $ defaultJob (TestMessage ("pulse #" <> tshow tick))
            )
        when (even tick)
          $ void
          $ HL.insertJob (setArchiveFor (keepEvery 6 tick) $ defaultJob (SendEmail ("digest #" <> tshow tick)))
        when (tick `mod` 3 == 0)
          $ void
          $ HL.insertJob (setArchiveFor (keepEvery 9 tick) $ defaultJob (PushNotification ("alert #" <> tshow tick)))
      go (tick + 1)

-- | Burst load. Every @sec@ seconds (jittered by half), insert @size@ jobs as
-- back-to-back multi-row inserts, with a four-times spike every fifth round.
-- Half of each burst joins one of @groups@ group keys.
burstPulse :: SimpleEnv DemoRegistry -> Int -> Int -> Int -> IO ()
burstPulse env sec size groups = go 0
  where
    perStatement = 250
    grouped index
      | groups <= 0 || odd index = id
      | otherwise = setGroupKey (Just ("bulk-" <> tshow ((index `div` 2) `mod` groups)))
    go :: Int -> IO ()
    go tick = do
      gap <- randomRIO (0.5, 1.5 :: Double)
      threadDelay (round (fromIntegral sec * gap * 1e6))
      let total = if tick `mod` 5 == 4 then size * 4 else size
          jobs = [grouped index (defaultJob (BulkTask ("burst " <> tshow tick <> " #" <> tshow index))) | index <- [1 .. total]]
      runSimpleDb env $ traverse_ (void . HL.insertJobsBatch_) (chunksOf perStatement jobs)
      go (tick + 1)

-- | Split into chunks of at most @chunkSize@.
chunksOf :: Int -> [a] -> [[a]]
chunksOf chunkSize = unfoldr (\remaining -> if null remaining then Nothing else Just (splitAt chunkSize remaining))

-- | Chunk sets a quick pipeline picks from, for variety across runs.
quickChunkSets :: [[Text]]
quickChunkSets =
  [ ["revenue-data", "expense-data", "forecast-data"]
  , ["inventory-data", "shipping-data", "support-data"]
  , ["revenue-data", "churn-data"]
  , ["forecast-data", "support-data", "churn-data"]
  ]

-- | Insert a small rollup every @sec@ seconds. Its root archives its merged result.
pipelinePulse :: SimpleEnv DemoRegistry -> Text -> Int -> IO ()
pipelinePulse env schemaName sec = go 0
  where
    go :: Int -> IO ()
    go tick = do
      threadDelay (sec * 1_000_000)
      now <- getCurrentTime
      let seed = fromInteger (diffTimeToPicoseconds (utctDayTime now)) :: Int
          chosen = quickChunkSets !! (seed `mod` length quickChunkSets)
          root = setArchiveFor (Just 3600) $ defaultJob (AggregateResults ("quick-report-" <> tshow tick))
          leaves = NE.fromList (map (JT.leaf . defaultJob . ProcessChunk) chosen)
      runSimpleDb env $ void $ JT.insertJobTree schemaName "pipeline" (JT.rollup root leaves)
      go (tick + 1)

demoTracer :: (MonadIO m) => m Trace.Tracer
demoTracer = do
  provider <- Trace.getGlobalTracerProvider
  pure (Trace.makeTracer provider "arbiter-demo" Trace.tracerOptions)

tshow :: (Show a) => a -> Text
tshow = T.pack . show

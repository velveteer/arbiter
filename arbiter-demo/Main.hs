{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Arbiter.Concurrency qualified as Conc
import Arbiter.Core qualified as Arb
import Arbiter.Core.Enum (showText)
import Arbiter.Core.Threads (micros)
import Arbiter.Migrations qualified as Mig
import Arbiter.Otel qualified as Otel
import Arbiter.RateLimit qualified as RL
import Arbiter.Servant qualified as Servant
import Arbiter.Servant.OpenApi qualified as OpenApi
import Arbiter.Servant.UI qualified as ServantUI
import Arbiter.Simple qualified as ArbS
import Arbiter.Worker qualified as Worker
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.Async (race_)
import Control.Exception (bracket)
import Control.Monad (replicateM_, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString.Char8 qualified as BS
import Data.Foldable (traverse_)
import Data.HashMap.Strict qualified as HM
import Data.Int (Int32)
import Data.List (unfoldr)
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Maybe (fromMaybe)
import Data.OpenApi (ToSchema)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, addUTCTime, getCurrentTime)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Types qualified as PGT
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
import Text.Read (readMaybe)

-- ---------------------------------------------------------------------------
-- Payload types
-- ---------------------------------------------------------------------------

data DemoPayload
  = TestMessage Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (Arb.HasKind, FromJSON, ToJSON, ToSchema)

data EmailPayload
  = SendEmail Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (Arb.HasKind, FromJSON, ToJSON, ToSchema)

data NotificationPayload
  = PushNotification Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (Arb.HasKind, FromJSON, ToJSON, ToSchema)

-- | Cheap throwaway work the burst generator floods its queue with.
newtype BulkPayload = BulkTask Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (Arb.HasKind, FromJSON, ToJSON, ToSchema)

-- | Pipeline payload for the rollup demo.
--
-- Leaf jobs carry a chunk name and produce @[Text]@ results.
-- Finalizer jobs merge child results and propagate them upward.
data PipelinePayload
  = ProcessChunk Text
  | AggregateResults Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (Arb.HasKind, FromJSON, ToJSON, ToSchema)

-- | The demo's routes: the queue API, an OpenAPI description of it with a Swagger UI
-- to read it in, and the dashboard. The dashboard is a catch-all and comes last.
type DemoAPI =
  Servant.ArbiterAPI DemoRegistry
    :<|> SwaggerSchemaUI "docs" "openapi.json"
    :<|> ServantUI.AdminUI

-- | The demo application. @mDevDir@ serves the dashboard from disk when set.
demoApp :: Maybe FilePath -> Servant.ArbiterServerConfig (ArbS.SimpleDb DemoRegistry IO) DemoRegistry -> Application
demoApp mDevDir config =
  serve (Proxy @DemoAPI) $
    Servant.arbiterServer config
      :<|> swaggerSchemaUIServer (OpenApi.openApiSpec @DemoRegistry "demo")
      :<|> maybe ServantUI.adminUIServer ServantUI.adminUIServerDev mDevDir

-- | Demo registry with multiple queues
type DemoRegistry =
  '[ Arb.Queue "demo_queue" DemoPayload
   , Arb.Queue "email_queue" EmailPayload
   , Arb.Queue "notifications" NotificationPayload
   , Arb.Queue "bulk_queue" BulkPayload
   , Arb.QueueWithResult "pipeline" PipelinePayload [Text]
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
instance RL.HasRateLimit EmailPayload where
  rateLimitFor = RL.limitByCase emailTier $ \case
    Internal -> RL.limitBy (RL.tokenBucket "email-internal" 5 30) emailKeySuffix
    External -> RL.limitBy (RL.tokenBucket "email-external" 2 30) emailKeySuffix
    System -> RL.globalLimit (RL.tokenBucket "email-system" 1 30) "digests"

instance RL.HasRateLimit NotificationPayload where
  rateLimitFor = RL.globalLimit (RL.tokenBucket "notify" 2 20) "all"

-- | At most 2 emails per recipient domain in flight at once, from the seeded
-- "email-domain" policy. Orthogonal to the rate limit on the same payload.
instance Conc.HasConcurrency EmailPayload where
  concurrencyFor = Conc.concurrencyBy (Conc.concurrencyPolicy "email-domain" 2) emailKeySuffix

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
  connStr <-
    maybe "host=localhost port=5432 user=postgres password=master dbname=postgres" BS.pack
      <$> lookupEnv "DATABASE_URL"
  schemaStr <- fromMaybe "arbiter_demo" <$> lookupEnv "SCHEMA"
  port <- envOr "PORT" defaultPort
  let schema = T.pack schemaStr
  putStrLn "Resetting schema..."
  bracket (PG.connectPostgreSQL connStr) PG.close $ \conn ->
    void $ PG.execute conn "DROP SCHEMA IF EXISTS ? CASCADE" (PG.Only (PGT.Identifier schema))

  putStrLn "Running migrations..."
  migrationResult <-
    Mig.runMigrationsForRegistry
      (Proxy @DemoRegistry)
      connStr
      schema
      Mig.defaultMigrationConfig {Mig.enableEventStreaming = True}
  case migrationResult of
    Mig.MigrationSuccess -> putStrLn "Migrations complete"
    Mig.MigrationError err -> die $ "Migration failed: " <> err

  -- Producer env for seeding and the background load generator. The worker
  -- pools get their own pool from the managed runner below.
  producerEnv <- ArbS.createSimpleEnv (Proxy @DemoRegistry) connStr schema

  putStrLn "Seeding demo data..."
  seedDemoData producerEnv

  putStrLn ""
  putStrLn "Setting up server..."
  serverConfig <- Servant.createArbiterServer (ArbS.runSimpleDb producerEnv)
  putStrLn "Server ready"

  putStrLn "Creating worker configs..."
  demoWorkerCfg <- simpleWorker 5 2 10 [demoTicker]
  emailWorkerCfg <- simpleWorker 3 2 10 [emailDigest]
  notifWorkerCfg <- simpleWorker 1 0.05 2 [notifBroadcast]
  bulkWorkerCfg <- mkBulkWorker
  pipelineWorkerCfg <- mkPipelineWorker
  putStrLn "Workers configured"

  let policy =
        simpleCorsResourcePolicy
          { corsRequestHeaders = ["Content-Type", "Accept"]
          , corsMethods = ["GET", "POST", "DELETE", "PATCH", "OPTIONS"]
          }

  -- Dev mode: serve static files from disk when ADMIN_DEV_DIR is set
  mDevDir <- lookupEnv "ADMIN_DEV_DIR"
  traceHttp <- newOpenTelemetryWaiMiddleware
  let app = traceHttp $ cors (const $ Just policy) $ logStdout $ demoApp mDevDir serverConfig

  putStrLn ""
  putStrLn "=== Server Starting ==="
  putStrLn $ "API:     http://localhost:" <> show port <> "/api"
  putStrLn $ "Admin:   http://localhost:" <> show port <> "/"
  putStrLn $ "Docs:    http://localhost:" <> show port <> "/docs"
  putStrLn "Workers and cron schedules running across all queues"
  case mDevDir of
    Just dir -> putStrLn $ "Dev: serving static files from " <> dir
    Nothing -> putStrLn "Set ADMIN_DEV_DIR to serve static files from disk"
  putStrLn ""
  putStrLn "Press Ctrl+C to stop"
  putStrLn ""

  let workers =
        [ Worker.namedWorkerPool demoWorkerCfg
        , Worker.namedWorkerPool emailWorkerCfg
        , Worker.namedWorkerPool notifWorkerCfg
        , Worker.namedWorkerPool bulkWorkerCfg
        , Worker.namedWorkerPool pipelineWorkerCfg
        ]
  let handler = Signals.Catch $ Worker.shutdownWorkerPools workers
  void $ Signals.installHandler Signals.sigTERM handler Nothing
  void $ Signals.installHandler Signals.sigINT handler Nothing

  -- Self-restart watchdog. After RESET_INTERVAL_MINUTES, raise SIGTERM. The
  -- handler above exits the process and the container's restart policy reseeds
  -- a clean demo. A value of 0 disables it.
  resetMin <- envOr "RESET_INTERVAL_MINUTES" defaultResetMinutes
  when (resetMin > 0) $ void $ forkIO $ do
    threadDelay (micros (fromIntegral (resetMin * secondsPerMinute)))
    putStrLn $ "[reset] " <> show resetMin <> "m elapsed, restarting for a clean demo"
    Signals.raiseSignal Signals.sigTERM

  -- Background load generator. Pulse a few jobs on an interval. A value of 0
  -- disables it.
  pulseSec <- envOr "LOAD_PULSE_SECONDS" defaultLoadPulseSeconds
  when (pulseSec > 0) $ void $ forkIO $ loadPulse producerEnv pulseSec

  -- Emit a small archivable pipeline on an interval. A value of 0 disables it.
  pipeSec <- envOr "PIPELINE_PULSE_SECONDS" defaultPipelinePulseSeconds
  when (pipeSec > 0) $ void $ forkIO $ pipelinePulse producerEnv pipeSec

  -- Bursty batch inserts into bulk_queue. A spike of multi-row inserts on a
  -- jittered interval. Either value at 0 disables it.
  burstSec <- envOr "BURST_INTERVAL_SECONDS" defaultBurstSeconds
  burstSize <- envOr "BURST_SIZE" defaultBurstSize
  burstGroups <- envOr "BURST_GROUPS" defaultBurstGroups
  when (burstSec > 0 && burstSize > 0) $ do
    putStrLn $ "Burst load: ~" <> show burstSize <> " jobs every ~" <> show burstSec <> "s into bulk_queue"
    void $ forkIO $ burstPulse producerEnv burstSec burstSize burstGroups

  poolCfg <- Worker.poolConfigForWorkers workers
  workerEnv <- ArbS.createSimpleEnvWithConfig (Proxy @DemoRegistry) connStr schema poolCfg

  putStrLn $ "Telemetry: " <> T.unpack (Otel.telemetrySummary tel)
  race_
    (ArbS.runSimpleDb workerEnv $ Otel.runWorkerPoolsWith tel Worker.defaultLogConfig workers)
    (runSettings (setPort port defaultSettings) app)

-- ---------------------------------------------------------------------------
-- Worker configs
-- ---------------------------------------------------------------------------

type DemoM = ArbS.SimpleDb DemoRegistry IO

-- | Seconds in one minute.
secondsPerMinute :: Int
secondsPerMinute = 60

simulateWork :: Double -> IO ()
simulateWork mean = do
  uniform <- randomRIO (1.0e-6, 1.0)
  threadDelay (micros (realToFrac (mean * negate (log uniform))))

-- | A transactional worker pool whose handler sleeps for @mean@ seconds on average.
simpleWorker
  :: (Arb.ResultOf DemoM payload ~ ())
  => Int
  -- ^ Worker count
  -> Double
  -- ^ Mean handler delay (seconds)
  -> NominalDiffTime
  -- ^ Poll interval
  -> [Worker.CronJob payload]
  -> IO (Worker.WorkerConfig DemoM payload)
simpleWorker count mean poll crons = do
  cfg <- Worker.transactionalWorkerConfig count (\_conn _job -> liftIO $ simulateWork mean)
  pure (polling poll cfg) {Worker.cronJobs = crons}

-- | Poll at @interval@ seconds with no liveness file.
polling :: NominalDiffTime -> Worker.WorkerConfig m payload -> Worker.WorkerConfig m payload
polling interval cfg = cfg {Worker.pollInterval = interval, Worker.livenessFile = Nothing}

-- | 'cronJob' for a literal expression. A bad expression is a crash.
mustCron
  :: Text -> Text -> Worker.OverlapPolicy -> (Worker.TickKind -> UTCTime -> Arb.JobWrite payload) -> Worker.CronJob payload
mustCron name expr policy = either error id . Worker.cronJob name expr policy

demoTicker :: Worker.CronJob DemoPayload
demoTicker =
  mustCron
    "demo-ticker"
    "* * * * *" -- every minute
    Worker.AllowOverlap
    (\_ tickTime -> Arb.defaultJob (TestMessage $ "tick:" <> showText tickTime))

emailDigest :: Worker.CronJob EmailPayload
emailDigest =
  mustCron
    "email-digest"
    "*/2 * * * *" -- every 2 minutes
    Worker.SkipOverlap
    (\_ _ -> Arb.defaultJob (SendEmail "scheduled-digest"))

notifBroadcast :: Worker.CronJob NotificationPayload
notifBroadcast =
  mustCron
    "notif-broadcast"
    "*/3 * * * *" -- every 3 minutes
    Worker.AllowOverlap
    (\_ tickTime -> Arb.defaultJob (PushNotification $ "broadcast:" <> showText tickTime))

-- | Drains the burst queue. Many workers claim large batches with a near-zero
-- handler. Sized by BURST_WORKERS and BURST_BATCH.
mkBulkWorker :: IO (Worker.WorkerConfig DemoM BulkPayload)
mkBulkWorker = do
  count <- envOr "BURST_WORKERS" defaultBurstWorkers
  batch <- envOr "BURST_BATCH" defaultBurstBatch
  cfg <- Worker.batchedWorkerConfig count batch handler
  pure (polling 1 cfg)
  where
    -- Roughly 300 jobs a second across the pool. A burst drains slowly enough
    -- to be sampled.
    handler jobs cbs = do
      liftIO $ simulateWork 0.5
      Worker.ackAll cbs (NE.toList jobs)

-- ---------------------------------------------------------------------------
-- Pipeline worker - rollup demo
-- ---------------------------------------------------------------------------

mkPipelineWorker :: IO (Worker.WorkerConfig DemoM PipelinePayload)
mkPipelineWorker = do
  cfg <- Worker.transactionalWorkerConfig 3 handler
  pure (polling 2 cfg)
  where
    handler _conn job = case Arb.payload job of
      ProcessChunk chunkName -> do
        liftIO $ simulateWork 1
        pure (chunkFindings chunkName)
      AggregateResults _ -> fst <$> Worker.mergedChildResults job

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
seedDemoData :: ArbS.SimpleEnv DemoRegistry -> IO ()
seedDemoData env = ArbS.runSimpleDb env $ do
  seedPipeline
  seedQueues
  liftIO $ putStrLn "  Seeded jobs across demo_queue, email_queue, notifications, and pipeline"

-- | Rollup branches under the seeded pipeline's root.
seedPipelineBranches :: Int
seedPipelineBranches = 300

-- | The first @n@ elements, at least one.
takeNonEmpty :: Int -> NonEmpty a -> NonEmpty a
takeNonEmpty n (x :| xs) = x :| take (n - 1) xs

-- | A 3-level rollup pipeline. Results flow up from leaves through finalizers.
-- Priority 10 keeps this long run behind the quick pipelines from 'pipelinePulse'.
seedPipeline :: DemoM ()
seedPipeline = do
  let background job = Arb.setPriority 10 $ Arb.defaultJob job
      chunk = Arb.leaf . background . ProcessChunk
      agg name = Arb.rollup (background (AggregateResults name))
  tracer <- demoTracer
  result <- Trace.inSpan' tracer "seed pipeline" Trace.defaultSpanArguments $ \seedSpan -> do
    tree <-
      Arb.insertJobTree $
        agg
          "final-report"
          ( takeNonEmpty seedPipelineBranches $
              NE.cycle
                [ agg "financials" [chunk "revenue-data", chunk "expense-data", chunk "forecast-data"]
                , agg "operations" [chunk "inventory-data", chunk "shipping-data", chunk "support-data"]
                ]
          )
    traverse_
      ( \nodes ->
          Trace.addAttributes seedSpan $
            HM.fromList
              [ ("demo.pipeline.node_count", Attr.toAttribute (NE.length nodes))
              , ("demo.pipeline.root_id", Attr.toAttribute (showText (Arb.primaryKey (NE.head nodes))))
              ]
      )
      tree
    pure tree
  case result of
    Left err -> liftIO $ die $ "pipeline seed failed: " <> show err
    Right (root :| rest) ->
      liftIO
        $ putStrLn
        $ "  pipeline: root #" <> show (Arb.primaryKey root) <> " with " <> show (length rest) <> " descendants"

-- | A spread across the flat queues covering ready, scheduled, suspended,
-- backoff, and dead-letter, with varied priorities and group keys.
seedQueues :: DemoM ()
seedQueues = do
  now <- liftIO getCurrentTime

  -- Backoff and DLQ examples are produced through the normal lifecycle.
  void $
    Arb.insertJob
      (Arb.setGroupKey (Just "integrations") $ Arb.setMaxAttempts (Just 5) $ Arb.defaultJob (TestMessage "flaky-webhook"))
  [flaky1] <- Arb.claimNextVisibleJobs @DemoPayload 1 60
  void $ Arb.updateJobForRetry 0 "connection reset by upstream" flaky1
  [flaky2] <- Arb.claimNextVisibleJobs @DemoPayload 1 60
  void $ Arb.updateJobForRetry 180 "connection reset by upstream" flaky2

  void $
    Arb.insertJob
      (Arb.setGroupKey (Just "ingest") $ Arb.setMaxAttempts (Just 5) $ Arb.defaultJob (TestMessage "corrupt-record"))
  replicateM_ 4 $ do
    [failed] <- Arb.claimNextVisibleJobs @DemoPayload 1 60
    void $ Arb.updateJobForRetry 0 "unparseable payload" failed
  [doomed] <- Arb.claimNextVisibleJobs @DemoPayload 1 60
  void $ Arb.moveToDLQ "unparseable payload after 5 attempts" doomed

  -- demo_queue: ready jobs with varied priority and group keys
  traverse_
    ( \(index, (grp, msg)) ->
        void $
          Arb.insertJob
            (Arb.setGroupKey (Just grp) $ Arb.setPriority (fromIntegral (index `mod` 5)) $ Arb.defaultJob (TestMessage msg))
    )
    (zip [0 :: Int ..] demoTasks)
  -- scheduled (not visible yet) and suspended (paused)
  void $
    Arb.insertJob
      ( Arb.setGroupKey (Just "maintenance")
          $ Arb.setNotVisibleUntil (Just (addUTCTime 3600 now))
          $ Arb.defaultJob (TestMessage "nightly-reindex")
      )
  paused <-
    need =<< Arb.insertJob (Arb.setGroupKey (Just "maintenance") $ Arb.defaultJob (TestMessage "paused-migration"))
  void $ Arb.suspendJob @DemoPayload (Arb.primaryKey paused)

  -- email_queue: a few ready, one scheduled, one bounced to the DLQ
  void $ Arb.insertJob (Arb.setMaxAttempts (Just 2) $ Arb.defaultJob (SendEmail "nobody@invalid.test"))
  [rejected] <- Arb.claimNextVisibleJobs @EmailPayload 1 60
  void $ Arb.updateJobForRetry 0 "recipient rejected (550)" rejected
  traverse_
    (void . Arb.insertJob . Arb.defaultJob . SendEmail)
    (["welcome@acme.test", "receipt@acme.test", "reset@acme.test"] :: [Text])
  void $ Arb.insertJob (Arb.setNotVisibleUntil (Just (addUTCTime 1800 now)) $ Arb.defaultJob (SendEmail "weekly-digest"))

  -- notifications: a few ready
  traverse_
    (void . Arb.insertJob . Arb.defaultJob . PushNotification)
    (["deploy finished", "build green", "nightly backup ok"] :: [Text])
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
loadPulse :: ArbS.SimpleEnv DemoRegistry -> Int -> IO ()
loadPulse env sec = traverse_ pulse (enumFrom (0 :: Int))
  where
    -- Archive one job in @every@.
    keepEvery every tick = if tick `mod` every == 0 then Just archiveRetention else Nothing
    pulse tick = do
      threadDelay (micros (fromIntegral sec))
      ArbS.runSimpleDb env $ do
        void $
          Arb.insertJob
            ( Arb.setArchiveFor (keepEvery 4 tick)
                $ Arb.setPriority (fromIntegral (tick `mod` 5))
                $ Arb.defaultJob (TestMessage ("pulse #" <> showText tick))
            )
        when (even tick)
          $ void
          $ Arb.insertJob (Arb.setArchiveFor (keepEvery 6 tick) $ Arb.defaultJob (SendEmail ("digest #" <> showText tick)))
        when (tick `mod` 3 == 0)
          $ void
          $ Arb.insertJob (Arb.setArchiveFor (keepEvery 9 tick) $ Arb.defaultJob (PushNotification ("alert #" <> showText tick)))

-- | Burst load. Every @sec@ seconds (jittered by half), insert @size@ jobs as
-- back-to-back multi-row inserts, with a four-times spike every fifth round.
-- Half of each burst joins one of @groups@ group keys.
burstPulse :: ArbS.SimpleEnv DemoRegistry -> Int -> Int -> Int -> IO ()
burstPulse env sec size groups = traverse_ pulse (enumFrom (0 :: Int))
  where
    perStatement = 250
    spikeEvery = 5
    spikeFactor = 4
    grouped index
      | groups <= 0 || odd index = id
      | otherwise = Arb.setGroupKey (Just ("bulk-" <> showText ((index `div` 2) `mod` groups)))
    pulse tick = do
      gap <- randomRIO (0.5, 1.5 :: Double)
      threadDelay (micros (realToFrac (fromIntegral sec * gap)))
      let total = if tick `mod` spikeEvery == spikeEvery - 1 then size * spikeFactor else size
          jobs =
            [ grouped index (Arb.defaultJob (BulkTask ("burst " <> showText tick <> " #" <> showText index))) | index <- [1 .. total]
            ]
      ArbS.runSimpleDb env $ traverse_ (void . Arb.insertJobsBatch_) (chunksOf perStatement jobs)

-- | Split into chunks of at most @chunkSize@.
chunksOf :: Int -> [a] -> [[a]]
chunksOf chunkSize = unfoldr (\remaining -> if null remaining then Nothing else Just (splitAt chunkSize remaining))

-- | Chunk sets a quick pipeline picks from, for variety across runs.
quickChunkSets :: NonEmpty (NonEmpty Text)
quickChunkSets =
  [ ["revenue-data", "expense-data", "forecast-data"]
  , ["inventory-data", "shipping-data", "support-data"]
  , ["revenue-data", "churn-data"]
  , ["forecast-data", "support-data", "churn-data"]
  ]

-- | Insert a small rollup every @sec@ seconds. Its root archives its merged result.
pipelinePulse :: ArbS.SimpleEnv DemoRegistry -> Int -> IO ()
pipelinePulse env sec = traverse_ pulse (enumFrom (0 :: Int))
  where
    pulse tick = do
      threadDelay (micros (fromIntegral sec))
      pick <- randomRIO (0, NE.length quickChunkSets - 1)
      let chosen = quickChunkSets NE.!! pick
          root = Arb.setArchiveFor (Just archiveRetention) $ Arb.defaultJob (AggregateResults ("quick-report-" <> showText tick))
          leaves = Arb.leaf . Arb.defaultJob . ProcessChunk <$> chosen
      ArbS.runSimpleDb env $ void $ Arb.insertJobTree (Arb.rollup root leaves)

demoTracer :: (MonadIO m) => m Trace.Tracer
demoTracer = do
  provider <- Trace.getGlobalTracerProvider
  pure (Trace.makeTracer provider "arbiter-demo" Trace.tracerOptions)

-- | How long pulse jobs stay in the archive.
archiveRetention :: Int32
archiveRetention = 3600

defaultPort :: Int
defaultPort = 8080

defaultResetMinutes :: Int
defaultResetMinutes = 20

defaultLoadPulseSeconds :: Int
defaultLoadPulseSeconds = 5

defaultPipelinePulseSeconds :: Int
defaultPipelinePulseSeconds = 2

defaultBurstSeconds :: Int
defaultBurstSeconds = 10

defaultBurstSize :: Int
defaultBurstSize = 1000

defaultBurstGroups :: Int
defaultBurstGroups = 8

defaultBurstWorkers :: Int
defaultBurstWorkers = 6

defaultBurstBatch :: Int
defaultBurstBatch = 25

-- | Read an environment variable, or the default when unset.
envOr :: (Read a) => String -> a -> IO a
envOr name fallback = lookupEnv name >>= maybe (pure fallback) parse
  where
    parse raw = maybe (die (name <> ": cannot parse " <> show raw)) pure (readMaybe raw)

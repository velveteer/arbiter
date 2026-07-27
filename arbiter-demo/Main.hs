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
import Arbiter.Core.Job.Types (Job (..), defaultJob)
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Migrations (MigrationConfig (..), MigrationResult (..), defaultMigrationConfig, runMigrationsForRegistry)
import Arbiter.RateLimit (HasRateLimit (..), globalLimit, limitBy, limitByCase, tokenBucket)
import Arbiter.Servant (initArbiterServer)
import Arbiter.Servant.UI (arbiterAppWithAdmin, arbiterAppWithAdminDev)
import Arbiter.Simple
import Arbiter.Worker
  ( WorkerConfig (..)
  , mergedChildResults
  , namedWorkerPool
  , poolConfigForWorkers
  , runWorkerPools
  , shutdownPools
  , transactionalWorkerConfig
  )
import Arbiter.Worker.Cron (OverlapPolicy (..), cronJob)
import Control.Concurrent (forkIO, threadDelay)
import Control.Concurrent.Async (race_)
import Control.Monad (forM_, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString.Char8 qualified as BS
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (addUTCTime, diffTimeToPicoseconds, getCurrentTime, utctDayTime)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Network.Wai.Handler.Warp (defaultSettings, runSettings, setPort, setTimeout)
import Network.Wai.Middleware.Cors
  ( CorsResourcePolicy (..)
  , cors
  , simpleCorsResourcePolicy
  )
import Network.Wai.Middleware.RequestLogger (logStdout)
import System.Environment (lookupEnv)
import System.Exit (die)
import System.Posix.Signals qualified as Signals

-- ---------------------------------------------------------------------------
-- Payload types
-- ---------------------------------------------------------------------------

data DemoPayload
  = TestMessage Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

data EmailPayload
  = SendEmail Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

data NotificationPayload
  = PushNotification Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Pipeline payload for the rollup demo.
--
-- Leaf jobs carry a chunk name and produce @[Text]@ results.
-- Finalizer jobs merge child results and propagate them upward.
data PipelinePayload
  = ProcessChunk Text
  | AggregateResults Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Demo registry with multiple queues
type DemoRegistry =
  '[ Queue "demo_queue" DemoPayload
   , Queue "email_queue" EmailPayload
   , Queue "notifications" NotificationPayload
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
main = do
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
  pipelineWorkerCfg <- mkPipelineWorker
  putStrLn "Workers configured"

  let policy =
        simpleCorsResourcePolicy
          { corsRequestHeaders = ["Content-Type", "Accept"]
          , corsMethods = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
          }

  -- Dev mode: serve static files from disk when ADMIN_DEV_DIR is set
  mDevDir <- lookupEnv "ADMIN_DEV_DIR"
  let app = case mDevDir of
        Just dir -> cors (const $ Just policy) $ logStdout $ arbiterAppWithAdminDev @DemoRegistry dir serverConfig
        Nothing -> cors (const $ Just policy) $ logStdout $ arbiterAppWithAdmin @DemoRegistry serverConfig

  -- Start server
  putStrLn ""
  putStrLn "=== Server Starting ==="
  putStrLn $ "API:     http://localhost:" <> show port <> "/api/v1"
  putStrLn $ "Admin:   http://localhost:" <> show port <> "/"
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
        , namedWorkerPool pipelineWorkerCfg
        ]
      handler = Signals.Catch $ shutdownPools workers
  void $ Signals.installHandler Signals.sigTERM handler Nothing
  void $ Signals.installHandler Signals.sigINT handler Nothing

  -- Self-restart watchdog: after RESET_INTERVAL_MINUTES, raise SIGTERM so the
  -- process exits via the handler above and the container's restart policy
  -- reseeds a clean demo. A value of 0 disables it.
  resetMin <- maybe 20 read <$> lookupEnv "RESET_INTERVAL_MINUTES"
  when (resetMin > 0) $ void $ forkIO $ do
    threadDelay (resetMin * 60 * 1_000_000)
    putStrLn $ "[reset] " <> show resetMin <> "m elapsed, restarting for a clean demo"
    Signals.raiseSignal Signals.sigTERM

  -- Background load generator: pulse a few jobs on an interval so the queues
  -- stay visibly active between cron ticks. A value of 0 disables it.
  pulseSec <- maybe 5 read <$> lookupEnv "LOAD_PULSE_SECONDS"
  when (pulseSec > 0) $ void $ forkIO $ loadPulse producerEnv pulseSec

  -- Emit a small archivable pipeline on an interval so completed runs accumulate
  -- in the archive with visible results. A value of 0 disables it.
  pipeSec <- maybe 2 read <$> lookupEnv "PIPELINE_PULSE_SECONDS"
  when (pipeSec > 0) $ void $ forkIO $ pipelinePulse producerEnv schema pipeSec

  poolCfg <- poolConfigForWorkers workers
  workerEnv <- createSimpleEnvWithConfig (Proxy @DemoRegistry) connStr schema poolCfg
  race_
    (runSimpleDb workerEnv $ runWorkerPools workers)
    (runSettings (setPort port $ setTimeout 0 defaultSettings) app)

-- ---------------------------------------------------------------------------
-- Worker configs
-- ---------------------------------------------------------------------------

type DemoM = SimpleDb DemoRegistry IO

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
    handler _conn _job = liftIO $ threadDelay 5_000_000 -- simulate work
    demoCrons =
      [ either error id $
          cronJob
            "demo-ticker"
            "* * * * *" -- every minute
            AllowOverlap
            (\_ t -> defaultJob (TestMessage $ "tick:" <> tshow t))
      ]

mkEmailWorker :: IO (WorkerConfig DemoM EmailPayload)
mkEmailWorker = do
  cfg <- transactionalWorkerConfig 1 handler
  pure
    cfg
      { cronJobs = emailCrons
      , pollInterval = 10
      , livenessFile = Nothing
      }
  where
    handler _conn _job = liftIO $ threadDelay 5_000_000 -- simulate work
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
    handler _conn _job = pure ()
    notifCrons =
      [ either error id $
          cronJob
            "notif-broadcast"
            "*/3 * * * *" -- every 3 minutes
            AllowOverlap
            (\_ t -> defaultJob (PushNotification $ "broadcast:" <> tshow t))
      ]

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
        liftIO $ threadDelay 1_500_000 -- simulate 1.5s of work
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

-- | Seed a varied initial dataset so a fresh demo exercises every queue and
-- every status the admin UI can show: ready, scheduled, suspended, backoff,
-- in-flight (as workers claim jobs), and dead-letter.
seedDemoData :: SimpleEnv DemoRegistry -> Text -> IO ()
seedDemoData env schemaName = runSimpleDb env $ do
  seedPipeline schemaName
  seedQueues
  liftIO $ putStrLn "  Seeded jobs across demo_queue, email_queue, notifications, and pipeline"

-- | A 3-level rollup pipeline. Results flow up from leaves through finalizers.
-- Priority 10 keeps this long run behind the quick pipelines from 'pipelinePulse'.
seedPipeline :: Text -> DemoM ()
seedPipeline schemaName = do
  let bg p = (defaultJob p) {priority = 10}
      chunk = JT.leaf . bg . ProcessChunk
      agg name = JT.rollup (bg (AggregateResults name))
  result <-
    JT.insertJobTree schemaName "pipeline" $
      agg
        "final-report"
        ( NE.fromList $
            NE.take 300 $
              NE.cycle
                [ agg "financials" [chunk "revenue-data", chunk "expense-data", chunk "forecast-data"]
                , agg "operations" [chunk "inventory-data", chunk "shipping-data", chunk "support-data"]
                ]
        )
  case result of
    Left err -> liftIO $ die $ "pipeline seed failed: " <> show err
    Right (root :| rest) ->
      liftIO $ putStrLn $ "  pipeline: root #" <> show (primaryKey root) <> " with " <> show (length rest) <> " descendants"

-- | A spread across the flat queues covering ready, scheduled, suspended,
-- backoff, and dead-letter, with varied priorities and group keys.
seedQueues :: DemoM ()
seedQueues = do
  now <- liftIO getCurrentTime

  -- demo_queue: ready jobs with varied priority and group keys
  forM_ (zip [0 :: Int ..] demoTasks) $ \(i, (grp, msg)) ->
    void $ HL.insertJob ((defaultJob (TestMessage msg)) {priority = fromIntegral (i `mod` 5), groupKey = Just grp})
  -- scheduled (not visible yet) and suspended (paused)
  void $
    HL.insertJob
      ( (defaultJob (TestMessage "nightly-reindex"))
          { notVisibleUntil = Just (addUTCTime 3600 now)
          , groupKey = Just "maintenance"
          }
      )
  void $ HL.insertJob ((defaultJob (TestMessage "paused-migration")) {suspended = True, groupKey = Just "maintenance"})
  -- backoff: failed twice, waiting to retry
  void $
    HL.insertJob
      ( (defaultJob (TestMessage "flaky-webhook"))
          { attempts = 2
          , lastError = Just "connection reset by upstream"
          , notVisibleUntil = Just (addUTCTime 180 now)
          , groupKey = Just "integrations"
          }
      )
  -- dead-letter: exhausted its retries
  doomed <-
    need
      =<< HL.insertJob
        ( (defaultJob (TestMessage "corrupt-record"))
            { attempts = 5
            , maxAttempts = Just 5
            , lastError = Just "unparseable payload"
            , groupKey = Just "integrations"
            }
        )
  void $ HL.moveToDLQ "unparseable payload after 5 attempts" doomed

  -- email_queue: a few ready, one scheduled, one bounced to the DLQ
  forM_ (["welcome@acme.test", "receipt@acme.test", "reset@acme.test"] :: [Text]) $ \addr ->
    void $ HL.insertJob (defaultJob (SendEmail addr))
  void $ HL.insertJob ((defaultJob (SendEmail "weekly-digest")) {notVisibleUntil = Just (addUTCTime 1800 now)})
  bounced <-
    need
      =<< HL.insertJob
        ( (defaultJob (SendEmail "nobody@invalid.test"))
            { attempts = 3
            , maxAttempts = Just 3
            , lastError = Just "recipient rejected (550)"
            }
        )
  void $ HL.moveToDLQ "recipient rejected (550)" bounced

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

-- | Background load generator: a small pulse of jobs every @sec@ seconds so the
-- queues stay visibly active between cron ticks.
loadPulse :: SimpleEnv DemoRegistry -> Int -> IO ()
loadPulse env sec = go 0
  where
    -- Archive one job in @k@ so ordinary completed jobs also fill the archive.
    keepEvery k n = if n `mod` k == 0 then Just 3600 else Nothing
    go :: Int -> IO ()
    go n = do
      threadDelay (sec * 1_000_000)
      runSimpleDb env $ do
        void $
          HL.insertJob
            ((defaultJob (TestMessage ("pulse #" <> tshow n))) {priority = fromIntegral (n `mod` 5), archiveFor = keepEvery 4 n})
        when (even n) $ void $ HL.insertJob ((defaultJob (SendEmail ("digest #" <> tshow n))) {archiveFor = keepEvery 6 n})
        when (n `mod` 3 == 0) $
          void $
            HL.insertJob ((defaultJob (PushNotification ("alert #" <> tshow n))) {archiveFor = keepEvery 9 n})
      go (n + 1)

-- | Chunk sets a quick pipeline picks from, for variety across runs.
quickChunkSets :: [[Text]]
quickChunkSets =
  [ ["revenue-data", "expense-data", "forecast-data"]
  , ["inventory-data", "shipping-data", "support-data"]
  , ["revenue-data", "churn-data"]
  , ["forecast-data", "support-data", "churn-data"]
  ]

-- | Insert a small rollup every @sec@ seconds whose root archives its merged
-- result, so finished runs accumulate in the archive with visible results.
pipelinePulse :: SimpleEnv DemoRegistry -> Text -> Int -> IO ()
pipelinePulse env schemaName sec = go 0
  where
    go :: Int -> IO ()
    go n = do
      threadDelay (sec * 1_000_000)
      t <- getCurrentTime
      let seed = fromInteger (diffTimeToPicoseconds (utctDayTime t)) :: Int
          chosen = quickChunkSets !! (seed `mod` length quickChunkSets)
          root = (defaultJob (AggregateResults ("quick-report-" <> tshow n))) {archiveFor = Just 3600}
          leaves = NE.fromList (map (JT.leaf . defaultJob . ProcessChunk) chosen)
      runSimpleDb env $ void $ JT.insertJobTree schemaName "pipeline" (JT.rollup root leaves)
      go (n + 1)

tshow :: (Show a) => a -> Text
tshow = T.pack . show

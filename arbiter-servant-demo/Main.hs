{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

module Main (main) where

import Arbiter.Core.Job.Types (Job (..), defaultJob)
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Migrations (MigrationConfig (..), MigrationResult (..), defaultMigrationConfig, runMigrationsForRegistry)
import Arbiter.Servant (initArbiterServer)
import Arbiter.Servant.UI (arbiterAppWithAdmin, arbiterAppWithAdminDev)
import Arbiter.Simple
import Arbiter.Worker
  ( WorkerConfig (..)
  , defaultRollupWorkerConfig
  , defaultWorkerConfig
  , namedWorkerPool
  , runWorkerPools
  , signalShutdown
  )
import Arbiter.Worker.Cron (OverlapPolicy (..), cronJob)
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (race_)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS
import Data.List.NonEmpty (NonEmpty (..))
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Network.Wai.Handler.Warp (defaultSettings, runSettings, setPort, setTimeout)
import Network.Wai.Middleware.Cors
  ( CorsResourcePolicy (..)
  , cors
  , simpleCorsResourcePolicy
  )
import Network.Wai.Middleware.RequestLogger (logStdoutDev)
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
  '[ '("demo_queue", DemoPayload)
   , '("email_queue", EmailPayload)
   , '("notifications", NotificationPayload)
   , '("pipeline", PipelinePayload)
   ]

main :: IO ()
main = do
  -- Get config from environment or use defaults
  connStr <-
    maybe "host=localhost port=54322 user=postgres password=master dbname=postgres" BS.pack
      <$> lookupEnv "DATABASE_URL"
  schemaStr <- maybe "arbiter_demo" id <$> lookupEnv "SCHEMA"
  portStr <- maybe "8080" id <$> lookupEnv "PORT"
  let schema = T.pack schemaStr
      port = read portStr :: Int

  putStrLn "=== Arbiter Servant Demo Server ==="
  putStrLn $ "Database: " <> BS.unpack connStr
  putStrLn $ "Schema: " <> schemaStr
  putStrLn $ "Port: " <> show port
  putStrLn ""

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

  -- Create worker environment (its own pool)
  workerEnv <- createSimpleEnv (Proxy @DemoRegistry) connStr schema

  -- Seed demo data
  putStrLn "Seeding pipeline (rollup) demo..."
  seedPipelineJobs workerEnv schema

  -- Create server config (own connection pool for admin API)
  putStrLn ""
  putStrLn "Setting up server..."
  serverConfig <- initArbiterServer (Proxy @DemoRegistry) connStr schema
  putStrLn "Server ready"

  -- Create worker configs with cron jobs
  putStrLn "Creating worker configs..."
  demoWorkerCfg <- mkDemoWorker connStr
  emailWorkerCfg <- mkEmailWorker connStr
  notifWorkerCfg <- mkNotifWorker connStr
  pipelineWorkerCfg <- mkPipelineWorker connStr
  putStrLn "Workers configured"

  let policy =
        simpleCorsResourcePolicy
          { corsRequestHeaders = ["Content-Type", "Accept"]
          , corsMethods = ["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"]
          }

  -- Dev mode: serve static files from disk when ADMIN_DEV_DIR is set
  mDevDir <- lookupEnv "ADMIN_DEV_DIR"
  let app = case mDevDir of
        Just dir -> cors (const $ Just policy) $ logStdoutDev $ arbiterAppWithAdminDev @DemoRegistry dir serverConfig
        Nothing -> cors (const $ Just policy) $ logStdoutDev $ arbiterAppWithAdmin @DemoRegistry serverConfig

  -- Start server
  putStrLn ""
  putStrLn "=== Server Starting ==="
  putStrLn $ "API:     http://localhost:" <> show port <> "/api/v1"
  putStrLn $ "Admin:   http://localhost:" <> show port <> "/"
  putStrLn "Workers: 4 queues (demo_queue, email_queue, notifications, pipeline)"
  putStrLn "Cron:    demo-ticker (every min), email-digest (every 2 min), notif-broadcast (every 3 min)"
  case mDevDir of
    Just dir -> putStrLn $ "Dev:     serving static files from " <> dir
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
      installSignals st = do
        let handler = Signals.Catch $ signalShutdown st
        void $ Signals.installHandler Signals.sigTERM handler Nothing
        void $ Signals.installHandler Signals.sigINT handler Nothing

  race_
    (runSimpleDb workerEnv $ runWorkerPools (Proxy @DemoRegistry) workers installSignals)
    (runSettings (setPort port $ setTimeout 0 defaultSettings) app)

-- ---------------------------------------------------------------------------
-- Worker configs
-- ---------------------------------------------------------------------------

type DemoM = SimpleDb DemoRegistry IO

mkDemoWorker :: ByteString -> IO (WorkerConfig DemoM DemoPayload ())
mkDemoWorker connStr = do
  cfg <- defaultWorkerConfig connStr 5 handler
  pure
    cfg
      { cronJobs = demoCrons
      , pollInterval = 10
      , livenessConfig = Nothing
      }
  where
    handler _conn job = liftIO $ do
      threadDelay 5_000_000
      putStrLn $ "[demo_queue] Processed: " <> show (payload job)
    demoCrons =
      [ either error id $
          cronJob
            "demo-ticker"
            "* * * * *" -- every minute
            AllowOverlap
            (\t -> defaultJob (TestMessage $ "tick:" <> fmtTime t))
      ]

mkEmailWorker :: ByteString -> IO (WorkerConfig DemoM EmailPayload ())
mkEmailWorker connStr = do
  cfg <- defaultWorkerConfig connStr 1 handler
  pure
    cfg
      { cronJobs = emailCrons
      , pollInterval = 10
      , livenessConfig = Nothing
      }
  where
    handler _conn job = liftIO $ do
      threadDelay 5_000_000
      putStrLn $ "[email_queue] Processed: " <> show (payload job)
    emailCrons =
      [ either error id $
          cronJob
            "email-digest"
            "*/2 * * * *" -- every 2 minutes
            SkipOverlap
            (\_ -> defaultJob (SendEmail "scheduled-digest"))
      ]

mkNotifWorker :: ByteString -> IO (WorkerConfig DemoM NotificationPayload ())
mkNotifWorker connStr = do
  cfg <- defaultWorkerConfig connStr 1 handler
  pure
    cfg
      { cronJobs = notifCrons
      , pollInterval = 2
      , livenessConfig = Nothing
      }
  where
    handler _conn job = liftIO $ putStrLn $ "[notifications] Processed: " <> show (payload job)
    notifCrons =
      [ either error id $
          cronJob
            "notif-broadcast"
            "*/3 * * * *" -- every 3 minutes
            AllowOverlap
            (\t -> defaultJob (PushNotification $ "broadcast:" <> fmtTime t))
      ]

-- ---------------------------------------------------------------------------
-- Pipeline worker — rollup demo
-- ---------------------------------------------------------------------------

mkPipelineWorker :: ByteString -> IO (WorkerConfig DemoM PipelinePayload [Text])
mkPipelineWorker connStr = do
  cfg <- defaultRollupWorkerConfig connStr 3 handler
  pure cfg {pollInterval = 2, livenessConfig = Nothing}
  where
    handler childResults _dlqFailures _conn job = case payload job of
      ProcessChunk chunkName -> do
        liftIO $ threadDelay 1_500_000 -- simulate 1.5s of work
        let findings = chunkFindings chunkName
        liftIO $
          putStrLn $
            "[pipeline] Leaf #"
              <> show (primaryKey job)
              <> " \""
              <> T.unpack chunkName
              <> "\" -> "
              <> show findings
        pure findings
      AggregateResults label -> do
        liftIO $
          putStrLn $
            "[pipeline] Finalizer #"
              <> show (primaryKey job)
              <> " \""
              <> T.unpack label
              <> "\" — merged ("
              <> show (length childResults)
              <> " items): "
              <> show childResults
        pure childResults

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
-- Seed data: 3-level rollup pipeline
-- ---------------------------------------------------------------------------
seedPipelineJobs :: SimpleEnv DemoRegistry -> Text -> IO ()
seedPipelineJobs env schemaName = runSimpleDb env $ do
  let chunk = JT.leaf . defaultJob . ProcessChunk
      agg name = JT.rollup (defaultJob (AggregateResults name))

  Right (root :| rest) <-
    JT.insertJobTree schemaName "pipeline" $
      agg
        "final-report"
        [ agg "financials" [chunk "revenue-data", chunk "expense-data", chunk "forecast-data"]
        , agg "operations" [chunk "inventory-data", chunk "shipping-data", chunk "support-data"]
        ]
  liftIO $ do
    putStrLn $
      "  Created 3-level pipeline: root=#"
        <> show (primaryKey root)
        <> " with "
        <> show (length rest)
        <> " descendants"
    putStrLn "  Tree: final-report -> [financials -> [revenue, expense, forecast],"
    putStrLn "                         operations -> [inventory, shipping, support]]"
    putStrLn "  Watch the console for results flowing up through the tree!"

-- | Format a UTCTime for use in payloads
fmtTime :: UTCTime -> Text
fmtTime = T.pack . show

{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | The @arbiter@ binary: migrate the configured queues, serve the REST API and pools.
module Main (main) where

import Arbiter.Core.Concurrency.Spec (concurrencyPool)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.PoolConfig (PoolConfig (..), defaultPoolConfig)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Arbiter.Core.RateLimit.Schema (PolicyRow (..))
import Arbiter.Hasql (HasqlPool, createHasqlEnvWithPool, runHasqlDb, setPreparedStatements, withHasqlPool)
import Arbiter.Migrations
  ( AdmissionSeeds (..)
  , MigrationConfig (..)
  , MigrationResult (..)
  , allTableAdmission
  , defaultMigrationConfig
  , runMigrationsTrackedForTables
  )
import Arbiter.RateLimit (Durability (..))
import Arbiter.Servant (ArbiterServerConfig (..), initArbiterServer, runtimeQueue)
import Arbiter.Servant.UI (arbiterAppWithAdmin)
import Arbiter.Serve qualified as Serve
import Arbiter.Worker.Cron (CronJob)
import Control.Exception (SomeException, try)
import Control.Monad (when)
import Data.Aeson (Value)
import Data.ByteString (ByteString)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Network.HTTP.Client (Manager)
import Network.HTTP.Client.TLS (newTlsManager)
import Options.Applicative
import System.Exit (die)

import Arbiter.Cli.Config
import Arbiter.Cli.GenericWorker
  ( ClaimAdmission (..)
  , GenericHandler
  , PoolEnv (..)
  , genericPool
  , queuePoolSize
  )
import Arbiter.Cli.Webhook (WebhookConfig (..), defaultWebhookConfig, newWebhookHandler)

type EmptyReg = ('[] :: JobPayloadRegistry)

data Command = Migrate FilePath | Serve FilePath

main :: IO ()
main = do
  cmd <- execParser opts
  case cmd of
    Migrate path -> load path >>= runMigrate
    Serve path -> load path >>= runServe
  where
    opts =
      info
        (helper <*> commandParser)
        (fullDesc <> progDesc "Arbiter: a Postgres-backed job queue server")

commandParser :: Parser Command
commandParser =
  subparser
    ( command "migrate" (info (Migrate <$> configOpt) (progDesc "Create/upgrade the configured queue tables"))
        <> command "serve" (info (Serve <$> configOpt) (progDesc "Run migrations, then serve the API and any webhook workers"))
    )

configOpt :: Parser FilePath
configOpt =
  strOption
    (long "config" <> short 'c' <> metavar "FILE" <> value "arbiter.toml" <> showDefault <> help "Path to the TOML config")

load :: FilePath -> IO Config
load path = loadConfig path >>= either (die . ("config error:\n" <>)) pure

connBits :: Config -> (ByteString, Text)
connBits cfg =
  ( TE.encodeUtf8 (dbUrl (cfgDatabase cfg))
  , fromMaybe "arbiter" (dbSchema (cfgDatabase cfg))
  )

buildSeeds :: Config -> AdmissionSeeds
buildSeeds cfg =
  AdmissionSeeds
    { seedRateLimitPolicies =
        [PolicyRow (rlpPrefix p) (rlpMax p) (rlpRefill p) (rlpInterval p) | p <- cfgRateLimits cfg]
    , seedConcurrencyPolicies =
        [concurrencyPool (cpcPrefix p) (fromIntegral (cpcLimit p)) | p <- cfgConcurrency cfg]
    , seedDurability = Unlogged
    }

runMigrate :: Config -> IO ()
runMigrate cfg = do
  let (connStr, sch) = connBits cfg
      tables = [(qName q, allTableAdmission) | q <- cfgQueues cfg]
      mcfg = defaultMigrationConfig {enableEventStreaming = eventsEnabled cfg}
  when (null tables) $ die "no [[queue]] configured"
  result <- runMigrationsTrackedForTables connStr sch tables mcfg (buildSeeds cfg)
  case result of
    MigrationSuccess -> putStrLn ("migrated " <> show (length tables) <> " queue(s) in schema " <> T.unpack sch)
    MigrationError e -> die ("migration failed: " <> e)

-- | Serve the configured queues after migrating, or serve the API over already-registered queues when the config declares none.
runServe :: Config -> IO ()
runServe cfg = do
  let (connStr, sch) = connBits cfg
      prepared = dbPreparedStatements (cfgDatabase cfg)
  queues <-
    either die pure $
      traverse (\q -> (,,) q (workerSettings (qWorker q)) <$> queueCronJobs q) (cfgQueues cfg)

  let tel = cfgTelemetry cfg
      serveCfg =
        (Serve.defaultServeConfig connStr sch)
          { Serve.host = serverHost cfg
          , Serve.port = serverPort cfg
          , Serve.telemetry = if tcEnabled tel then Serve.TelemetryManaged else Serve.TelemetryOff
          , Serve.metricsScrape = tcMetrics tel
          , Serve.metricsPort = tcMetricsPort tel
          , Serve.preparedStatements = prepared
          , Serve.gaugeInterval = realToFrac (tcGaugeIntervalSecs tel)
          , Serve.shutdownGraceSecs = realToFrac (serverShutdownGraceSecs cfg)
          }

  mgr <- newTlsManager
  let poolCfg = defaultPoolConfig {poolSize = max 1 (sum [queuePoolSize (workersFor q) crons | (q, _, crons) <- queues])}
  withHasqlPool connStr poolCfg $ \pool -> do
    served <-
      if null queues
        then discoverServedQueues sch prepared pool
        else runMigrate cfg >> pure [qName q | (q, _, _) <- queues]
    admission <- claimAdmission sch prepared pool
    apiCfg0 <- initArbiterServer (Proxy @EmptyReg) connStr sch
    let apiCfg =
          apiCfg0
            { enableSSE = eventsEnabled cfg
            , serverQueues = Map.fromList [(name, runtimeQueue admission) | name <- served]
            }
        poolEnv =
          PoolEnv
            { peConnStr = connStr
            , pePool = pool
            , peSchema = sch
            , peQueues = served
            , peAdmission = admission
            , pePrepared = prepared
            }
    pools <- traverse (workerPool poolEnv mgr) queues
    Serve.runServeHarness serveCfg apiCfg arbiterAppWithAdmin pools

-- | Workers a queue runs locally. A queue with no webhook is served over HTTP instead.
workersFor :: QueueC -> Maybe Int
workersFor = fmap webhookWorkers . qWebhook

webhookWorkers :: WebhookC -> Int
webhookWorkers = fromMaybe 1 . wcWorkers

-- | A queue's pool: one that calls its webhook, or one that only runs the background loops.
workerPool :: PoolEnv -> Manager -> (QueueC, WorkerSettings, [CronJob Value]) -> IO Serve.ServePool
workerPool poolEnv mgr (q, ws, crons) = do
  handler <- traverse (webhookHandler mgr) (qWebhook q)
  genericPool poolEnv (qName q) ws crons handler

-- | The webhook a queue's claimed jobs are posted to, with the workers calling it.
webhookHandler :: Manager -> WebhookC -> IO (Int, GenericHandler)
webhookHandler mgr wh = do
  let base = defaultWebhookConfig (T.unpack (wcUrl wh))
      wcfg =
        base
          { whSecret = TE.encodeUtf8 <$> wcSecret wh
          , whTimeoutSecs = fromMaybe (whTimeoutSecs base) (wcTimeout wh)
          }
  (,) (webhookWorkers wh) <$> newWebhookHandler mgr wcfg

-- | Queue names to serve when the config declares none, read from the registry another process seeded.
discoverServedQueues :: Text -> Bool -> HasqlPool -> IO [Text]
discoverServedQueues sch prepared pool = do
  let env = setPreparedStatements prepared (createHasqlEnvWithPool (Proxy @EmptyReg) pool sch)
  result <- try (runHasqlDb env (Ops.discoverQueues sch)) :: IO (Either SomeException [Text])
  case result of
    Left e -> die ("no [[queue]] configured and the queue registry is unreadable in schema " <> T.unpack sch <> ": " <> show e)
    Right [] -> die ("no [[queue]] configured and the queue registry is empty in schema " <> T.unpack sch)
    Right names -> do
      putStrLn ("serving API over " <> show (length names) <> " registered queue(s) in schema " <> T.unpack sch)
      pure names

-- | What this process's claims gate on, read from the policy tables rather than the config.
claimAdmission :: Text -> Bool -> HasqlPool -> IO ClaimAdmission
claimAdmission sch prepared pool = do
  let env = setPreparedStatements prepared (createHasqlEnvWithPool (Proxy @EmptyReg) pool sch)
  (rateLimited, concurrent) <-
    runHasqlDb env ((,) <$> Ops.listRateLimitPrefixes sch <*> Ops.listConcurrencyPrefixes sch)
  pure
    ClaimAdmission
      { admitRateLimited = not (null rateLimited)
      , admitConcurrent = not (null concurrent)
      }

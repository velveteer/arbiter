{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Drives 'runArbiterServe' the way the README does: one call, a typed registry,
-- a pool. Runs against a real Postgres (ARBITER_TEST_CONN_STRING).
module Main (main) where

import Arbiter.Concurrency (HasConcurrency)
import Arbiter.Core.Job.Types (Job (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Migrations (MigrationResult (..), defaultMigrationConfig, runMigrationsForRegistry)
import Arbiter.RateLimit (HasRateLimit)
import Arbiter.Simple (SimpleDb, createSimpleEnv, runSimpleDb)
import Arbiter.Test.Config (getTestConnectionString)
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (resetSchema)
import Arbiter.Worker (WorkerConfig (..), defaultWorkerConfig, namedWorkerPool)
import Control.Concurrent.Async (async, wait)
import Control.Concurrent.STM (TVar, atomically, newTVarIO, readTVar, retry, writeTVar)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON, encode, object, (.=))
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import GHC.Generics (Generic)
import Network.HTTP.Client
  ( RequestBody (RequestBodyLBS)
  , defaultManagerSettings
  , httpLbs
  , method
  , newManager
  , parseRequest
  , requestBody
  , requestHeaders
  , responseStatus
  )
import Network.HTTP.Types (methodPost, statusCode)
import System.Posix.Signals qualified as Signals
import System.Timeout (timeout)
import Test.Hspec

import Arbiter.Serve qualified as Serve

newtype Greeting = Greeting Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, HasConcurrency, HasRateLimit, ToJSON)

type Reg = '[ '("greetings", Greeting)]

type ServeM = SimpleDb Reg IO

schema :: Text
schema = "arbiter_serve_test"

-- | Fixed, since the harness owns its listening socket.
servePort :: Int
servePort = 18099

-- | The handler records what it ran, so the test can see the job reach a worker.
greetingWorker :: ByteString -> TVar [Text] -> IO (WorkerConfig ServeM Greeting ())
greetingWorker connStr seen = do
  cfg <- defaultWorkerConfig connStr 1 handler
  pure cfg {pollInterval = 1, livenessFile = Nothing}
  where
    handler _conn job = liftIO $ do
      let Greeting who = payload job
      atomically $ readTVar seen >>= writeTVar seen . (who :)

main :: IO ()
main = hspec $ do
  describe "runGatedShared" $
    it "serves the winner's result to a caller that lost the gate, until it goes stale" $ do
      connStr <- getTestConnectionString
      resetSchema connStr schema
      migrated <- runMigrationsForRegistry (Proxy @Reg) connStr schema defaultMigrationConfig
      migrated `shouldBe` MigrationSuccess
      env <- createSimpleEnv (Proxy @Reg) connStr schema
      let shared :: NominalDiffTime -> ServeM (Maybe Text)
          shared maxAge = Ops.runGatedShared schema "test-task" 3600 maxAge (pure "scanned")
      runSimpleDb env $ do
        -- The winner runs the work and publishes what it computed.
        won <- shared 3600
        liftIO $ won `shouldBe` Just "scanned"
        -- The gate is shut now, so this stands in for a replica that lost it: it reads
        -- the winner's result rather than repeating the scan.
        lost <- shared 3600
        liftIO $ lost `shouldBe` Just "scanned"
        -- Past its max age it is withheld, so a task that stopped publishing leaves its
        -- callers with nothing rather than a reading that quietly stopped moving.
        stale <- shared 0
        liftIO $ stale `shouldBe` Nothing

  describe "runArbiterServe" $
    it "serves the API, runs the pool, and drains on SIGTERM" $ do
      connStr <- getTestConnectionString
      resetSchema connStr schema
      migrated <- runMigrationsForRegistry (Proxy @Reg) connStr schema defaultMigrationConfig
      migrated `shouldBe` MigrationSuccess

      env <- createSimpleEnv (Proxy @Reg) connStr schema
      seen <- newTVarIO []
      pool <- greetingWorker connStr seen
      mgr <- newManager defaultManagerSettings

      let serveCfg =
            (Serve.defaultServeConfig connStr schema)
              { Serve.port = servePort
              , Serve.telemetry = Serve.TelemetryOff
              , Serve.metricsScrape = False
              }
      -- Exactly the README's call.
      served <-
        async $
          runSimpleDb env $
            Serve.runArbiterServe @Reg serveCfg [namedWorkerPool pool]

      let get path = do
            req <- parseRequest ("http://localhost:" <> show servePort <> path)
            statusCode . responseStatus <$> httpLbs req mgr

      waitUntil 20_000 ((== Just 200) <$> timeout 500_000 (get "/healthz"))
      get "/readyz" `shouldReturn` 200

      base <- parseRequest ("http://localhost:" <> show servePort <> "/api/v1/queues/greetings/jobs")
      let req =
            base
              { method = methodPost
              , requestBody = RequestBodyLBS (encode (object ["payload" .= ("hello" :: Text)]))
              , requestHeaders = [("Content-Type", "application/json")]
              }
      -- The enqueue route the harness mounted, at the registry's queue.
      enqueued <- httpLbs req mgr
      statusCode (responseStatus enqueued) `shouldBe` 200

      ran <- timeout 20_000_000 $ atomically $ readTVar seen >>= \s -> if null s then retry else pure s
      ran `shouldBe` Just ["hello"]

      Signals.raiseSignal Signals.sigTERM
      drained <- timeout 30_000_000 (wait served)
      drained `shouldBe` Just ()

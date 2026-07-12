{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Integration tests for the TOML config the binary loads, and for the pools it runs
-- over a queue named only at runtime, against a real Postgres (ARBITER_TEST_CONN_STRING).
-- The HTTP routes those queues are served over are arbiter-servant's, and are tested there.
module Main (main) where

import Arbiter.Core.Concurrency.Spec (ConcurrencyPolicy (..))
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.PoolConfig (PoolConfig (..), defaultPoolConfig)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Arbiter.Core.RateLimit.Schema (PolicyRow (..))
import Arbiter.Core.RateLimit.Spec (RateLimitKey (..))
import Arbiter.Hasql (createHasqlEnv, runHasqlDb, withHasqlPool)
import Arbiter.Migrations
  ( AdmissionSeeds (..)
  , allTableAdmission
  , defaultMigrationConfig
  , runMigrationsTrackedForTables
  )
import Arbiter.RateLimit (Durability (Unlogged))
import Arbiter.Servant (ArbiterServerConfig (..), arbiterApp, initArbiterServer, runtimeQueue)
import Arbiter.Servant.Types (ApiJob, ClaimResponse (..))
import Arbiter.Serve (ServeEnv (..), ServePool (..))
import Arbiter.Test.Config (getTestConnectionString)
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (resetSchema)
import Arbiter.Worker (newWorkerState, signalShutdown)
import Arbiter.Worker.Cron (CronJob (..), TickKind (..))
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (async, wait)
import Control.Exception (bracket)
import Data.Aeson (Value (..), decode, encode, object, (.=))
import Data.Aeson.KeyMap qualified as KM
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as LBS
import Data.Either (isRight)
import Data.List (isInfixOf)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime (..), fromGregorian, secondsToDiffTime)
import Database.PostgreSQL.Simple qualified as PG
import Network.HTTP.Client hiding (Proxy)
import Network.HTTP.Types (Method, methodPost, statusCode)
import Network.Wai.Handler.Warp qualified as Warp
import System.Environment (setEnv)
import System.IO.Temp (writeSystemTempFile)
import System.Timeout (timeout)
import Test.Hspec

import Arbiter.Cli.Config
  ( Config (..)
  , CronC (..)
  , DatabaseC (..)
  , QueueC (..)
  , loadConfig
  , queueCronJobs
  , workerSettings
  )
import Arbiter.Cli.GenericWorker (ClaimAdmission (..), PoolEnv (..), genericPool, queuePoolSize)

reaperSchema :: Text
reaperSchema = "arbiter_cli_test_reaper"

maintSchema :: Text
maintSchema = "arbiter_cli_test_maint"

cronSchema :: Text
cronSchema = "arbiter_cli_test_cron"

-- ---------------------------------------------------------------------------
-- HTTP helpers
-- ---------------------------------------------------------------------------

data Server = Server {srvPort :: Int, srvMgr :: Manager}

send :: Server -> Method -> String -> Value -> IO (Int, LBS.ByteString)
send srv verb path body = do
  base <- parseRequest ("http://localhost:" <> show (srvPort srv) <> path)
  let req =
        base
          { method = verb
          , requestBody = RequestBodyLBS (encode body)
          , requestHeaders = [("Content-Type", "application/json")]
          }
  resp <- httpLbs req (srvMgr srv)
  pure (statusCode (responseStatus resp), responseBody resp)

post :: Server -> String -> Value -> IO (Int, LBS.ByteString)
post srv = send srv methodPost

-- | The jobs a claim response carries.
claimed :: LBS.ByteString -> [ApiJob Value]
claimed = foldMap claimedJobs . decode

-- ---------------------------------------------------------------------------
-- Specs
-- ---------------------------------------------------------------------------

main :: IO ()
main = hspec $ do
  describe "config validation" configSpec
  reaperSpec
  maintenanceSpec
  cronSpec

-- | The admission-maintenance operations the reaper runs for a runtime worker
-- must execute against a Value-payload registry without error. Guards the
-- generic (non-Haskell) worker's reaper path.
type ReaperReg = ('[ '("reaper_jobs", Value)] :: JobPayloadRegistry)

reaperSpec :: Spec
reaperSpec =
  describe "reaper (Value registry)" $
    it "prunes rate-limit buckets and reconciles concurrency" $ do
      connStr <- getTestConnectionString
      resetSchema connStr reaperSchema
      _ <-
        runMigrationsTrackedForTables
          connStr
          reaperSchema
          [("reaper_jobs", allTableAdmission)]
          defaultMigrationConfig
          (AdmissionSeeds [PolicyRow "rl" 5 5 60] [ConcurrencyPolicy "cc" 3] Unlogged)
      env <- createHasqlEnv (Proxy @ReaperReg) connStr reaperSchema
      runHasqlDb env (Arb.addRateLimitTokens (RateLimitKey "rl" "t1") 5)
      pruned <- runHasqlDb env (Arb.pruneRateLimitBuckets 0)
      pruned `shouldSatisfy` (>= 0)
      runHasqlDb env Arb.reconcileConcurrencyCountsIfStale
      runHasqlDb env Arb.reconcileAndPruneConcurrency

-- | Every generic pool carries a single-queue registry, so its maintenance loop
-- must still recount concurrency across every queue the process serves. Scoped to
-- its own queue alone, the recount finds no live jobs for a key another queue
-- holds and zeroes it, and the prune then deletes the row - breaching the cap.
maintenanceSpec :: Spec
maintenanceSpec =
  describe "maintenance (single-queue pool)" $
    it "keeps a concurrency key held by another queue in the process" $ do
      connStr <- getTestConnectionString
      resetSchema connStr maintSchema
      _ <-
        runMigrationsTrackedForTables
          connStr
          maintSchema
          [(q, allTableAdmission) | q <- maintQueues]
          defaultMigrationConfig
          (AdmissionSeeds [] [ConcurrencyPolicy "cc" 5] Unlogged)
      cfg0 <- initArbiterServer (Proxy @'[]) connStr maintSchema
      let cfg =
            cfg0
              { serverQueues =
                  Map.fromList [(q, runtimeQueue (ClaimAdmission False True)) | q <- maintQueues]
              }
      mgr <- newManager defaultManagerSettings
      Warp.testWithApplication (pure (arbiterApp cfg)) $ \port -> do
        let srv = Server port mgr
        -- Hold the key from queue "m_b" only, and leave it in flight.
        _ <-
          post srv "/api/v1/queues/m_b/jobs" $
            object ["payload" .= object [], "concurrency" .= object ["prefix" .= ("cc" :: Text), "suffix" .= ("acme" :: Text)]]
        (_, cb) <- post srv "/api/v1/queues/m_b/claim" (object ["maxJobs" .= (1 :: Int)])
        length (claimed cb) `shouldBe` 1
        inFlight connStr `shouldReturn` [1]

        -- The other queue's pool runs its maintenance pass, then drains.
        drainMaintenance connStr maintSchema maintQueues (ClaimAdmission False True) "m_a" [] $
          threadDelay 3_000_000

        inFlight connStr `shouldReturn` [1]
  where
    maintQueues = ["m_a", "m_b"]
    inFlight connStr =
      bracket (PG.connectPostgreSQL connStr) PG.close $ \conn ->
        map PG.fromOnly
          <$> PG.query_
            conn
            ( fromString
                ("SELECT in_flight FROM " <> T.unpack maintSchema <> ".arbiter_concurrency WHERE concurrency_key = 'cc:acme'")
            )
          :: IO [Int]

-- | A schedule declared only in TOML reaches the database: the pool's scheduler fires
-- the current tick at startup and the job carries the minute it fired for.
cronSpec :: Spec
cronSpec =
  describe "cron (declared in TOML)" $
    it "fires the current tick at startup and stamps it into the payload" $ do
      connStr <- getTestConnectionString
      resetSchema connStr cronSchema
      _ <-
        runMigrationsTrackedForTables
          connStr
          cronSchema
          [("c_a", allTableAdmission)]
          defaultMigrationConfig
          (AdmissionSeeds [] [] Unlogged)
      withConfig
        [ "[database]"
        , "url = \"x\""
        , "[[queue]]"
        , "name = \"c_a\""
        , "[[queue.cron]]"
        , "name = \"c_ticker\""
        , "expression = \"* * * * *\""
        , "overlap = \"allow\""
        , "payload = { kind = \"tick\" }"
        ]
        $ \r -> do
          cfg <- either (fail . ("config: " <>)) pure r
          crons <- either (fail . ("cron: " <>)) pure (concat <$> traverse queueCronJobs (cfgQueues cfg))
          drainMaintenance connStr cronSchema ["c_a"] (ClaimAdmission False False) "c_a" crons $
            waitUntil 10_000 (not . null <$> cronPayloads connStr)

          rows <- cronPayloads connStr
          case rows of
            [Object o] -> do
              KM.lookup "kind" o `shouldBe` Just (String "tick")
              KM.lookup "tick" o `shouldSatisfy` isJust
            _ -> expectationFailure ("expected exactly one cron job, got " <> show (length rows))
  where
    cronPayloads connStr =
      bracket (PG.connectPostgreSQL connStr) PG.close $ \conn ->
        map PG.fromOnly
          <$> PG.query_ conn (fromString ("SELECT payload FROM " <> T.unpack cronSchema <> ".c_a"))
          :: IO [Value]

-- | Run a queue's background loops until @settle@ says they have done their pass, then drain them.
drainMaintenance :: ByteString -> Text -> [Text] -> ClaimAdmission -> Text -> [CronJob Value] -> IO () -> IO ()
drainMaintenance connStr sch queues adm queue crons settle = do
  stateVar <- newWorkerState
  withHasqlPool connStr (defaultPoolConfig {poolSize = queuePoolSize Nothing crons}) $ \pool -> do
    let poolEnv =
          PoolEnv
            { peConnStr = connStr
            , pePool = pool
            , peSchema = sch
            , peQueues = queues
            , peAdmission = adm
            , pePrepared = True
            }
    maintenance <- genericPool poolEnv queue (workerSettings Nothing) crons Nothing
    let serveEnv = ServeEnv {stateVar = stateVar, meters = Nothing, logs = Nothing}
    loops <- async $ poolRun maintenance serveEnv
    settle
    signalShutdown stateVar
    timeout 5_000_000 (wait loops) `shouldNotReturn` Nothing

configSpec :: Spec
configSpec = do
  it "rejects duplicate queue names" $
    withConfig
      ["[database]", "url = \"x\"", "[[queue]]", "name = \"q\"", "[[queue]]", "name = \"q\""]
      (`shouldSatisfy` failsWith "duplicate")

  it "interpolates ${ENV} references" $ do
    setEnv "ARB_TEST_DBURL" "host=example dbname=z"
    withConfig
      ["[database]", "url = \"${ARB_TEST_DBURL}\"", "[[queue]]", "name = \"q\""]
      (\r -> fmap (dbUrl . cfgDatabase) r `shouldBe` Right "host=example dbname=z")

  -- A misspelled key that is silently ignored is how a webhook ships unsigned.
  it "rejects an unrecognized key" $
    withConfig
      [ "[database]"
      , "url = \"x\""
      , "[[queue]]"
      , "name = \"q\""
      , "[queue.webhook]"
      , "url = \"http://h\""
      , "secret_fil = \"/run/s\""
      ]
      (`shouldSatisfy` failsWith "secret_fil")

  it "accepts a [queue.worker] block" $
    withConfig
      (queueWorker ["visibility_timeout = 120.0", "job_heartbeat_interval = 20.0", "jitter = \"full\""])
      (`shouldSatisfy` isRight)

  it "rejects a job heartbeat that cannot outpace the visibility timeout" $
    withConfig
      (queueWorker ["visibility_timeout = 20.0", "job_heartbeat_interval = 30.0"])
      (`shouldSatisfy` failsWith "job_heartbeat_interval")

  it "rejects a worker heartbeat that cannot outpace the stale threshold" $
    withConfig
      (queueWorker ["worker_heartbeat_interval = 300.0", "worker_stale_threshold = 60.0"])
      (`shouldSatisfy` failsWith "worker_stale_threshold")

  it "rejects an unknown jitter" $
    withConfig (queueWorker ["jitter = \"some\""]) (`shouldSatisfy` failsWith "jitter")

  it "rejects a backoff key that does not apply to the strategy" $
    withConfig
      (queueWorker ["[queue.worker.backoff]", "strategy = \"linear\"", "base = 3.0"])
      (`shouldSatisfy` failsWith "does not apply")

  -- A cap of 0 makes every retry immediately visible, so a failing job burns its
  -- whole attempt budget in milliseconds.
  it "rejects a backoff cap of 0 rather than retrying with no delay" $
    withConfig
      (queueWorker ["[queue.worker.backoff]", "cap = 0.0"])
      (`shouldSatisfy` failsWith "backoff.cap")

  it "rejects a constant backoff delay of 0" $
    withConfig
      (queueWorker ["[queue.worker.backoff]", "strategy = \"constant\"", "delay = 0.0"])
      (`shouldSatisfy` failsWith "backoff.delay")

  -- The policy row's limit is a positive 32-bit integer, and 0 would abort the migration
  -- at boot with a bare constraint violation.
  it "rejects a concurrency limit of 0" $
    withConfig
      (queueWith ["[[concurrency]]", "prefix = \"cc\"", "limit = 0"])
      (`shouldSatisfy` failsWith "concurrency \"cc\".limit")

  it "rejects a concurrency limit past the 32-bit bound rather than wrapping it" $
    withConfig
      (queueWith ["[[concurrency]]", "prefix = \"cc\"", "limit = 4294967297"])
      (`shouldSatisfy` failsWith "concurrency \"cc\".limit")

  it "rejects a rate-limit interval of 0" $
    withConfig
      (queueWith ["[[ratelimit]]", "prefix = \"rl\"", "max = 5.0", "refill = 1.0", "interval = 0.0"])
      (`shouldSatisfy` failsWith "ratelimit \"rl\".interval")

  it "rejects workers = 0 rather than quietly running one" $
    withConfig
      [ "[database]"
      , "url = \"x\""
      , "[[queue]]"
      , "name = \"q\""
      , "[queue.webhook]"
      , "url = \"https://h.example.com\""
      , "workers = 0"
      ]
      (`shouldSatisfy` failsWith "webhook.workers")

  it "rejects workers outside a [queue.webhook], where it would do nothing" $
    withConfig
      ["[database]", "url = \"x\"", "[[queue]]", "name = \"q\"", "workers = 4"]
      (`shouldSatisfy` failsWith "workers")

  it "interpolates ${ENV} in the schema, not just the url" $ do
    setEnv "ARB_TEST_SCHEMA" "tenant_7"
    withConfig
      ["[database]", "url = \"x\"", "schema = \"${ARB_TEST_SCHEMA}\"", "[[queue]]", "name = \"q\""]
      (\r -> fmap (dbSchema . cfgDatabase) r `shouldBe` Right (Just "tenant_7"))

  -- 0 is an instant timeout, not an unlimited one, so every delivery would fail.
  it "rejects a webhook timeout_secs of 0" $
    withConfig
      (queueWebhook ["url = \"https://h.example.com/j\"", "timeout_secs = 0"])
      (`shouldSatisfy` failsWith "timeout_secs")

  -- Otherwise parseRequest throws from the pool thread, with the server already up.
  it "rejects a webhook url with no scheme" $
    withConfig
      (queueWebhook ["url = \"h.example.com/j\""])
      (`shouldSatisfy` failsWith "url")

  it "accepts a [[queue.cron]] block" $
    withConfig (queueCron ["overlap = \"allow\"", "backfill = 3600.0", "payload = { kind = \"digest\" }"]) $ \r ->
      fmap (map cnName . concatMap qCron . cfgQueues) r `shouldBe` Right ["nightly"]

  -- Otherwise the schedule is only rejected at the first tick, hours after boot.
  it "rejects an invalid cron expression at load" $
    withConfig
      (queueWith ["[[queue.cron]]", "name = \"n\"", "expression = \"not a cron\""])
      (`shouldSatisfy` failsWith "cron \"n\"")

  it "rejects an unknown cron timezone" $
    withConfig
      (queueCron ["timezone = \"Mars/Olympus\""])
      (`shouldSatisfy` failsWith "Unknown timezone")

  it "rejects an unknown overlap policy" $
    withConfig (queueCron ["overlap = \"sometimes\""]) (`shouldSatisfy` failsWith "cron.overlap")

  -- A schedule row is keyed by name alone, so a collision would silently share state.
  it "rejects two schedules sharing a name across queues" $
    withConfig
      ( queueCron []
          <> ["[[queue]]", "name = \"other\"", "[[queue.cron]]", "name = \"nightly\"", "expression = \"0 3 * * *\""]
      )
      (`shouldSatisfy` failsWith "duplicate cron name")

  it "injects the tick time into the configured payload" $
    withConfig (queueCron ["payload = { kind = \"digest\" }"]) $ \r -> do
      cfg <- either (fail . ("config: " <>)) pure r
      crons <- either (fail . ("cron: " <>)) pure (concat <$> traverse queueCronJobs (cfgQueues cfg))
      let tick = UTCTime (fromGregorian 2026 7 13) (secondsToDiffTime (3 * 3600))
      case crons of
        [c] ->
          Job.payload (builder c Live tick)
            `shouldBe` object ["kind" .= ("digest" :: Text), "tick" .= ("2026-07-13T03:00:00Z" :: Text)]
        _ -> expectationFailure "expected exactly one schedule"
  where
    failsWith needle = either (needle `isInfixOf`) (const False)
    queueWith ls = ["[database]", "url = \"x\"", "[[queue]]", "name = \"q\""] <> ls
    queueWorker ls = queueWith ("[queue.worker]" : ls)
    queueWebhook ls = queueWith ("[queue.webhook]" : ls)
    queueCron ls = queueWith (["[[queue.cron]]", "name = \"nightly\"", "expression = \"0 3 * * *\""] <> ls)

-- | Write a TOML config to a temp file, load it, and hand the result to @k@.
withConfig :: [String] -> (Either String Config -> Expectation) -> Expectation
withConfig ls k = writeSystemTempFile "arbiter.toml" (unlines ls) >>= loadConfig >>= k

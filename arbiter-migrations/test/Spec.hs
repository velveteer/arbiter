{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}

-- | Golden tests guarding migration checksums against in-place edits.
--
-- @postgresql-migration@ records a checksum per migration name. Editing an
-- already-shipped migration changes that checksum and every deployed database
-- rejects the run, so migrations must be add-only / rename-on-change. Each
-- migration's SQL body is pinned to its own golden file. A body change fails the
-- corresponding test with a diff. Accept an intentional add or rename with
-- @cabal test arbiter-migrations-tests --test-options=--accept@.
module Main (main) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Core.RateLimit.Schema (PolicyRow (..), arbiterRateLimitsTableName)
import Arbiter.Core.RateLimit.Spec (Durability (Durable))
import Control.Exception (bracket)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Lazy qualified as LBS
import Data.Int (Int64)
import Data.Maybe (listToMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Migration (MigrationCommand (..))
import GHC.Generics (Generic)
import System.Environment (lookupEnv)
import System.FilePath ((<.>), (</>))
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.Golden (goldenVsString)
import Test.Tasty.HUnit (testCase, (@?=))

import Arbiter.Migrations
  ( MigrationConfig (..)
  , MigrationResult (..)
  , allTableAdmission
  , conflictingPolicyPrefixes
  , defaultMigrationConfig
  , jobQueueMigrationsForTable
  , runMigrationsForRegistry
  , schemaLevelMigrations
  , validateRegistryNames
  )

main :: IO ()
main = do
  connStr <- getTestConnectionString
  defaultMain $
    testGroup
      "arbiter-migrations"
      [ testGroup "migration checksums" (map migrationGolden shippedMigrations)
      , testGroup "policy conflict detection" conflictTests
      , testGroup "registry name validation" registryNameTests
      , migrationReconciliationTests connStr
      ]

-- | 'conflictingPolicyPrefixes' flags only a prefix carrying two distinct parameter sets.
conflictTests :: [TestTree]
conflictTests =
  [ testCase "distinct prefixes are not a conflict" $
      conflictingPolicyPrefixes [row "a" 1 1 1, row "b" 2 2 2] @?= []
  , testCase "identical duplicate rows are not a conflict" $
      conflictingPolicyPrefixes [row "a" 1 1 1, row "a" 1 1 1] @?= []
  , testCase "one prefix with two parameter sets is a conflict" $
      conflictingPolicyPrefixes [row "a" 1 1 1, row "a" 2 1 1] @?= ["a"]
  ]
  where
    row p mx rf iv = PolicyRow {prefixId = p, maxTokens = mx, refillAmt = rf, interval = iv}

registryNameTests :: [TestTree]
registryNameTests =
  [ testCase "accepts distinct generated names" $
      validateRegistryNames "arbiter" ["jobs", "other_jobs"] @?= Right ()
  , testCase "rejects generated table collisions" $
      validateRegistryNames "arbiter" ["jobs", "jobs_dlq"]
        @?= Left "Arbiter queue names generate the same PostgreSQL object: jobs_dlq"
  , testCase "rejects queue names too long for generated identifiers" $
      validateRegistryNames "arbiter" ["abcdefghijklmnopqrstuvwxyz0123456789"]
        @?= Left "Arbiter queue name exceeds the 35-byte generated-identifier limit: abcdefghijklmnopqrstuvwxyz0123456789"
  ]

newtype MigrationPayload = MigrationPayload Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype RemovedQueuePayload = RemovedQueuePayload Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type MigrationRegistry = '[Queue "migration_reconciliation_q" MigrationPayload]
type ExpandedMigrationRegistry =
  '[ Queue "migration_reconciliation_q" MigrationPayload
   , Queue "removed_reconciliation_q" RemovedQueuePayload
   ]
type DLQSuffixRegistry = '[Queue "events_dlq" MigrationPayload]

reconciliationSchema :: Text
reconciliationSchema = "arbiter_migration_reconciliation_test"

migrationReconciliationTests :: ByteString -> TestTree
migrationReconciliationTests connStr =
  testGroup
    "migration reconciliation"
    [ testCase "reconciles rate-limit bucket durability" $
        withFreshSchema connStr $ \conn -> do
          migrate durableConfig
          bucketPersistence conn >>= (@?= Just "p")
          migrate defaultMigrationConfig
          bucketPersistence conn >>= (@?= Just "u")
    , testCase "enables, disables, and re-enables optional triggers" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOff
          optionalTriggerCount conn >>= (@?= 0)
          optionalFunctionCount conn >>= (@?= 0)

          migrate triggerOn
          optionalTriggerCount conn >>= (@?= 3)
          optionalFunctionCount conn >>= (@?= 2)
          triggerOids <- optionalTriggerOids conn
          functionOids <- optionalFunctionOids conn

          migrate triggerOn
          optionalTriggerOids conn >>= (@?= triggerOids)
          optionalFunctionOids conn >>= (@?= functionOids)

          migrate triggerOff
          optionalTriggerCount conn >>= (@?= 0)
          optionalFunctionCount conn >>= (@?= 0)

          -- Create migrations are already recorded. Desired-state
          -- reconciliation must restore the objects without editing them.
          migrate triggerOn
          optionalTriggerCount conn >>= (@?= 3)
          optionalFunctionCount conn >>= (@?= 2)
    , testCase "drops streaming triggers for queues removed from the registry" $
        withFreshSchema connStr $ \conn -> do
          runMigrationsForRegistry (Proxy @ExpandedMigrationRegistry) connStr reconciliationSchema triggerOn >>= shouldMigrate
          removedQueueTriggerCount conn >>= (@?= 2)
          removedNotifyObjectCount conn >>= (@?= 2)

          migrate triggerOff
          removedQueueTriggerCount conn >>= (@?= 0)
          removedNotifyObjectCount conn >>= (@?= 0)
          optionalFunctionCount conn >>= (@?= 0)
    , testCase "repairs stale notification objects" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE OR REPLACE FUNCTION arbiter_migration_reconciliation_test.notify_migration_reconciliation_q_created() RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql; DROP TRIGGER migration_reconciliation_q_notify_trigger ON arbiter_migration_reconciliation_test.migration_reconciliation_q; CREATE TRIGGER migration_reconciliation_q_notify_trigger BEFORE INSERT ON arbiter_migration_reconciliation_test.migration_reconciliation_q FOR EACH ROW EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_migration_reconciliation_q_created()"
          notificationObjectsAreCurrent conn >>= (@?= False)
          migrate triggerOn
          notificationObjectsAreCurrent conn >>= (@?= True)
    , testCase "repairs a stale event function body" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE OR REPLACE FUNCTION arbiter_migration_reconciliation_test.notify_job_event() RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
          eventFunctionIsCurrent conn >>= (@?= False)
          migrate triggerOn
          eventFunctionIsCurrent conn >>= (@?= True)
    , testCase "repairs an event trigger with stale arguments" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "DROP TRIGGER notify_job_event_migration_reconciliation_q ON arbiter_migration_reconciliation_test.migration_reconciliation_q; CREATE TRIGGER notify_job_event_migration_reconciliation_q AFTER INSERT OR UPDATE OR DELETE ON arbiter_migration_reconciliation_test.migration_reconciliation_q FOR EACH ROW EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_job_event('wrong_queue', 'true')"
          migrate triggerOn
          definitions <- eventTriggerDefinitionsFor conn "notify_job_event_migration_reconciliation_q"
          length (filter (T.isInfixOf "('migration_reconciliation_q', 'false')") definitions) @?= 1
    , testCase "preserves queue names ending in the DLQ suffix" $
        withFreshSchema connStr $ \conn -> do
          runMigrationsForRegistry (Proxy @DLQSuffixRegistry) connStr reconciliationSchema triggerOn >>= shouldMigrate
          definitions <- eventTriggerDefinitions conn
          length (filter (T.isInfixOf "('events_dlq', 'false')") definitions) @?= 1
          length (filter (T.isInfixOf "('events_dlq', 'true')") definitions) @?= 1
    ]
  where
    durableConfig = defaultMigrationConfig {rateLimitDurability = Durable}
    triggerOff = defaultMigrationConfig {enableNotifications = False, enableEventStreaming = False}
    triggerOn = defaultMigrationConfig {enableNotifications = True, enableEventStreaming = True}
    migrate config = runMigrationsForRegistry (Proxy @MigrationRegistry) connStr reconciliationSchema config >>= shouldMigrate

withFreshSchema :: ByteString -> (PG.Connection -> IO a) -> IO a
withFreshSchema connStr action =
  bracket (PG.connectPostgreSQL connStr) PG.close $ \conn -> do
    _ <- PG.execute_ conn "SET client_min_messages = WARNING"
    _ <- PG.execute_ conn "DROP SCHEMA IF EXISTS arbiter_migration_reconciliation_test CASCADE"
    action conn

shouldMigrate :: MigrationResult String -> IO ()
shouldMigrate MigrationSuccess = pure ()
shouldMigrate (MigrationError err) = fail ("migration failed: " <> err)

optionalTriggerOids :: PG.Connection -> IO [Int64]
optionalTriggerOids conn =
  map PG.fromOnly
    <$> PG.query
      conn
      "SELECT t.oid::bigint FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND t.tgname IN ('migration_reconciliation_q_notify_trigger', 'notify_job_event_migration_reconciliation_q', 'notify_job_event_migration_reconciliation_q_dlq') ORDER BY t.tgname"
      (PG.Only reconciliationSchema)

optionalFunctionOids :: PG.Connection -> IO [Int64]
optionalFunctionOids conn =
  map PG.fromOnly
    <$> PG.query
      conn
      "SELECT p.oid::bigint FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ? AND p.proname IN ('notify_migration_reconciliation_q_created', 'notify_job_event') ORDER BY p.proname"
      (PG.Only reconciliationSchema)

optionalTriggerCount :: PG.Connection -> IO Int64
optionalTriggerCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND t.tgname IN ('migration_reconciliation_q_notify_trigger', 'notify_job_event_migration_reconciliation_q', 'notify_job_event_migration_reconciliation_q_dlq')"
      (PG.Only reconciliationSchema)

notificationObjectsAreCurrent :: PG.Connection -> IO Bool
notificationObjectsAreCurrent conn =
  any PG.fromOnly
    <$> PG.query
      conn
      "SELECT p.prosrc LIKE '%pg_notify(''migration_reconciliation_q_created'', '''')%' AND t.tgtype = 5 FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relname = 'migration_reconciliation_q' AND t.tgname = 'migration_reconciliation_q_notify_trigger'"
      (PG.Only reconciliationSchema)

eventFunctionIsCurrent :: PG.Connection -> IO Bool
eventFunctionIsCurrent conn =
  any PG.fromOnly
    <$> PG.query
      conn
      "SELECT p.prosrc LIKE '%queue_name text := TG_ARGV[0]%' AND p.prosrc LIKE '%is_dlq boolean := TG_ARGV[1]::boolean%' FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ? AND p.proname = 'notify_job_event' AND p.pronargs = 0"
      (PG.Only reconciliationSchema)

eventTriggerDefinitions :: PG.Connection -> IO [Text]
eventTriggerDefinitions conn = do
  first <- eventTriggerDefinitionsFor conn "notify_job_event_events_dlq"
  second <- eventTriggerDefinitionsFor conn "notify_job_event_events_dlq_dlq"
  pure (first <> second)

eventTriggerDefinitionsFor :: PG.Connection -> Text -> IO [Text]
eventTriggerDefinitionsFor conn trigger =
  map PG.fromOnly
    <$> PG.query
      conn
      "SELECT pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND t.tgname = ? ORDER BY t.tgname"
      (reconciliationSchema, trigger)

removedQueueTriggerCount :: PG.Connection -> IO Int64
removedQueueTriggerCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND t.tgname IN ('notify_job_event_removed_reconciliation_q', 'notify_job_event_removed_reconciliation_q_dlq')"
      (PG.Only reconciliationSchema)

removedNotifyObjectCount :: PG.Connection -> IO Int64
removedNotifyObjectCount conn = do
  triggerCount <-
    fromOnlyOne
      <$> PG.query
        conn
        "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND t.tgname = 'removed_reconciliation_q_notify_trigger'"
        (PG.Only reconciliationSchema)
  functionCount <-
    fromOnlyOne
      <$> PG.query
        conn
        "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ? AND p.proname = 'notify_removed_reconciliation_q_created'"
        (PG.Only reconciliationSchema)
  pure (triggerCount + functionCount)

optionalFunctionCount :: PG.Connection -> IO Int64
optionalFunctionCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = ? AND p.proname IN ('notify_migration_reconciliation_q_created', 'notify_job_event')"
      (PG.Only reconciliationSchema)

fromOnlyOne :: [PG.Only Int64] -> Int64
fromOnlyOne = maybe 0 PG.fromOnly . listToMaybe

bucketPersistence :: PG.Connection -> IO (Maybe Text)
bucketPersistence conn =
  fmap (fmap PG.fromOnly . listToMaybe) $
    PG.query
      conn
      "SELECT relpersistence::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relname = ?"
      (reconciliationSchema, arbiterRateLimitsTableName)

getTestConnectionString :: IO ByteString
getTestConnectionString = do
  configured <- lookupEnv "ARBITER_TEST_CONN_STRING"
  pure $ maybe "host=localhost port=5432 user=postgres password=master dbname=postgres" BS8.pack configured

-- | Every shipped migration as @(name, SQL body)@, with all features on so the
-- notify and event-streaming triggers are pinned alongside the core migrations.
-- Covers both the schema-level migrations and the per-table migrations.
shippedMigrations :: [(String, BS.ByteString)]
shippedMigrations =
  [ (name, body)
  | MigrationScript name body <-
      schemaLevelMigrations allFeaturesConfig "arbiter"
        <> jobQueueMigrationsForTable "arbiter" "golden_jobs" allFeaturesConfig allTableAdmission
  ]

allFeaturesConfig :: MigrationConfig
allFeaturesConfig = defaultMigrationConfig {enableEventStreaming = True}

-- | Pin one migration's body to @test/golden/<name>.sql@.
migrationGolden :: (String, BS.ByteString) -> TestTree
migrationGolden (name, body) =
  goldenVsString name ("test" </> "golden" </> name <.> "sql") (pure (LBS.fromStrict body))

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
  , testCase "accepts a queue name at the limit" $
      validateRegistryNames "arbiter" ["abcdefghijklmnopqrstuvwxyz012345678"] @?= Right ()
  , testCase "rejects a queue name owned by a schema-wide table" $
      validateRegistryNames "arbiter" ["arbiter_gates"]
        @?= Left "Arbiter queue name generates a reserved arbiter table: arbiter_gates"
  , testCase "rejects a queue named after the cron table" $
      validateRegistryNames "arbiter" ["cron_schedules"]
        @?= Left "Arbiter queue name generates a reserved arbiter table: cron_schedules"
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

          -- A second run rebuilds nothing, so no queue table is locked.
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
    , testCase "drops notify objects for queues removed from the registry" $
        withFreshSchema connStr $ \conn -> do
          runMigrationsForRegistry (Proxy @ExpandedMigrationRegistry) connStr reconciliationSchema triggerOn >>= shouldMigrate
          removedNotifyObjectCount conn >>= (@?= 2)

          migrate triggerOn
          removedNotifyObjectCount conn >>= (@?= 0)
          optionalTriggerCount conn >>= (@?= 3)
          optionalFunctionCount conn >>= (@?= 2)
    , testCase "drops the notify function of a removed queue whose table is gone" $
        withFreshSchema connStr $ \conn -> do
          runMigrationsForRegistry (Proxy @ExpandedMigrationRegistry) connStr reconciliationSchema triggerOn >>= shouldMigrate
          removedNotifyObjectCount conn >>= (@?= 2)

          _ <-
            PG.execute_
              conn
              "DROP TABLE arbiter_migration_reconciliation_test.removed_reconciliation_q CASCADE"
          migrate triggerOn
          removedNotifyObjectCount conn >>= (@?= 0)
    , testCase "repairs stale notification objects" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE OR REPLACE FUNCTION arbiter_migration_reconciliation_test.notify_migration_reconciliation_q_created() \
              \RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql; \
              \DROP TRIGGER migration_reconciliation_q_notify_trigger \
              \ON arbiter_migration_reconciliation_test.migration_reconciliation_q; \
              \CREATE TRIGGER migration_reconciliation_q_notify_trigger \
              \BEFORE INSERT ON arbiter_migration_reconciliation_test.migration_reconciliation_q \
              \FOR EACH ROW EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_migration_reconciliation_q_created()"
          notificationObjectsAreCurrent conn >>= (@?= False)
          migrate triggerOn
          notificationObjectsAreCurrent conn >>= (@?= True)
    , testCase "repairs a notify trigger missing its transition table" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "DROP TRIGGER migration_reconciliation_q_notify_trigger \
              \ON arbiter_migration_reconciliation_test.migration_reconciliation_q; \
              \CREATE TRIGGER migration_reconciliation_q_notify_trigger \
              \AFTER INSERT ON arbiter_migration_reconciliation_test.migration_reconciliation_q \
              \FOR EACH STATEMENT \
              \EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_migration_reconciliation_q_created()"
          notificationObjectsAreCurrent conn >>= (@?= False)
          migrate triggerOn
          notificationObjectsAreCurrent conn >>= (@?= True)
    , testCase "leaves unmarked notify lookalikes alone" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE TABLE arbiter_migration_reconciliation_test.orders (id bigint); \
              \CREATE FUNCTION arbiter_migration_reconciliation_test.notify_orders_created() \
              \RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql; \
              \CREATE TRIGGER orders_notify_trigger \
              \AFTER INSERT ON arbiter_migration_reconciliation_test.orders \
              \REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT \
              \EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_orders_created()"
          migrate triggerOn
          functionCountNamed conn "notify_orders_created" >>= (@?= 1)
          triggerCountNamed conn "orders_notify_trigger" >>= (@?= 1)

          migrate triggerOff
          functionCountNamed conn "notify_orders_created" >>= (@?= 1)
          triggerCountNamed conn "orders_notify_trigger" >>= (@?= 1)
    , testCase "leaves an unmarked event function alone" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOff
          _ <-
            PG.execute_
              conn
              "CREATE FUNCTION arbiter_migration_reconciliation_test.notify_job_event() \
              \RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
          migrate triggerOff
          functionCountNamed conn "notify_job_event" >>= (@?= 1)
    , testCase "disables streaming with a leftover unmarked event trigger" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE TABLE arbiter_migration_reconciliation_test.legacy_q (id bigint); \
              \CREATE TRIGGER notify_job_event_legacy_q \
              \AFTER INSERT ON arbiter_migration_reconciliation_test.legacy_q \
              \FOR EACH ROW EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_job_event('legacy_q', 'false')"
          migrate triggerOff
          optionalTriggerCount conn >>= (@?= 0)
    , -- Installs predating the marker have an unmarked function with an older body, so
      -- enabling streaming has to replace and stamp it rather than treat it as foreign.
      testCase "adopts an unmarked event function" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOff
          _ <-
            PG.execute_
              conn
              "CREATE FUNCTION arbiter_migration_reconciliation_test.notify_job_event() \
              \RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
          migrate triggerOn
          eventFunctionIsCurrent conn >>= (@?= True)
    , testCase "repairs a stale event function body" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE OR REPLACE FUNCTION arbiter_migration_reconciliation_test.notify_job_event() \
              \RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
          eventFunctionIsCurrent conn >>= (@?= False)
          migrate triggerOn
          eventFunctionIsCurrent conn >>= (@?= True)
    , testCase "repairs an event trigger with stale arguments" $
        withFreshSchema connStr $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "DROP TRIGGER notify_job_event_migration_reconciliation_q \
              \ON arbiter_migration_reconciliation_test.migration_reconciliation_q; \
              \CREATE TRIGGER notify_job_event_migration_reconciliation_q \
              \AFTER INSERT OR UPDATE OR DELETE ON arbiter_migration_reconciliation_test.migration_reconciliation_q \
              \FOR EACH ROW EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_job_event('wrong_queue', 'true')"
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
      ( "SELECT t.oid::bigint "
          <> triggerJoins
          <> "WHERE n.nspname = ? AND t.tgname IN "
          <> optionalTriggerNames
          <> " ORDER BY t.tgname"
      )
      (PG.Only reconciliationSchema)

optionalFunctionOids :: PG.Connection -> IO [Int64]
optionalFunctionOids conn =
  map PG.fromOnly
    <$> PG.query
      conn
      ( "SELECT p.oid::bigint "
          <> functionJoins
          <> "WHERE n.nspname = ? AND p.proname IN "
          <> optionalFunctionNames
          <> " ORDER BY p.proname"
      )
      (PG.Only reconciliationSchema)

optionalTriggerCount :: PG.Connection -> IO Int64
optionalTriggerCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      ("SELECT count(*) " <> triggerJoins <> "WHERE n.nspname = ? AND t.tgname IN " <> optionalTriggerNames)
      (PG.Only reconciliationSchema)

-- | Compares the installed objects against the rendered trigger definition rather
-- than a hand-listed set of catalog attributes, so a clause the reconciler forgot
-- to emit still fails.
notificationObjectsAreCurrent :: PG.Connection -> IO Bool
notificationObjectsAreCurrent conn = do
  rows <-
    PG.query
      conn
      "SELECT pg_get_triggerdef(t.oid), p.prosrc \
      \FROM pg_trigger t \
      \JOIN pg_proc p ON p.oid = t.tgfoid \
      \JOIN pg_class c ON c.oid = t.tgrelid \
      \JOIN pg_namespace n ON n.oid = c.relnamespace \
      \WHERE n.nspname = ? AND c.relname = 'migration_reconciliation_q' \
      \AND t.tgname = 'migration_reconciliation_q_notify_trigger'"
      (PG.Only reconciliationSchema)
  pure $ case rows of
    [(definition, body)] ->
      all (`T.isInfixOf` definition) ["AFTER INSERT", "REFERENCING NEW TABLE AS new_table", "FOR EACH STATEMENT"]
        && T.isInfixOf "pg_notify('migration_reconciliation_q_created', '')" body
    _ -> False

eventFunctionIsCurrent :: PG.Connection -> IO Bool
eventFunctionIsCurrent conn =
  any PG.fromOnly
    <$> PG.query
      conn
      ( "SELECT p.prosrc LIKE '%queue_name text := TG_ARGV[0]%' \
        \AND p.prosrc LIKE '%is_dlq boolean := TG_ARGV[1]::boolean%' "
          <> functionJoins
          <> "WHERE n.nspname = ? AND p.proname = 'notify_job_event' AND p.pronargs = 0"
      )
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
      ("SELECT pg_get_triggerdef(t.oid) " <> triggerJoins <> "WHERE n.nspname = ? AND t.tgname = ? ORDER BY t.tgname")
      (reconciliationSchema, trigger)

removedQueueTriggerCount :: PG.Connection -> IO Int64
removedQueueTriggerCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      ( "SELECT count(*) "
          <> triggerJoins
          <> "WHERE n.nspname = ? AND t.tgname IN \
             \('notify_job_event_removed_reconciliation_q', 'notify_job_event_removed_reconciliation_q_dlq')"
      )
      (PG.Only reconciliationSchema)

removedNotifyObjectCount :: PG.Connection -> IO Int64
removedNotifyObjectCount conn = do
  triggerCount <-
    fromOnlyOne
      <$> PG.query
        conn
        ( "SELECT count(*) "
            <> triggerJoins
            <> "WHERE n.nspname = ? AND t.tgname = 'removed_reconciliation_q_notify_trigger'"
        )
        (PG.Only reconciliationSchema)
  functionCount <-
    fromOnlyOne
      <$> PG.query
        conn
        ( "SELECT count(*) "
            <> functionJoins
            <> "WHERE n.nspname = ? AND p.proname = 'notify_removed_reconciliation_q_created'"
        )
        (PG.Only reconciliationSchema)
  pure (triggerCount + functionCount)

triggerCountNamed :: PG.Connection -> Text -> IO Int64
triggerCountNamed conn name =
  fromOnlyOne
    <$> PG.query
      conn
      ("SELECT count(*) " <> triggerJoins <> "WHERE n.nspname = ? AND t.tgname = ?")
      (reconciliationSchema, name)

functionCountNamed :: PG.Connection -> Text -> IO Int64
functionCountNamed conn name =
  fromOnlyOne
    <$> PG.query
      conn
      ("SELECT count(*) " <> functionJoins <> "WHERE n.nspname = ? AND p.proname = ?")
      (reconciliationSchema, name)

optionalFunctionCount :: PG.Connection -> IO Int64
optionalFunctionCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      ("SELECT count(*) " <> functionJoins <> "WHERE n.nspname = ? AND p.proname IN " <> optionalFunctionNames)
      (PG.Only reconciliationSchema)

-- | Catalog joins shared by the trigger probes.
triggerJoins :: PG.Query
triggerJoins = "FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace "

-- | Catalog joins shared by the function probes.
functionJoins :: PG.Query
functionJoins = "FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace "

-- | The triggers the reconciliation registry installs when both options are on.
optionalTriggerNames :: PG.Query
optionalTriggerNames =
  "('migration_reconciliation_q_notify_trigger', \
  \'notify_job_event_migration_reconciliation_q', \
  \'notify_job_event_migration_reconciliation_q_dlq')"

-- | The functions the reconciliation registry installs when both options are on.
optionalFunctionNames :: PG.Query
optionalFunctionNames = "('notify_migration_reconciliation_q_created', 'notify_job_event')"

fromOnlyOne :: [PG.Only Int64] -> Int64
fromOnlyOne = maybe 0 PG.fromOnly . listToMaybe

bucketPersistence :: PG.Connection -> IO (Maybe Text)
bucketPersistence conn =
  fmap (fmap PG.fromOnly . listToMaybe) $
    PG.query
      conn
      "SELECT relpersistence::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
      \WHERE n.nspname = ? AND c.relname = ?"
      (reconciliationSchema, arbiterRateLimitsTableName)

getTestConnectionString :: IO ByteString
getTestConnectionString = do
  configured <- lookupEnv "ARBITER_TEST_CONN_STRING"
  pure $ maybe "host=localhost port=5432 user=postgres password=master dbname=postgres" BS8.pack configured

-- | Every shipped migration as @(name, SQL body)@, covering both the schema-level
-- migrations and the per-table migrations.
shippedMigrations :: [(String, BS.ByteString)]
shippedMigrations =
  [ (name, body)
  | MigrationScript name body <-
      schemaLevelMigrations "arbiter"
        <> jobQueueMigrationsForTable "arbiter" "golden_jobs" allTableAdmission
  ]

-- | Pin one migration's body to @test/golden/<name>.sql@.
migrationGolden :: (String, BS.ByteString) -> TestTree
migrationGolden (name, body) =
  goldenVsString name ("test" </> "golden" </> name <.> "sql") (pure (LBS.fromStrict body))

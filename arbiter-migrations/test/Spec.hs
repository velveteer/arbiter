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
  )

main :: IO ()
main = do
  connStr <- getTestConnectionString
  defaultMain $
    testGroup
      "arbiter-migrations"
      [ testGroup "migration checksums" (map migrationGolden shippedMigrations)
      , testGroup "policy conflict detection" conflictTests
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

newtype MigrationPayload = MigrationPayload Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type MigrationRegistry = '[Queue "migration_reconciliation_q" MigrationPayload]

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

          migrate triggerOff
          optionalTriggerCount conn >>= (@?= 0)
          optionalFunctionCount conn >>= (@?= 0)

          -- Create migrations are already recorded. Desired-state
          -- reconciliation must restore the objects without editing them.
          migrate triggerOn
          optionalTriggerCount conn >>= (@?= 3)
          optionalFunctionCount conn >>= (@?= 2)
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

optionalTriggerCount :: PG.Connection -> IO Int64
optionalTriggerCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid = t.tgrelid JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND t.tgname IN ('migration_reconciliation_q_notify_trigger', 'notify_job_event_migration_reconciliation_q', 'notify_job_event_migration_reconciliation_q_dlq')"
      (PG.Only reconciliationSchema)

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

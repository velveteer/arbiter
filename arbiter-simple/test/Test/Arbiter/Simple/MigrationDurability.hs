{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Test.Arbiter.Simple.MigrationDurability (spec) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitsTableName)
import Arbiter.Migrations (MigrationConfig (..), MigrationResult (..), defaultMigrationConfig, runMigrationsForRegistry)
import Arbiter.RateLimit (Durability (..))
import Control.Exception (bracket)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Maybe (listToMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Test.Hspec

newtype DurPayload = DurPayload Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type DurReg = '[Queue "durability_q" DurPayload]

testSchema :: Text
testSchema = "arbiter_simple_durability_test"

spec :: ByteString -> Spec
spec connStr =
  it "reconciles the bucket table to the configured durability" $
    bracket (PG.connectPostgreSQL connStr) PG.close $ \conn -> do
      _ <- PG.execute_ conn "SET client_min_messages = WARNING"
      _ <- PG.execute_ conn "DROP SCHEMA IF EXISTS arbiter_simple_durability_test CASCADE"
      runMigrationsForRegistry (Proxy @DurReg) connStr testSchema durableConfig >>= shouldMigrate
      bucketPersistence conn `shouldReturn` Just "p"
      -- A re-run with the default config reconciles it back.
      runMigrationsForRegistry (Proxy @DurReg) connStr testSchema defaultMigrationConfig >>= shouldMigrate
      bucketPersistence conn `shouldReturn` Just "u"
  where
    durableConfig = defaultMigrationConfig {rateLimitDurability = Durable}

shouldMigrate :: MigrationResult String -> IO ()
shouldMigrate MigrationSuccess = pure ()
shouldMigrate (MigrationError e) = expectationFailure ("migration failed: " <> e)

bucketPersistence :: PG.Connection -> IO (Maybe Text)
bucketPersistence conn =
  fmap (fmap PG.fromOnly . listToMaybe) $
    PG.query
      conn
      "SELECT relpersistence::text FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = ? AND c.relname = ?"
      (testSchema, arbiterRateLimitsTableName)

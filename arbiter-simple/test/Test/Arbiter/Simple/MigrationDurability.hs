{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications #-}

module Test.Arbiter.Simple.MigrationDurability (spec) where

import Arbiter.Core.RateLimit.Schema (arbiterRateLimitsTableName)
import Arbiter.Migrations (MigrationResult (..), defaultMigrationConfig, runMigrationsForRegistry)
import Arbiter.RateLimit (Durability (..), RateLimitDurability (..))
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

-- Two registries over the same schema, distinguished only so each can declare
-- its own durability. The second relies on the default (Unlogged).
type DurableReg = '[ '("durability_q", DurPayload)]

type ResetReg = '[ '("durability_q2", DurPayload)]

instance RateLimitDurability DurableReg where
  rateLimitDurability _ = Durable

testSchema :: Text
testSchema = "arbiter_simple_durability_test"

spec :: ByteString -> Spec
spec connStr =
  it "reconciles the bucket table to the registry's declared durability" $
    bracket (PG.connectPostgreSQL connStr) PG.close $ \conn -> do
      _ <- PG.execute_ conn "SET client_min_messages = WARNING"
      _ <- PG.execute_ conn "DROP SCHEMA IF EXISTS arbiter_simple_durability_test CASCADE"
      runMigrationsForRegistry (Proxy @DurableReg) connStr testSchema defaultMigrationConfig >>= shouldMigrate
      bucketPersistence conn `shouldReturn` Just "p"
      -- A re-run with a registry declaring the default reconciles it back.
      runMigrationsForRegistry (Proxy @ResetReg) connStr testSchema defaultMigrationConfig >>= shouldMigrate
      bucketPersistence conn `shouldReturn` Just "u"

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

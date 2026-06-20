{-# LANGUAGE OverloadedStrings #-}

-- | Golden tests guarding migration checksums against in-place edits.
--
-- @postgresql-migration@ records a checksum per migration name. Editing an
-- already-shipped migration changes that checksum and every deployed database
-- rejects the run, so migrations must be add-only / rename-on-change. Each
-- migration's SQL body is pinned to its own golden file. A body change fails the
-- corresponding test with a diff. Accept an intentional add or rename with
-- @cabal test arbiter-migrations-tests --test-options=--accept@.
module Main (main) where

import Arbiter.Core.RateLimit.Schema (PolicyRow (..))
import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as LBS
import Database.PostgreSQL.Simple.Migration (MigrationCommand (..))
import System.FilePath ((<.>), (</>))
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.Golden (goldenVsString)
import Test.Tasty.HUnit (testCase, (@?=))

import Arbiter.Migrations
  ( MigrationConfig (..)
  , allTableAdmission
  , conflictingPolicyPrefixes
  , defaultMigrationConfig
  , jobQueueMigrationsForTable
  , schemaLevelMigrations
  )

main :: IO ()
main =
  defaultMain $
    testGroup
      "arbiter-migrations"
      [ testGroup "migration checksums" (map migrationGolden shippedMigrations)
      , testGroup "policy conflict detection" conflictTests
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

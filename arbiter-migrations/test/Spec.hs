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

import Data.ByteString qualified as BS
import Data.ByteString.Lazy qualified as LBS
import Database.PostgreSQL.Simple.Migration (MigrationCommand (..))
import System.FilePath ((<.>), (</>))
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.Golden (goldenVsString)

import Arbiter.Migrations (MigrationConfig (..), defaultMigrationConfig, jobQueueMigrationsForTable)

main :: IO ()
main =
  defaultMain $
    testGroup "migration checksums" (map migrationGolden shippedMigrations)

-- | Every per-table migration as @(name, SQL body)@, with all features on so the
-- notify and event-streaming triggers are pinned alongside the core migrations.
shippedMigrations :: [(String, BS.ByteString)]
shippedMigrations =
  [ (name, body)
  | MigrationScript name body <- jobQueueMigrationsForTable "arbiter" "golden_jobs" allFeaturesConfig
  ]

allFeaturesConfig :: MigrationConfig
allFeaturesConfig = defaultMigrationConfig {enableEventStreaming = True}

-- | Pin one migration's body to @test/golden/<name>.sql@.
migrationGolden :: (String, BS.ByteString) -> TestTree
migrationGolden (name, body) =
  goldenVsString name ("test" </> "golden" </> name <.> "sql") (pure (LBS.fromStrict body))

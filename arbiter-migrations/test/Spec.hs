{-# LANGUAGE OverloadedStrings #-}

-- | Holds every migration body to the bytes first recorded for it under @test\/golden@.
-- Migrations are add-only: an edited body changes its checksum, and every deployed
-- database then refuses to start.
module Main (main) where

import Arbiter.Core.RateLimit.Schema (PolicyRow (..))
import Control.Monad (unless)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8 (unpack)
import Data.Proxy (Proxy (..))
import Database.PostgreSQL.Simple.Migration (MigrationCommand (..))
import System.Directory (doesFileExist)
import System.FilePath ((<.>), (</>))
import Test.Tasty
  ( TestTree
  , askOption
  , defaultIngredients
  , defaultMainWithIngredients
  , includingOptions
  , testGroup
  )
import Test.Tasty.HUnit (assertFailure, testCase, (@?=))
import Test.Tasty.Options (IsOption (..), OptionDescription (..), flagCLParser, safeReadBool)

import Arbiter.Migrations
  ( MigrationConfig (..)
  , allTableAdmission
  , conflictingPolicyPrefixes
  , defaultMigrationConfig
  , jobQueueMigrationsForTable
  , schemaLevelMigrations
  )

-- | @--accept@ records a migration that has no golden file yet.
newtype Accept = Accept Bool

instance IsOption Accept where
  defaultValue = Accept False
  parseValue = fmap Accept . safeReadBool
  optionName = pure "accept"
  optionHelp = pure "Record the body of any migration that has no golden file yet"
  optionCLParser = flagCLParser Nothing (Accept True)

main :: IO ()
main =
  defaultMainWithIngredients (includingOptions [Option (Proxy :: Proxy Accept)] : defaultIngredients) $
    askOption $ \accept ->
      testGroup
        "arbiter-migrations"
        [ testGroup "migration bodies" (map (recordedBody accept) shippedMigrations)
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

-- | Compare one migration's body to its recording, or record it under @--accept@.
recordedBody :: Accept -> (String, BS.ByteString) -> TestTree
recordedBody (Accept accept) (name, body) = testCase name $ do
  let path = "test" </> "golden" </> name <.> "sql"
  recorded <- doesFileExist path
  case (recorded, accept) of
    (True, _) -> do
      applied <- BS.readFile path
      unless (applied == body) $ assertFailure (bodyChanged name applied body)
    (False, True) -> BS.writeFile path body
    (False, False) ->
      assertFailure ("migration " <> name <> " has no golden file. Review its body and re-run with --accept")

-- | Why a changed body is never the answer, and what to do instead.
bodyChanged :: String -> BS.ByteString -> BS.ByteString -> String
bodyChanged name applied body =
  unlines
    [ name <> " has already been applied to deployed databases, and its body changed."
    , "Every such database would reject the upgrade with a checksum mismatch."
    , "Ship the change as a new migration instead of editing this one."
    , ""
    , "recorded:"
    , BS8.unpack applied
    , "generated:"
    , BS8.unpack body
    ]

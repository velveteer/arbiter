{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Test.Setup
  ( SetupConfig (..)
  , defaultSetupConfig
  , setupDDL
  , setupDDLWithNotify
  , cleanupData
  , execute_
  , execStatement
  , execQuery
  , setupOnce
  , addQueueTable
  , disableNoticeReporting
  , createSharedPool
  , truncateToMicros
  , seedConcurrencyPoolSQL
  , drainWith
  ) where

import Arbiter.Core.Codec (RowCodec)
import Arbiter.Core.Concurrency.Schema qualified as CC
import Arbiter.Core.Concurrency.Spec (ConcurrencyPolicy (..))
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.MonadArbiter (MonadArbiter, Params)
import Arbiter.Core.MonadArbiter qualified as MA
import Arbiter.Core.RateLimit.Schema qualified as RL
import Arbiter.Core.SchemaTables (allSchemaTables)
import Arbiter.Core.SqlLiterals (textLiteral)
import Arbiter.Migrations
  ( MigrationConfig (..)
  , allTableAdmission
  , defaultMigrationConfig
  , jobQueueMigrationsForTable
  , schemaLevelMigrations
  )
import Control.Concurrent (threadDelay)
import Control.Exception (throwIO, try)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime (..), picosecondsToDiffTime)
import Database.PostgreSQL.LibPQ qualified as LibPQ
import Database.PostgreSQL.Simple (Connection, SqlError (..), close, connectPostgreSQL, execute)
import Database.PostgreSQL.Simple.Internal qualified as PGS
import Database.PostgreSQL.Simple.Migration (MigrationCommand (..))
import Database.PostgreSQL.Simple.Types (Query (..))

-- | Configuration for test setup
data SetupConfig = SetupConfig
  { setupEnableNotifications :: Bool
  -- ^ Whether to create LISTEN/NOTIFY triggers
  , setupEnableRankingIndexes :: Bool
  -- ^ Whether to create ranking indexes for optimized claim queries
  }
  deriving stock (Eq, Show)

-- | Default test setup configuration
defaultSetupConfig :: SetupConfig
defaultSetupConfig =
  SetupConfig
    { setupEnableNotifications = True
    , setupEnableRankingIndexes = True
    }

setupDDL :: Text -> Text -> Connection -> IO ()
setupDDL = setupDDLWithConfig defaultSetupConfig {setupEnableNotifications = False}

setupDDLWithNotify :: Text -> Text -> Connection -> IO ()
setupDDLWithNotify = setupDDLWithConfig defaultSetupConfig

-- | Rebuild the schema by running the shipped migration scripts, so tests exercise
-- exactly the DDL that deploys do.
setupDDLWithConfig :: SetupConfig -> Text -> Text -> Connection -> IO ()
setupDDLWithConfig config schemaName tableName conn = do
  void $ execute_ conn $ "DROP SCHEMA IF EXISTS " <> schemaName <> " CASCADE"
  void $ execute_ conn $ Schema.createSchemaSQL schemaName
  traverse_ runScript $
    schemaLevelMigrations migrationConfig schemaName
      <> jobQueueMigrationsForTable schemaName tableName migrationConfig allTableAdmission
  where
    migrationConfig =
      defaultMigrationConfig
        { enableNotifications = setupEnableNotifications config
        , enableEventStreaming = False
        }
    skipped
      | setupEnableRankingIndexes config = []
      | otherwise = map ((T.unpack tableName <> "-") <>) ["create-group-key-index", "migrate-ungrouped-ready-split-indexes"]
    runScript (MigrationScript name sql)
      | name `elem` skipped = pure ()
      | otherwise = void $ execute conn (Query sql) ()
    runScript _ = pure ()

cleanupData :: Text -> Text -> Connection -> IO ()
cleanupData schemaName tableName conn = do
  execute_ conn "SET client_min_messages = WARNING"
  -- A draining worker pool may still hold locks on the job and groups tables in
  -- the opposite order from this TRUNCATE, so bound the wait and retry on a
  -- deadlock or lock timeout instead of failing the test.
  execute_ conn "SET lock_timeout = '5s'"
  -- Rate-limit policies are seeded once per suite, not per test.
  let truncated = filter (/= RL.arbiterRateLimitPoliciesTableName) (allSchemaTables [tableName])
      truncateSql =
        "TRUNCATE "
          <> T.intercalate ", " (map (Schema.qualifiedTable schemaName) truncated)
          <> " CASCADE"
      go n = do
        r <- try (execute_ conn truncateSql) :: IO (Either SqlError ())
        case r of
          Right () -> pure ()
          -- 40P01 deadlock_detected, 55P03 lock_not_available
          Left e
            | sqlState e `elem` ["40P01", "55P03"] && n > (0 :: Int) ->
                threadDelay 100_000 >> go (n - 1)
            | otherwise -> throwIO e
  go 10
  execute_ conn "RESET lock_timeout"
  execute_ conn "SET client_min_messages = NOTICE"

execute_ :: Connection -> Text -> IO ()
execute_ conn sql = void $ execute conn (fromString (T.unpack sql) :: Query) ()

-- | Run a pre-rendered statement with positional parameters (test setup only).
execStatement :: (MonadArbiter m) => Text -> Params -> m Int64
execStatement sql params = MA.executeStatement (MA.Query sql params (pure ()))

-- | Run a pre-rendered query with positional parameters and a decoder (test setup only).
execQuery :: (MonadArbiter m) => Text -> Params -> RowCodec a -> m [a]
execQuery sql params codec = MA.executeQuery (MA.Query sql params codec)

setupOnce :: ByteString -> Text -> Text -> Bool -> IO ()
setupOnce connStr schemaName tableName withNotify = do
  conn <- connectPostgreSQL connStr
  disableNoticeReporting conn
  let config = defaultSetupConfig {setupEnableNotifications = withNotify}
  setupDDLWithConfig config schemaName tableName conn
  close conn

-- | Add one more job-queue table to an existing schema without dropping it, for
-- tests that exercise several queues in one schema.
addQueueTable :: ByteString -> Text -> Text -> Bool -> IO ()
addQueueTable connStr schemaName tableName withNotify = do
  conn <- connectPostgreSQL connStr
  disableNoticeReporting conn
  let config =
        defaultMigrationConfig
          { enableNotifications = withNotify
          , enableEventStreaming = False
          }
  traverse_ (runScript conn) (jobQueueMigrationsForTable schemaName tableName config allTableAdmission)
  close conn
  where
    runScript conn (MigrationScript _ sql) = void $ execute conn (Query sql) ()
    runScript _ _ = pure ()

disableNoticeReporting :: Connection -> IO ()
disableNoticeReporting conn =
  PGS.withConnection conn LibPQ.disableNoticeReporting

-- | A single-stripe pool of 5 connections for tests.
createSharedPool :: ByteString -> IO (Pool Connection)
createSharedPool connStr =
  newPool $ setNumStripes (Just 1) $ defaultPoolConfig (connectPostgreSQL connStr) close 60 5

-- | Seed a concurrency pool's default limit and clear any override, as SQL statements.
-- Callers run them in their own context (monad or raw connection).
seedConcurrencyPoolSQL :: Text -> Text -> Int32 -> [Text]
seedConcurrencyPoolSQL schema prefix lim =
  [ CC.upsertConcurrencyPolicyRowSQL schema (ConcurrencyPolicy prefix lim)
  , "UPDATE "
      <> CC.arbiterConcurrencyPoliciesTable schema
      <> " SET override_limit = NULL WHERE prefix_id = "
      <> textLiteral prefix
  ]

-- | Repeat a batch action until it returns empty, accumulating the results.
drainWith :: IO [a] -> IO [a]
drainWith fetch = go []
  where
    go batches = do
      batch <- fetch
      if null batch then pure (concat (reverse batches)) else go (batch : batches)

-- | Truncate to microsecond precision to match PostgreSQL @timestamptz@.
truncateToMicros :: UTCTime -> UTCTime
truncateToMicros (UTCTime d t) =
  let micros = floor (t * 1e6) :: Integer
   in UTCTime d (picosecondsToDiffTime (micros * 1000000))

{-# LANGUAGE OverloadedStrings #-}

-- | Versioned, tracked migrations for job queue schemas.
--
-- Uses the @postgresql-migration@ library to:
--
--   * Track which migrations have been run in a database table
--   * Run migrations in order
--   * Prevent re-running completed migrations
--   * Support incremental schema changes
--
-- The migration history is stored in a @<schema_name>.schema_migrations@ table.
-- For example, if you use the schema name @"arbiter"@, the migration tracking
-- table will be created at @arbiter.schema_migrations@.
module Arbiter.Migrations
  ( -- * Configuration
    MigrationConfig (..)
  , defaultMigrationConfig

    -- * Tracked Migrations
  , runMigrationsForRegistry
  , runMigrationsTrackedForTables
  , jobQueueMigrationsForTable

    -- * Re-exports
  , MigrationResult (..)
  ) where

import Arbiter.Core.CronSchedule (createCronSchedulesTableSQL)
import Arbiter.Core.Job.Schema
  ( createDLQFailedAtIndexSQL
  , createDLQGroupKeyIndexSQL
  , createDLQParentIdIndexSQL
  , createDedupKeyIndexSQL
  , createEventStreamingFunctionSQL
  , createEventStreamingTriggersSQL
  , createGroupsIndexSQL
  , createGroupsTableSQL
  , createGroupsTriggerFunctionsSQL
  , createGroupsTriggersSQL
  , createJobQueueDLQTableSQL
  , createJobQueueGroupKeyIndexSQL
  , createJobQueueTableSQL
  , createJobQueueUngroupedRankingIndexSQL
  , createNotifyFunctionSQL
  , createNotifyTriggerSQL
  , createParentIdIndexSQL
  , createReaperSeqSQL
  , createResultsTableSQL
  , createSchemaSQL
  )
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Control.Exception (bracket, try)
import Control.Monad (when)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Database.PostgreSQL.LibPQ qualified as LibPQ
import Database.PostgreSQL.Simple (Only (..), close, connectPostgreSQL, execute_, query)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Internal (withConnection)
import Database.PostgreSQL.Simple.Migration
  ( MigrationCommand (..)
  , MigrationOptions (..)
  , MigrationResult (..)
  , Verbosity (..)
  , defaultOptions
  , runMigrations
  )
import Database.PostgreSQL.Simple.Types (Query (..))

-- | Configuration for job queue migrations
--
-- Controls which optional features are enabled when creating job queue tables.
data MigrationConfig = MigrationConfig
  { enableNotifications :: Bool
  -- ^ Whether to create LISTEN/NOTIFY triggers for reactive job claiming.
  -- When enabled, workers can subscribe to notifications instead of polling.
  -- Default: 'True'
  , enableEventStreaming :: Bool
  -- ^ Whether to create event streaming triggers for the admin UI.
  -- When enabled, every INSERT\/UPDATE\/DELETE on job tables fires an enriched
  -- JSON event via @pg_notify@ on the @arbiter_job_events@ channel.
  -- This adds overhead to every row operation — disable for maximum throughput.
  -- Default: 'False'
  }
  deriving stock (Eq, Show)

-- | Default migration configuration
defaultMigrationConfig :: MigrationConfig
defaultMigrationConfig =
  MigrationConfig
    { enableNotifications = True
    , enableEventStreaming = False
    }

-- | Run migrations for all tables in a queue registry
--
-- This function creates all tables defined in the type-level registry
-- within a single PostgreSQL schema. Each payload type gets its own
-- table pair (main + DLQ) within the schema.
--
-- __Note__: This function creates the schema first (outside of migration tracking)
-- so that the @schema_migrations@ table can be placed inside the same schema.
--
-- PostgreSQL notices (like "NOTICE: relation already exists") are suppressed
-- during migration to reduce log noise.
--
-- Example:
--
-- @
-- type AppRegistry =
--   '[ '("email_jobs", EmailPayload)
--    , '("order_jobs", OrderPayload)
--    ]
--
-- main :: IO ()
-- main = do
--   result <- runMigrationsForRegistry
--               (Proxy @AppRegistry)
--               "host=localhost dbname=mydb"
--               "arbiter"
--               defaultMigrationConfig
-- @
runMigrationsForRegistry
  :: forall registry
   . (RegistryTables registry)
  => Proxy registry
  -- ^ Proxy for the job payload registry
  -> ByteString
  -- ^ Database connection string
  -> Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> MigrationConfig
  -- ^ Migration configuration
  -> IO (MigrationResult String)
  -- ^ Migration results
runMigrationsForRegistry proxy connStr schemaName config = do
  let tables = registryTableNames proxy
  runMigrationsTrackedForTables connStr schemaName tables config

-- | Run migrations for multiple tables within a single schema.
runMigrationsTrackedForTables
  :: ByteString
  -- ^ Database connection string
  -> Text
  -- ^ Schema name
  -> [Text]
  -- ^ List of table names to create
  -> MigrationConfig
  -- ^ Migration configuration
  -> IO (MigrationResult String)
runMigrationsTrackedForTables connStr schemaName tableNames config =
  bracket (connectPostgreSQL connStr) close $ \conn -> do
    -- Disable NOTICE messages on the underlying LibPQ connection
    withConnection conn $ \libpqConn ->
      LibPQ.disableNoticeReporting libpqConn

    -- Create the schema. If CREATE SCHEMA fails (e.g. insufficient privileges),
    -- check whether the schema already exists (manual creation) and proceed.
    let schemaSQL = Query (encodeUtf8 $ createSchemaSQL schemaName)
    result <- try $ execute_ conn schemaSQL
    case result of
      Right _ -> pure ()
      Left (e :: PG.SqlError) -> do
        -- Check if the schema exists despite the CREATE failure
        exists <- schemaExists conn schemaName
        when (not exists) $
          ioError
            ( userError $
                "Failed to create schema "
                  <> T.unpack schemaName
                  <> " and it does not exist. Either grant CREATE privilege on the database"
                  <> " or create the schema manually: CREATE SCHEMA "
                  <> T.unpack schemaName
                  <> ";"
                  <> "\nOriginal error: "
                  <> show e
            )

    -- Re-enable notice reporting for the migrations
    withConnection conn $ \libpqConn ->
      LibPQ.enableNoticeReporting libpqConn

    -- Build migrations: schema-level (once) + per-table migrations
    let schemaMigrations =
          [ MigrationScript
              "create-cron-schedules"
              (encodeUtf8 $ createCronSchedulesTableSQL schemaName)
          ]
            <> [ MigrationScript
                   "create-event-streaming-function"
                   (encodeUtf8 $ createEventStreamingFunctionSQL schemaName)
               | enableEventStreaming config
               ]
        tableMigrations = concatMap (\tableName -> jobQueueMigrationsForTable schemaName tableName config) tableNames
        migrations = schemaMigrations <> tableMigrations
        migrationTableName = encodeUtf8 $ schemaName <> ".schema_migrations"
        options =
          defaultOptions
            { optVerbose = Quiet
            , optTableName = migrationTableName
            }

    -- Initialize the migration system
    _ <- runMigrations conn options [MigrationInitialization]

    -- Run the actual migrations
    runMigrations conn options migrations

-- | All job queue migrations for a single table
--
-- This creates migrations for one table and its DLQ within a schema.
-- Each table gets its own set of migrations with unique version identifiers.
jobQueueMigrationsForTable
  :: Text
  -- ^ Schema name
  -> Text
  -- ^ Table name
  -> MigrationConfig
  -- ^ Migration configuration
  -> [MigrationCommand]
  -- ^ List of migration commands
jobQueueMigrationsForTable schemaName tableName config =
  let prefix = T.unpack tableName <> "-"
      script name sql = MigrationScript (prefix <> name) (encodeUtf8 sql)

      coreMigrations =
        [ script "create-table" $ createJobQueueTableSQL schemaName tableName
        , script "create-dlq-table" $ createJobQueueDLQTableSQL schemaName tableName
        , script "create-dlq-group-key-index" $ createDLQGroupKeyIndexSQL schemaName tableName
        , script "create-dlq-failed-at-index" $ createDLQFailedAtIndexSQL schemaName tableName
        , script "create-dedup-key-index" $ createDedupKeyIndexSQL schemaName tableName
        , script "create-group-key-index" $ createJobQueueGroupKeyIndexSQL schemaName tableName
        , script "create-ungrouped-ranking-index" $ createJobQueueUngroupedRankingIndexSQL schemaName tableName
        , script "create-parent-id-index" $ createParentIdIndexSQL schemaName tableName
        , script "create-dlq-parent-id-index" $ createDLQParentIdIndexSQL schemaName tableName
        , script "create-results-table" $ createResultsTableSQL schemaName tableName
        , script "create-groups-table" $ createGroupsTableSQL schemaName tableName
        , script "create-groups-index" $ createGroupsIndexSQL schemaName tableName
        , script "create-groups-trigger-functions" $ createGroupsTriggerFunctionsSQL schemaName tableName
        , script "create-groups-triggers" $ createGroupsTriggersSQL schemaName tableName
        , script "create-reaper-seq" $ createReaperSeqSQL schemaName tableName
        ]
      notifyTriggers
        | enableNotifications config =
            [ script "create-notify-function" $ createNotifyFunctionSQL schemaName tableName
            , script "create-notify-trigger" $ createNotifyTriggerSQL schemaName tableName
            ]
        | otherwise = []
      eventStreamingTriggers
        | enableEventStreaming config =
            [ script "create-event-streaming-triggers" $ createEventStreamingTriggersSQL schemaName tableName
            ]
        | otherwise = []
   in coreMigrations <> notifyTriggers <> eventStreamingTriggers

-- | Check whether a schema exists in the database.
schemaExists :: PG.Connection -> Text -> IO Bool
schemaExists conn schemaName = do
  rows <- query conn "SELECT 1 FROM pg_namespace WHERE nspname = ?" (Only schemaName) :: IO [Only Int]
  pure (not (null rows))

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
-- Migration history is stored in a @schema_migrations@ table inside the
-- target schema (e.g., @arbiter.schema_migrations@).
module Arbiter.Migrations
  ( -- * Configuration
    MigrationConfig (..)
  , defaultMigrationConfig

    -- * Tracked Migrations
  , runMigrationsForRegistry
  , runMigrationsTrackedForTables
  , jobQueueMigrationsForTable
  , schemaLevelMigrations
  , AdmissionSeeds (..)
  , noAdmissionSeeds
  , TableAdmission (..)
  , allTableAdmission

    -- * Rate-limit reconciliation
  , conflictingPolicyPrefixes

    -- * Re-exports
  , MigrationResult (..)
  ) where

import Arbiter.Core.Concurrency.Schema
  ( addConcurrencyColumnsSQL
  , createConcurrencyIndexSQL
  , createConcurrencyPoliciesTableSQL
  , createConcurrencyTableSQL
  , createConcurrencyTriggerFunctionsSQL
  , createConcurrencyTriggersSQL
  , upsertConcurrencyPolicyRowSQL
  )
import Arbiter.Core.Concurrency.Spec (ConcurrencyPolicy (..), registryConcurrencyPolicies, registryConcurrencyTables)
import Arbiter.Core.CronSchedule
  ( addLastManualRunColumnSQL
  , addQueueNameColumnSQL
  , addRunRequestedColumnSQL
  , addTimezoneColumnSQL
  , createCronSchedulesTableSQL
  )
import Arbiter.Core.Gates (createGatesTableSQL)
import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , createArchiveCompletedAtIndexSQL
  , createArchiveExpiresAtIndexSQL
  , createArchiveGroupKeyIndexSQL
  , createArchiveJobIdIndexSQL
  , createArchiveParentIdIndexSQL
  , createDLQFailedAtIndexSQL
  , createDLQGroupKeyIndexSQL
  , createDLQParentIdIndexSQL
  , createDedupKeyIndexSQL
  , createEventStreamingFunctionSQL
  , createEventStreamingTriggersSQL
  , createGroupsTableSQL
  , createGroupsTriggerFunctionsSQL
  , createGroupsTriggersSQL
  , createJobQueueArchiveTableSQL
  , createJobQueueDLQTableSQL
  , createJobQueueGroupKeyIndexSQL
  , createJobQueueTableSQL
  , createNotifyFunctionSQL
  , createNotifyTriggerSQL
  , createParentIdIndexSQL
  , createResultsTableSQL
  , createSchemaSQL
  , jobQueueTable
  , migrateGroupsReadyRankingSQL
  , migrateUngroupedReadySplitIndexesSQL
  , setMaxAttemptsDefaultSQL
  )
import Arbiter.Core.Job.Types (RegistryAdmissionPolicies)
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Arbiter.Core.Queues (createQueuesTableSQL)
import Arbiter.Core.RateLimit.Schema
  ( PolicyRow (..)
  , addRateLimitColumnsSQL
  , addRateLimitCostColumnSQL
  , alterRateLimitsDurabilitySQL
  , arbiterRateLimitsTableName
  , createRateLimitBucketTriggerFunctionsSQL
  , createRateLimitBucketTriggersSQL
  , createRateLimitPoliciesTableSQL
  , createRateLimitsTableSQL
  , createThrottledIndexSQL
  , toPolicyRow
  , upsertPolicyRowSQL
  )
import Arbiter.Core.RateLimit.Spec
  ( Durability (..)
  , registryRateLimitPolicies
  , registryRateLimitTables
  )
import Arbiter.Core.Worker
  ( addArchiveForColumnSQL
  , addCancelRequestedAtColumnSQL
  , addClaimedByColumnSQL
  , createWorkersTableSQL
  )
import Control.Exception (SomeAsyncException, SomeException, bracket, displayException, fromException, throwIO, try)
import Control.Monad (void, when)
import Data.ByteString (ByteString)
import Data.Foldable (find, traverse_)
import Data.Map.Strict qualified as Map
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
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
  -- This adds overhead to every row operation - disable for maximum throughput.
  -- Default: 'False'
  , rateLimitDurability :: Durability
  -- ^ WAL-logging for the rate-limit bucket table in this schema. 'Unlogged'
  -- (default) resets buckets on crash\/failover. 'Durable' preserves them at a
  -- throughput cost.
  }
  deriving stock (Eq, Show)

-- | Default migration configuration
defaultMigrationConfig :: MigrationConfig
defaultMigrationConfig =
  MigrationConfig
    { enableNotifications = True
    , enableEventStreaming = False
    , rateLimitDurability = Unlogged
    }

-- | Run migrations for all tables in a queue registry.
--
-- Creates tables for each payload type in the registry (main + DLQ) within
-- a single PostgreSQL schema. The schema itself is created first, outside of
-- migration tracking. PostgreSQL notices are suppressed during migration.
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
   . ( RegistryAdmissionPolicies registry
     , RegistryTables registry
     )
  => Proxy registry
  -- ^ Proxy for the job payload registry
  -> ByteString
  -- ^ Database connection string
  -> SchemaName
  -- ^ Schema name
  -> MigrationConfig
  -- ^ Migration configuration
  -> IO (MigrationResult String)
  -- ^ Migration results
runMigrationsForRegistry proxy connStr schemaName config = do
  let ccTables = Map.fromList (registryConcurrencyTables @registry)
      rlTables = Map.fromList (registryRateLimitTables @registry)
      admissionFor t =
        TableAdmission
          { tableConcurrency = Map.findWithDefault False t ccTables
          , tableRateLimit = Map.findWithDefault False t rlTables
          }
      tables = [(t, admissionFor t) | t <- registryTableNames proxy]
      -- Policies are collected from each payload's 'rateLimitFor' selector, so a
      -- referenced policy is always seeded.
      seeds =
        AdmissionSeeds
          { seedRateLimitPolicies = map toPolicyRow (Set.toList (registryRateLimitPolicies @registry))
          , seedConcurrencyPolicies = Set.toList (registryConcurrencyPolicies @registry)
          , seedDurability = rateLimitDurability config
          }
  runMigrationsTrackedForTables connStr schemaName tables config seeds

-- | Admission policy rows to seed after a successful migration.
data AdmissionSeeds = AdmissionSeeds
  { seedRateLimitPolicies :: [PolicyRow]
  , seedConcurrencyPolicies :: [ConcurrencyPolicy]
  , seedDurability :: Durability
  }

-- | Seeds for a deployment with no admission policies.
noAdmissionSeeds :: AdmissionSeeds
noAdmissionSeeds = AdmissionSeeds [] [] Unlogged

-- | Which admission trigger kinds a table's payload declares. Trigger migrations
-- are install-only: once recorded they are never dropped, so a kind removed from
-- a payload keeps its triggers (rolling deploys may still enqueue keyed jobs).
data TableAdmission = TableAdmission
  { tableConcurrency :: Bool
  , tableRateLimit :: Bool
  }
  deriving stock (Eq, Show)

-- | Install every admission trigger kind.
allTableAdmission :: TableAdmission
allTableAdmission = TableAdmission True True

-- | Run migrations for multiple tables within a single schema, seeding the given
-- rate-limit policies. On migration success, reconciles the policy and bucket
-- tables on the same connection.
runMigrationsTrackedForTables
  :: ByteString
  -> SchemaName
  -> [(TableName, TableAdmission)]
  -> MigrationConfig
  -> AdmissionSeeds
  -> IO (MigrationResult String)
runMigrationsTrackedForTables connStr schemaName tableNames config (AdmissionSeeds policyRows concRows durability) =
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
    let schemaMigrations = schemaLevelMigrations config schemaName
        tableMigrations = concatMap (\(tableName, adm) -> jobQueueMigrationsForTable schemaName tableName config adm) tableNames
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
    migrationResult <- runMigrations conn options migrations
    -- Reconciliation can throw (conflicting prefix, ALTER failure). Surface as MigrationError.
    case migrationResult of
      MigrationSuccess -> do
        reconciled <-
          try $ do
            reconcileRateLimitPolicies conn schemaName policyRows
            reconcileConcurrencyPolicies conn schemaName concRows
            reconcileRateLimitDurability conn schemaName durability
        case reconciled of
          Right () -> pure MigrationSuccess
          -- Convert synchronous failures to a result. Async exceptions (cancellation) propagate.
          Left (e :: SomeException)
            | Just (_ :: SomeAsyncException) <- fromException e -> throwIO e
            | otherwise -> pure (MigrationError (displayException e))
      other -> pure other

-- | Upsert each policy's @default_*@ params into the policies table (created
-- unconditionally by the schema migrations), leaving operator @override_*@
-- intact. Idempotent and never deletes, so overrides and removed prefixes survive
-- a deploy.
reconcileRateLimitPolicies :: PG.Connection -> SchemaName -> [PolicyRow] -> IO ()
reconcileRateLimitPolicies =
  reconcilePolicyRows "rate-limit policy" "parameters" prefixId policyParamsKey upsertPolicyRowSQL

-- | 'conflictingPrefixes' specialized to policy rows.
conflictingPolicyPrefixes :: [PolicyRow] -> [Text]
conflictingPolicyPrefixes = conflictingPrefixes prefixId policyParamsKey

-- | A canonical conflict key for a policy's params. Rendered so a non-finite value
-- (NaN) compares equal to itself and identical declarations dedup.
policyParamsKey :: PolicyRow -> String
policyParamsKey r = show (maxTokens r, refillAmt r, interval r)

-- | Upsert each pool's @default_limit@, leaving operator overrides intact. Two pools
-- with the same prefix but different limits fail the migration.
reconcileConcurrencyPolicies :: PG.Connection -> SchemaName -> [ConcurrencyPolicy] -> IO ()
reconcileConcurrencyPolicies =
  reconcilePolicyRows "concurrency pool" "limits" cpPrefix cpLimit upsertConcurrencyPolicyRowSQL

-- | Upsert each policy row's @default_*@ params, after failing on any prefix that
-- contains the @:@ key separator or is declared with conflicting params. Idempotent
-- and never deletes, so operator overrides and removed prefixes survive a deploy.
reconcilePolicyRows
  :: (Ord params)
  => String
  -> String
  -> (row -> Text)
  -> (row -> params)
  -> (SchemaName -> row -> Text)
  -> PG.Connection
  -> SchemaName
  -> [row]
  -> IO ()
reconcilePolicyRows label noun prefixOf paramsOf upsertSQL conn schemaName rows
  | Just p <- find (T.isInfixOf ":") (map prefixOf rows) =
      ioError
        ( userError $
            label
              <> " prefix "
              <> T.unpack p
              <> " contains ':', the key separator, so its keys would alias another prefix's. Use a colon-free prefix."
        )
  | (p : _) <- conflictingPrefixes prefixOf paramsOf rows =
      ioError
        ( userError $
            label <> " prefix " <> T.unpack p <> " is declared with conflicting " <> noun <> ". Give each a unique prefix."
        )
  | otherwise =
      PG.withTransaction conn $
        traverse_ (\row -> void $ execute_ conn (Query (encodeUtf8 (upsertSQL schemaName row)))) rows

-- | Prefixes declared with more than one distinct parameter set, which would clobber on the prefix primary key.
conflictingPrefixes :: (Ord params) => (row -> Text) -> (row -> params) -> [row] -> [Text]
conflictingPrefixes prefixOf paramsOf rows =
  Map.keys (Map.filter ((> 1) . Set.size) paramsByPrefix)
  where
    paramsByPrefix =
      Map.fromListWith Set.union [(prefixOf r, Set.singleton (paramsOf r)) | r <- rows]

-- | Converge the bucket table's WAL persistence to the declared durability. Reads
-- the current @pg_class.relpersistence@ and issues @SET LOGGED@/@SET UNLOGGED@
-- only on a change. The ALTER rewrites the table under @ACCESS EXCLUSIVE@, so
-- token consumes block briefly while a switch runs.
reconcileRateLimitDurability :: PG.Connection -> SchemaName -> Durability -> IO ()
reconcileRateLimitDurability conn schemaName durability = do
  rows <-
    query
      conn
      "SELECT c.relpersistence::text FROM pg_class c \
      \JOIN pg_namespace n ON n.oid = c.relnamespace \
      \WHERE n.nspname = ? AND c.relname = ?"
      (schemaName, arbiterRateLimitsTableName)
  let target = case durability of
        Durable -> "p" :: Text
        Unlogged -> "u"
  case rows of
    (Only current : _)
      | current /= target ->
          void $ execute_ conn (Query (encodeUtf8 (alterRateLimitsDurabilitySQL durability schemaName)))
    _ -> pure ()

-- | The schema-level (non-per-table) migrations, run once per schema. Exposed so
-- the golden suite can pin every shipped migration body.
schemaLevelMigrations :: MigrationConfig -> SchemaName -> [MigrationCommand]
schemaLevelMigrations config schemaName =
  [ MigrationScript "create-cron-schedules" (encodeUtf8 $ createCronSchedulesTableSQL schemaName)
  , MigrationScript "cron-schedules-add-timezone" (encodeUtf8 $ addTimezoneColumnSQL schemaName)
  , MigrationScript "cron-schedules-add-queue-name" (encodeUtf8 $ addQueueNameColumnSQL schemaName)
  , MigrationScript "cron-schedules-add-run-requested" (encodeUtf8 $ addRunRequestedColumnSQL schemaName)
  , MigrationScript "cron-schedules-add-last-manual-run" (encodeUtf8 $ addLastManualRunColumnSQL schemaName)
  , MigrationScript "create-arbiter-workers" (encodeUtf8 $ createWorkersTableSQL schemaName)
  , MigrationScript "create-arbiter-queues" (encodeUtf8 $ createQueuesTableSQL schemaName)
  , MigrationScript "create-arbiter-gates" (encodeUtf8 $ createGatesTableSQL schemaName)
  , MigrationScript "create-arbiter-rate-limit-policies" (encodeUtf8 $ createRateLimitPoliciesTableSQL schemaName)
  , MigrationScript "create-arbiter-rate-limits" (encodeUtf8 $ createRateLimitsTableSQL schemaName)
  , MigrationScript "create-arbiter-concurrency-policies" (encodeUtf8 $ createConcurrencyPoliciesTableSQL schemaName)
  , MigrationScript "create-arbiter-concurrency" (encodeUtf8 $ createConcurrencyTableSQL schemaName)
  ]
    <> [ MigrationScript "create-event-streaming-function" (encodeUtf8 $ createEventStreamingFunctionSQL schemaName)
       | enableEventStreaming config
       ]

-- | All job queue migrations for a single table
--
-- This creates migrations for one table and its DLQ within a schema.
-- Each table gets its own set of migrations with unique version identifiers.
jobQueueMigrationsForTable
  :: SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> MigrationConfig
  -- ^ Migration configuration
  -> TableAdmission
  -- ^ Which admission trigger kinds to install
  -> [MigrationCommand]
  -- ^ List of migration commands
jobQueueMigrationsForTable schemaName tableName config adm =
  let prefix = T.unpack tableName <> "-"
      script name sql = MigrationScript (prefix <> name) (encodeUtf8 sql)

      coreMigrations =
        [ script "create-table" $ createJobQueueTableSQL schemaName tableName
        , script "create-dlq-table" $ createJobQueueDLQTableSQL schemaName tableName
        , script "create-dlq-group-key-index" $ createDLQGroupKeyIndexSQL schemaName tableName
        , script "create-dlq-failed-at-index" $ createDLQFailedAtIndexSQL schemaName tableName
        , script "create-dedup-key-index" $ createDedupKeyIndexSQL schemaName tableName
        , script "create-group-key-index" $ createJobQueueGroupKeyIndexSQL schemaName tableName
        , script "create-parent-id-index" $ createParentIdIndexSQL schemaName tableName
        , script "create-dlq-parent-id-index" $ createDLQParentIdIndexSQL schemaName tableName
        , script "create-results-table" $ createResultsTableSQL schemaName tableName
        , script "create-groups-table" $ createGroupsTableSQL schemaName tableName
        , script "add-claimed-by-column" $ addClaimedByColumnSQL schemaName tableName
        , script "migrate-ungrouped-ready-split-indexes" $ migrateUngroupedReadySplitIndexesSQL schemaName tableName
        , script "migrate-groups-ready-ranking" $ migrateGroupsReadyRankingSQL schemaName tableName
        , script "add-rate-limit-columns" $ addRateLimitColumnsSQL schemaName tableName
        , script "create-throttled-index" $ createThrottledIndexSQL schemaName tableName
        , script "create-groups-trigger-functions-v7" $ createGroupsTriggerFunctionsSQL schemaName tableName
        , script "create-groups-triggers" $ createGroupsTriggersSQL schemaName tableName
        , script "add-concurrency-columns" $ addConcurrencyColumnsSQL schemaName tableName
        , script "create-concurrency-index" $ createConcurrencyIndexSQL schemaName tableName
        , script "add-rate-limit-cost-column" $ addRateLimitCostColumnSQL schemaName tableName
        , script "set-job-queue-fillfactor-100" $
            "ALTER TABLE " <> jobQueueTable schemaName tableName <> " SET (fillfactor = 100);"
        , script "set-max-attempts-default" $ setMaxAttemptsDefaultSQL schemaName tableName
        , script "add-cancel-requested-at-column" $ addCancelRequestedAtColumnSQL schemaName tableName
        , script "add-archive-for-column" $ addArchiveForColumnSQL schemaName tableName
        , script "create-archive-table" $ createJobQueueArchiveTableSQL schemaName tableName
        , script "create-archive-completed-at-index" $ createArchiveCompletedAtIndexSQL schemaName tableName
        , script "create-archive-expires-at-index" $ createArchiveExpiresAtIndexSQL schemaName tableName
        , script "create-archive-job-id-index" $ createArchiveJobIdIndexSQL schemaName tableName
        , script "create-archive-parent-id-index" $ createArchiveParentIdIndexSQL schemaName tableName
        , script "create-archive-group-key-index" $ createArchiveGroupKeyIndexSQL schemaName tableName
        ]
      concurrencyTriggers
        | tableConcurrency adm =
            [ script "create-concurrency-trigger-functions" $ createConcurrencyTriggerFunctionsSQL schemaName tableName
            , script "create-concurrency-triggers" $ createConcurrencyTriggersSQL schemaName tableName
            ]
        | otherwise = []
      rateLimitTriggers
        | tableRateLimit adm =
            [ script "create-rate-limit-bucket-trigger-functions" $ createRateLimitBucketTriggerFunctionsSQL schemaName tableName
            , script "create-rate-limit-bucket-triggers" $ createRateLimitBucketTriggersSQL schemaName tableName
            ]
        | otherwise = []
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
   in coreMigrations <> concurrencyTriggers <> rateLimitTriggers <> notifyTriggers <> eventStreamingTriggers

-- | Check whether a schema exists in the database.
schemaExists :: PG.Connection -> Text -> IO Bool
schemaExists conn schemaName = do
  rows <- query conn "SELECT 1 FROM pg_namespace WHERE nspname = ?" (Only schemaName) :: IO [Only Int]
  pure (not (null rows))

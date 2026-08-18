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
  ( -- * Registry
    QueueSpec (..)
  , Queue

    -- * Configuration
  , MigrationConfig (..)
  , defaultMigrationConfig
  , validateRegistryNames

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
import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Gates (addGateMetadataColumnSQL, createGatesTableSQL)
import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , addClaimSeqColumnSQL
  , addTraceContextColumnSQL
  , cancelNotifyChannel
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
  , createGroupsEmptiedIndexSQL
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
  , dropEventStreamingFunctionSQL
  , eventStreamingDLQTriggerName
  , eventStreamingFunctionName
  , eventStreamingObjectComment
  , eventStreamingObjectCommentPrefix
  , eventStreamingTriggerName
  , jobQueueTable
  , migrateGroupsReadyRankingSQL
  , migrateUngroupedReadySplitIndexesSQL
  , notifyFunctionName
  , notifyObjectComment
  , notifyObjectCommentPrefix
  , notifyTriggerName
  , pauseNotifyChannel
  , queueTableNames
  , setMaxAttemptsDefaultSQL
  )
import Arbiter.Core.Job.Types (RegistryAdmissionPolicies)
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..), RegistryTables (..))
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
import Control.Exception (SomeAsyncException, SomeException, bracket, fromException, throwIO, try)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
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
import Database.PostgreSQL.Simple.Types (PGArray (..), Query (..))

-- | Configuration for job queue migrations
--
-- Controls the desired state reconciled after tracked migrations.
data MigrationConfig = MigrationConfig
  { enableNotifications :: Bool
  -- ^ Whether LISTEN/NOTIFY triggers for reactive job claiming should be
  -- installed. Re-running migrations reconciles existing schemas in either
  -- direction. Default: 'True'.
  , enableEventStreaming :: Bool
  -- ^ Whether event-streaming triggers for the admin UI should be installed.
  -- When enabled, every INSERT\/UPDATE\/DELETE on job tables fires an enriched
  -- JSON event via @pg_notify@. Re-running migrations with this disabled drops
  -- the triggers and shared function. Default: 'False'.
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
--   '[ Queue "email_jobs" EmailPayload
--    , Queue "order_jobs" OrderPayload
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

-- | Validate identifiers before PostgreSQL can truncate generated object or
-- channel names into another queue's. The 35-byte queue limit leaves generated
-- indexes and channels untruncated. Rate-limit bucket triggers do truncate past
-- 30 bytes and stay distinct.
validateRegistryNames :: SchemaName -> [TableName] -> Either Text ()
validateRegistryNames schemaName tables
  | T.null schemaName = Left "Arbiter schema name must not be empty"
  | byteLength schemaName > 63 = Left "Arbiter schema name exceeds PostgreSQL's 63-byte identifier limit"
  | any T.null tables = Left "Arbiter queue name must not be empty"
  | Just table <- find ((> 35) . byteLength) tables =
      Left ("Arbiter queue name exceeds the 35-byte generated-identifier limit: " <> table)
  | Just generated <- generatedCollision =
      Left ("Arbiter queue names generate the same PostgreSQL object: " <> generated)
  | Just channel <- channelCollision =
      Left ("Arbiter queue names generate the same notification channel: " <> channel)
  | otherwise = Right ()
  where
    byteLength = BS.length . encodeUtf8
    generatedOwners =
      Map.fromListWith
        Set.union
        [ (generated, Set.singleton table)
        | table <- tables
        , generated <- queueTableNames table
        ]
    generatedCollision = fst <$> find ((> 1) . Set.size . snd) (Map.toList generatedOwners)
    channelOwners =
      Map.fromListWith
        Set.union
        [ (channel schemaName table, Set.singleton table)
        | table <- tables
        , channel <- [pauseNotifyChannel, cancelNotifyChannel]
        ]
    channelCollision = fst <$> find ((> 1) . Set.size . snd) (Map.toList channelOwners)

-- | Run migrations for multiple tables within a single schema, seeding the given
-- rate-limit policies. On migration success, reconciles the policy and bucket
-- tables on the same connection. The table list must be the schema's whole queue
-- set. Reconciliation reads it as authoritative and treats an omitted queue as
-- removed, dropping its notify and event-streaming objects.
runMigrationsTrackedForTables
  :: ByteString
  -> SchemaName
  -> [(TableName, TableAdmission)]
  -> MigrationConfig
  -> AdmissionSeeds
  -> IO (MigrationResult String)
runMigrationsTrackedForTables connStr schemaName tableNames config (AdmissionSeeds policyRows concRows durability) =
  case validateRegistryNames schemaName (map fst tableNames) of
    Left err -> pure (MigrationError (T.unpack err))
    Right () -> bracket (connectPostgreSQL connStr) close $ \conn -> do
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
      let schemaMigrations = schemaLevelMigrations schemaName
          tableMigrations = concatMap (\(tableName, adm) -> jobQueueMigrationsForTable schemaName tableName adm) tableNames
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
              reconcileOptionalTriggers conn schemaName (map fst tableNames) config
          case reconciled of
            Right () -> pure MigrationSuccess
            -- Convert synchronous failures to a result. Async exceptions (cancellation) propagate.
            Left (e :: SomeException)
              | Just (_ :: SomeAsyncException) <- fromException e -> throwIO e
              | otherwise -> pure (MigrationError (T.unpack (displayEx e)))
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

-- | Converge optional triggers after tracked migrations. This restores objects
-- removed by an earlier disabled configuration without changing migration history.
-- The sweep is schema-wide by design: the table list is the schema's whole queue
-- set, so objects belonging to a queue outside it are left over from a registry
-- that shrank and are dropped.
reconcileOptionalTriggers :: PG.Connection -> SchemaName -> [TableName] -> MigrationConfig -> IO ()
reconcileOptionalTriggers conn schemaName tables config =
  PG.withTransaction conn $ do
    if enableNotifications config
      then do
        dropNotifyObjectsExcept tables
        traverse_ installNotify tables
      else dropNotifyObjectsExcept []
    if enableEventStreaming config
      then do
        executeSQL (createEventStreamingFunctionSQL schemaName)
        dropEventTriggersExcept (concatMap (\table -> [table, table <> "_dlq"]) tables)
        traverse_ installEventStream tables
      else do
        dropEventTriggersExcept []
        ours <- eventFunctionIsMarked
        when ours $ executeSQL (dropEventStreamingFunctionSQL schemaName)
  where
    -- Functions are replaced in place, which takes no lock on the queue table. Triggers
    -- are rebuilt only when their marker is off the current version, so an unchanged
    -- deploy never blocks claims.
    installNotify table = do
      executeSQL (createNotifyFunctionSQL schemaName table)
      current <- triggerIsCurrent table (notifyTriggerName table) notifyObjectComment
      unless current $ executeSQL (createNotifyTriggerSQL schemaName table)

    installEventStream table = do
      mainCurrent <- triggerIsCurrent table (eventStreamingTriggerName table) eventStreamingObjectComment
      dlqCurrent <- triggerIsCurrent (table <> "_dlq") (eventStreamingDLQTriggerName table) eventStreamingObjectComment
      unless (mainCurrent && dlqCurrent) $
        executeSQL (createEventStreamingTriggersSQL schemaName table)

    triggerIsCurrent table trigger marker = do
      rows <-
        query
          conn
          ( "SELECT EXISTS (SELECT 1 "
              <> triggerJoins
              <> "WHERE n.nspname = ? AND c.relname = ? AND t.tgname = ? \
                 \AND obj_description(t.oid, 'pg_trigger') = ?)"
          )
          (schemaName, table, trigger, marker)
      pure $ case rows of
        Only current : _ -> current
        _ -> False

    eventFunctionIsMarked = do
      rows <-
        query
          conn
          "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
          \WHERE n.nspname = ? AND p.proname = ? AND p.pronargs = 0 \
          \AND obj_description(p.oid, 'pg_proc') LIKE ?)"
          (schemaName, eventStreamingFunctionName, eventStreamingObjectCommentPrefix <> "%")
      pure $ case rows of
        Only marked : _ -> marked
        _ -> False

    dropEventTriggersExcept keep = do
      commands <-
        query
          conn
          ( dropTriggerSelect
              <> triggerJoins
              <> "WHERE n.nspname = ? AND obj_description(t.oid, 'pg_trigger') LIKE ? \
                 \AND NOT (c.relname::text = ANY(?::text[]))"
          )
          (schemaName, eventStreamingObjectCommentPrefix <> "%", PGArray keep)
      traverse_ (\(Only command) -> executeSQL command) commands

    dropNotifyObjectsExcept keep = do
      triggerCommands <-
        query
          conn
          ( dropTriggerSelect
              <> triggerJoins
              <> "WHERE n.nspname = ? AND obj_description(t.oid, 'pg_trigger') LIKE ? \
                 \AND NOT (c.relname::text = ANY(?::text[]))"
          )
          (schemaName, notifyObjectCommentPrefix <> "%", PGArray keep)
      traverse_ (\(Only command) -> executeSQL command) triggerCommands
      functionCommands <-
        query
          conn
          "SELECT format('DROP FUNCTION IF EXISTS %I.%I();', n.nspname, p.proname) \
          \FROM pg_proc p \
          \JOIN pg_namespace n ON n.oid = p.pronamespace \
          \WHERE n.nspname = ? AND obj_description(p.oid, 'pg_proc') LIKE ? AND p.pronargs = 0 \
          \AND NOT (p.proname::text = ANY(?::text[]))"
          (schemaName, notifyObjectCommentPrefix <> "%", PGArray (map notifyFunctionName keep))
      traverse_ (\(Only command) -> executeSQL command) functionCommands

    executeSQL = void . execute_ conn . Query . encodeUtf8

-- | The catalog joins the trigger sweeps share.
triggerJoins :: Query
triggerJoins =
  "FROM pg_trigger t \
  \JOIN pg_class c ON c.oid = t.tgrelid \
  \JOIN pg_namespace n ON n.oid = c.relnamespace "

-- | Renders a @DROP TRIGGER@ statement per matched row.
dropTriggerSelect :: Query
dropTriggerSelect = "SELECT format('DROP TRIGGER IF EXISTS %I ON %I.%I;', t.tgname, n.nspname, c.relname) "

-- | The schema-level (non-per-table) migrations, run once per schema. Exposed so
-- the golden suite can pin every shipped migration body. Optional notification and
-- event-streaming objects are not tracked here, 'reconcileOptionalTriggers' owns them.
schemaLevelMigrations :: SchemaName -> [MigrationCommand]
schemaLevelMigrations schemaName =
  [ MigrationScript "create-cron-schedules" (encodeUtf8 $ createCronSchedulesTableSQL schemaName)
  , MigrationScript "cron-schedules-add-timezone" (encodeUtf8 $ addTimezoneColumnSQL schemaName)
  , MigrationScript "cron-schedules-add-queue-name" (encodeUtf8 $ addQueueNameColumnSQL schemaName)
  , MigrationScript "cron-schedules-add-run-requested" (encodeUtf8 $ addRunRequestedColumnSQL schemaName)
  , MigrationScript "cron-schedules-add-last-manual-run" (encodeUtf8 $ addLastManualRunColumnSQL schemaName)
  , MigrationScript "create-arbiter-workers" (encodeUtf8 $ createWorkersTableSQL schemaName)
  , MigrationScript "create-arbiter-queues" (encodeUtf8 $ createQueuesTableSQL schemaName)
  , MigrationScript "create-arbiter-gates" (encodeUtf8 $ createGatesTableSQL schemaName)
  , MigrationScript "arbiter-gates-add-metadata" (encodeUtf8 $ addGateMetadataColumnSQL schemaName)
  , MigrationScript "create-arbiter-rate-limit-policies" (encodeUtf8 $ createRateLimitPoliciesTableSQL schemaName)
  , MigrationScript "create-arbiter-rate-limits" (encodeUtf8 $ createRateLimitsTableSQL schemaName)
  , MigrationScript "create-arbiter-concurrency-policies" (encodeUtf8 $ createConcurrencyPoliciesTableSQL schemaName)
  , MigrationScript "create-arbiter-concurrency" (encodeUtf8 $ createConcurrencyTableSQL schemaName)
  ]

-- | All job queue migrations for a single table
--
-- This creates migrations for one table and its DLQ within a schema.
-- Each table gets its own set of migrations with unique version identifiers.
-- Optional notification and event-streaming objects are not tracked here,
-- 'reconcileOptionalTriggers' owns them.
jobQueueMigrationsForTable
  :: SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> TableAdmission
  -- ^ Which admission trigger kinds to install
  -> [MigrationCommand]
  -- ^ List of migration commands
jobQueueMigrationsForTable schemaName tableName adm =
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
        , script "create-groups-trigger-functions-v8" $ createGroupsTriggerFunctionsSQL schemaName tableName
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
        , -- After the archive table exists, since it alters that too.
          script "add-trace-context-column" $ addTraceContextColumnSQL schemaName tableName
        , script "add-claim-seq-column" $ addClaimSeqColumnSQL schemaName tableName
        , script "create-groups-emptied-index" $ createGroupsEmptiedIndexSQL schemaName tableName
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
   in coreMigrations <> concurrencyTriggers <> rateLimitTriggers

-- | Check whether a schema exists in the database.
schemaExists :: PG.Connection -> Text -> IO Bool
schemaExists conn schemaName = do
  rows <- query conn "SELECT 1 FROM pg_namespace WHERE nspname = ?" (Only schemaName) :: IO [Only Int]
  pure (not (null rows))

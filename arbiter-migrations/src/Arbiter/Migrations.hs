{-# LANGUAGE OverloadedStrings #-}

-- | Versioned, tracked migrations for job queue schemas, run once in order.
-- History lives in a @schema_migrations@ table inside the target schema.
-- Each run also seeds the admission policies and reconciles the rate-limit durability
-- and optional triggers in 'MigrationConfig'.
module Arbiter.Migrations
  ( -- * Configuration
    MigrationConfig (..)
  , defaultMigrationConfig

    -- * Tracked migrations
  , runMigrationsForRegistry
  , runMigrationsTrackedForTables
  , jobQueueMigrationsForTable
  , schemaLevelMigrations
  , AdmissionSeeds (..)
  , noAdmissionSeeds
  , TableAdmission (..)
  , allTableAdmission

    -- * Validation
  , validateRegistryNames
  , maxQueueNameBytes
  , conflictingPolicyPrefixes

    -- * Re-exports
  , QueueSpec (..)
  , Queue
  , MigrationCommand (..)
  , MigrationResult (..)
  , Durability (..)
  , RateLimitPolicy (..)
  , ConcurrencyPolicy (..)
  , SchemaName
  , TableName
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
import Arbiter.Core.Enum (showText)
import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Gates (addGateMetadataColumnSQL, createGatesTableSQL)
import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , addArchiveForColumnSQL
  , addCancelRequestedAtColumnSQL
  , addClaimSeqColumnSQL
  , addClaimedByColumnSQL
  , addKindColumnSQL
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
  , createJobQueueArchiveTableSQL
  , createJobQueueDLQTableSQL
  , createJobQueueTableSQL
  , createNotifyFunctionSQL
  , createNotifyTriggerSQL
  , createParentIdIndexSQL
  , createResultsTableSQL
  , createSchemaSQL
  , dlqSuffix
  , dropEventStreamingFunctionSQL
  , eventStreamingAdoptedObjectComment
  , eventStreamingDLQTriggerName
  , eventStreamingFunctionName
  , eventStreamingObjectComment
  , eventStreamingObjectCommentPrefix
  , eventStreamingTriggerName
  , jobQueueTable
  , legacyEventStreamingTriggers
  , migrateUngroupedReadySplitIndexesSQL
  , notifyAdoptedObjectComment
  , notifyFunctionName
  , notifyObjectComment
  , notifyObjectCommentPrefix
  , notifyTriggerName
  , pauseNotifyChannel
  , queueTableNames
  , setMaxAttemptsDefaultSQL
  )
import Arbiter.Core.Job.Schema.Groups
  ( createGroupsEmptiedIndexSQL
  , createGroupsTableSQL
  , createGroupsTriggerFunctionsSQL
  , createGroupsTriggersSQL
  , createJobQueueGroupInFlightIndexSQL
  , createJobQueueGroupKeyIndexSQL
  , createJobQueueGroupRetriedIndexSQL
  , createJobQueueGroupedDueIndexSQL
  , migrateGroupsReadyRankingSQL
  )
import Arbiter.Core.Job.Types (RegistryAdmissionPolicies)
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..), RegistryTables (..))
import Arbiter.Core.Queues (createQueuesTableSQL)
import Arbiter.Core.RateLimit.Schema
  ( addRateLimitColumnsSQL
  , addRateLimitCostColumnSQL
  , alterRateLimitsDurabilitySQL
  , arbiterRateLimitsTableName
  , createRateLimitBucketTriggerFunctionsSQL
  , createRateLimitBucketTriggersSQL
  , createRateLimitPoliciesTableSQL
  , createRateLimitsTableSQL
  , createThrottledIndexSQL
  , upsertRateLimitPolicyRowSQL
  )
import Arbiter.Core.RateLimit.Spec
  ( Durability (..)
  , RateLimitPolicy (..)
  , registryRateLimitPolicies
  , registryRateLimitTables
  )
import Arbiter.Core.SchemaTables (sharedArbiterTables)
import Arbiter.Core.SqlLiterals (maxIdentifierBytes)
import Arbiter.Core.Worker (createWorkersTableSQL)
import Control.Exception (bracket, catch, throwIO, try)
import Control.Monad (unless, void, when)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.Foldable (find, traverse_)
import Data.Int (Int32)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust, listToMaybe)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (decodeUtf8, encodeUtf8)
import Data.Time (NominalDiffTime)
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
import UnliftIO.Exception (tryAny)

-- | Migration options and the desired state reconciled after a schema's tracked migrations run.
data MigrationConfig = MigrationConfig
  { enableNotifications :: Bool
  -- ^ Install the LISTEN\/NOTIFY triggers that wake workers. Each run adds or drops
  -- them to match. Default: 'True'.
  , enableEventStreaming :: Bool
  -- ^ Install the admin UI's event-streaming triggers, which send a JSON event via
  -- @pg_notify@ for every insert, update and delete on a queue table and every insert on
  -- its DLQ. A lease extend sends no event. Each run adds or drops them to match, and
  -- drops the shared function when no trigger uses it.
  -- Default: 'False'.
  , rateLimitDurability :: Durability
  -- ^ WAL-logging for the rate-limit bucket table in this schema. 'Unlogged'
  -- (default) resets buckets on crash\/failover. 'Durable' preserves them at a
  -- throughput cost.
  , migrationLockTimeout :: Maybe NominalDiffTime
  -- ^ Seconds to wait for the schema's migration lock, then return 'MigrationError'.
  -- 'Nothing' (default) waits indefinitely.
  }
  deriving stock (Eq, Show)

-- | Notify triggers on, event streaming off, unlogged rate-limit buckets, no lock timeout.
defaultMigrationConfig :: MigrationConfig
defaultMigrationConfig =
  MigrationConfig
    { enableNotifications = True
    , enableEventStreaming = False
    , rateLimitDurability = Unlogged
    , migrationLockTimeout = Nothing
    }

-- | Migrate every queue in a registry into one schema. The schema itself is created
-- first, outside migration tracking. Names that fail 'validateRegistryNames' return
-- 'MigrationError' before the connection opens.
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
--               (Proxy \@AppRegistry)
--               "host=localhost dbname=mydb"
--               "arbiter"
--               defaultMigrationConfig
--   print result
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
  -- ^ Schema for the arbiter tables
  -> MigrationConfig
  -- ^ Migration options
  -> IO (MigrationResult String)
runMigrationsForRegistry proxy connStr schemaName config = do
  let ccTables = Map.fromList (registryConcurrencyTables @registry)
      rlTables = Map.fromList (registryRateLimitTables @registry)
      admissionFor table =
        TableAdmission
          { tableConcurrency = Map.findWithDefault False table ccTables
          , tableRateLimit = Map.findWithDefault False table rlTables
          }
      tables = [(table, admissionFor table) | table <- registryTableNames proxy]
      -- Policies come from each payload's 'rateLimitFor' and 'concurrencyFor' selectors.
      seeds =
        AdmissionSeeds
          { seedRateLimitPolicies = Set.toList (registryRateLimitPolicies @registry)
          , seedConcurrencyPolicies = Set.toList (registryConcurrencyPolicies @registry)
          }
  runMigrationsTrackedForTables connStr schemaName tables config seeds

-- | Admission policy rows to seed after a successful migration.
data AdmissionSeeds = AdmissionSeeds
  { seedRateLimitPolicies :: [RateLimitPolicy]
  -- ^ Rate-limit policies to upsert into the policies table.
  , seedConcurrencyPolicies :: [ConcurrencyPolicy]
  -- ^ Concurrency policies to upsert into the policies table.
  }

-- | Seeds for a deployment with no admission policies.
noAdmissionSeeds :: AdmissionSeeds
noAdmissionSeeds = AdmissionSeeds {seedRateLimitPolicies = [], seedConcurrencyPolicies = []}

-- | Which admission trigger kinds a table's payload declares. Trigger migrations are
-- install-only. A kind removed from a payload keeps its triggers.
data TableAdmission = TableAdmission
  { tableConcurrency :: Bool
  -- ^ Install the concurrency triggers.
  , tableRateLimit :: Bool
  -- ^ Install the rate-limit bucket triggers.
  }
  deriving stock (Eq, Show)

-- | Install every admission trigger kind.
allTableAdmission :: TableAdmission
allTableAdmission = TableAdmission {tableConcurrency = True, tableRateLimit = True}

-- | The longest queue name whose generated identifiers stay distinct after PostgreSQL
-- truncates them to 'maxIdentifierBytes'.
maxQueueNameBytes :: Int
maxQueueNameBytes = length (takeWhile identifiersDistinct [1 .. maxIdentifierBytes])
  where
    identifiersDistinct len = null (conflictingPrefixes (T.take maxIdentifierBytes) id (renderedIdentifiers (T.replicate len "q")))

-- | All identifiers in a queue's rendered DDL.
renderedIdentifiers :: TableName -> [Text]
renderedIdentifiers table =
  concatMap quoted $
    [decodeUtf8 body | MigrationScript _ body <- jobQueueMigrationsForTable probeSchema table allTableAdmission]
      <> [ createNotifyFunctionSQL probeSchema table
         , createNotifyTriggerSQL probeSchema table
         , createEventStreamingTriggersSQL probeSchema table
         ]
  where
    probeSchema = "arbiter"
    quoted = everyOther . T.splitOn "\""
    everyOther (_ : name : rest) = name : everyOther rest
    everyOther _ = []

-- | Reject an empty or over-long schema name, and queue names that are empty, exceed
-- 'maxQueueNameBytes', generate a schema-wide arbiter table, or generate the same
-- object or channel name as another queue.
validateRegistryNames :: SchemaName -> [TableName] -> Either Text ()
validateRegistryNames schemaName tables
  | T.null schemaName = Left "Arbiter schema name must not be empty"
  | byteLength schemaName > maxIdentifierBytes =
      Left ("Arbiter schema name exceeds PostgreSQL's " <> showText maxIdentifierBytes <> "-byte identifier limit")
  | any T.null tables = Left "Arbiter queue name must not be empty"
  | Just table <- find ((> maxQueueNameBytes) . byteLength) tables =
      Left
        ( "Arbiter queue name exceeds the "
            <> showText maxQueueNameBytes
            <> "-byte generated-identifier limit: "
            <> table
        )
  | Just reserved <- reservedCollision =
      Left ("Arbiter queue name generates a reserved arbiter table: " <> reserved)
  | Just generated <- generatedCollision =
      Left ("Arbiter queue names generate the same PostgreSQL object: " <> generated)
  | Just channel <- channelCollision =
      Left ("Arbiter queue names generate the same notification channel: " <> channel)
  | otherwise = Right ()
  where
    byteLength = BS.length . encodeUtf8
    generatedCollision =
      sharedName [(generated, table) | table <- tables, generated <- queueTableNames table]
    reservedCollision =
      find (`Set.member` Set.fromList (concatMap queueTableNames tables)) sharedArbiterTables
    channelCollision =
      sharedName
        [ (channel schemaName table, table)
        | table <- tables
        , channel <- [pauseNotifyChannel, cancelNotifyChannel]
        ]
    -- The first generated name more than one queue claims.
    sharedName = listToMaybe . conflictingPrefixes fst snd

-- | Migrate several tables in one schema. On success, seed the policies, then reconcile
-- durability and triggers on the same connection. The table list must be the schema's
-- whole queue set. An omitted queue loses its notify and event-streaming objects.
-- Names that fail 'validateRegistryNames' return 'MigrationError' before the
-- connection opens.
runMigrationsTrackedForTables
  :: ByteString
  -- ^ Database connection string
  -> SchemaName
  -- ^ Schema for the arbiter tables
  -> [(TableName, TableAdmission)]
  -- ^ Every queue table in the schema, with its admission trigger kinds
  -> MigrationConfig
  -- ^ Migration options
  -> AdmissionSeeds
  -- ^ Admission policy rows to seed
  -> IO (MigrationResult String)
runMigrationsTrackedForTables connStr schemaName tableAdmissions config seeds =
  case validateRegistryNames schemaName (map fst tableAdmissions) of
    Left err -> pure (MigrationError (T.unpack err))
    Right () -> bracket (connectPostgreSQL connStr) close $ \conn ->
      withMigrationLock conn schemaName (migrationLockTimeout config) $
        migrateSchema conn schemaName tableAdmissions config seeds

-- | Hold the schema's session-level migration lock for the run. Replicas that migrate at
-- the same time run one after another. A session lock spans the reconciles, which run
-- after the tracked migrations commit.
withMigrationLock
  :: PG.Connection
  -> SchemaName
  -> Maybe NominalDiffTime
  -> IO (MigrationResult String)
  -> IO (MigrationResult String)
withMigrationLock conn schemaName timeout act = bracket acquire release run
  where
    lockName = Only ("arbiter.migrations:" <> schemaName)
    acquire =
      try (PG.withTransaction conn (setTimeouts >> lock)) >>= \case
        Right () -> pure True
        Left sqlError
          | isJust timeout, PG.sqlState sqlError == lockNotAvailable -> pure False
          | otherwise -> throwIO sqlError
    lock = void (query conn "SELECT pg_advisory_lock(hashtextextended(?, 0))::text" lockName :: IO [Only (Maybe Text)])
    -- Pinned for this transaction.
    setTimeouts = traverse_ setLocal [("lock_timeout", maybe "0" millis timeout), ("statement_timeout", "0")]
    setLocal (name, value) =
      void (query conn "SELECT set_config(?, ?, TRUE)" (name :: Text, value :: Text) :: IO [Only Text])
    millis seconds = showText (max 1 (min maxLockTimeoutMillis (ceiling (seconds * millisPerSecond) :: Int)))
    maxLockTimeoutMillis = fromIntegral (maxBound :: Int32)
    millisPerSecond = 1000
    lockNotAvailable = "55P03"
    release locked =
      when locked $
        void (try unlock :: IO (Either PG.SqlError [Only Bool]))
    unlock = query conn "SELECT pg_advisory_unlock(hashtextextended(?, 0))" lockName :: IO [Only Bool]
    run locked
      | locked = act
      | otherwise =
          pure (MigrationError ("Timed out waiting on the arbiter migration lock for schema " <> T.unpack schemaName))

-- | Apply a schema's tracked migrations and reconcile it, under the migration lock.
migrateSchema
  :: PG.Connection
  -> SchemaName
  -> [(TableName, TableAdmission)]
  -> MigrationConfig
  -> AdmissionSeeds
  -> IO (MigrationResult String)
migrateSchema conn schemaName tableAdmissions config seeds = do
  withConnection conn LibPQ.disableNoticeReporting

  ensureSchema conn schemaName

  withConnection conn LibPQ.enableNoticeReporting

  let schemaMigrations = schemaLevelMigrations schemaName
      tableMigrations = concatMap (uncurry (jobQueueMigrationsForTable schemaName)) tableAdmissions
      migrations = schemaMigrations <> tableMigrations
      migrationTableName = encodeUtf8 $ schemaName <> ".schema_migrations"
      options =
        defaultOptions
          { optVerbose = Quiet
          , optTableName = migrationTableName
          }

  void (runMigrations conn options [MigrationInitialization])
  migrationResult <- runMigrations conn options migrations
  case migrationResult of
    -- A reconcile throws on a conflicting prefix or a failed ALTER. Async exceptions propagate.
    MigrationSuccess ->
      either (MigrationError . T.unpack . displayEx) (const MigrationSuccess) <$> tryAny reconcile
    other -> pure other
  where
    reconcile = do
      reconcileRateLimitPolicies conn schemaName (seedRateLimitPolicies seeds)
      reconcileConcurrencyPolicies conn schemaName (seedConcurrencyPolicies seeds)
      reconcileRateLimitDurability conn schemaName (rateLimitDurability config)
      reconcileOptionalTriggers conn schemaName (map fst tableAdmissions) config

-- | Create the schema. A failed CREATE is fine when the schema exists.
ensureSchema :: PG.Connection -> SchemaName -> IO ()
ensureSchema conn schemaName =
  executeSQL conn (createSchemaSQL schemaName) `catch` \(createError :: PG.SqlError) -> do
    present <- schemaExists conn schemaName
    unless present $
      ioError
        ( userError $
            "Failed to create schema "
              <> T.unpack schemaName
              <> " and it does not exist. Either grant CREATE privilege on the database"
              <> " or create the schema manually: CREATE SCHEMA "
              <> T.unpack schemaName
              <> ";"
              <> "\nOriginal error: "
              <> show createError
        )

-- | Upsert each policy's @default_*@ params into the policies table. Operator
-- @override_*@ values stay intact. Idempotent. Overrides and removed prefixes survive
-- a deploy.
reconcileRateLimitPolicies :: PG.Connection -> SchemaName -> [RateLimitPolicy] -> IO ()
reconcileRateLimitPolicies =
  reconcilePolicyRows "rate-limit policy" "parameters" policyPrefix policyParamsKey upsertRateLimitPolicyRowSQL

-- | Rate-limit policy prefixes declared with more than one parameter set.
conflictingPolicyPrefixes :: [RateLimitPolicy] -> [Text]
conflictingPolicyPrefixes = conflictingPrefixes policyPrefix policyParamsKey

-- | A canonical conflict key for a policy's params, rendered as text. A NaN compares
-- equal to itself.
policyParamsKey :: RateLimitPolicy -> String
policyParamsKey policy = show (policyMax policy, policyRefill policy, policyInterval policy)

-- | Upsert each policy's @default_limit@, leaving operator overrides intact. Two policies
-- with the same prefix but different limits fail the migration.
reconcileConcurrencyPolicies :: PG.Connection -> SchemaName -> [ConcurrencyPolicy] -> IO ()
reconcileConcurrencyPolicies =
  reconcilePolicyRows "concurrency policy" "limits" cpPrefix cpLimit upsertConcurrencyPolicyRowSQL

-- | Upsert each policy row's @default_*@ params. Fails on a prefix that contains the
-- @:@ key separator or is declared with conflicting params. Idempotent. Operator
-- overrides and removed prefixes survive a deploy.
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
  | Just prefix <- find (T.isInfixOf ":") (map prefixOf rows) =
      ioError
        ( userError $
            label
              <> " prefix "
              <> T.unpack prefix
              <> " contains ':', the key separator, so its keys would alias another prefix's. Use a colon-free prefix."
        )
  | (prefix : _) <- conflictingPrefixes prefixOf paramsOf rows =
      ioError
        ( userError $
            label <> " prefix " <> T.unpack prefix <> " is declared with conflicting " <> noun <> ". Give each a unique prefix."
        )
  | otherwise =
      PG.withTransaction conn $
        traverse_ (executeSQL conn . upsertSQL schemaName) rows

-- | Prefixes declared with more than one distinct parameter set.
conflictingPrefixes :: (Ord params) => (row -> Text) -> (row -> params) -> [row] -> [Text]
conflictingPrefixes prefixOf paramsOf rows =
  Map.keys (Map.filter ((> 1) . Set.size) paramsByPrefix)
  where
    paramsByPrefix =
      Map.fromListWith Set.union [(prefixOf row, Set.singleton (paramsOf row)) | row <- rows]

-- | Converge the bucket table's WAL persistence to the declared durability. Reads
-- the current @pg_class.relpersistence@ and issues @SET LOGGED@ or @SET UNLOGGED@ on a
-- change. The ALTER rewrites the table under @ACCESS EXCLUSIVE@. Token consumes block
-- briefly while a switch runs.
reconcileRateLimitDurability :: PG.Connection -> SchemaName -> Durability -> IO ()
reconcileRateLimitDurability conn schemaName durability = do
  rows <-
    query
      conn
      "SELECT relation.relpersistence::text FROM pg_class relation \
      \JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace \
      \WHERE namespace.nspname = ? AND relation.relname = ?"
      (schemaName, arbiterRateLimitsTableName)
  let target = case durability of
        Durable -> "p" :: Text
        Unlogged -> "u"
  traverse_
    (\(Only current) -> when (current /= target) $ executeSQL conn (alterRateLimitsDurabilitySQL schemaName durability))
    (listToMaybe rows)

-- | Reconcile optional triggers schema-wide after tracked migrations. Install or drop
-- each option's objects to match the config. Drop objects for queues outside the
-- supplied table list. Migration history remains unchanged.
reconcileOptionalTriggers :: PG.Connection -> SchemaName -> [TableName] -> MigrationConfig -> IO ()
reconcileOptionalTriggers conn schemaName tables config =
  PG.withTransaction conn $ do
    adoptUnmarkedObjects conn schemaName tables
    reconcileNotify conn schemaName tables (enableNotifications config)
    reconcileEventStreaming conn schemaName tables (enableEventStreaming config)

-- | Install notify objects for each queue and drop the rest, or drop them all when disabled.
reconcileNotify :: PG.Connection -> SchemaName -> [TableName] -> Bool -> IO ()
reconcileNotify conn schemaName tables enabled
  | enabled = do
      dropNotifyObjectsExcept conn schemaName tables
      traverse_ (installNotify conn schemaName) tables
  | otherwise = dropNotifyObjectsExcept conn schemaName []

-- | Install event-stream objects for each queue and drop the rest, or drop them all when disabled.
reconcileEventStreaming :: PG.Connection -> SchemaName -> [TableName] -> Bool -> IO ()
reconcileEventStreaming conn schemaName tables enabled
  | enabled = do
      executeSQL conn (createEventStreamingFunctionSQL schemaName)
      dropEventTriggersExcept conn schemaName (eventTables tables)
      traverse_ (installEventStream conn schemaName) tables
  | otherwise = do
      dropEventTriggersExcept conn schemaName []
      dropEventFunctionIfUnused conn schemaName

-- | Install one queue's notify function and trigger. Functions are replaced in place.
-- That takes no lock on the queue table. Triggers are rebuilt when their marker is off
-- the current version.
installNotify :: PG.Connection -> SchemaName -> TableName -> IO ()
installNotify conn schemaName table = do
  executeSQL conn (createNotifyFunctionSQL schemaName table)
  current <- triggerIsCurrent conn schemaName table (notifyTriggerName table) notifyObjectComment
  unless current $ executeSQL conn (createNotifyTriggerSQL schemaName table)

-- | Install one queue's event-stream triggers on its main and DLQ tables when either
-- marker is off the current version.
installEventStream :: PG.Connection -> SchemaName -> TableName -> IO ()
installEventStream conn schemaName table = do
  mainCurrent <- triggerIsCurrent conn schemaName table (eventStreamingTriggerName table) eventStreamingObjectComment
  dlqCurrent <-
    triggerIsCurrent conn schemaName (table <> dlqSuffix) (eventStreamingDLQTriggerName table) eventStreamingObjectComment
  unless (mainCurrent && dlqCurrent) $
    executeSQL conn (createEventStreamingTriggersSQL schemaName table)

-- | Drop the shared event-stream function when arbiter marked it and no trigger uses it.
dropEventFunctionIfUnused :: PG.Connection -> SchemaName -> IO ()
dropEventFunctionIfUnused conn schemaName = do
  ours <- eventFunctionIsMarked
  -- The function stays while foreign triggers depend on it.
  depended <- eventFunctionHasTriggers
  when (ours && not depended) $ executeSQL conn (dropEventStreamingFunctionSQL schemaName)
  where
    eventFunctionIsMarked =
      exists
        conn
        ( "SELECT EXISTS (SELECT 1 "
            <> functionJoins
            <> "WHERE namespace.nspname = ? AND function.proname = ? AND function.pronargs = 0 \
               \AND obj_description(function.oid, 'pg_proc') LIKE ?)"
        )
        (schemaName, eventStreamingFunctionName, eventStreamingObjectCommentPrefix <> "%")

    eventFunctionHasTriggers =
      exists
        conn
        "SELECT EXISTS (SELECT 1 FROM pg_trigger trigger JOIN pg_proc function ON function.oid = trigger.tgfoid \
        \JOIN pg_namespace namespace ON namespace.oid = function.pronamespace \
        \WHERE namespace.nspname = ? AND function.proname = ? AND NOT trigger.tgisinternal)"
        (schemaName, eventStreamingFunctionName)

-- | Drop event-stream triggers outside the @keep@ tables.
dropEventTriggersExcept :: PG.Connection -> SchemaName -> [TableName] -> IO ()
dropEventTriggersExcept conn schemaName = dropTriggersMarked conn schemaName eventStreamingObjectCommentPrefix

-- | Drop notify triggers and functions for queues outside @keep@.
dropNotifyObjectsExcept :: PG.Connection -> SchemaName -> [TableName] -> IO ()
dropNotifyObjectsExcept conn schemaName keep = do
  dropTriggersMarked conn schemaName notifyObjectCommentPrefix keep
  -- A function that still carries a trigger stays.
  executeRendered
    conn
    ( "SELECT format('DROP FUNCTION IF EXISTS %I.%I();', namespace.nspname, function.proname) "
        <> functionJoins
        <> "WHERE namespace.nspname = ? AND obj_description(function.oid, 'pg_proc') LIKE ? AND function.pronargs = 0 \
           \AND NOT (function.proname::text = ANY(?::text[])) \
           \AND NOT EXISTS (SELECT 1 FROM pg_trigger trigger WHERE trigger.tgfoid = function.oid AND NOT trigger.tgisinternal)"
    )
    (schemaName, notifyObjectCommentPrefix <> "%", PGArray (map notifyFunctionName keep))

-- | Objects installed before arbiter stamped markers carry no comment. Adoption
-- stamps a marker that no install writes. The sweeps match it. The currency probe
-- rejects it. Adoption covers the names this registry generates. An unmarked
-- lookalike stays foreign.
adoptUnmarkedObjects :: PG.Connection -> SchemaName -> [TableName] -> IO ()
adoptUnmarkedObjects conn schemaName tables = do
  executeRendered
    conn
    ( commentFunctionSelect
        <> functionJoins
        <> "WHERE namespace.nspname = ? AND function.proname = ANY(?::text[]) AND function.pronargs = 0 \
           \AND obj_description(function.oid, 'pg_proc') IS NULL"
    )
    (notifyAdoptedObjectComment, schemaName, PGArray (map notifyFunctionName tables))
  adoptTriggers notifyAdoptedObjectComment (map notifyTriggerName tables) tables
  adoptTriggers eventStreamingAdoptedObjectComment eventTriggerNames (eventTables tables)
  -- The shared function is adopted when one of its triggers is.
  executeRendered
    conn
    ( commentFunctionSelect
        <> functionJoins
        <> "WHERE namespace.nspname = ? AND function.proname = ? AND function.pronargs = 0 \
           \AND obj_description(function.oid, 'pg_proc') IS NULL \
           \AND EXISTS (SELECT 1 FROM pg_trigger trigger WHERE trigger.tgfoid = function.oid AND NOT trigger.tgisinternal \
           \AND obj_description(trigger.oid, 'pg_trigger') LIKE ?)"
    )
    ( eventStreamingAdoptedObjectComment
    , schemaName
    , eventStreamingFunctionName
    , eventStreamingObjectCommentPrefix <> "%"
    )
  where
    adoptTriggers marker triggerNames relNames =
      executeRendered
        conn
        ( commentTriggerSelect
            <> triggerJoins
            <> "WHERE namespace.nspname = ? AND NOT trigger.tgisinternal AND trigger.tgname = ANY(?::text[]) \
               \AND relation.relname = ANY(?::text[]) AND obj_description(trigger.oid, 'pg_trigger') IS NULL"
        )
        (marker, schemaName, PGArray triggerNames, PGArray relNames)

    eventTriggerNames =
      map fst legacyEventStreamingTriggers
        <> concatMap (\table -> [eventStreamingTriggerName table, eventStreamingDLQTriggerName table]) tables

-- | Each queue's main and DLQ tables, which carry event-stream triggers.
eventTables :: [TableName] -> [TableName]
eventTables = concatMap (\table -> [table, table <> dlqSuffix])

-- | Run one rendered DDL or DML statement.
executeSQL :: PG.Connection -> Text -> IO ()
executeSQL conn = void . execute_ conn . Query . encodeUtf8

-- | The catalog joins the trigger sweeps share.
triggerJoins :: Query
triggerJoins =
  "FROM pg_trigger trigger \
  \JOIN pg_class relation ON relation.oid = trigger.tgrelid \
  \JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace "

-- | The catalog joins the function sweeps share.
functionJoins :: Query
functionJoins = "FROM pg_proc function JOIN pg_namespace namespace ON namespace.oid = function.pronamespace "

-- | A @COMMENT ON FUNCTION@ statement per matched row, stamping the first parameter.
commentFunctionSelect :: Query
commentFunctionSelect = "SELECT format('COMMENT ON FUNCTION %I.%I() IS %L;', namespace.nspname, function.proname, ?::text) "

-- | Whether a single-row boolean probe returns true.
exists :: (PG.ToRow params) => PG.Connection -> Query -> params -> IO Bool
exists conn sql params = maybe False fromOnly . listToMaybe <$> query conn sql params

-- | Whether a trigger exists on a table and carries the given marker.
triggerIsCurrent :: PG.Connection -> SchemaName -> TableName -> Text -> Text -> IO Bool
triggerIsCurrent conn schemaName table trigger marker =
  exists
    conn
    ( "SELECT EXISTS (SELECT 1 "
        <> triggerJoins
        <> "WHERE namespace.nspname = ? AND relation.relname = ? AND trigger.tgname = ? \
           \AND obj_description(trigger.oid, 'pg_trigger') = ?)"
    )
    (schemaName, table, trigger, marker)

-- | Drop the triggers whose marker starts with @marker@, outside the @keep@ tables.
dropTriggersMarked :: PG.Connection -> SchemaName -> Text -> [TableName] -> IO ()
dropTriggersMarked conn schemaName marker keep =
  executeRendered
    conn
    ( dropTriggerSelect
        <> triggerJoins
        <> "WHERE namespace.nspname = ? AND obj_description(trigger.oid, 'pg_trigger') LIKE ? \
           \AND NOT (relation.relname::text = ANY(?::text[]))"
    )
    (schemaName, marker <> "%", PGArray keep)

-- | Run each statement a rendering query returns.
executeRendered :: (PG.ToRow params) => PG.Connection -> Query -> params -> IO ()
executeRendered conn sql params = do
  commands <- query conn sql params
  traverse_ (\(Only command) -> executeSQL conn command) commands

-- | A @DROP TRIGGER@ statement per matched row.
dropTriggerSelect :: Query
dropTriggerSelect = "SELECT format('DROP TRIGGER IF EXISTS %I ON %I.%I;', trigger.tgname, namespace.nspname, relation.relname) "

-- | A @COMMENT ON TRIGGER@ statement per matched row, stamping the first parameter.
commentTriggerSelect :: Query
commentTriggerSelect =
  "SELECT format('COMMENT ON TRIGGER %I ON %I.%I IS %L;', trigger.tgname, namespace.nspname, relation.relname, ?::text) "

-- | The schema-level migrations, run once per schema. The optional notification and
-- event-streaming objects are reconciled after the migrations run.
schemaLevelMigrations :: SchemaName -> [MigrationCommand]
schemaLevelMigrations schemaName =
  [ script "create-cron-schedules" createCronSchedulesTableSQL
  , script "cron-schedules-add-timezone" addTimezoneColumnSQL
  , script "cron-schedules-add-queue-name" addQueueNameColumnSQL
  , script "cron-schedules-add-run-requested" addRunRequestedColumnSQL
  , script "cron-schedules-add-last-manual-run" addLastManualRunColumnSQL
  , script "create-arbiter-workers" createWorkersTableSQL
  , script "create-arbiter-queues" createQueuesTableSQL
  , script "create-arbiter-gates" createGatesTableSQL
  , script "arbiter-gates-add-metadata" addGateMetadataColumnSQL
  , script "create-arbiter-rate-limit-policies" createRateLimitPoliciesTableSQL
  , script "create-arbiter-rate-limits" createRateLimitsTableSQL
  , script "create-arbiter-concurrency-policies" createConcurrencyPoliciesTableSQL
  , script "create-arbiter-concurrency" createConcurrencyTableSQL
  ]
  where
    script name sql = MigrationScript name (encodeUtf8 (sql schemaName))

-- | One queue's tracked migrations, each under its own version identifier.
-- The optional notify and event-streaming objects are reconciled after the migrations run.
jobQueueMigrationsForTable
  :: SchemaName
  -- ^ Schema for the arbiter tables
  -> TableName
  -- ^ Queue table name
  -> TableAdmission
  -- ^ Which admission trigger kinds to install
  -> [MigrationCommand]
jobQueueMigrationsForTable schemaName tableName admission =
  coreMigrations <> concurrencyTriggers <> rateLimitTriggers
  where
    prefix = T.unpack tableName <> "-"
    script name sql = MigrationScript (prefix <> name) (encodeUtf8 (sql schemaName tableName))

    coreMigrations =
      [ script "create-table" createJobQueueTableSQL
      , script "create-dlq-table" createJobQueueDLQTableSQL
      , script "create-dlq-group-key-index" createDLQGroupKeyIndexSQL
      , script "create-dlq-failed-at-index" createDLQFailedAtIndexSQL
      , script "create-dedup-key-index" createDedupKeyIndexSQL
      , script "create-group-key-index" createJobQueueGroupKeyIndexSQL
      , script "create-parent-id-index" createParentIdIndexSQL
      , script "create-dlq-parent-id-index" createDLQParentIdIndexSQL
      , script "create-results-table" createResultsTableSQL
      , script "create-groups-table" createGroupsTableSQL
      , script "add-claimed-by-column" addClaimedByColumnSQL
      , script "migrate-ungrouped-ready-split-indexes" migrateUngroupedReadySplitIndexesSQL
      , script "migrate-groups-ready-ranking" migrateGroupsReadyRankingSQL
      , script "add-rate-limit-columns" addRateLimitColumnsSQL
      , script "create-throttled-index" createThrottledIndexSQL
      , script "add-concurrency-columns" addConcurrencyColumnsSQL
      , script "create-concurrency-index" createConcurrencyIndexSQL
      , script "add-rate-limit-cost-column" addRateLimitCostColumnSQL
      , script "set-job-queue-fillfactor-100" setJobQueueFillfactorSQL
      , script "set-max-attempts-default" setMaxAttemptsDefaultSQL
      , script "add-cancel-requested-at-column" addCancelRequestedAtColumnSQL
      , script "add-archive-for-column" addArchiveForColumnSQL
      , script "create-archive-table" createJobQueueArchiveTableSQL
      , script "create-archive-completed-at-index" createArchiveCompletedAtIndexSQL
      , script "create-archive-expires-at-index" createArchiveExpiresAtIndexSQL
      , script "create-archive-job-id-index" createArchiveJobIdIndexSQL
      , script "create-archive-parent-id-index" createArchiveParentIdIndexSQL
      , script "create-archive-group-key-index" createArchiveGroupKeyIndexSQL
      , -- Runs after the archive table exists. It alters that table too.
        script "add-trace-context-column" addTraceContextColumnSQL
      , script "add-claim-seq-column" addClaimSeqColumnSQL
      , script "create-groups-emptied-index" createGroupsEmptiedIndexSQL
      , script "create-group-retried-index" createJobQueueGroupRetriedIndexSQL
      , script "create-grouped-due-index" createJobQueueGroupedDueIndexSQL
      , script "create-group-in-flight-index" createJobQueueGroupInFlightIndexSQL
      , script "create-groups-trigger-functions-v12" createGroupsTriggerFunctionsSQL
      , script "create-groups-triggers" createGroupsTriggersSQL
      , script "add-kind-column" addKindColumnSQL
      ]
    concurrencyTriggers
      | tableConcurrency admission =
          [ script "create-concurrency-trigger-functions" createConcurrencyTriggerFunctionsSQL
          , script "create-concurrency-triggers" createConcurrencyTriggersSQL
          ]
      | otherwise = []
    rateLimitTriggers
      | tableRateLimit admission =
          [ script "create-rate-limit-bucket-trigger-functions" createRateLimitBucketTriggerFunctionsSQL
          , script "create-rate-limit-bucket-triggers" createRateLimitBucketTriggersSQL
          ]
      | otherwise = []

    setJobQueueFillfactorSQL schema table = "ALTER TABLE " <> jobQueueTable schema table <> " SET (fillfactor = 100);"

-- | Whether a schema exists.
schemaExists :: PG.Connection -> SchemaName -> IO Bool
schemaExists conn schemaName =
  exists conn "SELECT EXISTS (SELECT 1 FROM pg_namespace WHERE nspname = ?)" (Only schemaName)

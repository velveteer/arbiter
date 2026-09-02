{-# LANGUAGE OverloadedStrings #-}

-- | Versioned, tracked migrations for job queue schemas, run in order and never rerun.
-- History lives in a @schema_migrations@ table inside the target schema.
module Arbiter.Migrations
  ( -- * Registry
    QueueSpec (..)
  , Queue

    -- * Configuration
  , MigrationConfig (..)
  , defaultMigrationConfig
  , validateRegistryNames
  , maxQueueNameBytes

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
  , createJobQueueArchiveTableSQL
  , createJobQueueDLQTableSQL
  , createJobQueueTableSQL
  , createNotifyFunctionSQL
  , createNotifyTriggerSQL
  , createParentIdIndexSQL
  , createResultsTableSQL
  , createSchemaSQL
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
import Arbiter.Core.SchemaTables (sharedArbiterTables)
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

-- | The desired state reconciled after a schema's tracked migrations run.
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
  , migrationLockTimeout :: Maybe NominalDiffTime
  -- ^ How many seconds to wait for the schema's migration lock, which serializes
  -- replicas that migrate at the same time. 'Nothing' (default) waits indefinitely.
  }
  deriving stock (Eq, Show)

-- | Notify triggers on, event streaming off, unlogged rate-limit buckets.
defaultMigrationConfig :: MigrationConfig
defaultMigrationConfig =
  MigrationConfig
    { enableNotifications = True
    , enableEventStreaming = False
    , rateLimitDurability = Unlogged
    , migrationLockTimeout = Nothing
    }

-- | Migrate every queue in a registry into one schema. The schema itself is created
-- first, outside migration tracking.
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

-- | The longest queue name whose generated identifiers survive PostgreSQL's 63-byte
-- truncation distinct. Derived by rendering a probe queue's own DDL at each length, so
-- an object added with a name that truncates into a sibling's lowers this on its own.
maxQueueNameBytes :: Int
maxQueueNameBytes = length (takeWhile identifiersDistinct [1 .. 63])
  where
    identifiersDistinct n = null (conflictingPrefixes (T.take 63) id (renderedIdentifiers (T.replicate n "q")))

-- | All identifiers in a queue's rendered DDL. This derives the set directly
-- from the SQL and includes newly added objects.
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

-- | Reject queue names that generate a schema-wide arbiter table, or that generate
-- object or channel names PostgreSQL truncates into each other. Truncation itself is
-- harmless, so the length limit is where two of a queue's generated names collide.
validateRegistryNames :: SchemaName -> [TableName] -> Either Text ()
validateRegistryNames schemaName tables
  | T.null schemaName = Left "Arbiter schema name must not be empty"
  | byteLength schemaName > 63 = Left "Arbiter schema name exceeds PostgreSQL's 63-byte identifier limit"
  | any T.null tables = Left "Arbiter queue name must not be empty"
  | Just table <- find ((> maxQueueNameBytes) . byteLength) tables =
      Left
        ( "Arbiter queue name exceeds the "
            <> T.pack (show maxQueueNameBytes)
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
runMigrationsTrackedForTables connStr schemaName tableNames config seeds =
  case validateRegistryNames schemaName (map fst tableNames) of
    Left err -> pure (MigrationError (T.unpack err))
    Right () -> bracket (connectPostgreSQL connStr) close $ \conn ->
      withMigrationLock conn schemaName (migrationLockTimeout config) $
        migrateSchema conn schemaName tableNames config seeds

-- | Hold the schema's migration lock for the whole session, so replicas that migrate
-- at the same time run one after another. A session lock is what spans the reconciles,
-- which run after the tracked migrations commit.
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
        Left e
          | isJust timeout, PG.sqlState e == "55P03" -> pure False
          | otherwise -> throwIO e
    lock = void (query conn "SELECT pg_advisory_lock(hashtextextended(?, 0))::text" lockName :: IO [Only (Maybe Text)])
    -- Pinned for this transaction only, so an inherited timeout cannot cut the wait short.
    setTimeouts = traverse_ setLocal [("lock_timeout", maybe "0" millis timeout), ("statement_timeout", "0")]
    setLocal (name, value) =
      void (query conn "SELECT set_config(?, ?, TRUE)" (name :: Text, value :: Text) :: IO [Only Text])
    millis t = T.pack (show (max 1 (min maxLockTimeoutMillis (ceiling (t * 1000) :: Int))))
    maxLockTimeoutMillis = 2147483647
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
migrateSchema conn schemaName tableNames config (AdmissionSeeds policyRows concRows durability) = do
  withConnection conn $ \libpqConn ->
    LibPQ.disableNoticeReporting libpqConn

  -- A CREATE that fails for want of privilege is fine if someone made the schema by hand.
  let schemaSQL = Query (encodeUtf8 $ createSchemaSQL schemaName)
  result <- try $ execute_ conn schemaSQL
  case result of
    Right _ -> pure ()
    Left (e :: PG.SqlError) -> do
      exists <- schemaExists conn schemaName
      unless exists $
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

  withConnection conn $ \libpqConn ->
    LibPQ.enableNoticeReporting libpqConn

  let schemaMigrations = schemaLevelMigrations schemaName
      tableMigrations = concatMap (uncurry (jobQueueMigrationsForTable schemaName)) tableNames
      migrations = schemaMigrations <> tableMigrations
      migrationTableName = encodeUtf8 $ schemaName <> ".schema_migrations"
      options =
        defaultOptions
          { optVerbose = Quiet
          , optTableName = migrationTableName
          }

  _ <- runMigrations conn options [MigrationInitialization]
  migrationResult <- runMigrations conn options migrations
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
        -- A reconcile throws on a conflicting prefix or a failed ALTER. Async exceptions propagate.
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

-- | Converge the optional triggers after the tracked migrations, restoring what an
-- earlier disabled configuration removed without touching migration history. The sweep is
-- schema-wide by design: the table list is the schema's whole queue set, so an object
-- belonging to a queue outside it is left over from a registry that shrank, and is
-- dropped.
reconcileOptionalTriggers :: PG.Connection -> SchemaName -> [TableName] -> MigrationConfig -> IO ()
reconcileOptionalTriggers conn schemaName tables config =
  PG.withTransaction conn $ do
    adoptUnmarkedObjects
    if enableNotifications config
      then do
        dropNotifyObjectsExcept tables
        traverse_ installNotify tables
      else dropNotifyObjectsExcept []
    if enableEventStreaming config
      then do
        executeSQL (createEventStreamingFunctionSQL schemaName)
        dropEventTriggersExcept eventTables
        traverse_ installEventStream tables
      else do
        dropEventTriggersExcept []
        ours <- eventFunctionIsMarked
        -- Triggers left after the sweep are not ours, so the function they depend on stays.
        depended <- eventFunctionHasTriggers
        when (ours && not depended) $ executeSQL (dropEventStreamingFunctionSQL schemaName)
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

    triggerIsCurrent table trigger marker =
      exists
        ( "SELECT EXISTS (SELECT 1 "
            <> triggerJoins
            <> "WHERE n.nspname = ? AND c.relname = ? AND t.tgname = ? \
               \AND obj_description(t.oid, 'pg_trigger') = ?)"
        )
        (schemaName, table, trigger, marker)

    eventFunctionIsMarked =
      exists
        "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace \
        \WHERE n.nspname = ? AND p.proname = ? AND p.pronargs = 0 \
        \AND obj_description(p.oid, 'pg_proc') LIKE ?)"
        (schemaName, eventStreamingFunctionName, eventStreamingObjectCommentPrefix <> "%")

    eventFunctionHasTriggers =
      exists
        "SELECT EXISTS (SELECT 1 FROM pg_trigger t JOIN pg_proc p ON p.oid = t.tgfoid \
        \JOIN pg_namespace n ON n.oid = p.pronamespace \
        \WHERE n.nspname = ? AND p.proname = ? AND NOT t.tgisinternal)"
        (schemaName, eventStreamingFunctionName)

    exists :: (PG.ToRow params) => Query -> params -> IO Bool
    exists sql params = do
      rows <- query conn sql params
      pure (maybe False fromOnly (listToMaybe rows))

    dropTriggersMarked marker keep =
      executeRendered
        ( dropTriggerSelect
            <> triggerJoins
            <> "WHERE n.nspname = ? AND obj_description(t.oid, 'pg_trigger') LIKE ? \
               \AND NOT (c.relname::text = ANY(?::text[]))"
        )
        (schemaName, marker <> "%", PGArray keep)

    dropEventTriggersExcept = dropTriggersMarked eventStreamingObjectCommentPrefix

    dropNotifyObjectsExcept keep = do
      dropTriggersMarked notifyObjectCommentPrefix keep
      -- A function still carrying a trigger is one the sweep spared, so it stays.
      executeRendered
        "SELECT format('DROP FUNCTION IF EXISTS %I.%I();', n.nspname, p.proname) \
        \FROM pg_proc p \
        \JOIN pg_namespace n ON n.oid = p.pronamespace \
        \WHERE n.nspname = ? AND obj_description(p.oid, 'pg_proc') LIKE ? AND p.pronargs = 0 \
        \AND NOT (p.proname::text = ANY(?::text[])) \
        \AND NOT EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgfoid = p.oid AND NOT t.tgisinternal)"
        (schemaName, notifyObjectCommentPrefix <> "%", PGArray (map notifyFunctionName keep))

    -- Objects installed before arbiter stamped markers carry no comment, so both
    -- sweeps miss them. Adoption stamps a marker no install ever writes, which the
    -- sweeps match and the currency probe never accepts. Only the names this registry
    -- generates are adopted, so an unmarked lookalike stays foreign.
    adoptUnmarkedObjects = do
      executeRendered
        "SELECT format('COMMENT ON FUNCTION %I.%I() IS %L;', n.nspname, p.proname, ?::text) \
        \FROM pg_proc p \
        \JOIN pg_namespace n ON n.oid = p.pronamespace \
        \WHERE n.nspname = ? AND p.proname = ANY(?::text[]) AND p.pronargs = 0 \
        \AND obj_description(p.oid, 'pg_proc') IS NULL"
        (notifyAdoptedObjectComment, schemaName, PGArray (map notifyFunctionName tables))
      adoptTriggers notifyAdoptedObjectComment (map notifyTriggerName tables) tables
      adoptTriggers eventStreamingAdoptedObjectComment eventTriggerNames eventTables
      -- The shared function is ours only once one of its triggers is.
      executeRendered
        "SELECT format('COMMENT ON FUNCTION %I.%I() IS %L;', n.nspname, p.proname, ?::text) \
        \FROM pg_proc p \
        \JOIN pg_namespace n ON n.oid = p.pronamespace \
        \WHERE n.nspname = ? AND p.proname = ? AND p.pronargs = 0 \
        \AND obj_description(p.oid, 'pg_proc') IS NULL \
        \AND EXISTS (SELECT 1 FROM pg_trigger t WHERE t.tgfoid = p.oid AND NOT t.tgisinternal \
        \AND obj_description(t.oid, 'pg_trigger') LIKE ?)"
        ( eventStreamingAdoptedObjectComment
        , schemaName
        , eventStreamingFunctionName
        , eventStreamingObjectCommentPrefix <> "%"
        )

    adoptTriggers marker triggerNames relNames =
      executeRendered
        ( commentTriggerSelect
            <> triggerJoins
            <> "WHERE n.nspname = ? AND NOT t.tgisinternal AND t.tgname = ANY(?::text[]) \
               \AND c.relname = ANY(?::text[]) AND obj_description(t.oid, 'pg_trigger') IS NULL"
        )
        (marker, schemaName, PGArray triggerNames, PGArray relNames)

    eventTables = concatMap (\table -> [table, table <> "_dlq"]) tables

    eventTriggerNames =
      map fst legacyEventStreamingTriggers
        <> concatMap (\table -> [eventStreamingTriggerName table, eventStreamingDLQTriggerName table]) tables

    executeRendered :: (PG.ToRow params) => Query -> params -> IO ()
    executeRendered sql params = do
      commands <- query conn sql params
      traverse_ (\(Only command) -> executeSQL command) commands

    executeSQL = void . execute_ conn . Query . encodeUtf8

-- | The catalog joins the trigger sweeps share.
triggerJoins :: Query
triggerJoins =
  "FROM pg_trigger t \
  \JOIN pg_class c ON c.oid = t.tgrelid \
  \JOIN pg_namespace n ON n.oid = c.relnamespace "

-- | A @DROP TRIGGER@ statement per matched row.
dropTriggerSelect :: Query
dropTriggerSelect = "SELECT format('DROP TRIGGER IF EXISTS %I ON %I.%I;', t.tgname, n.nspname, c.relname) "

-- | A @COMMENT ON TRIGGER@ statement per matched row, stamping the first parameter.
commentTriggerSelect :: Query
commentTriggerSelect = "SELECT format('COMMENT ON TRIGGER %I ON %I.%I IS %L;', t.tgname, n.nspname, c.relname, ?::text) "

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

-- | One queue's tracked migrations, each under its own version identifier. The optional
-- notify and event-streaming objects are not tracked here, 'reconcileOptionalTriggers'
-- owns them.
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
        , script "create-group-retried-index" $ createJobQueueGroupRetriedIndexSQL schemaName tableName
        , script "create-grouped-due-index" $ createJobQueueGroupedDueIndexSQL schemaName tableName
        , script "create-group-in-flight-index" $ createJobQueueGroupInFlightIndexSQL schemaName tableName
        , script "create-groups-trigger-functions-v10" $ createGroupsTriggerFunctionsSQL schemaName tableName
        , script "create-groups-triggers" $ createGroupsTriggersSQL schemaName tableName
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

-- | Whether a schema exists.
schemaExists :: PG.Connection -> Text -> IO Bool
schemaExists conn schemaName = do
  rows <- query conn "SELECT 1 FROM pg_namespace WHERE nspname = ?" (Only schemaName) :: IO [Only Int]
  pure (not (null rows))

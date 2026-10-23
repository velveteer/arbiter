{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Golden tests guarding migration checksums against in-place edits.
--
-- @postgresql-migration@ records a checksum per migration name. Editing an
-- already-shipped migration changes that checksum. Every deployed database then
-- rejects the run. Migrations are add-only or rename-on-change. Each
-- migration's SQL body is pinned to its own golden file. A body change fails the
-- corresponding test with a diff. Accept an intentional add or rename with
-- @cabal test arbiter-migrations-tests --test-options=--accept@.
module Main (main) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitsTableName)
import Arbiter.Core.RateLimit.Spec (Durability (Durable), RateLimitPolicy (..))
import Control.Concurrent.Async (replicateConcurrently)
import Control.Exception (bracket, bracket_)
import Control.Monad (void)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Lazy qualified as LBS
import Data.Foldable (traverse_)
import Data.Int (Int64)
import Data.List (isInfixOf)
import Data.Maybe (listToMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Migration (MigrationCommand (..))
import Database.PostgreSQL.Simple.Notification (Notification (..), getNotification)
import Database.PostgreSQL.Simple.Types (PGArray (..), Query (..))
import GHC.Generics (Generic)
import System.Environment (lookupEnv)
import System.FilePath ((<.>), (</>))
import System.Timeout (timeout)
import Test.Tasty (TestTree, defaultMain, testGroup)
import Test.Tasty.Golden (goldenVsString)
import Test.Tasty.HUnit (assertBool, testCase, (@?=))

import Arbiter.Migrations
  ( MigrationConfig (..)
  , MigrationResult (..)
  , allTableAdmission
  , conflictingPolicyPrefixes
  , defaultMigrationConfig
  , dropQueue
  , forceDropQueue
  , jobQueueMigrationsForTable
  , noAdmissionSeeds
  , orphanedQueues
  , runMigrationsForRegistry
  , runMigrationsTrackedForTables
  , schemaLevelMigrations
  , validateRegistryNames
  )

main :: IO ()
main = do
  connStr <- getTestConnectionString
  defaultMain $
    testGroup
      "arbiter-migrations"
      [ testGroup "migration checksums" (map migrationGolden shippedMigrations)
      , testGroup "policy conflict detection" conflictTests
      , testGroup "registry name validation" registryNameTests
      , migrationReconciliationTests connStr
      , migrationLockTests connStr
      , dropQueueTests connStr
      ]

-- | 'conflictingPolicyPrefixes' flags a prefix carrying two distinct parameter sets.
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
    row = RateLimitPolicy

registryNameTests :: [TestTree]
registryNameTests =
  [ testCase "accepts distinct generated names" $
      validateRegistryNames "arbiter" ["jobs", "other_jobs"] @?= Right ()
  , testCase "rejects generated table collisions" $
      validateRegistryNames "arbiter" ["jobs", "jobs_dlq"]
        @?= Left "Arbiter queue names generate the same PostgreSQL object: jobs_dlq"
  , testCase "rejects queue names too long for generated identifiers" $
      validateRegistryNames "arbiter" ["abcdefghijklmnopqrstuvwxyz0123456789"]
        @?= Left "Arbiter queue name exceeds the 35-byte generated-identifier limit: abcdefghijklmnopqrstuvwxyz0123456789"
  , testCase "accepts a queue name at the limit" $
      validateRegistryNames "arbiter" ["abcdefghijklmnopqrstuvwxyz012345678"] @?= Right ()
  , testCase "rejects a queue name owned by a schema-wide table" $
      validateRegistryNames "arbiter" ["arbiter_gates"]
        @?= Left "Arbiter queue name generates a reserved arbiter table: arbiter_gates"
  , testCase "rejects a queue named after the cron table" $
      validateRegistryNames "arbiter" ["cron_schedules"]
        @?= Left "Arbiter queue name generates a reserved arbiter table: cron_schedules"
  ]

newtype MigrationPayload = MigrationPayload Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype RemovedQueuePayload = RemovedQueuePayload Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type MigrationRegistry = '[Queue "migration_reconciliation_q" MigrationPayload]
type ExpandedMigrationRegistry =
  '[ Queue "migration_reconciliation_q" MigrationPayload
   , Queue "removed_reconciliation_q" RemovedQueuePayload
   ]
type DLQSuffixRegistry = '[Queue "events_dlq" MigrationPayload]

reconciliationSchema :: Text
reconciliationSchema = "arbiter_migration_reconciliation_test"

migrationReconciliationTests :: ByteString -> TestTree
migrationReconciliationTests connStr =
  testGroup
    "migration reconciliation"
    [ testCase "reconciles rate-limit bucket durability" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate durableConfig
          bucketPersistence conn >>= (@?= Just "p")
          migrate defaultMigrationConfig
          bucketPersistence conn >>= (@?= Just "u")
    , testCase "registers each migrated queue and keeps its row's state" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          runMigrationsForRegistry (Proxy @ExpandedMigrationRegistry) connStr reconciliationSchema defaultMigrationConfig
            >>= shouldMigrate
          void $
            PG.execute_
              conn
              ( sql
                  ( "UPDATE "
                      <> reconciliationSchema
                      <> ".arbiter_queues SET dlq_retention_secs = 60 WHERE queue_name = 'migration_reconciliation_q'"
                  )
              )
          migrate defaultMigrationConfig
          registered <-
            PG.query_
              conn
              (sql ("SELECT queue_name, dlq_retention_secs FROM " <> reconciliationSchema <> ".arbiter_queues ORDER BY queue_name"))
          registered @?= [("migration_reconciliation_q" :: Text, Just 60), ("removed_reconciliation_q", Nothing :: Maybe Int64)]
    , testCase "enables, disables, and re-enables optional triggers" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOff
          optionalTriggerCount conn >>= (@?= 0)
          optionalFunctionCount conn >>= (@?= 0)

          migrate triggerOn
          optionalTriggerCount conn >>= (@?= 3)
          optionalFunctionCount conn >>= (@?= 2)
          triggerOids <- optionalTriggerOids conn
          functionOids <- optionalFunctionOids conn

          -- A second run rebuilds nothing.
          migrate triggerOn
          optionalTriggerOids conn >>= (@?= triggerOids)
          optionalFunctionOids conn >>= (@?= functionOids)

          migrate triggerOff
          optionalTriggerCount conn >>= (@?= 0)
          optionalFunctionCount conn >>= (@?= 0)

          -- Create migrations are already recorded. Desired-state
          -- reconciliation restores the objects.
          migrate triggerOn
          optionalTriggerCount conn >>= (@?= 3)
          optionalFunctionCount conn >>= (@?= 2)
    , testCase "drops streaming triggers for queues removed from the registry" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          runMigrationsForRegistry (Proxy @ExpandedMigrationRegistry) connStr reconciliationSchema triggerOn >>= shouldMigrate
          removedQueueTriggerCount conn >>= (@?= 2)
          removedNotifyObjectCount conn >>= (@?= 2)

          migrate triggerOff
          removedQueueTriggerCount conn >>= (@?= 0)
          removedNotifyObjectCount conn >>= (@?= 0)
          optionalFunctionCount conn >>= (@?= 0)
    , testCase "drops notify objects for queues removed from the registry" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          runMigrationsForRegistry (Proxy @ExpandedMigrationRegistry) connStr reconciliationSchema triggerOn >>= shouldMigrate
          removedNotifyObjectCount conn >>= (@?= 2)

          migrate triggerOn
          removedNotifyObjectCount conn >>= (@?= 0)
          optionalTriggerCount conn >>= (@?= 3)
          optionalFunctionCount conn >>= (@?= 2)
    , testCase "drops the notify function of a removed queue whose table is gone" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          runMigrationsForRegistry (Proxy @ExpandedMigrationRegistry) connStr reconciliationSchema triggerOn >>= shouldMigrate
          removedNotifyObjectCount conn >>= (@?= 2)

          _ <-
            PG.execute_
              conn
              "DROP TABLE arbiter_migration_reconciliation_test.removed_reconciliation_q CASCADE"
          migrate triggerOn
          removedNotifyObjectCount conn >>= (@?= 0)
    , testCase "repairs stale notification objects" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE OR REPLACE FUNCTION arbiter_migration_reconciliation_test.notify_migration_reconciliation_q_created() \
              \RETURNS trigger AS $$ BEGIN RETURN NEW; END; $$ LANGUAGE plpgsql; \
              \DROP TRIGGER migration_reconciliation_q_notify_trigger \
              \ON arbiter_migration_reconciliation_test.migration_reconciliation_q; \
              \CREATE TRIGGER migration_reconciliation_q_notify_trigger \
              \BEFORE INSERT ON arbiter_migration_reconciliation_test.migration_reconciliation_q \
              \FOR EACH ROW EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_migration_reconciliation_q_created()"
          notificationObjectsAreCurrent conn >>= (@?= False)
          migrate triggerOn
          notificationObjectsAreCurrent conn >>= (@?= True)
    , testCase "repairs a notify trigger missing its transition table" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "DROP TRIGGER migration_reconciliation_q_notify_trigger \
              \ON arbiter_migration_reconciliation_test.migration_reconciliation_q; \
              \CREATE TRIGGER migration_reconciliation_q_notify_trigger \
              \AFTER INSERT ON arbiter_migration_reconciliation_test.migration_reconciliation_q \
              \FOR EACH STATEMENT \
              \EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_migration_reconciliation_q_created()"
          notificationObjectsAreCurrent conn >>= (@?= False)
          migrate triggerOn
          notificationObjectsAreCurrent conn >>= (@?= True)
    , testCase "leaves unmarked notify lookalikes alone" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE TABLE arbiter_migration_reconciliation_test.orders (id bigint); \
              \CREATE FUNCTION arbiter_migration_reconciliation_test.notify_orders_created() \
              \RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql; \
              \CREATE TRIGGER orders_notify_trigger \
              \AFTER INSERT ON arbiter_migration_reconciliation_test.orders \
              \REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT \
              \EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_orders_created()"
          migrate triggerOn
          functionCountNamed conn "notify_orders_created" >>= (@?= 1)
          triggerCountNamed conn "orders_notify_trigger" >>= (@?= 1)

          migrate triggerOff
          functionCountNamed conn "notify_orders_created" >>= (@?= 1)
          triggerCountNamed conn "orders_notify_trigger" >>= (@?= 1)
    , testCase "leaves an unmarked event function alone" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOff
          _ <-
            PG.execute_
              conn
              "CREATE FUNCTION arbiter_migration_reconciliation_test.notify_job_event() \
              \RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
          migrate triggerOff
          functionCountNamed conn "notify_job_event" >>= (@?= 1)
    , testCase "disables streaming with a leftover unmarked event trigger" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE TABLE arbiter_migration_reconciliation_test.legacy_q (id bigint); \
              \CREATE TRIGGER notify_job_event_legacy_q \
              \AFTER INSERT ON arbiter_migration_reconciliation_test.legacy_q \
              \FOR EACH ROW EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_job_event('legacy_q', 'false')"
          migrate triggerOff
          optionalTriggerCount conn >>= (@?= 0)
    , -- Installs predating the marker have an unmarked function with an older body.
      -- Enabling streaming replaces and stamps it.
      testCase "adopts an unmarked event function" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOff
          _ <-
            PG.execute_
              conn
              "CREATE FUNCTION arbiter_migration_reconciliation_test.notify_job_event() \
              \RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
          migrate triggerOn
          eventFunctionIsCurrent conn >>= (@?= True)
    , testCase "repairs a stale event function body" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "CREATE OR REPLACE FUNCTION arbiter_migration_reconciliation_test.notify_job_event() \
              \RETURNS trigger AS $$ BEGIN RETURN NULL; END; $$ LANGUAGE plpgsql"
          eventFunctionIsCurrent conn >>= (@?= False)
          migrate triggerOn
          eventFunctionIsCurrent conn >>= (@?= True)
    , testCase "repairs an event trigger with stale arguments" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOn
          _ <-
            PG.execute_
              conn
              "DROP TRIGGER notify_job_event_migration_reconciliation_q \
              \ON arbiter_migration_reconciliation_test.migration_reconciliation_q; \
              \CREATE TRIGGER notify_job_event_migration_reconciliation_q \
              \AFTER INSERT OR UPDATE OR DELETE ON arbiter_migration_reconciliation_test.migration_reconciliation_q \
              \FOR EACH ROW EXECUTE FUNCTION arbiter_migration_reconciliation_test.notify_job_event('wrong_queue', 'true')"
          migrate triggerOn
          definitions <- eventTriggerDefinitionsFor conn "notify_job_event_migration_reconciliation_q"
          length (filter (T.isInfixOf "('migration_reconciliation_q', 'false')") definitions) @?= 1
    , testCase "does not stream a lease extension as a job event" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOn
          let tbl = reconciliationSchema <> ".migration_reconciliation_q"
          bracket (PG.connectPostgreSQL connStr) PG.close $ \lconn -> do
            _ <- PG.execute_ lconn "LISTEN arbiter_job_events"
            _ <-
              PG.execute_
                conn
                ( sql
                    ( "INSERT INTO "
                        <> tbl
                        <> " (payload, attempts, claimed_by, not_visible_until) VALUES ('{}', 1, gen_random_uuid(), NOW() + interval '60 seconds')"
                    )
                )
            nextEvent lconn >>= assertEvent "job_inserted"
            _ <-
              PG.execute_
                conn
                (sql ("UPDATE " <> tbl <> " SET not_visible_until = NOW() + interval '120 seconds', updated_at = NOW()"))
            nextEvent lconn >>= (@?= Nothing)
            _ <-
              PG.execute_
                conn
                ( sql
                    ( "UPDATE "
                        <> tbl
                        <> " SET attempts = attempts + 1, claim_seq = claim_seq + 1, not_visible_until = NOW() + interval '60 seconds'"
                    )
                )
            nextEvent lconn >>= assertEvent "job_updated"
            _ <- PG.execute_ conn (sql ("UPDATE " <> tbl <> " SET not_visible_until = NULL, updated_at = NOW()"))
            nextEvent lconn >>= assertEvent "job_updated"
            _ <- PG.execute_ conn (sql ("UPDATE " <> tbl <> " SET last_error = 'boom', claimed_by = NULL"))
            nextEvent lconn >>= assertEvent "job_updated"
    , testCase "streams a DLQ insert with the job id and the DLQ flag" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          migrate triggerOn
          let tbl = reconciliationSchema <> ".migration_reconciliation_q"
          bracket (PG.connectPostgreSQL connStr) PG.close $ \lconn -> do
            _ <- PG.execute_ lconn "LISTEN arbiter_job_events"
            _ <- PG.execute_ conn (sql ("INSERT INTO " <> tbl <> " (payload) VALUES ('{}')"))
            nextEvent lconn >>= assertEvent "\"dlq\" : false"
            _ <- PG.execute_ conn (sql ("INSERT INTO " <> tbl <> "_dlq (job_id, payload) VALUES (4242, '{}')"))
            dlqEvent <- nextEvent lconn
            assertEvent "\"job_id\" : 4242" dlqEvent
            assertEvent "\"dlq\" : true" dlqEvent
    , testCase "preserves queue names ending in the DLQ suffix" $
        withFreshSchema connStr reconciliationSchema $ \conn -> do
          runMigrationsForRegistry (Proxy @DLQSuffixRegistry) connStr reconciliationSchema triggerOn >>= shouldMigrate
          definitions <- eventTriggerDefinitions conn
          length (filter (T.isInfixOf "('events_dlq', 'false')") definitions) @?= 1
          length (filter (T.isInfixOf "('events_dlq', 'true')") definitions) @?= 1
    ]
  where
    durableConfig = defaultMigrationConfig {rateLimitDurability = Durable}
    triggerOff = defaultMigrationConfig {enableNotifications = False, enableEventStreaming = False}
    triggerOn = defaultMigrationConfig {enableNotifications = True, enableEventStreaming = True}
    migrate config = runMigrationsForRegistry (Proxy @MigrationRegistry) connStr reconciliationSchema config >>= shouldMigrate

lockSchema :: Text
lockSchema = "arbiter_migration_lock_test"

-- | Replicas that race to migrate the same schema.
migratingReplicas :: Int
migratingReplicas = 4

migrationLockTests :: ByteString -> TestTree
migrationLockTests connStr =
  testGroup
    "migration lock"
    [ testCase "applies each migration once when replicas migrate at the same time" $
        withFreshSchema connStr lockSchema $ \conn -> do
          results <- replicateConcurrently migratingReplicas (migrate defaultMigrationConfig)
          traverse_ shouldMigrate results
          appliedTwice conn >>= (@?= Just 0)
    , testCase "gives up on a lock still held at the timeout, and migrates once it is free" $
        withFreshSchema connStr lockSchema $ \conn -> do
          held <- withMigrationLockHeld conn (migrate bounded)
          assertBool "expected a lock timeout" (timedOut held)
          migrate bounded >>= shouldMigrate
    ]
  where
    migrate config =
      runMigrationsTrackedForTables connStr lockSchema [("lock_probe_jobs", allTableAdmission)] config noAdmissionSeeds
    bounded = defaultMigrationConfig {migrationLockTimeout = Just 0.25}

dropSchema :: Text
dropSchema = "arbiter_drop_queue_test"

-- | The queue the drop tests remove, and one whose name extends it.
droppedQueue, extendingQueue :: Text
droppedQueue = "drop_q"
extendingQueue = "drop_q2"

dropQueueTests :: ByteString -> TestTree
dropQueueTests connStr =
  testGroup
    "dropping queues"
    [ testCase "removes the queue's tables, functions, rows and history, and nothing of another queue" $
        withFreshSchema connStr dropSchema $ \conn -> do
          migrate
          exec
            conn
            ( "INSERT INTO "
                <> inSchema "cron_schedules"
                <> " (name, default_expression, default_overlap, queue_name) VALUES ('nightly', '0 0 * * *', 'SkipOverlap', '"
                <> droppedQueue
                <> "')"
            )
          exec conn (workerRow droppedQueue "NOW() - interval '1 hour'")
          dropQ droppedQueue >>= shouldMigrate
          queueFootprint conn droppedQueue >>= (@?= (0, 0, 0, 0))
          queueFootprint conn extendingQueue >>= (@?= migratedFootprint extendingQueue)
    , testCase "removes history from earlier versions, and none of a queue extending the name with a hyphen" $
        withFreshSchema connStr dropSchema $ \conn -> do
          let hyphenated = droppedQueue <> "-x"
              historyOf prefix =
                fromOnlyOne
                  <$> PG.query
                    conn
                    (sql ("SELECT count(*) FROM " <> inSchema "schema_migrations" <> " WHERE starts_with(filename, ?)"))
                    (PG.Only (prefix <> "-"))
          runMigrationsTrackedForTables
            connStr
            dropSchema
            [(droppedQueue, allTableAdmission), (hyphenated, allTableAdmission)]
            defaultMigrationConfig
            noAdmissionSeeds
            >>= shouldMigrate
          exec
            conn
            ( "INSERT INTO "
                <> inSchema "schema_migrations"
                <> " (filename, checksum) VALUES ('"
                <> droppedQueue
                <> "-create-groups-trigger-functions-v11', '')"
            )
          dropQ droppedQueue >>= shouldMigrate
          historyOf hyphenated
            >>= (@?= fromIntegral (length (jobQueueMigrationsForTable dropSchema hyphenated allTableAdmission)))
          (-) <$> historyOf droppedQueue <*> historyOf hyphenated >>= (@?= (0 :: Int64))
    , testCase "creates a dropped queue fresh on the next migrate" $
        withFreshSchema connStr dropSchema $ \conn -> do
          migrate
          dropQ droppedQueue >>= shouldMigrate
          migrate
          queueFootprint conn droppedQueue >>= (@?= migratedFootprint droppedQueue)
          exec conn ("INSERT INTO " <> inSchema droppedQueue <> " (payload) VALUES ('{}')")
          rowCount conn (inSchema droppedQueue) >>= (@?= 1)
    , testCase "refuses a queue holding jobs or DLQ rows" $
        withFreshSchema connStr dropSchema $ \conn -> do
          migrate
          exec conn ("INSERT INTO " <> inSchema droppedQueue <> " (payload) VALUES ('{}')")
          dropQ droppedQueue >>= shouldRefuse "still holds jobs"
          exec conn ("DELETE FROM " <> inSchema droppedQueue)
          exec conn ("INSERT INTO " <> inSchema (droppedQueue <> "_dlq") <> " (job_id, payload) VALUES (1, '{}')")
          dropQ droppedQueue >>= shouldRefuse "still holds jobs"
          queueFootprint conn droppedQueue >>= (@?= migratedFootprint droppedQueue)
          exec conn ("DELETE FROM " <> inSchema (droppedQueue <> "_dlq"))
          dropQ droppedQueue >>= shouldMigrate
    , testCase "refuses a queue a live worker serves, and drops it once the worker is stale" $
        withFreshSchema connStr dropSchema $ \conn -> do
          migrate
          exec conn (workerRow droppedQueue "NOW()")
          dropQ droppedQueue >>= shouldRefuse "still has a live worker"
          exec conn ("UPDATE " <> inSchema "arbiter_workers" <> " SET last_heartbeat = NOW() - interval '1 hour'")
          dropQ droppedQueue >>= shouldMigrate
    , testCase "refuses a queue another transaction has locked, without waiting" $
        withFreshSchema connStr dropSchema $ \_ -> do
          migrate
          bracket (PG.connectPostgreSQL connStr) PG.close $ \other -> do
            PG.begin other
            exec other ("LOCK TABLE " <> inSchema (droppedQueue <> "_archive") <> " IN ACCESS SHARE MODE")
            dropQ droppedQueue >>= shouldRefuse "is in use by another transaction"
            PG.rollback other
          dropQ droppedQueue >>= shouldMigrate
    , testCase "force drops a queue holding jobs and DLQ rows that a live worker serves" $
        withFreshSchema connStr dropSchema $ \conn -> do
          migrate
          exec conn ("INSERT INTO " <> inSchema droppedQueue <> " (payload) VALUES ('{}')")
          exec conn ("INSERT INTO " <> inSchema (droppedQueue <> "_dlq") <> " (job_id, payload) VALUES (1, '{}')")
          exec conn (workerRow droppedQueue "NOW()")
          traverse_ (claimKeyed conn) [droppedQueue, droppedQueue, extendingQueue]
          inFlight conn >>= (@?= 3)
          forceDropQ defaultMigrationConfig droppedQueue >>= shouldMigrate
          inFlight conn >>= (@?= 1)
          queueFootprint conn droppedQueue >>= (@?= (0, 0, 0, 0))
          queueFootprint conn extendingQueue >>= (@?= migratedFootprint extendingQueue)
    , testCase "force drop waits on another transaction's lock up to the lock timeout" $
        withFreshSchema connStr dropSchema $ \_ -> do
          migrate
          bracket (PG.connectPostgreSQL connStr) PG.close $ \other -> do
            PG.begin other
            exec other ("LOCK TABLE " <> inSchema (droppedQueue <> "_archive") <> " IN ACCESS SHARE MODE")
            forceDropQ defaultMigrationConfig {migrationLockTimeout = Just 0.2} droppedQueue
              >>= shouldRefuse "is in use by another transaction"
            PG.rollback other
          forceDropQ defaultMigrationConfig droppedQueue >>= shouldMigrate
    , testCase "force drop refuses a table without an arbiter_queues row" $
        withFreshSchema connStr dropSchema $ \conn -> do
          migrate
          exec conn ("CREATE TABLE " <> inSchema "users" <> " (id bigint)")
          forceDropQ defaultMigrationConfig "users" >>= shouldRefuse "has no arbiter_queues row"
          rowCount conn (inSchema "users") >>= (@?= 0)
    , testCase "refuses a table without an arbiter_queues row" $
        withFreshSchema connStr dropSchema $ \conn -> do
          migrate
          exec conn ("CREATE TABLE " <> inSchema "users" <> " (id bigint)")
          dropQ "users" >>= shouldRefuse "has no arbiter_queues row"
          rowCount conn (inSchema "users") >>= (@?= 0)
    , testCase "lists the recorded queues the registry lacks" $
        withFreshSchema connStr dropSchema $ \_ -> do
          runMigrationsForRegistry (Proxy @ExpandedMigrationRegistry) connStr dropSchema defaultMigrationConfig
            >>= shouldMigrate
          orphanedQueues (Proxy @MigrationRegistry) connStr dropSchema >>= (@?= ["removed_reconciliation_q"])
          orphanedQueues (Proxy @ExpandedMigrationRegistry) connStr dropSchema >>= (@?= [])
    , testCase "drops retired queues on migrate, keeping the ones it refuses until a later run" $
        withFreshSchema connStr dropSchema $ \conn -> do
          runMigrationsForRegistry (Proxy @ExpandedMigrationRegistry) connStr dropSchema defaultMigrationConfig
            >>= shouldMigrate
          exec conn ("INSERT INTO " <> inSchema "removed_reconciliation_q" <> " (payload) VALUES ('{}')")
          let retiring = defaultMigrationConfig {retiredQueues = ["removed_reconciliation_q"]}
              migrateRetiring = runMigrationsForRegistry (Proxy @MigrationRegistry) connStr dropSchema retiring
          migrateRetiring >>= shouldMigrate
          orphanedQueues (Proxy @MigrationRegistry) connStr dropSchema >>= (@?= ["removed_reconciliation_q"])
          exec conn ("DELETE FROM " <> inSchema "removed_reconciliation_q")
          migrateRetiring >>= shouldMigrate
          queueFootprint conn "removed_reconciliation_q" >>= (@?= (0, 0, 0, 0))
          migrateRetiring >>= shouldMigrate
    , testCase "rejects a queue both registered and retired" $
        runMigrationsForRegistry
          (Proxy @MigrationRegistry)
          connStr
          dropSchema
          defaultMigrationConfig {retiredQueues = ["migration_reconciliation_q"]}
          >>= shouldRefuse "both registered and retired"
    ]
  where
    migrate =
      runMigrationsTrackedForTables
        connStr
        dropSchema
        [(droppedQueue, allTableAdmission), (extendingQueue, allTableAdmission)]
        defaultMigrationConfig
        noAdmissionSeeds
        >>= shouldMigrate
    dropQ = dropQueue connStr dropSchema defaultMigrationConfig
    forceDropQ = forceDropQueue connStr dropSchema
    exec conn = void . PG.execute_ conn . sql
    claimKeyed conn table = do
      exec
        conn
        ("INSERT INTO " <> inSchema table <> " (payload, concurrency_key, concurrency_prefix) VALUES ('{}', 'drop:k', 'drop')")
      exec conn ("UPDATE " <> inSchema table <> " SET claimed_by = gen_random_uuid() WHERE claimed_by IS NULL")
    inFlight conn =
      fromOnlyOne
        <$> PG.query_
          conn
          (sql ("SELECT in_flight::int8 FROM " <> inSchema "arbiter_concurrency" <> " WHERE concurrency_key = 'drop:k'"))
    workerRow table heartbeat =
      "INSERT INTO "
        <> inSchema "arbiter_workers"
        <> " (worker_id, queue_name, last_heartbeat) VALUES (gen_random_uuid(), '"
        <> table
        <> "', "
        <> heartbeat
        <> ")"

-- | A relation in the drop test schema.
inSchema :: Text -> Text
inSchema relation = dropSchema <> "." <> relation

rowCount :: PG.Connection -> Text -> IO Int64
rowCount conn relation = fromOnlyOne <$> PG.query_ conn (sql ("SELECT count(*) FROM " <> relation))

-- | A queue's tables, trigger functions, bookkeeping rows and migration history rows in
-- the drop test schema.
queueFootprint :: PG.Connection -> Text -> IO (Int64, Int64, Int64, Int64)
queueFootprint conn table = do
  tables <-
    countWhere
      "pg_class relation JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace"
      "namespace.nspname = ? AND relation.relkind = 'r' AND relation.relname = ANY(?)"
      (dropSchema, PGArray (map (table <>) ["", "_dlq", "_archive", "_results", "_groups"]))
  functions <-
    countWhere
      "pg_proc function JOIN pg_namespace namespace ON namespace.oid = function.pronamespace"
      "namespace.nspname = ? AND function.proname = ANY(?)"
      (dropSchema, PGArray (queueFunctionNames table))
  bookkeeping <-
    sum
      <$> traverse
        (\relation -> countWhere (inSchema relation) "queue_name = ?" (PG.Only table))
        ["arbiter_queues", "cron_schedules", "arbiter_workers"]
  history <- countWhere (inSchema "schema_migrations") "starts_with(filename, ?)" (PG.Only (table <> "-"))
  pure (tables, functions, bookkeeping, history)
  where
    countWhere :: (PG.ToRow params) => Text -> Text -> params -> IO Int64
    countWhere from predicate params =
      fromOnlyOne <$> PG.query conn (sql ("SELECT count(*) FROM " <> from <> " WHERE " <> predicate)) params

-- | The footprint of a queue migrated with every admission trigger kind and no worker.
migratedFootprint :: Text -> (Int64, Int64, Int64, Int64)
migratedFootprint table =
  ( 5
  , fromIntegral (length (queueFunctionNames table)) - 1
  , 1
  , fromIntegral (length (jobQueueMigrationsForTable dropSchema table allTableAdmission))
  )

-- | Every trigger function name a queue can own. The rate-limit triggers have no
-- delete function.
queueFunctionNames :: Text -> [Text]
queueFunctionNames table =
  ("notify_" <> table <> "_created")
    : [ base <> suffix
      | base <-
          ["maintain_" <> table <> "_groups", "maintain_" <> table <> "_concurrency", "ensure_" <> table <> "_rate_limit_buckets"]
      , suffix <- ["_insert", "_delete", "_update"]
      ]

shouldRefuse :: String -> MigrationResult String -> IO ()
shouldRefuse reason result =
  assertBool ("expected a refusal: " <> reason) $ case result of
    MigrationError message -> reason `isInfixOf` message
    MigrationSuccess -> False

-- | Take the same lock the migration takes, on a connection of the caller's own.
withMigrationLockHeld :: PG.Connection -> IO a -> IO a
withMigrationLockHeld conn = bracket_ (advisory "pg_advisory_lock") (advisory "pg_advisory_unlock")
  where
    advisory lockFn =
      void
        (PG.query conn ("SELECT " <> lockFn <> "(hashtextextended(?, 0))::text") (PG.Only lockName) :: IO [PG.Only (Maybe Text)])
    lockName = "arbiter.migrations:" <> lockSchema

-- | How many migration names the history records more than once.
appliedTwice :: PG.Connection -> IO (Maybe Int)
appliedTwice conn =
  fmap (fmap PG.fromOnly . listToMaybe) $
    PG.query_ conn (sql ("SELECT count(*) - count(DISTINCT filename) FROM " <> lockSchema <> ".schema_migrations"))

timedOut :: MigrationResult String -> Bool
timedOut (MigrationError message) = "Timed out waiting on the arbiter migration lock" `isInfixOf` message
timedOut MigrationSuccess = False

-- | The most time to wait for a reconciliation event.
eventTimeoutMicros :: Int
eventTimeoutMicros = 1_000_000

-- | The next event for the reconciliation queue, within a second. Every schema in the
-- database shares the channel.
nextEvent :: PG.Connection -> IO (Maybe ByteString)
nextEvent lconn = timeout eventTimeoutMicros own
  where
    own = do
      payload <- notificationData <$> getNotification lconn
      if BS.isInfixOf "\"table\" : \"migration_reconciliation_q\"" payload then pure payload else own

assertEvent :: ByteString -> Maybe ByteString -> IO ()
assertEvent event = assertBool ("expected a " <> BS8.unpack event <> " event") . maybe False (BS.isInfixOf event)

withFreshSchema :: ByteString -> Text -> (PG.Connection -> IO a) -> IO a
withFreshSchema connStr schemaName action =
  bracket (PG.connectPostgreSQL connStr) PG.close $ \conn -> do
    _ <- PG.execute_ conn "SET client_min_messages = WARNING"
    _ <- PG.execute_ conn (sql ("DROP SCHEMA IF EXISTS " <> schemaName <> " CASCADE"))
    action conn

sql :: Text -> Query
sql = Query . encodeUtf8

shouldMigrate :: MigrationResult String -> IO ()
shouldMigrate MigrationSuccess = pure ()
shouldMigrate (MigrationError err) = fail ("migration failed: " <> err)

optionalTriggerOids :: PG.Connection -> IO [Int64]
optionalTriggerOids conn =
  map PG.fromOnly
    <$> PG.query
      conn
      ( "SELECT trigger.oid::bigint "
          <> triggerJoins
          <> "WHERE namespace.nspname = ? AND trigger.tgname IN "
          <> optionalTriggerNames
          <> " ORDER BY trigger.tgname"
      )
      (PG.Only reconciliationSchema)

optionalFunctionOids :: PG.Connection -> IO [Int64]
optionalFunctionOids conn =
  map PG.fromOnly
    <$> PG.query
      conn
      ( "SELECT function.oid::bigint "
          <> functionJoins
          <> "WHERE namespace.nspname = ? AND function.proname IN "
          <> optionalFunctionNames
          <> " ORDER BY function.proname"
      )
      (PG.Only reconciliationSchema)

optionalTriggerCount :: PG.Connection -> IO Int64
optionalTriggerCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      ("SELECT count(*) " <> triggerJoins <> "WHERE namespace.nspname = ? AND trigger.tgname IN " <> optionalTriggerNames)
      (PG.Only reconciliationSchema)

-- | Compares the installed objects against the rendered trigger definition.
notificationObjectsAreCurrent :: PG.Connection -> IO Bool
notificationObjectsAreCurrent conn = do
  rows <-
    PG.query
      conn
      "SELECT pg_get_triggerdef(trigger.oid), function.prosrc \
      \FROM pg_trigger trigger \
      \JOIN pg_proc function ON function.oid = trigger.tgfoid \
      \JOIN pg_class relation ON relation.oid = trigger.tgrelid \
      \JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace \
      \WHERE namespace.nspname = ? AND relation.relname = 'migration_reconciliation_q' \
      \AND trigger.tgname = 'migration_reconciliation_q_notify_trigger'"
      (PG.Only reconciliationSchema)
  pure $ case rows of
    [(definition, body)] ->
      all (`T.isInfixOf` definition) ["AFTER INSERT", "REFERENCING NEW TABLE AS new_table", "FOR EACH STATEMENT"]
        && T.isInfixOf "pg_notify('migration_reconciliation_q_created', '')" body
    _ -> False

eventFunctionIsCurrent :: PG.Connection -> IO Bool
eventFunctionIsCurrent conn =
  any PG.fromOnly
    <$> PG.query
      conn
      ( "SELECT function.prosrc LIKE '%queue_name text := TG_ARGV[0]%' \
        \AND function.prosrc LIKE '%is_dlq boolean := TG_ARGV[1]::boolean%' "
          <> functionJoins
          <> "WHERE namespace.nspname = ? AND function.proname = 'notify_job_event' AND function.pronargs = 0"
      )
      (PG.Only reconciliationSchema)

eventTriggerDefinitions :: PG.Connection -> IO [Text]
eventTriggerDefinitions conn = do
  first <- eventTriggerDefinitionsFor conn "notify_job_event_events_dlq"
  second <- eventTriggerDefinitionsFor conn "notify_job_event_events_dlq_dlq"
  pure (first <> second)

eventTriggerDefinitionsFor :: PG.Connection -> Text -> IO [Text]
eventTriggerDefinitionsFor conn trigger =
  map PG.fromOnly
    <$> PG.query
      conn
      ( "SELECT pg_get_triggerdef(trigger.oid) "
          <> triggerJoins
          <> "WHERE namespace.nspname = ? AND trigger.tgname = ? ORDER BY trigger.tgname"
      )
      (reconciliationSchema, trigger)

removedQueueTriggerCount :: PG.Connection -> IO Int64
removedQueueTriggerCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      ( "SELECT count(*) "
          <> triggerJoins
          <> "WHERE namespace.nspname = ? AND trigger.tgname IN \
             \('notify_job_event_removed_reconciliation_q', 'notify_job_event_removed_reconciliation_q_dlq')"
      )
      (PG.Only reconciliationSchema)

removedNotifyObjectCount :: PG.Connection -> IO Int64
removedNotifyObjectCount conn = do
  triggerCount <-
    fromOnlyOne
      <$> PG.query
        conn
        ( "SELECT count(*) "
            <> triggerJoins
            <> "WHERE namespace.nspname = ? AND trigger.tgname = 'removed_reconciliation_q_notify_trigger'"
        )
        (PG.Only reconciliationSchema)
  functionCount <-
    fromOnlyOne
      <$> PG.query
        conn
        ( "SELECT count(*) "
            <> functionJoins
            <> "WHERE namespace.nspname = ? AND function.proname = 'notify_removed_reconciliation_q_created'"
        )
        (PG.Only reconciliationSchema)
  pure (triggerCount + functionCount)

triggerCountNamed :: PG.Connection -> Text -> IO Int64
triggerCountNamed conn name =
  fromOnlyOne
    <$> PG.query
      conn
      ("SELECT count(*) " <> triggerJoins <> "WHERE namespace.nspname = ? AND trigger.tgname = ?")
      (reconciliationSchema, name)

functionCountNamed :: PG.Connection -> Text -> IO Int64
functionCountNamed conn name =
  fromOnlyOne
    <$> PG.query
      conn
      ("SELECT count(*) " <> functionJoins <> "WHERE namespace.nspname = ? AND function.proname = ?")
      (reconciliationSchema, name)

optionalFunctionCount :: PG.Connection -> IO Int64
optionalFunctionCount conn =
  fromOnlyOne
    <$> PG.query
      conn
      ("SELECT count(*) " <> functionJoins <> "WHERE namespace.nspname = ? AND function.proname IN " <> optionalFunctionNames)
      (PG.Only reconciliationSchema)

-- | Catalog joins shared by the trigger probes.
triggerJoins :: PG.Query
triggerJoins =
  "FROM pg_trigger trigger JOIN pg_class relation ON relation.oid = trigger.tgrelid JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace "

-- | Catalog joins shared by the function probes.
functionJoins :: PG.Query
functionJoins = "FROM pg_proc function JOIN pg_namespace namespace ON namespace.oid = function.pronamespace "

-- | The triggers the reconciliation registry installs when both options are on.
optionalTriggerNames :: PG.Query
optionalTriggerNames =
  "('migration_reconciliation_q_notify_trigger', \
  \'notify_job_event_migration_reconciliation_q', \
  \'notify_job_event_migration_reconciliation_q_dlq')"

-- | The functions the reconciliation registry installs when both options are on.
optionalFunctionNames :: PG.Query
optionalFunctionNames = "('notify_migration_reconciliation_q_created', 'notify_job_event')"

fromOnlyOne :: [PG.Only Int64] -> Int64
fromOnlyOne = maybe 0 PG.fromOnly . listToMaybe

bucketPersistence :: PG.Connection -> IO (Maybe Text)
bucketPersistence conn =
  fmap (fmap PG.fromOnly . listToMaybe) $
    PG.query
      conn
      "SELECT relpersistence::text FROM pg_class relation JOIN pg_namespace namespace ON namespace.oid = relation.relnamespace \
      \WHERE namespace.nspname = ? AND relation.relname = ?"
      (reconciliationSchema, arbiterRateLimitsTableName)

getTestConnectionString :: IO ByteString
getTestConnectionString = do
  configured <- lookupEnv "ARBITER_TEST_CONN_STRING"
  pure $
    maybe "host=localhost port=5432 user=postgres password=master dbname=postgres connect_timeout=10" BS8.pack configured

-- | Every shipped migration as @(name, SQL body)@, covering both the schema-level
-- migrations and the per-table migrations.
shippedMigrations :: [(String, BS.ByteString)]
shippedMigrations =
  [ (name, body)
  | MigrationScript name body <-
      schemaLevelMigrations "arbiter"
        <> jobQueueMigrationsForTable "arbiter" "golden_jobs" allTableAdmission
  ]

-- | Pin one migration's body to @test/golden/<name>.sql@.
migrationGolden :: (String, BS.ByteString) -> TestTree
migrationGolden (name, body) =
  goldenVsString name ("test" </> "golden" </> name <.> "sql") (pure (LBS.fromStrict body))

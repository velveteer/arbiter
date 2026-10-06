{-# LANGUAGE OverloadedStrings #-}

-- | Schema setup, teardown, and connection helpers for the arbiter test suites.
module Arbiter.Test.Setup
  ( cleanupData
  , cleanupOnce
  , withConn
  , terminateBackendsMatching
  , terminatePid
  , listenerConnectionCount
  , execute_
  , execStatement
  , execQuery
  , setupOnce
  , addQueueTable
  , disableNoticeReporting
  , createSharedPool
  , createPoolOf
  , createPoolWith
  , truncateToMicros
  , mkTime
  , seedConcurrencyPolicySQL
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
import Arbiter.Core.Sql.Query (Piece (..))
import Arbiter.Core.SqlLiterals (textLiteral)
import Arbiter.Migrations
  ( allTableAdmission
  , jobQueueMigrationsForTable
  , schemaLevelMigrations
  )
import Control.Concurrent (threadDelay)
import Control.Exception (bracket, throwIO, try)
import Control.Monad (void, when)
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.List (intersperse)
import Data.Maybe (listToMaybe)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime (..), fromGregorian, picosecondsToDiffTime, secondsToDiffTime)
import Database.PostgreSQL.LibPQ qualified as LibPQ
import Database.PostgreSQL.Simple (Connection, Only (..), SqlError (..), close, connectPostgreSQL, execute, query)
import Database.PostgreSQL.Simple.Internal qualified as PGS
import Database.PostgreSQL.Simple.Migration (MigrationCommand (..))
import Database.PostgreSQL.Simple.Types (Query (..))

-- | Rebuild the schema with the shipped migration scripts.
setupDDLWithConfig :: Bool -> Text -> Text -> Connection -> IO ()
setupDDLWithConfig withNotify schemaName tableName conn = do
  execute_ conn $ "DROP SCHEMA IF EXISTS " <> schemaName <> " CASCADE"
  execute_ conn $ Schema.createSchemaSQL schemaName
  traverse_ (runScript conn) $
    schemaLevelMigrations schemaName
      <> jobQueueMigrationsForTable schemaName tableName allTableAdmission
  createNotifyObjects withNotify schemaName tableName conn

runScript :: Connection -> MigrationCommand -> IO ()
runScript conn (MigrationScript _ sql) = void $ execute conn (Query sql) ()
runScript _ _ = pure ()

-- | Truncate the queue's tables and the schema-wide tables, except rate-limit policies.
cleanupData :: Text -> Text -> Connection -> IO ()
cleanupData schemaName tableName conn = do
  execute_ conn "SET client_min_messages = WARNING"
  -- A draining worker pool can still hold locks on the job and groups tables.
  -- Bound the wait and retry on a deadlock or lock timeout.
  execute_ conn "SET lock_timeout = '5s'"
  -- Rate-limit policies are seeded once per suite.
  let truncated = filter (/= RL.arbiterRateLimitPoliciesTableName) (allSchemaTables [tableName])
      truncateSql =
        "TRUNCATE "
          <> T.intercalate ", " (map (Schema.qualifiedTable schemaName) truncated)
          <> " CASCADE"
      go remaining = do
        outcome <- try (execute_ conn truncateSql) :: IO (Either SqlError ())
        case outcome of
          Right () -> pure ()
          -- 40P01 deadlock_detected, 55P03 lock_not_available
          Left sqlErr
            | sqlState sqlErr `elem` ["40P01", "55P03"] && remaining > (0 :: Int) ->
                threadDelay 100_000 >> go (remaining - 1)
            | otherwise -> throwIO sqlErr
  go 10
  execute_ conn "RESET lock_timeout"
  execute_ conn "SET client_min_messages = NOTICE"

-- | 'cleanupData' on a fresh connection.
cleanupOnce :: ByteString -> Text -> Text -> IO ()
cleanupOnce connStr schemaName tableName = withConn connStr (cleanupData schemaName tableName)

-- | Run an action on a fresh connection that is closed afterwards.
withConn :: ByteString -> (Connection -> IO a) -> IO a
withConn connStr = bracket (connectPostgreSQL connStr) close

-- | Terminate every other backend on this database whose current query matches the LIKE pattern.
terminateBackendsMatching :: ByteString -> Text -> IO ()
terminateBackendsMatching connStr queryLike =
  withConn connStr $ \conn ->
    void $
      query @_ @(Only Bool)
        conn
        "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND datname = current_database() AND query LIKE ?"
        (Only queryLike)

-- | Terminate one backend by pid.
terminatePid :: ByteString -> Int32 -> IO ()
terminatePid connStr pid =
  withConn connStr $ \conn -> void $ query @_ @(Only Bool) conn "SELECT pg_terminate_backend(?)" (Only pid)

-- | Count distinct backends holding a LISTEN on any of this schema's channels.
listenerConnectionCount :: ByteString -> Text -> IO Int
listenerConnectionCount connStr schema =
  withConn connStr $ \conn -> do
    rows <-
      query @_ @(Only Int)
        conn
        "SELECT count(DISTINCT pid)::int FROM pg_stat_activity WHERE datname = current_database() AND query LIKE 'LISTEN%' AND query LIKE ?"
        (Only ("%" <> schema <> "%" :: Text))
    pure (maybe 0 fromOnly (listToMaybe rows))

-- | Run a statement with no parameters (test setup only).
execute_ :: Connection -> Text -> IO ()
execute_ conn sql = void $ execute conn (fromString (T.unpack sql) :: Query) ()

-- | Run a pre-rendered statement with @?@ placeholders and positional parameters
-- (test setup only).
execStatement :: (MonadArbiter m) => Text -> Params -> m Int64
execStatement sql params = MA.executeStatement (MA.mkQuery (placeholderPieces sql) params (pure ()))

-- | Run a pre-rendered query with @?@ placeholders, positional parameters and a
-- decoder (test setup only).
execQuery :: (MonadArbiter m) => Text -> Params -> RowCodec a -> m [a]
execQuery sql params codec = MA.executeQuery (MA.mkQuery (placeholderPieces sql) params codec)

-- | Every @?@ in the text is a hole.
placeholderPieces :: Text -> [Piece]
placeholderPieces = intersperse Hole . map Lit . T.splitOn "?"

-- | Connect and rebuild the schema once, for a suite's outer bracket.
-- It drops the schema with CASCADE and creates it again.
setupOnce
  :: ByteString
  -> Text
  -> Text
  -> Bool
  -- ^ Install the job-arrival notify trigger
  -> IO ()
setupOnce connStr schemaName tableName withNotify = withConn connStr $ \conn -> do
  disableNoticeReporting conn
  setupDDLWithConfig withNotify schemaName tableName conn

-- | Add one more job-queue table to an existing schema.
addQueueTable
  :: ByteString
  -> Text
  -> Text
  -> Bool
  -- ^ Install the job-arrival notify trigger
  -> IO ()
addQueueTable connStr schemaName tableName withNotify = withConn connStr $ \conn -> do
  disableNoticeReporting conn
  traverse_ (runScript conn) (jobQueueMigrationsForTable schemaName tableName allTableAdmission)
  createNotifyObjects withNotify schemaName tableName conn

-- | Install the notification function and trigger the reconciler would.
createNotifyObjects :: Bool -> Text -> Text -> Connection -> IO ()
createNotifyObjects withNotify schemaName tableName conn =
  when withNotify $ do
    execute_ conn (Schema.createNotifyFunctionSQL schemaName tableName)
    execute_ conn (Schema.createNotifyTriggerSQL schemaName tableName)

-- | Silence libpq notice output for the connection.
disableNoticeReporting :: Connection -> IO ()
disableNoticeReporting conn =
  PGS.withConnection conn LibPQ.disableNoticeReporting

-- | A single-stripe pool of the default size for tests.
createSharedPool :: ByteString -> IO (Pool Connection)
createSharedPool = createPoolOf sharedPoolSize

-- | A single-stripe pool of the given size for tests.
createPoolOf :: Int -> ByteString -> IO (Pool Connection)
createPoolOf size connStr = createPoolWith size (connectPostgreSQL connStr) close

-- | A single-stripe pool of the given size over any connect and release actions.
createPoolWith :: Int -> IO conn -> (conn -> IO ()) -> IO (Pool conn)
createPoolWith size connect release =
  newPool $ setNumStripes (Just 1) $ defaultPoolConfig connect release sharedPoolIdleSeconds size

sharedPoolSize :: Int
sharedPoolSize = 5

sharedPoolIdleSeconds :: Double
sharedPoolIdleSeconds = 60

-- | Seed a concurrency policy's default limit and clear any override, as SQL statements.
seedConcurrencyPolicySQL :: Text -> Text -> Int32 -> [Text]
seedConcurrencyPolicySQL schema prefix lim =
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

-- | A UTCTime from calendar and clock components.
mkTime :: Integer -> Int -> Int -> Int -> Int -> Int -> UTCTime
mkTime year month day hour minute second =
  UTCTime (fromGregorian year month day) (secondsToDiffTime (fromIntegral (hour * 3600 + minute * 60 + second)))

-- | Truncate to microsecond precision to match PostgreSQL @timestamptz@.
truncateToMicros :: UTCTime -> UTCTime
truncateToMicros (UTCTime day dayTime) =
  let micros = floor (dayTime * 1e6) :: Integer
   in UTCTime day (picosecondsToDiffTime (micros * 1000000))

{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Test.Setup
  ( SetupConfig (..)
  , defaultSetupConfig
  , setupDDL
  , setupDDLWithNotify
  , cleanupData
  , execute_
  , setupOnce
  , disableNoticeReporting
  ) where

import Arbiter.Core.CronSchedule qualified as Cron
import Arbiter.Core.Gates qualified as Gates
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Queues qualified as Q
import Arbiter.Core.Worker qualified as W
import Control.Concurrent (threadDelay)
import Control.Exception (throwIO, try)
import Control.Monad (void, when)
import Data.ByteString (ByteString)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.LibPQ qualified as LibPQ
import Database.PostgreSQL.Simple (Connection, Query, SqlError (..), close, connectPostgreSQL, execute)
import Database.PostgreSQL.Simple.Internal qualified as PGS

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

setupDDLWithConfig :: SetupConfig -> Text -> Text -> Connection -> IO ()
setupDDLWithConfig config schemaName tableName conn = do
  void $ execute_ conn $ "DROP SCHEMA IF EXISTS " <> schemaName <> " CASCADE"
  void $ execute_ conn $ Schema.createSchemaSQL schemaName
  void $ execute_ conn $ Schema.createJobQueueTableSQL schemaName tableName
  void $ execute_ conn $ Schema.createJobQueueDLQTableSQL schemaName tableName
  void $ execute_ conn $ Schema.createDLQGroupKeyIndexSQL schemaName tableName
  void $ execute_ conn $ Schema.createDLQFailedAtIndexSQL schemaName tableName
  void $ execute_ conn $ Schema.createDedupKeyIndexSQL schemaName tableName
  void $ execute_ conn $ Cron.createCronSchedulesTableSQL schemaName
  void $ execute_ conn $ Cron.addTimezoneColumnSQL schemaName
  void $ execute_ conn $ Cron.addQueueNameColumnSQL schemaName
  void $ execute_ conn $ W.createWorkersTableSQL schemaName
  void $ execute_ conn $ Q.createQueuesTableSQL schemaName
  void $ execute_ conn $ Gates.createGatesTableSQL schemaName
  void $ execute_ conn $ W.addClaimedByColumnSQL schemaName tableName
  when (setupEnableRankingIndexes config) $ do
    void $ execute_ conn $ Schema.createJobQueueGroupKeyIndexSQL schemaName tableName
    void $ execute_ conn $ Schema.createJobQueueUngroupedReadyRankingIndexSQL schemaName tableName
    void $ execute_ conn $ Schema.createJobQueueUngroupedDueIndexSQL schemaName tableName
  void $ execute_ conn $ Schema.createParentIdIndexSQL schemaName tableName
  void $ execute_ conn $ Schema.createDLQParentIdIndexSQL schemaName tableName
  void $ execute_ conn $ Schema.createResultsTableSQL schemaName tableName
  void $ execute_ conn $ Schema.createGroupsTableSQL schemaName tableName
  void $ execute_ conn $ Schema.migrateGroupsReadyRankingSQL schemaName tableName
  void $ execute_ conn $ Schema.createGroupsTriggerFunctionsSQL schemaName tableName
  void $ execute_ conn $ Schema.createGroupsTriggersSQL schemaName tableName
  when (setupEnableNotifications config) $ do
    void $ execute_ conn $ Schema.createNotifyFunctionSQL schemaName tableName
    void $ execute_ conn $ Schema.createNotifyTriggerSQL schemaName tableName

cleanupData :: Text -> Text -> Connection -> IO ()
cleanupData schemaName tableName conn = do
  execute_ conn "SET client_min_messages = WARNING"
  -- A draining worker pool may still hold locks on the job and groups tables in
  -- the opposite order from this TRUNCATE, so bound the wait and retry on a
  -- deadlock or lock timeout instead of failing the test.
  execute_ conn "SET lock_timeout = '5s'"
  let truncateSql =
        "TRUNCATE "
          <> Schema.jobQueueTable schemaName tableName
          <> ", "
          <> Schema.jobQueueDLQTable schemaName tableName
          <> ", "
          <> Schema.jobQueueGroupsTable schemaName tableName
          <> ", "
          <> W.arbiterWorkersTable schemaName
          <> ", "
          <> Q.arbiterQueuesTable schemaName
          <> ", "
          <> Gates.arbiterGatesTable schemaName
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

setupOnce :: ByteString -> Text -> Text -> Bool -> IO ()
setupOnce connStr schemaName tableName withNotify = do
  conn <- connectPostgreSQL connStr
  disableNoticeReporting conn
  let config = defaultSetupConfig {setupEnableNotifications = withNotify}
  setupDDLWithConfig config schemaName tableName conn
  close conn

disableNoticeReporting :: Connection -> IO ()
disableNoticeReporting conn =
  PGS.withConnection conn LibPQ.disableNoticeReporting

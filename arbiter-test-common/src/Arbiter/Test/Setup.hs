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
import Arbiter.Core.Job.Schema qualified as Schema
import Control.Monad (void, when)
import Data.ByteString (ByteString)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.LibPQ qualified as LibPQ
import Database.PostgreSQL.Simple (Connection, Query, close, connectPostgreSQL, execute)
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
  when (setupEnableRankingIndexes config) $ do
    void $ execute_ conn $ Schema.createJobQueueGroupKeyIndexSQL schemaName tableName
    void $ execute_ conn $ Schema.createJobQueueUngroupedRankingIndexSQL schemaName tableName
  void $ execute_ conn $ Schema.createParentIdIndexSQL schemaName tableName
  void $ execute_ conn $ Schema.createDLQParentIdIndexSQL schemaName tableName
  void $ execute_ conn $ Schema.createResultsTableSQL schemaName tableName
  void $ execute_ conn $ Schema.createGroupsTableSQL schemaName tableName
  void $ execute_ conn $ Schema.createGroupsIndexSQL schemaName tableName
  void $ execute_ conn $ Schema.createReaperSeqSQL schemaName tableName
  void $ execute_ conn $ Schema.createGroupsTriggerFunctionsSQL schemaName tableName
  void $ execute_ conn $ Schema.createGroupsTriggersSQL schemaName tableName
  when (setupEnableNotifications config) $ do
    void $ execute_ conn $ Schema.createNotifyFunctionSQL schemaName tableName
    void $ execute_ conn $ Schema.createNotifyTriggerSQL schemaName tableName

cleanupData :: Text -> Text -> Connection -> IO ()
cleanupData schemaName tableName conn = do
  execute_ conn "SET client_min_messages = WARNING"
  void $
    execute_
      conn
      ( "TRUNCATE "
          <> Schema.jobQueueTable schemaName tableName
          <> ", "
          <> Schema.jobQueueDLQTable schemaName tableName
          <> ", "
          <> Schema.jobQueueGroupsTable schemaName tableName
          <> " CASCADE"
      )
  execute_ conn $
    "DO $$ BEGIN PERFORM setval('" <> Schema.jobQueueReaperSeq schemaName tableName <> "', 0, false); END $$"
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

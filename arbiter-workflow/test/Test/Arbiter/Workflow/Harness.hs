{-# LANGUAGE OverloadedStrings #-}

-- | The database harness every run suite shares: the schema its queues and workflow
-- tables live in, and an env over the shared pool with both cleared first.
module Test.Arbiter.Workflow.Harness
  ( setupWorkflowSchema
  , withCleanWorkflowRun
  , runHasStatus
  ) where

import Arbiter.Simple (SimpleEnv, createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Setup (addQueueTable, cleanupData, execute_, setupOnce)
import Control.Exception (bracket)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy)
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Migration (MigrationCommand (..))
import Database.PostgreSQL.Simple.Types qualified as PGT

import Arbiter.Workflow.Ops (getRun)
import Arbiter.Workflow.Schema (workflowMigrations, workflowTables)
import Arbiter.Workflow.Sql (runRowStatus)
import Arbiter.Workflow.Types (RunId, RunStatus)

-- | The schema, its queues, and the workflow tables. The first queue creates the
-- schema and the rest are added to it.
setupWorkflowSchema :: ByteString -> Text -> [Text] -> IO ()
setupWorkflowSchema _ _ [] = pure ()
setupWorkflowSchema connStr schemaName (primary : extras) = do
  setupOnce connStr schemaName primary False
  traverse_ (\queue -> addQueueTable connStr schemaName queue False) extras
  bracket (PG.connectPostgreSQL connStr) PG.close $ \conn ->
    traverse_ (runScript conn) (workflowMigrations schemaName)
  where
    runScript conn (MigrationScript _ body) = void (PG.execute_ conn (PGT.Query body))
    runScript _ _ = pure ()

-- | An env over the shared pool, with every run and every job cleared first.
withCleanWorkflowRun
  :: Proxy registry
  -> Text
  -> [Text]
  -> Pool PG.Connection
  -> (SimpleEnv registry -> IO a)
  -> IO a
withCleanWorkflowRun proxy schemaName queues pool action = do
  env <- createSimpleEnvWithPool proxy pool schemaName
  withResource pool $ \conn -> do
    traverse_ (\queue -> cleanupData schemaName queue conn) queues
    void (execute_ conn ("TRUNCATE " <> T.intercalate ", " (workflowTables schemaName) <> " CASCADE"))
  action env

-- | Whether a run is in the status given.
runHasStatus :: SimpleEnv registry -> RunId -> RunStatus -> IO Bool
runHasStatus env runId status = do
  found <- runSimpleDb env (getRun runId)
  pure (fmap runRowStatus found == Just status)

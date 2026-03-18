{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Orville.TestHelpers
  ( executeSql
  , setupOrvilleTest
  , cleanupOrvilleTest
  , runOrvilleTest
  , OrvilleTestEnv (..)
  , TestOrville (..)
  ) where

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema (..))
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Test.Setup qualified as TestSetup
import Control.Monad (void)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.Trans.Reader (ReaderT (..), asks, runReaderT)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Orville.PostgreSQL qualified as O
import Orville.PostgreSQL.Raw.RawSql qualified as RawSql
import Orville.PostgreSQL.UnliftIO qualified as O
import UnliftIO (MonadIO (..), MonadUnliftIO (..))

import Arbiter.Orville.MonadArbiter
  ( orvilleExecuteQuery
  , orvilleExecuteStatement
  , orvilleRunHandlerWithConnection
  , orvilleWithDbTransaction
  )

-- Test environment combining schema name, table name, and OrvilleState
data OrvilleTestEnv registry = OrvilleTestEnv
  { testSchema :: Text
  , testTableName :: Text
  , testConnStr :: ByteString
  , testOrvilleState :: O.OrvilleState
  }

-- | Test monad that provides both OrvilleState and ArbiterEnv
newtype TestOrville registry a = TestOrville {unTestOrville :: ReaderT (OrvilleTestEnv registry) IO a}
  deriving newtype
    (Applicative, Functor, Monad, MonadCatch, MonadFail, MonadIO, MonadMask, MonadThrow, MonadUnliftIO, O.MonadOrville)

instance O.HasOrvilleState (TestOrville registry) where
  askOrvilleState = TestOrville $ asks testOrvilleState
  localOrvilleState f (TestOrville action) = TestOrville $ ReaderT $ \env ->
    runReaderT action (env {testOrvilleState = f (testOrvilleState env)})

instance O.MonadOrvilleControl (TestOrville registry) where
  liftWithConnection = O.liftWithConnectionViaUnliftIO
  liftCatch = O.liftCatchViaUnliftIO
  liftMask = O.liftMaskViaUnliftIO

instance HasArbiterSchema (TestOrville registry) registry where
  getSchema = TestOrville $ asks testSchema

instance MonadArbiter (TestOrville registry) where
  type Handler (TestOrville registry) jobs result = jobs -> TestOrville registry result
  executeQuery = orvilleExecuteQuery
  executeStatement = orvilleExecuteStatement
  withDbTransaction = orvilleWithDbTransaction
  runHandlerWithConnection = orvilleRunHandlerWithConnection

-- Helper to execute raw SQL
executeSql :: (O.MonadOrville m) => Text -> m ()
executeSql sql = O.withConnection $ \conn -> do
  let rawSql = RawSql.fromText sql
  void $ liftIO $ RawSql.execute conn rawSql

setupOrvilleTest :: ByteString -> Text -> Text -> Int -> IO (OrvilleTestEnv registry)
setupOrvilleTest connStr schemaName tableName maxConns = do
  -- Setup DDL using test-common helper
  TestSetup.setupOnce connStr schemaName tableName False

  -- Create Orville connection pool
  let options =
        O.ConnectionOptions
          { O.connectionString = T.unpack (TE.decodeUtf8 connStr)
          , O.connectionNoticeReporting = O.DisableNoticeReporting
          , O.connectionPoolStripes = O.StripeCount 1
          , O.connectionPoolLingerTime = 60
          , O.connectionPoolMaxConnections = O.MaxConnectionsTotal maxConns
          }
  orvillePool <- O.createConnectionPool options
  let orvilleState = O.newOrvilleState O.defaultErrorDetailLevel orvillePool

  pure $
    OrvilleTestEnv
      { testSchema = schemaName
      , testTableName = tableName
      , testConnStr = connStr
      , testOrvilleState = orvilleState
      }

cleanupOrvilleTest :: OrvilleTestEnv registry -> IO ()
cleanupOrvilleTest env = do
  conn <- connectPostgreSQL (testConnStr env)
  TestSetup.cleanupData (testSchema env) (testTableName env) conn
  close conn

-- | Run a TestOrville action with the test environment
runOrvilleTest :: OrvilleTestEnv registry -> TestOrville registry a -> IO a
runOrvilleTest env (TestOrville action) = runReaderT action env

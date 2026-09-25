module Test.Arbiter.Orville.TestHelpers
  ( setupOrvilleTest
  , createOrvilleTestEnv
  , destroyOrvilleTestEnv
  , disableOrvilleListener
  , cleanupOrvilleTest
  , runOrvilleTest
  , orvilleTestHandler
  , OrvilleTestEnv (..)
  , TestOrville
  ) where

import Arbiter.Core.Listen (Listener)
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Arbiter.LibPQ (newLibPQListener)
import Arbiter.Test.Setup qualified as TestSetup
import Control.Monad.Trans.Reader (ReaderT, runReaderT)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Orville.PostgreSQL qualified as O
import Orville.PostgreSQL.Raw.Connection (destroyIdleConnections)

import Arbiter.Orville.OrvilleDb (OrvilleDb, OrvilleEnv (..), runOrvilleDb)

-- Test environment combining schema name, table name, and OrvilleState
data OrvilleTestEnv (registry :: JobPayloadRegistry) = OrvilleTestEnv
  { testSchema :: Text
  , testTableName :: Text
  , testConnStr :: ByteString
  , testOrvilleState :: O.OrvilleState
  , testPool :: O.ConnectionPool
  , testListen :: Maybe Listener
  }

-- | 'OrvilleDb' over a reader of the env's 'O.OrvilleState'
type TestOrville registry = OrvilleDb registry (ReaderT O.OrvilleState IO)

setupOrvilleTest :: ByteString -> Text -> Text -> Int -> IO (OrvilleTestEnv registry)
setupOrvilleTest connStr schemaName tableName maxConns = do
  -- Setup DDL using test-common helper
  TestSetup.setupOnce connStr schemaName tableName False
  createOrvilleTestEnv connStr schemaName tableName maxConns

-- | Build an env (Orville pool plus a dedicated LISTEN connection) against a
-- schema whose tables already exist. The DDL is the caller's responsibility.
createOrvilleTestEnv :: ByteString -> Text -> Text -> Int -> IO (OrvilleTestEnv registry)
createOrvilleTestEnv connStr schemaName tableName maxConns = do
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
  listen <- newLibPQListener connStr

  pure $
    OrvilleTestEnv
      { testSchema = schemaName
      , testTableName = tableName
      , testConnStr = connStr
      , testOrvilleState = orvilleState
      , testPool = orvillePool
      , testListen = Just listen
      }

-- | Release the env's Orville connection pool, closing its idle connections.
destroyOrvilleTestEnv :: OrvilleTestEnv registry -> IO ()
destroyOrvilleTestEnv = destroyIdleConnections . testPool

-- | Drop the env's listener, leaving it poll-only.
disableOrvilleListener :: OrvilleTestEnv registry -> OrvilleTestEnv registry
disableOrvilleListener env = env {testListen = Nothing}

cleanupOrvilleTest :: OrvilleTestEnv registry -> IO ()
cleanupOrvilleTest env = TestSetup.cleanupOnce (testConnStr env) (testSchema env) (testTableName env)

-- | Run a TestOrville action with the test environment
runOrvilleTest :: OrvilleTestEnv registry -> TestOrville registry a -> IO a
runOrvilleTest env = flip runReaderT (testOrvilleState env) . runOrvilleDb (arbiterEnv env)

-- | A handler written in 'TestOrville' against the given schema, run in its base monad
orvilleTestHandler :: Text -> (job -> TestOrville registry result) -> job -> ReaderT O.OrvilleState IO result
orvilleTestHandler schemaName handler = runOrvilleDb OrvilleEnv {schema = schemaName, listener = Nothing} . handler

arbiterEnv :: OrvilleTestEnv registry -> OrvilleEnv registry
arbiterEnv env = OrvilleEnv {schema = testSchema env, listener = testListen env}

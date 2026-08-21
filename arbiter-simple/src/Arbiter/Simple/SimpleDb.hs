{-# LANGUAGE TypeFamilies #-}

-- | The postgresql-simple database monad. Its 'MonadArbiter' instance is built in, so it
-- is usable directly:
--
-- @
-- import Arbiter.Core
-- import Arbiter.Simple
--
-- myFunction :: SimpleDb MyRegistry IO ()
-- myFunction = insertJob (defaultJob myPayload)
-- @
module Arbiter.Simple.SimpleDb
  ( -- * Database Monad
    SimpleDb (..)
  , SimpleEnv (..)
  , runSimpleDb
  , inTransaction

    -- * Environment Creation
  , createSimpleEnv
  , createSimpleEnvWithConfig
  , createSimpleEnvWithPool
  , destroySimpleEnv
  , disableListener
  , useDedicatedListener
  ) where

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Listen (Listener, dedicatedListener, newDedicatedListen, newPoolListener)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.PoolConfig qualified as PC
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, asks, local)
import Control.Monad.Trans.Reader (ReaderT (..), runReaderT)
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Pool (Pool, defaultPoolConfig, destroyAllResources, newPool, setNumStripes, withResource)
import Data.Proxy (Proxy (..))
import Database.PostgreSQL.Simple (Connection, close, connectPostgreSQL)
import Database.PostgreSQL.Simple.Internal (withConnection)
import UnliftIO (MonadUnliftIO)

import Arbiter.Simple.MonadArbiter
  ( HasSimplePool (..)
  , SimpleConnectionPool (..)
  , simpleExecuteQuery
  , simpleExecuteStatement
  , simpleRunHandlerWithConnection
  , simpleWithDbTransaction
  )

-- | Schema name and connection pool for 'SimpleDb'.
data SimpleEnv (registry :: JobPayloadRegistry) = SimpleEnv
  { schema :: SchemaName
  -- ^ Schema name
  , simplePool :: SimpleConnectionPool
  -- ^ The connection pool state
  , listener :: Maybe Listener
  -- ^ Resolved LISTEN source. 'Nothing' runs poll-only.
  }

-- | The postgresql-simple database monad.
newtype SimpleDb (registry :: JobPayloadRegistry) m a = SimpleDb {unSimpleDb :: ReaderT (SimpleEnv registry) m a}
  deriving newtype
    ( Applicative
    , Functor
    , Monad
    , MonadCatch
    , MonadFail
    , MonadIO
    , MonadMask
    , MonadReader (SimpleEnv registry)
    , MonadThrow
    , MonadUnliftIO
    )

instance (Monad m) => HasSimplePool (SimpleDb registry m) where
  getSimplePool = asks simplePool
  localSimplePool f = local (\env -> env {simplePool = f (simplePool env)})

instance (MonadUnliftIO m) => MonadArbiter (SimpleDb registry m) where
  type RegistryOf (SimpleDb registry m) = registry
  type Handler (SimpleDb registry m) job result = Connection -> job -> SimpleDb registry m result
  getSchema = asks schema
  executeQuery = simpleExecuteQuery
  executeStatement = simpleExecuteStatement
  withDbTransaction = simpleWithDbTransaction
  runHandlerWithConnection = simpleRunHandlerWithConnection
  getListener = asks listener

-- | Release the env's connection pool, closing its open connections.
destroySimpleEnv :: (MonadIO m) => SimpleEnv registry -> m ()
destroySimpleEnv env =
  liftIO $ traverse_ destroyAllResources (connectionPool (simplePool env))

-- | Turn off the shared LISTEN listener for an env, running poll-only.
disableListener :: SimpleEnv registry -> SimpleEnv registry
disableListener env = env {listener = Nothing}

-- | Give the env a dedicated LISTEN connection opened from a connection string,
-- rather than borrowing a slot from the pool.
useDedicatedListener :: (MonadIO m) => ByteString -> SimpleEnv registry -> m (SimpleEnv registry)
useDedicatedListener connStr env = do
  d <- newDedicatedListen connStr
  pure env {listener = Just (dedicatedListener d)}

-- | A listener that borrows one pool connection for the hub's lifetime.
poolListener :: Pool Connection -> IO Listener
poolListener pool = newPoolListener (\action -> withResource pool (`withConnection` action))

-- | Run a 'SimpleDb' action in its env.
runSimpleDb :: SimpleEnv registry -> SimpleDb registry m a -> m a
runSimpleDb env action = runReaderT (unSimpleDb action) env

-- | Run a 'SimpleDb' action on one connection, no pool or env involved. It is pinned as
-- though a transaction were already open, so 'Arbiter.Core.MonadArbiter.withDbTransaction'
-- nests through savepoints and the caller keeps ownership of the transaction itself.
--
-- @
-- PG.withTransaction conn $ do
--   PG.execute conn "INSERT INTO orders ..." params
--   inTransaction conn "arbiter" $
--     Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
-- @
inTransaction
  :: forall registry m a
   . Connection
  -> SchemaName
  -- ^ Schema name
  -> SimpleDb registry m a
  -> m a
inTransaction conn schemaName action =
  let env =
        SimpleEnv
          { schema = schemaName
          , simplePool =
              SimpleConnectionPool
                { connectionPool = Nothing
                , activeConn = Just conn
                , transactionDepth = 1
                }
          , listener = Nothing
          }
   in runSimpleDb env action

-- | Create a 'SimpleEnv' with default pool settings.
-- For workers, use 'createSimpleEnvWithConfig' with @poolConfigForWorkers@ instead.
createSimpleEnv
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> SchemaName
  -- ^ Schema name
  -> m (SimpleEnv registry)
createSimpleEnv proxy connStr schemaName =
  createSimpleEnvWithConfig proxy connStr schemaName PC.defaultPoolConfig

-- | Create a 'SimpleEnv' with custom pool settings.
--
-- @
-- let config = PoolConfig
--       { poolSize = 50
--       , poolIdleTimeout = 120
--       , poolStripes = Just 4
--       }
-- env <- createSimpleEnvWithConfig (Proxy @MyRegistry) "host=localhost dbname=mydb" "arbiter" config
-- @
createSimpleEnvWithConfig
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> SchemaName
  -- ^ Schema name
  -> PoolConfig
  -- ^ Pool configuration
  -> m (SimpleEnv registry)
createSimpleEnvWithConfig _proxy connStr schemaName config = liftIO $ do
  let stripes = poolStripes config
  connPool <-
    newPool
      $ setNumStripes stripes
      $ defaultPoolConfig
        (connectPostgreSQL connStr)
        close
        (fromIntegral $ poolIdleTimeout config) -- idle time (seconds)
        (poolSize config)
  lstn <- poolListener connPool
  pure
    SimpleEnv
      { schema = schemaName
      , simplePool = SimpleConnectionPool {connectionPool = Just connPool, activeConn = Nothing, transactionDepth = 0}
      , listener = Just lstn
      }

-- | Create a 'SimpleEnv' over a caller's own connection pool. The shared
-- listener borrows one connection from that pool and holds it for the env's
-- lifetime, so size the pool for the worker load plus one. Use 'disableListener'
-- to run poll-only and reclaim that slot, or 'useDedicatedListener' to give the
-- listener its own connection.
createSimpleEnvWithPool
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> Pool Connection
  -- ^ User-provided connection pool
  -> SchemaName
  -- ^ Schema name
  -> m (SimpleEnv registry)
createSimpleEnvWithPool _proxy connPool schemaName = liftIO $ do
  lstn <- poolListener connPool
  pure
    SimpleEnv
      { schema = schemaName
      , simplePool = SimpleConnectionPool {connectionPool = Just connPool, activeConn = Nothing, transactionDepth = 0}
      , listener = Just lstn
      }

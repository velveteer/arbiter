{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

-- | Simple database monad for Arbiter with postgresql-simple backend.
--
-- 'SimpleDb' has a built-in 'MonadArbiter' instance, so you can use it directly:
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
  ) where

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema (..))
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.PoolConfig qualified as PC
import Arbiter.Core.QueueRegistry (AllQueuesUnique)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, asks, local)
import Control.Monad.Trans.Reader (ReaderT (..), runReaderT)
import Data.ByteString (ByteString)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple (Connection, close, connectPostgreSQL)
import UnliftIO (MonadUnliftIO)

import Arbiter.Simple.MonadArbiter
  ( HasSimplePool (..)
  , SimpleConnectionPool (..)
  , simpleExecuteQuery
  , simpleExecuteStatement
  , simpleRunHandlerWithConnection
  , simpleWithDbTransaction
  )

-- | Environment for SimpleDb operations
--
-- Contains both the schema name and the connection pool.
data SimpleEnv registry = SimpleEnv
  { schema :: Text
  -- ^ PostgreSQL schema name where job tables are located
  , simplePool :: SimpleConnectionPool
  -- ^ The connection pool state
  }

-- | Simple database monad using postgresql-simple.
newtype SimpleDb registry m a = SimpleDb {unSimpleDb :: ReaderT (SimpleEnv registry) m a}
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

instance (Monad m) => HasArbiterSchema (SimpleDb registry m) registry where
  getSchema = asks schema

instance (Monad m) => HasSimplePool (SimpleDb registry m) where
  getSimplePool = asks simplePool
  localSimplePool f = local (\env -> env {simplePool = f (simplePool env)})

instance (Monad m, MonadIO m, MonadUnliftIO m) => MonadArbiter (SimpleDb registry m) where
  type Handler (SimpleDb registry m) jobs result = Connection -> jobs -> SimpleDb registry m result
  executeQuery = simpleExecuteQuery
  executeStatement = simpleExecuteStatement
  withDbTransaction = simpleWithDbTransaction
  runHandlerWithConnection = simpleRunHandlerWithConnection

-- | Run a SimpleDb action with a SimpleEnv.
runSimpleDb :: SimpleEnv registry -> SimpleDb registry m a -> m a
runSimpleDb env action = runReaderT (unSimpleDb action) env

-- | Run a SimpleDb action using a single postgresql-simple connection.
--
-- No pool or env is needed. The connection is pinned with @transactionDepth = 1@,
-- so arbiter's 'withDbTransaction' uses savepoints instead of issuing @BEGIN@.
-- The caller is responsible for transaction lifecycle on the connection.
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
  -> Text
  -- ^ PostgreSQL schema name
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
          }
   in runSimpleDb env action

-- | Create a JobQueue environment with resource-pool connection pooling
--
-- Uses conservative defaults (10 connections, 300s idle timeout, 1 stripe).
--
-- For worker pools, consider using 'createSimpleEnvWithConfig' with @poolConfigForWorkers@
-- to size the pool based on worker count:
--
-- @
-- poolCfg <- poolConfigForWorkers 10
-- env <- createSimpleEnvWithConfig (Proxy @MyRegistry) connStr "arbiter" poolCfg
-- @
--
-- All job tables are created within the specified schema.
createSimpleEnv
  :: forall registry m
   . (AllQueuesUnique registry, MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> Text
  -- ^ PostgreSQL schema name (e.g., "arbiter", "public")
  -> m (SimpleEnv registry)
createSimpleEnv proxy connStr schemaName =
  createSimpleEnvWithConfig proxy connStr schemaName PC.defaultPoolConfig

-- | Control pool sizing, idle timeout, and striping.
--
-- Example:
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
   . (AllQueuesUnique registry, MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> Text
  -- ^ PostgreSQL schema name
  -> PoolConfig
  -- ^ Pool configuration
  -> m (SimpleEnv registry)
createSimpleEnvWithConfig _proxy connStr schemaName config = liftIO $ do
  let stripes = poolStripes config
  connPool <-
    newPool $
      setNumStripes stripes $
        defaultPoolConfig
          (connectPostgreSQL connStr)
          close
          (fromIntegral $ poolIdleTimeout config) -- idle time (seconds)
          (poolSize config)
  pure
    SimpleEnv
      { schema = schemaName
      , simplePool = SimpleConnectionPool {connectionPool = Just connPool, activeConn = Nothing, transactionDepth = 0}
      }

-- | Create a SimpleEnv with a user-provided connection pool
createSimpleEnvWithPool
  :: forall registry
   . (AllQueuesUnique registry)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> Pool Connection
  -- ^ User-provided connection pool
  -> Text
  -- ^ PostgreSQL schema name
  -> SimpleEnv registry
createSimpleEnvWithPool _proxy connPool schemaName =
  SimpleEnv
    { schema = schemaName
    , simplePool = SimpleConnectionPool {connectionPool = Just connPool, activeConn = Nothing, transactionDepth = 0}
    }

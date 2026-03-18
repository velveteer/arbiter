{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

-- | Hasql database monad for Arbiter.
--
-- 'HasqlDb' has a built-in 'MonadArbiter' instance, so you can use it directly:
--
-- @
-- import Arbiter.Core
-- import Arbiter.Hasql
--
-- myFunction :: HasqlDb MyRegistry IO ()
-- myFunction = insertJob (defaultJob myPayload)
-- @
module Arbiter.Hasql.HasqlDb
  ( -- * Database Monad
    HasqlDb (..)
  , HasqlEnv (..)
  , runHasqlDb
  , inTransaction

    -- * Environment Creation
  , createHasqlEnv
  , createHasqlEnvWithConfig
  , createHasqlEnvWithPool

    -- * Hasql Settings
  , hasqlSettings
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
import Hasql.Connection qualified as Hasql
import UnliftIO (MonadUnliftIO)

import Arbiter.Hasql.Compat qualified as Compat
import Arbiter.Hasql.MonadArbiter
  ( HasHasqlPool (..)
  , HasqlConnectionPool (..)
  , hasqlExecuteQuery
  , hasqlExecuteStatement
  , hasqlRunHandlerWithConnection
  , hasqlWithDbTransaction
  )

-- | Environment for HasqlDb operations
--
-- Contains both the schema name and the connection pool.
data HasqlEnv registry = HasqlEnv
  { schema :: Text
  -- ^ PostgreSQL schema name where job tables are located
  , hasqlPool :: HasqlConnectionPool
  -- ^ The connection pool state
  }

-- | Hasql database monad for Arbiter.
newtype HasqlDb registry m a = HasqlDb {unHasqlDb :: ReaderT (HasqlEnv registry) m a}
  deriving newtype
    ( Applicative
    , Functor
    , Monad
    , MonadCatch
    , MonadFail
    , MonadIO
    , MonadMask
    , MonadReader (HasqlEnv registry)
    , MonadThrow
    , MonadUnliftIO
    )

instance (Monad m) => HasArbiterSchema (HasqlDb registry m) registry where
  getSchema = asks schema

instance (Monad m) => HasHasqlPool (HasqlDb registry m) where
  getHasqlPool = asks hasqlPool
  localHasqlPool f = local (\env -> env {hasqlPool = f (hasqlPool env)})

instance (Monad m, MonadIO m, MonadUnliftIO m) => MonadArbiter (HasqlDb registry m) where
  type Handler (HasqlDb registry m) jobs result = Hasql.Connection -> jobs -> HasqlDb registry m result
  executeQuery = hasqlExecuteQuery
  executeStatement = hasqlExecuteStatement
  withDbTransaction = hasqlWithDbTransaction
  runHandlerWithConnection = hasqlRunHandlerWithConnection

-- | Run a HasqlDb action with a HasqlEnv.
runHasqlDb :: HasqlEnv registry -> HasqlDb registry m a -> m a
runHasqlDb env action = runReaderT (unHasqlDb action) env

-- | Run a HasqlDb action using a single hasql connection.
--
-- No pool is needed. The connection is pinned with @transactionDepth = 1@,
-- so arbiter's 'withDbTransaction' uses savepoints instead of issuing @BEGIN@.
-- The caller is responsible for transaction lifecycle on the connection.
--
-- @
-- _ <- Hasql.use conn (Session.script "BEGIN")
-- inTransaction conn "arbiter" $ do
--   Arb.insertJob (Arb.defaultJob myPayload)
-- _ <- Hasql.use conn (Session.script "COMMIT")
-- @
inTransaction
  :: forall registry m a
   . Hasql.Connection
  -> Text
  -- ^ PostgreSQL schema name
  -> HasqlDb registry m a
  -> m a
inTransaction conn schemaName action =
  let env =
        HasqlEnv
          { schema = schemaName
          , hasqlPool =
              HasqlConnectionPool
                { connectionPool = Nothing
                , activeConn = Just conn
                , transactionDepth = 1
                }
          }
   in runHasqlDb env action

-- | Create a HasqlEnv with conservative defaults (10 connections, 300s idle timeout, 1 stripe).
--
-- For worker pools, consider using 'createHasqlEnvWithConfig' with @poolConfigForWorkers@
-- to size the pool based on worker count.
createHasqlEnv
  :: forall registry m
   . (AllQueuesUnique registry, MonadIO m)
  => Proxy registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> m (HasqlEnv registry)
createHasqlEnv proxy connStr schemaName =
  createHasqlEnvWithConfig proxy connStr schemaName PC.defaultPoolConfig

-- | Create a HasqlEnv with custom pool configuration.
createHasqlEnvWithConfig
  :: forall registry m
   . (AllQueuesUnique registry, MonadIO m)
  => Proxy registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> Text
  -- ^ PostgreSQL schema name
  -> PoolConfig
  -> m (HasqlEnv registry)
createHasqlEnvWithConfig _proxy connStr schemaName config = liftIO $ do
  connPool <-
    newPool $
      setNumStripes (poolStripes config) $
        defaultPoolConfig
          ( do
              result <- Hasql.acquire (hasqlSettings connStr)
              case result of
                Right conn -> pure conn
                Left err -> error $ "hasql: connection failed: " <> show err
          )
          Hasql.release
          (fromIntegral $ poolIdleTimeout config)
          (poolSize config)
  pure
    HasqlEnv
      { schema = schemaName
      , hasqlPool = HasqlConnectionPool {connectionPool = Just connPool, activeConn = Nothing, transactionDepth = 0}
      }

-- | Create a HasqlEnv with a user-provided connection pool.
createHasqlEnvWithPool
  :: forall registry
   . (AllQueuesUnique registry)
  => Proxy registry
  -> Pool Hasql.Connection
  -> Text
  -- ^ PostgreSQL schema name
  -> HasqlEnv registry
createHasqlEnvWithPool _proxy connPool schemaName =
  HasqlEnv
    { schema = schemaName
    , hasqlPool = HasqlConnectionPool {connectionPool = Just connPool, activeConn = Nothing, transactionDepth = 0}
    }

-- | Re-exported from "Arbiter.Hasql.Compat".
hasqlSettings :: ByteString -> Compat.HasqlSettings
hasqlSettings = Compat.hasqlSettings

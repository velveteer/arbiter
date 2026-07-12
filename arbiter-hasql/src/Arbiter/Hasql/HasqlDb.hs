{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TypeFamilies #-}

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
  , withHasqlEnv
  , HasqlPool
  , newHasqlPool
  , withHasqlPool
  , setPreparedStatements

    -- * Hasql Settings
  , hasqlSettings

    -- * Exceptions
  , HasqlConnectionError (..)
  ) where

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema (..))
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.PoolConfig qualified as PC
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Control.Exception (Exception, bracket, throwIO)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, asks, local)
import Control.Monad.Trans.Reader (ReaderT (..), runReaderT)
import Data.ByteString (ByteString)
import Data.Pool (Pool, defaultPoolConfig, destroyAllResources, newPool, setNumStripes)
import Data.Proxy (Proxy (..))
import Hasql.Connection qualified as Hasql
import UnliftIO (MonadUnliftIO)

import Arbiter.Hasql.Compat qualified as Compat
import Arbiter.Hasql.MonadArbiter
  ( HasHasqlPool (..)
  , HasqlConnectionPool (..)
  , hasqlExecuteQuery
  , hasqlExecuteQueryPrepared
  , hasqlExecuteStatement
  , hasqlRunHandlerWithConnection
  , hasqlWithDbTransaction
  )

-- | Thrown when a hasql connection cannot be acquired from the pool.
newtype HasqlConnectionError = HasqlConnectionError String
  deriving stock (Show)
  deriving anyclass (Exception)

-- | Schema name and connection pool for 'HasqlDb'.
data HasqlEnv (registry :: JobPayloadRegistry) = HasqlEnv
  { schema :: SchemaName
  -- ^ Schema name
  , hasqlPool :: HasqlConnectionPool
  -- ^ The connection pool state
  }

-- | Hasql database monad for Arbiter.
newtype HasqlDb (registry :: JobPayloadRegistry) m a = HasqlDb {unHasqlDb :: ReaderT (HasqlEnv registry) m a}
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
  executeQueryPrepared = hasqlExecuteQueryPrepared
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
  -> SchemaName
  -- ^ Schema name
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
                , preparedStatements = False
                }
          }
   in runHasqlDb env action

-- | Create a HasqlEnv with conservative defaults (10 connections, 300s idle timeout, 1 stripe).
--
-- For worker pools, consider using 'createHasqlEnvWithConfig' with @poolConfigForWorkers@
-- to size the pool based on worker count.
createHasqlEnv
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> SchemaName
  -- ^ Schema name
  -> m (HasqlEnv registry)
createHasqlEnv proxy connStr schemaName =
  createHasqlEnvWithConfig proxy connStr schemaName PC.defaultPoolConfig

-- | Create a HasqlEnv with custom pool configuration.
createHasqlEnvWithConfig
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> SchemaName
  -- ^ Schema name
  -> PoolConfig
  -> m (HasqlEnv registry)
createHasqlEnvWithConfig proxy connStr schemaName config = liftIO $ do
  connPool <- newHasqlPool connStr config
  pure (createHasqlEnvWithPool proxy connPool schemaName)

-- | Connections the envs built over it draw from.
type HasqlPool = Pool Hasql.Connection

-- | A connection pool for a schema, shareable across the envs built over it.
newHasqlPool :: (MonadIO m) => ByteString -> PoolConfig -> m HasqlPool
newHasqlPool connStr config =
  liftIO . newPool $
    setNumStripes (poolStripes config) $
      defaultPoolConfig
        ( do
            result <- Hasql.acquire (hasqlSettings connStr)
            case result of
              Right conn -> pure conn
              Left err -> throwIO $ HasqlConnectionError (show err)
        )
        Hasql.release
        (fromIntegral $ poolIdleTimeout config)
        (poolSize config)

-- | Run an action with a pool, destroyed on exit.
withHasqlPool :: ByteString -> PoolConfig -> (HasqlPool -> IO a) -> IO a
withHasqlPool connStr config = bracket (newHasqlPool connStr config) destroyAllResources

-- | Run an action with a HasqlEnv over a pool created for it, destroyed on exit.
withHasqlEnv
  :: forall registry a
   . Proxy registry
  -> ByteString
  -- ^ PostgreSQL connection string
  -> SchemaName
  -- ^ Schema name
  -> PoolConfig
  -> (HasqlEnv registry -> IO a)
  -> IO a
withHasqlEnv proxy connStr schemaName config act =
  withHasqlPool connStr config (act . flip (createHasqlEnvWithPool proxy) schemaName)

-- | Create a HasqlEnv with a user-provided connection pool.
createHasqlEnvWithPool
  :: forall registry
   . Proxy registry
  -> HasqlPool
  -> SchemaName
  -- ^ Schema name
  -> HasqlEnv registry
createHasqlEnvWithPool _proxy connPool schemaName =
  HasqlEnv
    { schema = schemaName
    , hasqlPool =
        HasqlConnectionPool
          { connectionPool = Just connPool
          , activeConn = Nothing
          , transactionDepth = 0
          , preparedStatements = True
          }
    }

-- | Enable or disable prepared hot statements (the claim), on by default. Prepared
-- once per pooled connection, so the plan is reused instead of rebuilt every call.
-- Disable it behind a pooler that does not support server-side prepared statements,
-- such as PgBouncer in transaction or statement mode.
setPreparedStatements :: Bool -> HasqlEnv registry -> HasqlEnv registry
setPreparedStatements flag env = env {hasqlPool = (hasqlPool env) {preparedStatements = flag}}

-- | Re-exported from "Arbiter.Hasql.Compat".
hasqlSettings :: ByteString -> Compat.HasqlSettings
hasqlSettings = Compat.hasqlSettings

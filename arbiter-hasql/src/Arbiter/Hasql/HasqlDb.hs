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
  , destroyHasqlEnv
  , disableListener
  , useDedicatedListener
  , setPreparedStatements

    -- * Hasql Settings
  , hasqlSettings

    -- * Exceptions
  , HasqlConnectionError (..)
  ) where

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Listen (Listener, dedicatedListener, newDedicatedListen, newPoolListener)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.PoolConfig qualified as PC
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Control.Exception (Exception, throwIO)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, asks, local)
import Control.Monad.Trans.Reader (ReaderT (..), runReaderT)
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Pool (Pool, defaultPoolConfig, destroyAllResources, newPool, setNumStripes, withResource)
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
  , listener :: Maybe Listener
  -- ^ Resolved LISTEN source. 'Nothing' runs poll-only.
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

instance (Monad m) => HasHasqlPool (HasqlDb registry m) where
  getHasqlPool = asks hasqlPool
  localHasqlPool f = local (\env -> env {hasqlPool = f (hasqlPool env)})

instance (MonadUnliftIO m) => MonadArbiter (HasqlDb registry m) where
  type RegistryOf (HasqlDb registry m) = registry
  type Handler (HasqlDb registry m) job result = Hasql.Connection -> job -> HasqlDb registry m result
  getSchema = asks schema
  executeQuery = hasqlExecuteQuery
  executeQueryPrepared = hasqlExecuteQueryPrepared
  executeStatement = hasqlExecuteStatement
  withDbTransaction = hasqlWithDbTransaction
  runHandlerWithConnection = hasqlRunHandlerWithConnection
  getListener = asks listener

-- | Release the env's connection pool, closing its open connections.
destroyHasqlEnv :: (MonadIO m) => HasqlEnv registry -> m ()
destroyHasqlEnv env =
  liftIO $ traverse_ destroyAllResources (connectionPool (hasqlPool env))

-- | Turn off the shared LISTEN listener for an env, running poll-only.
disableListener :: HasqlEnv registry -> HasqlEnv registry
disableListener env = env {listener = Nothing}

-- | Give the env a dedicated LISTEN connection opened from a connection string,
-- rather than borrowing a slot from the pool.
useDedicatedListener :: (MonadIO m) => ByteString -> HasqlEnv registry -> m (HasqlEnv registry)
useDedicatedListener connStr env = do
  d <- newDedicatedListen connStr
  pure env {listener = Just (dedicatedListener d)}

-- | A listener that borrows one pool connection for the hub's lifetime.
poolListener :: Pool Hasql.Connection -> IO Listener
poolListener pool = newPoolListener (\action -> withResource pool (`Compat.withHasqlLibPQConnection` action))

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
                , preparedStatements = True
                }
          , listener = Nothing
          }
   in runHasqlDb env action

-- | Create a HasqlEnv with conservative defaults.
--
-- For worker pools, use 'createHasqlEnvWithConfig' with @poolConfigForWorkers@
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
createHasqlEnvWithConfig _proxy connStr schemaName config = liftIO $ do
  connPool <-
    newPool $
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
  lstn <- poolListener connPool
  pure
    HasqlEnv
      { schema = schemaName
      , hasqlPool =
          HasqlConnectionPool
            { connectionPool = Just connPool
            , activeConn = Nothing
            , transactionDepth = 0
            , preparedStatements = True
            }
      , listener = Just lstn
      }

-- | Create a HasqlEnv with a user-provided connection pool. The shared
-- listener borrows one connection from that pool and holds it for the env's
-- lifetime, so size the pool for the worker load plus one. Use 'disableListener'
-- to run poll-only and reclaim that slot, or 'useDedicatedListener' to give the
-- listener its own connection.
createHasqlEnvWithPool
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -> Pool Hasql.Connection
  -> SchemaName
  -- ^ Schema name
  -> m (HasqlEnv registry)
createHasqlEnvWithPool _proxy connPool schemaName = liftIO $ do
  lstn <- poolListener connPool
  pure
    HasqlEnv
      { schema = schemaName
      , hasqlPool =
          HasqlConnectionPool
            { connectionPool = Just connPool
            , activeConn = Nothing
            , transactionDepth = 0
            , preparedStatements = True
            }
      , listener = Just lstn
      }

-- | Enable or disable prepared hot statements (the claim). Prepared once per pooled
-- connection, so the plan is reused instead of rebuilt every call. Requires direct
-- connections or a pooler that supports server-side prepared statements.
setPreparedStatements :: Bool -> HasqlEnv registry -> HasqlEnv registry
setPreparedStatements flag env = env {hasqlPool = (hasqlPool env) {preparedStatements = flag}}

-- | Re-exported from "Arbiter.Hasql.Compat".
hasqlSettings :: ByteString -> Compat.HasqlSettings
hasqlSettings = Compat.hasqlSettings

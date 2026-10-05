{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE TypeFamilies #-}

-- | The hasql database monad with a built-in 'MonadArbiter' instance:
--
-- @
-- import Arbiter.Core
-- import Arbiter.Hasql
-- import Control.Monad (void)
--
-- myFunction :: HasqlDb MyRegistry IO ()
-- myFunction = void $ insertJob (defaultJob myPayload)
-- @
module Arbiter.Hasql.HasqlDb
  ( -- * Database Monad
    HasqlDb (..)
  , HasqlEnv
  , Db
  , Env (..)
  , HasqlConfig (..)
  , PoolState (..)
  , HasPoolState (..)
  , runHasqlDb
  , inTransaction
  , inTransactionWith

    -- * Environment creation
  , HasqlConnect
  , toHasqlConnect
  , acquireConnect
  , createHasqlEnv
  , createHasqlEnvWithConfig
  , createHasqlEnvWithPool
  , destroyHasqlEnv
  , disableListener
  , useDedicatedListener
  , setPreparedStatements

    -- * Hasql settings
  , HasqlSettings
  , hasqlSettings

    -- * Exceptions
  , HasqlConnectionError (..)
  ) where

import Arbiter.Core.Backend
  ( Db
  , Driver (..)
  , Env (..)
  , HasPoolState (..)
  , PoolState (..)
  , createEnvWithConfig
  , createEnvWithPool
  , destroyEnv
  , disableListener
  , runDb
  )
import Arbiter.Core.Backend qualified as Backend
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.PoolConfig (PoolConfig)
import Arbiter.Core.PoolConfig qualified as PC
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Control.Exception (Exception, throwIO)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader, asks)
import Data.Pool (Pool)
import Data.Proxy (Proxy (..))
import Hasql.Connection qualified as Hasql
import UnliftIO (MonadUnliftIO)

import Arbiter.Hasql.Compat
  ( HasqlConnect
  , HasqlSettings
  , acquireConnect
  , hasqlSettings
  , toHasqlConnect
  , withDedicatedListenConn
  , withHasqlListenConn
  )
import Arbiter.Hasql.MonadArbiter
  ( hasqlExecuteQuery
  , hasqlExecuteQueryPrepared
  , hasqlExecuteStatement
  , hasqlRunHandlerWithConnection
  , hasqlWithDbTransaction
  )

-- | Thrown when a new hasql connection cannot be opened.
newtype HasqlConnectionError = HasqlConnectionError String
  deriving stock (Show)
  deriving anyclass (Exception)

-- | The 'Env' for 'HasqlDb'.
type HasqlEnv = Env Hasql.Connection HasqlConfig

-- | The env state only hasql has.
newtype HasqlConfig = HasqlConfig
  { preparedStatements :: Bool
  -- ^ Prepare hot statements once per connection. Default: 'True'.
  }

hasqlDriver :: Driver Hasql.Connection HasqlConfig
hasqlDriver = Driver {withListenConn = withHasqlListenConn, initialConfig = HasqlConfig True}

-- | The hasql database monad.
newtype HasqlDb (registry :: JobPayloadRegistry) m a = HasqlDb
  { unHasqlDb :: Db Hasql.Connection HasqlConfig registry m a
  }
  deriving newtype
    ( Applicative
    , Functor
    , HasPoolState Hasql.Connection
    , Monad
    , MonadCatch
    , MonadFail
    , MonadIO
    , MonadMask
    , MonadReader (HasqlEnv registry)
    , MonadThrow
    , MonadUnliftIO
    )

instance (MonadUnliftIO m) => MonadArbiter (HasqlDb registry m) where
  type RegistryOf (HasqlDb registry m) = registry
  type Handler (HasqlDb registry m) job result = Hasql.Connection -> job -> HasqlDb registry m result
  getSchema = asks schema
  executeQuery = hasqlExecuteQuery
  executeQueryPrepared query = asks (preparedStatements . driverConfig) >>= (`hasqlExecuteQueryPrepared` query)
  executeStatement = hasqlExecuteStatement
  withDbTransaction = hasqlWithDbTransaction
  runHandlerWithConnection = hasqlRunHandlerWithConnection
  getListener = asks listener

-- | Release the env's connection pool, closing its open connections.
destroyHasqlEnv :: (MonadIO m) => HasqlEnv registry -> m ()
destroyHasqlEnv = destroyEnv

-- | Run a 'HasqlDb' action in its env.
runHasqlDb :: HasqlEnv registry -> HasqlDb registry m a -> m a
runHasqlDb env = runDb env . unHasqlDb

-- | Run a 'HasqlDb' action on one connection pinned as the caller's open transaction.
--
-- @
-- _ <- Hasql.use conn (Session.script "BEGIN")
-- inTransaction conn "arbiter" $ do
--   Arb.insertJob (Arb.defaultJob myPayload)
-- _ <- Hasql.use conn (Session.script "COMMIT")
-- @
--
-- @Session.script@ exists only on hasql >= 1.10. Use @Session.sql@ on older versions.
inTransaction
  :: forall registry m a
   . Hasql.Connection
  -- ^ Connection with an open transaction
  -> SchemaName
  -- ^ Schema name
  -> HasqlDb registry m a
  -> m a
inTransaction = inTransactionWith (initialConfig hasqlDriver)

-- | 'inTransaction' with explicit settings, such as prepared statements off.
inTransactionWith
  :: forall registry m a
   . HasqlConfig
  -- ^ Driver settings
  -> Hasql.Connection
  -- ^ Connection with an open transaction
  -> SchemaName
  -- ^ Schema name
  -> HasqlDb registry m a
  -> m a
inTransactionWith config conn schemaName =
  Backend.inTransaction hasqlDriver {initialConfig = config} conn schemaName . unHasqlDb

-- | Create a 'HasqlEnv' with 'Arbiter.Core.PoolConfig.defaultPoolConfig'. Size worker pools with
-- 'createHasqlEnvWithConfig' and @Arbiter.Worker.poolConfigForWorkers@. The listener holds one pool slot.
createHasqlEnv
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> HasqlConnect
  -- ^ Connection settings
  -> SchemaName
  -- ^ Schema name
  -> m (HasqlEnv registry)
createHasqlEnv proxy connect schemaName = createHasqlEnvWithConfig proxy connect schemaName PC.defaultPoolConfig

-- | Create a 'HasqlEnv' with custom pool settings. The listener holds one pool slot.
--
-- @
-- let config = PoolConfig
--       { poolSize = 50
--       , poolIdleTimeout = 120
--       , poolStripes = Just 4
--       }
-- env <- createHasqlEnvWithConfig (Proxy \@MyRegistry) (toHasqlConnect Ffi.adapter connStr) "arbiter" config
-- @
createHasqlEnvWithConfig
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> HasqlConnect
  -- ^ Connection settings
  -> SchemaName
  -- ^ Schema name
  -> PoolConfig
  -- ^ Pool configuration
  -> m (HasqlEnv registry)
createHasqlEnvWithConfig _proxy connect = createEnvWithConfig hasqlDriver (acquireOrThrow connect) Hasql.release

-- | Give the env a dedicated LISTEN connection that takes no pool slot.
useDedicatedListener :: (MonadIO m) => HasqlConnect -> HasqlEnv registry -> m (HasqlEnv registry)
useDedicatedListener = Backend.useDedicatedListener . withDedicatedListenConn

acquireOrThrow :: HasqlConnect -> IO Hasql.Connection
acquireOrThrow connect = acquireConnect connect >>= either (throwIO . HasqlConnectionError) pure

-- | Create a 'HasqlEnv' over a caller's own connection pool. The listener holds one pool slot.
createHasqlEnvWithPool
  :: forall registry m
   . (MonadIO m)
  => Proxy registry
  -- ^ Type-level job payload registry
  -> Pool Hasql.Connection
  -- ^ Caller's connection pool
  -> SchemaName
  -- ^ Schema name
  -> m (HasqlEnv registry)
createHasqlEnvWithPool _proxy = createEnvWithPool hasqlDriver

-- | Enable or disable prepared statements on the hot path. On by default. Needs direct
-- connections or a pooler that supports them.
setPreparedStatements :: Bool -> HasqlEnv registry -> HasqlEnv registry
setPreparedStatements flag env = env {driverConfig = HasqlConfig flag}

{-# LANGUAGE TypeFamilies #-}

-- | The postgresql-simple database monad with a built-in 'MonadArbiter' instance:
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
  , SimpleEnv
  , Db
  , Env (..)
  , PoolState (..)
  , HasPoolState (..)
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
import Arbiter.LibPQ (libpqListenConn, withLibPQListenConn)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Reader (MonadReader, asks)
import Data.ByteString (ByteString)
import Data.Pool (Pool)
import Data.Proxy (Proxy (..))
import Database.PostgreSQL.Simple (Connection, close, connectPostgreSQL)
import Database.PostgreSQL.Simple.Internal (withConnection)
import UnliftIO (MonadUnliftIO)

import Arbiter.Simple.MonadArbiter
  ( simpleExecuteQuery
  , simpleExecuteStatement
  , simpleRunHandlerWithConnection
  , simpleWithDbTransaction
  )

-- | Schema name and connection pool for 'SimpleDb'.
type SimpleEnv = Env Connection ()

simpleDriver :: Driver Connection ()
simpleDriver = Driver {withListenConn = \conn action -> withConnection conn (action . libpqListenConn), initialConfig = ()}

-- | The postgresql-simple database monad.
newtype SimpleDb (registry :: JobPayloadRegistry) m a = SimpleDb {unSimpleDb :: Db Connection () registry m a}
  deriving newtype
    ( Applicative
    , Functor
    , HasPoolState Connection
    , Monad
    , MonadCatch
    , MonadFail
    , MonadIO
    , MonadMask
    , MonadReader (SimpleEnv registry)
    , MonadThrow
    , MonadUnliftIO
    )

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
destroySimpleEnv = destroyEnv

-- | Run a 'SimpleDb' action in its env.
runSimpleDb :: SimpleEnv registry -> SimpleDb registry m a -> m a
runSimpleDb env = runDb env . unSimpleDb

-- | Run a 'SimpleDb' action on one connection pinned as the caller's open transaction.
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
inTransaction conn schemaName = Backend.inTransaction simpleDriver conn schemaName . unSimpleDb

-- | Create a 'SimpleEnv' with default pool settings. Size worker pools with
-- 'createSimpleEnvWithConfig' and @poolConfigForWorkers@.
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
-- env <- createSimpleEnvWithConfig (Proxy \@MyRegistry) "host=localhost dbname=mydb" "arbiter" config
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
createSimpleEnvWithConfig _proxy connStr = createEnvWithConfig simpleDriver (connectPostgreSQL connStr) close

-- | Create a 'SimpleEnv' over a caller's own connection pool. The listener holds one pool slot.
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
createSimpleEnvWithPool _proxy = createEnvWithPool simpleDriver

-- | Give the env a dedicated LISTEN connection that takes no pool slot.
useDedicatedListener :: (MonadIO m) => ByteString -> SimpleEnv registry -> m (SimpleEnv registry)
useDedicatedListener connStr = Backend.useDedicatedListener (withLibPQListenConn connStr)

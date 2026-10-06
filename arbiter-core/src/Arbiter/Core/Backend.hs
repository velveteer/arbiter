{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE OverloadedStrings #-}

-- | The env, monad, pool state, and savepoint ladder shared by the pooled backends.
--
-- This is the API for backend authors.
module Arbiter.Core.Backend
  ( -- * Database monad
    Db (..)
  , Env (..)
  , Driver (..)
  , runDb
  , inTransaction

    -- * Environment creation
  , createEnvWithConfig
  , createEnvWithPool
  , destroyEnv
  , disableListener
  , useDedicatedListener
  , poolListener

    -- * Pool state
  , PoolState (..)
  , HasPoolState (..)
  , withConn
  , pinConnection
  , withSavepointTransaction
  ) where

import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Reader (MonadReader, ReaderT (..), asks, local, runReaderT)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.Foldable (traverse_)
import Data.Pool (Pool, defaultPoolConfig, destroyAllResources, newPool, setNumStripes, withResource)
import Data.Text (Text)
import UnliftIO (MonadUnliftIO, mask, onException, withRunInIO)

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Listen (ListenConn, Listener, newListener)
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)

-- | Pool and the pinned connection with its transaction depth.
data PoolState conn = PoolState
  { connectionPool :: Maybe (Pool conn)
  -- ^ The pool to check connections out of. 'Nothing' for a pinned-only env.
  , pinned :: Maybe (conn, Int)
  -- ^ The pinned connection and its transaction depth. Depth 0 means no open transaction.
  }

-- | Ambient access to the pool state.
class (Monad m) => HasPoolState conn m | m -> conn where
  -- | The current pool state.
  getPoolState :: m (PoolState conn)

  -- | Run an action under an adjusted pool state.
  localPoolState :: (PoolState conn -> PoolState conn) -> m a -> m a

-- | What a connection type contributes to its env.
data Driver conn cfg = Driver
  { withListenConn :: conn -> (ListenConn -> IO ()) -> IO ()
  -- ^ Run the listener loop on a connection's driver handle.
  , initialConfig :: cfg
  -- ^ The driver state a new env starts with.
  }

-- | Schema name, pool state, listener, and the driver's own state.
data Env conn cfg (registry :: JobPayloadRegistry) = Env
  { schema :: SchemaName
  -- ^ The schema the arbiter tables live in.
  , poolState :: PoolState conn
  -- ^ The pool and the pinned connection.
  , listener :: Maybe Listener
  -- ^ Resolved LISTEN source. 'Nothing' runs poll-only.
  , driverConfig :: cfg
  -- ^ The driver's own state.
  }

-- | A pooled backend's database monad.
newtype Db conn cfg (registry :: JobPayloadRegistry) m a = Db
  { unDb :: ReaderT (Env conn cfg registry) m a
  -- ^ The action as a reader over its env.
  }
  deriving newtype
    ( Applicative
    , Functor
    , Monad
    , MonadCatch
    , MonadFail
    , MonadIO
    , MonadMask
    , MonadReader (Env conn cfg registry)
    , MonadThrow
    , MonadUnliftIO
    )

instance (Monad m) => HasPoolState conn (Db conn cfg registry m) where
  getPoolState = asks poolState
  localPoolState adjust = local (\env -> env {poolState = adjust (poolState env)})

-- | Run a 'Db' action in its env.
runDb :: Env conn cfg registry -> Db conn cfg registry m a -> m a
runDb env action = runReaderT (unDb action) env

-- | Run a 'Db' action on one connection pinned as the caller's open transaction.
inTransaction :: Driver conn cfg -> conn -> SchemaName -> Db conn cfg registry m a -> m a
inTransaction drv conn schemaName =
  runDb
    Env
      { schema = schemaName
      , poolState = PoolState {connectionPool = Nothing, pinned = Just (conn, 1)}
      , listener = Nothing
      , driverConfig = initialConfig drv
      }

-- | Close the idle connections in the env's pool. Connections in use stay open and go
-- back to the pool. The pool stays usable.
destroyEnv :: (MonadIO m) => Env conn cfg registry -> m ()
destroyEnv env = liftIO $ traverse_ destroyAllResources (connectionPool (poolState env))

-- | Turn off the shared LISTEN listener for an env, running poll-only.
disableListener :: Env conn cfg registry -> Env conn cfg registry
disableListener env = env {listener = Nothing}

-- | Give the env a LISTEN connection of its own from a connection runner.
useDedicatedListener
  :: (MonadIO m) => ((ListenConn -> IO ()) -> IO ()) -> Env conn cfg registry -> m (Env conn cfg registry)
useDedicatedListener withDedicated env = liftIO $ do
  lstn <- newListener withDedicated
  pure env {listener = Just lstn}

-- | A listener that borrows one pool connection for the hub's lifetime.
poolListener :: Driver conn cfg -> Pool conn -> IO Listener
poolListener drv pool = newListener (\action -> withResource pool (\conn -> withListenConn drv conn action))

-- | Create an env over a new pool opened with the connect and release actions. While
-- anything listens, the listener holds one pool slot.
createEnvWithConfig
  :: (MonadIO m)
  => Driver conn cfg
  -> IO conn
  -> (conn -> IO ())
  -> SchemaName
  -> PoolConfig
  -> m (Env conn cfg registry)
createEnvWithConfig drv connect release schemaName config = liftIO $ do
  connPool <-
    newPool
      $ setNumStripes (poolStripes config)
      $ defaultPoolConfig connect release (fromIntegral $ poolIdleTimeout config) (poolSize config)
  createEnvWithPool drv connPool schemaName

-- | Create an env over a caller's own connection pool. While anything listens, the
-- listener holds one pool slot.
createEnvWithPool :: (MonadIO m) => Driver conn cfg -> Pool conn -> SchemaName -> m (Env conn cfg registry)
createEnvWithPool drv connPool schemaName = liftIO $ do
  lstn <- poolListener drv connPool
  pure
    Env
      { schema = schemaName
      , poolState = PoolState {connectionPool = Just connPool, pinned = Nothing}
      , listener = Just lstn
      , driverConfig = initialConfig drv
      }

-- | The pinned connection, or one checked out of the pool.
withConn :: (HasPoolState conn m, MonadUnliftIO m) => (conn -> m a) -> m a
withConn action = do
  pool <- getPoolState
  case (fst <$> pinned pool, connectionPool pool) of
    (Just conn, _) -> action conn
    (Nothing, Just connPool) -> withRunInIO $ \run -> withResource connPool (run . action)
    (Nothing, Nothing) -> throwInternal noConnection

-- | Pin one pooled connection for the action.
pinConnection :: (HasPoolState conn m, MonadUnliftIO m) => m a -> m a
pinConnection action = withConn $ \conn -> localPoolState (\st -> st {pinned = Just (conn, maybe 0 snd (pinned st))}) action

-- | Transaction bracket over the backend's own statement runner and bracket. Nests via
-- savepoints.
withSavepointTransaction
  :: (HasPoolState conn m, MonadUnliftIO m)
  => (conn -> ByteString -> IO ())
  -> (conn -> IO a -> IO a)
  -> m a
  -> m a
withSavepointTransaction runSql transaction action = do
  st <- getPoolState
  case pinned st of
    Just (conn, depth) | depth > 0 -> mask $ \restore -> do
      let spName = "arbiter_sp_" <> BSC.pack (show depth)
      liftIO $ runSql conn ("SAVEPOINT " <> spName)
      result <-
        restore (localPoolState (\s -> s {pinned = Just (conn, depth + 1)}) action)
          `onException` liftIO (runSql conn ("ROLLBACK TO SAVEPOINT " <> spName))
      liftIO $ runSql conn ("RELEASE SAVEPOINT " <> spName)
      pure result
    _ -> withConn $ \conn -> withRunInIO $ \run ->
      transaction conn
        $ run
        $ localPoolState (\s -> s {pinned = Just (conn, 1)}) action

noConnection :: Text
noConnection = "No active connection and no connection pool available"

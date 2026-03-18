{-# LANGUAGE OverloadedStrings #-}

-- | @hasql@ implementation helpers for 'MonadArbiter'.
--
-- Handlers receive a @Hasql.Connection.Connection@ for running typed hasql
-- queries inside the worker transaction:
--
-- @
-- import Arbiter.Hasql.MonadArbiter
-- import Hasql.Connection qualified as Hasql
--
-- instance MonadArbiter MyApp where
--   type Handler MyApp jobs result = Hasql.Connection -> jobs -> MyApp result
--   executeQuery             = hasqlExecuteQuery
--   executeStatement         = hasqlExecuteStatement
--   withDbTransaction        = hasqlWithDbTransaction
--   runHandlerWithConnection = hasqlRunHandlerWithConnection
-- @
module Arbiter.Hasql.MonadArbiter
  ( -- * MonadArbiter implementation
    hasqlExecuteQuery
  , hasqlExecuteStatement
  , hasqlWithDbTransaction
  , hasqlRunHandlerWithConnection

    -- * Connection pool management
  , HasqlConnectionPool (..)
  , HasHasqlPool (..)
  , localHasqlConnection
  ) where

import Arbiter.Core.Codec (RowCodec)
import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.MonadArbiter (Params)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString.Char8 qualified as BSC
import Data.Int (Int64)
import Data.Pool qualified as Pool
import Data.Text (Text)
import Data.Text qualified as T
import Hasql.Connection qualified as Hasql
import Hasql.Session qualified as Session
import Hasql.Statement qualified as S
import UnliftIO (MonadUnliftIO, mask, onException, throwIO, withRunInIO)
import UnliftIO.Exception (SomeException, try)

import Arbiter.Hasql.Compat qualified as Compat
import Arbiter.Hasql.Decode qualified as Decode
import Arbiter.Hasql.Encode qualified as Encode

-- | Connection pool state for hasql connections.
--
-- Mirrors @SimpleConnectionPool@ from @arbiter-simple@.
data HasqlConnectionPool = HasqlConnectionPool
  { connectionPool :: Maybe (Pool.Pool Hasql.Connection)
  -- ^ The underlying resource pool. 'Nothing' when using connection-only mode
  -- via 'inTransaction'.
  , activeConn :: Maybe Hasql.Connection
  -- ^ Pinned connection when inside a transaction
  , transactionDepth :: Int
  -- ^ Current nesting depth (0 = no active transaction)
  }

-- | Typeclass for monads that carry a hasql connection pool.
class (Monad m) => HasHasqlPool m where
  getHasqlPool :: m HasqlConnectionPool
  localHasqlPool :: (HasqlConnectionPool -> HasqlConnectionPool) -> m a -> m a

-- | Pin a hasql connection for transactional work.
--
-- All arbiter operations within the callback will use this connection.
-- The caller must have already issued @BEGIN@ on the connection.
localHasqlConnection :: (HasHasqlPool m) => Hasql.Connection -> m a -> m a
localHasqlConnection conn = localHasqlPool (\pool -> pool {activeConn = Just conn, transactionDepth = 1})

hasqlExecuteQuery
  :: (HasHasqlPool m, MonadIO m)
  => Text
  -> Params
  -> RowCodec a
  -> m [a]
hasqlExecuteQuery sql params codec = withConn $ \conn -> liftIO $ do
  let stmt = S.preparable (Encode.convertPlaceholders sql) (Encode.buildEncoder params) (Decode.hasqlRowDecoder codec)
  result <- Hasql.use conn (Session.statement () stmt)
  case result of
    Right rows -> pure rows
    Left err -> throwInternal $ "hasql query error: " <> T.pack (show err)

hasqlExecuteStatement
  :: (HasHasqlPool m, MonadIO m)
  => Text
  -> Params
  -> m Int64
hasqlExecuteStatement sql params = withConn $ \conn -> liftIO $ do
  let stmt = Encode.buildStatementRowCount sql params
  result <- Hasql.use conn (Session.statement () stmt)
  case result of
    Right n -> pure n
    Left err -> throwInternal $ "hasql statement error: " <> T.pack (show err)

-- | Run a block of code within a database transaction.
--
-- Supports nested transactions via savepoints, matching @arbiter-simple@.
hasqlWithDbTransaction :: (HasHasqlPool m, MonadUnliftIO m) => m a -> m a
hasqlWithDbTransaction action = do
  pool <- getHasqlPool
  let depth = transactionDepth pool
  case (activeConn pool, depth) of
    (Nothing, _) -> case connectionPool pool of
      Nothing -> throwInternal "No active connection and no connection pool available"
      Just p -> withRunInIO $ \run ->
        Pool.withResource p $ \conn ->
          beginCommitOrRollback conn $
            run (localHasqlPool (\hpool -> hpool {activeConn = Just conn, transactionDepth = 1}) action)
    (Just conn, 0) -> withRunInIO $ \run ->
      beginCommitOrRollback conn $
        run (localHasqlPool (\p -> p {transactionDepth = 1}) action)
    (Just conn, d) -> mask $ \restore -> do
      let spName = "arbiter_sp_" <> BSC.pack (show d)
      liftIO $ Compat.runSQL conn ("SAVEPOINT " <> spName)
      a <-
        restore (localHasqlPool (\p -> p {transactionDepth = d + 1}) action)
          `onException` liftIO (Compat.runSQL conn ("ROLLBACK TO SAVEPOINT " <> spName))
      liftIO $ Compat.runSQL conn ("RELEASE SAVEPOINT " <> spName)
      pure a

beginCommitOrRollback :: forall a. Hasql.Connection -> IO a -> IO a
beginCommitOrRollback conn action = do
  Compat.runSQL conn "BEGIN"
  eitherResult <- try action :: IO (Either SomeException a)
  case eitherResult of
    Right result -> do
      Compat.runSQL conn "COMMIT"
      pure result
    Left exc -> do
      _ <- try (Compat.runSQL conn "ROLLBACK") :: IO (Either SomeException ())
      throwIO exc

-- | Invoke a handler by passing the active hasql connection.
--
-- The handler receives a @Hasql.Connection@ so it can run typed hasql
-- queries within the worker transaction.
hasqlRunHandlerWithConnection
  :: (HasHasqlPool m, MonadIO m)
  => (Hasql.Connection -> jobs -> m result)
  -> jobs
  -> m result
hasqlRunHandlerWithConnection handler jobs = do
  pool <- getHasqlPool
  case activeConn pool of
    Just conn -> handler conn jobs
    Nothing -> throwInternal "hasqlRunHandlerWithConnection: no active connection"

-- ---------------------------------------------------------------------------
-- Internal
-- ---------------------------------------------------------------------------

-- | Get a connection from the pool state or check one out.
withConn :: (HasHasqlPool m, MonadIO m) => (Hasql.Connection -> IO a) -> m a
withConn f = do
  pool <- getHasqlPool
  case (activeConn pool, connectionPool pool) of
    (Just conn, _) -> liftIO $ f conn
    (Nothing, Just p) -> liftIO $ Pool.withResource p f
    (Nothing, Nothing) -> throwInternal "No active connection and no connection pool available"

{-# LANGUAGE OverloadedStrings #-}

-- | @hasql@ implementation helpers for 'Arbiter.Core.MonadArbiter.MonadArbiter'.
--
-- Handlers receive a @Hasql.Connection.Connection@ for running typed hasql
-- queries inside the worker transaction:
--
-- @
-- import Arbiter.Hasql.MonadArbiter
-- import Hasql.Connection qualified as Hasql
--
-- instance MonadArbiter MyApp where
--   type RegistryOf MyApp = MyRegistry
--   type Handler MyApp job result = Hasql.Connection -> job -> MyApp result
--   getSchema                = asks appSchema
--   executeQuery             = hasqlExecuteQuery
--   executeStatement         = hasqlExecuteStatement
--   withDbTransaction        = hasqlWithDbTransaction
--   runHandlerWithConnection = hasqlRunHandlerWithConnection
--   getListener              = asks appListener
-- @
--
-- Write a handler's own signature as @JobHandler MyApp MyPayload MyResult@, which is
-- 'Arbiter.Core.MonadArbiter.Handler' at that queue's job and declared result types.
module Arbiter.Hasql.MonadArbiter
  ( -- * MonadArbiter implementation
    hasqlExecuteQuery
  , hasqlExecuteQueryPrepared
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
import Arbiter.Core.MonadArbiter (Params, Query (..))
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString.Char8 qualified as BSC
import Data.Int (Int64)
import Data.Pool qualified as Pool
import Data.Text (Text)
import Data.Text qualified as T
import Hasql.Connection qualified as Hasql
import Hasql.Session qualified as Session
import Hasql.Statement qualified as S
import UnliftIO (MonadUnliftIO, mask, onException, withRunInIO)
import UnliftIO.Exception (SomeException, try)

import Arbiter.Hasql.Compat qualified as Compat
import Arbiter.Hasql.Decode qualified as Decode
import Arbiter.Hasql.Encode qualified as Encode

-- | Connection pool state for hasql connections.
data HasqlConnectionPool = HasqlConnectionPool
  { connectionPool :: Maybe (Pool.Pool Hasql.Connection)
  -- ^ The underlying resource pool. 'Nothing' when using connection-only mode
  -- via 'Arbiter.Hasql.HasqlDb.inTransaction'.
  , activeConn :: Maybe Hasql.Connection
  -- ^ Pinned connection when inside a transaction
  , transactionDepth :: Int
  -- ^ Current nesting depth (0 = no active transaction)
  , preparedStatements :: Bool
  -- ^ Whether 'hasqlExecuteQueryPrepared' prepares server-side. Off by default.
  }

-- | Typeclass for monads that carry a hasql connection pool.
class (Monad m) => HasHasqlPool m where
  getHasqlPool :: m HasqlConnectionPool
  localHasqlPool :: (HasqlConnectionPool -> HasqlConnectionPool) -> m a -> m a

-- | Pin a hasql connection for the callback. Every arbiter operation inside it runs on
-- that connection. The caller has already issued its @BEGIN@.
localHasqlConnection :: (HasHasqlPool m) => Hasql.Connection -> m a -> m a
localHasqlConnection conn = localHasqlPool (\pool -> pool {activeConn = Just conn, transactionDepth = 1})

-- | Run a query unprepared, decoding rows.
hasqlExecuteQuery
  :: (HasHasqlPool m, MonadIO m)
  => Query a
  -> m [a]
hasqlExecuteQuery query = withConn $ \conn ->
  runQueryStatement False conn (qPositional query) (qParams query) (qDecode query)

-- | 'hasqlExecuteQuery' that prepares the statement once per connection and reuses
-- the plan, when the pool enables prepared statements.
hasqlExecuteQueryPrepared
  :: (HasHasqlPool m, MonadIO m)
  => Query a
  -> m [a]
hasqlExecuteQueryPrepared query = do
  pool <- getHasqlPool
  withConn $ \conn -> runQueryStatement (preparedStatements pool) conn (qPositional query) (qParams query) (qDecode query)

runQueryStatement :: Bool -> Hasql.Connection -> Text -> Params -> RowCodec a -> IO [a]
runQueryStatement prepare conn positional params codec = do
  let mkStatement = if prepare then S.preparable else S.unpreparable
      stmt = mkStatement positional (Encode.buildEncoder params) (Decode.hasqlRowDecoder codec)
  result <- Hasql.use conn (Session.statement () stmt)
  case result of
    Right rows -> pure rows
    Left err -> throwInternal $ "hasql query error: " <> T.pack (show err)

-- | Run a statement unprepared, returning rows affected.
hasqlExecuteStatement
  :: (HasHasqlPool m, MonadIO m)
  => Query a
  -> m Int64
hasqlExecuteStatement query = withConn $ \conn -> liftIO $ do
  let stmt = Encode.buildStatementRowCount (qPositional query) (qParams query)
  result <- Hasql.use conn (Session.statement () stmt)
  case result of
    Right rowCount -> pure rowCount
    Left err -> throwInternal $ "hasql statement error: " <> T.pack (show err)

-- | Transaction bracket. Nests via savepoints.
hasqlWithDbTransaction :: (HasHasqlPool m, MonadUnliftIO m) => m a -> m a
hasqlWithDbTransaction action = do
  pool <- getHasqlPool
  let depth = transactionDepth pool
  case (activeConn pool, depth) of
    (Nothing, _) -> case connectionPool pool of
      Nothing -> throwInternal "No active connection and no connection pool available"
      Just connPool -> withRunInIO $ \run ->
        Pool.withResource connPool $ \conn ->
          beginCommitOrRollback conn $
            run (localHasqlPool (\hpool -> hpool {activeConn = Just conn, transactionDepth = 1}) action)
    (Just conn, 0) -> withRunInIO $ \run ->
      beginCommitOrRollback conn $
        run (localHasqlPool (\hpool -> hpool {transactionDepth = 1}) action)
    (Just conn, nestedDepth) -> mask $ \restore -> do
      let spName = "arbiter_sp_" <> BSC.pack (show nestedDepth)
      liftIO $ Compat.runSQL conn ("SAVEPOINT " <> spName)
      result <-
        restore (localHasqlPool (\hpool -> hpool {transactionDepth = nestedDepth + 1}) action)
          `onException` liftIO (Compat.runSQL conn ("ROLLBACK TO SAVEPOINT " <> spName))
      liftIO $ Compat.runSQL conn ("RELEASE SAVEPOINT " <> spName)
      pure result

beginCommitOrRollback :: forall a. Hasql.Connection -> IO a -> IO a
beginCommitOrRollback conn action = mask $ \restore -> do
  Compat.runSQL conn "BEGIN"
  result <- restore action `onException` rollbackSafely
  Compat.runSQL conn "COMMIT"
  pure result
  where
    rollbackSafely :: IO ()
    rollbackSafely = do
      inTx <- Compat.connectionInTransaction conn
      when inTx $ do
        _ <- try (Compat.runSQL conn "ROLLBACK") :: IO (Either SomeException ())
        pure ()

-- | Run a handler on the active connection. The handler can issue typed hasql queries
-- inside the worker transaction.
hasqlRunHandlerWithConnection
  :: (HasHasqlPool m, MonadIO m)
  => (Hasql.Connection -> job -> m result)
  -> job
  -> m result
hasqlRunHandlerWithConnection handler job = do
  pool <- getHasqlPool
  case activeConn pool of
    Just conn -> handler conn job
    Nothing -> throwInternal "hasqlRunHandlerWithConnection: no active connection"

-- ---------------------------------------------------------------------------
-- Internal
-- ---------------------------------------------------------------------------

-- | The pinned connection, or one checked out of the pool.
withConn :: (HasHasqlPool m, MonadIO m) => (Hasql.Connection -> IO a) -> m a
withConn action = do
  pool <- getHasqlPool
  case (activeConn pool, connectionPool pool) of
    (Just conn, _) -> liftIO $ action conn
    (Nothing, Just connPool) -> liftIO $ Pool.withResource connPool action
    (Nothing, Nothing) -> throwInternal "No active connection and no connection pool available"

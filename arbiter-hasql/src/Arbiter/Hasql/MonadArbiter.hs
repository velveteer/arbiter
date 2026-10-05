{-# LANGUAGE OverloadedStrings #-}

-- | hasql primitives for writing a custom 'Arbiter.Core.MonadArbiter.MonadArbiter' instance.
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
--   executeQueryPrepared     = hasqlExecuteQueryPrepared True
--   executeStatement         = hasqlExecuteStatement
--   withDbTransaction        = hasqlWithDbTransaction
--   runHandlerWithConnection = hasqlRunHandlerWithConnection
--   getListener              = asks appListener
-- @
--
-- The primitives also need a @HasPoolState Hasql.Connection MyApp@ instance. See
-- 'Arbiter.Hasql.HasqlDb.HasPoolState'.
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

    -- * Connection
  , hasqlWithConnection
  ) where

import Arbiter.Core.Backend (HasPoolState (..), PoolState (..), pinConnection, withConn, withSavepointTransaction)
import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.MonadArbiter (Query (..))
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Int (Int64)
import Data.Text qualified as T
import Hasql.Connection qualified as Hasql
import Hasql.Session qualified as Session
import Hasql.Statement qualified as S
import UnliftIO (MonadUnliftIO, mask, onException)
import UnliftIO.Exception (SomeException, try)

import Arbiter.Hasql.Compat (connectionInTransaction, runSQL)
import Arbiter.Hasql.Decode qualified as Decode
import Arbiter.Hasql.Encode qualified as Encode

-- | Pin one pooled connection for the action.
hasqlWithConnection :: (HasPoolState Hasql.Connection m, MonadUnliftIO m) => m a -> m a
hasqlWithConnection = pinConnection

-- | Run a query unprepared, decoding rows.
hasqlExecuteQuery
  :: (HasPoolState Hasql.Connection m, MonadUnliftIO m)
  => Query a
  -> m [a]
hasqlExecuteQuery query = withConn $ \conn -> liftIO (runQueryStatement False conn query)

-- | 'hasqlExecuteQuery', prepared once per connection when the flag is on.
hasqlExecuteQueryPrepared
  :: (HasPoolState Hasql.Connection m, MonadUnliftIO m)
  => Bool
  -> Query a
  -> m [a]
hasqlExecuteQueryPrepared prepare query = withConn $ \conn -> liftIO (runQueryStatement prepare conn query)

runQueryStatement :: Bool -> Hasql.Connection -> Query a -> IO [a]
runQueryStatement prepare conn query = do
  let mkStatement = if prepare then S.preparable else S.unpreparable
      stmt = mkStatement (qPositional query) (Encode.buildEncoder (qParams query)) (Decode.hasqlRowDecoder (qDecode query))
  result <- Hasql.use conn (Session.statement () stmt)
  case result of
    Right rows -> pure rows
    Left err -> throwInternal $ "hasql query error: " <> T.pack (show err)

-- | Run a statement unprepared, returning rows affected.
hasqlExecuteStatement
  :: (HasPoolState Hasql.Connection m, MonadUnliftIO m)
  => Query a
  -> m Int64
hasqlExecuteStatement query = withConn $ \conn -> liftIO $ do
  let stmt = Encode.buildStatementRowCount (qPositional query) (qParams query)
  result <- Hasql.use conn (Session.statement () stmt)
  case result of
    Right rowCount -> pure rowCount
    Left err -> throwInternal $ "hasql statement error: " <> T.pack (show err)

-- | Transaction bracket. Nests via savepoints.
hasqlWithDbTransaction :: (HasPoolState Hasql.Connection m, MonadUnliftIO m) => m a -> m a
hasqlWithDbTransaction = withSavepointTransaction runSQL beginCommitOrRollback

beginCommitOrRollback :: forall a. Hasql.Connection -> IO a -> IO a
beginCommitOrRollback conn action = mask $ \restore -> do
  runSQL conn "BEGIN"
  result <- restore action `onException` rollbackSafely
  runSQL conn "COMMIT"
  pure result
  where
    rollbackSafely :: IO ()
    rollbackSafely = do
      inTx <- connectionInTransaction conn
      when inTx $ do
        _ <- try (runSQL conn "ROLLBACK") :: IO (Either SomeException ())
        pure ()

-- | Run a handler on the pinned connection. Throws when no connection is pinned.
hasqlRunHandlerWithConnection
  :: (HasPoolState Hasql.Connection m, MonadIO m)
  => (Hasql.Connection -> job -> m result)
  -> job
  -> m result
hasqlRunHandlerWithConnection handler job = do
  pool <- getPoolState
  case fst <$> pinned pool of
    Just conn -> handler conn job
    Nothing -> throwInternal "hasqlRunHandlerWithConnection: no active connection"

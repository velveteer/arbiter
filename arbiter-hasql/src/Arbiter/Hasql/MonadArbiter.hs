{-# LANGUAGE OverloadedStrings #-}

-- | hasql primitives for writing a custom 'Arbiter.Core.MonadArbiter.MonadArbiter' instance.
--
-- Handlers receive a @Hasql.Connection.Connection@ for running typed hasql
-- queries inside the worker transaction:
--
-- @
-- import Arbiter.Core qualified as Arb
-- import Arbiter.Hasql qualified as ArbH
-- import Hasql.Connection qualified as Hasql
--
-- instance Arb.MonadArbiter MyApp where
--   type RegistryOf MyApp = MyRegistry
--   type Handler MyApp job result = Hasql.Connection -> job -> MyApp result
--   getSchema                = asks appSchema
--   executeQuery             = ArbH.hasqlExecuteQuery
--   executeQueryPrepared     = ArbH.hasqlExecuteQueryPrepared True
--   executeStatement         = ArbH.hasqlExecuteStatement
--   withDbTransaction        = ArbH.hasqlWithDbTransaction
--   runHandlerWithConnection = ArbH.hasqlRunHandlerWithConnection
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

import Arbiter.Core.Backend (HasPoolState, pinConnection, withConn, withSavepointTransaction)
import Arbiter.Core.Enum (showText)
import Arbiter.Core.Exceptions (orThrowInternal)
import Arbiter.Core.MonadArbiter (Query (..))
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Int (Int64)
import Data.Text (Text)
import Hasql.Connection qualified as Hasql
import Hasql.Decoders qualified as D
import Hasql.Session qualified as Session
import Hasql.Statement qualified as S
import UnliftIO (MonadUnliftIO, mask, onException, tryAny)

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
hasqlExecuteQuery = hasqlExecuteQueryPrepared False

-- | 'hasqlExecuteQuery', prepared once per connection when the flag is on.
hasqlExecuteQueryPrepared
  :: (HasPoolState Hasql.Connection m, MonadUnliftIO m)
  => Bool
  -> Query a
  -> m [a]
hasqlExecuteQueryPrepared prepare query = withConn $ \conn ->
  liftIO . runStatement "query" conn $
    mkStatement (qPositional query) (Encode.buildEncoder (qParams query)) (Decode.hasqlRowDecoder (qDecode query))
  where
    mkStatement = if prepare then S.preparable else S.unpreparable

-- | Run a statement unprepared, returning rows affected.
hasqlExecuteStatement
  :: (HasPoolState Hasql.Connection m, MonadUnliftIO m)
  => Query a
  -> m Int64
hasqlExecuteStatement query = withConn $ \conn ->
  liftIO . runStatement "statement" conn $
    S.unpreparable (qPositional query) (Encode.buildEncoder (qParams query)) D.rowsAffected

runStatement :: Text -> Hasql.Connection -> S.Statement () r -> IO r
runStatement label conn stmt =
  Hasql.use conn (Session.statement () stmt)
    >>= orThrowInternal (\err -> "hasql " <> label <> " error: " <> showText err)

-- | Transaction bracket. Nests via savepoints.
hasqlWithDbTransaction :: (HasPoolState Hasql.Connection m, MonadUnliftIO m) => m a -> m a
hasqlWithDbTransaction = withSavepointTransaction runSQL beginCommitOrRollback

beginCommitOrRollback :: Hasql.Connection -> IO a -> IO a
beginCommitOrRollback conn action = mask $ \restore -> do
  runSQL conn "BEGIN"
  result <- restore action `onException` rollbackSafely
  runSQL conn "COMMIT"
  pure result
  where
    rollbackSafely :: IO ()
    rollbackSafely = do
      inTx <- connectionInTransaction conn
      when inTx $ void (tryAny (runSQL conn "ROLLBACK"))

-- | Run a handler on the pinned connection, or on a borrowed pool connection when none
-- is pinned.
hasqlRunHandlerWithConnection
  :: (HasPoolState Hasql.Connection m, MonadUnliftIO m)
  => (Hasql.Connection -> job -> m result)
  -> job
  -> m result
hasqlRunHandlerWithConnection handler job =
  withConn $ \conn -> handler conn job

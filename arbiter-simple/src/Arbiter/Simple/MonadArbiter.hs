{-# LANGUAGE OverloadedStrings #-}

-- | 'Arbiter.Core.MonadArbiter.MonadArbiter' primitives backed by postgresql-simple.
module Arbiter.Simple.MonadArbiter
  ( HasSimplePool (..)
  , SimpleConnectionPool (..)

    -- * MonadArbiter implementation
  , simpleExecuteQuery
  , simpleExecuteStatement
  , simpleWithDbTransaction
  , simpleWithConnection
  , simpleRunHandlerWithConnection
  ) where

import Arbiter.Core.Codec (Col (..), NullCol (..), runCodec)
import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.MonadArbiter hiding (Query (..))
import Arbiter.Core.MonadArbiter qualified as MA
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString.Char8 qualified as BSC
import Data.Int (Int64)
import Data.Pool (Pool, withResource)
import Data.Text.Encoding qualified as T
import Database.PostgreSQL.Simple (Connection)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.FromRow (RowParser, field)
import Database.PostgreSQL.Simple.ToField (Action, ToField (..), toField, toJSONField)
import Database.PostgreSQL.Simple.Types (PGArray (..), Query (..))
import UnliftIO (MonadUnliftIO, mask, onException, withRunInIO)

-- | Pool, pinned connection, and savepoint depth.
data SimpleConnectionPool = SimpleConnectionPool
  { connectionPool :: Maybe (Pool Connection)
  , activeConn :: Maybe Connection
  , transactionDepth :: Int
  }

-- | Ambient access to the connection pool.
class (Monad m) => HasSimplePool m where
  getSimplePool :: m SimpleConnectionPool
  localSimplePool :: (SimpleConnectionPool -> SimpleConnectionPool) -> m a -> m a

-- | Run a query, decoding rows.
simpleExecuteQuery
  :: (HasSimplePool m, MonadUnliftIO m)
  => MA.Query a
  -> m [a]
simpleExecuteQuery (MA.Query sqlTemplate params codec) = do
  let sql = Query $ T.encodeUtf8 sqlTemplate
      parser = runCodec interpretNullCol codec
  withConn $ \conn -> liftIO $ case params of
    [] -> PG.queryWith_ parser conn sql
    _ -> PG.queryWith parser conn sql (map someParamToAction params)

-- | Run a statement, returning rows affected.
simpleExecuteStatement
  :: (HasSimplePool m, MonadUnliftIO m)
  => MA.Query a
  -> m Int64
simpleExecuteStatement (MA.Query sqlTemplate params _) = do
  let sql = Query $ T.encodeUtf8 sqlTemplate
  withConn $ \conn -> liftIO $ case params of
    [] -> PG.execute_ conn sql
    _ -> PG.execute conn sql (map someParamToAction params)

interpretNullCol :: NullCol a -> RowParser a
interpretNullCol (NotNull _ col) = colField col
interpretNullCol (Nullable _ col) = colFieldNullable col

colField :: Col a -> RowParser a
colField CInt4 = field
colField CInt8 = field
colField CText = field
colField CBool = field
colField CTimestamptz = field
colField CJsonb = field
colField CFloat8 = field
colField CUuid = field

colFieldNullable :: Col a -> RowParser (Maybe a)
colFieldNullable CInt4 = field
colFieldNullable CInt8 = field
colFieldNullable CText = field
colFieldNullable CBool = field
colFieldNullable CTimestamptz = field
colFieldNullable CJsonb = field
colFieldNullable CFloat8 = field
colFieldNullable CUuid = field

-- | Transaction bracket. Nests via savepoints.
simpleWithDbTransaction
  :: (HasSimplePool m, MonadUnliftIO m)
  => m a
  -> m a
simpleWithDbTransaction action = do
  pool <- getSimplePool
  let depth = transactionDepth pool
  case (activeConn pool, depth) of
    (Nothing, _) -> case connectionPool pool of
      Nothing -> throwInternal "No active connection and no connection pool available"
      Just connPool -> withRunInIO $ \run ->
        withResource connPool $ \conn ->
          PG.withTransaction conn
            $ run
            $ localSimplePool (\spool -> spool {activeConn = Just conn, transactionDepth = 1}) action
    (Just conn, 0) -> withRunInIO $ \run ->
      PG.withTransaction conn
        $ run
        $ localSimplePool (\spool -> spool {transactionDepth = 1}) action
    (Just conn, _) -> mask $ \restore -> do
      let spName = Query $ "arbiter_sp_" <> BSC.pack (show depth)
      void . liftIO $ PG.execute_ conn $ "SAVEPOINT " <> spName
      result <-
        restore (localSimplePool (\spool -> spool {transactionDepth = depth + 1}) action)
          `onException` liftIO (PG.execute_ conn $ "ROLLBACK TO SAVEPOINT " <> spName)
      void . liftIO $ PG.execute_ conn $ "RELEASE SAVEPOINT " <> spName
      pure result

-- | Pin one pooled connection for the action.
simpleWithConnection
  :: (HasSimplePool m, MonadUnliftIO m)
  => m a
  -> m a
simpleWithConnection action = do
  pool <- getSimplePool
  case (activeConn pool, connectionPool pool) of
    (Just _, _) -> action
    (Nothing, Just connPool) -> withRunInIO $ \run ->
      withResource connPool $ \conn ->
        run $ localSimplePool (\spool -> spool {activeConn = Just conn}) action
    (Nothing, Nothing) -> throwInternal "No active connection and no connection pool available"

-- | Run a handler on the pinned connection.
simpleRunHandlerWithConnection
  :: (HasSimplePool m, MonadUnliftIO m)
  => (Connection -> job -> m result)
  -> job
  -> m result
simpleRunHandlerWithConnection handler job =
  withConn $ \conn -> handler conn job

withConn
  :: (HasSimplePool m, MonadUnliftIO m)
  => (Connection -> m a)
  -> m a
withConn action = do
  pool <- getSimplePool
  case (activeConn pool, connectionPool pool) of
    (Just conn, _) -> action conn
    (Nothing, Just connPool) -> withRunInIO $ \run ->
      withResource connPool $ \conn -> run $ action conn
    (Nothing, Nothing) -> throwInternal "No active connection and no connection pool available"

someParamToAction :: SomeParam -> Action
someParamToAction (SomeParam (PScalar CJsonb) value) = toJSONField value
someParamToAction (SomeParam (PScalar col) value) = withColToField col (toField value)
someParamToAction (SomeParam (PNullable CJsonb) value) = maybe (toField (Nothing :: Maybe Int)) toJSONField value
someParamToAction (SomeParam (PNullable col) value) = withColToField col (toField value)
someParamToAction (SomeParam (PArray col) value) = withColToField col (toField (PGArray value))
someParamToAction (SomeParam (PNullArray col) value) = withColToField col (toField (PGArray value))

withColToField :: Col a -> ((ToField a) => r) -> r
withColToField CInt4 continuation = continuation
withColToField CInt8 continuation = continuation
withColToField CText continuation = continuation
withColToField CBool continuation = continuation
withColToField CTimestamptz continuation = continuation
withColToField CJsonb continuation = continuation
withColToField CFloat8 continuation = continuation
withColToField CUuid continuation = continuation

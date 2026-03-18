{-# LANGUAGE OverloadedStrings #-}

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

import Arbiter.Core.Codec (Col (..), NullCol (..), RowCodec, runCodec)
import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.MonadArbiter
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString.Char8 qualified as BSC
import Data.Int (Int64)
import Data.Pool (Pool, withResource)
import Data.Text (Text)
import Data.Text.Encoding qualified as T
import Database.PostgreSQL.Simple (Connection)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.FromRow (RowParser, field)
import Database.PostgreSQL.Simple.ToField (Action, ToField (..), toField, toJSONField)
import Database.PostgreSQL.Simple.Types (PGArray (..), Query (..))
import UnliftIO (MonadUnliftIO, mask, onException, withRunInIO)

data SimpleConnectionPool = SimpleConnectionPool
  { connectionPool :: Maybe (Pool Connection)
  , activeConn :: Maybe Connection
  , transactionDepth :: Int
  }

class (Monad m) => HasSimplePool m where
  getSimplePool :: m SimpleConnectionPool
  localSimplePool :: (SimpleConnectionPool -> SimpleConnectionPool) -> m a -> m a

simpleExecuteQuery
  :: (HasSimplePool m, MonadUnliftIO m)
  => Text
  -> Params
  -> RowCodec a
  -> m [a]
simpleExecuteQuery sqlTemplate params codec = do
  let sql = Query $ T.encodeUtf8 sqlTemplate
      pgParams = map someParamToAction params
      parser = runCodec interpretNullCol codec
  withConn $ \conn ->
    liftIO $ PG.queryWith parser conn sql pgParams

simpleExecuteStatement
  :: (HasSimplePool m, MonadUnliftIO m)
  => Text
  -> Params
  -> m Int64
simpleExecuteStatement sqlTemplate params = do
  let sql = Query $ T.encodeUtf8 sqlTemplate
      pgParams = map someParamToAction params
  withConn $ \conn ->
    liftIO $ PG.execute conn sql pgParams

interpretNullCol :: NullCol a -> RowParser a
interpretNullCol (NotNull _ c) = colField c
interpretNullCol (Nullable _ c) = colFieldNullable c

colField :: Col a -> RowParser a
colField CInt4 = field
colField CInt8 = field
colField CText = field
colField CBool = field
colField CTimestamptz = field
colField CJsonb = field
colField CFloat8 = field

colFieldNullable :: Col a -> RowParser (Maybe a)
colFieldNullable CInt4 = field
colFieldNullable CInt8 = field
colFieldNullable CText = field
colFieldNullable CBool = field
colFieldNullable CTimestamptz = field
colFieldNullable CJsonb = field
colFieldNullable CFloat8 = field

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
      Just p -> withRunInIO $ \run ->
        withResource p $ \conn ->
          PG.withTransaction conn $
            run $
              localSimplePool (\sp -> sp {activeConn = Just conn, transactionDepth = 1}) action
    (Just conn, 0) -> withRunInIO $ \run ->
      PG.withTransaction conn $
        run $
          localSimplePool (\p -> p {transactionDepth = 1}) action
    (Just conn, _) -> mask $ \restore -> do
      let spName = Query $ "arbiter_sp_" <> BSC.pack (show depth)
      void . liftIO $ PG.execute_ conn $ "SAVEPOINT " <> spName
      a <-
        restore (localSimplePool (\p -> p {transactionDepth = depth + 1}) action)
          `onException` liftIO (PG.execute_ conn $ "ROLLBACK TO SAVEPOINT " <> spName)
      void . liftIO $ PG.execute_ conn $ "RELEASE SAVEPOINT " <> spName
      pure a

simpleWithConnection
  :: (HasSimplePool m, MonadUnliftIO m)
  => m a
  -> m a
simpleWithConnection action = do
  pool <- getSimplePool
  case (activeConn pool, connectionPool pool) of
    (Just _, _) -> action
    (Nothing, Just p) -> withRunInIO $ \run ->
      withResource p $ \conn ->
        run $ localSimplePool (\sp -> sp {activeConn = Just conn}) action
    (Nothing, Nothing) -> throwInternal "No active connection and no connection pool available"

simpleRunHandlerWithConnection
  :: (HasSimplePool m, MonadUnliftIO m)
  => (Connection -> jobs -> m result)
  -> jobs
  -> m result
simpleRunHandlerWithConnection handler jobs =
  withConn $ \conn -> handler conn jobs

withConn
  :: (HasSimplePool m, MonadUnliftIO m)
  => (Connection -> m a)
  -> m a
withConn f = do
  pool <- getSimplePool
  case (activeConn pool, connectionPool pool) of
    (Just conn, _) -> f conn
    (Nothing, Just p) -> withRunInIO $ \run ->
      withResource p $ \conn -> run $ f conn
    (Nothing, Nothing) -> throwInternal "No active connection and no connection pool available"

someParamToAction :: SomeParam -> Action
someParamToAction (SomeParam (PScalar CJsonb) v) = toJSONField v
someParamToAction (SomeParam (PScalar c) v) = withColToField c (toField v)
someParamToAction (SomeParam (PNullable CJsonb) v) = maybe (toField (Nothing :: Maybe Int)) toJSONField v
someParamToAction (SomeParam (PNullable c) v) = withColToField c (toField v)
someParamToAction (SomeParam (PArray c) v) = withColToField c (toField (PGArray v))
someParamToAction (SomeParam (PNullArray c) v) = withColToField c (toField (PGArray v))

withColToField :: Col a -> ((ToField a) => r) -> r
withColToField CInt4 r = r
withColToField CInt8 r = r
withColToField CText r = r
withColToField CBool r = r
withColToField CTimestamptz r = r
withColToField CJsonb r = r
withColToField CFloat8 r = r

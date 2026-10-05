-- | postgresql-simple primitives for writing a custom
-- 'Arbiter.Core.MonadArbiter.MonadArbiter' instance.
--
-- Handlers receive a @Database.PostgreSQL.Simple.Connection@ for running queries
-- inside the worker transaction:
--
-- @
-- import Arbiter.Simple.MonadArbiter
-- import Database.PostgreSQL.Simple (Connection)
--
-- instance MonadArbiter MyApp where
--   type RegistryOf MyApp = MyRegistry
--   type Handler MyApp job result = Connection -> job -> MyApp result
--   getSchema                = asks appSchema
--   executeQuery             = simpleExecuteQuery
--   executeStatement         = simpleExecuteStatement
--   withDbTransaction        = simpleWithDbTransaction
--   runHandlerWithConnection = simpleRunHandlerWithConnection
--   getListener              = asks appListener
-- @
--
-- The primitives also need a @HasPoolState Connection MyApp@ instance. See
-- 'Arbiter.Simple.SimpleDb.HasPoolState'.
--
-- Write a handler's own signature as @JobHandler MyApp MyPayload MyResult@, which is
-- 'Arbiter.Core.MonadArbiter.Handler' at that queue's job and declared result types.
module Arbiter.Simple.MonadArbiter
  ( -- * MonadArbiter implementation
    simpleExecuteQuery
  , simpleExecuteStatement
  , simpleWithDbTransaction
  , simpleRunHandlerWithConnection

    -- * Connection
  , simpleWithConnection
  ) where

import Arbiter.Core.Backend (HasPoolState, pinConnection, withConn, withSavepointTransaction)
import Arbiter.Core.Codec (Col (..), NullCol (..), runCodec)
import Arbiter.Core.Job.Types.Internal (Stored (..), storedBytes)
import Arbiter.Core.MonadArbiter hiding (Query (..))
import Arbiter.Core.MonadArbiter qualified as MA
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Text.Encoding qualified as T
import Database.PostgreSQL.Simple (Connection)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.FromField (FromField (..), ResultError (..), returnError, typeOid)
import Database.PostgreSQL.Simple.FromRow (RowParser, field)
import Database.PostgreSQL.Simple.ToField (Action (..), ToField (..), toField, toJSONField)
import Database.PostgreSQL.Simple.TypeInfo.Static qualified as TI
import Database.PostgreSQL.Simple.Types (PGArray (..), Query (..))
import UnliftIO (MonadUnliftIO)

-- | Pin one pooled connection for the action.
simpleWithConnection :: (HasPoolState Connection m, MonadUnliftIO m) => m a -> m a
simpleWithConnection = pinConnection

-- | Run a query, decoding rows.
simpleExecuteQuery
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => MA.Query a
  -> m [a]
simpleExecuteQuery query = do
  let sql = Query $ T.encodeUtf8 (MA.qSql query)
      params = MA.qParams query
      parser = runCodec interpretNullCol (MA.qDecode query)
  withConn $ \conn -> liftIO $ case params of
    [] -> PG.queryWith_ parser conn sql
    _ -> PG.queryWith parser conn sql (map someParamToAction params)

-- | Run a statement, returning rows affected.
simpleExecuteStatement
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => MA.Query a
  -> m Int64
simpleExecuteStatement query = do
  let sql = Query $ T.encodeUtf8 (MA.qSql query)
      params = MA.qParams query
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
colField CStored = storedJson <$> field
colField CFloat8 = field
colField CUuid = field

colFieldNullable :: Col a -> RowParser (Maybe a)
colFieldNullable CInt4 = field
colFieldNullable CInt8 = field
colFieldNullable CText = field
colFieldNullable CBool = field
colFieldNullable CTimestamptz = field
colFieldNullable CJsonb = field
colFieldNullable CStored = fmap storedJson <$> field
colFieldNullable CFloat8 = field
colFieldNullable CUuid = field

-- | Transaction bracket. Nests via savepoints.
simpleWithDbTransaction
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => m a
  -> m a
simpleWithDbTransaction =
  withSavepointTransaction (\conn sql -> void (PG.execute_ conn (Query sql))) PG.withTransaction

-- | Run a handler on the pinned connection, or on a borrowed pool connection when none is pinned.
simpleRunHandlerWithConnection
  :: (HasPoolState Connection m, MonadUnliftIO m)
  => (Connection -> job -> m result)
  -> job
  -> m result
simpleRunHandlerWithConnection handler job =
  withConn $ \conn -> handler conn job

someParamToAction :: SomeParam -> Action
someParamToAction (SomeParam (PScalar CJsonb) value) = toJSONField value
someParamToAction (SomeParam (PScalar col) value) = withColToField col (\wrap -> toField (wrap value))
someParamToAction (SomeParam (PNullable CJsonb) value) = maybe (toField (Nothing :: Maybe Int)) toJSONField value
someParamToAction (SomeParam (PNullable col) value) = withColToField col (\wrap -> toField (wrap <$> value))
someParamToAction (SomeParam (PArray col) value) = withColToField col (\wrap -> toField (PGArray (map wrap value)))
someParamToAction (SomeParam (PNullArray col) value) = withColToField col (\wrap -> toField (PGArray (map (fmap wrap) value)))

-- | The 'ToField' instance for a column, with raw JSON wrapped in 'RawJson'.
withColToField :: Col a -> (forall b. (ToField b) => (a -> b) -> r) -> r
withColToField CInt4 k = k id
withColToField CInt8 k = k id
withColToField CText k = k id
withColToField CBool k = k id
withColToField CTimestamptz k = k id
withColToField CJsonb k = k id
withColToField CStored k = k (RawJson . storedBytes)
withColToField CFloat8 k = k id
withColToField CUuid k = k id

-- | JSON bytes passed through a @json@ or @jsonb@ column without parsing.
newtype RawJson = RawJson ByteString

storedJson :: RawJson -> Stored payload
storedJson (RawJson bytes) = Stored bytes

instance FromField RawJson where
  fromField f mdata
    | typeOid f /= TI.jsonbOid && typeOid f /= TI.jsonOid = returnError Incompatible f ""
    | otherwise = maybe (returnError UnexpectedNull f "") (pure . RawJson) mdata

instance ToField RawJson where
  toField (RawJson bytes) = Escape bytes

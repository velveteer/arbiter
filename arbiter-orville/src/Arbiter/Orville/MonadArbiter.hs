{-# LANGUAGE OverloadedStrings #-}

-- | Orville primitives for writing a custom
-- 'Arbiter.Core.MonadArbiter.MonadArbiter' instance.
--
-- Handlers run in the application monad on Orville's own connection:
--
-- @
-- import Arbiter.Orville.MonadArbiter
--
-- instance MonadArbiter MyApp where
--   type RegistryOf MyApp = MyRegistry
--   type Handler MyApp job result = job -> MyApp result
--   getSchema                = asks appSchema
--   executeQuery             = orvilleExecuteQuery
--   executeStatement         = orvilleExecuteStatement
--   withDbTransaction        = orvilleWithDbTransaction
--   runHandlerWithConnection = orvilleRunHandlerWithConnection
--   getListener              = asks appListener
-- @
--
-- The primitives also need a @MonadOrville MyApp@ instance.
--
-- Write a handler's own signature as @JobHandler MyApp MyPayload MyResult@, which is
-- 'Arbiter.Core.MonadArbiter.Handler' at that queue's job and declared result types.
module Arbiter.Orville.MonadArbiter
  ( -- * MonadArbiter implementation
    orvilleExecuteQuery
  , orvilleExecuteStatement
  , orvilleWithDbTransaction
  , orvilleRunHandlerWithConnection
  ) where

import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.MonadArbiter
  ( Col (..)
  , NullCol (..)
  , ParamType (..)
  , Query (..)
  , SomeParam (..)
  , Stored (..)
  , runCodec
  , storedBytes
  )
import Control.Monad ((<=<))
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (Value, eitherDecodeStrict', encode)
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Text.Lazy qualified as TL
import Data.Text.Lazy.Encoding qualified as TLE
import Database.PostgreSQL.LibPQ qualified as LibPQ
import Orville.PostgreSQL qualified as O
import Orville.PostgreSQL.Marshall.FieldDefinition qualified as FieldDef
import Orville.PostgreSQL.Marshall.SqlMarshaller qualified as O
import Orville.PostgreSQL.Raw.Connection qualified as Conn
import Orville.PostgreSQL.Raw.PgTextFormatValue (PgTextFormatValue)
import Orville.PostgreSQL.Raw.PgTextFormatValue qualified as PgText
import Orville.PostgreSQL.Raw.SqlValue (SqlValue)
import Orville.PostgreSQL.Raw.SqlValue qualified as SqlValue

import Arbiter.Orville.Array qualified as Array

-- | Run a query, decoding rows.
orvilleExecuteQuery
  :: (O.MonadOrville m)
  => Query a
  -> m [a]
orvilleExecuteQuery query = O.withConnection $ \conn -> do
  result <- runRaw conn query
  let marshaller = O.annotateSqlMarshallerEmptyAnnotation (runCodec orvilleCol (qDecode query))
  decoded <- liftIO $ O.marshallResultFromSql O.defaultErrorDetailLevel marshaller result
  either (\err -> throwInternal $ "orville decode error: " <> T.pack (show err)) pure decoded

-- | Run a statement, returning rows affected.
orvilleExecuteStatement
  :: (O.MonadOrville m)
  => Query a
  -> m Int64
orvilleExecuteStatement query = O.withConnection $ \conn -> runRaw conn query >>= liftIO . readRowCount

runRaw :: (MonadIO m) => Conn.Connection -> Query a -> m LibPQ.Result
runRaw conn query = do
  pgParams <- encodeParams (qParams query)
  liftIO $ Conn.executeRaw conn (TE.encodeUtf8 (qPositional query)) pgParams

-- | Transaction bracket. Nests via savepoints.
orvilleWithDbTransaction :: (O.MonadOrville m) => m a -> m a
orvilleWithDbTransaction = O.withTransaction

-- | Run a handler. Orville manages its own connection.
orvilleRunHandlerWithConnection :: (job -> m result) -> job -> m result
orvilleRunHandlerWithConnection = id

someParamToSqlValue :: SomeParam -> Either Text SqlValue
someParamToSqlValue (SomeParam (PScalar col) value) =
  Right $ FieldDef.fieldValueToSqlValue (colFieldDef "" col) value
someParamToSqlValue (SomeParam (PNullable col) value) =
  Right $ FieldDef.fieldValueToSqlValue (O.nullableField (colFieldDef "" col)) value
someParamToSqlValue (SomeParam (PArray col) values) =
  SqlValue.fromRawBytes . Array.fmtArray <$> traverse (colToBytes col) values
someParamToSqlValue (SomeParam (PNullArray col) values) =
  SqlValue.fromRawBytes . Array.fmtNullableArray <$> traverse (traverse (colToBytes col)) values

colToBytes :: Col a -> a -> Either Text ByteString
colToBytes col value = sqlValueToBytes $ FieldDef.fieldValueToSqlValue (colFieldDef "" col) value

sqlValueToBytes :: SqlValue -> Either Text ByteString
sqlValueToBytes value =
  sqlValueToParam value
    >>= maybe (Left "sqlValueToBytes: got NULL, expected non-null scalar") (Right . PgText.toByteString)

sqlValueToParam :: SqlValue -> Either Text (Maybe PgTextFormatValue)
sqlValueToParam =
  SqlValue.foldSqlValue
    (Right . Just)
    (const $ Left "sqlValueToParam: got composite row, expected scalar")
    (Right Nothing)

-- | Encode the ordered params into the libpq values 'Conn.executeRaw' consumes.
encodeParams :: (MonadIO m) => [SomeParam] -> m [Maybe PgTextFormatValue]
encodeParams = traverse (either (throwInternal . ("param encoding error: " <>)) pure . (sqlValueToParam <=< someParamToSqlValue))

orvilleCol :: NullCol a -> O.SqlMarshaller () a
orvilleCol (NotNull name col) = O.marshallReadOnlyField (colFieldDef name col)
orvilleCol (Nullable name col) = O.marshallReadOnlyField (O.nullableField (colFieldDef name col))

colFieldDef :: Text -> Col a -> O.FieldDefinition O.NotNull a
colFieldDef name CInt4 = O.integerField (T.unpack name)
colFieldDef name CInt8 = O.bigIntegerField (T.unpack name)
colFieldDef name CText = O.unboundedTextField (T.unpack name)
colFieldDef name CBool = O.booleanField (T.unpack name)
colFieldDef name CTimestamptz = O.utcTimestampField (T.unpack name)
colFieldDef name CJsonb = O.fieldOfType jsonbValue (T.unpack name)
colFieldDef name CStored = O.fieldOfType storedJsonb (T.unpack name)
colFieldDef name CFloat8 = O.doubleField (T.unpack name)
colFieldDef name CUuid = O.uuidField (T.unpack name)

jsonbValue :: O.SqlType Value
jsonbValue =
  O.tryConvertSqlType
    (TL.toStrict . TLE.decodeUtf8 . encode)
    (eitherDecodeStrict' . TE.encodeUtf8)
    O.jsonb

-- | The JSON bytes as stored, through orville's text-typed @jsonb@.
storedJsonb :: O.SqlType (Stored payload)
storedJsonb = O.convertSqlType (TE.decodeUtf8 . storedBytes) (Stored . TE.encodeUtf8) O.jsonb

readRowCount :: LibPQ.Result -> IO Int64
readRowCount res =
  maybe 0 (either (const 0) fromIntegral . SqlValue.toInt . SqlValue.fromRawBytes) <$> LibPQ.cmdTuples res

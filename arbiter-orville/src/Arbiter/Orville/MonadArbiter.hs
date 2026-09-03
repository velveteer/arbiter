{-# LANGUAGE OverloadedStrings #-}

-- | 'Arbiter.Core.MonadArbiter.MonadArbiter' primitives backed by Orville.
module Arbiter.Orville.MonadArbiter
  ( orvilleExecuteQuery
  , orvilleExecuteStatement
  , orvilleWithDbTransaction
  , orvilleRunHandlerWithConnection
  ) where

import Arbiter.Core.Array qualified as Array
import Arbiter.Core.Codec (Col (..), NullCol (..), ParamType (..), SomeParam (..), runCodec)
import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.MonadArbiter (Query (..))
import Arbiter.Core.Sql.Query (numberPlaceholders)
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

-- | Run a query, decoding rows.
orvilleExecuteQuery
  :: (O.MonadOrville m)
  => Query a
  -> m [a]
orvilleExecuteQuery (Query sql params codec) = O.withConnection $ \conn -> do
  pgParams <- encodeParams params
  result <- liftIO $ Conn.executeRaw conn (TE.encodeUtf8 (numberPlaceholders sql)) pgParams
  let marshaller = O.annotateSqlMarshallerEmptyAnnotation (O.marshallReadOnly (runCodec orvilleCol codec))
  decoded <- liftIO $ O.marshallResultFromSql O.defaultErrorDetailLevel marshaller result
  case decoded of
    Right rows -> pure rows
    Left err -> throwInternal $ "orville decode error: " <> T.pack (show err)

-- | Run a statement, returning rows affected.
orvilleExecuteStatement
  :: (O.MonadOrville m)
  => Query a
  -> m Int64
orvilleExecuteStatement (Query sql params _) = O.withConnection $ \conn -> do
  pgParams <- encodeParams params
  result <- liftIO $ Conn.executeRaw conn (TE.encodeUtf8 (numberPlaceholders sql)) pgParams
  liftIO $ readRowCount result

-- | Transaction bracket. Nests via savepoints.
orvilleWithDbTransaction :: (O.MonadOrville m) => m a -> m a
orvilleWithDbTransaction = O.withTransaction

-- | Run a handler. Orville manages its own connection.
orvilleRunHandlerWithConnection :: (job -> m result) -> job -> m result
orvilleRunHandlerWithConnection handler job = handler job

someParamToSqlValue :: SomeParam -> Either Text SqlValue
someParamToSqlValue (SomeParam (PScalar col) value) =
  Right $ FieldDef.fieldValueToSqlValue (colFieldDef "" col) value
someParamToSqlValue (SomeParam (PNullable col) value) =
  Right $ FieldDef.fieldValueToSqlValue (O.nullableField (colFieldDef "" col)) value
someParamToSqlValue (SomeParam (PArray col) values) = do
  bytes <- traverse (colToBytes col) values
  Right $ SqlValue.fromRawBytes $ Array.fmtArray bytes
someParamToSqlValue (SomeParam (PNullArray col) values) = do
  bytes <- traverse (colToNullableBytes col) values
  Right $ SqlValue.fromRawBytes $ Array.fmtNullableArray bytes

colToBytes :: Col a -> a -> Either Text ByteString
colToBytes col value = sqlValueToBytes $ FieldDef.fieldValueToSqlValue (colFieldDef "" col) value

colToNullableBytes :: Col a -> Maybe a -> Either Text (Maybe ByteString)
colToNullableBytes _ Nothing = Right Nothing
colToNullableBytes col (Just value) = Just <$> colToBytes col value

sqlValueToBytes :: SqlValue -> Either Text ByteString
sqlValueToBytes =
  SqlValue.foldSqlValue
    (Right . PgText.toByteString)
    (const $ Left "sqlValueToBytes: got composite row, expected scalar")
    (Left "sqlValueToBytes: got NULL, expected non-null scalar")

sqlValueToParam :: SqlValue -> Either Text (Maybe PgTextFormatValue)
sqlValueToParam =
  SqlValue.foldSqlValue
    (Right . Just)
    (const $ Left "sqlValueToParam: got composite row, expected scalar")
    (Right Nothing)

-- | Encode the ordered params into the libpq values 'Conn.executeRaw' consumes.
encodeParams :: (MonadIO m) => [SomeParam] -> m [Maybe PgTextFormatValue]
encodeParams = traverse toPgParam
  where
    toPgParam param = case someParamToSqlValue param >>= sqlValueToParam of
      Left err -> throwInternal $ "param encoding error: " <> err
      Right pgValue -> pure pgValue

orvilleCol :: NullCol a -> O.SqlMarshaller () a
orvilleCol (NotNull name col) = O.marshallReadOnly $ O.marshallField id (colFieldDef name col)
orvilleCol (Nullable name col) = O.marshallReadOnly $ O.marshallField id (O.nullableField (colFieldDef name col))

colFieldDef :: Text -> Col a -> O.FieldDefinition O.NotNull a
colFieldDef name CInt4 = O.integerField (T.unpack name)
colFieldDef name CInt8 = O.bigIntegerField (T.unpack name)
colFieldDef name CText = O.unboundedTextField (T.unpack name)
colFieldDef name CBool = O.booleanField (T.unpack name)
colFieldDef name CTimestamptz = O.utcTimestampField (T.unpack name)
colFieldDef name CJsonb = O.fieldOfType jsonbValue (T.unpack name)
colFieldDef name CFloat8 = O.doubleField (T.unpack name)
colFieldDef name CUuid = O.uuidField (T.unpack name)

jsonbValue :: O.SqlType Value
jsonbValue =
  O.tryConvertSqlType
    (TL.toStrict . TLE.decodeUtf8 . encode)
    (eitherDecodeStrict' . TE.encodeUtf8)
    O.jsonb

readRowCount :: LibPQ.Result -> IO Int64
readRowCount res = do
  mbTuples <- LibPQ.cmdTuples res
  case mbTuples of
    Nothing -> pure 0
    Just bytes -> case SqlValue.toInt (SqlValue.fromRawBytes bytes) of
      Right count -> pure (fromIntegral count)
      Left _ -> pure 0

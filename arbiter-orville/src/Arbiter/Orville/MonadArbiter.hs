{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Orville.MonadArbiter
  ( orvilleExecuteQuery
  , orvilleExecuteStatement
  , orvilleWithDbTransaction
  , orvilleRunHandlerWithConnection
  ) where

import Arbiter.Core.Array qualified as Array
import Arbiter.Core.Codec (Col (..), NullCol (..), ParamType (..), RowCodec, SomeParam (..), runCodec)
import Arbiter.Core.Exceptions (throwInternal)
import Arbiter.Core.MonadArbiter (Params)
import Control.Monad (foldM)
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
import Orville.PostgreSQL.Raw.PgTextFormatValue qualified as PgText
import Orville.PostgreSQL.Raw.RawSql (RawSql)
import Orville.PostgreSQL.Raw.RawSql qualified as RawSql
import Orville.PostgreSQL.Raw.SqlValue (SqlValue)
import Orville.PostgreSQL.Raw.SqlValue qualified as SqlValue

orvilleExecuteQuery
  :: (O.MonadOrville m)
  => Text
  -> Params
  -> RowCodec a
  -> m [a]
orvilleExecuteQuery sql params codec = O.withConnection $ \conn -> do
  rawSql <- validateAndBuildRawSql sql params
  result <- liftIO $ RawSql.execute conn rawSql
  let marshaller = O.annotateSqlMarshallerEmptyAnnotation (O.marshallReadOnly (runCodec orvilleCol codec))
  decoded <- liftIO $ O.marshallResultFromSql O.defaultErrorDetailLevel marshaller result
  case decoded of
    Right rows -> pure rows
    Left err -> throwInternal $ "orville decode error: " <> T.pack (show err)

orvilleExecuteStatement
  :: (O.MonadOrville m)
  => Text
  -> Params
  -> m Int64
orvilleExecuteStatement sql params = O.withConnection $ \conn -> do
  rawSql <- validateAndBuildRawSql sql params
  result <- liftIO $ RawSql.execute conn rawSql
  liftIO $ readRowCount result

orvilleWithDbTransaction :: (O.MonadOrville m) => m a -> m a
orvilleWithDbTransaction = O.withTransaction

orvilleRunHandlerWithConnection :: (jobs -> m result) -> jobs -> m result
orvilleRunHandlerWithConnection handler jobs = handler jobs

someParamToSqlValue :: SomeParam -> Either Text SqlValue
someParamToSqlValue (SomeParam (PScalar c) v) =
  Right $ FieldDef.fieldValueToSqlValue (colFieldDef "" c) v
someParamToSqlValue (SomeParam (PNullable c) v) =
  Right $ FieldDef.fieldValueToSqlValue (O.nullableField (colFieldDef "" c)) v
someParamToSqlValue (SomeParam (PArray c) vs) = do
  bs <- traverse (colToBytes c) vs
  Right $ SqlValue.fromRawBytes $ Array.fmtArray bs
someParamToSqlValue (SomeParam (PNullArray c) vs) = do
  bs <- traverse (colToNullableBytes c) vs
  Right $ SqlValue.fromRawBytes $ Array.fmtNullableArray bs

colToBytes :: Col a -> a -> Either Text ByteString
colToBytes c v = sqlValueToBytes $ FieldDef.fieldValueToSqlValue (colFieldDef "" c) v

colToNullableBytes :: Col a -> Maybe a -> Either Text (Maybe ByteString)
colToNullableBytes _ Nothing = Right Nothing
colToNullableBytes c (Just v) = Just <$> colToBytes c v

sqlValueToBytes :: SqlValue -> Either Text ByteString
sqlValueToBytes =
  SqlValue.foldSqlValue
    (Right . PgText.toByteString)
    (const $ Left "sqlValueToBytes: got composite row, expected scalar")
    (Left "sqlValueToBytes: got NULL, expected non-null scalar")

validateAndBuildRawSql :: (MonadIO m) => Text -> Params -> m RawSql
validateAndBuildRawSql sqlTemplate params =
  case T.splitOn "?" sqlTemplate of
    [] -> pure mempty
    (first : rest)
      | length params /= length rest ->
          throwInternal $
            "SQL parameter count mismatch: expected "
              <> T.pack (show (length rest))
              <> " but got "
              <> T.pack (show (length params))
      | otherwise ->
          foldM
            ( \acc (p, txt) -> case someParamToSqlValue p of
                Left err -> throwInternal $ "param encoding error: " <> err
                Right sv -> pure $ acc <> RawSql.parameter sv <> RawSql.fromText txt
            )
            (RawSql.fromText first)
            (zip params rest)

orvilleCol :: NullCol a -> O.SqlMarshaller () a
orvilleCol (NotNull name c) = O.marshallReadOnly $ O.marshallField id (colFieldDef name c)
orvilleCol (Nullable name c) = O.marshallReadOnly $ O.marshallField id (O.nullableField (colFieldDef name c))

colFieldDef :: Text -> Col a -> O.FieldDefinition O.NotNull a
colFieldDef name CInt4 = O.integerField (T.unpack name)
colFieldDef name CInt8 = O.bigIntegerField (T.unpack name)
colFieldDef name CText = O.unboundedTextField (T.unpack name)
colFieldDef name CBool = O.booleanField (T.unpack name)
colFieldDef name CTimestamptz = O.utcTimestampField (T.unpack name)
colFieldDef name CJsonb = O.fieldOfType jsonbValue (T.unpack name)
colFieldDef name CFloat8 = O.doubleField (T.unpack name)

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
    Just bs -> case SqlValue.toInt (SqlValue.fromRawBytes bs) of
      Right n -> pure (fromIntegral n)
      Left _ -> pure 0

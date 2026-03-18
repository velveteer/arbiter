{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Hasql.Encode
  ( buildEncoder
  , buildStatementRowCount
  , convertPlaceholders
  , encodeSomeParam
  , colEncoder
  ) where

import Arbiter.Core.Codec (Col (..), ParamType (..), Params, SomeParam (..))
import Data.Functor.Contravariant (contramap)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as T
import Hasql.Decoders qualified as D
import Hasql.Encoders qualified as E
import Hasql.Statement qualified as S

buildStatementRowCount :: Text -> Params -> S.Statement () Int64
buildStatementRowCount sql ps =
  S.preparable (convertPlaceholders sql) (buildEncoder ps) D.rowsAffected

buildEncoder :: Params -> E.Params ()
buildEncoder = mconcat . map encodeSomeParam

encodeSomeParam :: SomeParam -> E.Params ()
encodeSomeParam (SomeParam pt v) = case pt of
  PScalar c -> contramap (const v) $ E.param (E.nonNullable (colEncoder c))
  PNullable c -> contramap (const v) $ E.param (E.nullable (colEncoder c))
  PArray c ->
    contramap (const v) $ E.param (E.nonNullable (E.array (E.dimension foldl' (E.element (E.nonNullable (colEncoder c))))))
  PNullArray c -> contramap (const v) $ E.param (E.nonNullable (E.array (E.dimension foldl' (E.element (E.nullable (colEncoder c))))))

colEncoder :: Col a -> E.Value a
colEncoder CInt4 = E.int4
colEncoder CInt8 = E.int8
colEncoder CText = E.text
colEncoder CBool = E.bool
colEncoder CTimestamptz = E.timestamptz
colEncoder CJsonb = E.jsonb
colEncoder CFloat8 = E.float8

convertPlaceholders :: Text -> Text
convertPlaceholders sql =
  case T.splitOn "?" sql of
    [] -> ""
    (first : rest) ->
      first <> mconcat (zipWith (\i part -> "$" <> T.pack (show (i :: Int)) <> part) [1 ..] rest)

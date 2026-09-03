-- | Backend-neutral parameters as Hasql encoders.
module Arbiter.Hasql.Encode
  ( buildEncoder
  , buildStatementRowCount
  , encodeSomeParam
  , colEncoder
  ) where

import Arbiter.Core.Codec (Col (..), ParamType (..), Params, SomeParam (..))
import Arbiter.Core.Sql.Query (numberPlaceholders)
import Data.Functor.Contravariant (contramap)
import Data.Int (Int64)
import Data.Text (Text)
import Hasql.Decoders qualified as D
import Hasql.Encoders qualified as E
import Hasql.Statement qualified as S

-- | Unprepared statement over numbered placeholders, returning rows affected.
buildStatementRowCount :: Text -> Params -> S.Statement () Int64
buildStatementRowCount sql params =
  S.unpreparable (numberPlaceholders sql) (buildEncoder params) D.rowsAffected

-- | A parameter list as one positional encoder.
buildEncoder :: Params -> E.Params ()
buildEncoder = mconcat . map encodeSomeParam

-- | Encode one parameter at its declared nullability.
encodeSomeParam :: SomeParam -> E.Params ()
encodeSomeParam (SomeParam paramType value) = case paramType of
  PScalar col -> contramap (const value) $ E.param (E.nonNullable (colEncoder col))
  PNullable col -> contramap (const value) $ E.param (E.nullable (colEncoder col))
  PArray col ->
    contramap (const value) $
      E.param (E.nonNullable (E.array (E.dimension foldl' (E.element (E.nonNullable (colEncoder col))))))
  PNullArray col ->
    contramap (const value) $
      E.param (E.nonNullable (E.array (E.dimension foldl' (E.element (E.nullable (colEncoder col))))))

-- | Encoder for a column type.
colEncoder :: Col a -> E.Value a
colEncoder CInt4 = E.int4
colEncoder CInt8 = E.int8
colEncoder CText = E.text
colEncoder CBool = E.bool
colEncoder CTimestamptz = E.timestamptz
colEncoder CJsonb = E.jsonb
colEncoder CFloat8 = E.float8
colEncoder CUuid = E.uuid

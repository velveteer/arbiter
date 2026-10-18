-- | Backend-neutral parameters as Hasql encoders.
module Arbiter.Hasql.Encode
  ( buildEncoder
  ) where

import Arbiter.Core.Codec (Col (..), ParamType (..), Params, SomeParam (..))
import Arbiter.Core.Job.Types (storedBytes)
import Data.Functor.Contravariant (contramap)
import Hasql.Encoders qualified as E

-- | A parameter list as one positional encoder.
buildEncoder :: Params -> E.Params ()
buildEncoder = mconcat . map encodeSomeParam

-- | Encode one parameter at its declared nullability.
encodeSomeParam :: SomeParam -> E.Params ()
encodeSomeParam (SomeParam paramType value) = contramap (const value) (E.param (paramEncoder paramType))

paramEncoder :: ParamType a -> E.NullableOrNot E.Value a
paramEncoder (PScalar col) = E.nonNullable (colEncoder col)
paramEncoder (PNullable col) = E.nullable (colEncoder col)
paramEncoder (PArray col) = E.nonNullable (E.array (E.dimension foldl' (E.element (E.nonNullable (colEncoder col)))))
paramEncoder (PNullArray col) = E.nonNullable (E.array (E.dimension foldl' (E.element (E.nullable (colEncoder col)))))

-- | Encoder for a column type.
colEncoder :: Col a -> E.Value a
colEncoder CInt4 = E.int4
colEncoder CInt8 = E.int8
colEncoder CText = E.text
colEncoder CBool = E.bool
colEncoder CTimestamptz = E.timestamptz
colEncoder CJsonb = E.jsonb
colEncoder CStored = contramap storedBytes E.jsonbBytes
colEncoder CFloat8 = E.float8
colEncoder CUuid = E.uuid

module Arbiter.Hasql.Decode
  ( hasqlRowDecoder
  ) where

import Arbiter.Core.Codec (Col (..), NullCol (..), RowCodec, runCodec)
import Hasql.Decoders qualified as D

hasqlRowDecoder :: RowCodec a -> D.Result [a]
hasqlRowDecoder codec = D.rowList (runCodec interpretCol codec)

interpretCol :: NullCol a -> D.Row a
interpretCol (NotNull _ c) = D.column (D.nonNullable (colValue c))
interpretCol (Nullable _ c) = D.column (D.nullable (colValue c))

colValue :: Col a -> D.Value a
colValue CInt4 = D.int4
colValue CInt8 = D.int8
colValue CText = D.text
colValue CBool = D.bool
colValue CTimestamptz = D.timestamptz
colValue CJsonb = D.jsonb
colValue CFloat8 = D.float8

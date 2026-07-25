{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Encoding and decoding of handler results.
module Arbiter.Core.JobResult
  ( JobResult
  , EncodeJobResult (..)
  , DecodeJobResult (..)
  ) where

import Data.Aeson (FromJSON, ToJSON, Value, toJSON)
import Data.Aeson qualified as Aeson
import Data.Text (Text)
import Data.Text qualified as T

-- | Results a handler can store. @()@ is fire-and-forget. Any @ToJSON a@ from a
-- job with a parent is stored in the results table for the parent rollup to
-- collect. A root job's result is stored on its archive entry, if archived.
class EncodeJobResult a where
  encodeJobResult :: a -> Maybe Value

-- | Results a rollup finalizer can read back with
-- 'Arbiter.Worker.childResults' or 'Arbiter.Worker.mergedChildResults'.
class DecodeJobResult a where
  decodeJobResult :: Value -> Either Text a

-- | Both halves, for a type that is stored by one handler and read by another.
type JobResult a = (EncodeJobResult a, DecodeJobResult a)

instance EncodeJobResult () where
  encodeJobResult _ = Nothing

instance DecodeJobResult () where
  decodeJobResult _ = Right ()

instance {-# OVERLAPPABLE #-} (ToJSON a) => EncodeJobResult a where
  encodeJobResult = Just . toJSON

instance {-# OVERLAPPABLE #-} (FromJSON a) => DecodeJobResult a where
  decodeJobResult = decodeJobResultAeson

-- | A @Maybe@ result is optional: @Nothing@ stores nothing, @Just x@ stores @x@.
instance {-# OVERLAPPING #-} (ToJSON a) => EncodeJobResult (Maybe a) where
  encodeJobResult = fmap toJSON

instance {-# OVERLAPPING #-} (FromJSON a) => DecodeJobResult (Maybe a) where
  decodeJobResult = decodeJobResultAeson

decodeJobResultAeson :: (FromJSON a) => Value -> Either Text a
decodeJobResultAeson v = case Aeson.fromJSON v of
  Aeson.Success a -> Right a
  Aeson.Error err -> Left (T.pack err)

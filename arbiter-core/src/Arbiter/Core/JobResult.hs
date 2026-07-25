{-# LANGUAGE UndecidableInstances #-}

-- | Encoding and decoding of handler results.
module Arbiter.Core.JobResult
  ( EncodeJobResult (..)
  , decodeJobResult
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

instance EncodeJobResult () where
  encodeJobResult _ = Nothing

instance {-# OVERLAPPABLE #-} (ToJSON a) => EncodeJobResult a where
  encodeJobResult = Just . toJSON

-- | An optional result. @Nothing@ stores nothing, @Just@ defers to @a@'s
-- instance, which may also store nothing.
instance {-# OVERLAPPING #-} (EncodeJobResult a) => EncodeJobResult (Maybe a) where
  encodeJobResult = (encodeJobResult =<<)

-- | Read a stored result back, for 'Arbiter.Worker.childResults' or
-- 'Arbiter.Worker.mergedChildResults'.
decodeJobResult :: (FromJSON a) => Value -> Either Text a
decodeJobResult v = case Aeson.fromJSON v of
  Aeson.Success a -> Right a
  Aeson.Error err -> Left (T.pack err)

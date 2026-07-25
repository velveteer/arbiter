{-# LANGUAGE UndecidableInstances #-}

-- | Encoding and decoding of handler results.
module Arbiter.Core.JobResult
  ( EncodeJobResult
  , encodeJobResult
  , decodeJobResult
  ) where

import Data.Aeson (FromJSON, ToJSON, Value, toJSON)
import Data.Aeson qualified as Aeson
import Data.Text (Text)
import Data.Text qualified as T

-- | Results a handler can store. A result from a job with a parent goes in the
-- results table for the parent rollup to collect. A root job's result goes on
-- its archive entry, if archived.
--
-- Serialization is the type's @ToJSON@, reads its @FromJSON@. @()@ and @Nothing@
-- store nothing.
class (ToJSON a) => EncodeJobResult a where
  shouldStore :: a -> Bool
  shouldStore _ = True

instance EncodeJobResult () where
  shouldStore _ = False

instance {-# OVERLAPPABLE #-} (ToJSON a) => EncodeJobResult a

-- | An optional result. @Nothing@ stores nothing, @Just@ defers to @a@'s
-- instance, which may also store nothing.
instance {-# OVERLAPPING #-} (EncodeJobResult a) => EncodeJobResult (Maybe a) where
  shouldStore = maybe False shouldStore

-- | A result's stored JSON, or 'Nothing' when its instance declines to store it.
encodeJobResult :: (EncodeJobResult a) => a -> Maybe Value
encodeJobResult a
  | shouldStore a = Just (toJSON a)
  | otherwise = Nothing

-- | Read a stored result back. 'Arbiter.Worker.childResults' surfaces a failure
-- as the child's 'Left', 'Arbiter.Worker.mergedChildResults' folds it to 'mempty'.
decodeJobResult :: (FromJSON a) => Value -> Either Text a
decodeJobResult v = case Aeson.fromJSON v of
  Aeson.Success a -> Right a
  Aeson.Error err -> Left (T.pack err)

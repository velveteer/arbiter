{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | The result type of a queue's jobs, and how results are stored and read back.
module Arbiter.Core.JobResult
  ( HasJobResult
  , ResultOf
  , JobResult
  , EncodeJobResult (..)
  , DecodeJobResult (..)
  ) where

import Data.Aeson (FromJSON, ToJSON, Value, toJSON)
import Data.Aeson qualified as Aeson
import Data.Text (Text)
import Data.Text qualified as T

-- | The result type a queue's jobs produce, keyed on the payload type.
--
-- A rollup parent and its children are jobs in the same queue, so this is what
-- makes them agree: the child stores @ResultOf payload@ and the parent reads
-- @ResultOf payload@, and a mismatch is a type error rather than a result that
-- silently fails to decode.
--
-- The result type must be storable, which the superclass enforces, so a handler
-- can always store what the queue declares.
--
-- Defaults to @()@, so a queue that stores nothing can derive it:
--
-- @
-- data EmailPayload = ...
--   deriving anyclass (FromJSON, ToJSON, HasJobResult)
--
-- instance HasJobResult ImagePayload where
--   type ResultOf ImagePayload = Score
-- @
class (EncodeJobResult (ResultOf payload)) => HasJobResult payload where
  type ResultOf payload
  type ResultOf _payload = ()

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

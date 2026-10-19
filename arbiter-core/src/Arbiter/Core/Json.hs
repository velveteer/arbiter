{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Shared Aeson helpers. Patch decoders are triple-state. An omitted field leaves
-- the value unchanged. An explicit @null@ clears the override.
module Arbiter.Core.Json
  ( patchOptions
  , decodeOrEmpty
  ) where

import Data.Aeson (FromJSON, Options (..), Value, defaultOptions, parseJSON)
import Data.Aeson.Types (parseMaybe)
import Data.Maybe (fromMaybe)

-- | Generic options that omit @Nothing@ fields. An unchanged field is absent. A
-- cleared override is @null@.
patchOptions :: Options
patchOptions = defaultOptions {omitNothingFields = True}

-- | Decode a value, or 'mempty' when it has another shape.
decodeOrEmpty :: (FromJSON a, Monoid a) => Value -> a
decodeOrEmpty = fromMaybe mempty . parseMaybe parseJSON

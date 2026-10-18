{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Shared Aeson helpers. Patch decoders are triple-state. An omitted field leaves
-- the value unchanged. An explicit @null@ clears the override.
module Arbiter.Core.Json
  ( patchOptions
  , explicitOptionalField
  , decodeOrEmpty
  ) where

import Data.Aeson (FromJSON, Object, Options (..), Value, defaultOptions, parseJSON, (.:))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Aeson.Types (Parser, parseMaybe)
import Data.Maybe (fromMaybe)
import Data.Text (Text)

-- | Generic options that omit @Nothing@ fields. An unchanged field is absent. A
-- cleared override is @null@.
patchOptions :: Options
patchOptions = defaultOptions {omitNothingFields = True}

-- | Decode an override field with three states: absent (@Nothing@, leave unchanged),
-- present and @null@ (@Just Nothing@, clear), or present with a value (@Just (Just v)@).
explicitOptionalField :: (FromJSON a) => Object -> Text -> Parser (Maybe (Maybe a))
explicitOptionalField obj name =
  let key = Key.fromText name
   in if KeyMap.member key obj
        then Just <$> obj .: key
        else pure Nothing

-- | Decode a value, or 'mempty' when it has another shape.
decodeOrEmpty :: (FromJSON a, Monoid a) => Value -> a
decodeOrEmpty = fromMaybe mempty . parseMaybe parseJSON

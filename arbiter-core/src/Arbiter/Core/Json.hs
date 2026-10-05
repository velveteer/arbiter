{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Shared Aeson helpers for triple-state patch decoders. An omitted field leaves
-- the value unchanged. An explicit @null@ clears the override.
module Arbiter.Core.Json
  ( patchOptions
  , explicitOptionalField
  ) where

import Data.Aeson (FromJSON, Object, Options (..), defaultOptions, (.:))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Aeson.Types (Parser)
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

{-# LANGUAGE OverloadedStrings #-}

-- | Shared Aeson helpers for triple-state patch decoders, where an omitted field
-- (leave unchanged) must stay distinct from an explicit @null@ (clear the override).
module Arbiter.Core.Json
  ( patchOptions
  , explicitOptionalField
  ) where

import Data.Aeson (FromJSON, Object, Options (..), defaultOptions, (.:))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Aeson.Types (Parser)
import Data.Text (Text)

-- | Generic options that omit @Nothing@ fields, so a cleared override is dropped
-- from the JSON rather than serialized as an explicit @null@.
patchOptions :: Options
patchOptions = defaultOptions {omitNothingFields = True}

-- | Decode an override field with three states: absent (@Nothing@, leave unchanged),
-- present and @null@ (@Just Nothing@, clear), or present with a value (@Just (Just v)@).
explicitOptionalField :: (FromJSON a) => Object -> Text -> Parser (Maybe (Maybe a))
explicitOptionalField o n =
  let k = Key.fromText n
   in if KeyMap.member k o
        then Just <$> o .: k
        else pure Nothing

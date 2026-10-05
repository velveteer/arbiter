{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Text lookups for finite enums.
module Arbiter.Core.Enum
  ( enumFromText
  , enumFromTextCI
  ) where

import Data.Text (Text, toCaseFold)

-- | The enum value whose name is the given text. The label names the enum in the error.
enumFromText :: (Bounded a, Enum a) => Text -> (a -> Text) -> Text -> Either Text a
enumFromText = enumFromTextBy id

-- | 'enumFromText', ignoring case.
enumFromTextCI :: (Bounded a, Enum a) => Text -> (a -> Text) -> Text -> Either Text a
enumFromTextCI = enumFromTextBy toCaseFold

enumFromTextBy :: (Bounded a, Enum a) => (Text -> Text) -> Text -> (a -> Text) -> Text -> Either Text a
enumFromTextBy norm label toName input =
  maybe
    (Left ("unknown " <> label <> ": " <> input))
    Right
    (lookup (norm input) [(norm (toName value), value) | value <- [minBound .. maxBound]])

{-# LANGUAGE OverloadedStrings #-}

-- | Deduplication strategy carried by a job at enqueue.
module Arbiter.Core.Job.Dedup
  ( DedupKey (..)

    -- * Internal

    -- | Internal to the arbiter packages. Not covered by the PVP.
  , dedupParts
  , dedupFromParts
  , dedupStrategyText
  , dedupFromStrategy
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import Data.Aeson.Types (Parser)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import GHC.Generics (Generic)

-- | Deduplication strategy, checked on INSERT via @ON CONFLICT@ on the dedup key.
data DedupKey
  = -- | Skip if the main queue table holds a job with this key (@DO NOTHING@).
    -- DLQ and archive rows do not conflict.
    IgnoreDuplicate Text
  | -- | Replace the main queue table's job with this key (@DO UPDATE@), unless it is
    -- in flight (claimed under a live lease), force-cancel-flagged, has a different
    -- parent, or has children. Children in the DLQ count.
    ReplaceDuplicate Text
  deriving stock (Eq, Generic, Show)

instance ToJSON DedupKey where
  toJSON dedup = object ["key" .= dedupKeyValue dedup, "strategy" .= dedupStrategyText dedup]

instance FromJSON DedupKey where
  parseJSON = withObject "DedupKey" $ \obj -> do
    key <- obj .: "key"
    strategy <- obj .: "strategy" :: Parser Text
    case dedupFromStrategy strategy of
      Just build -> pure $ build key
      Nothing -> fail $ "Unknown dedup strategy: " <> show strategy

-- | The @dedup_strategy@ text for a 'DedupKey'.
dedupStrategyText :: DedupKey -> Text
dedupStrategyText IgnoreDuplicate {} = "ignore"
dedupStrategyText ReplaceDuplicate {} = "replace"

-- | The 'DedupKey' constructor for a @dedup_strategy@ text.
dedupFromStrategy :: Text -> Maybe (Text -> DedupKey)
dedupFromStrategy "ignore" = Just IgnoreDuplicate
dedupFromStrategy "replace" = Just ReplaceDuplicate
dedupFromStrategy _ = Nothing

dedupKeyValue :: DedupKey -> Text
dedupKeyValue (IgnoreDuplicate key) = key
dedupKeyValue (ReplaceDuplicate key) = key

-- | The @dedup_key@ and @dedup_strategy@ column values for a 'DedupKey'.
dedupParts :: Maybe DedupKey -> (Maybe Text, Maybe Text)
dedupParts dedup = (dedupKeyValue <$> dedup, dedupStrategyText <$> dedup)

-- | The inverse of 'dedupParts'. An unknown or absent strategy reads as ignore.
dedupFromParts :: Maybe Text -> Maybe Text -> Maybe DedupKey
dedupFromParts key strategy = fromMaybe IgnoreDuplicate (dedupFromStrategy =<< strategy) <$> key

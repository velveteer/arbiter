{-# LANGUAGE OverloadedStrings #-}

-- | Deduplication strategy carried by a job at enqueue.
module Arbiter.Core.Job.Dedup
  ( DedupKey (..)

    -- * Internal

    -- | Internal to the arbiter packages. Not covered by the PVP.
  , dedupParts
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import Data.Aeson.Types (Parser)
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
  toJSON (IgnoreDuplicate key) = object ["key" .= key, "strategy" .= ("ignore" :: Text)]
  toJSON (ReplaceDuplicate key) = object ["key" .= key, "strategy" .= ("replace" :: Text)]

instance FromJSON DedupKey where
  parseJSON = withObject "DedupKey" $ \obj -> do
    key <- obj .: "key"
    strategy <- obj .: "strategy" :: Parser Text
    case strategy of
      "ignore" -> pure $ IgnoreDuplicate key
      "replace" -> pure $ ReplaceDuplicate key
      _ -> fail $ "Unknown dedup strategy: " <> show strategy

-- | The @dedup_key@ and @dedup_strategy@ column values for a 'DedupKey'.
dedupParts :: Maybe DedupKey -> (Maybe Text, Maybe Text)
dedupParts Nothing = (Nothing, Nothing)
dedupParts (Just (IgnoreDuplicate key)) = (Just key, Just "ignore")
dedupParts (Just (ReplaceDuplicate key)) = (Just key, Just "replace")

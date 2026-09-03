{-# LANGUAGE OverloadedStrings #-}

-- | Deduplication strategy carried by a job at enqueue.
module Arbiter.Core.Job.Dedup
  ( DedupKey (..)
  , dedupParts
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import Data.Aeson.Types (Parser)
import Data.Text (Text)
import GHC.Generics (Generic)

-- | Deduplication strategy, checked on INSERT via @ON CONFLICT@ on the dedup key.
data DedupKey
  = -- | Skip if a job with this key exists (@DO NOTHING@).
    IgnoreDuplicate Text
  | -- | Replace the existing job with this key (@DO UPDATE@), unless it is
    -- actively claimed, force-cancel flagged, or has children.
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

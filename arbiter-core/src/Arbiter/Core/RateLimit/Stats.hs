{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- View and patch types for the rate-limit management and observability API.
module Arbiter.Core.RateLimit.Stats
  ( RateLimitPolicyView (..)
  , RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.:?), (.=))
import Data.Aeson qualified as Aeson
import Data.Int (Int64)
import Data.Text (Text)
import Data.Time (UTCTime)
import GHC.Generics (Generic)

import Arbiter.Core.Json (explicitOptionalField, patchOptions)

-- | A policy with its default and override params plus live bucket and throttle
-- stats. Each effective param is its @override*@ field when set, else its @default*@ field.
data RateLimitPolicyView = RateLimitPolicyView
  { prefix :: Text
  -- ^ The policy's key prefix.
  , defaultMaxTokens :: Double
  -- ^ The declared bucket capacity, in tokens.
  , defaultRefillAmount :: Double
  -- ^ The declared tokens added per interval.
  , defaultInterval :: Double
  -- ^ The declared refill interval, in seconds.
  , overrideMaxTokens :: Maybe Double
  -- ^ The operator's bucket capacity, in tokens. 'Nothing' when unset.
  , overrideRefillAmount :: Maybe Double
  -- ^ The operator's tokens added per interval. 'Nothing' when unset.
  , overrideInterval :: Maybe Double
  -- ^ The operator's refill interval, in seconds. 'Nothing' when unset.
  , bucketCount :: Int64
  -- ^ Buckets under the prefix.
  , throttledCount :: Int64
  -- ^ Jobs the policy holds throttled, over every registry queue.
  , throttledQueues :: [Text]
  -- ^ The queues that hold throttled jobs, the most first.
  , minTokens :: Maybe Double
  -- ^ The lowest token count of one bucket, refill included. 'Nothing' when the
  -- prefix has no bucket.
  , avgTokens :: Maybe Double
  -- ^ The mean token count over the buckets, refill included. 'Nothing' when the
  -- prefix has no bucket.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | A single key's bucket: current tokens, effective max, and fill fraction.
data RateLimitBucketView = RateLimitBucketView
  { rateLimitKey :: Text
  -- ^ The full @prefix:suffix@ key.
  , rateLimitPrefix :: Text
  -- ^ The policy's key prefix.
  , tokens :: Double
  -- ^ Tokens available now, refill included.
  , maxTokens :: Double
  -- ^ The effective bucket capacity, in tokens.
  , fillFraction :: Maybe Double
  -- ^ Tokens divided by capacity. 'Nothing' when the capacity is 0.
  , lastRefill :: UTCTime
  -- ^ When the bucket last stored a refill.
  }
  deriving stock (Eq, Generic, Show)

instance ToJSON RateLimitBucketView where
  toJSON view =
    object
      [ "key" .= rateLimitKey view
      , "prefix" .= rateLimitPrefix view
      , "tokens" .= tokens view
      , "maxTokens" .= maxTokens view
      , "fillFraction" .= fillFraction view
      , "lastRefill" .= lastRefill view
      ]

instance FromJSON RateLimitBucketView where
  parseJSON = withObject "RateLimitBucketView" $ \obj ->
    RateLimitBucketView
      <$> obj .: "key"
      <*> obj .: "prefix"
      <*> obj .: "tokens"
      <*> obj .: "maxTokens"
      <*> obj .:? "fillFraction"
      <*> obj .: "lastRefill"

-- | A patch over a policy's override params. Per field: 'Nothing' leaves it
-- unchanged, @Just Nothing@ clears the override (reverts to the default), and
-- @Just (Just v)@ sets it.
data RateLimitPolicyUpdate = RateLimitPolicyUpdate
  { overrideMaxTokens :: Maybe (Maybe Double)
  -- ^ The bucket capacity override, in tokens.
  , overrideRefillAmount :: Maybe (Maybe Double)
  -- ^ The tokens-per-interval override.
  , overrideInterval :: Maybe (Maybe Double)
  -- ^ The refill interval override, in seconds.
  }
  deriving stock (Eq, Generic, Show)

instance ToJSON RateLimitPolicyUpdate where
  toJSON = Aeson.genericToJSON patchOptions
  toEncoding = Aeson.genericToEncoding patchOptions

-- | A missing key leaves the field unchanged. An explicit @null@
-- clears the override.
instance FromJSON RateLimitPolicyUpdate where
  parseJSON = withObject "RateLimitPolicyUpdate" $ \obj ->
    RateLimitPolicyUpdate
      <$> explicitOptionalField obj "overrideMaxTokens"
      <*> explicitOptionalField obj "overrideRefillAmount"
      <*> explicitOptionalField obj "overrideInterval"

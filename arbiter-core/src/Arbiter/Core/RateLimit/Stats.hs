{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

-- | View and patch types for the rate-limit management/observability API.
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
-- stats. The effective param is @override@ when set, else @default@.
data RateLimitPolicyView = RateLimitPolicyView
  { prefix :: Text
  , defaultMaxTokens :: Double
  , defaultRefillAmount :: Double
  , defaultInterval :: Double
  , overrideMaxTokens :: Maybe Double
  , overrideRefillAmount :: Maybe Double
  , overrideInterval :: Maybe Double
  , bucketCount :: Int64
  , throttledCount :: Int64
  , minTokens :: Maybe Double
  , avgTokens :: Maybe Double
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | A single key's bucket: current tokens, effective max, and fill fraction.
data RateLimitBucketView = RateLimitBucketView
  { rateLimitKey :: Text
  , policyPrefix :: Text
  , tokens :: Double
  , maxTokens :: Double
  , fillFraction :: Maybe Double
  , lastRefill :: UTCTime
  }
  deriving stock (Eq, Generic, Show)

instance ToJSON RateLimitBucketView where
  toJSON view =
    object
      [ "key" .= rateLimitKey view
      , "prefix" .= policyPrefix view
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
  , overrideRefillAmount :: Maybe (Maybe Double)
  , overrideInterval :: Maybe (Maybe Double)
  }
  deriving stock (Eq, Generic, Show)

instance ToJSON RateLimitPolicyUpdate where
  toJSON = Aeson.genericToJSON patchOptions
  toEncoding = Aeson.genericToEncoding patchOptions

-- | Hand-written. A missing key leaves the field unchanged. An explicit @null@
-- clears the override.
instance FromJSON RateLimitPolicyUpdate where
  parseJSON = withObject "RateLimitPolicyUpdate" $ \obj ->
    RateLimitPolicyUpdate
      <$> explicitOptionalField obj "overrideMaxTokens"
      <*> explicitOptionalField obj "overrideRefillAmount"
      <*> explicitOptionalField obj "overrideInterval"

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

-- | View and patch types for the concurrency management/observability API.
module Arbiter.Core.Concurrency.Stats
  ( ConcurrencyPolicyView (..)
  , ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.:?), (.=))
import Data.Aeson qualified as Aeson
import Data.Int (Int32, Int64)
import Data.Text (Text)
import GHC.Generics (Generic)

import Arbiter.Core.Json (explicitOptionalField, patchOptions)

-- | A pool with its default and override limits plus live key and in-flight stats.
-- The effective cap is @override@ when set, else @default@.
data ConcurrencyPolicyView = ConcurrencyPolicyView
  { prefix :: Text
  , defaultLimit :: Int32
  , overrideLimit :: Maybe Int32
  , keyCount :: Int64
  , totalInFlight :: Int64
  , maxInFlight :: Maybe Int32
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | A single key's in-flight count, its effective cap, and fill fraction.
data ConcurrencyKeyView = ConcurrencyKeyView
  { concurrencyKey :: Text
  , concurrencyPrefix :: Text
  , inFlight :: Int32
  , effectiveLimit :: Int32
  , fillFraction :: Maybe Double
  }
  deriving stock (Eq, Generic, Show)

instance ToJSON ConcurrencyKeyView where
  toJSON view =
    object
      [ "key" .= concurrencyKey view
      , "prefix" .= concurrencyPrefix view
      , "inFlight" .= inFlight view
      , "effectiveLimit" .= effectiveLimit view
      , "fillFraction" .= fillFraction view
      ]

instance FromJSON ConcurrencyKeyView where
  parseJSON = withObject "ConcurrencyKeyView" $ \obj ->
    ConcurrencyKeyView
      <$> obj .: "key"
      <*> obj .: "prefix"
      <*> obj .: "inFlight"
      <*> obj .: "effectiveLimit"
      <*> obj .:? "fillFraction"

-- | A patch over a pool's override limit. 'Nothing' leaves it unchanged, @Just
-- Nothing@ clears the override (reverts to the default), @Just (Just v)@ sets it.
data ConcurrencyPolicyUpdate = ConcurrencyPolicyUpdate
  { overrideLimit :: Maybe (Maybe Int32)
  }
  deriving stock (Eq, Generic, Show)

instance ToJSON ConcurrencyPolicyUpdate where
  toJSON = Aeson.genericToJSON patchOptions
  toEncoding = Aeson.genericToEncoding patchOptions

-- | Hand-written. A missing key leaves the field unchanged. An explicit @null@
-- clears the override.
instance FromJSON ConcurrencyPolicyUpdate where
  parseJSON = withObject "ConcurrencyPolicyUpdate" $ \obj ->
    ConcurrencyPolicyUpdate <$> explicitOptionalField obj "overrideLimit"

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- View and patch types for the concurrency management and observability API.
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

-- | A concurrency policy with its default and override limits plus live key and
-- in-flight stats. The effective cap is @overrideLimit@ when set, else @defaultLimit@.
data ConcurrencyPolicyView = ConcurrencyPolicyView
  { prefix :: Text
  -- ^ The policy's key prefix.
  , defaultLimit :: Int32
  -- ^ The declared per-key limit, in jobs.
  , overrideLimit :: Maybe Int32
  -- ^ The operator's per-key limit, in jobs. 'Nothing' when unset.
  , keyCount :: Int64
  -- ^ Keys tracked under the prefix.
  , totalInFlight :: Int64
  -- ^ In-flight jobs summed over every key.
  , maxInFlight :: Maybe Int32
  -- ^ The highest in-flight count of one key. 'Nothing' when the prefix has no key.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | A single key's in-flight count, its effective cap, and fill fraction.
data ConcurrencyKeyView = ConcurrencyKeyView
  { concurrencyKey :: Text
  -- ^ The full @prefix:suffix@ key.
  , concurrencyPrefix :: Text
  -- ^ The policy's key prefix.
  , inFlight :: Int32
  -- ^ Jobs in flight under the key.
  , effectiveLimit :: Int32
  -- ^ The override limit when set, else the default, in jobs.
  , fillFraction :: Maybe Double
  -- ^ In-flight jobs divided by the effective limit. 'Nothing' when the limit is 0.
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

-- | A patch over a concurrency policy's override limit. 'Nothing' leaves it unchanged,
-- @Just Nothing@ clears the override (reverts to the default), and @Just (Just v)@ sets it.
data ConcurrencyPolicyUpdate = ConcurrencyPolicyUpdate
  { overrideLimit :: Maybe (Maybe Int32)
  -- ^ The per-key limit override, in jobs.
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

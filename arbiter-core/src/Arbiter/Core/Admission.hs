{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Shared machinery for per-job admission policies (rate limits and concurrency
-- pools). Both kinds pick a policy and a @prefix:suffix@ key per job via a
-- 'Selector', seed every policy a registry references, and store policies in a
-- default\/override table.
module Arbiter.Core.Admission
  ( -- * Keys
    prefixedKeyText
  , prefixedKeyToJSON
  , prefixedKeyParseJSON
  , splitPrefixedSuffix

    -- * Policies and selectors
  , AdmissionPolicy (..)
  , selectNone
  , selectBy

    -- * Registry reflection
  , CollectFor (..)
  , RegistryPolicies (..)
  , registryPolicies
  , registryPolicyTables

    -- * SQL fragments
  , effectivePolicyCol
  , excludedAssignment
  , policyUpsertSQL
  ) where

import Data.Aeson (Value, object, withObject, (.:), (.=))
import Data.Aeson.Types (Parser)
import Data.Proxy (Proxy (..))
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import GHC.TypeLits (KnownSymbol, symbolVal)
import NeatInterpolation (text)

import Arbiter.Core.QueueRegistry (JobPayloadRegistry, SpecName, SpecPayload)
import Arbiter.Core.Selector (Selector, field, usePolicy)

-- Keys -----------------------------------------------------------------------

-- | The stored key text, @prefix:suffix@.
prefixedKeyText :: Text -> Text -> Text
prefixedKeyText prefix suffix = prefix <> ":" <> suffix

-- | JSON for a @prefix:suffix@ key.
prefixedKeyToJSON :: Text -> Text -> Value
prefixedKeyToJSON prefix suffix = object ["prefix" .= prefix, "suffix" .= suffix]

-- | Parse a @prefix:suffix@ key into a constructor.
prefixedKeyParseJSON :: String -> (Text -> Text -> a) -> Value -> Parser a
prefixedKeyParseJSON name mkKey = withObject name $ \obj -> mkKey <$> obj .: "prefix" <*> obj .: "suffix"

-- | Recover the suffix of a stored @prefix:suffix@ key given its prefix.
splitPrefixedSuffix :: Text -> Text -> Text
splitPrefixedSuffix prefix key = T.drop (T.length prefix + 1) key

-- Policies and selectors -----------------------------------------------------

-- | A policy that admits jobs under a named prefix.
class (Ord p) => AdmissionPolicy p where
  policyPrefixOf :: p -> Text

-- | This payload is unrestricted by this policy kind.
selectNone :: Selector p payload (Maybe key)
selectNone = pure Nothing

-- | Restrict by a fixed policy, keyed by a per-job suffix (e.g. a tenant id).
selectBy
  :: (AdmissionPolicy p)
  => (Text -> Text -> key)
  -> p
  -> (payload -> Text)
  -> Selector p payload (Maybe key)
selectBy mkKey pol suffix =
  (\policy suffixText -> Just (mkKey (policyPrefixOf policy) suffixText)) <$> usePolicy pol <*> field suffix

-- SQL fragments ---------------------------------------------------------------

-- | The effective (override-or-default) column of the policy at @alias@.
effectivePolicyCol :: Text -> Text -> Text
effectivePolicyCol alias name = [text|COALESCE(${alias}.override_${name}, ${alias}.default_${name})|]

-- | An @ON CONFLICT DO UPDATE SET@ assignment copying a column from the excluded row.
excludedAssignment :: Text -> Text
excludedAssignment column = [text|${column} = EXCLUDED.${column}|]

-- | Upsert a policy row's defaults keyed on prefix_id, preserving operator overrides.
policyUpsertSQL :: Text -> Text -> [(Text, Text)] -> Text
policyUpsertSQL policiesTable prefixLit defaults =
  let names = T.intercalate ", " (map fst defaults)
      values = T.intercalate ", " (map snd defaults)
      setClause = T.intercalate ", " (map (excludedAssignment . fst) defaults)
   in [text|INSERT INTO ${policiesTable} (prefix_id, ${names}) VALUES (${prefixLit}, ${values}) ON CONFLICT (prefix_id) DO UPDATE SET ${setClause};|]

-- Registry reflection --------------------------------------------------------

-- | The policies of kind @p@ a single payload's selector can reach. Each feature
-- provides an instance from its 'Arbiter.Core.RateLimit.Spec.HasRateLimit' \/
-- 'Arbiter.Core.Concurrency.Spec.HasConcurrency' selector.
class CollectFor payload p where
  collectFor :: Set p

-- | Each registry table's policies of kind @p@, in registry order.
class RegistryPolicies (registry :: JobPayloadRegistry) p where
  registryTablePolicies :: [(Text, Set p)]

instance RegistryPolicies '[] p where
  registryTablePolicies = []

instance
  (CollectFor (SpecPayload spec) p, KnownSymbol (SpecName spec), RegistryPolicies rest p)
  => RegistryPolicies (spec ': rest) p
  where
  registryTablePolicies =
    (T.pack (symbolVal (Proxy @(SpecName spec))), collectFor @(SpecPayload spec) @p)
      : registryTablePolicies @rest @p

-- | Every policy of kind @p@ declared across a registry's payloads.
registryPolicies :: forall registry p. (Ord p, RegistryPolicies registry p) => Set p
registryPolicies = Set.unions (map snd (registryTablePolicies @registry @p))

-- | Each registry table paired with whether its payload declares any policy of kind @p@.
registryPolicyTables :: forall registry p. (RegistryPolicies registry p) => [(Text, Bool)]
registryPolicyTables = map (fmap (not . Set.null)) (registryTablePolicies @registry @p)

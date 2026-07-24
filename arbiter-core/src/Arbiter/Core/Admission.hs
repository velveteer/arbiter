{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

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
  , policyViewScope
  ) where

import Data.Aeson (Value, object, withObject, (.:), (.=))
import Data.Aeson.Types (Parser)
import Data.Proxy (Proxy (..))
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import GHC.TypeLits (KnownSymbol, symbolVal)

import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Arbiter.Core.Selector (Selector, field, usePolicy)

-- Keys -----------------------------------------------------------------------

-- | The stored key text, @prefix:suffix@.
prefixedKeyText :: Text -> Text -> Text
prefixedKeyText pfx sfx = pfx <> ":" <> sfx

-- | JSON for a @prefix:suffix@ key.
prefixedKeyToJSON :: Text -> Text -> Value
prefixedKeyToJSON p s = object ["prefix" .= p, "suffix" .= s]

-- | Parse a @prefix:suffix@ key into a constructor.
prefixedKeyParseJSON :: String -> (Text -> Text -> a) -> Value -> Parser a
prefixedKeyParseJSON name mk = withObject name $ \v -> mk <$> v .: "prefix" <*> v .: "suffix"

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
  (\p s -> Just (mkKey (policyPrefixOf p) s)) <$> usePolicy pol <*> field suffix

-- SQL fragments ---------------------------------------------------------------

-- | A policy's effective (override-or-default) column, for policy alias @p@ and
-- base column name (e.g. @"max_tokens"@).
effectivePolicyCol :: Text -> Text -> Text
effectivePolicyCol p name =
  "COALESCE(" <> p <> ".override_" <> name <> ", " <> p <> ".default_" <> name <> ")"

-- | An @ON CONFLICT DO UPDATE SET@ assignment copying a column from the excluded row.
excludedAssignment :: Text -> Text
excludedAssignment n = n <> " = EXCLUDED." <> n

-- | Upsert a policy row's defaults keyed on prefix_id, preserving operator overrides.
policyUpsertSQL :: Text -> Text -> [(Text, Text)] -> Text
policyUpsertSQL policiesTable prefixLit defaults =
  let names = map fst defaults
      setClause = T.intercalate ", " (map excludedAssignment names)
   in T.concat
        [ "INSERT INTO "
        , policiesTable
        , " (prefix_id, "
        , T.intercalate ", " names
        , ") VALUES ("
        , prefixLit
        , ", "
        , T.intercalate ", " (map snd defaults)
        , ") ON CONFLICT (prefix_id) DO UPDATE SET "
        , setClause
        , ";"
        ]

-- | Single-vs-list scoping for the policy views. Single mode scopes both the
-- aggregate (via @aggPrefixCol@) and the outer query to the prefix bound in CTE
-- @k@, which the caller supplies. Returns @(aggWhere, scope)@.
policyViewScope :: Bool -> Text -> (Text, Text)
policyViewScope single aggPrefixCol
  | single =
      ( "WHERE " <> aggPrefixCol <> " = (SELECT prefix FROM k)"
      , "WHERE p.prefix_id = (SELECT prefix FROM k)"
      )
  | otherwise = ("", "ORDER BY p.prefix_id")

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
  (CollectFor payload p, KnownSymbol name, RegistryPolicies rest p)
  => RegistryPolicies ('(name, payload) ': rest) p
  where
  registryTablePolicies =
    (T.pack (symbolVal (Proxy @name)), collectFor @payload @p) : registryTablePolicies @rest @p

-- | Every policy of kind @p@ declared across a registry's payloads. The migration
-- seeds these.
registryPolicies :: forall registry p. (Ord p, RegistryPolicies registry p) => Set p
registryPolicies = Set.unions (map snd (registryTablePolicies @registry @p))

-- | Each registry table paired with whether its payload declares any policy of kind @p@.
registryPolicyTables :: forall registry p. (RegistryPolicies registry p) => [(Text, Bool)]
registryPolicyTables = map (fmap (not . Set.null)) (registryTablePolicies @registry @p)

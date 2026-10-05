{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Per-job concurrency limits. A payload's 'concurrencyFor' describes how to
-- select a policy and key. Static inspection finds all policies that the migration
-- must initialize. Each selected policy supplies the limit.
module Arbiter.Core.Concurrency.Spec
  ( -- * Core types
    ConcurrencyKey (..)
  , concurrencyKeyText
  , ConcurrencyPolicy (..)
  , concurrencyPool

    -- * Selecting a policy per job
  , HasConcurrency (..)
  , ConcurrencyFor
  , noConcurrency
  , concurrencyBy
  , globalConcurrency
  , concurrencyByCase
  , chooseWhen
  , collectPolicies

    -- * Registry reflection
  , RegistryConcurrencyPolicies
  , registryConcurrencyPolicies
  , registryConcurrencyTables
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..))
import Data.Int (Int32)
import Data.Set (Set)
import Data.Text (Text)

import Arbiter.Core.Admission
  ( AdmissionPolicy (..)
  , CollectFor (..)
  , RegistryPolicies (..)
  , prefixedKeyParseJSON
  , prefixedKeyText
  , prefixedKeyToJSON
  , registryPolicies
  , registryPolicyTables
  , selectBy
  , selectNone
  )
import Arbiter.Core.Selector (Selector, chooseWhen, collectPolicies, selectByCase)

-- | A resolved concurrency key with a policy prefix and per-key suffix. The
-- stored form is @prefix:suffix@. The separate prefix supports policy lookup.
data ConcurrencyKey = ConcurrencyKey
  { ckPrefix :: Text
  -- ^ The policy prefix.
  , ckSuffix :: Text
  -- ^ The per-key suffix, such as a tenant id.
  }
  deriving stock (Eq, Show)

instance ToJSON ConcurrencyKey where
  toJSON (ConcurrencyKey prefix suffix) = prefixedKeyToJSON prefix suffix

instance FromJSON ConcurrencyKey where
  parseJSON = prefixedKeyParseJSON "ConcurrencyKey" ConcurrencyKey

-- | The stored key text, @prefix:suffix@.
concurrencyKeyText :: ConcurrencyKey -> Text
concurrencyKeyText (ConcurrencyKey prefix suffix) = prefixedKeyText prefix suffix

-- | A concurrency policy. At most @cpLimit@ jobs are in flight per key under @cpPrefix@. The
-- default is seeded. An operator override on the policy takes precedence.
data ConcurrencyPolicy = ConcurrencyPolicy
  { cpPrefix :: Text
  -- ^ The key prefix. It must not contain @:@.
  , cpLimit :: Int32
  -- ^ The default in-flight limit per key, in jobs. The migration rejects a
  -- declared limit below 1.
  }
  deriving stock (Eq, Ord, Show)

instance AdmissionPolicy ConcurrencyPolicy where
  policyPrefixOf = cpPrefix

-- | A policy named @prefix@ admitting at most @limit@ concurrent jobs per key. The cap is
-- floored at 1. Pause it with @'Arbiter.Core.HighLevel.setConcurrencyLimit' policy {cpLimit = 0}@. The prefix must not contain @:@, the key
-- separator. The migration enforces this.
concurrencyPool :: Text -> Int32 -> ConcurrencyPolicy
concurrencyPool prefix limit = ConcurrencyPolicy prefix (max 1 limit)

-- | A selective description of the concurrency key for a payload. Evaluation
-- returns the job key. Static inspection returns the reachable policies.
type ConcurrencyFor payload = Selector ConcurrencyPolicy payload (Maybe ConcurrencyKey)

-- | This payload is unbounded.
noConcurrency :: ConcurrencyFor payload
noConcurrency = selectNone

-- | Cap by a fixed policy, keyed by a per-job suffix, such as a tenant id.
concurrencyBy :: ConcurrencyPolicy -> (payload -> Text) -> ConcurrencyFor payload
concurrencyBy = selectBy ConcurrencyKey

-- | Cap by a fixed policy under one shared key.
globalConcurrency :: ConcurrencyPolicy -> Text -> ConcurrencyFor payload
globalConcurrency pol suffix = concurrencyBy pol (const suffix)

-- | N-way 'chooseWhen'. Maps the job to a finite tag, then each tag to its selector.
-- Policy collection evaluates every tag in @[minBound..maxBound]@. The tag's
-- 'Bounded'\/'Enum' and the selector must be total over @k@.
concurrencyByCase
  :: (Bounded k, Enum k, Eq k) => (payload -> k) -> (k -> ConcurrencyFor payload) -> ConcurrencyFor payload
concurrencyByCase = selectByCase

-- | A payload's per-job policy selection. Defaults to unbounded. Only capped
-- payloads need an instance.
class HasConcurrency payload where
  -- | The selector deciding which policy (if any) caps a given job.
  concurrencyFor :: ConcurrencyFor payload
  concurrencyFor = noConcurrency

instance {-# OVERLAPPABLE #-} HasConcurrency payload

instance (HasConcurrency payload) => CollectFor payload ConcurrencyPolicy where
  collectFor = collectPolicies (concurrencyFor @payload)

-- | The constraint that lets the migration collect a registry's declared policies.
type RegistryConcurrencyPolicies registry = RegistryPolicies registry ConcurrencyPolicy

-- | Every distinct policy declared across the registry's payloads.
registryConcurrencyPolicies :: forall registry. (RegistryConcurrencyPolicies registry) => Set ConcurrencyPolicy
registryConcurrencyPolicies = registryPolicies @registry @ConcurrencyPolicy

-- | Each registry table paired with whether its payload declares any policy.
registryConcurrencyTables :: forall registry. (RegistryConcurrencyPolicies registry) => [(Text, Bool)]
registryConcurrencyTables = registryPolicyTables @registry @ConcurrencyPolicy

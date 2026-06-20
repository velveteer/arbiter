{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Per-job concurrency limits, declared like rate limits. A payload's
-- 'concurrencyFor' picks a pool and key per job, and statically collects every
-- pool it could use so the migration seeds them. The limit is the pool's.
module Arbiter.Core.Concurrency.Spec
  ( -- * Core types
    ConcurrencyKey (..)
  , concurrencyKeyText
  , ConcurrencyPolicy (..)
  , concurrencyPool

    -- * Selecting a pool per job
  , HasConcurrency (..)
  , ConcurrencyFor
  , noConcurrency
  , concurrencyBy
  , globalConcurrency
  , concurrencyByCase
  , chooseWhen
  , runConcurrencyFor
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
import Arbiter.Core.Selector (Selector, chooseWhen, collectPolicies, runSelector, selectByCase)

-- | A resolved concurrency key: pool prefix and per-key suffix. Stored as
-- @prefix:suffix@, prefix kept separate so the policy lookup never re-splits on @:@.
data ConcurrencyKey = ConcurrencyKey
  { ckPrefix :: Text
  , ckSuffix :: Text
  }
  deriving stock (Eq, Show)

instance ToJSON ConcurrencyKey where
  toJSON (ConcurrencyKey p s) = prefixedKeyToJSON p s

instance FromJSON ConcurrencyKey where
  parseJSON = prefixedKeyParseJSON "ConcurrencyKey" ConcurrencyKey

-- | The stored key text, @prefix:suffix@.
concurrencyKeyText :: ConcurrencyKey -> Text
concurrencyKeyText (ConcurrencyKey p s) = prefixedKeyText p s

-- | A concurrency pool: at most @cpLimit@ jobs share a key under @cpPrefix@. The
-- default is seeded. An operator override on the pool takes precedence.
data ConcurrencyPolicy = ConcurrencyPolicy
  { cpPrefix :: Text
  , cpLimit :: Int32
  }
  deriving stock (Eq, Ord, Show)

instance AdmissionPolicy ConcurrencyPolicy where
  policyPrefixOf = cpPrefix

-- | A pool named @prefix@ admitting at most @limit@ concurrent jobs per key. The cap is
-- floored at 1, since the seeded default must be positive (pause a pool via an override).
-- The prefix must not contain @:@ (the key separator), which the migration enforces.
concurrencyPool :: Text -> Int32 -> ConcurrencyPolicy
concurrencyPool prefix limit = ConcurrencyPolicy prefix (max 1 limit)

-- | A selective description of the concurrency key for a payload. Run it against a
-- job to pick the key. Inspect it statically to collect reachable pools.
type ConcurrencyFor payload = Selector ConcurrencyPolicy payload (Maybe ConcurrencyKey)

-- | This payload is unbounded.
noConcurrency :: ConcurrencyFor payload
noConcurrency = selectNone

-- | Cap by a fixed pool, keyed by a per-job suffix (e.g. a tenant id).
concurrencyBy :: ConcurrencyPolicy -> (payload -> Text) -> ConcurrencyFor payload
concurrencyBy = selectBy ConcurrencyKey

-- | Cap by a fixed pool under one shared key (a single global pool).
globalConcurrency :: ConcurrencyPolicy -> Text -> ConcurrencyFor payload
globalConcurrency pol suffix = concurrencyBy pol (const suffix)

-- | Concurrency 'selectByCase'. See 'selectByCase' for the totality requirement on @k@.
concurrencyByCase
  :: (Bounded k, Enum k, Eq k) => (payload -> k) -> (k -> ConcurrencyFor payload) -> ConcurrencyFor payload
concurrencyByCase = selectByCase

-- | Run a selector against a concrete job to get its key.
runConcurrencyFor :: payload -> ConcurrencyFor payload -> Maybe ConcurrencyKey
runConcurrencyFor = runSelector

-- | A payload's per-job pool selection. Defaults to unbounded, so only capped
-- payloads need an instance.
class HasConcurrency payload where
  concurrencyFor :: ConcurrencyFor payload
  concurrencyFor = noConcurrency

instance {-# OVERLAPPABLE #-} HasConcurrency payload

instance (HasConcurrency payload) => CollectFor payload ConcurrencyPolicy where
  collectFor = collectPolicies (concurrencyFor @payload)

-- | Collect every pool declared across a registry's payloads, by statically
-- inspecting each payload's 'concurrencyFor'. The migration seeds these.
type RegistryConcurrencyPolicies registry = RegistryPolicies registry ConcurrencyPolicy

registryConcurrencyPolicies :: forall registry. (RegistryConcurrencyPolicies registry) => Set ConcurrencyPolicy
registryConcurrencyPolicies = registryPolicies @registry @ConcurrencyPolicy

-- | Each registry table paired with whether its payload declares any pool.
registryConcurrencyTables :: forall registry. (RegistryConcurrencyPolicies registry) => [(Text, Bool)]
registryConcurrencyTables = registryPolicyTables @registry @ConcurrencyPolicy

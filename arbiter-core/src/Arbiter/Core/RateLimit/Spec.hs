{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Per-job token-bucket rate limits. A payload's 'rateLimitFor' is a small
-- selective description: running it against a job picks the policy and key, while
-- statically inspecting it collects every policy it could use. So the migration
-- seeds exactly the policies a registry references.
module Arbiter.Core.RateLimit.Spec
  ( -- * Core types
    Durability (..)
  , RateLimitKey (..)
  , rateLimitKeyText
  , Policy (..)
  , tokenBucket

    -- * Selecting a policy per job
  , HasRateLimit (..)
  , RateLimitFor
  , noLimit
  , limitBy
  , globalLimit
  , chooseWhen
  , limitByCase
  , runRateLimitFor
  , collectPolicies

    -- * Registry reflection
  , RegistryRateLimitPolicies
  , registryRateLimitPolicies
  , registryRateLimitTables
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..))
import Data.Set (Set)
import Data.Text (Text)
import Data.Time (NominalDiffTime)

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

-- | Whether the rate-limit bucket table is WAL-logged. Set at migration time.
data Durability = Durable | Unlogged
  deriving stock (Eq, Show)

-- | A resolved key: prefix and per-key suffix. Stored as @prefix:suffix@, prefix
-- kept separate so policy lookup never re-splits on @:@.
data RateLimitKey = RateLimitKey
  { rlkPrefix :: Text
  , rlkSuffix :: Text
  }
  deriving stock (Eq, Show)

instance ToJSON RateLimitKey where
  toJSON (RateLimitKey p s) = prefixedKeyToJSON p s

instance FromJSON RateLimitKey where
  parseJSON = prefixedKeyParseJSON "RateLimitKey" RateLimitKey

rateLimitKeyText :: RateLimitKey -> Text
rateLimitKeyText (RateLimitKey p s) = prefixedKeyText p s

-- | A token-bucket policy: burst @policyMax@, refilling @policyRefill@ every
-- @policyInterval@. A @policyRefill@ of 0 is a manually-refilled bucket.
data Policy = Policy
  { policyPrefix :: Text
  , policyMax :: Double
  , policyRefill :: Double
  , policyInterval :: NominalDiffTime
  }
  deriving stock (Eq, Ord, Show)

instance AdmissionPolicy Policy where
  policyPrefixOf = policyPrefix

-- | "N per period" with burst N (max = refill = n). The period is floored to a
-- tiny positive value, since the seeded default must be positive. The prefix must
-- not contain @:@ (the key separator), which the migration enforces.
tokenBucket :: Text -> Double -> NominalDiffTime -> Policy
tokenBucket prefix n period = Policy prefix n n (max 1e-6 period)

-- | A selective description of the rate-limit key for a payload. Run it against
-- a job to pick the key. Inspect it statically to collect reachable policies.
type RateLimitFor payload = Selector Policy payload (Maybe RateLimitKey)

-- | This payload is unlimited.
noLimit :: RateLimitFor payload
noLimit = selectNone

-- | Limit by a fixed policy, keyed by a per-job suffix (e.g. a tenant id).
limitBy :: Policy -> (payload -> Text) -> RateLimitFor payload
limitBy = selectBy RateLimitKey

-- | Limit by a fixed policy under one shared key (a single global bucket).
globalLimit :: Policy -> Text -> RateLimitFor payload
globalLimit pol suffix = limitBy pol (const suffix)

-- | N-way 'chooseWhen': map the job to a finite tag, then each tag to its selector.
-- Policy collection evaluates every tag in @[minBound..maxBound]@, so the tag's
-- 'Bounded'\/'Enum' and the selector must be total over @k@.
limitByCase :: (Bounded k, Enum k, Eq k) => (payload -> k) -> (k -> RateLimitFor payload) -> RateLimitFor payload
limitByCase = selectByCase

-- | Run a selector against a concrete job to get its key.
runRateLimitFor :: payload -> RateLimitFor payload -> Maybe RateLimitKey
runRateLimitFor = runSelector

-- | A payload's per-job key selection. Defaults to unlimited, so only limited
-- payloads need an instance.
class HasRateLimit payload where
  -- | The selector deciding which policy (if any) limits a given job.
  rateLimitFor :: RateLimitFor payload
  rateLimitFor = noLimit

  -- | How many tokens this job spends. Defaults to 1.
  rateLimitCost :: payload -> Double
  rateLimitCost _ = 1

instance {-# OVERLAPPABLE #-} HasRateLimit payload

instance (HasRateLimit payload) => CollectFor payload Policy where
  collectFor = collectPolicies (rateLimitFor @payload)

-- | Collect every policy declared across a registry's payloads, by statically
-- inspecting each payload's 'rateLimitFor'. The migration seeds these.
type RegistryRateLimitPolicies registry = RegistryPolicies registry Policy

registryRateLimitPolicies :: forall registry. (RegistryRateLimitPolicies registry) => Set Policy
registryRateLimitPolicies = registryPolicies @registry @Policy

-- | Each registry table paired with whether its payload declares any policy.
registryRateLimitTables :: forall registry. (RegistryRateLimitPolicies registry) => [(Text, Bool)]
registryRateLimitTables = registryPolicyTables @registry @Policy

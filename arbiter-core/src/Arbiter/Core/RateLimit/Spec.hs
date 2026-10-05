{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ConstraintKinds #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Per-job token-bucket rate limits. A payload's 'rateLimitFor' describes how
-- to select its policy and key. Static inspection finds all policies that the
-- migration must initialize.
module Arbiter.Core.RateLimit.Spec
  ( -- * Core types
    Durability (..)
  , RateLimitKey (..)
  , rateLimitKeyText
  , RateLimitPolicy (..)
  , tokenBucket

    -- * Selecting a policy per job
  , HasRateLimit (..)
  , RateLimitFor
  , noLimit
  , limitBy
  , globalLimit
  , chooseWhen
  , limitByCase
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
import Arbiter.Core.Selector (Selector, chooseWhen, collectPolicies, selectByCase)

-- | Whether the rate-limit bucket table is WAL-logged. Set by the migration config.
data Durability
  = -- | WAL-logged. Buckets survive a crash.
    Durable
  | -- | Not WAL-logged. A crash resets the buckets. The migrations use this by default.
    Unlogged
  deriving stock (Eq, Show)

-- | A resolved key with a prefix and per-key suffix. The stored form is
-- @prefix:suffix@. The separate prefix supports policy lookup.
data RateLimitKey = RateLimitKey
  { rlkPrefix :: Text
  -- ^ The policy prefix.
  , rlkSuffix :: Text
  -- ^ The per-key suffix, such as a tenant id.
  }
  deriving stock (Eq, Show)

instance ToJSON RateLimitKey where
  toJSON (RateLimitKey prefix suffix) = prefixedKeyToJSON prefix suffix

instance FromJSON RateLimitKey where
  parseJSON = prefixedKeyParseJSON "RateLimitKey" RateLimitKey

-- | The stored key text, @prefix:suffix@.
rateLimitKeyText :: RateLimitKey -> Text
rateLimitKeyText (RateLimitKey prefix suffix) = prefixedKeyText prefix suffix

-- | A token-bucket policy. Burst @policyMax@, refilling @policyRefill@ every
-- @policyInterval@. A @policyRefill@ of 0 is a manually-refilled bucket. Fields are
-- non-negative and the interval is positive. The migration rejects other values.
data RateLimitPolicy = RateLimitPolicy
  { policyPrefix :: Text
  -- ^ The key prefix. It must not contain @:@.
  , policyMax :: Double
  -- ^ The bucket capacity, in tokens.
  , policyRefill :: Double
  -- ^ The tokens added per interval.
  , policyInterval :: NominalDiffTime
  -- ^ The refill interval.
  }
  deriving stock (Eq, Ord, Show)

instance AdmissionPolicy RateLimitPolicy where
  policyPrefixOf = policyPrefix

-- | @count@ per @period@, with a burst of @count@. The period is floored to a
-- tiny positive value. The prefix must not contain @:@, the key separator. The
-- migration enforces this.
tokenBucket :: Text -> Double -> NominalDiffTime -> RateLimitPolicy
tokenBucket prefix count period = RateLimitPolicy prefix count count (max 1e-6 period)

-- | A selective description of the rate-limit key for a payload. Evaluation
-- returns the job key. Static inspection returns the reachable policies.
type RateLimitFor payload = Selector RateLimitPolicy payload (Maybe RateLimitKey)

-- | This payload is unlimited.
noLimit :: RateLimitFor payload
noLimit = selectNone

-- | Limit by a fixed policy, keyed by a per-job suffix, such as a tenant id.
limitBy :: RateLimitPolicy -> (payload -> Text) -> RateLimitFor payload
limitBy = selectBy RateLimitKey

-- | Limit by a fixed policy under one shared key (a single global bucket).
globalLimit :: RateLimitPolicy -> Text -> RateLimitFor payload
globalLimit pol suffix = limitBy pol (const suffix)

-- | N-way 'chooseWhen'. Maps the job to a finite tag, then each tag to its selector.
-- Policy collection evaluates every tag in @[minBound..maxBound]@. The tag's
-- 'Bounded'\/'Enum' and the selector must be total over @k@.
limitByCase :: (Bounded k, Enum k, Eq k) => (payload -> k) -> (k -> RateLimitFor payload) -> RateLimitFor payload
limitByCase = selectByCase

-- | A payload's per-job policy selection. Defaults to unlimited. Only limited
-- payloads need an instance.
class HasRateLimit payload where
  -- | The selector deciding which policy (if any) limits a given job.
  rateLimitFor :: RateLimitFor payload
  rateLimitFor = noLimit

  -- | How many tokens this job spends, clamped to the bucket's range. Defaults to 1.
  rateLimitCost :: payload -> Double
  rateLimitCost _ = 1

instance {-# OVERLAPPABLE #-} HasRateLimit payload

instance (HasRateLimit payload) => CollectFor payload RateLimitPolicy where
  collectFor = collectPolicies (rateLimitFor @payload)

-- | The constraint that lets the migration collect a registry's declared policies.
type RegistryRateLimitPolicies registry = RegistryPolicies registry RateLimitPolicy

-- | Every distinct policy declared across the registry's payloads.
registryRateLimitPolicies :: forall registry. (RegistryRateLimitPolicies registry) => Set RateLimitPolicy
registryRateLimitPolicies = registryPolicies @registry @RateLimitPolicy

-- | Each registry table paired with whether its payload declares any policy.
registryRateLimitTables :: forall registry. (RegistryRateLimitPolicies registry) => [(Text, Bool)]
registryRateLimitTables = registryPolicyTables @registry @RateLimitPolicy

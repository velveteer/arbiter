{-# LANGUAGE DuplicateRecordFields #-}

-- | Per-job rate limiting.
--
-- This is the user-facing rate-limit module. The @Arbiter.Core.RateLimit.*@ modules are internal.
--
-- Declare which policy (if any) limits each job with a 'HasRateLimit' instance.
-- The migration seeds every policy a selector can reach.
module Arbiter.RateLimit
  ( -- * Declaring a payload's limit
    HasRateLimit (..)
  , RateLimitFor
  , Selector
  , noLimit
  , limitBy
  , globalLimit
  , chooseWhen
  , limitByCase
  , RegistryRateLimitPolicies

    -- * Policies
  , RateLimitPolicy (..)
  , tokenBucket
  , AdmissionPolicy (..)

    -- * Bucket durability
  , Durability (..)

    -- * Keys
  , RateLimitKey (..)

    -- * Management and observability views
  , RateLimitPolicyView (..)
  , RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)

    -- * Operations
  , addRateLimitTokens
  , pruneRateLimitBuckets
  , resetRateLimitBuckets
  , listRateLimitPolicies
  , listRateLimitBuckets
  , updateRateLimitPolicyOverrides
  , setRateLimit
  , clearRateLimit
  , getRateLimitPolicy
  , rateLimitPolicyExists
  ) where

import Arbiter.Core.Admission (AdmissionPolicy (..))
import Arbiter.Core.HighLevel
  ( addRateLimitTokens
  , clearRateLimit
  , getRateLimitPolicy
  , listRateLimitBuckets
  , listRateLimitPolicies
  , pruneRateLimitBuckets
  , rateLimitPolicyExists
  , resetRateLimitBuckets
  , setRateLimit
  , updateRateLimitPolicyOverrides
  )
import Arbiter.Core.RateLimit.Spec
  ( Durability (..)
  , HasRateLimit (..)
  , RateLimitFor
  , RateLimitKey (..)
  , RateLimitPolicy (..)
  , RegistryRateLimitPolicies
  , chooseWhen
  , globalLimit
  , limitBy
  , limitByCase
  , noLimit
  , tokenBucket
  )
import Arbiter.Core.RateLimit.Stats
  ( RateLimitBucketView (..)
  , RateLimitPolicyUpdate (..)
  , RateLimitPolicyView (..)
  )
import Arbiter.Core.Selector (Selector)

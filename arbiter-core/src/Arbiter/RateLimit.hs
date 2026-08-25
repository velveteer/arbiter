{-# LANGUAGE DuplicateRecordFields #-}

-- | Per-job rate limiting.
--
-- Declare which policy (if any) limits each job with a 'HasRateLimit' instance,
-- building the selector from 'noLimit' \/ 'limitBy' \/ 'globalLimit' \/
-- 'chooseWhen' \/ 'limitByCase'. The migration statically collects every policy a
-- selector can reach and seeds it.
module Arbiter.RateLimit
  ( -- * Declaring a payload's limit
    HasRateLimit (..)
  , RateLimitFor
  , noLimit
  , limitBy
  , globalLimit
  , chooseWhen
  , limitByCase

    -- * Policies
  , Policy (..)
  , tokenBucket
  , policyPrefixOf

    -- * Bucket durability
  , Durability (..)

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

    -- * Keys
  , RateLimitKey (..)
  ) where

import Arbiter.Core.Admission (AdmissionPolicy (..))
import Arbiter.Core.HighLevel
  ( addRateLimitTokens
  , clearRateLimit
  , listRateLimitBuckets
  , listRateLimitPolicies
  , pruneRateLimitBuckets
  , resetRateLimitBuckets
  , setRateLimit
  , updateRateLimitPolicyOverrides
  )
import Arbiter.Core.RateLimit.Spec
  ( Durability (..)
  , HasRateLimit (..)
  , Policy (..)
  , RateLimitFor
  , RateLimitKey (..)
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

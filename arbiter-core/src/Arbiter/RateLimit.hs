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

    -- * Bucket durability
  , Durability (..)
  , RateLimitDurability (..)

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
  ) where

import Arbiter.Core.HighLevel
  ( addRateLimitTokens
  , listRateLimitBuckets
  , listRateLimitPolicies
  , pruneRateLimitBuckets
  , resetRateLimitBuckets
  , updateRateLimitPolicyOverrides
  )
import Arbiter.Core.RateLimit.Spec
  ( Durability (..)
  , HasRateLimit (..)
  , Policy (..)
  , RateLimitDurability (..)
  , RateLimitFor
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

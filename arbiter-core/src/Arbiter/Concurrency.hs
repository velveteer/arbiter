{-# LANGUAGE DuplicateRecordFields #-}

-- | Per-job concurrency limits.
--
-- Declare which pool (if any) caps each job with a 'HasConcurrency' instance,
-- building the selector from 'noConcurrency' \/ 'concurrencyBy' \/
-- 'globalConcurrency' \/ 'concurrencyByCase'. The migration collects every pool a
-- selector can reach and seeds it. The limit is the pool's, not the job's.
module Arbiter.Concurrency
  ( -- * Declaring a payload's pool
    HasConcurrency (..)
  , ConcurrencyFor
  , noConcurrency
  , concurrencyBy
  , globalConcurrency
  , concurrencyByCase
  , chooseWhen

    -- * Pools
  , ConcurrencyPolicy
  , concurrencyPool

    -- * Management and observability views
  , ConcurrencyPolicyView (..)
  , ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)

    -- * Operations
  , updateConcurrencyPolicyOverrides
  , pruneConcurrencyKeys
  , reconcileConcurrencyCounts
  , listConcurrencyPolicies
  , listConcurrencyKeys
  ) where

import Arbiter.Core.Concurrency.Spec
  ( ConcurrencyFor
  , ConcurrencyPolicy
  , HasConcurrency (..)
  , chooseWhen
  , concurrencyBy
  , concurrencyByCase
  , concurrencyPool
  , globalConcurrency
  , noConcurrency
  )
import Arbiter.Core.Concurrency.Stats
  ( ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)
  , ConcurrencyPolicyView (..)
  )
import Arbiter.Core.HighLevel
  ( listConcurrencyKeys
  , listConcurrencyPolicies
  , pruneConcurrencyKeys
  , reconcileConcurrencyCounts
  , updateConcurrencyPolicyOverrides
  )

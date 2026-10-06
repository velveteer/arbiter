{-# LANGUAGE DuplicateRecordFields #-}

-- | Per-job concurrency limits.
--
-- This is the user-facing concurrency module. The @Arbiter.Core.Concurrency.*@ modules are internal.
--
-- Declare which policy (if any) caps each job with a 'HasConcurrency' instance.
-- The migration seeds every policy a selector can reach. The policy holds the limit.
module Arbiter.Concurrency
  ( -- * Declaring a payload's policy
    HasConcurrency (..)
  , ConcurrencyFor
  , Selector
  , noConcurrency
  , concurrencyBy
  , globalConcurrency
  , chooseWhen
  , concurrencyByCase
  , RegistryConcurrencyPolicies

    -- * Policies
  , ConcurrencyPolicy (..)
  , concurrencyPolicy
  , AdmissionPolicy (..)

    -- * Keys
  , ConcurrencyKey (..)

    -- * Management and observability views
  , ConcurrencyPolicyView (..)
  , ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)

    -- * Operations
  , updateConcurrencyPolicyOverrides
  , setConcurrencyLimit
  , clearConcurrencyLimit
  , pruneConcurrencyKeys
  , reconcileConcurrencyCounts
  , reconcileConcurrencyCountsIfStale
  , reconcileAndPruneConcurrency
  , listConcurrencyPolicies
  , listConcurrencyKeys
  , getConcurrencyPolicy
  , concurrencyPolicyExists
  ) where

import Arbiter.Core.Admission (AdmissionPolicy (..))
import Arbiter.Core.Concurrency.Spec
  ( ConcurrencyFor
  , ConcurrencyKey (..)
  , ConcurrencyPolicy (..)
  , HasConcurrency (..)
  , RegistryConcurrencyPolicies
  , chooseWhen
  , concurrencyBy
  , concurrencyByCase
  , concurrencyPolicy
  , globalConcurrency
  , noConcurrency
  )
import Arbiter.Core.Concurrency.Stats
  ( ConcurrencyKeyView (..)
  , ConcurrencyPolicyUpdate (..)
  , ConcurrencyPolicyView (..)
  )
import Arbiter.Core.HighLevel
  ( clearConcurrencyLimit
  , concurrencyPolicyExists
  , getConcurrencyPolicy
  , listConcurrencyKeys
  , listConcurrencyPolicies
  , pruneConcurrencyKeys
  , reconcileAndPruneConcurrency
  , reconcileConcurrencyCounts
  , reconcileConcurrencyCountsIfStale
  , setConcurrencyLimit
  , updateConcurrencyPolicyOverrides
  )
import Arbiter.Core.Selector (Selector)

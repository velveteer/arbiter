{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- One guard per pool. It fences every batch in flight and extends their leases.
-- A batch registers through 'guardBatch' and is signalled through 'recheck'.
-- 'runHeartbeatGuard' is the loop that fences and extends.
--
-- Written against io-classes, so the pool runs it in IO and the tests run it
-- under io-sim.
module Arbiter.Worker.Heartbeat.Guard
  ( HeartbeatGuard
  , GuardConfig (..)
  , newHeartbeatGuard
  , runHeartbeatGuard
  , guardKey
  , Batch (..)
  , guardBatch
  , recheck
  , trySync
  , toDiffTime
  , minRetryPause
  , settleGrace
  , leaseExpiredReason
  , reclaimedReason
  , deletedReason
  ) where

import Arbiter.Worker.Heartbeat.Guard.Loop (runHeartbeatGuard, trySync)
import Arbiter.Worker.Heartbeat.Guard.Signal (guardBatch, recheck)
import Arbiter.Worker.Heartbeat.Guard.State
  ( Batch (..)
  , GuardConfig (..)
  , HeartbeatGuard
  , deletedReason
  , guardKey
  , leaseExpiredReason
  , minRetryPause
  , newHeartbeatGuard
  , reclaimedReason
  , settleGrace
  , toDiffTime
  )

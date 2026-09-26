-- | One guard per pool. It fences every batch in flight and extends their leases.
--
-- The state and who writes it is "Arbiter.Worker.Heartbeat.Guard.State". A
-- batch registers and is signalled through "Arbiter.Worker.Heartbeat.Guard.Signal".
-- The loop that fences and extends is "Arbiter.Worker.Heartbeat.Guard.Loop".
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
  ) where

import Arbiter.Worker.Heartbeat.Guard.Loop (runHeartbeatGuard, trySync)
import Arbiter.Worker.Heartbeat.Guard.Signal (guardBatch, recheck)
import Arbiter.Worker.Heartbeat.Guard.State
  ( Batch (..)
  , GuardConfig (..)
  , HeartbeatGuard
  , guardKey
  , leaseExpiredReason
  , minRetryPause
  , newHeartbeatGuard
  , reclaimedReason
  , settleGrace
  , toDiffTime
  )

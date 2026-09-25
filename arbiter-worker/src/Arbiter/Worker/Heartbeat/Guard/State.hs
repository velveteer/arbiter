{-# LANGUAGE OverloadedStrings #-}

-- | The guard's shared state. Three threads write it: the handler through
-- register and unregister, the guard loop, and the extend thread the loop
-- forks. Each field says who writes it.
module Arbiter.Worker.Heartbeat.Guard.State
  ( GuardConfig (..)
  , Batch (..)
  , Guarded (..)
  , Status (..)
  , InFlight (..)
  , HeartbeatGuard (..)
  , Wake (..)
  , newHeartbeatGuard
  , guardKey
  , snapshot
  , adjust
  , pendingOf
  , wakeFor
  , wake
  , heartbeatWait
  , toDiffTime
  , minRetryPause
  , settleGrace
  , leaseExpiredReason
  , reclaimedReason
  ) where

import Arbiter.Core.HighLevel (SetVisibilityResult (..))
import Arbiter.Core.Job.Types (JobId)
import Control.Concurrent.Class.MonadSTM (MonadSTM, STM, TVar, atomically, modifyTVar', newTVarIO, readTVar, readTVarIO)
import Control.Monad (when)
import Control.Monad.Class.MonadFork (MonadThread (..))
import Control.Monad.Class.MonadTime.SI (DiffTime, Time, UTCTime)
import Data.Fixed (Fixed (..))
import Data.List.NonEmpty (NonEmpty)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Set (Set)
import Data.Text (Text)
import Data.Time.Clock (NominalDiffTime, nominalDiffTimeToSeconds, picosecondsToDiffTime)

import Arbiter.Worker.Logger (LogLevel (..))

-- | Shortest gap between failed extends.
minRetryPause :: DiffTime
minRetryPause = 0.25

-- | Settle time a landed extend gets past its give-up.
settleGrace :: DiffTime
settleGrace = 0.25

-- | The reason a batch is stopped at its lease.
leaseExpiredReason :: Text
leaseExpiredReason = "lease expired without renewal"

-- | The reason a batch another worker reclaimed is stopped.
reclaimedReason :: Text
reclaimedReason = "reclaimed by another worker"

-- | A wall-clock span as a monotonic one. Exact, no Rational detour.
toDiffTime :: NominalDiffTime -> DiffTime
toDiffTime elapsed = picosecondsToDiffTime picos
  where
    MkFixed picos = nominalDiffTimeToSeconds elapsed

-- | Wait before the next extend. At most a beat and at least 'minRetryPause'.
heartbeatWait :: DiffTime -> Bool -> DiffTime -> DiffTime
heartbeatWait beat extended remaining
  | extended, remaining > beat = beat
  | otherwise = min beat (max minRetryPause (remaining / 2))

-- | What the guard needs from the pool.
data GuardConfig n job = GuardConfig
  { configInterval :: DiffTime
  , configTimeout :: DiffTime
  , configMaxDuration :: Maybe DiffTime
  , configKey :: job -> JobId
  , configLease :: job -> Maybe UTCTime
  -- ^ The row's lease deadline, as the claim read it back.
  , configExtend :: [job] -> n [SetVisibilityResult]
  -- ^ Extend the jobs' leases by 'configTimeout', reporting each.
  , configExtended :: n ()
  -- ^ Runs after each extend that reached the database.
  , configLog :: LogLevel -> [job] -> Text -> n ()
  -- ^ The pool log, with the jobs in context.
  , configHeartbeat :: job -> UTCTime -> UTCTime -> n ()
  -- ^ The heartbeat hook: the job, now, and the batch start.
  }

-- | A batch under guard.
data Batch n job = Batch
  { batchJobs :: NonEmpty job
  , batchPending :: n [job]
  -- ^ Read on the guard's threads, for the jobs still awaiting an outcome.
  , batchStart :: UTCTime
  , batchInherit :: n () -> n ()
  -- ^ Runs the heartbeat hooks under the batch's context.
  }

-- | A registered batch. Fixed at register.
data Guarded n job = Guarded
  { guardedToken :: Int
  , guardedBatch :: Batch n job
  , guardedHandler :: ThreadId n
  , guardedDeadline :: Maybe Time
  -- ^ When the duration fence fires.
  , guardedStatus :: TVar n (Status n)
  }

-- | A registered batch's timers and flags.
data Status n = Status
  { leaseAt :: !Time
  -- ^ When the lease runs out. Register and settle write it.
  , beatAt :: !Time
  -- ^ When the next extend is due. Register, settle and a failed extend write it.
  , leaseLapsed :: !Bool
  -- ^ The lease lapsed and the batch gets no further extend. The fence writes it.
  , deadlineSent :: !Bool
  -- ^ The deadline signal went out. The fence writes it.
  , signalledAt :: !(Maybe Time)
  -- ^ When the last signal went out. Signal writes it.
  , couriers :: !(Maybe [ThreadId n])
  -- ^ Threads carrying a signal to the handler. Nothing once unregistered. Couriers and unregister write it.
  }

-- | The extend statement in flight. The loop issues it, the extend thread lands and clears it.
-- The loop abandons it past its give-up and settle grace.
data InFlight = InFlight
  { issuedAt :: !Time
  , givesUp :: !Time
  -- ^ When its timeout fires.
  , carries :: !(Set Int)
  -- ^ The batches it covers.
  , landed :: !Bool
  -- ^ It returned. Its results are being settled.
  }

-- | The pool's heartbeat guard.
data HeartbeatGuard n job = HeartbeatGuard
  { guardConfig :: GuardConfig n job
  , guardEntries :: TVar n (Map Int (Guarded n job))
  , guardNextToken :: TVar n Int
  , guardInFlight :: TVar n (Maybe InFlight)
  , guardAbandoned :: TVar n (Maybe Time)
  -- ^ The issue time of the abandoned extend still running. The loop sets it,
  -- that extend's thread clears it.
  , guardWake :: Wake n
  }

-- | How the guard loop is woken before its target.
data Wake n = Wake
  { wakeCount :: TVar n Int
  -- ^ Bumped by each wake.
  , wakeTarget :: TVar n (Maybe Time)
  -- ^ The time the loop sleeps until.
  }

newHeartbeatGuard :: (MonadSTM n) => GuardConfig n job -> n (HeartbeatGuard n job)
newHeartbeatGuard config =
  HeartbeatGuard config
    <$> newTVarIO Map.empty
    <*> newTVarIO 0
    <*> newTVarIO Nothing
    <*> newTVarIO Nothing
    <*> (Wake <$> newTVarIO 0 <*> newTVarIO Nothing)

-- | The key the guard tracks jobs by.
guardKey :: HeartbeatGuard n job -> job -> JobId
guardKey = configKey . guardConfig

-- | Every registered batch with its status.
snapshot :: (MonadSTM n) => HeartbeatGuard n job -> STM n [(Guarded n job, Status n)]
snapshot guard = do
  entries <- readTVar (guardEntries guard)
  traverse (\entry -> (,) entry <$> readTVar (guardedStatus entry)) (Map.elems entries)

adjust :: (MonadSTM n) => Guarded n job -> (Status n -> Status n) -> n ()
adjust entry = atomically . modifyTVar' (guardedStatus entry)

-- | The batch's jobs still awaiting an outcome. None once it unregistered.
pendingOf :: (MonadSTM n) => Guarded n job -> n [job]
pendingOf entry = do
  status <- readTVarIO (guardedStatus entry)
  if isJust (couriers status) then batchPending (guardedBatch entry) else pure []

-- | Wake the loop when @at@ precedes its target.
wakeFor :: (MonadSTM n) => HeartbeatGuard n job -> Time -> STM n ()
wakeFor guard at = do
  target <- readTVar (wakeTarget (guardWake guard))
  when (maybe True (at <) target) (wake guard)

wake :: (MonadSTM n) => HeartbeatGuard n job -> STM n ()
wake guard = modifyTVar' (wakeCount (guardWake guard)) (+ 1)

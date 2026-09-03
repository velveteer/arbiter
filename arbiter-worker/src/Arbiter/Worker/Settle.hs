-- | Batch finalization state and ordered finalization operations.
--
-- A finalization commits the outcome, records it, and then calls its hooks. The
-- record precedes the hooks. 'settle' enforces this order.
module Arbiter.Worker.Settle
  ( -- * Handoff
    CancelHandoff
  , newCancelHandoff
  , pendingJobs
  , unownedJobs
  , recordCancelled
  , cancelFinalized
  , markCancelFinalized

    -- * Settling
  , Settled
  , finalized
  , disowned
  , settle
  , settleBy
  , settleInterruptibly
  , record

    -- * Job sets
  , hasIdIn
  , byIdDesc
  ) where

import Arbiter.Core.Job.Types qualified as Job
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Foldable (toList)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.List (sortOn)
import Data.List.NonEmpty (NonEmpty)
import Data.Ord (Down (..))
import Data.Set qualified as Set
import UnliftIO (MonadUnliftIO, mask_)

-- | What a batch has settled so far.
data BatchProgress = BatchProgress
  { progressHandled :: !(Set.Set Int64)
  -- ^ Jobs whose outcome has been recorded.
  , progressUnowned :: !(Set.Set Int64)
  -- ^ Jobs a batch ack found under another claim.
  , progressCancelled :: !(Set.Set Int64)
  -- ^ Jobs a force-cancel accounted for.
  , progressFinalized :: !Bool
  -- ^ Whether the force-cancel finalizer ran to completion.
  }

-- | Finalization state shared by the handler and force-cancel operation.
newtype CancelHandoff = CancelHandoff (IORef BatchProgress)

newCancelHandoff :: (MonadIO m) => m CancelHandoff
newCancelHandoff = liftIO (CancelHandoff <$> newIORef (BatchProgress mempty mempty mempty False))

readProgress :: (MonadIO m) => CancelHandoff -> m BatchProgress
readProgress (CancelHandoff ref) = liftIO (readIORef ref)

onProgress :: (MonadIO m) => CancelHandoff -> (BatchProgress -> (BatchProgress, a)) -> m a
onProgress (CancelHandoff ref) = liftIO . atomicModifyIORef' ref

-- | Whether a set of job ids names this job.
hasIdIn :: Set.Set Int64 -> Job.JobRead payload -> Bool
hasIdIn ids job = Set.member (Job.primaryKey job) ids

-- | Select jobs from the batch in descending identifier order. This gives ack,
-- force-cancel, and heartbeat operations the same row-lock order.
byIdDesc :: (Job.JobRead payload -> Bool) -> NonEmpty (Job.JobRead payload) -> [Job.JobRead payload]
byIdDesc keep = sortOn (Down . Job.primaryKey) . filter keep . toList

-- | Jobs in the batch that have no recorded outcome.
pendingJobs :: (MonadIO m) => CancelHandoff -> NonEmpty (Job.JobRead payload) -> m [Job.JobRead payload]
pendingJobs handoff jobs = do
  progress <- readProgress handoff
  pure (byIdDesc (not . hasIdIn (progressHandled progress)) jobs)

-- | The batch's jobs a settle found under another claim.
unownedJobs :: (MonadIO m) => CancelHandoff -> NonEmpty (Job.JobRead payload) -> m [Job.JobRead payload]
unownedJobs handoff jobs = do
  progress <- readProgress handoff
  pure (byIdDesc (hasIdIn (progressUnowned progress)) jobs)

-- | Add to the jobs a force-cancel accounted for, returning the ids new to this call
-- and every id recorded so far.
recordCancelled :: (MonadIO m) => CancelHandoff -> Set.Set Int64 -> m (Set.Set Int64, Set.Set Int64)
recordCancelled handoff ids =
  onProgress handoff $ \progress ->
    let cancelled = progressCancelled progress <> ids
     in (progress {progressCancelled = cancelled}, (ids Set.\\ progressCancelled progress, cancelled))

-- | Whether the force-cancel finalizer ran to completion.
cancelFinalized :: (MonadIO m) => CancelHandoff -> m Bool
cancelFinalized = fmap progressFinalized . readProgress

markCancelFinalized :: (MonadIO m) => CancelHandoff -> m ()
markCancelFinalized handoff = onProgress handoff $ \progress -> (progress {progressFinalized = True}, ())

-- | What a settle accounted for: the jobs it finalized, and the jobs it found under
-- another claim.
data Settled payload = Settled [Job.JobRead payload] [Job.JobRead payload]

instance Semigroup (Settled payload) where
  Settled handledA unownedA <> Settled handledB unownedB = Settled (handledA <> handledB) (unownedA <> unownedB)

-- | Jobs a settle finalized.
finalized :: [Job.JobRead payload] -> Settled payload
finalized jobs = Settled jobs []

-- | Jobs a settle found under another claim.
disowned :: [Job.JobRead payload] -> Settled payload
disowned jobs = Settled [] jobs

-- | Record a finalization in one atomic update. Jobs owned by another claim also
-- count as handled.
record :: (MonadIO m) => CancelHandoff -> Settled payload -> m ()
record handoff (Settled handled unowned) =
  onProgress handoff $ \progress ->
    ( progress
        { progressHandled = progressHandled progress <> ids handled <> gone
        , progressUnowned = progressUnowned progress <> gone
        }
    , ()
    )
  where
    gone = ids unowned
    ids = Set.fromList . map Job.primaryKey

-- | Commit a settle, record it, then run its hooks. @protect@ covers the commit and
-- the record together.
settleWith
  :: (MonadIO m)
  => (m a -> m a)
  -> CancelHandoff
  -> m a
  -- ^ Commit.
  -> (a -> Settled payload)
  -- ^ What it accounted for.
  -> (a -> m b)
  -- ^ Hooks. The record precedes them.
  -> m b
settleWith protect handoff commit accounts hooks = do
  outcome <- protect $ commit >>= \result -> result <$ record handoff (accounts result)
  hooks outcome

-- | Apply 'settleWith' to a set known before the commit. Mask asynchronous
-- exceptions between the commit and state update.
settle
  :: (MonadUnliftIO m)
  => CancelHandoff
  -> Settled payload
  -> m a
  -> (a -> m b)
  -> m b
settle handoff settled commit = settleWith mask_ handoff commit (const settled)

-- | 'settle' for a commit that decides what it settled.
settleBy
  :: (MonadUnliftIO m)
  => CancelHandoff
  -> m a
  -> (a -> Settled payload)
  -> (a -> m b)
  -> m b
settleBy = settleWith mask_

-- | Apply 'settle' with an interruptible commit for a transaction that contains
-- the handler.
settleInterruptibly
  :: (MonadIO m)
  => CancelHandoff
  -> Settled payload
  -> m a
  -> (a -> m b)
  -> m b
settleInterruptibly handoff settled commit = settleWith id handoff commit (const settled)

-- | The batch handoff and the settle ordering built on it.
--
-- A settle commits a job's outcome, records that it did, then reports it. The record
-- has to land before the report, or a heartbeat tick reads a job this worker already
-- settled and reports it a second time. 'settle' runs the three in that order, so a
-- caller reaching the handoff through it cannot fire a hook first.
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

-- | What a force-cancel finalizer needs from the handler's scope, whichever side of
-- the job span runs it.
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

-- | The batch's jobs a predicate selects, children-first, so the row locks taken for
-- them follow the same order as ack and force-cancel. The heartbeat needs this too:
-- its batch update joins the rows in the order they are handed to it.
byIdDesc :: (Job.JobRead payload -> Bool) -> NonEmpty (Job.JobRead payload) -> [Job.JobRead payload]
byIdDesc p = sortOn (Down . Job.primaryKey) . filter p . toList

-- | The batch's jobs still awaiting an outcome, which keeps the force-cancel finalizer
-- and the outcome report from both reporting the same job.
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
  onProgress handoff $ \p ->
    let s = progressCancelled p <> ids in (p {progressCancelled = s}, (ids Set.\\ progressCancelled p, s))

-- | Whether a force-cancel was finalized in full, which only the finalizer records.
cancelFinalized :: (MonadIO m) => CancelHandoff -> m Bool
cancelFinalized = fmap progressFinalized . readProgress

markCancelFinalized :: (MonadIO m) => CancelHandoff -> m ()
markCancelFinalized handoff = onProgress handoff $ \p -> (p {progressFinalized = True}, ())

-- | What a settle accounted for: the jobs it finalized, and the jobs it found under
-- another claim.
data Settled payload = Settled [Job.JobRead payload] [Job.JobRead payload]

instance Semigroup (Settled payload) where
  Settled a b <> Settled c d = Settled (a <> c) (b <> d)

instance Monoid (Settled payload) where
  mempty = Settled [] []

-- | Jobs a settle finalized.
finalized :: [Job.JobRead payload] -> Settled payload
finalized js = Settled js []

-- | Jobs a settle found under another claim.
disowned :: [Job.JobRead payload] -> Settled payload
disowned js = Settled [] js

-- | Write a settle into the handoff in one update, so no reader sees half of it. A job
-- found under another claim counts as handled too.
record :: (MonadIO m) => CancelHandoff -> Settled payload -> m ()
record handoff (Settled handled unowned) =
  onProgress handoff $ \p ->
    ( p
        { progressHandled = progressHandled p <> ids handled <> gone
        , progressUnowned = progressUnowned p <> gone
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
  -- ^ Hooks, which the record always precedes.
  -> m b
settleWith protect handoff commit accounts hooks = do
  a <- protect $ commit >>= \r -> r <$ record handoff (accounts r)
  hooks a

-- | 'settleWith' over a set known before the commit runs, holding off an async exception
-- between the commit and the record.
settle
  :: (MonadUnliftIO m)
  => CancelHandoff
  -> Settled payload
  -> m a
  -> (a -> m b)
  -> m b
settle handoff s commit = settleWith mask_ handoff commit (const s)

-- | 'settle' for a commit that decides what it settled, which only a bulk ack needs.
settleBy
  :: (MonadUnliftIO m)
  => CancelHandoff
  -> m a
  -> (a -> Settled payload)
  -> (a -> m b)
  -> m b
settleBy = settleWith mask_

-- | 'settle' leaving the commit interruptible, for a transaction that holds the handler.
settleInterruptibly
  :: (MonadIO m)
  => CancelHandoff
  -> Settled payload
  -> m a
  -> (a -> m b)
  -> m b
settleInterruptibly handoff s commit = settleWith id handoff commit (const s)

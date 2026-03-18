{-# LANGUAGE DeriveAnyClass #-}

-- | Exceptions that job handlers throw to signal failure.
--
-- Using these types lets a handler control whether a failed job is retried
-- or moved directly to the dead-letter queue.
module Arbiter.Core.Exceptions
  ( -- * Job Processing Exceptions
    JobException (..)
  , JobRetryableException (..)
  , JobPermanentException (..)
  , TreeCancelException (..)
  , BranchCancelException (..)
  , ParsingException (..)
  , InternalException (..)
  , JobNotFoundException (..)
  , JobStolenException (..)

    -- * Helpers
  , throwRetryable
  , throwPermanent
  , throwTreeCancel
  , throwBranchCancel
  , throwParsing
  , throwInternal
  , throwJobNotFound
  , throwJobStolen
  ) where

import Control.Exception (Exception)
import Control.Exception qualified as E
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Text (Text)
import Data.Typeable (Typeable)
import GHC.Generics (Generic)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Exception qualified as UE

-- | Caught by the worker to decide retry vs DLQ vs cancellation.
data JobException
  = Retryable JobRetryableException
  | Permanent JobPermanentException
  | ParseFailure ParsingException
  | -- | Deletes the entire job tree from root to leaves.
    TreeCancel TreeCancelException
  | -- | Cascade-deletes the parent and all siblings.
    BranchCancel BranchCancelException
  | JobNotFound JobNotFoundException
  | JobStolen JobStolenException
  deriving stock (Show, Typeable)
  deriving anyclass (Exception)

-- | Transient failure — the job will be retried with backoff.
newtype JobRetryableException = JobRetryableException Text
  deriving stock (Eq, Generic, Show, Typeable)
  deriving anyclass (Exception)

-- | Permanent failure — the job goes straight to the DLQ, no retries.
newtype JobPermanentException = JobPermanentException Text
  deriving stock (Eq, Generic, Show, Typeable)
  deriving anyclass (Exception)

throwRetryable :: (MonadUnliftIO m) => Text -> m a
throwRetryable msg = UE.throwIO (Retryable (JobRetryableException msg))

throwPermanent :: (MonadUnliftIO m) => Text -> m a
throwPermanent msg = UE.throwIO (Permanent (JobPermanentException msg))

-- | Cancels an entire job tree from root to leaves.
-- Use when a failure invalidates all work in the tree.
newtype TreeCancelException = TreeCancelException Text
  deriving stock (Eq, Generic, Show, Typeable)
  deriving anyclass (Exception)

-- | Cancels the current branch (parent + all siblings).
-- If the parent has a grandparent, the grandparent is resumed.
newtype BranchCancelException = BranchCancelException Text
  deriving stock (Eq, Generic, Show, Typeable)
  deriving anyclass (Exception)

throwTreeCancel :: (MonadUnliftIO m) => Text -> m a
throwTreeCancel msg = UE.throwIO (TreeCancel (TreeCancelException msg))

throwBranchCancel :: (MonadUnliftIO m) => Text -> m a
throwBranchCancel msg = UE.throwIO (BranchCancel (BranchCancelException msg))

-- | Row decoding failure (internal).
newtype ParsingException = ParsingException Text
  deriving stock (Eq, Generic, Show, Typeable)
  deriving anyclass (Exception)

throwParsing :: (MonadIO m) => Text -> m a
throwParsing msg = liftIO $ E.throwIO (ParseFailure (ParsingException msg))

newtype InternalException = InternalException Text
  deriving stock (Eq, Generic, Show, Typeable)
  deriving anyclass (Exception)

throwInternal :: (MonadIO m) => Text -> m a
throwInternal msg = liftIO $ E.throwIO (InternalException msg)

-- | Job was deleted or reclaimed between claim and ack (internal).
newtype JobNotFoundException = JobNotFoundException Text
  deriving stock (Eq, Generic, Show, Typeable)
  deriving anyclass (Exception)

throwJobNotFound :: (MonadUnliftIO m) => Text -> m a
throwJobNotFound msg = UE.throwIO (JobNotFound (JobNotFoundException msg))

-- | Heartbeat detected another worker reclaimed the job (internal).
newtype JobStolenException = JobStolenException Text
  deriving stock (Eq, Generic, Show, Typeable)
  deriving anyclass (Exception)

throwJobStolen :: (MonadUnliftIO m) => Text -> m a
throwJobStolen msg = UE.throwIO (JobStolen (JobStolenException msg))

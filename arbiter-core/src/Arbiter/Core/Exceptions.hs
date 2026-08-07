{-# LANGUAGE OverloadedStrings #-}

-- | Exceptions thrown by job handlers and by the worker engine.
--
-- 'JobException' is the user-facing decision sum. A handler throws one of
-- these to signal how a failed job should be processed (retry, DLQ, cancel
-- the tree or branch). A handler can also throw 'JobNackException' (via
-- 'throwNack') to reprocess the job without recording a failure.
--
-- The engine-internal exceptions ('ParsingException', 'InternalException',
-- 'JobNotFoundException', 'JobStolenException') are thrown directly as
-- their own types, not wrapped in any sum. They are handled by the worker's
-- retry combinators and classifier, and are not part of the surface API
-- that user handlers throw.
module Arbiter.Core.Exceptions
  ( -- * User-facing job decisions
    JobException (..)
  , JobRetryableException (..)
  , JobPermanentException (..)
  , TreeCancelException (..)
  , BranchCancelException (..)
  , JobNackException (..)

    -- * Engine-internal signals
  , ParsingException (..)
  , InternalException (..)
  , JobNotFoundException (..)
  , JobStolenException (..)
  , JobForceCancelled (..)

    -- * Helpers
  , throwRetryable
  , throwPermanent
  , throwTreeCancel
  , throwBranchCancel
  , throwNack
  , throwParsing
  , throwInternal
  , throwJobNotFoundIds
  , throwJobStolen
  , throwJobStolenIds
  , namedJobIds
  ) where

import Control.Exception (Exception (..), asyncExceptionFromException, asyncExceptionToException)
import Control.Monad.IO.Class (MonadIO)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Generics (Generic)
import UnliftIO.Exception qualified as UE

-- | Decisions a handler can signal by throwing. Caught by the worker to
-- decide retry vs DLQ vs cancellation.
data JobException
  = Retryable JobRetryableException
  | Permanent JobPermanentException
  | -- | Deletes the entire job tree from root to leaves.
    TreeCancel TreeCancelException
  | -- | Cascade-deletes the parent and all siblings.
    BranchCancel BranchCancelException
  deriving stock (Show)

instance Exception JobException where
  displayException = \case
    Retryable e -> displayException e
    Permanent e -> displayException e
    TreeCancel e -> displayException e
    BranchCancel e -> displayException e

-- | Transient failure - the job will be retried with backoff.
newtype JobRetryableException = JobRetryableException Text
  deriving stock (Eq, Generic, Show)

instance Exception JobRetryableException where
  displayException (JobRetryableException msg) = T.unpack msg

-- | Permanent failure - the job goes straight to the DLQ, no retries.
newtype JobPermanentException = JobPermanentException Text
  deriving stock (Eq, Generic, Show)

instance Exception JobPermanentException where
  displayException (JobPermanentException msg) = T.unpack msg

-- | Cancels an entire job tree from root to leaves.
-- Use when a failure invalidates all work in the tree.
newtype TreeCancelException = TreeCancelException Text
  deriving stock (Eq, Generic, Show)

instance Exception TreeCancelException where
  displayException (TreeCancelException msg) = T.unpack msg

-- | Cancels the current branch (parent + all siblings).
-- If the parent has a grandparent, the grandparent is resumed.
newtype BranchCancelException = BranchCancelException Text
  deriving stock (Eq, Generic, Show)

instance Exception BranchCancelException where
  displayException (BranchCancelException msg) = T.unpack msg

-- | Reprocess the job later without recording a failure (a soft nack). The
-- worker skips retry\/DLQ, hands back the attempt the claim consumed, and leaves
-- the job to become visible again.
data JobNackException = JobNackException
  deriving stock (Eq, Generic, Show)

instance Exception JobNackException where
  displayException JobNackException = "job nacked for reprocessing"

-- | Row decoding failure (engine-internal). Classified as a permanent failure
-- by the worker.
newtype ParsingException = ParsingException Text
  deriving stock (Eq, Generic, Show)

instance Exception ParsingException where
  displayException (ParsingException msg) = T.unpack msg

-- | Generic engine-internal failure (e.g. missing connection, bad params).
newtype InternalException = InternalException Text
  deriving stock (Eq, Generic, Show)

instance Exception InternalException where
  displayException (InternalException msg) = T.unpack msg

-- | Job was deleted or reclaimed between claim and ack. The worker recognizes
-- this signal and skips retry/DLQ.
-- The ids are the jobs actually gone, empty when the thrower did not name them.
data JobNotFoundException = JobNotFoundException Text [Int64]
  deriving stock (Eq, Generic, Show)

instance Exception JobNotFoundException where
  displayException (JobNotFoundException msg ids) = T.unpack (msg <> namedJobIds ids)

-- | Heartbeat detected another worker reclaimed the job. The heartbeat retry
-- combinator propagates this signal so the worker can stop duplicate work.
-- The ids are the jobs actually reclaimed, empty when the thrower did not name them.
data JobStolenException = JobStolenException Text [Int64]
  deriving stock (Eq, Generic, Show)

instance Exception JobStolenException where
  displayException (JobStolenException msg ids) = T.unpack (msg <> namedJobIds ids)

-- | Async exception for user-initiated force-cancel, naming the jobs it cancels
-- and any the same check found reclaimed by another worker.
data JobForceCancelled = JobForceCancelled [Int64] [Int64]
  deriving stock (Show)

instance Exception JobForceCancelled where
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

throwRetryable :: (MonadIO m) => Text -> m a
throwRetryable msg = UE.throwIO (Retryable (JobRetryableException msg))

throwPermanent :: (MonadIO m) => Text -> m a
throwPermanent msg = UE.throwIO (Permanent (JobPermanentException msg))

throwTreeCancel :: (MonadIO m) => Text -> m a
throwTreeCancel msg = UE.throwIO (TreeCancel (TreeCancelException msg))

throwBranchCancel :: (MonadIO m) => Text -> m a
throwBranchCancel msg = UE.throwIO (BranchCancel (BranchCancelException msg))

throwNack :: (MonadIO m) => m a
throwNack = UE.throwIO JobNackException

throwParsing :: (MonadIO m) => Text -> m a
throwParsing msg = UE.throwIO (ParsingException msg)

throwInternal :: (MonadIO m) => Text -> m a
throwInternal msg = UE.throwIO (InternalException msg)

-- | Names the jobs that went away, so the worker can tell them from the rest of the batch.
throwJobNotFoundIds :: (MonadIO m) => Text -> [Int64] -> m a
throwJobNotFoundIds msg ids = UE.throwIO (JobNotFoundException msg ids)

throwJobStolen :: (MonadIO m) => Text -> m a
throwJobStolen msg = UE.throwIO (JobStolenException msg [])

-- | 'throwJobStolen' naming the reclaimed jobs, so the worker can tell them from
-- the rest of the batch.
throwJobStolenIds :: (MonadIO m) => [Int64] -> m a
throwJobStolenIds = UE.throwIO . JobStolenException "reclaimed by another worker"

-- | The ids a signal names, appended to its message. Empty when it names none.
namedJobIds :: [Int64] -> Text
namedJobIds [] = ""
namedJobIds ids = ": " <> T.intercalate ", " (map (T.pack . show) ids)

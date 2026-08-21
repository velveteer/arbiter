{-# LANGUAGE OverloadedStrings #-}

-- | Exceptions thrown by job handlers and by the worker engine.
--
-- 'JobException' is the decision a handler throws to say how its failure should be
-- settled: retry, DLQ, or cancel the tree or branch. 'JobNackException' asks for a
-- reprocess with no failure recorded. The engine's own signals are separate types rather
-- than constructors of that sum, and a user handler never throws them.
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
  , JobGoneException (..)
  , JobForceCancelled (..)

    -- * Helpers
  , throwRetryable
  , throwPermanent
  , throwTreeCancel
  , throwBranchCancel
  , throwNack
  , throwParsing
  , throwInternal
  , throwJobGone
  , throwJobGoneIds
  , namedJobIds
  , displayEx
  ) where

import Control.Exception (Exception (..), SomeException (..), asyncExceptionFromException, asyncExceptionToException)
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
  backtraceDesired _ = False
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

-- | Cancel a whole job tree, root to leaves, for a failure that invalidates all of it.
newtype TreeCancelException = TreeCancelException Text
  deriving stock (Eq, Generic, Show)

instance Exception TreeCancelException where
  displayException (TreeCancelException msg) = T.unpack msg

-- | Cancel this branch: the parent and every sibling. A grandparent above it is resumed.
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
  backtraceDesired _ = False
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

-- | Job was deleted or reclaimed between claim and ack. The worker recognizes this
-- signal and skips retry/DLQ. The message is the reason reported to
-- 'Arbiter.Core.Job.Types.onJobUnavailable'. The ids are the jobs actually gone,
-- empty when the thrower did not name them.
data JobGoneException = JobGoneException Text [Int64]
  deriving stock (Eq, Generic, Show)

instance Exception JobGoneException where
  backtraceDesired _ = False
  displayException (JobGoneException msg ids) = T.unpack (msg <> namedJobIds ids)

-- | Async exception for user-initiated force-cancel, naming the jobs it cancels
-- and any the same check found reclaimed by another worker.
data JobForceCancelled = JobForceCancelled [Int64] [Int64]
  deriving stock (Show)

instance Exception JobForceCancelled where
  backtraceDesired _ = False
  toException = asyncExceptionToException
  fromException = asyncExceptionFromException

-- | Fail the job and let it retry.
throwRetryable :: (MonadIO m) => Text -> m a
throwRetryable msg = UE.throwIO (Retryable (JobRetryableException msg))

-- | Fail the job straight to the DLQ.
throwPermanent :: (MonadIO m) => Text -> m a
throwPermanent msg = UE.throwIO (Permanent (JobPermanentException msg))

-- | Cancel the whole job tree.
throwTreeCancel :: (MonadIO m) => Text -> m a
throwTreeCancel msg = UE.throwIO (TreeCancel (TreeCancelException msg))

-- | Cancel this job and its descendants.
throwBranchCancel :: (MonadIO m) => Text -> m a
throwBranchCancel msg = UE.throwIO (BranchCancel (BranchCancelException msg))

-- | Return the job to the queue without consuming an attempt.
throwNack :: (MonadIO m) => m a
throwNack = UE.throwIO JobNackException

-- | Fail on an undecodable payload.
throwParsing :: (MonadIO m) => Text -> m a
throwParsing msg = UE.throwIO (ParsingException msg)

-- | Fail on an arbiter-internal error.
throwInternal :: (MonadIO m) => Text -> m a
throwInternal msg = UE.throwIO (InternalException msg)

-- | Names the jobs that went away, so the worker can tell them from the rest of the batch.
throwJobGoneIds :: (MonadIO m) => Text -> [Int64] -> m a
throwJobGoneIds msg ids = UE.throwIO (JobGoneException msg ids)

-- | Signal that the claim is no longer valid, naming no ids.
throwJobGone :: (MonadIO m) => Text -> m a
throwJobGone msg = throwJobGoneIds msg []

-- | The ids a signal names, appended to its message. Empty when it names none.
namedJobIds :: [Int64] -> Text
namedJobIds [] = ""
namedJobIds ids = ": " <> T.intercalate ", " (map (T.pack . show) ids)

-- | An exception's message. Unwraps first, so the reported text carries none of
-- the backtrace base attaches to 'SomeException'.
displayEx :: SomeException -> Text
displayEx (SomeException e) = T.pack (displayException e)

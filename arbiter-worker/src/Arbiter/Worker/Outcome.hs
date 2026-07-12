{-# LANGUAGE OverloadedStrings #-}

-- | How a job ends: acked, dead-lettered, or retried.
module Arbiter.Worker.Outcome
  ( RetryPolicy (..)
  , defaultRetryPolicy
  , FailureOutcome (..)
  , FailureReport (..)
  , failJob
  , completeJob
  , storeResult
  , failureHookCalls
  ) where

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (Value, toJSON)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Time (NominalDiffTime, UTCTime)

import Arbiter.Worker.BackoffStrategy
  ( BackoffStrategy
  , Jitter (..)
  , applyJitter
  , calculateBackoff
  , exponentialBackoff
  )

-- | How a queue spaces out its retries.
data RetryPolicy = RetryPolicy
  { retryBackoff :: BackoffStrategy
  , retryJitter :: Jitter
  }

-- | Exponential from 2s, capped at ~12 days, with equal jitter.
defaultRetryPolicy :: RetryPolicy
defaultRetryPolicy = RetryPolicy {retryBackoff = exponentialBackoff 2.0 1_048_576, retryJitter = EqualJitter}

data FailureOutcome
  = DeadLettered
  | Retried NominalDiffTime
  deriving stock (Eq, Show)

-- | What the failure write did. Zero 'reportRows' means the job had already moved on.
data FailureReport = FailureReport
  { reportOutcome :: FailureOutcome
  , reportRows :: Int64
  }

-- | The hooks a committed failure fires, in order, each with its name for logging.
failureHookCalls
  :: (Job.JobPayload payload)
  => Job.ObservabilityHooks m payload
  -> Job.JobRead payload
  -> Text
  -- ^ Error recorded on the job.
  -> UTCTime
  -- ^ When the lease was taken.
  -> UTCTime
  -- ^ When it failed.
  -> FailureOutcome
  -> [(Text, m ())]
failureHookCalls hooks job errorMsg startTime endTime outcome =
  ("onJobFailure", Job.onJobFailure hooks job errorMsg startTime endTime) : case outcome of
    DeadLettered -> [("onJobFailedAndMovedToDLQ", Job.onJobFailedAndMovedToDLQ hooks errorMsg job)]
    Retried backoff -> [("onJobRetry", Job.onJobRetry hooks job backoff)]

-- | Store the job's rollup result, then ack it. Zero rows means it was reclaimed.
completeJob
  :: (MonadArbiter m)
  => SchemaName
  -> Maybe Value
  -- ^ The result to store, when the job has a parent to roll up into.
  -> JobRead payload
  -> m Int64
completeJob schemaName mResult job = do
  storeResult schemaName mResult job
  Ops.ackJob schemaName (Job.queueName job) job

-- | Store a job's result for its parent rollup, if it has both.
storeResult :: (MonadArbiter m) => SchemaName -> Maybe Value -> JobRead payload -> m ()
storeResult schemaName mResult job =
  traverse_
    (\(parent, value) -> void (Ops.insertResult schemaName (Job.queueName job) parent (Job.primaryKey job) value))
    ((,) <$> Job.parentId job <*> mResult)

-- | Dead-letter the job when the failure is permanent or its attempts are spent, otherwise
-- schedule a retry. Fires no hooks.
failJob
  :: (MonadArbiter m)
  => SchemaName
  -> RetryPolicy
  -> Maybe NominalDiffTime
  -- ^ A delay the caller chose, used instead of the policy's.
  -> Bool
  -- ^ The failure is permanent: dead-letter it whatever its attempts.
  -> Int32
  -- ^ Attempts this job is allowed.
  -> Text
  -- ^ Error to record on the job.
  -> JobRead payload
  -> m FailureReport
failJob schemaName policy mDelay permanent maxAtts errorMsg job
  | permanent || Job.attempts job >= maxAtts = do
      -- Snapshot into parent_state before the DLQ move (survives CASCADE delete).
      when (Job.isRollup job) $ do
        (results, failures, mSnapshot, _dlqFailures) <- Ops.readChildResultsRaw schemaName queue jobId
        let merged = Ops.mergeRawChildResults results failures mSnapshot
        unless (Map.null merged) $
          void $
            Ops.persistParentState schemaName queue jobId (toJSON merged)
      rows <- Ops.moveToDLQ schemaName queue errorMsg job
      pure FailureReport {reportOutcome = DeadLettered, reportRows = rows}
  | otherwise = do
      delay <- maybe (liftIO (applyJitter (retryJitter policy) baseDelay)) pure mDelay
      rows <- Ops.updateJobForRetry schemaName queue delay errorMsg job
      pure FailureReport {reportOutcome = Retried delay, reportRows = rows}
  where
    queue = Job.queueName job
    jobId = Job.primaryKey job
    baseDelay = calculateBackoff (retryBackoff policy) (Job.attempts job)

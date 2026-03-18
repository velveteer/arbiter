{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Core.Job.Types
  ( -- * Core Job Type
    Job (..)
  , JobRead
  , JobWrite
  , defaultJob
  , defaultGroupedJob

    -- * Type Constraints
  , JobPayload

    -- * Deduplication
  , DedupKey (..)

    -- * Observability
  , ObservabilityHooks (..)
  , defaultObservabilityHooks
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), object, withObject, (.:), (.=))
import Data.Aeson.Types (Parser)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Time (NominalDiffTime, UTCTime)
import GHC.Generics (Generic)

-- | The core t'Job' type, representing a unit of work in the queue.
data Job payload key q insertedAt = Job
  { primaryKey :: key
  -- ^ The job's primary key (unit or @Int64@).
  , payload :: payload
  -- ^ The user-defined payload, stored as JSONB.
  , queueName :: q
  -- ^ The name of the queue (table) this job belongs to.
  -- This is a read-only virtual field, it does not get serialized.
  , groupKey :: Maybe Text
  -- ^ An optional key for grouping jobs to be processed serially.
  -- Jobs with the same group key are processed in order.
  -- Use @Nothing@ for ungrouped jobs that can run in parallel.
  , insertedAt :: insertedAt
  -- ^ The time the job was inserted (unit or @UTCTime@).
  , updatedAt :: Maybe UTCTime
  -- ^ The time the job was last updated.
  , attempts :: Int32
  -- ^ The number of times this job has been attempted.
  , lastError :: Maybe Text
  -- ^ The error message from the last failed attempt.
  , priority :: Int32
  -- ^ The job's priority. Lower numbers are higher priority.
  , lastAttemptedAt :: Maybe UTCTime
  -- ^ The time this job was last claimed by a worker.
  , notVisibleUntil :: Maybe UTCTime
  -- ^ When this job becomes visible for claiming.
  , dedupKey :: Maybe DedupKey
  -- ^ The deduplication strategy for this job.
  , maxAttempts :: Maybe Int32
  -- ^ Override the global maxAttempts config for this specific job.
  -- If @Nothing@, uses the worker config's global @maxAttempts@ value.
  , parentId :: Maybe Int64
  -- ^ Optional parent job ID for job dependencies.
  -- When set, this job is a child of the specified parent.
  -- The parent is automatically suspended when acked if children exist,
  -- and resumed for a completion round when the last child finishes.
  , isRollup :: Bool
  -- ^ Whether this job is a rollup finalizer (has children whose results
  -- are collected). Set by 'rollup' / '<~~'. When @True@, the worker
  -- passes child results as a typed argument to the handler.
  , suspended :: Bool
  -- ^ Whether this job is suspended (not claimable).
  -- @TRUE@ for: finalizers waiting for children to complete,
  -- or operator-paused jobs.
  }
  deriving stock (Eq, Generic, Show)

-- | Creates an ungrouped 'JobWrite' with default values.
--
-- Jobs created with this function can be processed in parallel by multiple workers.
-- For serial processing within a group, use 'defaultGroupedJob'.
defaultJob :: payload -> JobWrite payload
defaultJob p =
  Job
    { primaryKey = ()
    , payload = p
    , queueName = ()
    , groupKey = Nothing
    , insertedAt = ()
    , updatedAt = Nothing
    , attempts = 0
    , lastError = Nothing
    , priority = 0
    , lastAttemptedAt = Nothing
    , notVisibleUntil = Nothing
    , dedupKey = Nothing
    , maxAttempts = Nothing
    , parentId = Nothing
    , isRollup = False
    , suspended = False
    }

-- | Creates a grouped 'JobWrite' with default values.
--
-- Jobs with the same group key are processed serially (head-of-line blocking),
-- ensuring that only one job per group is processed at a time.
-- Use for operations that must be ordered, like processing events for a specific user.
--
-- @
-- let job = defaultGroupedJob "user-123" (MyPayload data)
-- @
defaultGroupedJob :: Text -> payload -> JobWrite payload
defaultGroupedJob gk p =
  Job
    { primaryKey = ()
    , payload = p
    , queueName = ()
    , groupKey = Just gk
    , insertedAt = ()
    , updatedAt = Nothing
    , attempts = 0
    , lastError = Nothing
    , priority = 0
    , lastAttemptedAt = Nothing
    , notVisibleUntil = Nothing
    , dedupKey = Nothing
    , maxAttempts = Nothing
    , parentId = Nothing
    , isRollup = False
    , suspended = False
    }

-- | A type alias for a job that has been read from the database.
type JobRead payload = Job payload Int64 Text UTCTime

-- | A type alias for a job that is ready to be written to the database.
-- It does not yet have an ID or insertion timestamp.
type JobWrite payload = Job payload () () ()

-- | Constraint for types that can be used as a job payload.
--
-- Payloads are serialized to and from JSON for storage in the database.
-- The table name is looked up from the registry via @TableForPayload@.
type JobPayload payload = (FromJSON payload, ToJSON payload)

-- | Defines the deduplication strategy for a job upon insertion.
data DedupKey
  = -- | On conflict, keep the existing job. Corresponds to @ON CONFLICT DO NOTHING@.
    IgnoreDuplicate Text
  | -- | On conflict, replace the existing job with the new one. Corresponds to @ON CONFLICT DO UPDATE@.
    ReplaceDuplicate Text
  deriving stock (Eq, Generic, Show)

instance ToJSON DedupKey where
  toJSON (IgnoreDuplicate k) = object ["key" .= k, "strategy" .= ("ignore" :: Text)]
  toJSON (ReplaceDuplicate k) = object ["key" .= k, "strategy" .= ("replace" :: Text)]

instance FromJSON DedupKey where
  parseJSON = withObject "DedupKey" $ \v -> do
    key <- v .: "key"
    strategy <- v .: "strategy" :: Parser Text
    case strategy of
      "ignore" -> pure $ IgnoreDuplicate key
      "replace" -> pure $ ReplaceDuplicate key
      _ -> fail $ "Unknown dedup strategy: " <> show strategy

-- | A set of callbacks invoked at key points in the job lifecycle.
--
-- Use these hooks to integrate with metrics, logging, or tracing systems.
-- Hooks are exception-safe; any exception thrown within a hook is caught
-- and ignored to prevent crashing the worker.
data ObservabilityHooks m payload = ObservabilityHooks
  { onJobClaimed
      :: (JobPayload payload)
      => JobRead payload
      -> UTCTime
      -- \^ Claim time.
      -> m ()
  -- ^ Called immediately after a job is claimed by a worker.
  , onJobSuccess
      :: (JobPayload payload)
      => JobRead payload
      -> UTCTime
      -- \^ Start time.
      -> UTCTime
      -- \^ End time.
      -> m ()
  -- ^ Called after a job handler succeeds. Use @diffUTCTime@ on the timestamps
  -- to calculate job duration.
  , onJobFailure
      :: (JobPayload payload)
      => JobRead payload
      -> Text
      -- \^ Error message.
      -> UTCTime
      -- \^ Start time.
      -> UTCTime
      -- \^ End time.
      -> m ()
  -- ^ Called after a job handler fails. Use @diffUTCTime@ on the timestamps
  -- to calculate job duration.
  , onJobRetry
      :: (JobPayload payload)
      => JobRead payload
      -> NominalDiffTime
      -- \^ The backoff delay until the job becomes visible again.
      -> m ()
  -- ^ Called when a failed job is successfully scheduled for retry.
  , onJobFailedAndMovedToDLQ
      :: (JobPayload payload)
      => Text
      -- \^ Error message.
      -> JobRead payload
      -> m ()
  -- ^ Called when a job is successfully moved to the dead-letter queue.
  , onJobHeartbeat
      :: (JobPayload payload)
      => JobRead payload
      -> UTCTime
      -- \^ Current time.
      -> UTCTime
      -- \^ Start time.
      -> m ()
  -- ^ Called periodically for a running job.
  }

-- | A default set of t'ObservabilityHooks' that do nothing.
--
-- This can be used as a starting point for your own custom hooks.
--
-- @
-- myHooks = defaultObservabilityHooks
--   { onJobSuccess = \\job startTime endTime -> do
--       let duration = diffUTCTime endTime startTime
--       logInfo $ "Job " <> show (primaryKey job) <> " succeeded in " <> show duration
--   }
-- @
defaultObservabilityHooks :: (Applicative m) => ObservabilityHooks m payload
defaultObservabilityHooks =
  ObservabilityHooks
    { onJobClaimed = \_ _ -> pure ()
    , onJobSuccess = \_ _ _ -> pure ()
    , onJobFailure = \_ _ _ _ -> pure ()
    , onJobRetry = \_ _ -> pure ()
    , onJobFailedAndMovedToDLQ = \_ _ -> pure ()
    , onJobHeartbeat = \_ _ _ -> pure ()
    }

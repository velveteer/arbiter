{-# LANGUAGE NoFieldSelectors #-}

module Arbiter.Core.Job.Types.Internal
  ( JobRecord (..)
  , primaryKey
  , payload
  , queueName
  , groupKey
  , insertedAt
  , updatedAt
  , attempts
  , lastError
  , priority
  , lastAttemptedAt
  , notVisibleUntil
  , dedupKey
  , maxAttempts
  , parentId
  , parentState
  , traceContext
  , suspended
  , claimedBy
  , claimSeq
  , archiveFor
  , payloadKeys
  ) where

import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)

import Arbiter.Core.Job.Dedup (DedupKey)
import Arbiter.Core.Job.TraceContext (TraceContext)

-- | Internal representation shared by writable and stored jobs.
data JobRecord payload key q insertedAt adm = Job
  { primaryKey :: key
  , payload :: payload
  , queueName :: q
  , groupKey :: Maybe Text
  , insertedAt :: insertedAt
  , updatedAt :: Maybe UTCTime
  , attempts :: Int32
  , lastError :: Maybe Text
  , priority :: Int32
  , lastAttemptedAt :: Maybe UTCTime
  , notVisibleUntil :: Maybe UTCTime
  , dedupKey :: Maybe DedupKey
  , maxAttempts :: Maybe Int32
  , parentId :: Maybe Int64
  , parentState :: Maybe Value
  , traceContext :: Maybe TraceContext
  , suspended :: Bool
  , claimedBy :: Maybe UUID
  , claimSeq :: Int64
  , archiveFor :: Maybe Int32
  , payloadKeys :: adm
  }
  deriving stock (Eq, Show)

-- | Database-assigned identifier for a stored job.
primaryKey :: JobRecord payload key q insertedAt adm -> key
primaryKey Job {primaryKey = value} = value

-- | User-defined payload stored as JSONB.
payload :: JobRecord payload key q insertedAt adm -> payload
payload Job {payload = value} = value

-- | Queue containing a stored job.
queueName :: JobRecord payload key q insertedAt adm -> q
queueName Job {queueName = value} = value

-- | Serial-processing group, or 'Nothing' for an ungrouped job.
groupKey :: JobRecord payload key q insertedAt adm -> Maybe Text
groupKey Job {groupKey = value} = value

-- | Time at which the job was inserted.
insertedAt :: JobRecord payload key q insertedAt adm -> insertedAt
insertedAt Job {insertedAt = value} = value

-- | Time at which the job was last updated.
updatedAt :: JobRecord payload Int64 q insertedAt adm -> Maybe UTCTime
updatedAt Job {updatedAt = value} = value

-- | Number of attempts made so far.
attempts :: JobRecord payload Int64 q insertedAt adm -> Int32
attempts Job {attempts = value} = value

-- | Error message from the last failed attempt.
lastError :: JobRecord payload Int64 q insertedAt adm -> Maybe Text
lastError Job {lastError = value} = value

-- | Claim priority. Lower numbers have higher priority.
priority :: JobRecord payload key q insertedAt adm -> Int32
priority Job {priority = value} = value

-- | Time at which a worker last claimed the job.
lastAttemptedAt :: JobRecord payload Int64 q insertedAt adm -> Maybe UTCTime
lastAttemptedAt Job {lastAttemptedAt = value} = value

-- | Earliest time at which the job can be claimed.
notVisibleUntil :: JobRecord payload key q insertedAt adm -> Maybe UTCTime
notVisibleUntil Job {notVisibleUntil = value} = value

-- | Deduplication strategy and key.
dedupKey :: JobRecord payload key q insertedAt adm -> Maybe DedupKey
dedupKey Job {dedupKey = value} = value

-- | Attempt limit before the job moves to the DLQ.
maxAttempts :: JobRecord payload key q insertedAt adm -> Maybe Int32
maxAttempts Job {maxAttempts = value} = value

-- | Identifier of this job's parent in a job tree.
parentId :: JobRecord payload Int64 q insertedAt adm -> Maybe Int64
parentId Job {parentId = value} = value

-- | Snapshot of accumulated child results for a rollup finalizer.
parentState :: JobRecord payload Int64 q insertedAt adm -> Maybe Value
parentState Job {parentState = value} = value

-- | W3C trace context captured at enqueue.
traceContext :: JobRecord payload key q insertedAt adm -> Maybe TraceContext
traceContext Job {traceContext = value} = value

-- | Whether the job is currently ineligible for claiming.
suspended :: JobRecord payload Int64 q insertedAt adm -> Bool
suspended Job {suspended = value} = value

-- | Worker pool that most recently claimed the job.
claimedBy :: JobRecord payload Int64 q insertedAt adm -> Maybe UUID
claimedBy Job {claimedBy = value} = value

-- | Monotonically increasing claim identifier.
claimSeq :: JobRecord payload Int64 q insertedAt adm -> Int64
claimSeq Job {claimSeq = value} = value

-- | Completed-job archive retention in seconds.
archiveFor :: JobRecord payload key q insertedAt adm -> Maybe Int32
archiveFor Job {archiveFor = value} = value

-- | The labels and keys stamped from the payload at enqueue.
payloadKeys :: JobRecord payload key q insertedAt adm -> adm
payloadKeys Job {payloadKeys = value} = value

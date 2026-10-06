{-# LANGUAGE NoFieldSelectors #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- The job record type and its field accessors.
module Arbiter.Core.Job.Types.Internal
  ( JobRecord (..)
  , Job
  , Stored (..)
  , storedBytes
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

import Data.Aeson (FromJSON (..), ToJSON (..), Value (Null), decodeStrict, encode)
import Data.Aeson.Encoding (unsafeToEncoding)
import Data.ByteString (ByteString)
import Data.ByteString.Builder (byteString)
import Data.ByteString.Lazy qualified as BL
import Data.Int (Int32, Int64)
import Data.Kind (Type)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Time (UTCTime)
import Data.UUID.Types (UUID)

import Arbiter.Core.Job.Dedup (DedupKey)
import Arbiter.Core.Job.TraceContext (TraceContext)

-- | A payload as the JSON bytes the database holds. The type it decodes to is a phantom.
newtype Stored (payload :: Type) = Stored ByteString
  deriving stock (Eq, Show)

-- | The JSON bytes of a stored payload.
storedBytes :: Stored payload -> ByteString
storedBytes (Stored bytes) = bytes

-- | The bytes are emitted as they are. 'toJSON' has to parse them back.
instance ToJSON (Stored payload) where
  toJSON = fromMaybe Null . decodeStrict . storedBytes
  toEncoding = unsafeToEncoding . byteString . storedBytes

instance FromJSON (Stored payload) where
  parseJSON = pure . Stored . BL.toStrict . encode

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

-- | A job parametrized over payload, primary key, queue name, insertion
-- timestamp, and the columns derived from the payload. The constructor is internal.
type Job payload key q insertedAt adm =
  JobRecord payload key q insertedAt adm

-- | Database-assigned identifier for a stored job.
primaryKey :: Job payload key q insertedAt adm -> key
primaryKey Job {primaryKey = value} = value

-- | User-defined payload stored as JSONB.
payload :: Job payload key q insertedAt adm -> payload
payload Job {payload = value} = value

-- | Queue containing a stored job.
queueName :: Job payload key q insertedAt adm -> q
queueName Job {queueName = value} = value

-- | Serial-processing group, or 'Nothing' for an ungrouped job.
groupKey :: Job payload key q insertedAt adm -> Maybe Text
groupKey Job {groupKey = value} = value

-- | Time at which the job was inserted.
insertedAt :: Job payload key q insertedAt adm -> insertedAt
insertedAt Job {insertedAt = value} = value

-- | Time at which the job was last updated.
updatedAt :: Job payload Int64 q insertedAt adm -> Maybe UTCTime
updatedAt Job {updatedAt = value} = value

-- | Number of attempts made so far.
attempts :: Job payload Int64 q insertedAt adm -> Int32
attempts Job {attempts = value} = value

-- | Error message from the last failed attempt.
lastError :: Job payload Int64 q insertedAt adm -> Maybe Text
lastError Job {lastError = value} = value

-- | Claim priority. Lower numbers have higher priority. In a group, a retried job goes first.
priority :: Job payload key q insertedAt adm -> Int32
priority Job {priority = value} = value

-- | Time at which a worker last claimed the job.
lastAttemptedAt :: Job payload Int64 q insertedAt adm -> Maybe UTCTime
lastAttemptedAt Job {lastAttemptedAt = value} = value

-- | Earliest time at which the job can be claimed.
notVisibleUntil :: Job payload key q insertedAt adm -> Maybe UTCTime
notVisibleUntil Job {notVisibleUntil = value} = value

-- | Deduplication strategy and key.
dedupKey :: Job payload key q insertedAt adm -> Maybe DedupKey
dedupKey Job {dedupKey = value} = value

-- | Attempt limit before the job moves to the DLQ.
maxAttempts :: Job payload key q insertedAt adm -> Maybe Int32
maxAttempts Job {maxAttempts = value} = value

-- | Identifier of this job's parent in a job tree.
parentId :: Job payload Int64 q insertedAt adm -> Maybe Int64
parentId Job {parentId = value} = value

-- | Snapshot of accumulated child results for a rollup finalizer.
parentState :: Job payload Int64 q insertedAt adm -> Maybe Value
parentState Job {parentState = value} = value

-- | W3C trace context captured at enqueue.
traceContext :: Job payload key q insertedAt adm -> Maybe TraceContext
traceContext Job {traceContext = value} = value

-- | Whether the job is suspended. It stays unclaimable until resumed.
suspended :: Job payload Int64 q insertedAt adm -> Bool
suspended Job {suspended = value} = value

-- | Holder of the outstanding claim. 'Nothing' when no claim is outstanding.
claimedBy :: Job payload Int64 q insertedAt adm -> Maybe UUID
claimedBy Job {claimedBy = value} = value

-- | Monotonically increasing claim identifier.
claimSeq :: Job payload Int64 q insertedAt adm -> Int64
claimSeq Job {claimSeq = value} = value

-- | Completed-job archive retention in seconds.
archiveFor :: Job payload key q insertedAt adm -> Maybe Int32
archiveFor Job {archiveFor = value} = value

-- | The labels and keys stamped from the payload at enqueue.
payloadKeys :: Job payload key q insertedAt adm -> adm
payloadKeys Job {payloadKeys = value} = value

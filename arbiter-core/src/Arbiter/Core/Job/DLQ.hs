-- | Dead Letter Queue (DLQ) job type.
module Arbiter.Core.Job.DLQ
  ( DLQJob (..)
  , JobSnapshot
  ) where

import Data.Int (Int64)
import Data.Text (Text)
import Data.Time (UTCTime)
import GHC.Generics (Generic)

import Arbiter.Core.Job.Types (Job)

-- | A snapshot of a job's complete state when it was moved to the DLQ.
type JobSnapshot payload = Job payload Int64 Text UTCTime

-- | Represents a job that has failed all its retry attempts and has been
-- moved to the dead-letter queue for manual inspection.
data DLQJob payload = DLQJob
  { dlqPrimaryKey :: Int64
  -- ^ The primary key of the entry in the DLQ table
  , failedAt :: UTCTime
  -- ^ The time the job was moved to the DLQ.
  , jobSnapshot :: JobSnapshot payload
  -- ^ The complete job state when it failed.
  }
  deriving stock (Eq, Generic, Show)

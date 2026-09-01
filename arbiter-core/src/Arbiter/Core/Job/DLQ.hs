-- | Dead-letter queue types. Arbiter moves jobs here after their last retry.
-- Recover a job with 'Arbiter.Core.HighLevel.retryFromDLQ' or delete it with
-- 'Arbiter.Core.HighLevel.deleteDLQJob'.
module Arbiter.Core.Job.DLQ
  ( DLQJob (..)
  , JobSnapshot
  ) where

import Data.Int (Int64)
import Data.Time (UTCTime)
import GHC.Generics (Generic)

import Arbiter.Core.Job.Types (JobRead)

-- | Full job state at the time of DLQ insertion.
type JobSnapshot payload = JobRead payload

-- | A job in the dead-letter queue.
data DLQJob payload = DLQJob
  { dlqPrimaryKey :: Int64
  -- ^ DLQ table primary key, distinct from the snapshot's own job id
  , failedAt :: UTCTime
  -- ^ When the job was moved to the DLQ
  , jobSnapshot :: JobSnapshot payload
  -- ^ Full job state at time of failure (payload, attempts, last_error, etc.).
  -- For DLQ rollup finalizers, 'Arbiter.Core.Job.Types.parentState' in the snapshot contains the
  -- accumulated child results captured before the cascade delete.
  }
  deriving stock (Eq, Generic, Show)

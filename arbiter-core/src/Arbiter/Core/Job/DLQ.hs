-- | Dead-letter queue types. Jobs land here after exhausting retries.
-- Recover with 'Arbiter.Core.HighLevel.retryFromDLQ' or delete with
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
  -- ^ DLQ table primary key (distinct from the original job ID in the snapshot)
  , failedAt :: UTCTime
  -- ^ When the job was moved to the DLQ
  , jobSnapshot :: JobSnapshot payload
  -- ^ Full job state at time of failure (payload, attempts, last_error, etc.).
  -- For DLQ'd rollup finalizers, 'parentState' on the snapshot carries the
  -- accumulated child results captured before the cascade delete.
  }
  deriving stock (Eq, Generic, Show)

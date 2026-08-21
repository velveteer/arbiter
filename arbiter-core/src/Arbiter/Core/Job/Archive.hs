-- | Completed-job archive types. An acked job with a positive @archiveFor@ lands
-- here and is purged once its retention expires. Entries can be re-run via
-- 'Arbiter.Core.HighLevel.reEnqueueFromArchive'.
module Arbiter.Core.Job.Archive
  ( ArchiveJob (..)
  ) where

import Data.Aeson (Value)
import Data.Int (Int64)
import Data.Time (UTCTime)
import GHC.Generics (Generic)

import Arbiter.Core.Job.DLQ (JobSnapshot)

-- | A completed job in the archive.
data ArchiveJob payload = ArchiveJob
  { archivePrimaryKey :: Int64
  -- ^ Archive table primary key, distinct from the snapshot's own job id
  , completedAt :: UTCTime
  -- ^ When the job was acked and archived
  , jobSnapshot :: JobSnapshot payload
  -- ^ Full job state at time of completion (payload, attempts, etc.)
  , archivedResult :: Maybe Value
  -- ^ Handler result stored for a completed root job (one with no parent).
  }
  deriving stock (Eq, Generic, Show)

{-# LANGUAGE OverloadedStrings #-}

-- | Status values derived from a job row by the queue status SQL.
module Arbiter.Core.Job.Status
  ( JobStatus (..)
  , jobStatusToText
  , jobStatusFromText
  ) where

import Data.Aeson (FromJSON (..), ToJSON (..), withText)
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Generics (Generic)

-- | Effective job status. Arbiter derives status from the stored fields. The status SQL
-- in "Arbiter.Core.Sql.Jobs" is its source of truth.
data JobStatus = Ready | InFlight | Backoff | Scheduled | Suspended | Throttled | Cancelled
  deriving stock (Bounded, Enum, Eq, Generic, Show)

-- | The wire name for a status.
jobStatusToText :: JobStatus -> Text
jobStatusToText Ready = "ready"
jobStatusToText InFlight = "in_flight"
jobStatusToText Backoff = "backoff"
jobStatusToText Scheduled = "scheduled"
jobStatusToText Suspended = "suspended"
jobStatusToText Throttled = "throttled"
jobStatusToText Cancelled = "cancelled"

-- | Strict inverse of 'jobStatusToText'. Unknown values are rejected.
jobStatusFromText :: Text -> Either Text JobStatus
jobStatusFromText name =
  maybe (Left ("unknown job status: " <> name)) Right $
    lookup name [(jobStatusToText status, status) | status <- [minBound .. maxBound]]

instance ToJSON JobStatus where
  toJSON = toJSON . jobStatusToText

instance FromJSON JobStatus where
  parseJSON = withText "JobStatus" $ either (fail . T.unpack) pure . jobStatusFromText

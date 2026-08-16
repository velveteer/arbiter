-- | Public worker API. Single-pool execution and multi-pool orchestration are
-- implemented separately in "Arbiter.Worker.Pool" and
-- "Arbiter.Worker.MultiQueue".
module Arbiter.Worker
  ( -- * Running workers
    runWorkerPool
  , module Arbiter.Worker.MultiQueue

    -- * Job results
  , module Arbiter.Core.JobResult
  , childResults
  , mergedChildResults
  , mergeChildResults

    -- * Configuration and logging
  , module Arbiter.Worker.Config
  , module Arbiter.Worker.BackoffStrategy
  , module Arbiter.Worker.Logger
  , module Arbiter.Worker.WorkerState

    -- * Reaper
  , runReaperOp

    -- * Cron
  , CronJob (..)
  , OverlapPolicy (..)
  , BackfillPolicy (..)
  , TickKind (..)
  , cronJob
  , cronJobInTimezone
  , initCronSchedules
  , overlapPolicyToText
  , overlapPolicyFromText
  , validateCronScheduleUpdate
  , updateCronScheduleChecked
  ) where

import Arbiter.Core.JobResult

import Arbiter.Worker.BackoffStrategy
import Arbiter.Worker.Config
import Arbiter.Worker.Cron
  ( BackfillPolicy (..)
  , CronJob (..)
  , OverlapPolicy (..)
  , TickKind (..)
  , cronJob
  , cronJobInTimezone
  , initCronSchedules
  , overlapPolicyFromText
  , overlapPolicyToText
  , updateCronScheduleChecked
  , validateCronScheduleUpdate
  )
import Arbiter.Worker.Logger
import Arbiter.Worker.MultiQueue
import Arbiter.Worker.Pool
  ( childResults
  , mergeChildResults
  , mergedChildResults
  , runReaperOp
  , runWorkerPool
  )
import Arbiter.Worker.WorkerState

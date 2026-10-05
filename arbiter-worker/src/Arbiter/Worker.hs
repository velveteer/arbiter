-- | Public worker API: single-pool execution, multi-pool orchestration, job
-- results, configuration, and logging.
module Arbiter.Worker
  ( -- * Running workers
    runWorkerPool
  , module Arbiter.Worker.MultiQueue
  , getEnabledQueues

    -- * Job results
  , module Arbiter.Core.JobResult
  , childResults
  , mergedChildResults
  , mergeChildResults
  , storeJobResult
  , storeEncodedResult

    -- * Configuration and logging
  , module Arbiter.Worker.Config
  , module Arbiter.Worker.BackoffStrategy
  , module Arbiter.Worker.Logger
  , module Arbiter.Worker.WorkerState

    -- * Reaper
  , runMaintenancePass
  , MaintenancePace (..)
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
import Arbiter.Worker.Cron.Scheduler (initCronSchedules)
import Arbiter.Worker.Cron.Types
  ( BackfillPolicy (..)
  , CronJob (..)
  , OverlapPolicy (..)
  , TickKind (..)
  , cronJob
  , cronJobInTimezone
  , overlapPolicyFromText
  , overlapPolicyToText
  , updateCronScheduleChecked
  , validateCronScheduleUpdate
  )
import Arbiter.Worker.EnabledQueues (getEnabledQueues)
import Arbiter.Worker.Logger
import Arbiter.Worker.MultiQueue
import Arbiter.Worker.Pool (runReaperOp, runWorkerPool)
import Arbiter.Worker.Reaper (MaintenancePace (..), runMaintenancePass)
import Arbiter.Worker.Results (childResults, mergeChildResults, mergedChildResults, storeEncodedResult, storeJobResult)
import Arbiter.Worker.WorkerState

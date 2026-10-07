-- | Public worker API: pools, job results, configuration, logging, maintenance,
-- and cron.
module Arbiter.Worker
  ( -- * Running worker pools
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

    -- * Configuration
  , WorkerConfig (..)
  , transactionalWorkerConfig
  , manualWorkerConfig
  , batchedWorkerConfig
  , withHooks
  , withMaintenance
  , HandlerMode (..)
  , handlerBatchSize
  , ResultOf
  , WorkerConfigException (..)
  , validateWorkerConfig
  , module Arbiter.Worker.BackoffStrategy

    -- * Batch callbacks
  , BatchCallbacks (..)
  , hoistBatchCallbacks

    -- * Worker state
  , module Arbiter.Worker.WorkerState
  , WorkerRuntime
  , shutdownWorkerPool
  , getWorkerState
  , getListenerReady
  , readEffectiveState

    -- * Logging
  , LogConfig (..)
  , LogDestination (..)
  , defaultLogConfig
  , silentLogConfig
  , LogLevel (..)
  , Pair
  , (.=)

    -- * Maintenance
  , runMaintenancePass
  , MaintenancePace (..)
  , MaintenanceOp (..)
  , maintenanceOpName

    -- * Cron
  , CronJob (..)
  , OverlapPolicy (..)
  , BackfillPolicy (..)
  , TickKind (..)
  , cronJob
  , cronJobInTimezone
  , overlapPolicyToText
  , overlapPolicyFromText
  , validateCronScheduleUpdate
  , updateCronScheduleChecked
  ) where

import Arbiter.Core.JobResult

import Arbiter.Worker.BackoffStrategy
import Arbiter.Worker.Config
  ( BatchCallbacks (..)
  , HandlerMode (..)
  , MaintenanceOp (..)
  , ResultOf
  , WorkerConfig (..)
  , WorkerConfigException (..)
  , WorkerRuntime
  , batchedWorkerConfig
  , getListenerReady
  , getWorkerState
  , handlerBatchSize
  , hoistBatchCallbacks
  , maintenanceOpName
  , manualWorkerConfig
  , readEffectiveState
  , shutdownWorkerPool
  , transactionalWorkerConfig
  , validateWorkerConfig
  , withHooks
  , withMaintenance
  )
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
  ( LogConfig (..)
  , LogDestination (..)
  , LogLevel (..)
  , Pair
  , defaultLogConfig
  , silentLogConfig
  , (.=)
  )
import Arbiter.Worker.MultiQueue
import Arbiter.Worker.Pool (runWorkerPool)
import Arbiter.Worker.Reaper (MaintenancePace (..), runMaintenancePass)
import Arbiter.Worker.Results (childResults, mergeChildResults, mergedChildResults, storeEncodedResult, storeJobResult)
import Arbiter.Worker.WorkerState

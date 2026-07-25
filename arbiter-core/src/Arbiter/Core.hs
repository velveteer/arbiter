{-# LANGUAGE DuplicateRecordFields #-}

-- | Re-exports commonly used Arbiter functionality.
module Arbiter.Core
  ( -- * Core types
    module Arbiter.Core.Job.DLQ
  , module Arbiter.Core.Job.Types
  , module Arbiter.Core.MonadArbiter
  , module Arbiter.Core.HasArbiterSchema
  , module Arbiter.Core.QueueRegistry

    -- * High-level operations
  , module Arbiter.Core.HighLevel

    -- * Job tree DSL
  , module Arbiter.Core.JobTree

    -- * Job results
  , module Arbiter.Core.JobResult

    -- * Archived jobs
  , ArchiveJob (..)

    -- * Typed codecs and parameters
  , module Arbiter.Core.Codec

    -- * Schema
  , module Arbiter.Core.Job.Schema

    -- * Exceptions
  , module Arbiter.Core.Exceptions

    -- * Cron schedule overrides and worker health

    -- | 'CronScheduleRow' fields (@name@, @enabled@, @queueName@) are too
    -- generically named to re-export: @import Arbiter.Core.CronSchedule@ for
    -- raw field access.
  , CronScheduleRow
  , CronScheduleUpdate (..)
  , effectiveExpression
  , effectiveOverlap
  , effectiveTimezone
  , WorkerHealth (..)
  , workerHealthFromText

    -- * Connection pool settings
  , module Arbiter.Core.PoolConfig

    -- * Listener types
  , Notification (..)
  , Listener
  , RunningHub
  , HubLog (..)
  , withChannels
  , newPoolListener
  , DedicatedListen
  , newDedicatedListen
  , dedicatedListener
  ) where

import Arbiter.Core.Codec
import Arbiter.Core.CronSchedule
  ( CronScheduleRow
  , CronScheduleUpdate (..)
  , effectiveExpression
  , effectiveOverlap
  , effectiveTimezone
  )
import Arbiter.Core.Exceptions
import Arbiter.Core.HasArbiterSchema
import Arbiter.Core.HighLevel
import Arbiter.Core.Job.Archive (ArchiveJob (..))
import Arbiter.Core.Job.DLQ
import Arbiter.Core.Job.Schema
import Arbiter.Core.Job.Types
import Arbiter.Core.JobResult
import Arbiter.Core.JobTree hiding (insertJobTree) -- use HighLevel.insertJobTree
import Arbiter.Core.Listen
  ( DedicatedListen
  , HubLog (..)
  , Listener
  , Notification (..)
  , RunningHub
  , dedicatedListener
  , newDedicatedListen
  , newPoolListener
  , withChannels
  )
import Arbiter.Core.MonadArbiter
import Arbiter.Core.PoolConfig
import Arbiter.Core.QueueRegistry
import Arbiter.Core.Worker (WorkerHealth (..), workerHealthFromText)

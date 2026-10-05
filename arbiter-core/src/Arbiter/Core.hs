{-# LANGUAGE DuplicateRecordFields #-}

-- | Re-exports commonly used Arbiter functionality.
module Arbiter.Core
  ( -- * Core types
    module Arbiter.Core.Job.DLQ
  , module Arbiter.Core.Job.Types
  , module Arbiter.Core.MonadArbiter
  , module Arbiter.Core.QueueRegistry

    -- * High-level operations
  , module Arbiter.Core.HighLevel

    -- * Job tree DSL
  , module Arbiter.Core.JobTree

    -- * Job results
  , module Arbiter.Core.JobResult

    -- * Archived jobs
  , ArchiveJob (..)

    -- * Schema names
  , SchemaName
  , TableName

    -- * Exceptions
  , module Arbiter.Core.Exceptions

    -- * Cron schedule overrides and worker health

    -- | The admin row records share field names with each other and with 'Job'.
    -- Unqualified use needs @DuplicateRecordFields@ or @OverloadedRecordDot@.
  , CronScheduleRow (..)
  , CronScheduleUpdate (..)
  , effectiveExpression
  , effectiveOverlap
  , effectiveTimezone
  , WorkerHealth (..)
  , workerHealthFromText

    -- * Connection pool settings
  , module Arbiter.Core.PoolConfig

    -- * Listener types

    -- | 'Arbiter.Core.Listen.Notification' collides with @Database.PostgreSQL.Simple.Notification@.
    -- Import it from @Arbiter.Core.Listen@.
  , Listener
  , ListenConn (..)
  , HubLog (..)
  , withChannels
  , newListener
  ) where

import Arbiter.Core.CronSchedule
  ( CronScheduleRow (..)
  , CronScheduleUpdate (..)
  , effectiveExpression
  , effectiveOverlap
  , effectiveTimezone
  )
import Arbiter.Core.Exceptions
import Arbiter.Core.HighLevel
import Arbiter.Core.Job.Archive (ArchiveJob (..))
import Arbiter.Core.Job.DLQ
import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.Job.Types hiding (attemptsLeftSQL, defaultMaxAttemptsSQL, minMaxAttemptsSQL)
import Arbiter.Core.JobResult
import Arbiter.Core.JobTree hiding (insertJobTree) -- use HighLevel.insertJobTree
import Arbiter.Core.Listen
  ( HubLog (..)
  , ListenConn (..)
  , Listener
  , newListener
  , withChannels
  )
import Arbiter.Core.MonadArbiter hiding (Query (..), countOr0, countOr0Prepared)
import Arbiter.Core.PoolConfig
import Arbiter.Core.QueueRegistry
import Arbiter.Core.Worker (WorkerHealth (..), workerHealthFromText)

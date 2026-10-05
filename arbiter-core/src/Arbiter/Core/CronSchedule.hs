{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Types for the @cron_schedules@ table.
--
-- Code-defined defaults and user overrides sit in separate columns. Worker startup
-- upserts the defaults. An override survives every deploy.
module Arbiter.Core.CronSchedule
  ( -- * Types
    CronScheduleRow (..)
  , CronScheduleUpdate (..)

    -- * Effective values
  , effectiveExpression
  , effectiveOverlap
  , effectiveTimezone

    -- * DDL
  , cronSchedulesTable
  , cronSchedulesTableName
  , createCronSchedulesTableSQL
  , addTimezoneColumnSQL
  , addQueueNameColumnSQL
  , addRunRequestedColumnSQL
  , addLastManualRunColumnSQL
  ) where

import Control.Applicative ((<|>))
import Data.Aeson
  ( FromJSON (..)
  , ToJSON (..)
  , genericToEncoding
  , genericToJSON
  , withObject
  , (.:?)
  )
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import GHC.Generics (Generic)

import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Json (explicitOptionalField, patchOptions)
import Arbiter.Core.SqlLiterals (quoteIdentifier)

-- | A row from the @cron_schedules@ table.
data CronScheduleRow = CronScheduleRow
  { name :: Text
  -- ^ Schedule name.
  , queueName :: Text
  -- ^ The queue the schedule inserts into.
  , defaultExpression :: Text
  -- ^ Code-defined cron expression.
  , defaultOverlap :: Text
  -- ^ Code-defined overlap policy: @SkipOverlap@ or @AllowOverlap@.
  , defaultTimezone :: Maybe Text
  -- ^ Code-defined IANA tz name. 'Nothing' means UTC.
  , overrideExpression :: Maybe Text
  -- ^ User override. 'Nothing' uses the default.
  , overrideOverlap :: Maybe Text
  -- ^ User override: @SkipOverlap@ or @AllowOverlap@. 'Nothing' uses the default.
  , overrideTimezone :: Maybe Text
  -- ^ User override. 'Nothing' uses the default. To force UTC when the default is
  -- not UTC, set to @\"UTC\"@.
  , enabled :: Bool
  -- ^ A disabled schedule fires no jobs.
  , lastFiredAt :: Maybe UTCTime
  -- ^ The minute of the last tick that fired. 'Nothing' when it never fired.
  , lastCheckedAt :: Maybe UTCTime
  -- ^ How far the scheduler has checked for ticks. 'Nothing' when it never checked.
  , runRequestedAt :: Maybe UTCTime
  -- ^ Manual run awaiting a worker pool. 'Nothing' when none is pending.
  , lastManualRunAt :: Maybe UTCTime
  -- ^ When a manual run last fired a job. 'Nothing' when it never fired.
  , createdAt :: UTCTime
  -- ^ When the row was created.
  , updatedAt :: UTCTime
  -- ^ When the row last changed.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | The override expression if set, else the default.
effectiveExpression :: CronScheduleRow -> Text
effectiveExpression CronScheduleRow {defaultExpression = def, overrideExpression = mOvr} = fromMaybe def mOvr

-- | The override overlap policy if set, else the default.
effectiveOverlap :: CronScheduleRow -> Text
effectiveOverlap CronScheduleRow {defaultOverlap = def, overrideOverlap = mOvr} = fromMaybe def mOvr

-- | The override timezone if set, else the default. 'Nothing' means UTC.
effectiveTimezone :: CronScheduleRow -> Maybe Text
effectiveTimezone CronScheduleRow {defaultTimezone = mDef, overrideTimezone = mOvr} = mOvr <|> mDef

-- | A patch over a cron schedule's overrides. 'Nothing' leaves a field unchanged,
-- @Just Nothing@ clears the override back to the default, @Just (Just v)@ sets it.
data CronScheduleUpdate = CronScheduleUpdate
  { overrideExpression :: Maybe (Maybe Text)
  -- ^ The cron expression override.
  , overrideOverlap :: Maybe (Maybe Text)
  -- ^ The overlap policy override.
  , overrideTimezone :: Maybe (Maybe Text)
  -- ^ The timezone override.
  , enabled :: Maybe Bool
  -- ^ The enabled flag. @Just@ sets it.
  }
  deriving stock (Eq, Generic, Show)

instance ToJSON CronScheduleUpdate where
  toJSON = genericToJSON patchOptions
  toEncoding = genericToEncoding patchOptions

-- Plain @.:?@ collapses missing and null for @Maybe (Maybe a)@. 'explicitOptionalField' distinguishes them.
instance FromJSON CronScheduleUpdate where
  parseJSON = withObject "CronScheduleUpdate" $ \obj -> do
    expression <- explicitOptionalField obj "overrideExpression"
    overlap <- explicitOptionalField obj "overrideOverlap"
    timezone <- explicitOptionalField obj "overrideTimezone"
    enabledPatch <- obj .:? "enabled"
    pure
      CronScheduleUpdate
        { overrideExpression = expression
        , overrideOverlap = overlap
        , overrideTimezone = timezone
        , enabled = enabledPatch
        }

-- | Qualified table name for the @cron_schedules@ table.
cronSchedulesTable :: SchemaName -> Text
cronSchedulesTable schemaName = quoteIdentifier schemaName <> "." <> cronSchedulesTableName

-- | Bare name of the cron table, for catalog lookups by relname.
cronSchedulesTableName :: Text
cronSchedulesTableName = "cron_schedules"

-- | DDL for the @cron_schedules@ table.
createCronSchedulesTableSQL :: SchemaName -> Text
createCronSchedulesTableSQL schemaName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> cronSchedulesTable schemaName <> " ("
    , "  name TEXT PRIMARY KEY,"
    , "  default_expression TEXT NOT NULL,"
    , "  default_overlap TEXT NOT NULL CHECK (default_overlap IN ('SkipOverlap', 'AllowOverlap')),"
    , "  override_expression TEXT,"
    , "  override_overlap TEXT CHECK (override_overlap IS NULL OR override_overlap IN ('SkipOverlap', 'AllowOverlap')),"
    , "  enabled BOOLEAN NOT NULL DEFAULT TRUE,"
    , "  last_fired_at TIMESTAMPTZ,"
    , "  last_checked_at TIMESTAMPTZ,"
    , "  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
    , ");"
    ]

-- | Idempotent migration adding the timezone columns to an existing table.
addTimezoneColumnSQL :: SchemaName -> Text
addTimezoneColumnSQL schemaName =
  T.unlines
    [ "ALTER TABLE " <> cronSchedulesTable schemaName <> " ADD COLUMN IF NOT EXISTS default_timezone TEXT;"
    , "ALTER TABLE " <> cronSchedulesTable schemaName <> " ADD COLUMN IF NOT EXISTS override_timezone TEXT;"
    ]

-- | Idempotent migration adding the queue name column to an existing table.
addQueueNameColumnSQL :: SchemaName -> Text
addQueueNameColumnSQL schemaName =
  "ALTER TABLE "
    <> cronSchedulesTable schemaName
    <> " ADD COLUMN IF NOT EXISTS queue_name TEXT NOT NULL DEFAULT 'pre-migration';"

-- | Idempotent migration adding the manual run-request column to an existing table.
addRunRequestedColumnSQL :: SchemaName -> Text
addRunRequestedColumnSQL schemaName =
  "ALTER TABLE " <> cronSchedulesTable schemaName <> " ADD COLUMN IF NOT EXISTS run_requested_at TIMESTAMPTZ;"

-- | Idempotent migration adding the manual last-run column to an existing table.
addLastManualRunColumnSQL :: SchemaName -> Text
addLastManualRunColumnSQL schemaName =
  "ALTER TABLE " <> cronSchedulesTable schemaName <> " ADD COLUMN IF NOT EXISTS last_manual_run_at TIMESTAMPTZ;"

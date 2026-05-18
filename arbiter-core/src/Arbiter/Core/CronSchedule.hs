{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Types for the @cron_schedules@ table.
--
-- The table stores both the code-defined defaults and user overrides separately.
-- On worker init, only the @default_*@ columns are upserted -- user overrides
-- (@override_*@, @enabled@) are preserved.
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
  , createCronSchedulesTableSQL
  , addTimezoneColumnSQL
  ) where

import Control.Applicative ((<|>))
import Data.Aeson (FromJSON (..), ToJSON, withObject, (.:), (.:?))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import GHC.Generics (Generic)

import Arbiter.Core.Job.Schema (quoteIdentifier)

-- | A row from the @cron_schedules@ table.
data CronScheduleRow = CronScheduleRow
  { name :: Text
  , defaultExpression :: Text
  , defaultOverlap :: Text
  , defaultTimezone :: Maybe Text
  -- ^ Code-defined IANA tz name. @NULL@ = UTC.
  , overrideExpression :: Maybe Text
  , overrideOverlap :: Maybe Text
  , overrideTimezone :: Maybe Text
  -- ^ User override. @NULL@ = use default. To force UTC when the default is
  -- not UTC, set to @\"UTC\"@.
  , enabled :: Bool
  , lastFiredAt :: Maybe UTCTime
  , lastCheckedAt :: Maybe UTCTime
  , createdAt :: UTCTime
  , updatedAt :: UTCTime
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Effective expression: override if set, else default.
effectiveExpression :: CronScheduleRow -> Text
effectiveExpression CronScheduleRow {defaultExpression = def, overrideExpression = mOvr} = fromMaybe def mOvr

-- | Effective overlap policy: override if set, else default.
effectiveOverlap :: CronScheduleRow -> Text
effectiveOverlap CronScheduleRow {defaultOverlap = def, overrideOverlap = mOvr} = fromMaybe def mOvr

-- | Effective timezone: override if set, else default. 'Nothing' means UTC.
effectiveTimezone :: CronScheduleRow -> Maybe Text
effectiveTimezone CronScheduleRow {defaultTimezone = mDef, overrideTimezone = mOvr} = mOvr <|> mDef

-- | Patch update for a cron schedule.
--
-- Each field uses @Maybe (Maybe a)@:
--
--   * @Nothing@ = don't change
--   * @Just Nothing@ = reset to default (set column to NULL)
--   * @Just (Just x)@ = set to @x@
data CronScheduleUpdate = CronScheduleUpdate
  { overrideExpression :: Maybe (Maybe Text)
  , overrideOverlap :: Maybe (Maybe Text)
  , overrideTimezone :: Maybe (Maybe Text)
  , enabled :: Maybe Bool
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (ToJSON)

-- @.:?@ can't distinguish missing from null for @Maybe (Maybe a)@ (both
-- yield @Nothing@), so we check key membership first.
instance FromJSON CronScheduleUpdate where
  parseJSON = withObject "CronScheduleUpdate" $ \o -> do
    oe <-
      if KeyMap.member (Key.fromText "overrideExpression") o
        then Just <$> o .: "overrideExpression"
        else pure Nothing
    oo <-
      if KeyMap.member (Key.fromText "overrideOverlap") o
        then Just <$> o .: "overrideOverlap"
        else pure Nothing
    ot <-
      if KeyMap.member (Key.fromText "overrideTimezone") o
        then Just <$> o .: "overrideTimezone"
        else pure Nothing
    en <- o .:? "enabled"
    pure CronScheduleUpdate {overrideExpression = oe, overrideOverlap = oo, overrideTimezone = ot, enabled = en}

-- | Qualified table name for the cron_schedules table.
cronSchedulesTable :: Text -> Text
cronSchedulesTable schemaName = quoteIdentifier schemaName <> ".cron_schedules"

-- | DDL for the @cron_schedules@ table.
createCronSchedulesTableSQL :: Text -> Text
createCronSchedulesTableSQL schemaName =
  T.unlines
    [ "CREATE TABLE IF NOT EXISTS " <> cronSchedulesTable schemaName <> " ("
    , "  name TEXT PRIMARY KEY,"
    , "  default_expression TEXT NOT NULL,"
    , "  default_overlap TEXT NOT NULL CHECK (default_overlap IN ('SkipOverlap', 'AllowOverlap')),"
    , "  default_timezone TEXT,"
    , "  override_expression TEXT,"
    , "  override_overlap TEXT CHECK (override_overlap IS NULL OR override_overlap IN ('SkipOverlap', 'AllowOverlap')),"
    , "  override_timezone TEXT,"
    , "  enabled BOOLEAN NOT NULL DEFAULT TRUE,"
    , "  last_fired_at TIMESTAMPTZ,"
    , "  last_checked_at TIMESTAMPTZ,"
    , "  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
    , ");"
    ]

-- | Idempotent migration adding the timezone columns to an existing table.
addTimezoneColumnSQL :: Text -> Text
addTimezoneColumnSQL schemaName =
  T.unlines
    [ "ALTER TABLE " <> cronSchedulesTable schemaName <> " ADD COLUMN IF NOT EXISTS default_timezone TEXT;"
    , "ALTER TABLE " <> cronSchedulesTable schemaName <> " ADD COLUMN IF NOT EXISTS override_timezone TEXT;"
    ]

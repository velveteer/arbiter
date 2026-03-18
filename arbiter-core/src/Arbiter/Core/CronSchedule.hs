{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Types and @postgresql-simple@ operations for the @cron_schedules@ table.
--
-- Since cron operations are administrative metadata (not part of job processing
-- transactions), they use @postgresql-simple@ directly via a 'Connection'
-- parameter -- no 'MonadArbiter' needed.
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

    -- * Operations
  , upsertCronScheduleDefault
  , listCronSchedules
  , getCronScheduleByName
  , updateCronSchedule
  , touchCronScheduleLastFired
  , deleteStaleSchedules
  , touchCronSchedulesChecked

    -- * SQL helpers
  , cronSchedulesTable
  , createCronSchedulesTableSQL
  ) where

import Data.Aeson (FromJSON (..), ToJSON, withObject, (.:?))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KeyMap
import Data.Int (Int64)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Data.Time (UTCTime)
import Database.PostgreSQL.Simple
  ( Connection
  , Only (..)
  , execute
  , query
  , query_
  )
import Database.PostgreSQL.Simple.FromRow (FromRow)
import Database.PostgreSQL.Simple.ToRow (ToRow)
import Database.PostgreSQL.Simple.Types (In (..), Query (..))
import GHC.Generics (Generic)

import Arbiter.Core.Job.Schema (quoteIdentifier)

-- | A row from the @cron_schedules@ table.
data CronScheduleRow = CronScheduleRow
  { name :: Text
  , defaultExpression :: Text
  , defaultOverlap :: Text
  , overrideExpression :: Maybe Text
  , overrideOverlap :: Maybe Text
  , enabled :: Bool
  , lastFiredAt :: Maybe UTCTime
  , lastCheckedAt :: Maybe UTCTime
  , createdAt :: UTCTime
  , updatedAt :: UTCTime
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, FromRow, ToJSON, ToRow)

-- | Effective expression: override if set, else default.
effectiveExpression :: CronScheduleRow -> Text
effectiveExpression row = fromMaybe row.defaultExpression row.overrideExpression

-- | Effective overlap policy: override if set, else default.
effectiveOverlap :: CronScheduleRow -> Text
effectiveOverlap row = fromMaybe row.defaultOverlap row.overrideOverlap

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
  , enabled :: Maybe Bool
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (ToJSON)

-- | Manual instance to distinguish missing keys from @null@ values.
--
-- * Key missing → @Nothing@ (don't change)
-- * Key present with @null@ → @Just Nothing@ (reset to default)
-- * Key present with value → @Just (Just x)@ (set override)
instance FromJSON CronScheduleUpdate where
  parseJSON = withObject "CronScheduleUpdate" $ \o -> do
    oe <-
      if KeyMap.member (Key.fromText "overrideExpression") o
        then Just <$> o .:? "overrideExpression"
        else pure Nothing
    oo <-
      if KeyMap.member (Key.fromText "overrideOverlap") o
        then Just <$> o .:? "overrideOverlap"
        else pure Nothing
    en <- o .:? "enabled"
    pure
      CronScheduleUpdate
        { overrideExpression = oe
        , overrideOverlap = oo
        , enabled = en
        }

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
    , "  override_expression TEXT,"
    , "  override_overlap TEXT CHECK (override_overlap IS NULL OR override_overlap IN ('SkipOverlap', 'AllowOverlap')),"
    , "  enabled BOOLEAN NOT NULL DEFAULT TRUE,"
    , "  last_fired_at TIMESTAMPTZ,"
    , "  last_checked_at TIMESTAMPTZ,"
    , "  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),"
    , "  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()"
    , ");"
    ]

-- | Upsert a cron schedule's default values.
--
-- INSERT ON CONFLICT (name) DO UPDATE SET default_expression, default_overlap, updated_at.
-- Does NOT touch override_expression, override_overlap, or enabled.
upsertCronScheduleDefault :: Connection -> Text -> Text -> Text -> Text -> IO ()
upsertCronScheduleDefault conn schemaName scheduleName defaultExpr defaultOv = do
  let sql =
        Query . encodeUtf8 $
          "INSERT INTO "
            <> cronSchedulesTable schemaName
            <> " (name, default_expression, default_overlap) VALUES (?, ?, ?)"
            <> " ON CONFLICT (name) DO UPDATE SET"
            <> " default_expression = EXCLUDED.default_expression,"
            <> " default_overlap = EXCLUDED.default_overlap,"
            <> " updated_at = NOW()"
  _ <- execute conn sql (scheduleName, defaultExpr, defaultOv)
  pure ()

-- | List all cron schedules.
listCronSchedules :: Connection -> Text -> IO [CronScheduleRow]
listCronSchedules conn schemaName =
  query_ conn . Query . encodeUtf8 $
    "SELECT name, default_expression, default_overlap,"
      <> " override_expression, override_overlap, enabled,"
      <> " last_fired_at, last_checked_at, created_at, updated_at"
      <> " FROM "
      <> cronSchedulesTable schemaName
      <> " ORDER BY name"

-- | Get a single cron schedule by name.
getCronScheduleByName :: Connection -> Text -> Text -> IO (Maybe CronScheduleRow)
getCronScheduleByName conn schemaName scheduleName = do
  rows <-
    query
      conn
      ( Query . encodeUtf8 $
          "SELECT name, default_expression, default_overlap,"
            <> " override_expression, override_overlap, enabled,"
            <> " last_fired_at, last_checked_at, created_at, updated_at"
            <> " FROM "
            <> cronSchedulesTable schemaName
            <> " WHERE name = ?"
      )
      (Only scheduleName)
  pure $ case rows of
    [row] -> Just row
    _ -> Nothing

-- | Update a cron schedule (patch semantics).
--
-- Returns the number of rows affected (0 = not found, 1 = updated).
updateCronSchedule :: Connection -> Text -> Text -> CronScheduleUpdate -> IO Int64
updateCronSchedule conn schemaName scheduleName upd = do
  let (clauses, params) =
        mconcat
          [ case upd.overrideExpression of
              Nothing -> ([], [])
              Just Nothing -> (["override_expression = NULL"], [])
              Just (Just expr) -> (["override_expression = ?"], [expr])
          , case upd.overrideOverlap of
              Nothing -> ([], [])
              Just Nothing -> (["override_overlap = NULL"], [])
              Just (Just ov) -> (["override_overlap = ?"], [ov])
          , case upd.enabled of
              Nothing -> ([], [])
              Just True -> (["enabled = TRUE"], [])
              Just False -> (["enabled = FALSE"], [])
          ]
  if null clauses
    then pure 0
    else do
      let setSQL = T.intercalate ", " clauses <> ", updated_at = NOW()"
          sql =
            Query . encodeUtf8 $
              "UPDATE "
                <> cronSchedulesTable schemaName
                <> " SET "
                <> setSQL
                <> " WHERE name = ?"
      execute conn sql (params ++ [scheduleName])

-- | Update @last_fired_at@ to NOW().
touchCronScheduleLastFired :: Connection -> Text -> Text -> IO ()
touchCronScheduleLastFired conn schemaName scheduleName = do
  let sql =
        Query . encodeUtf8 $
          "UPDATE "
            <> cronSchedulesTable schemaName
            <> " SET last_fired_at = NOW(), updated_at = NOW()"
            <> " WHERE name = ?"
  _ <- execute conn sql (Only scheduleName)
  pure ()

-- | Delete schedules whose names are not in the given list.
--
-- Returns the number of rows deleted. Does nothing if the list is empty.
deleteStaleSchedules :: Connection -> Text -> [Text] -> IO Int64
deleteStaleSchedules _ _ [] = pure 0
deleteStaleSchedules conn schemaName names = do
  let sql =
        Query . encodeUtf8 $
          "DELETE FROM "
            <> cronSchedulesTable schemaName
            <> " WHERE name NOT IN ?"
  execute conn sql (Only (In names))

-- | Update @last_checked_at@ to NOW() for the given schedule names.
touchCronSchedulesChecked :: Connection -> Text -> [Text] -> IO ()
touchCronSchedulesChecked _ _ [] = pure ()
touchCronSchedulesChecked conn schemaName names = do
  let sql =
        Query . encodeUtf8 $
          "UPDATE "
            <> cronSchedulesTable schemaName
            <> " SET last_checked_at = NOW()"
            <> " WHERE name IN ?"
  _ <- execute conn sql (Only (In names))
  pure ()

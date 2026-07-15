{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Cron SQL templates.
module Arbiter.Core.Sql.Cron
  ( allCronColumns
  , upsertCronDefaultSQL
  , listCronSchedulesSQL
  , getCronScheduleByNameSQL
  , touchCronLastFiredSQL
  , touchCronCheckedSQL
  , tryFireCronGateSQL
  , tryAcquireCronLeaderSQL
  , requestCronRunSQL
  , claimCronRunSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Codec (codecColumns, cronScheduleRowCodec)
import Arbiter.Core.CronSchedule (cronSchedulesTable)
import Arbiter.Core.Job.Schema (cronRunNotifyChannel)

allCronColumns :: Text
allCronColumns = T.intercalate ", " (codecColumns cronScheduleRowCodec)

-- | Upsert a cron schedule's default values, preserving @override_*@ columns on conflict.
-- Parameters: name, queue_name, default_expression, default_overlap, default_timezone
upsertCronDefaultSQL :: Text -> Text
upsertCronDefaultSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "INSERT INTO "
        <> tbl
        <> " (name, queue_name, default_expression, default_overlap, default_timezone) VALUES (?, ?, ?, ?, ?)"
        <> " ON CONFLICT (name) DO UPDATE SET"
        <> " queue_name = EXCLUDED.queue_name,"
        <> " default_expression = EXCLUDED.default_expression,"
        <> " default_overlap = EXCLUDED.default_overlap,"
        <> " default_timezone = EXCLUDED.default_timezone,"
        <> " updated_at = NOW()"

-- | List cron schedules ordered by name, optionally filtered by queue.
-- Parameters: queue_name (NULL = all queues)
listCronSchedulesSQL :: Text -> Text
listCronSchedulesSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in [text|
        SELECT ${allCronColumns} FROM ${tbl}
        WHERE ?::text IS NULL OR queue_name = ?::text
        ORDER BY name
      |]

-- | Get a single cron schedule by name.
--
-- Parameters: schedule name
getCronScheduleByNameSQL :: Text -> Text
getCronScheduleByNameSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "SELECT " <> allCronColumns <> " FROM " <> tbl <> " WHERE name = ?"

-- | Set @last_fired_at@ to NOW() for a schedule.
touchCronLastFiredSQL :: Text -> Text
touchCronLastFiredSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "UPDATE " <> tbl <> " SET last_fired_at = NOW() WHERE name = ?"

-- | Set @last_checked_at@ to the caller-supplied watermark for the given
-- schedule names. The watermark must be the minute boundary the scheduler
-- finished evaluating (not DB @NOW()@) so a slow iteration cannot leapfrog
-- @last_checked_at@ past minutes it never tested. 'GREATEST' guards against
-- backward motion under concurrent worker pools with skewed clocks.
--
-- Parameters: watermark timestamp, schedule names (text array)
touchCronCheckedSQL :: Text -> Text
touchCronCheckedSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "UPDATE "
        <> tbl
        <> " SET last_checked_at = GREATEST(last_checked_at, ?) WHERE name = ANY(?)"

-- | Fire-once-per-minute gate. Advances @last_fired_at@ to the minute floor
-- only if the existing value is strictly less. 0 rows = another pool won.
--
-- Parameters: minute floor, schedule name, minute floor
tryFireCronGateSQL :: Text -> Text
tryFireCronGateSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "UPDATE "
        <> tbl
        <> " SET last_fired_at = GREATEST(last_fired_at, ?)"
        <> " WHERE name = ?"
        <> " AND (last_fired_at IS NULL OR last_fired_at < ?)"

-- | Per-(schema, queue, name) transaction-scoped advisory lock for cron.
-- Parameters: schema, queue, name.
tryAcquireCronLeaderSQL :: Text
tryAcquireCronLeaderSQL =
  "SELECT pg_try_advisory_xact_lock(hashtextextended(? || ':' || ? || ':' || ?, 0)) AS result"

-- | Stamp a manual run request and NOTIFY the run-now channel for the matched
-- row. Returns the matched-row count (0 = no such schedule). Parameters: schedule name.
requestCronRunSQL :: Text -> Text
requestCronRunSQL schemaName =
  let tbl = cronSchedulesTable schemaName
      chan = T.replace "'" "''" (cronRunNotifyChannel schemaName)
   in "WITH upd AS (UPDATE " <> tbl <> " SET run_requested_at = NOW(), updated_at = NOW() WHERE name = ? RETURNING name), notif AS (SELECT pg_notify('" <> chan <> "', name) FROM upd) SELECT count(*)::int8 AS count FROM upd WHERE (SELECT count(*) FROM notif) >= 0"

-- | Claim a fresh run request, clearing the flag and advancing the gate.
-- Parameters: minute floor, schedule name, staleness cutoff.
claimCronRunSQL :: Text -> Text
claimCronRunSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "UPDATE " <> tbl <> " SET run_requested_at = NULL, last_fired_at = GREATEST(last_fired_at, ?), updated_at = NOW() WHERE name = ? AND enabled AND run_requested_at IS NOT NULL AND run_requested_at >= ?"

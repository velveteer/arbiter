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
  , touchCronManualRunSQL
  , pendingCronRunsSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Codec (codecColumns, cronScheduleRowCodec)
import Arbiter.Core.CronSchedule (cronSchedulesTable)
import Arbiter.Core.Job.Schema (cronRunNotifyChannel)
import Arbiter.Core.SqlLiterals (textLiteral)

allCronColumns :: Text
allCronColumns = T.intercalate ", " (codecColumns cronScheduleRowCodec)

-- | 'allCronColumns' with an expired @run_requested_at@ read back as NULL, so a
-- request no pool claimed in time reaches readers as the no-op it now is.
cronReadColumns :: Text
cronReadColumns = T.intercalate ", " (map expire (codecColumns cronScheduleRowCodec))
  where
    expire "run_requested_at" = "(CASE WHEN " <> cronRunPending <> " THEN run_requested_at END) AS run_requested_at"
    expire c = c

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
        SELECT ${cronReadColumns} FROM ${tbl}
        WHERE ?::text IS NULL OR queue_name = ?::text
        ORDER BY name
      |]

-- | Get a single cron schedule by name.
--
-- Parameters: schedule name
getCronScheduleByNameSQL :: Text -> Text
getCronScheduleByNameSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "SELECT " <> cronReadColumns <> " FROM " <> tbl <> " WHERE name = ?"

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

-- | How long a run request stays claimable. Only a pool serving the queue can
-- claim one, so a request older than this had no pool to take it and expires
-- instead of pinning the schedule.
cronRunRequestTtl :: Text
cronRunRequestTtl = "INTERVAL '5 minutes'"

-- | A run request that is still claimable.
cronRunPending :: Text
cronRunPending = "(run_requested_at IS NOT NULL AND run_requested_at > NOW() - " <> cronRunRequestTtl <> ")"

-- | Stamp a run request on an enabled schedule and NOTIFY. Returns 0 (no such
-- schedule), 1 (disabled), 2 (stamped), or 3 (a request is already pending).
-- The status read locks, so it reports the same row version the update sees.
-- An expired request is overwritten rather than reported as pending.
requestCronRunSQL :: Text -> Text
requestCronRunSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "WITH found AS (SELECT enabled, run_requested_at FROM "
        <> tbl
        <> " WHERE name = ? FOR UPDATE),"
        <> " upd AS ("
        <> "UPDATE "
        <> tbl
        <> " SET run_requested_at = NOW(), updated_at = NOW()"
        <> " WHERE name = ? AND enabled AND NOT "
        <> cronRunPending
        <> " RETURNING pg_notify("
        <> textLiteral (cronRunNotifyChannel schemaName)
        <> ", name))"
        <> " SELECT (CASE"
        <> " WHEN EXISTS (SELECT 1 FROM upd) THEN 2"
        <> " WHEN EXISTS (SELECT 1 FROM found WHERE enabled AND "
        <> cronRunPending
        <> ") THEN 3"
        <> " WHEN EXISTS (SELECT 1 FROM found) THEN 1"
        <> " ELSE 0 END)::int8 AS count"

-- | Claim a pending run request, clearing the flag and returning the claimed row.
claimCronRunSQL :: Text -> Text
claimCronRunSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "UPDATE "
        <> tbl
        <> " SET run_requested_at = NULL, updated_at = NOW()"
        <> " WHERE name = ? AND enabled AND "
        <> cronRunPending
        <> " RETURNING "
        <> allCronColumns

-- | Record when a manual run last fired a job.
--
-- Parameters: fired-at timestamp, schedule name
touchCronManualRunSQL :: Text -> Text
touchCronManualRunSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "UPDATE " <> tbl <> " SET last_manual_run_at = ?, updated_at = NOW() WHERE name = ?"

-- | Names of enabled schedules with a pending run request among the given names.
pendingCronRunsSQL :: Text -> Text
pendingCronRunsSQL schemaName =
  let tbl = cronSchedulesTable schemaName
   in "SELECT name FROM "
        <> tbl
        <> " WHERE name = ANY(?) AND enabled AND "
        <> cronRunPending

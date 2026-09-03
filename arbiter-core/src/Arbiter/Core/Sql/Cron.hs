{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Cron SQL templates.
module Arbiter.Core.Sql.Cron
  ( allCronColumns
  , upsertCronDefaultSQL
  , listCronSchedulesSQL
  , getCronScheduleByNameSQL
  , updateCronScheduleSQL
  , touchCronLastFiredSQL
  , touchCronCheckedSQL
  , tryFireCronGateSQL
  , tryAcquireCronLeaderSQL
  , requestCronRunSQL
  , claimCronRunSQL
  , touchCronManualRunSQL
  , pendingCronRunsSQL
  ) where

import Control.Monad (join)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Time (UTCTime)
import NeatInterpolation (text)

import Arbiter.Core.Codec (cronScheduleRowCodec)
import Arbiter.Core.CronSchedule (CronScheduleRow, CronScheduleUpdate (..), cronSchedulesTable)
import Arbiter.Core.Job.Schema (cronRunNotifyChannel)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)
import Arbiter.Core.SqlLiterals (textLiteral)

-- | The @cron_schedules@ read columns, in codec order.
allCronColumns :: Text
allCronColumns =
  [text|
    name, queue_name, default_expression, default_overlap, default_timezone,
    override_expression, override_overlap, override_timezone, enabled,
    last_fired_at, last_checked_at, run_requested_at, last_manual_run_at, created_at, updated_at
  |]

-- | 'allCronColumns' with an expired @run_requested_at@ read back as NULL.
cronReadColumns :: Text
cronReadColumns =
  [text|
    name, queue_name, default_expression, default_overlap, default_timezone,
    override_expression, override_overlap, override_timezone, enabled,
    last_fired_at, last_checked_at,
    (CASE WHEN ${cronRunPending} THEN run_requested_at END) AS run_requested_at,
    last_manual_run_at, created_at, updated_at
  |]

-- | Upsert a cron schedule's default values, preserving @override_*@ columns on conflict.
-- An unchanged schedule is left alone.
upsertCronDefaultSQL :: Text -> Text -> Text -> Text -> Text -> Maybe Text -> Query ()
upsertCronDefaultSQL schemaName name queueName defaultExpr defaultOv defaultTz =
  let tbl = cronSchedulesTable schemaName
   in [sql|
        INSERT INTO ${tbl} (name, queue_name, default_expression, default_overlap, default_timezone)
        VALUES (#{name :: CText}, #{queueName :: CText}, #{defaultExpr :: CText},
                #{defaultOv :: CText}, #{defaultTz :: Maybe CText})
        ON CONFLICT (name) DO UPDATE SET
          queue_name = EXCLUDED.queue_name,
          default_expression = EXCLUDED.default_expression,
          default_overlap = EXCLUDED.default_overlap,
          default_timezone = EXCLUDED.default_timezone,
          updated_at = NOW()
        WHERE (${tbl}.queue_name, ${tbl}.default_expression, ${tbl}.default_overlap, ${tbl}.default_timezone)
          IS DISTINCT FROM (EXCLUDED.queue_name, EXCLUDED.default_expression,
                            EXCLUDED.default_overlap, EXCLUDED.default_timezone)
      |]

-- | List cron schedules ordered by name, optionally filtered by queue.
listCronSchedulesSQL :: Text -> Maybe Text -> Query CronScheduleRow
listCronSchedulesSQL schemaName queue =
  let tbl = cronSchedulesTable schemaName
   in rows
        cronScheduleRowCodec
        [sql|
          SELECT ${cronReadColumns} FROM ${tbl}
          WHERE #{queue :: Maybe CText}::text IS NULL OR queue_name = #{queue :: Maybe CText}::text
          ORDER BY name
        |]

-- | Get a single cron schedule by name.
getCronScheduleByNameSQL :: Text -> Text -> Query CronScheduleRow
getCronScheduleByNameSQL schemaName name =
  let tbl = cronSchedulesTable schemaName
   in rows cronScheduleRowCodec [sql|SELECT ${cronReadColumns} FROM ${tbl} WHERE name = #{name :: CText}|]

-- | Patch a cron schedule's overrides. 'Nothing' when the patch sets no column.
-- @Just Nothing@ clears an override. Disabling also drops a pending run request.
updateCronScheduleSQL :: Text -> Text -> CronScheduleUpdate -> Maybe (Query ())
updateCronScheduleSQL _ _ (CronScheduleUpdate Nothing Nothing Nothing Nothing) = Nothing
updateCronScheduleSQL schemaName name (CronScheduleUpdate mExpression mOverlap mTimezone mEnabled) =
  let tbl = cronSchedulesTable schemaName
      setExpression = isJust mExpression
      expression = join mExpression
      setOverlap = isJust mOverlap
      overlap = join mOverlap
      setTimezone = isJust mTimezone
      timezone = join mTimezone
   in Just
        [sql|
          UPDATE ${tbl}
          SET override_expression = CASE WHEN #{setExpression :: CBool}::boolean
                                         THEN #{expression :: Maybe CText}::text
                                         ELSE override_expression END,
              override_overlap = CASE WHEN #{setOverlap :: CBool}::boolean
                                      THEN #{overlap :: Maybe CText}::text
                                      ELSE override_overlap END,
              override_timezone = CASE WHEN #{setTimezone :: CBool}::boolean
                                       THEN #{timezone :: Maybe CText}::text
                                       ELSE override_timezone END,
              enabled = COALESCE(#{mEnabled :: Maybe CBool}::boolean, enabled),
              run_requested_at = CASE WHEN #{mEnabled :: Maybe CBool}::boolean IS FALSE
                                      THEN NULL
                                      ELSE run_requested_at END,
              updated_at = NOW()
          WHERE name = #{name :: CText}
        |]

-- | Set @last_fired_at@ to NOW() for a schedule.
touchCronLastFiredSQL :: Text -> Text -> Query ()
touchCronLastFiredSQL schemaName name =
  let tbl = cronSchedulesTable schemaName
   in [sql|UPDATE ${tbl} SET last_fired_at = NOW() WHERE name = #{name :: CText}|]

-- | Set @last_checked_at@ to the caller-supplied watermark for the given
-- schedule names. The watermark is the minute boundary the scheduler finished
-- evaluating. @GREATEST@ keeps it from moving backward under concurrent worker
-- pools with skewed clocks.
touchCronCheckedSQL :: Text -> UTCTime -> [Text] -> Query ()
touchCronCheckedSQL schemaName watermark names =
  let tbl = cronSchedulesTable schemaName
   in [sql|
        UPDATE ${tbl}
        SET last_checked_at = GREATEST(last_checked_at, #{watermark :: CTimestamptz})
        WHERE name = ANY(#{names :: [CText]})
      |]

-- | Fire-once-per-minute gate. Advances @last_fired_at@ to the minute floor
-- when the existing value is less. Zero rows means another pool won.
tryFireCronGateSQL :: Text -> UTCTime -> Text -> Query ()
tryFireCronGateSQL schemaName minuteFloor name =
  let tbl = cronSchedulesTable schemaName
   in [sql|
        UPDATE ${tbl}
        SET last_fired_at = GREATEST(last_fired_at, #{minuteFloor :: CTimestamptz})
        WHERE name = #{name :: CText}
          AND (last_fired_at IS NULL OR last_fired_at < #{minuteFloor :: CTimestamptz})
      |]

-- | Per-(schema, queue, name) transaction-scoped advisory lock for cron.
tryAcquireCronLeaderSQL :: Text -> Text -> Text -> Query Bool
tryAcquireCronLeaderSQL schema queue name =
  [sql|
    SELECT pg_try_advisory_xact_lock(
      hashtextextended(#{schema :: CText} || ':' || #{queue :: CText} || ':' || #{name :: CText}, 0)
    ) AS @{result :: CBool}
  |]

-- | How long a run request stays claimable.
cronRunRequestTtl :: Text
cronRunRequestTtl = "INTERVAL '5 minutes'"

-- | A run request that is still claimable.
cronRunPending :: Text
cronRunPending = [text|(run_requested_at IS NOT NULL AND run_requested_at > NOW() - ${cronRunRequestTtl})|]

-- | Stamp a run request on an enabled schedule and NOTIFY, returning a status.
requestCronRunSQL :: Text -> Text -> Query Text
requestCronRunSQL schemaName name =
  let tbl = cronSchedulesTable schemaName
      chan = textLiteral (cronRunNotifyChannel schemaName)
   in [sql|
        WITH found AS (SELECT enabled, run_requested_at FROM ${tbl} WHERE name = #{name :: CText} FOR UPDATE),
        stamped AS (
          UPDATE ${tbl} SET run_requested_at = NOW(), updated_at = NOW()
          WHERE name = #{name :: CText} AND enabled AND NOT ${cronRunPending}
          RETURNING pg_notify(${chan}, name))
        SELECT CASE
          WHEN EXISTS (SELECT 1 FROM stamped) THEN 'stamped'
          WHEN EXISTS (SELECT 1 FROM found WHERE enabled AND ${cronRunPending}) THEN 'pending'
          WHEN EXISTS (SELECT 1 FROM found) THEN 'disabled'
          ELSE 'not_found' END AS @{status :: CText}
      |]

-- | Claim a pending run request, clearing the flag and returning the claimed row.
claimCronRunSQL :: Text -> Text -> Query CronScheduleRow
claimCronRunSQL schemaName name =
  let tbl = cronSchedulesTable schemaName
   in rows
        cronScheduleRowCodec
        [sql|
          UPDATE ${tbl} SET run_requested_at = NULL, updated_at = NOW()
          WHERE name = #{name :: CText} AND enabled AND ${cronRunPending}
          RETURNING ${allCronColumns}
        |]

-- | Record when a manual run last fired a job.
touchCronManualRunSQL :: Text -> UTCTime -> Text -> Query ()
touchCronManualRunSQL schemaName firedAt name =
  let tbl = cronSchedulesTable schemaName
   in [sql|
        UPDATE ${tbl}
        SET last_manual_run_at = GREATEST(last_manual_run_at, #{firedAt :: CTimestamptz}), updated_at = NOW()
        WHERE name = #{name :: CText}
      |]

-- | Names of enabled schedules with a pending run request among the given names.
pendingCronRunsSQL :: Text -> [Text] -> Query Text
pendingCronRunsSQL schemaName names =
  let tbl = cronSchedulesTable schemaName
   in [sql|SELECT @{name :: CText} FROM ${tbl} WHERE name = ANY(#{names :: [CText]}) AND enabled AND ${cronRunPending}|]

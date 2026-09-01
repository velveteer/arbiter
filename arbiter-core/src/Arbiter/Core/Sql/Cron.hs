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

import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)

import Arbiter.Core.Codec (codecColumns, cronScheduleRowCodec)
import Arbiter.Core.CronSchedule (CronScheduleRow, CronScheduleUpdate (..), cronSchedulesTable)
import Arbiter.Core.Job.Schema (cronRunNotifyChannel)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, raw, rows, sepBy)
import Arbiter.Core.SqlLiterals (textLiteral)

-- | The @cron_schedules@ read columns, comma separated.
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
upsertCronDefaultSQL :: Text -> Text -> Text -> Text -> Text -> Maybe Text -> Query ()
upsertCronDefaultSQL schemaName name queueName defaultExpr defaultOv defaultTz =
  let tbl = cronSchedulesTable schemaName
   in [sql|
        INSERT INTO ${tbl} (name, queue_name, default_expression, default_overlap, default_timezone)
        VALUES (#{name :: CText}, #{queueName :: CText}, #{defaultExpr :: CText}, #{defaultOv :: CText}, #{defaultTz :: Maybe CText})
        ON CONFLICT (name) DO UPDATE SET
          queue_name = EXCLUDED.queue_name,
          default_expression = EXCLUDED.default_expression,
          default_overlap = EXCLUDED.default_overlap,
          default_timezone = EXCLUDED.default_timezone,
          updated_at = NOW()
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
updateCronScheduleSQL :: Text -> Text -> CronScheduleUpdate -> Maybe (Query ())
updateCronScheduleSQL schemaName name (CronScheduleUpdate mExpr mOverlap mTz mEnabled)
  | null clauses = Nothing
  | otherwise =
      let tbl = cronSchedulesTable schemaName
          setFrag = sepBy ", " (clauses <> [raw "updated_at = NOW()"])
       in Just [sql|UPDATE ${tbl} SET ${setFrag} WHERE name = #{name :: CText}|]
  where
    clauses =
      concat
        [ patch "override_expression" mExpr
        , patch "override_overlap" mOverlap
        , patch "override_timezone" mTz
        , case mEnabled of
            Nothing -> []
            Just True -> [raw "enabled = TRUE"]
            Just False -> [raw "enabled = FALSE", raw "run_requested_at = NULL"]
        ]
    patch col = foldMap (\mv -> [raw (col <> " = ") <> maybe (raw "NULL") (\v -> [sql|#{v :: CText}|]) mv])

-- | Set @last_fired_at@ to NOW() for a schedule.
touchCronLastFiredSQL :: Text -> Text -> Query ()
touchCronLastFiredSQL schemaName name =
  let tbl = cronSchedulesTable schemaName
   in [sql|UPDATE ${tbl} SET last_fired_at = NOW() WHERE name = #{name :: CText}|]

-- | Set @last_checked_at@ to the caller-supplied watermark for the given
-- schedule names. The watermark must be the minute boundary the scheduler
-- finished evaluating (not DB @NOW()@) so a slow iteration cannot leapfrog
-- @last_checked_at@ past minutes it never tested. @GREATEST@ guards against
-- backward motion under concurrent worker pools with skewed clocks.
touchCronCheckedSQL :: Text -> UTCTime -> [Text] -> Query ()
touchCronCheckedSQL schemaName watermark names =
  let tbl = cronSchedulesTable schemaName
   in [sql|UPDATE ${tbl} SET last_checked_at = GREATEST(last_checked_at, #{watermark :: CTimestamptz}) WHERE name = ANY(#{names :: [CText]})|]

-- | Fire-once-per-minute gate. Advances @last_fired_at@ to the minute floor
-- only if the existing value is strictly less. 0 rows = another pool won.
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
  [sql|SELECT pg_try_advisory_xact_lock(hashtextextended(#{schema :: CText} || ':' || #{queue :: CText} || ':' || #{name :: CText}, 0)) AS @{result :: CBool}|]

-- | How long a run request stays claimable. Only a pool serving the queue can
-- claim one, so a request older than this had no pool to take it and expires
-- and does not pin the schedule.
cronRunRequestTtl :: Text
cronRunRequestTtl = "INTERVAL '5 minutes'"

-- | A run request that is still claimable.
cronRunPending :: Text
cronRunPending = "(run_requested_at IS NOT NULL AND run_requested_at > NOW() - " <> cronRunRequestTtl <> ")"

-- | Stamp a run request on an enabled schedule and NOTIFY, returning a status.
requestCronRunSQL :: Text -> Text -> Query Text
requestCronRunSQL schemaName name =
  let tbl = cronSchedulesTable schemaName
      chan = textLiteral (cronRunNotifyChannel schemaName)
   in [sql|
        WITH found AS (SELECT enabled, run_requested_at FROM ${tbl} WHERE name = #{name :: CText} FOR UPDATE),
        upd AS (
          UPDATE ${tbl} SET run_requested_at = NOW(), updated_at = NOW()
          WHERE name = #{name :: CText} AND enabled AND NOT ${cronRunPending}
          RETURNING pg_notify(${chan}, name))
        SELECT CASE
          WHEN EXISTS (SELECT 1 FROM upd) THEN 'stamped'
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
   in [sql|UPDATE ${tbl} SET last_manual_run_at = GREATEST(last_manual_run_at, #{firedAt :: CTimestamptz}), updated_at = NOW() WHERE name = #{name :: CText}|]

-- | Names of enabled schedules with a pending run request among the given names.
pendingCronRunsSQL :: Text -> [Text] -> Query Text
pendingCronRunsSQL schemaName names =
  let tbl = cronSchedulesTable schemaName
   in [sql|SELECT @{name :: CText} FROM ${tbl} WHERE name = ANY(#{names :: [CText]}) AND enabled AND ${cronRunPending}|]

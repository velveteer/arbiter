{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Groups SQL templates.
module Arbiter.Core.Sql.Groups
  ( groupsWindowSQL
  , emptiedWindowSQL
  , lockGroupsSQL
  , refreshGroupsSQL
  , insertMissingGroupsSQL
  ) where

import Data.Int (Int64)
import Data.Text (Text)
import NeatInterpolation (text)

import Arbiter.Core.Job.Schema (jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Job.Schema.Groups (groupAggregates, inFlightPredicate)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query)

-- | At most @limit@ groups keys past @cursor@, in the database's key order, unlocked.
-- Its last key is the caller's resume cursor.
groupsWindowSQL :: Text -> Text -> Int -> Maybe Text -> Query Text
groupsWindowSQL schema tableName limit cursor =
  let groupsTbl = jobQueueGroupsTable schema tableName
      lim = fromIntegral limit :: Int64
      after = foldMap (\key -> [sql|WHERE group_key > #{key :: CText}|]) cursor
   in [sql|SELECT @{group_key :: CText} FROM ${groupsTbl} ${after} ORDER BY group_key LIMIT #{lim :: CInt8}|]

-- | At most @limit@ keys past @cursor@ the maintenance triggers emptied in place, in the
-- database's key order. Its last key is the caller's resume cursor.
emptiedWindowSQL :: Text -> Text -> Int -> Maybe Text -> Query Text
emptiedWindowSQL schema tableName limit cursor =
  let groupsTbl = jobQueueGroupsTable schema tableName
      lim = fromIntegral limit :: Int64
      after = foldMap (\key -> [sql|AND group_key > #{key :: CText}|]) cursor
   in [sql|
        SELECT @{group_key :: CText} FROM ${groupsTbl}
        WHERE job_count = 0 ${after}
        ORDER BY group_key LIMIT #{lim :: CInt8}
      |]

-- | @FOR UPDATE SKIP LOCKED@ over the window 'groupsWindowSQL' returned plus the keys
-- 'emptiedWindowSQL' returned, one ascending pass in the key order the maintenance
-- triggers use. Returns the keys this transaction holds.
lockGroupsSQL :: Text -> Text -> Maybe Text -> Maybe Text -> [Text] -> Query Text
lockGroupsSQL schema tableName cursor upper emptied =
  let groupsTbl = jobQueueGroupsTable schema tableName
      after = foldMap (\key -> [sql|AND group_key > #{key :: CText}|]) cursor
      window =
        foldMap
          ( \upperKey ->
              [sql|SELECT group_key FROM ${groupsTbl} WHERE group_key <= #{upperKey :: CText} ${after} UNION |]
          )
          upper
   in [sql|
        WITH targets AS (
          ${window}SELECT unnest(#{emptied :: [CText]}::text[]) AS group_key
        )
        SELECT @{group_key :: CText} FROM ${groupsTbl}
        WHERE group_key IN (SELECT group_key FROM targets)
        ORDER BY group_key
        FOR UPDATE SKIP LOCKED
      |]

-- | Summary columns, shared by the refresh and the insert.
summaryAggregates :: Text
summaryAggregates =
  let aggs = groupAggregates ""
      inFlight = inFlightPredicate ""
   in [text|${aggs}, MAX(not_visible_until) FILTER (WHERE ${inFlight}) AS in_flight_until|]

-- | Recompute the groups table, scoped to the locked keys from 'lockGroupsSQL'.
-- Returns the count of rows it rewrote. A separate statement whose snapshot post-dates the lock.
refreshGroupsSQL :: Text -> Text -> [Text] -> Query Int64
refreshGroupsSQL schema tableName keys =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      aggs = summaryAggregates
   in [sql|
        WITH params AS (
          SELECT unnest(#{keys :: [CText]}::text[]) AS group_key
        ),
        current AS (
          SELECT group_key, ${aggs}
          FROM ${tbl}
          WHERE group_key IN (SELECT group_key FROM params)
          GROUP BY group_key
        ),
        deleted AS (
          DELETE FROM ${groupsTbl} summary
          WHERE summary.group_key IN (SELECT group_key FROM params)
            AND NOT EXISTS (SELECT 1 FROM current fresh WHERE fresh.group_key = summary.group_key)
          RETURNING 1
        ),
        updated AS (
          UPDATE ${groupsTbl} summary
          SET min_priority = fresh.min_priority,
              min_id = fresh.min_id,
              job_count = fresh.job_count,
              ready_count = fresh.ready_count,
              next_due = fresh.next_due,
              in_flight_until = fresh.in_flight_until
          FROM current fresh
          WHERE summary.group_key = fresh.group_key
            AND (summary.min_priority <> fresh.min_priority OR summary.min_id <> fresh.min_id
                 OR summary.job_count <> fresh.job_count
                 OR summary.ready_count <> fresh.ready_count
                 OR summary.next_due IS DISTINCT FROM fresh.next_due
                 OR summary.in_flight_until IS DISTINCT FROM fresh.in_flight_until)
          RETURNING 1
        )
        SELECT (SELECT count(*) FROM deleted) + (SELECT count(*) FROM updated) AS @{rewritten :: CInt8}
      |]

-- | Insert summary rows for grouped jobs the triggers left without one, at most
-- @limit@ keys per call in key order, bounded to the @lower@ to @upper@ key window its
-- caller locked. Returns each key it covered and whether the insert landed.
insertMissingGroupsSQL :: Text -> Text -> Int -> Maybe Text -> Maybe Text -> Query (Text, Bool)
insertMissingGroupsSQL schema tableName limit lower upper =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      aggs = summaryAggregates
      lim = fromIntegral limit :: Int64
      after = foldMap (\key -> [sql|AND job.group_key > #{key :: CText}|]) lower
      upTo = foldMap (\key -> [sql|AND job.group_key <= #{key :: CText}|]) upper
   in [sql|
        WITH missing_keys AS (
          SELECT DISTINCT job.group_key
          FROM ${tbl} job
          WHERE job.group_key IS NOT NULL
            ${after}
            ${upTo}
            AND NOT EXISTS (SELECT 1 FROM ${groupsTbl} summary WHERE summary.group_key = job.group_key)
          ORDER BY job.group_key
          LIMIT #{lim :: CInt8}
        ),
        missing AS (
          SELECT group_key, ${aggs}
          FROM ${tbl}
          WHERE group_key IN (SELECT group_key FROM missing_keys)
          GROUP BY group_key
        ),
        inserted AS (
          INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count, ready_count, next_due, in_flight_until)
          SELECT group_key, min_priority, min_id, job_count, ready_count, next_due, in_flight_until
          FROM missing
          ORDER BY group_key
          ON CONFLICT (group_key) DO NOTHING
          RETURNING group_key
        )
        SELECT missing_key.group_key AS @{group_key :: CText}, (landed.group_key IS NOT NULL) AS @{inserted :: CBool}
        FROM missing_keys missing_key
        LEFT JOIN inserted landed ON landed.group_key = missing_key.group_key
        ORDER BY missing_key.group_key
      |]

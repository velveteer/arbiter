{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Groups SQL templates.
module Arbiter.Core.Sql.Groups
  ( groupsWindowSQL
  , lockGroupsSQL
  , refreshGroupsSQL
  , insertMissingGroupsSQL
  ) where

import Data.Int (Int64)
import Data.Text (Text)

import Arbiter.Core.Job.Schema (groupAggregates, inFlightPredicate, jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query)

-- | At most @limit@ groups keys past @cursor@, in the database's key order, unlocked.
-- Its last key is the caller's resume cursor.
groupsWindowSQL :: Text -> Text -> Int -> Maybe Text -> Query Text
groupsWindowSQL schema tableName limit cursor =
  let groupsTbl = jobQueueGroupsTable schema tableName
      lim = fromIntegral limit :: Int64
      after = foldMap (\k -> [sql|WHERE group_key > #{k :: CText}|]) cursor
   in [sql|SELECT @{group_key :: CText} FROM ${groupsTbl} ${after} ORDER BY group_key LIMIT #{lim :: CInt8}|]

-- | @FOR UPDATE SKIP LOCKED@ over the window 'groupsWindowSQL' returned plus at most
-- @limit@ rows the maintenance triggers emptied in place, one ascending pass in the key
-- order those triggers use. Returns the keys this transaction holds.
lockGroupsSQL :: Text -> Text -> Int -> Maybe Text -> Maybe Text -> Query Text
lockGroupsSQL schema tableName limit cursor upper =
  let groupsTbl = jobQueueGroupsTable schema tableName
      lim = fromIntegral limit :: Int64
      after = foldMap (\k -> [sql|AND group_key > #{k :: CText}|]) cursor
      window = foldMap (\hi -> [sql|SELECT group_key FROM ${groupsTbl} WHERE group_key <= #{hi :: CText} ${after} UNION |]) upper
   in [sql|
        WITH emptied AS (
          SELECT group_key FROM ${groupsTbl} WHERE job_count = 0 ORDER BY group_key LIMIT #{lim :: CInt8}
        ),
        targets AS (
          ${window}SELECT group_key FROM emptied
        )
        SELECT @{group_key :: CText} FROM ${groupsTbl}
        WHERE group_key IN (SELECT group_key FROM targets)
        ORDER BY group_key
        FOR UPDATE SKIP LOCKED
      |]

-- | Summary columns, shared by the refresh and the insert.
summaryAggregates :: Text
summaryAggregates =
  groupAggregates "" <> ", MAX(not_visible_until) FILTER (WHERE " <> inFlightPredicate "" <> ") AS in_flight_until"

-- | Recompute the groups table, scoped to the locked keys from 'lockGroupsSQL',
-- returning the rows it rewrote. A separate statement, so its snapshot post-dates
-- the lock and cannot clobber a concurrent claim's @in_flight_until@.
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
          DELETE FROM ${groupsTbl} g
          WHERE g.group_key IN (SELECT group_key FROM params)
            AND NOT EXISTS (SELECT 1 FROM current c WHERE c.group_key = g.group_key)
          RETURNING 1
        ),
        updated AS (
          UPDATE ${groupsTbl} g
          SET min_priority = c.min_priority,
              min_id = c.min_id,
              job_count = c.job_count,
              ready_count = c.ready_count,
              next_due = c.next_due,
              in_flight_until = c.in_flight_until
          FROM current c
          WHERE g.group_key = c.group_key
            AND (g.min_priority <> c.min_priority OR g.min_id <> c.min_id
                 OR g.job_count <> c.job_count
                 OR g.ready_count <> c.ready_count
                 OR g.next_due IS DISTINCT FROM c.next_due
                 OR g.in_flight_until IS DISTINCT FROM c.in_flight_until)
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
      after = foldMap (\k -> [sql|AND j.group_key > #{k :: CText}|]) lower
      upTo = foldMap (\k -> [sql|AND j.group_key <= #{k :: CText}|]) upper
   in [sql|
        WITH missing_keys AS (
          SELECT DISTINCT j.group_key
          FROM ${tbl} j
          WHERE j.group_key IS NOT NULL
            ${after}
            ${upTo}
            AND NOT EXISTS (SELECT 1 FROM ${groupsTbl} g WHERE g.group_key = j.group_key)
          ORDER BY j.group_key
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
        SELECT k.group_key AS @{group_key :: CText}, (i.group_key IS NOT NULL) AS @{inserted :: CBool}
        FROM missing_keys k
        LEFT JOIN inserted i ON i.group_key = k.group_key
        ORDER BY k.group_key
      |]

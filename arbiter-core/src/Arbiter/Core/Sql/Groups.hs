{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Groups SQL templates.
module Arbiter.Core.Sql.Groups
  ( lockGroupsSQL
  , refreshGroupsSQL
  , insertMissingGroupsSQL
  ) where

import Data.Int (Int64)
import Data.Text (Text)

import Arbiter.Core.Job.Schema (groupAggregates, inFlightPredicate, jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query)

-- | @FOR UPDATE SKIP LOCKED@ over at most @limit@ groups rows past @cursor@, in key
-- order, returning the locked keys for the reaper to recompute. Claim-locked groups
-- are skipped, and the caller resumes from the last key returned.
lockGroupsSQL :: Text -> Text -> Int -> Maybe Text -> Query Text
lockGroupsSQL schema tableName limit cursor =
  let groupsTbl = jobQueueGroupsTable schema tableName
      lim = fromIntegral limit :: Int64
      after = foldMap (\k -> [sql|WHERE group_key > #{k :: CText}|]) cursor
   in [sql|SELECT @{group_key :: CText} FROM ${groupsTbl} ${after} ORDER BY group_key LIMIT #{lim :: CInt8} FOR UPDATE SKIP LOCKED|]

-- | Recompute the groups table, scoped to the locked keys from 'lockGroupsSQL',
-- returning the rows it rewrote. A separate statement, so its snapshot post-dates
-- the lock and cannot clobber a concurrent claim's @in_flight_until@.
refreshGroupsSQL :: Text -> Text -> [Text] -> Query Int64
refreshGroupsSQL schema tableName keys =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      aggs = groupAggregates "" <> ", MAX(not_visible_until) FILTER (WHERE " <> inFlightPredicate "" <> ") AS in_flight_until"
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
-- @limit@ keys per call. Bounded to the key window its caller locked, so the anti-join
-- reads that slice of the group-key index rather than every job row. A key past the
-- window waits for the pass that covers it.
insertMissingGroupsSQL :: Text -> Text -> Int -> Maybe Text -> Maybe Text -> Query Int64
insertMissingGroupsSQL schema tableName limit lower upper =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      aggs = groupAggregates "" <> ", MAX(not_visible_until) FILTER (WHERE " <> inFlightPredicate "" <> ") AS in_flight_until"
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
          ON CONFLICT (group_key) DO NOTHING
          RETURNING 1
        )
        SELECT (SELECT count(*) FROM inserted) AS @{rewritten :: CInt8}
      |]

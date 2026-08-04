{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Groups SQL templates.
module Arbiter.Core.Sql.Groups
  ( lockGroupsSQL
  , refreshGroupsSQL
  ) where

import Data.Int (Int64)
import Data.Text (Text)

import Arbiter.Core.Job.Schema (inFlightPredicate, jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query)

-- | @FOR UPDATE SKIP LOCKED@ over at most @limit@ groups rows, returning the locked
-- keys for the reaper to recompute. Claim-locked groups are skipped. Bounded so a
-- table grown wide on short-lived keys is drained over several passes rather than in
-- one recompute that outlives the reaper's timeout. Unordered, so a pass covers rows
-- past the bound only as the ones ahead of them are reclaimed.
lockGroupsSQL :: Text -> Text -> Int -> Query Text
lockGroupsSQL schema tableName limit =
  let groupsTbl = jobQueueGroupsTable schema tableName
      lim = fromIntegral limit :: Int64
   in [sql|SELECT @{group_key :: CText} FROM ${groupsTbl} LIMIT #{lim :: CInt8} FOR UPDATE SKIP LOCKED|]

-- | Recompute the groups table, scoped to the locked keys from 'lockGroupsSQL',
-- returning the rows it rewrote. A separate statement, so its snapshot post-dates
-- the lock and cannot clobber a concurrent claim's @in_flight_until@. The INSERT
-- only repairs missing rows, at most @limit@ of them.
refreshGroupsSQL :: Text -> Text -> Int -> [Text] -> Query Int64
refreshGroupsSQL schema tableName limit keys =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      ifBucket = inFlightPredicate ""
      lim = fromIntegral limit :: Int64
   in [sql|
        WITH params AS (
          SELECT unnest(#{keys :: [CText]}::text[]) AS group_key
        ),
        current AS (
          SELECT group_key,
                 MIN(priority) AS min_priority,
                 MIN(id) AS min_id,
                 COUNT(*) AS job_count,
                 COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended) AS ready_count,
                 MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended) AS next_due,
                 MAX(not_visible_until) FILTER (WHERE ${ifBucket}) AS in_flight_until
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
        ),
        missing AS (
          SELECT group_key
          FROM ${tbl} j
          WHERE group_key IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM ${groupsTbl} g WHERE g.group_key = j.group_key)
          GROUP BY group_key
          LIMIT #{lim :: CInt8}
        ),
        inserted AS (
          INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count, ready_count, next_due, in_flight_until)
          SELECT group_key,
                 MIN(priority), MIN(id), COUNT(*),
                 COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended),
                 MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended),
                 MAX(not_visible_until) FILTER (WHERE ${ifBucket})
          FROM ${tbl}
          WHERE group_key IN (SELECT group_key FROM missing)
          GROUP BY group_key
          ON CONFLICT (group_key) DO NOTHING
          RETURNING 1
        )
        SELECT (SELECT count(*) FROM deleted) + (SELECT count(*) FROM updated) + (SELECT count(*) FROM inserted) AS @{rewritten :: CInt8}
      |]

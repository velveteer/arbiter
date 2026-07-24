{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Groups SQL templates.
module Arbiter.Core.Sql.Groups
  ( lockGroupsSQL
  , refreshGroupsSQL
  ) where

import Data.Text (Text)

import Arbiter.Core.Job.Schema (inFlightPredicate, jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query)

-- | @FOR UPDATE SKIP LOCKED@ over the groups rows, returning the locked keys for
-- the reaper to recompute. Claim-locked groups are skipped.
lockGroupsSQL :: Text -> Text -> Query Text
lockGroupsSQL schema tableName =
  let groupsTbl = jobQueueGroupsTable schema tableName
   in [sql|SELECT @{group_key :: CText} FROM ${groupsTbl} FOR UPDATE SKIP LOCKED|]

-- | Recompute the groups table, scoped to the locked keys from 'lockGroupsSQL'.
-- A separate statement, so its snapshot post-dates the lock and cannot clobber a
-- concurrent claim's @in_flight_until@. The INSERT only repairs missing rows.
refreshGroupsSQL :: Text -> Text -> [Text] -> Query ()
refreshGroupsSQL schema tableName keys =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      ifBucket = inFlightPredicate ""
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
        )
        INSERT INTO ${groupsTbl} (group_key, min_priority, min_id, job_count, ready_count, next_due, in_flight_until)
        SELECT group_key,
               MIN(priority), MIN(id), COUNT(*),
               COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended),
               MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended),
               MAX(not_visible_until) FILTER (WHERE ${ifBucket})
        FROM ${tbl}
        WHERE group_key IS NOT NULL
          AND group_key NOT IN (SELECT group_key FROM ${groupsTbl})
        GROUP BY group_key
        ON CONFLICT (group_key) DO NOTHING
      |]

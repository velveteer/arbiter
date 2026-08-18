{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Tree SQL templates.
module Arbiter.Core.Sql.Tree
  ( pauseChildrenSQL
  , resumeChildrenSQL
  , descendantsCte
  , lockedByIdsCte
  , lockJobTreesSQL
  , lockJobTreesFromRootSQL
  , cancelJobCascadeSQL
  , forceCancelJobSQL
  , deleteCancelledJobsSQL
  , selectCancelledReapableJobsSQL
  , cancelJobTreeSQL
  , tryWakeAncestorSQL
  , treeRollupIdsSQL
  , suspendJobSQL
  , resumeJobSQL
  , jobExistsSQL
  , getParentIdSQL
  , insertResultSQL
  , insertResultsBatchSQL
  , getResultsByParentSQL
  , getDLQChildErrorsByParentSQL
  , persistParentStateSQL
  , getParentStateSnapshotSQL
  , readChildResultsSQL
  ) where

import Data.Aeson (Value)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID.Types (UUID)

import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , cancelNotifyChannel
  , jobQueueDLQTable
  , jobQueueResultsTable
  , jobQueueTable
  )
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query)

-- | Pause all claimable jobs in a parent's subtree (set suspended = TRUE).
--
-- Walks the descendant tree and pauses every job that is currently
-- claimable (not in-flight, not already suspended). Naturally-suspended
-- rollup finalizers in the tree are left alone (they're already suspended
-- waiting for their own children). In-flight children are left alone so
-- their visibility timeout can expire normally if the worker crashes -
-- pausing them would break crash recovery.
pauseChildrenSQL :: Text -> Text -> Int64 -> Query ()
pauseChildrenSQL schema tableName parentId =
  let tbl = jobQueueTable schema tableName
      cte = childDescendantsCte tbl parentId
      locked = lockDescendantsCte tbl
   in [sql|
        ${cte},
        ${locked}
        UPDATE ${tbl}
        SET suspended = TRUE, updated_at = NOW()
        WHERE id IN (SELECT id FROM locked)
          AND NOT suspended
          AND (not_visible_until IS NULL OR not_visible_until <= NOW())
      |]

-- | Resume user-paused jobs in a parent's subtree (set suspended = FALSE).
--
-- Walks the descendant tree and resumes every suspended job that is NOT a
-- naturally-suspended rollup finalizer. A rollup is "naturally suspended"
-- when it has children still in the main queue - it should stay suspended
-- until those children complete, otherwise its handler would run with an
-- incomplete view of child results. Resuming the rollup's children directly
-- is correct: the rollup will wake itself when they finish.
resumeChildrenSQL :: Text -> Text -> Int64 -> Query ()
resumeChildrenSQL schema tableName parentId =
  let tbl = jobQueueTable schema tableName
      cte = childDescendantsCte tbl parentId
      locked = lockDescendantsCte tbl
   in [sql|
        ${cte},
        ${locked}
        UPDATE ${tbl} t
        SET suspended = FALSE, updated_at = NOW()
        WHERE t.id IN (SELECT id FROM locked)
          AND t.suspended = TRUE
          AND NOT (
            t.parent_state IS NOT NULL
            AND EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = t.id)
          )
      |]

-- | Recursive CTE binding @descendants@ to the rows @seed@ names and all of theirs.
-- A tree reaches each node from one seed row, so the walk does not deduplicate.
descendantsFromCte :: Text -> Query () -> Query ()
descendantsFromCte tbl seed =
  [sql|
    WITH RECURSIVE descendants AS (
      SELECT id FROM ${tbl} WHERE ${seed}
      UNION ALL
      SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
    )
  |]

-- | 'descendantsFromCte' seeded from a parent's children, excluding the parent itself.
childDescendantsCte :: Text -> Int64 -> Query ()
childDescendantsCte tbl parentId = descendantsFromCte tbl [sql|parent_id = #{parentId :: CInt8}|]

-- | 'descendantsFromCte' seeded from a job id. Shared by the cascade-delete templates.
descendantsCte :: Text -> Int64 -> Query ()
descendantsCte tbl jobId = descendantsFromCte tbl [sql|id = #{jobId :: CInt8}|]

-- | CTE binding @locked@ to the rows @descendants@ names, locked children-first to
-- match ack and force-cancel.
lockDescendantsCte :: Text -> Query ()
lockDescendantsCte tbl =
  [sql|
    locked AS (
      SELECT id FROM ${tbl} WHERE id IN (SELECT id FROM descendants)
      ORDER BY id DESC
      FOR UPDATE
    )
  |]

-- | CTE binding @locked@ to the ids named, in the descending order every multi-row
-- statement on a queue table takes its row locks in. The statement carries
-- @id IN (SELECT id FROM locked)@ so the CTE cannot be optimized away.
lockedByIdsCte :: Text -> [Int64] -> Query ()
lockedByIdsCte tbl ids =
  [sql|
    locked AS (
      SELECT id FROM ${tbl} WHERE id = ANY(#{ids :: [CInt8]})
      ORDER BY id DESC
      FOR UPDATE
    )
  |]

-- | Recursive CTEs binding @ancestors@ to the rows @seed@ names and every parent above
-- them, and @roots@ to the tops of those trees. Deduplicating, since two seeds under one
-- ancestor reach it twice.
rootsFromCte :: Text -> Query () -> Query ()
rootsFromCte tbl seed =
  [sql|
    WITH RECURSIVE ancestors AS (
      SELECT id, parent_id FROM ${tbl} WHERE ${seed}
      UNION
      SELECT j.id, j.parent_id FROM ${tbl} j JOIN ancestors a ON j.id = a.parent_id
    ),
    roots AS (
      SELECT id FROM ancestors WHERE parent_id IS NULL
    )
  |]

-- | 'descendantsFromCte' seeded from several roots, so their union locks in one pass.
-- Deduplicating, since a root named alongside its own ancestor is reached twice.
descendantsOfCte :: Text -> [Int64] -> Query ()
descendantsOfCte tbl jobIds =
  [sql|
    WITH RECURSIVE descendants AS (
      SELECT id FROM ${tbl} WHERE id = ANY(#{jobIds :: [CInt8]})
      UNION
      SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
    )
  |]

-- | Lock the named jobs and all their descendants descending, to match ack and
-- force-cancel. Several trees at once, so their union is taken in one pass.
lockJobTreesSQL :: Text -> Text -> [Int64] -> Query Int64
lockJobTreesSQL schema tableName jobIds =
  let tbl = jobQueueTable schema tableName
      cte = descendantsOfCte tbl jobIds
      locked = lockDescendantsCte tbl
   in [sql|
        ${cte},
        ${locked}
        SELECT count(*) AS @{count :: CInt8} FROM locked
      |]

-- | 'lockJobTreesSQL' widened to each named job's whole tree, so it covers what a tree
-- cancel goes on to delete rather than the subtree alone. The named ids seed the walk
-- down too, so an orphan locks its own subtree.
lockJobTreesFromRootSQL :: Text -> Text -> [Int64] -> Query Int64
lockJobTreesFromRootSQL schema tableName jobIds =
  let tbl = jobQueueTable schema tableName
      cte = rootsFromCte tbl [sql|id = ANY(#{jobIds :: [CInt8]})|]
      locked = lockDescendantsCte tbl
   in [sql|
        ${cte},
        descendants AS (
          SELECT id FROM ${tbl} WHERE id = ANY(#{jobIds :: [CInt8]}) OR id IN (SELECT id FROM roots)
          UNION
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        ),
        ${locked}
        SELECT count(*) AS @{count :: CInt8} FROM locked
      |]

-- | Cancel a job and all its descendants recursively, locking descending to match
-- ack and force-cancel.
cancelJobCascadeSQL :: Text -> Text -> Int64 -> Query Int64
cancelJobCascadeSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
      cte = descendantsCte tbl jobId
      locked = lockDescendantsCte tbl
   in [sql|
        ${cte},
        ${locked},
        deleted AS (
          DELETE FROM ${tbl} WHERE id IN (SELECT id FROM locked)
          RETURNING id
        )
        SELECT count(*) AS @{count :: CInt8} FROM deleted
      |]

-- | Force-cancel a job subtree: flag still-live claimed jobs (bumping the claim token to void their claim), delete the rest, and NOTIFY every claimed job affected.
forceCancelJobSQL :: SchemaName -> TableName -> Int64 -> Query Int64
forceCancelJobSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
      cte = descendantsCte tbl jobId
      chan = T.replace "'" "''" (cancelNotifyChannel schema tableName)
   in [sql|
        ${cte},
        locked AS (
          SELECT id, claimed_by, not_visible_until
          FROM ${tbl}
          WHERE id IN (SELECT id FROM descendants)
          ORDER BY id DESC
          FOR UPDATE
        ),
        cancelled AS (
          UPDATE ${tbl} t SET cancel_requested_at = NOW(), claim_seq = t.claim_seq + 1
          FROM locked l
          WHERE t.id = l.id
            AND l.claimed_by IS NOT NULL
            AND l.not_visible_until IS NOT NULL AND l.not_visible_until > NOW()
          RETURNING t.id, t.claimed_by
        ),
        deleted AS (
          DELETE FROM ${tbl} t
          USING locked l
          WHERE t.id = l.id
            AND (l.claimed_by IS NULL OR l.not_visible_until IS NULL OR l.not_visible_until <= NOW())
          RETURNING t.id, t.claimed_by
        ),
        notif AS (
          SELECT pg_notify(
            '${chan}',
            json_build_object('worker_id', n.claimed_by, 'job_id', n.id)::text
          )
          FROM (
            SELECT id, claimed_by FROM cancelled
            UNION ALL
            SELECT id, claimed_by FROM deleted WHERE claimed_by IS NOT NULL
          ) n
        )
        SELECT ((SELECT count(*) FROM cancelled) + (SELECT count(*) FROM deleted))::int8 AS @{count :: CInt8}
        WHERE (SELECT count(*) FROM notif) >= 0
      |]

-- | Delete force-cancel-flagged jobs @owner@ holds or no live lease holds, locking descending to match ack and force-cancel, returning each one's parent id.
deleteCancelledJobsSQL :: SchemaName -> TableName -> Maybe UUID -> [Int64] -> Query (Int64, Maybe Int64)
deleteCancelledJobsSQL schema tableName owner jobIds =
  let tbl = jobQueueTable schema tableName
   in [sql|WITH locked AS (SELECT id FROM ${tbl} WHERE id = ANY(#{jobIds :: [CInt8]}) AND cancel_requested_at IS NOT NULL AND (claimed_by = #{owner :: Maybe CUuid} OR not_visible_until IS NULL OR not_visible_until <= NOW()) ORDER BY id DESC FOR UPDATE) DELETE FROM ${tbl} WHERE id IN (SELECT id FROM locked) RETURNING @{id :: CInt8}, @{parent_id :: Maybe CInt8}|]

-- | Flagged jobs whose lease has lapsed, so the claiming worker is no longer
-- heartbeating and the reaper should delete them and resume their parents.
selectCancelledReapableJobsSQL :: SchemaName -> TableName -> Int -> Query Int64
selectCancelledReapableJobsSQL schema tableName limit =
  let tbl = jobQueueTable schema tableName
      lim = T.pack (show limit)
   in [sql|
        SELECT @{id :: CInt8}
        FROM ${tbl}
        WHERE cancel_requested_at IS NOT NULL
          AND (not_visible_until IS NULL OR not_visible_until <= NOW())
        ORDER BY id ASC
        LIMIT ${lim}
      |]

-- | Cancel an entire job tree by walking up from any node to the root,
-- then cascade-deleting everything from the root down, locking descending to
-- match ack and force-cancel.
cancelJobTreeSQL :: Text -> Text -> Int64 -> Query Int64
cancelJobTreeSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
      cte = rootsFromCte tbl [sql|id = #{jobId :: CInt8}|]
      locked = lockDescendantsCte tbl
   in [sql|
        ${cte},
        descendants AS (
          SELECT id FROM ${tbl} WHERE id IN (SELECT id FROM roots)
          UNION ALL
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        ),
        ${locked},
        deleted AS (
          DELETE FROM ${tbl} WHERE id IN (SELECT id FROM locked)
          RETURNING id
        )
        SELECT count(*) AS @{count :: CInt8} FROM deleted
      |]

-- | Try to wake a suspended ancestor when all its children are gone.
--
-- Resumes the parent for a completion round (sets suspended = FALSE).
-- Only wakes if the parent is suspended and has no remaining children
-- in the main queue.
tryWakeAncestorSQL :: Text -> Text -> Int64 -> Query ()
tryWakeAncestorSQL schema tableName ancestorId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET suspended = FALSE, updated_at = NOW()
        WHERE id = #{ancestorId :: CInt8}
          AND suspended = TRUE
          AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = #{ancestorId :: CInt8})
      |]

-- | Rollup finalizer ids in a job's tree, the job itself included.
--
-- Used before a DLQ move to identify the rollup nodes whose results need
-- persisting into @parent_state@ before deletion takes them.
treeRollupIdsSQL :: Text -> Text -> Int64 -> Query Int64
treeRollupIdsSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        WITH RECURSIVE descendants AS (
          SELECT id, parent_state FROM ${tbl} WHERE id = #{jobId :: CInt8}
          UNION ALL
          SELECT j.id, j.parent_state FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        )
        SELECT id AS @{result :: CInt8} FROM descendants WHERE parent_state IS NOT NULL
      |]

-- | Suspend a job (make it unclaimable).
--
-- Only suspends non-in-flight jobs (not currently being processed by workers).
--
-- Returns: number of rows updated (0 if job doesn't exist, is in-flight, or already suspended)
suspendJobSQL :: Text -> Text -> Int64 -> Query ()
suspendJobSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET suspended = TRUE, updated_at = NOW()
        WHERE id = #{jobId :: CInt8}
          AND NOT suspended
          AND NOT (attempts > 0 AND not_visible_until IS NOT NULL AND not_visible_until > NOW())
      |]

-- | Resume a suspended job (make it claimable again).
--
-- Refuses to resume a rollup finalizer that still has children in the main
-- queue, preventing premature handler execution. Children in the DLQ are
-- considered terminal - the finalizer's handler receives DLQ errors via
-- 'readChildResultsSQL' and can decide how to handle them.
--
-- Returns: number of rows updated (0 if job doesn't exist, isn't suspended,
--          or is a finalizer with remaining children in the main queue)
resumeJobSQL :: Text -> Text -> Int64 -> Query ()
resumeJobSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET suspended = FALSE, updated_at = NOW()
        WHERE id = #{jobId :: CInt8} AND suspended = TRUE
          AND NOT (
            parent_state IS NOT NULL
            AND EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ${tbl}.id)
          )
      |]

-- | Check whether a job with the given id exists.
jobExistsSQL :: Text -> Text -> Int64 -> Query Bool
jobExistsSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|SELECT EXISTS (SELECT 1 FROM ${tbl} WHERE id = #{jobId :: CInt8}) AS @{result :: CBool}|]

-- | Fetch just the parent_id for a given job.
--
-- Returns: single row with parent_id (NULL if no parent or job doesn't exist)
getParentIdSQL :: Text -> Text -> Int64 -> Query (Maybe Int64)
getParentIdSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|SELECT @{parent_id :: Maybe CInt8} FROM ${tbl} WHERE id = #{jobId :: CInt8}|]

-- ---------------------------------------------------------------------------
-- Results Table Operations
-- ---------------------------------------------------------------------------

-- | Insert a child's result into the results table.
insertResultSQL :: Text -> Text -> Int64 -> Int64 -> Value -> Query ()
insertResultSQL schema tableName parentId childId result =
  let resultsTbl = jobQueueResultsTable schema tableName
   in [sql|
        INSERT INTO ${resultsTbl} (parent_id, child_id, result)
        VALUES (#{parentId :: CInt8}, #{childId :: CInt8}, #{result :: CJsonb})
        ON CONFLICT (parent_id, child_id) DO UPDATE SET result = EXCLUDED.result
      |]

-- | 'insertResultSQL' for several children in one statement.
insertResultsBatchSQL :: Text -> Text -> [Int64] -> [Int64] -> [Value] -> Query ()
insertResultsBatchSQL schema tableName parentIds childIds results =
  let resultsTbl = jobQueueResultsTable schema tableName
   in [sql|
        INSERT INTO ${resultsTbl} (parent_id, child_id, result)
        SELECT parent_id, child_id, result FROM (
          SELECT unnest(#{parentIds :: [CInt8]}::bigint[]) AS parent_id,
                 unnest(#{childIds :: [CInt8]}::bigint[]) AS child_id,
                 unnest(#{results :: [CJsonb]}::jsonb[]) AS result
        ) src
        ON CONFLICT (parent_id, child_id) DO UPDATE SET result = EXCLUDED.result
      |]

-- | Get all child results for a parent from the results table.
getResultsByParentSQL :: Text -> Text -> Int64 -> Query (Int64, Value)
getResultsByParentSQL schema tableName parentId =
  let resultsTbl = jobQueueResultsTable schema tableName
   in [sql|
        SELECT @{child_id :: CInt8}, @{result :: CJsonb} FROM ${resultsTbl} WHERE parent_id = #{parentId :: CInt8}
      |]

-- | Get DLQ child errors for a parent, one @(job_id, last_error)@ row per DLQ'd child.
getDLQChildErrorsByParentSQL :: Text -> Text -> Int64 -> Query (Int64, Maybe Text)
getDLQChildErrorsByParentSQL schema tableName parentId =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|
        SELECT @{job_id :: CInt8}, @{last_error :: Maybe CText} FROM ${dlqTbl} WHERE parent_id = #{parentId :: CInt8}
      |]

-- | Snapshot results into parent_state before DLQ move.
persistParentStateSQL :: Text -> Text -> Value -> Int64 -> Query ()
persistParentStateSQL schema tableName parentState jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl} SET parent_state = #{parentState :: CJsonb}, updated_at = NOW() WHERE id = #{jobId :: CInt8}
      |]

-- | Read the raw parent_state snapshot from the DB (jsonb, may be NULL).
getParentStateSnapshotSQL :: Text -> Text -> Int64 -> Query (Maybe Value)
getParentStateSnapshotSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|SELECT @{parent_state :: Maybe CJsonb} FROM ${tbl} WHERE id = #{jobId :: CInt8}|]

-- | Read all child result data for a rollup finalizer in a single query.
--
-- Combines results table, DLQ child errors, and parent_state snapshot
-- into a tagged UNION ALL. Tags: @r@ = result, @e@ = DLQ error,
-- @s@ = parent_state snapshot.
readChildResultsSQL :: Text -> Text -> Int64 -> Query (Text, Maybe Int64, Maybe Value, Maybe Text, Maybe Int64)
readChildResultsSQL schema tableName parentId =
  let resultsTbl = jobQueueResultsTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      tbl = jobQueueTable schema tableName
   in [sql|
        SELECT 'r'::text AS @{source :: CText}, @{child_id :: Maybe CInt8}, @{result :: Maybe CJsonb}, NULL::text AS @{error :: Maybe CText}, NULL::bigint AS @{dlq_pk :: Maybe CInt8} FROM ${resultsTbl} WHERE parent_id = #{parentId :: CInt8}
        UNION ALL
        SELECT 'e' AS source, job_id AS child_id, NULL::jsonb AS result, last_error AS error, id AS dlq_pk FROM ${dlqTbl} WHERE parent_id = #{parentId :: CInt8}
        UNION ALL
        SELECT 's' AS source, NULL::bigint AS child_id, parent_state AS result, NULL::text AS error, NULL::bigint AS dlq_pk FROM ${tbl} WHERE id = #{parentId :: CInt8} AND parent_state IS NOT NULL
      |]

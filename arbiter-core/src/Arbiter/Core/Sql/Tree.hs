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
  , getParentIdsSQL
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
import Arbiter.Core.SqlLiterals (textLiteral)

-- | Suspend every claimable job in a parent's subtree. A finalizer already waiting on
-- its own children is left as it stands. An in-flight job is left as it stands.
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

-- | Resume the suspended jobs in a parent's subtree. A finalizer with children still
-- in the queue stays suspended and wakes itself once they finish.
resumeChildrenSQL :: Text -> Text -> Int64 -> Query ()
resumeChildrenSQL schema tableName parentId =
  let tbl = jobQueueTable schema tableName
      cte = childDescendantsCte tbl parentId
      locked = lockDescendantsCte tbl
   in [sql|
        ${cte},
        ${locked}
        UPDATE ${tbl} job
        SET suspended = FALSE, updated_at = NOW()
        WHERE job.id IN (SELECT id FROM locked)
          AND job.suspended = TRUE
          AND NOT (
            job.parent_state IS NOT NULL
            AND EXISTS (SELECT 1 FROM ${tbl} child WHERE child.parent_id = job.id)
          )
      |]

-- | Recursive CTE binding @descendants@ to the rows @seed@ names and all of theirs.
-- The walk does not deduplicate.
descendantsFromCte :: Text -> Query () -> Query ()
descendantsFromCte tbl seed =
  [sql|
    WITH RECURSIVE descendants AS (
      SELECT id FROM ${tbl} WHERE ${seed}
      UNION ALL
      SELECT job.id FROM ${tbl} job JOIN descendants descendant ON job.parent_id = descendant.id
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

-- | CTE binding @locked@ to the ids named, in the descending lock order of every
-- multi-row statement on a queue table. The statement carries
-- @id IN (SELECT id FROM locked)@ to keep the CTE.
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
-- them, and @roots@ to the tops of those trees. Deduplicating.
rootsFromCte :: Text -> Query () -> Query ()
rootsFromCte tbl seed =
  [sql|
    WITH RECURSIVE ancestors AS (
      SELECT id, parent_id FROM ${tbl} WHERE ${seed}
      UNION
      SELECT job.id, job.parent_id FROM ${tbl} job JOIN ancestors ancestor ON job.id = ancestor.parent_id
    ),
    roots AS (
      SELECT id FROM ancestors WHERE parent_id IS NULL
    )
  |]

-- | 'descendantsFromCte' seeded from several roots. Their union locks in one pass.
-- Deduplicating.
descendantsOfCte :: Text -> [Int64] -> Query ()
descendantsOfCte tbl jobIds =
  [sql|
    WITH RECURSIVE descendants AS (
      SELECT id FROM ${tbl} WHERE id = ANY(#{jobIds :: [CInt8]})
      UNION
      SELECT job.id FROM ${tbl} job JOIN descendants descendant ON job.parent_id = descendant.id
    )
  |]

-- | Lock the named jobs and all their descendants descending, to match ack and
-- force-cancel. Several trees at once, their union in one pass.
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

-- | Extend 'lockJobTreesSQL' to the complete tree of each named job. This locks
-- all rows that tree cancellation can delete. The named identifiers also start
-- the downward walk. An orphan locks its subtree.
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
          SELECT job.id FROM ${tbl} job JOIN descendants descendant ON job.parent_id = descendant.id
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

-- | Force-cancel a job subtree. Flags still-live claimed jobs and bumps their claim
-- token, deletes the rest, and NOTIFYs every claimed job affected.
forceCancelJobSQL :: SchemaName -> TableName -> Int64 -> Query Int64
forceCancelJobSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
      cte = descendantsCte tbl jobId
      chan = textLiteral (cancelNotifyChannel schema tableName)
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
          UPDATE ${tbl} job SET cancel_requested_at = NOW(), claim_seq = job.claim_seq + 1
          FROM locked held
          WHERE job.id = held.id
            AND held.claimed_by IS NOT NULL
            AND held.not_visible_until IS NOT NULL AND held.not_visible_until > NOW()
          RETURNING job.id, job.claimed_by
        ),
        deleted AS (
          DELETE FROM ${tbl} job
          USING locked held
          WHERE job.id = held.id
            AND (held.claimed_by IS NULL OR held.not_visible_until IS NULL OR held.not_visible_until <= NOW())
          RETURNING job.id, job.claimed_by
        ),
        notif AS (
          SELECT pg_notify(
            ${chan},
            json_build_object('worker_id', notified.claimed_by, 'job_id', notified.id)::text
          )
          FROM (
            SELECT id, claimed_by FROM cancelled
            UNION ALL
            SELECT id, claimed_by FROM deleted WHERE claimed_by IS NOT NULL
          ) notified
        )
        SELECT ((SELECT count(*) FROM cancelled) + (SELECT count(*) FROM deleted))::int8 AS @{count :: CInt8}
        WHERE (SELECT count(*) FROM notif) >= 0
      |]

-- | Delete force-cancel-flagged jobs @owner@ holds or no live lease holds, locking
-- descending to match ack and force-cancel, returning each one's parent id.
deleteCancelledJobsSQL :: SchemaName -> TableName -> Maybe UUID -> [Int64] -> Query (Int64, Maybe Int64)
deleteCancelledJobsSQL schema tableName owner jobIds =
  let tbl = jobQueueTable schema tableName
   in [sql|
        WITH locked AS (
          SELECT id FROM ${tbl}
          WHERE id = ANY(#{jobIds :: [CInt8]})
            AND cancel_requested_at IS NOT NULL
            AND (claimed_by = #{owner :: Maybe CUuid} OR not_visible_until IS NULL OR not_visible_until <= NOW())
          ORDER BY id DESC
          FOR UPDATE
        )
        DELETE FROM ${tbl} WHERE id IN (SELECT id FROM locked)
        RETURNING @{id :: CInt8}, @{parent_id :: Maybe CInt8}
      |]

-- | Flagged jobs whose lease has lapsed. The reaper deletes them and resumes their
-- parents.
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
          SELECT job.id FROM ${tbl} job JOIN descendants descendant ON job.parent_id = descendant.id
        ),
        ${locked},
        deleted AS (
          DELETE FROM ${tbl} WHERE id IN (SELECT id FROM locked)
          RETURNING id
        )
        SELECT count(*) AS @{count :: CInt8} FROM deleted
      |]

-- | Resume a suspended parent for its completion round, once no child of it is left in
-- the main queue.
tryWakeAncestorSQL :: Text -> Text -> Int64 -> Query ()
tryWakeAncestorSQL schema tableName ancestorId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET suspended = FALSE, updated_at = NOW()
        WHERE id = #{ancestorId :: CInt8}
          AND suspended = TRUE
          AND NOT EXISTS (SELECT 1 FROM ${tbl} child WHERE child.parent_id = #{ancestorId :: CInt8})
      |]

-- | Rollup finalizer ids in a job's tree, the job itself included. Read before a DLQ
-- move.
treeRollupIdsSQL :: Text -> Text -> Int64 -> Query Int64
treeRollupIdsSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        WITH RECURSIVE descendants AS (
          SELECT id, parent_state FROM ${tbl} WHERE id = #{jobId :: CInt8}
          UNION ALL
          SELECT job.id, job.parent_state FROM ${tbl} job JOIN descendants descendant ON job.parent_id = descendant.id
        )
        SELECT id AS @{result :: CInt8} FROM descendants WHERE parent_state IS NOT NULL
      |]

-- | Suspend a job, making it unclaimable. Refuses an in-flight job.
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

-- | Resume a suspended job. Refuses a finalizer with children still in the main queue.
-- A child in the DLQ is terminal. The finalizer reads its error through
-- 'readChildResultsSQL' and decides for itself.
resumeJobSQL :: Text -> Text -> Int64 -> Query ()
resumeJobSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET suspended = FALSE, updated_at = NOW()
        WHERE id = #{jobId :: CInt8} AND suspended = TRUE
          AND NOT (
            parent_state IS NOT NULL
            AND EXISTS (SELECT 1 FROM ${tbl} child WHERE child.parent_id = ${tbl}.id)
          )
      |]

-- | Check whether a job with the given id exists.
jobExistsSQL :: Text -> Text -> Int64 -> Query Bool
jobExistsSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|SELECT EXISTS (SELECT 1 FROM ${tbl} WHERE id = #{jobId :: CInt8}) AS @{result :: CBool}|]

-- | Fetch a job's parent id.
getParentIdSQL :: Text -> Text -> Int64 -> Query (Maybe Int64)
getParentIdSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|SELECT @{parent_id :: Maybe CInt8} FROM ${tbl} WHERE id = #{jobId :: CInt8}|]

-- | Fetch the parent ids of several jobs.
getParentIdsSQL :: Text -> Text -> [Int64] -> Query (Maybe Int64)
getParentIdsSQL schema tableName jobIds =
  let tbl = jobQueueTable schema tableName
   in [sql|SELECT @{parent_id :: Maybe CInt8} FROM ${tbl} WHERE id = ANY(#{jobIds :: [CInt8]})|]

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

-- | Snapshot child results into @parent_state@ before a DLQ move.
persistParentStateSQL :: Text -> Text -> Value -> Int64 -> Query ()
persistParentStateSQL schema tableName parentState jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl} SET parent_state = #{parentState :: CJsonb}, updated_at = NOW() WHERE id = #{jobId :: CInt8}
      |]

-- | Read a job's raw @parent_state@ snapshot.
getParentStateSnapshotSQL :: Text -> Text -> Int64 -> Query (Maybe Value)
getParentStateSnapshotSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|SELECT @{parent_state :: Maybe CJsonb} FROM ${tbl} WHERE id = #{jobId :: CInt8}|]

-- | A rollup finalizer's child results, DLQ child errors, and @parent_state@ snapshot in
-- one query, tagged @r@, @e@ and @s@ respectively.
readChildResultsSQL :: Text -> Text -> Int64 -> Query (Text, Maybe Int64, Maybe Value, Maybe Text, Maybe Int64)
readChildResultsSQL schema tableName parentId =
  let resultsTbl = jobQueueResultsTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      tbl = jobQueueTable schema tableName
   in [sql|
        SELECT 'r'::text AS @{source :: CText}, @{child_id :: Maybe CInt8}, @{result :: Maybe CJsonb},
               NULL::text AS @{error :: Maybe CText}, NULL::bigint AS @{dlq_pk :: Maybe CInt8}
        FROM ${resultsTbl} WHERE parent_id = #{parentId :: CInt8}
        UNION ALL
        SELECT 'e' AS source, job_id AS child_id, NULL::jsonb AS result, last_error AS error, id AS dlq_pk
        FROM ${dlqTbl} WHERE parent_id = #{parentId :: CInt8}
        UNION ALL
        SELECT 's' AS source, NULL::bigint AS child_id, parent_state AS result, NULL::text AS error,
               NULL::bigint AS dlq_pk
        FROM ${tbl} WHERE id = #{parentId :: CInt8} AND parent_state IS NOT NULL
      |]

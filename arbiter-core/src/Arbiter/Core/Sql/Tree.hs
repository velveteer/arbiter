{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Tree SQL templates.
module Arbiter.Core.Sql.Tree
  ( pauseChildrenSQL
  , resumeChildrenSQL
  , descendantsCte
  , cancelJobCascadeSQL
  , forceCancelJobSQL
  , cancelJobTreeSQL
  , tryWakeAncestorSQL
  , descendantRollupIdsSQL
  , suspendJobSQL
  , resumeJobSQL
  , parentExistsSQL
  , getParentIdSQL
  , insertResultSQL
  , getResultsByParentSQL
  , getDLQChildErrorsByParentSQL
  , persistParentStateSQL
  , getParentStateSnapshotSQL
  , readChildResultsSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Job.Schema
  ( SchemaName
  , TableName
  , cancelNotifyChannel
  , jobQueueDLQTable
  , jobQueueResultsTable
  , jobQueueTable
  )

-- | Pause all claimable jobs in a parent's subtree (set suspended = TRUE).
--
-- Walks the descendant tree and pauses every job that is currently
-- claimable (not in-flight, not already suspended). Naturally-suspended
-- rollup finalizers in the tree are left alone (they're already suspended
-- waiting for their own children). In-flight children are left alone so
-- their visibility timeout can expire normally if the worker crashes -
-- pausing them would break crash recovery.
--
-- Parameters: parent_id
pauseChildrenSQL :: Text -> Text -> Text
pauseChildrenSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH RECURSIVE descendants AS (
          SELECT id FROM ${tbl} WHERE parent_id = ?
          UNION ALL
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        )
        UPDATE ${tbl}
        SET suspended = TRUE, updated_at = NOW()
        WHERE id IN (SELECT id FROM descendants)
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
--
-- Parameters: parent_id
resumeChildrenSQL :: Text -> Text -> Text
resumeChildrenSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH RECURSIVE descendants AS (
          SELECT id FROM ${tbl} WHERE parent_id = ?
          UNION ALL
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        )
        UPDATE ${tbl} t
        SET suspended = FALSE, updated_at = NOW()
        WHERE t.id IN (SELECT id FROM descendants)
          AND t.suspended = TRUE
          AND NOT (
            t.parent_state IS NOT NULL
            AND EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = t.id)
          )
      |]

-- | Recursive CTE binding @descendants@ to a job id and all its descendants.
-- Shared by the cascade-delete templates. Binds one @?@ (the root job id).
descendantsCte :: Text -> Text
descendantsCte tbl =
  [text|
    WITH RECURSIVE descendants AS (
      SELECT id FROM ${tbl} WHERE id = ?
      UNION ALL
      SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
    )
  |]

-- | Cancel a job and all its descendants recursively.
--
-- Parameters: job_id
cancelJobCascadeSQL :: Text -> Text -> Text
cancelJobCascadeSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      cte = descendantsCte tbl
   in [text|
        ${cte},
        deleted AS (
          DELETE FROM ${tbl} WHERE id IN (SELECT id FROM descendants)
          RETURNING id
        )
        SELECT count(*) FROM deleted
      |]

-- | Cascade-delete like 'cancelJobCascadeSQL', plus a per-row cancel NOTIFY for each deleted claimed job.
-- Parameters: job_id
forceCancelJobSQL :: SchemaName -> TableName -> Text
forceCancelJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      cte = descendantsCte tbl
      chan = T.replace "'" "''" (cancelNotifyChannel schema tableName)
   in [text|
        ${cte},
        deleted AS (
          DELETE FROM ${tbl} WHERE id IN (SELECT id FROM descendants)
          RETURNING id, claimed_by
        ),
        notif AS (
          SELECT pg_notify(
            '${chan}',
            json_build_object('worker_id', d.claimed_by, 'job_id', d.id)::text
          )
          FROM deleted d
          WHERE d.claimed_by IS NOT NULL
        )
        SELECT count(*)::int8 FROM deleted
        WHERE (SELECT count(*) FROM notif) >= 0
      |]

-- | Cancel an entire job tree by walking up from any node to the root,
-- then cascade-deleting everything from the root down.
--
-- Parameters: job_id
cancelJobTreeSQL :: Text -> Text -> Text
cancelJobTreeSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH RECURSIVE
        ancestors AS (
          SELECT id, parent_id FROM ${tbl} WHERE id = ?
          UNION ALL
          SELECT j.id, j.parent_id FROM ${tbl} j JOIN ancestors a ON j.id = a.parent_id
        ),
        root AS (
          SELECT id FROM ancestors WHERE parent_id IS NULL
        ),
        descendants AS (
          SELECT id FROM ${tbl} WHERE id = (SELECT id FROM root)
          UNION ALL
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        ),
        deleted AS (
          DELETE FROM ${tbl} WHERE id IN (SELECT id FROM descendants)
          RETURNING id
        )
        SELECT count(*) FROM deleted
      |]

-- | Try to wake a suspended ancestor when all its children are gone.
--
-- Resumes the parent for a completion round (sets suspended = FALSE).
-- Only wakes if the parent is suspended and has no remaining children
-- in the main queue.
--
-- Parameters: ancestor_id (repeated 2 times)
tryWakeAncestorSQL :: Text -> Text -> Text
tryWakeAncestorSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET suspended = FALSE, updated_at = NOW()
        WHERE id = ?
          AND suspended = TRUE
          AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ?)
      |]

-- | Cascade all descendants of a rollup parent to the DLQ.
--
-- Recursively finds all descendants and moves them from the main queue
-- to the DLQ in a single operation. Used when a rollup parent is moved
-- to DLQ to prevent orphaned children from hitting FK violations on
-- the results table.
--
-- Parameters: parent_job_id, error_message

-- | Find descendant rollup finalizer IDs for snapshot preservation.
--
-- Used before cascade-DLQ to identify intermediate rollup nodes that
-- need their results persisted into @parent_state@ before deletion.
--
-- Parameters: parent_job_id
descendantRollupIdsSQL :: Text -> Text -> Text
descendantRollupIdsSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH RECURSIVE descendants AS (
          SELECT id, parent_state FROM ${tbl} WHERE parent_id = ?
          UNION ALL
          SELECT j.id, j.parent_state FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        )
        SELECT id AS result FROM descendants WHERE parent_state IS NOT NULL
      |]

-- | Suspend a job (make it unclaimable).
--
-- Only suspends non-in-flight jobs (not currently being processed by workers).
--
-- Parameters: job_id
-- Returns: number of rows updated (0 if job doesn't exist, is in-flight, or already suspended)
suspendJobSQL :: Text -> Text -> Text
suspendJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET suspended = TRUE, updated_at = NOW()
        WHERE id = ?
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
-- Parameters: job_id
-- Returns: number of rows updated (0 if job doesn't exist, isn't suspended,
--          or is a finalizer with remaining children in the main queue)
resumeJobSQL :: Text -> Text -> Text
resumeJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET suspended = FALSE, updated_at = NOW()
        WHERE id = ? AND suspended = TRUE
          AND NOT (
            parent_state IS NOT NULL
            AND EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = ${tbl}.id)
          )
      |]

-- | Check whether a parent job exists.
--
-- Parameters: parent_id
-- Returns: single row with a boolean
parentExistsSQL :: Text -> Text -> Text
parentExistsSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|SELECT EXISTS (SELECT 1 FROM ${tbl} WHERE id = ?) AS result|]

-- | Fetch just the parent_id for a given job.
--
-- Parameters: job_id
-- Returns: single row with parent_id (NULL if no parent or job doesn't exist)
getParentIdSQL :: Text -> Text -> Text
getParentIdSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|SELECT parent_id FROM ${tbl} WHERE id = ?|]

-- ---------------------------------------------------------------------------
-- Results Table Operations
-- ---------------------------------------------------------------------------

-- | Insert a child's result into the results table.
--
-- Parameters: parent_id (bigint), child_id (bigint), result (jsonb)
insertResultSQL :: Text -> Text -> Text
insertResultSQL schema tableName =
  let resultsTbl = jobQueueResultsTable schema tableName
   in [text|
        INSERT INTO ${resultsTbl} (parent_id, child_id, result)
        VALUES (?, ?, ?)
        ON CONFLICT (parent_id, child_id) DO UPDATE SET result = EXCLUDED.result
      |]

-- | Get all child results for a parent from the results table.
--
-- Parameters: parent_id (bigint)
-- Returns: rows of (child_id bigint, result jsonb)
getResultsByParentSQL :: Text -> Text -> Text
getResultsByParentSQL schema tableName =
  let resultsTbl = jobQueueResultsTable schema tableName
   in [text|
        SELECT child_id, result FROM ${resultsTbl} WHERE parent_id = ?
      |]

-- | Get DLQ child errors for a parent.
--
-- Returns rows of (job_id bigint, last_error text) for each DLQ'd child.
--
-- Parameters: parent_id (bigint)
getDLQChildErrorsByParentSQL :: Text -> Text -> Text
getDLQChildErrorsByParentSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|
        SELECT job_id, last_error FROM ${dlqTbl} WHERE parent_id = ?
      |]

-- | Snapshot results into parent_state before DLQ move.
--
-- Parameters: parent_state (jsonb), job_id (bigint)
persistParentStateSQL :: Text -> Text -> Text
persistParentStateSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl} SET parent_state = ?, updated_at = NOW() WHERE id = ?
      |]

-- | Read the raw parent_state snapshot from the DB.
--
-- Parameters: job_id (bigint)
-- Returns: single row with parent_state (jsonb, may be NULL)
getParentStateSnapshotSQL :: Text -> Text -> Text
getParentStateSnapshotSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|SELECT parent_state FROM ${tbl} WHERE id = ?|]

-- | Read all child result data for a rollup finalizer in a single query.
--
-- Combines results table, DLQ child errors, and parent_state snapshot
-- into a tagged UNION ALL. Tags: @'r'@ = result, @'e'@ = DLQ error,
-- @'s'@ = parent_state snapshot.
--
-- Parameters: parent_id (bigint) × 3
-- Returns: tagged rows (source, child_id, result_jsonb, error_text, dlq_pk)
readChildResultsSQL :: Text -> Text -> Text
readChildResultsSQL schema tableName =
  let resultsTbl = jobQueueResultsTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      tbl = jobQueueTable schema tableName
   in [text|
        SELECT 'r'::text AS source, child_id, result, NULL::text AS error, NULL::bigint AS dlq_pk FROM ${resultsTbl} WHERE parent_id = ?
        UNION ALL
        SELECT 'e' AS source, job_id AS child_id, NULL::jsonb AS result, last_error AS error, id AS dlq_pk FROM ${dlqTbl} WHERE parent_id = ?
        UNION ALL
        SELECT 's' AS source, NULL::bigint AS child_id, parent_state AS result, NULL::text AS error, NULL::bigint AS dlq_pk FROM ${tbl} WHERE id = ? AND parent_state IS NOT NULL
      |]

-- ---------------------------------------------------------------------------
-- Groups Table Operations
-- ---------------------------------------------------------------------------

-- | @FOR UPDATE SKIP LOCKED@ over the groups rows, returning the locked keys for
-- the reaper to recompute. Claim-locked groups are skipped.

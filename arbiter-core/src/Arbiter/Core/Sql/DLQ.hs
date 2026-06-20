{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | DLQ SQL templates.
module Arbiter.Core.Sql.DLQ
  ( moveToDLQSQL
  , selectExhaustedJobsSQL
  , retryFromDLQSQL
  , dlqJobExistsSQL
  , deleteDLQJobSQL
  , moveToDLQBatchSQL
  , deleteDLQJobsBatchSQL
  , cascadeChildrenToDLQSQL
  , countDLQChildrenBatchSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Job.Schema (jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.Job.Types (defaultMaxAttempts)
import Arbiter.Core.Sql.Jobs (dlqCarriedCols, jobColumns)

-- | SQL template for moving job to DLQ atomically
--
-- This preserves ALL job fields (complete snapshot) plus DLQ metadata.
-- The operation is atomic: the job is deleted from the main queue and
-- inserted into the DLQ in a single statement. The final error message
-- is passed as a parameter to capture the error that caused the DLQ move.
--
-- Parameters: job_id, attempts, last_error
moveToDLQSQL :: Text -> Text -> Text
moveToDLQSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      cols = dlqCarriedCols
   in [text|
        WITH deleted_job AS (
          DELETE FROM ${tbl}
          WHERE id = ? AND attempts = ?
          RETURNING *
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (
            job_id, ${cols}, last_error
          )
          SELECT
            id, ${cols}, ?
          FROM deleted_job
        )
        SELECT count(*) FROM deleted_job
      |]

-- | Select up to @limit@ claimable jobs whose attempts reached their limit,
-- with the scalar fields the tree-aware DLQ move needs. Each row is then moved
-- via 'moveToDLQFields'. The cap drains a large backlog over several passes
-- rather than fetching an unbounded set at once.
selectExhaustedJobsSQL :: Text -> Text -> Int -> Text
selectExhaustedJobsSQL schema tableName limit =
  let tbl = jobQueueTable schema tableName
      dma = T.pack (show defaultMaxAttempts)
      lim = T.pack (show limit)
   in [text|
        SELECT id, attempts, parent_id, (parent_state IS NOT NULL) AS is_rollup
        FROM ${tbl}
        WHERE NOT suspended
          AND attempts >= COALESCE(max_attempts, ${dma})
          AND (not_visible_until IS NULL OR not_visible_until <= NOW())
        ORDER BY id ASC
        LIMIT ${lim}
      |]

-- | SQL template for retrying a job from DLQ (tree-aware)
--
-- Tree-aware retry behavior - retrying any member of a DLQ'd tree recovers
-- the entire tree in a single operation:
--
-- 1. If the target is a child whose parent is in the DLQ (not in main queue),
--    the parent is auto-retried as @suspended = TRUE@, and ALL DLQ'd siblings
--    are auto-retried too. The parent waits for children to complete.
--
-- 2. If the target is a rollup finalizer with DLQ'd children, ALL children
--    are auto-retried and the finalizer comes back as @suspended = TRUE@
--    (waits for children to complete). If no DLQ'd children exist, it comes
--    back as @suspended = FALSE@ (runs immediately with snapshot data).
--
-- 3. Refuses to retry if the tree root's parent_id references a job that no
--    longer exists in the main queue - prevents creating orphaned children.
--
-- Retried rollup finalizers get @suspended = TRUE@ when they have children
-- being retried alongside them. @dedup_key@ and @dedup_strategy@ are
-- intentionally dropped on retry (columns omitted → NULL defaults).
--
-- Parameters: id (the DLQ primary key)
retryFromDLQSQL :: Text -> Text -> Text
retryFromDLQSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
      tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in [text|
        WITH RECURSIVE
        target AS (
          SELECT * FROM ${dlqTbl} WHERE id = ?
        ),
        -- Walk up through DLQ ancestors to find the root of the tree.
        -- Stops when parent_id IS NULL, parent is in main queue, or
        -- parent is not found in DLQ (orphaned).
        ancestors AS (
          SELECT d.job_id, d.parent_id, 0 AS depth
          FROM ${dlqTbl} d
          WHERE d.job_id = (SELECT parent_id FROM target)
            AND (SELECT parent_id FROM target) IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE id = (SELECT parent_id FROM target))
          UNION ALL
          SELECT d.job_id, d.parent_id, a.depth + 1
          FROM ${dlqTbl} d
          JOIN ancestors a ON d.job_id = a.parent_id
          WHERE a.parent_id IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE id = a.parent_id)
        ),
        -- Root is the topmost DLQ ancestor, or the target itself
        root_job_id AS (
          SELECT COALESCE(
            (SELECT job_id FROM ancestors ORDER BY depth DESC LIMIT 1),
            (SELECT job_id FROM target)
          ) AS job_id
        ),
        -- Guard: root's parent must be NULL or exist in main queue
        can_retry AS (
          SELECT EXISTS (
            SELECT 1
            FROM root_job_id r
            JOIN ${dlqTbl} d ON d.job_id = r.job_id
            WHERE d.parent_id IS NULL
               OR EXISTS (SELECT 1 FROM ${tbl} WHERE id = d.parent_id)
          ) AS val
        ),
        -- Walk down from root to collect all DLQ tree members
        tree AS (
          SELECT d.id AS dlq_id, d.job_id, d.payload, d.group_key, d.priority,
                 d.max_attempts, d.parent_id, d.parent_state, d.rate_limit_key, d.rate_limit_prefix,
                 d.rate_limit_cost, d.concurrency_key, d.concurrency_prefix
          FROM ${dlqTbl} d
          WHERE d.job_id = (SELECT job_id FROM root_job_id)
          UNION ALL
          SELECT d.id AS dlq_id, d.job_id, d.payload, d.group_key, d.priority,
                 d.max_attempts, d.parent_id, d.parent_state, d.rate_limit_key, d.rate_limit_prefix,
                 d.rate_limit_cost, d.concurrency_key, d.concurrency_prefix
          FROM ${dlqTbl} d
          JOIN tree t ON d.parent_id = t.job_id
        ),
        -- Delete all tree members from DLQ (guarded by can_retry)
        deleted AS (
          DELETE FROM ${dlqTbl}
          WHERE id IN (SELECT dlq_id FROM tree)
            AND (SELECT val FROM can_retry)
          RETURNING job_id, payload, group_key, priority, max_attempts, parent_id, parent_state, rate_limit_key, rate_limit_prefix, rate_limit_cost, concurrency_key, concurrency_prefix
        ),
        -- Re-insert into main queue with computed suspended state:
        -- rollup finalizers are suspended if they have children (in this
        -- retry batch OR already in the main queue).
        inserted AS (
          INSERT INTO ${tbl} (id, payload, group_key, attempts, priority, max_attempts,
                              parent_id, parent_state, suspended, rate_limit_key, rate_limit_prefix,
                              rate_limit_cost, concurrency_key, concurrency_prefix)
          SELECT d.job_id, d.payload, d.group_key, 0, d.priority, d.max_attempts,
                 d.parent_id, d.parent_state,
                 CASE WHEN d.parent_state IS NOT NULL
                   THEN EXISTS (SELECT 1 FROM deleted c WHERE c.parent_id = d.job_id)
                     OR EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = d.job_id)
                   ELSE FALSE
                 END,
                 d.rate_limit_key, d.rate_limit_prefix, d.rate_limit_cost, d.concurrency_key, d.concurrency_prefix
          FROM deleted d
          RETURNING *
        ),
        -- Re-suspend any rollup parents already in the main queue that just
        -- got fresh children re-inserted under them.
        parent_resuspend AS (
          UPDATE ${tbl}
          SET suspended = TRUE, updated_at = NOW()
          WHERE parent_state IS NOT NULL
            AND id IN (SELECT DISTINCT parent_id FROM inserted WHERE parent_id IS NOT NULL)
            AND NOT suspended
            AND NOT (attempts > 0 AND not_visible_until IS NOT NULL AND not_visible_until > NOW())
        )
        SELECT ${columns} FROM inserted WHERE id = (SELECT job_id FROM target)
      |]

-- | Check whether a DLQ job exists by ID.
--
-- Parameters: dlq_id
dlqJobExistsSQL :: Text -> Text -> Text
dlqJobExistsSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|SELECT EXISTS (SELECT 1 FROM ${dlqTbl} WHERE id = ?) AS result|]

-- | SQL template for deleting a DLQ job
--
-- Parameters: id (the DLQ primary key)
-- Returns: parent_id of the deleted job (NULL if no parent)
deleteDLQJobSQL :: Text -> Text -> Text
deleteDLQJobSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|DELETE FROM ${dlqTbl} WHERE id = ? RETURNING parent_id|]

-- * Admin Operations

-- | SQL template for getting a job by ID
--
-- Parameters: job_id
--
-- Returns: Single job row if found

-- | SQL template for moving multiple jobs to DLQ in a single operation
--
-- Uses unnest to process multiple (id, attempts, error_msg) tuples.
-- Returns the number of jobs moved.
--
-- Parameters: Array of job IDs, array of attempts, array of error messages
moveToDLQBatchSQL :: Text -> Text -> Text
moveToDLQBatchSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      cols = dlqCarriedCols
   in [text|
        WITH input_jobs AS (
          SELECT unnest(?::bigint[]) AS id,
                 unnest(?::int[]) AS expected_attempts,
                 unnest(?::text[]) AS error_msg
        ),
        deleted_jobs AS (
          DELETE FROM ${tbl} j
          USING input_jobs ij
          WHERE j.id = ij.id AND j.attempts = ij.expected_attempts
          RETURNING j.*, ij.error_msg AS new_error
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (job_id, failed_at, ${cols}, last_error)
          SELECT id, NOW(), ${cols}, new_error
          FROM deleted_jobs
        )
        SELECT count(*) FROM deleted_jobs
      |]

-- | SQL template for deleting multiple DLQ jobs by ID
--
-- Parameters: Array of DLQ job IDs
-- Returns: parent_id of each deleted job (NULL if no parent)
deleteDLQJobsBatchSQL :: Text -> Text -> Text
deleteDLQJobsBatchSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|DELETE FROM ${dlqTbl} WHERE id = ANY(?) RETURNING parent_id|]

-- ---------------------------------------------------------------------------
-- Job Dependency Operations
-- ---------------------------------------------------------------------------

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

-- | Cascade all descendants of a rollup parent to the DLQ.
--
-- Recursively finds all descendants and moves them from the main queue
-- to the DLQ in a single operation. Used when a rollup parent is moved
-- to DLQ to prevent orphaned children from hitting FK violations on
-- the results table.
--
-- Parameters: parent_job_id, error_message
cascadeChildrenToDLQSQL :: Text -> Text -> Text
cascadeChildrenToDLQSQL schema tableName =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      cols = dlqCarriedCols
   in [text|
        WITH RECURSIVE descendants AS (
          SELECT id FROM ${tbl} WHERE parent_id = ?
          UNION ALL
          SELECT j.id FROM ${tbl} j JOIN descendants d ON j.parent_id = d.id
        ),
        deleted AS (
          DELETE FROM ${tbl}
          WHERE id IN (SELECT id FROM descendants)
          RETURNING id, ${cols}
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (job_id, ${cols}, last_error)
          SELECT id, ${cols}, ?
          FROM deleted
        )
        SELECT count(*) FROM deleted
      |]

-- | Find descendant rollup finalizer IDs for snapshot preservation.
--
-- Used before cascade-DLQ to identify intermediate rollup nodes that
-- need their results persisted into @parent_state@ before deletion.
--
-- Parameters: parent_job_id

-- | Batch DLQ child count: returns (parent_id, count) for a set of job IDs
--
-- Parameters: array of job IDs
countDLQChildrenBatchSQL :: Text -> Text -> Text
countDLQChildrenBatchSQL schema tableName =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [text|
        SELECT parent_id, COUNT(*)
        FROM ${dlqTbl}
        WHERE parent_id = ANY(?)
        GROUP BY parent_id
      |]

-- ---------------------------------------------------------------------------
-- Batch Operations
-- ---------------------------------------------------------------------------

-- | SQL template for moving multiple jobs to DLQ in a single operation
--
-- Uses unnest to process multiple (id, attempts, error_msg) tuples.
-- Returns the number of jobs moved.
--
-- Parameters: Array of job IDs, array of attempts, array of error messages

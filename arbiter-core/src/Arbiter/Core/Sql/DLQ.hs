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

import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Text qualified as T

import Arbiter.Core.Codec (jobRowCodec)
import Arbiter.Core.Job.Schema (jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.Job.Types (JobRead, defaultMaxAttempts)
import Arbiter.Core.Sql.Jobs (dlqCarriedCols, jobColumns, requeuedCols)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)

-- | SQL template for moving job to DLQ atomically
--
-- This preserves ALL job fields (complete snapshot) plus DLQ metadata.
-- The operation is atomic: the job is deleted from the main queue and
-- inserted into the DLQ in a single statement. The final error message
-- is passed as a parameter to capture the error that caused the DLQ move.
moveToDLQSQL :: Text -> Text -> Int64 -> Int32 -> Text -> Query Int64
moveToDLQSQL schema tableName jobId att errorMsg =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      cols = dlqCarriedCols
   in [sql|
        WITH deleted_job AS (
          DELETE FROM ${tbl}
          WHERE id = #{jobId :: CInt8} AND attempts = #{att :: CInt4}
          RETURNING *
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (
            job_id, ${cols}, last_error
          )
          SELECT
            id, ${cols}, #{errorMsg :: CText}
          FROM deleted_job
        )
        SELECT count(*) AS @{count :: CInt8} FROM deleted_job
      |]

-- | Select up to @limit@ claimable jobs whose attempts reached their limit,
-- with the scalar fields the tree-aware DLQ move needs. Each row is then moved
-- via 'moveToDLQFields'. The cap drains a large backlog over several passes
-- rather than fetching an unbounded set at once.
selectExhaustedJobsSQL :: Text -> Text -> Int -> Query (Int64, Int32, Maybe Int64, Bool)
selectExhaustedJobsSQL schema tableName limit =
  let tbl = jobQueueTable schema tableName
      dma = T.pack (show defaultMaxAttempts)
      lim = T.pack (show limit)
   in [sql|
        SELECT @{id :: CInt8}, @{attempts :: CInt4}, @{parent_id :: Maybe CInt8}, (parent_state IS NOT NULL) AS @{is_rollup :: CBool}
        FROM ${tbl}
        WHERE NOT suspended
          AND cancel_requested_at IS NULL
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
retryFromDLQSQL :: Text -> Text -> Int64 -> Query (JobRead Value)
retryFromDLQSQL schema tableName dlqId =
  let dlqTbl = jobQueueDLQTable schema tableName
      tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
      carried = requeuedCols Nothing
      carriedFrom = requeuedCols (Just "d")
   in rows
        (jobRowCodec tableName)
        [sql|
        WITH RECURSIVE
        target AS (
          SELECT * FROM ${dlqTbl} WHERE id = #{dlqId :: CInt8}
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
          SELECT d.id AS dlq_id, d.job_id
          FROM ${dlqTbl} d
          WHERE d.job_id = (SELECT job_id FROM root_job_id)
          UNION ALL
          SELECT d.id AS dlq_id, d.job_id
          FROM ${dlqTbl} d
          JOIN tree t ON d.parent_id = t.job_id
        ),
        -- Delete all tree members from DLQ (guarded by can_retry)
        deleted AS (
          DELETE FROM ${dlqTbl}
          WHERE id IN (SELECT dlq_id FROM tree)
            AND (SELECT val FROM can_retry)
          RETURNING job_id, ${carried}
        ),
        -- Re-insert into main queue with computed suspended state:
        -- rollup finalizers are suspended if they have children (in this
        -- retry batch OR already in the main queue).
        inserted AS (
          INSERT INTO ${tbl} (id, attempts, suspended, ${carried})
          SELECT d.job_id, 0,
                 CASE WHEN d.parent_state IS NOT NULL
                   THEN EXISTS (SELECT 1 FROM deleted c WHERE c.parent_id = d.job_id)
                     OR EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = d.job_id)
                   ELSE FALSE
                 END,
                 ${carriedFrom}
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
dlqJobExistsSQL :: Text -> Text -> Int64 -> Query Bool
dlqJobExistsSQL schema tableName dlqId =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|SELECT EXISTS (SELECT 1 FROM ${dlqTbl} WHERE id = #{dlqId :: CInt8}) AS @{result :: CBool}|]

-- | SQL template for deleting a DLQ job. Returns the deleted job's parent_id (NULL if no parent).
deleteDLQJobSQL :: Text -> Text -> Int64 -> Query (Maybe Int64)
deleteDLQJobSQL schema tableName dlqId =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|DELETE FROM ${dlqTbl} WHERE id = #{dlqId :: CInt8} RETURNING @{parent_id :: Maybe CInt8}|]

-- | SQL template for moving multiple jobs to DLQ in a single operation
--
-- Uses unnest to process multiple (id, attempts, error_msg) tuples.
-- Returns the number of jobs moved.
moveToDLQBatchSQL :: Text -> Text -> [Int64] -> [Int32] -> [Text] -> Query Int64
moveToDLQBatchSQL schema tableName ids atts errs =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      cols = dlqCarriedCols
   in [sql|
        WITH input_jobs AS (
          SELECT unnest(#{ids :: [CInt8]}::bigint[]) AS id,
                 unnest(#{atts :: [CInt4]}::int[]) AS expected_attempts,
                 unnest(#{errs :: [CText]}::text[]) AS error_msg
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
        SELECT count(*) AS @{count :: CInt8} FROM deleted_jobs
      |]

-- | SQL template for deleting multiple DLQ jobs by ID
--
-- | Delete multiple DLQ jobs by id, returning each deleted job's parent_id (NULL if no parent).
deleteDLQJobsBatchSQL :: Text -> Text -> [Int64] -> Query (Int64, Maybe Int64)
deleteDLQJobsBatchSQL schema tableName dlqIds =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|DELETE FROM ${dlqTbl} WHERE id = ANY(#{dlqIds :: [CInt8]}) RETURNING @{id :: CInt8}, @{parent_id :: Maybe CInt8}|]

-- | Cascade all descendants of a rollup parent to the DLQ.
--
-- Recursively finds all descendants and moves them from the main queue
-- to the DLQ in a single operation. Used when a rollup parent is moved
-- to DLQ to prevent orphaned children from hitting FK violations on
-- the results table.
cascadeChildrenToDLQSQL :: Text -> Text -> Int64 -> Text -> Query Int64
cascadeChildrenToDLQSQL schema tableName parentId errorMsg =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      cols = dlqCarriedCols
   in [sql|
        WITH RECURSIVE descendants AS (
          SELECT id FROM ${tbl} WHERE parent_id = #{parentId :: CInt8}
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
          SELECT id, ${cols}, #{errorMsg :: CText}
          FROM deleted
        )
        SELECT count(*) AS @{count :: CInt8} FROM deleted
      |]

-- | Batch DLQ child count: returns (parent_id, count) for a set of job IDs.
countDLQChildrenBatchSQL :: Text -> Text -> [Int64] -> Query (Int64, Int64)
countDLQChildrenBatchSQL schema tableName jobIds =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|
        SELECT @{parent_id :: CInt8}, COUNT(*) AS @{count :: CInt8}
        FROM ${dlqTbl}
        WHERE parent_id = ANY(#{jobIds :: [CInt8]})
        GROUP BY parent_id
      |]

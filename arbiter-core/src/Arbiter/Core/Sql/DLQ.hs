{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | DLQ SQL templates.
module Arbiter.Core.Sql.DLQ
  ( DLQMove (..)
  , moveToDLQSQL
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
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Codec (jobRowCodec)
import Arbiter.Core.Job.Schema (jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.Job.Types (JobRead, defaultMaxAttempts)
import Arbiter.Core.Sql.Jobs (dlqCarriedCols, jobColumns, requeuedCols)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, mwhen, rows)
import Arbiter.Core.Sql.Tree (lockedByIdsCte)

-- | Whether a DLQ move re-checks the attempt budget it was selected on.
data DLQMove = MoveNow | MoveIfExhausted
  deriving stock (Eq, Show)

-- | The sweep's predicate. Claimable, uncancelled, and out of attempt budget.
sweepableGuard :: Text
sweepableGuard =
  let dma = T.pack (show defaultMaxAttempts)
   in [text|
        NOT suspended AND cancel_requested_at IS NULL
        AND attempts >= COALESCE(max_attempts, ${dma})
        AND (not_visible_until IS NULL OR not_visible_until <= NOW())
      |]

-- | Move a job to the DLQ in one statement. Copy each job column and the
-- failure message.
moveToDLQSQL :: DLQMove -> Text -> Text -> Int64 -> Int64 -> Text -> Query Int64
moveToDLQSQL move schema tableName jobId cseq errorMsg =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      exhausted = mwhen (move == MoveIfExhausted) [text|AND ${sweepableGuard}|]
   in [sql|
        WITH deleted_job AS (
          DELETE FROM ${tbl}
          WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8} ${exhausted}
          RETURNING *
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (job_id, ${dlqCarriedCols}, last_error)
          SELECT id, ${dlqCarriedCols}, #{errorMsg :: CText}
          FROM deleted_job
        )
        SELECT count(*) AS @{count :: CInt8} FROM deleted_job
      |]

-- | Select up to @limit@ claimable jobs that reached their attempt limit.
-- Include the scalar fields required by the tree-aware DLQ operation. Process a
-- large backlog in multiple bounded passes.
selectExhaustedJobsSQL :: Text -> Text -> Int -> Query (Int64, Int64, Maybe Int64, Bool)
selectExhaustedJobsSQL schema tableName limit =
  let tbl = jobQueueTable schema tableName
      lim = T.pack (show limit)
   in [sql|
        SELECT @{id :: CInt8}, @{claim_seq :: CInt8}, @{parent_id :: Maybe CInt8},
               (parent_state IS NOT NULL) AS @{is_rollup :: CBool}
        FROM ${tbl}
        WHERE ${sweepableGuard}
        ORDER BY id ASC
        LIMIT ${lim}
      |]

-- | Retry a DLQ job and its complete DLQ tree in one statement. Any member
-- identifies the tree. Restore the root, all descendants in the DLQ, and their
-- finalizers. Keep a finalizer suspended when it has restored children. Make it
-- ready when it has no children. Refuse a root whose parent is absent from the
-- main queue. Remove the deduplication key during the retry.
retryFromDLQSQL :: Text -> Text -> Int64 -> Query (JobRead Value)
retryFromDLQSQL schema tableName dlqId =
  let dlqTbl = jobQueueDLQTable schema tableName
      tbl = jobQueueTable schema tableName
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
          SELECT dead.job_id, dead.parent_id, 0 AS depth
          FROM ${dlqTbl} dead
          WHERE dead.job_id = (SELECT parent_id FROM target)
            AND (SELECT parent_id FROM target) IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE id = (SELECT parent_id FROM target))
          UNION ALL
          SELECT dead.job_id, dead.parent_id, ancestor.depth + 1
          FROM ${dlqTbl} dead
          JOIN ancestors ancestor ON dead.job_id = ancestor.parent_id
          WHERE ancestor.parent_id IS NOT NULL
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE id = ancestor.parent_id)
        ),
        -- Root is the topmost DLQ ancestor, or the target itself
        root_job_id AS (
          SELECT COALESCE(
            (SELECT job_id FROM ancestors ORDER BY depth DESC LIMIT 1),
            (SELECT job_id FROM target)
          ) AS job_id
        ),
        -- The root's parent is NULL or exists in the main queue
        can_retry AS (
          SELECT EXISTS (
            SELECT 1
            FROM root_job_id root
            JOIN ${dlqTbl} dead ON dead.job_id = root.job_id
            WHERE dead.parent_id IS NULL
               OR EXISTS (SELECT 1 FROM ${tbl} WHERE id = dead.parent_id)
          ) AS val
        ),
        -- Walk down from root to collect all DLQ tree members
        tree AS (
          SELECT dead.id AS dlq_id, dead.job_id
          FROM ${dlqTbl} dead
          WHERE dead.job_id = (SELECT job_id FROM root_job_id)
          UNION ALL
          SELECT dead.id AS dlq_id, dead.job_id
          FROM ${dlqTbl} dead
          JOIN tree member ON dead.parent_id = member.job_id
        ),
        -- Delete all tree members from DLQ (guarded by can_retry)
        deleted AS (
          DELETE FROM ${dlqTbl}
          WHERE id IN (SELECT dlq_id FROM tree)
            AND (SELECT val FROM can_retry)
          RETURNING job_id, claim_seq, ${requeuedCols}
        ),
        -- Re-insert into the main queue. A rollup finalizer is suspended when
        -- it has children in this retry batch or in the main queue.
        inserted AS (
          INSERT INTO ${tbl} (id, attempts, claim_seq, suspended, ${requeuedCols})
          SELECT dead.job_id, 0, dead.claim_seq + 1,
                 CASE WHEN dead.parent_state IS NOT NULL
                   THEN EXISTS (SELECT 1 FROM deleted child WHERE child.parent_id = dead.job_id)
                     OR EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = dead.job_id)
                   ELSE FALSE
                 END,
                 ${requeuedCols}
          FROM deleted dead
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
        SELECT ${jobColumns} FROM inserted WHERE id = (SELECT job_id FROM target)
      |]

-- | Whether a DLQ job with the given id exists.
dlqJobExistsSQL :: Text -> Text -> Int64 -> Query Bool
dlqJobExistsSQL schema tableName dlqId =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|SELECT EXISTS (SELECT 1 FROM ${dlqTbl} WHERE id = #{dlqId :: CInt8}) AS @{result :: CBool}|]

-- | Delete a DLQ job, returning its parent id.
deleteDLQJobSQL :: Text -> Text -> Int64 -> Query (Maybe Int64)
deleteDLQJobSQL schema tableName dlqId =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|DELETE FROM ${dlqTbl} WHERE id = #{dlqId :: CInt8} RETURNING @{parent_id :: Maybe CInt8}|]

-- | 'moveToDLQSQL' over @unnest@ed @(id, claim_seq, error_msg)@ arrays, locking
-- descending to match ack. Returns the ids moved.
moveToDLQBatchSQL :: Text -> Text -> [Int64] -> [Int64] -> [Text] -> Query Int64
moveToDLQBatchSQL schema tableName ids cseqs errs =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
      locked = lockedByIdsCte tbl ids
   in [sql|
        WITH input_jobs AS (
          SELECT unnest(#{ids :: [CInt8]}::bigint[]) AS id,
                 unnest(#{cseqs :: [CInt8]}::bigint[]) AS expected_claim_seq,
                 unnest(#{errs :: [CText]}::text[]) AS error_msg
        ),
        ${locked},
        deleted_jobs AS (
          DELETE FROM ${tbl} job
          USING input_jobs input_job
          WHERE job.id = input_job.id AND job.claim_seq = input_job.expected_claim_seq
            AND job.id IN (SELECT id FROM locked)
          RETURNING job.*, input_job.error_msg AS new_error
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (job_id, failed_at, ${dlqCarriedCols}, last_error)
          SELECT id, NOW(), ${dlqCarriedCols}, new_error
          FROM deleted_jobs
        )
        SELECT id AS @{result :: CInt8} FROM deleted_jobs
      |]

-- | Delete DLQ jobs by id, returning each one's parent id.
deleteDLQJobsBatchSQL :: Text -> Text -> [Int64] -> Query (Int64, Maybe Int64)
deleteDLQJobsBatchSQL schema tableName dlqIds =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|
        DELETE FROM ${dlqTbl} WHERE id = ANY(#{dlqIds :: [CInt8]})
        RETURNING @{id :: CInt8}, @{parent_id :: Maybe CInt8}
      |]

-- | Move every descendant of a rollup parent to the DLQ alongside it.
cascadeChildrenToDLQSQL :: Text -> Text -> Int64 -> Text -> Query Int64
cascadeChildrenToDLQSQL schema tableName parentId errorMsg =
  let tbl = jobQueueTable schema tableName
      dlqTbl = jobQueueDLQTable schema tableName
   in [sql|
        WITH RECURSIVE descendants AS (
          SELECT id FROM ${tbl} WHERE parent_id = #{parentId :: CInt8}
          UNION ALL
          SELECT job.id FROM ${tbl} job JOIN descendants descendant ON job.parent_id = descendant.id
        ),
        deleted AS (
          DELETE FROM ${tbl}
          WHERE id IN (SELECT id FROM descendants)
          RETURNING id, ${dlqCarriedCols}
        ),
        inserted_dlq AS (
          INSERT INTO ${dlqTbl} (job_id, ${dlqCarriedCols}, last_error)
          SELECT id, ${dlqCarriedCols}, #{errorMsg :: CText}
          FROM deleted
        )
        SELECT count(*) AS @{count :: CInt8} FROM deleted
      |]

-- | DLQ child count per parent, over a set of job ids.
countDLQChildrenBatchSQL :: Text -> Text -> [Int64] -> Query (Int64, Int64)
countDLQChildrenBatchSQL schema tableName jobIds =
  let dlqTbl = jobQueueDLQTable schema tableName
   in [sql|
        SELECT @{parent_id :: CInt8}, COUNT(*) AS @{count :: CInt8}
        FROM ${dlqTbl}
        WHERE parent_id = ANY(#{jobIds :: [CInt8]})
        GROUP BY parent_id
      |]

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Lifecycle SQL templates.
module Arbiter.Core.Sql.Lifecycle
  ( smartAckJobSQL
  , smartAckJobsBatchSQL
  , setVisibilityTimeoutSQL
  , setVisibilityTimeoutBatchSQL
  , updateJobForRetrySQL
  , nackJobSQL
  , nackJobsBatchSQL
  , promoteJobSQL
  ) where

import Data.Int (Int32, Int64)
import Data.Text (Text)

import Arbiter.Core.Job.Schema (jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Sql.Archive (archiveAckCte)
import Arbiter.Core.Sql.QQ (sql, stmt)
import Arbiter.Core.Sql.Query (Query, mwhen)
import Arbiter.Core.Sql.Tree (lockedByIdsCte)

-- | Parent-aware ack. Deletes a childless job, suspends one whose children are still
-- running, and wakes a suspended parent whose last child left the queue. Returns 1,
-- or 0 for a job gone, reclaimed or cancelled. When @archiveEnabled@, the deleted row
-- is teed into the archive per-row on @archive_for@.
smartAckJobSQL :: Bool -> Text -> Text -> Int64 -> Int64 -> Query Int64
smartAckJobSQL archiveEnabled schema tableName =
  let tbl = jobQueueTable schema tableName
      returning = if archiveEnabled then "*" else "id, parent_id" :: Text
      archived = mwhen archiveEnabled (archiveAckCte schema tableName "ack")
   in [stmt|
        WITH ack AS (
          DELETE FROM ${tbl}
          WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8}
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = #{jobId :: CInt8})
          RETURNING ${returning}
        ),
        ${archived}
        suspend AS (
          UPDATE ${tbl}
          SET suspended = TRUE, not_visible_until = NULL, claimed_by = NULL, updated_at = NOW()
          WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8}
            AND NOT EXISTS (SELECT 1 FROM ack)
            AND EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = #{jobId :: CInt8})
          RETURNING id
        ),
        wake_parent AS (
          UPDATE ${tbl}
          SET suspended = FALSE, updated_at = NOW()
          WHERE id = (SELECT parent_id FROM ack WHERE parent_id IS NOT NULL)
            AND suspended = TRUE
            AND NOT EXISTS (
              SELECT 1 FROM ${tbl} child
              WHERE child.parent_id = (SELECT parent_id FROM ack WHERE parent_id IS NOT NULL)
                AND child.id NOT IN (SELECT id FROM ack)
            )
          RETURNING id
        )
        SELECT
          (SELECT count(*) FROM ack) + (SELECT count(*) FROM suspend) AS @{result :: CInt8}
      |]

-- | Set-based smart ack over @unnest@ed @(id, claim_seq)@ arrays. Deletes leaves,
-- suspends finalizers that still have children, and wakes parents whose last
-- child completed. The wake check excludes acked children explicitly. Returns the
-- acked ids. Reclaimed jobs are absent. Locks children-first to match nack and
-- force-cancel. The caller holds the parent locks.
smartAckJobsBatchSQL :: Bool -> Text -> Text -> [Int64] -> [Int64] -> Query Int64
smartAckJobsBatchSQL archiveEnabled schema tableName =
  let tbl = jobQueueTable schema tableName
      returning = if archiveEnabled then "job.*" else "job.id, job.parent_id" :: Text
      archived = mwhen archiveEnabled (archiveAckCte schema tableName "ack")
   in [stmt|
        WITH input AS (
          SELECT unnest(#{ids :: [CInt8]}::bigint[]) AS id, unnest(#{cseqs :: [CInt8]}::bigint[]) AS cseq
        ),
        locked AS (
          SELECT id FROM ${tbl} WHERE id = ANY(#{ids :: [CInt8]})
          ORDER BY id DESC
          FOR UPDATE
        ),
        ack AS (
          DELETE FROM ${tbl} job
          USING input input_row
          WHERE job.id = input_row.id AND job.claim_seq = input_row.cseq
            AND job.id IN (SELECT id FROM locked)
            AND NOT EXISTS (SELECT 1 FROM ${tbl} child WHERE child.parent_id = job.id)
          RETURNING ${returning}
        ),
        ${archived}
        suspend AS (
          UPDATE ${tbl} job
          SET suspended = TRUE, not_visible_until = NULL, claimed_by = NULL, updated_at = NOW()
          FROM input input_row
          WHERE job.id = input_row.id AND job.claim_seq = input_row.cseq
            AND job.id IN (SELECT id FROM locked)
            AND NOT EXISTS (SELECT 1 FROM ack acked WHERE acked.id = job.id)
            AND EXISTS (SELECT 1 FROM ${tbl} child WHERE child.parent_id = job.id)
          RETURNING job.id
        ),
        wake_parent AS (
          UPDATE ${tbl} parent
          SET suspended = FALSE, updated_at = NOW()
          WHERE parent.id IN (SELECT DISTINCT parent_id FROM ack WHERE parent_id IS NOT NULL)
            AND parent.suspended = TRUE
            AND NOT EXISTS (
              SELECT 1 FROM ${tbl} child
              WHERE child.parent_id = parent.id
                AND NOT EXISTS (SELECT 1 FROM ack acked WHERE acked.id = child.id)
            )
          RETURNING parent.id
        )
        SELECT @{id :: CInt8} FROM ack
        UNION
        SELECT id FROM suspend
      |]

-- | Extend a job's visibility timeout. Matches on the claim token. Suspended rows
-- hold no lease.
setVisibilityTimeoutSQL :: Text -> Text -> Double -> Int64 -> Int64 -> Query ()
setVisibilityTimeoutSQL schema tableName secs jobId cseq =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET not_visible_until = CASE WHEN #{secs :: CFloat8}::double precision <= 0
                                     THEN NULL
                                     ELSE NOW() + (#{secs :: CFloat8}::double precision * interval '1 second') END,
            updated_at = NOW()
        WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8} AND NOT suspended
      |]

-- | 'setVisibilityTimeoutSQL' over a batch, for the heartbeat. Extends every job still
-- under this claim, held by the same worker and unsuspended, and reports per row whether
-- the update landed alongside its claim token, cancel flag and suspension. @valuesFrag@
-- carries the input @(id, claim_seq, claimed_by)@ rows. The statement never waits on a
-- lock. It takes the group summaries first, then the rows, both with SKIP LOCKED, and a
-- row whose summary or row is busy reads back unchanged.
setVisibilityTimeoutBatchSQL :: Text -> Text -> Query () -> [Int64] -> Double -> Query ()
setVisibilityTimeoutBatchSQL schema tableName valuesFrag ids secs =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
   in [sql|
        WITH input_jobs AS (
          SELECT input_values.id::bigint AS id,
                 input_values.expected_claim_seq::bigint AS expected_claim_seq,
                 input_values.expected_claimed_by::uuid AS expected_claimed_by
          FROM (VALUES ${valuesFrag}) AS input_values(id, expected_claim_seq, expected_claimed_by)
        ),
        wanted_groups AS MATERIALIZED (
          SELECT DISTINCT group_key FROM ${tbl}
          WHERE id = ANY(#{ids :: [CInt8]}) AND group_key IS NOT NULL
        ),
        held_groups AS (
          SELECT group_key FROM ${groupsTbl}
          WHERE group_key IN (SELECT group_key FROM wanted_groups)
          ORDER BY group_key
          FOR UPDATE SKIP LOCKED
        ),
        busy_groups AS MATERIALIZED (
          SELECT group_key FROM ${groupsTbl}
          WHERE group_key IN (SELECT group_key FROM wanted_groups)
          EXCEPT
          SELECT group_key FROM held_groups
        ),
        locked AS (
          SELECT id FROM ${tbl} job
          WHERE job.id = ANY(#{ids :: [CInt8]})
            AND (job.group_key IS NULL OR NOT EXISTS (SELECT 1 FROM busy_groups busy WHERE busy.group_key = job.group_key))
          ORDER BY id DESC
          FOR UPDATE SKIP LOCKED
        ),
        updated AS (
          UPDATE ${tbl} job
          SET not_visible_until = CASE WHEN #{secs :: CFloat8}::double precision <= 0
                                       THEN NULL
                                       ELSE NOW() + (#{secs :: CFloat8}::double precision * interval '1 second') END,
              updated_at = NOW()
          FROM input_jobs input_job
          WHERE job.id = input_job.id AND job.id IN (SELECT id FROM locked)
            AND job.claim_seq = input_job.expected_claim_seq AND NOT job.suspended
            AND job.claimed_by IS NOT DISTINCT FROM input_job.expected_claimed_by
          RETURNING job.id
        )
        SELECT
          input_job.id,
          (heartbeated.id IS NOT NULL) as was_heartbeated,
          job.claim_seq as current_db_claim_seq,
          (job.cancel_requested_at IS NOT NULL) as cancel_requested,
          (job.suspended IS TRUE) as suspended,
          job.claimed_by as claimed_by
        FROM input_jobs input_job
        LEFT JOIN updated heartbeated ON heartbeated.id = input_job.id
        LEFT JOIN ${tbl} job ON job.id = input_job.id
      |]

-- | Park a failed job for its retry backoff. Matches on the claim token.
updateJobForRetrySQL :: Text -> Text -> Int64 -> Text -> Int64 -> Int64 -> Query ()
updateJobForRetrySQL schema tableName backoff errorMsg jobId cseq =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET not_visible_until = NOW() + (#{backoff :: CInt8} * interval '1 second'),
            last_error = #{errorMsg :: CText},
            updated_at = NOW(),
            claimed_by = NULL
        WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8} AND NOT suspended
      |]

-- | Soft nack. Releases the claim and hands back the attempt it consumed, recording no
-- failure. Leaves @not_visible_until@ as it stands.
nackJobSQL :: Text -> Text -> Int64 -> Int64 -> Int32 -> Query ()
nackJobSQL schema tableName jobId cseq att =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET attempts = LEAST(GREATEST(#{att :: CInt4} - 1, attempts - 1, 0), attempts),
            claimed_by = NULL,
            updated_at = NOW()
        WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8} AND NOT suspended
          AND claimed_by IS NOT NULL
      |]

-- | 'nackJobSQL' over @unnest@ed @(id, claim_seq, attempts)@ arrays, locking
-- children-first to match ack and force-cancel. Returns the ids nacked.
nackJobsBatchSQL :: Text -> Text -> [Int64] -> [Int64] -> [Int32] -> Query Int64
nackJobsBatchSQL schema tableName ids cseqs atts =
  let tbl = jobQueueTable schema tableName
      locked = lockedByIdsCte tbl ids
   in [sql|
        WITH input AS (
          SELECT unnest(#{ids :: [CInt8]}::bigint[]) AS in_id,
                 unnest(#{cseqs :: [CInt8]}::bigint[]) AS cseq,
                 unnest(#{atts :: [CInt4]}::int[]) AS att
        ),
        ${locked}
        UPDATE ${tbl} job
        SET attempts = LEAST(GREATEST(input_row.att - 1, job.attempts - 1, 0), job.attempts),
            claimed_by = NULL,
            updated_at = NOW()
        FROM input input_row
        WHERE job.id = input_row.in_id AND job.id IN (SELECT id FROM locked)
          AND job.claim_seq = input_row.cseq AND NOT job.suspended
          AND job.claimed_by IS NOT NULL
        RETURNING @{id :: CInt8}
      |]

-- | Make a delayed or retrying job immediately visible. Refuses an in-flight job.
promoteJobSQL :: Text -> Text -> Int64 -> Query ()
promoteJobSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET not_visible_until = NULL,
            updated_at = NOW()
        WHERE id = #{jobId :: CInt8}
          AND NOT suspended
          AND not_visible_until IS NOT NULL
          AND not_visible_until > NOW()
          AND claimed_by IS NULL
      |]

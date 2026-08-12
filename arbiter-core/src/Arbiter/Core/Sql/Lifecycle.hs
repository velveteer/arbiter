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
  , promoteJobSQL
  ) where

import Data.Int (Int32, Int64)
import Data.Text (Text)

import Arbiter.Core.Job.Schema (jobQueueTable)
import Arbiter.Core.Sql.Archive (archiveAckCte)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, mwhen)

-- | Smart ack CTE for job dependencies.
--
-- 1. ack: DELETE the job only if it has no children. Returns deleted row.
-- 2. suspend: If ack returned nothing AND children exist, suspend the job
--    (it becomes a finalizer waiting for children to complete).
-- 3. wake_parent: If ack deleted a child whose parent is suspended with no
--    remaining siblings in the queue, resume the parent for its
--    completion round.
--
-- Returns @rows_affected@ (1 on success, 0 if stolen/gone/cancelled).
--
-- When @archiveEnabled@, the deleted row is teed into the archive per-row on @archive_for@.
smartAckJobSQL :: Bool -> Text -> Text -> Int64 -> Int64 -> Query Int64
smartAckJobSQL archiveEnabled schema tableName jobId cseq =
  let tbl = jobQueueTable schema tableName
      returning = if archiveEnabled then "*" else "id, parent_id" :: Text
      archived = mwhen archiveEnabled (archiveAckCte schema tableName "ack")
   in [sql|
        WITH ack AS (
          DELETE FROM ${tbl}
          WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8}
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = #{jobId :: CInt8})
          RETURNING ${returning}
        )${archived},
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
              SELECT 1 FROM ${tbl} c
              WHERE c.parent_id = (SELECT parent_id FROM ack WHERE parent_id IS NOT NULL)
                AND c.id NOT IN (SELECT id FROM ack)
            )
          RETURNING id
        )
        SELECT
          (SELECT count(*) FROM ack) + (SELECT count(*) FROM suspend) AS @{result :: CInt8}
      |]

-- | Set-based smart ack over @unnest@ed @(id, claim_seq)@ arrays: deletes leaves,
-- suspends finalizers that still have children, and wakes parents whose last
-- child completed. The wake check excludes acked children explicitly, since a
-- sibling CTE's deletes are not visible within the same statement. Returns the
-- acked ids. Reclaimed jobs (a different claim) are absent. The caller
-- holds the parent locks.
smartAckJobsBatchSQL :: Bool -> Text -> Text -> [Int64] -> [Int64] -> Query Int64
smartAckJobsBatchSQL archiveEnabled schema tableName ids cseqs =
  let tbl = jobQueueTable schema tableName
      returning = if archiveEnabled then "j.*" else "j.id, j.parent_id" :: Text
      archived = mwhen archiveEnabled (archiveAckCte schema tableName "ack")
   in [sql|
        WITH input AS (
          SELECT unnest(#{ids :: [CInt8]}::bigint[]) AS id, unnest(#{cseqs :: [CInt8]}::bigint[]) AS cseq
        ),
        ack AS (
          DELETE FROM ${tbl} j
          USING input i
          WHERE j.id = i.id AND j.claim_seq = i.cseq
            AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = j.id)
          RETURNING ${returning}
        )${archived},
        suspend AS (
          UPDATE ${tbl} j
          SET suspended = TRUE, not_visible_until = NULL, claimed_by = NULL, updated_at = NOW()
          FROM input i
          WHERE j.id = i.id AND j.claim_seq = i.cseq
            AND NOT EXISTS (SELECT 1 FROM ack a WHERE a.id = j.id)
            AND EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = j.id)
          RETURNING j.id
        ),
        wake_parent AS (
          UPDATE ${tbl} p
          SET suspended = FALSE, updated_at = NOW()
          WHERE p.id IN (SELECT DISTINCT parent_id FROM ack WHERE parent_id IS NOT NULL)
            AND p.suspended = TRUE
            AND NOT EXISTS (
              SELECT 1 FROM ${tbl} c
              WHERE c.parent_id = p.id
                AND NOT EXISTS (SELECT 1 FROM ack a WHERE a.id = c.id)
            )
          RETURNING p.id
        )
        SELECT @{id :: CInt8} FROM ack
        UNION
        SELECT id FROM suspend
      |]

-- | SQL template for setting visibility timeout
--
-- Matches on the claim token, so a job another worker reclaimed after the
-- visibility timeout expired is left alone.
setVisibilityTimeoutSQL :: Text -> Text -> Double -> Int64 -> Int64 -> Query ()
setVisibilityTimeoutSQL schema tableName secs jobId cseq =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET not_visible_until = CASE WHEN #{secs :: CFloat8}::double precision <= 0 THEN NULL ELSE NOW() + (#{secs :: CFloat8}::double precision * interval '1 second') END,
            updated_at = NOW()
        WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8}
      |]

-- | Atomically updates the visibility timeout for a batch of jobs and returns
-- the detailed status of each job in a single query.
--
-- This is used for heartbeating. The query attempts to update all jobs, and
-- then reports on which ones succeeded, which were missing (acked), which are
-- force-cancel-flagged (cancelled), and which are under a different claim
-- (stolen). @valuesFrag@ is the @(id, claim_seq)@ rows for the input VALUES.
setVisibilityTimeoutBatchSQL :: Text -> Text -> Query () -> Double -> Query ()
setVisibilityTimeoutBatchSQL schema tableName valuesFrag secs =
  let tbl = jobQueueTable schema tableName
   in [sql|
        WITH input_jobs AS (
          SELECT v.id::bigint AS id, v.expected_claim_seq::bigint AS expected_claim_seq
          FROM (VALUES ${valuesFrag}) AS v(id, expected_claim_seq)
        ),
        updated AS (
          UPDATE ${tbl} j
          SET not_visible_until = CASE WHEN #{secs :: CFloat8}::double precision <= 0 THEN NULL ELSE NOW() + (#{secs :: CFloat8}::double precision * interval '1 second') END,
              updated_at = NOW()
          FROM input_jobs ij
          WHERE j.id = ij.id AND j.claim_seq = ij.expected_claim_seq
          RETURNING j.id
        )
        SELECT
          ij.id,
          (u.id IS NOT NULL) as was_heartbeated,
          j.claim_seq as current_db_claim_seq,
          (j.cancel_requested_at IS NOT NULL) as cancel_requested,
          j.claimed_by as claimed_by
        FROM input_jobs ij
        LEFT JOIN updated u ON ij.id = u.id
        LEFT JOIN ${tbl} j ON j.id = ij.id
      |]

-- | SQL template for updating job for retry
--
-- Matches on the claim token, so a job another worker claimed after this one's
-- visibility timeout expired is left alone.
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

-- | SQL template for a soft nack: give back the attempt the claim consumed so
-- the reprocess is free, without recording a failure.
--
-- Leaves not_visible_until untouched so the job becomes visible again when the
-- claim's visibility timeout lapses, matching the documented nack semantics.
-- Matches on the claim token so a job reclaimed by another worker is left alone.
-- @att@ is the claim's own attempt count. The row clamps the result to one
-- refund, so repeated nacks on a claim settle at the same value and a
-- caller-supplied @att@ cannot move attempts further than that.
nackJobSQL :: Text -> Text -> Int64 -> Int64 -> Int32 -> Query ()
nackJobSQL schema tableName jobId cseq att =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET attempts = LEAST(GREATEST(#{att :: CInt4} - 1, attempts - 1, 0), attempts),
            updated_at = NOW()
        WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8} AND NOT suspended
          AND claimed_by IS NOT NULL
      |]

-- | Promote a delayed or retrying job to be immediately visible.
--
-- Refuses in-flight jobs (attempts > 0 with no last_error).
-- Returns 0 if job doesn't exist, is already visible, or is in-flight.
promoteJobSQL :: Text -> Text -> Int64 -> Query ()
promoteJobSQL schema tableName jobId =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET not_visible_until = NULL,
            updated_at = NOW()
        WHERE id = #{jobId :: CInt8}
          AND not_visible_until IS NOT NULL
          AND not_visible_until > NOW()
          AND (attempts = 0 OR last_error IS NOT NULL)
      |]

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

import Arbiter.Core.Job.Schema (jobQueueTable)
import Arbiter.Core.Sql.Archive (archiveAckCte)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, mwhen)
import Arbiter.Core.Sql.Tree (lockedByIdsCte)

-- | Parent-aware ack: delete a childless job, suspend one whose children are still
-- running, and wake a suspended parent whose last child just left the queue. Returns 1,
-- or 0 for a job gone, reclaimed or cancelled. When @archiveEnabled@, the deleted row
-- is teed into the archive per-row on @archive_for@.
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
-- acked ids. Reclaimed jobs (a different claim) are absent. Locks children-first
-- to match nack and force-cancel. The caller holds the parent locks.
smartAckJobsBatchSQL :: Bool -> Text -> Text -> [Int64] -> [Int64] -> Query Int64
smartAckJobsBatchSQL archiveEnabled schema tableName ids cseqs =
  let tbl = jobQueueTable schema tableName
      returning = if archiveEnabled then "j.*" else "j.id, j.parent_id" :: Text
      archived = mwhen archiveEnabled (archiveAckCte schema tableName "ack")
      locked = lockedByIdsCte tbl ids
   in [sql|
        WITH input AS (
          SELECT unnest(#{ids :: [CInt8]}::bigint[]) AS id, unnest(#{cseqs :: [CInt8]}::bigint[]) AS cseq
        ),
        ${locked},
        ack AS (
          DELETE FROM ${tbl} j
          USING input i
          WHERE j.id = i.id AND j.claim_seq = i.cseq
            AND j.id IN (SELECT id FROM locked)
            AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = j.id)
          RETURNING ${returning}
        )${archived},
        suspend AS (
          UPDATE ${tbl} j
          SET suspended = TRUE, not_visible_until = NULL, claimed_by = NULL, updated_at = NOW()
          FROM input i
          WHERE j.id = i.id AND j.claim_seq = i.cseq
            AND j.id IN (SELECT id FROM locked)
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

-- | Extend a job's visibility timeout. Matches on the claim token, so a job another
-- worker reclaimed is left alone. Suspended rows hold no lease.
setVisibilityTimeoutSQL :: Text -> Text -> Double -> Int64 -> Int64 -> Query ()
setVisibilityTimeoutSQL schema tableName secs jobId cseq =
  let tbl = jobQueueTable schema tableName
   in [sql|
        UPDATE ${tbl}
        SET not_visible_until = CASE WHEN #{secs :: CFloat8}::double precision <= 0 THEN NULL ELSE NOW() + (#{secs :: CFloat8}::double precision * interval '1 second') END,
            updated_at = NOW()
        WHERE id = #{jobId :: CInt8} AND claim_seq = #{cseq :: CInt8} AND NOT suspended
      |]

-- | 'setVisibilityTimeoutSQL' over a batch, for the heartbeat. Extends every job still
-- under this claim, held by the same worker and unsuspended, and reports per row whether
-- the update landed alongside its claim token, cancel flag and suspension. @valuesFrag@
-- carries the input @(id, claim_seq, claimed_by)@ rows.
setVisibilityTimeoutBatchSQL :: Text -> Text -> Query () -> [Int64] -> Double -> Query ()
setVisibilityTimeoutBatchSQL schema tableName valuesFrag ids secs =
  let tbl = jobQueueTable schema tableName
      locked = lockedByIdsCte tbl ids
   in [sql|
        WITH input_jobs AS (
          SELECT v.id::bigint AS id, v.expected_claim_seq::bigint AS expected_claim_seq, v.expected_claimed_by::uuid AS expected_claimed_by
          FROM (VALUES ${valuesFrag}) AS v(id, expected_claim_seq, expected_claimed_by)
        ),
        ${locked},
        updated AS (
          UPDATE ${tbl} j
          SET not_visible_until = CASE WHEN #{secs :: CFloat8}::double precision <= 0 THEN NULL ELSE NOW() + (#{secs :: CFloat8}::double precision * interval '1 second') END,
              updated_at = NOW()
          FROM input_jobs ij
          WHERE j.id = ij.id AND j.id IN (SELECT id FROM locked)
            AND j.claim_seq = ij.expected_claim_seq AND NOT j.suspended
            AND j.claimed_by IS NOT DISTINCT FROM ij.expected_claimed_by
          RETURNING j.id
        )
        SELECT
          ij.id,
          (u.id IS NOT NULL) as was_heartbeated,
          j.claim_seq as current_db_claim_seq,
          (j.cancel_requested_at IS NOT NULL) as cancel_requested,
          (j.suspended IS TRUE) as suspended,
          j.claimed_by as claimed_by
        FROM input_jobs ij
        LEFT JOIN updated u ON u.id = ij.id
        LEFT JOIN ${tbl} j ON j.id = ij.id
      |]

-- | Park a failed job for its retry backoff. Matches on the claim token, so a job
-- another worker reclaimed is left alone.
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

-- | Soft nack: hand back the attempt the claim consumed, recording no failure. The
-- visibility timeout is left as it stands, so the job becomes claimable again when the
-- claim's own lease lapses. Matches on the claim token, so a job another worker
-- reclaimed is left alone. @att@ is the claim's attempt count, clamped to the row so
-- repeated nacks on one claim settle at a single refund.
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

-- | 'nackJobSQL' over @unnest@ed @(id, claim_seq, attempts)@ arrays, locking
-- children-first to match ack and force-cancel. Returns the ids nacked.
nackJobsBatchSQL :: Text -> Text -> [Int64] -> [Int64] -> [Int32] -> Query Int64
nackJobsBatchSQL schema tableName ids cseqs atts =
  let tbl = jobQueueTable schema tableName
      locked = lockedByIdsCte tbl ids
   in [sql|
        WITH input AS (
          SELECT unnest(#{ids :: [CInt8]}::bigint[]) AS in_id, unnest(#{cseqs :: [CInt8]}::bigint[]) AS cseq, unnest(#{atts :: [CInt4]}::int[]) AS att
        ),
        ${locked}
        UPDATE ${tbl} j
        SET attempts = LEAST(GREATEST(i.att - 1, j.attempts - 1, 0), j.attempts),
            updated_at = NOW()
        FROM input i
        WHERE j.id = i.in_id AND j.id IN (SELECT id FROM locked)
          AND j.claim_seq = i.cseq AND NOT j.suspended
          AND j.claimed_by IS NOT NULL
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
          AND not_visible_until IS NOT NULL
          AND not_visible_until > NOW()
          AND (attempts = 0 OR last_error IS NOT NULL)
      |]

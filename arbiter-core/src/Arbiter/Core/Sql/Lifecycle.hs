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

import Data.Text (Text)
import NeatInterpolation (text)

import Arbiter.Core.Job.Schema (jobQueueTable)

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
-- Parameters: job_id, attempts, job_id, job_id, attempts, job_id
smartAckJobSQL :: Text -> Text -> Text
smartAckJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH ack AS (
          DELETE FROM ${tbl}
          WHERE id = ? AND attempts = ?
            AND NOT EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = ?)
          RETURNING id, parent_id
        ),
        suspend AS (
          UPDATE ${tbl}
          SET suspended = TRUE, not_visible_until = NULL, claimed_by = NULL, updated_at = NOW()
          WHERE id = ? AND attempts = ?
            AND NOT EXISTS (SELECT 1 FROM ack)
            AND EXISTS (SELECT 1 FROM ${tbl} WHERE parent_id = ?)
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
          (SELECT count(*) FROM ack) + (SELECT count(*) FROM suspend) AS result
      |]

-- | Set-based smart ack over @unnest@ed @(id, attempts)@ arrays: deletes leaves,
-- suspends finalizers that still have children, and wakes parents whose last
-- child completed. The wake check excludes acked children explicitly, since a
-- sibling CTE's deletes are not visible within the same statement. Returns the
-- acked ids. Reclaimed jobs (attempts no longer match) are absent. The caller
-- holds the parent locks.
smartAckJobsBatchSQL :: Text -> Text -> Text
smartAckJobsBatchSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH input AS (
          SELECT unnest(?::bigint[]) AS id, unnest(?::int[]) AS att
        ),
        ack AS (
          DELETE FROM ${tbl} j
          USING input i
          WHERE j.id = i.id AND j.attempts = i.att
            AND NOT EXISTS (SELECT 1 FROM ${tbl} c WHERE c.parent_id = j.id)
          RETURNING j.id, j.parent_id
        ),
        suspend AS (
          UPDATE ${tbl} j
          SET suspended = TRUE, not_visible_until = NULL, claimed_by = NULL, updated_at = NOW()
          FROM input i
          WHERE j.id = i.id AND j.attempts = i.att
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
        SELECT id FROM ack
        UNION
        SELECT id FROM suspend
      |]

-- | SQL template for setting visibility timeout
--
-- Parameters: timeout, job_id, attempts
--
-- Uses optimistic locking (attempts check) to prevent race conditions when
-- another worker has reclaimed the job after visibility timeout expired.
setVisibilityTimeoutSQL :: Text -> Text -> Text
setVisibilityTimeoutSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET not_visible_until = CASE WHEN ?::double precision <= 0 THEN NULL ELSE NOW() + (?::double precision * interval '1 second') END,
            updated_at = NOW()
        WHERE id = ? AND attempts = ?
      |]

-- | Atomically updates the visibility timeout for a batch of jobs and returns
-- the detailed status of each job in a single query.
--
-- This is used for heartbeating. The query attempts to update all jobs, and
-- then reports on which ones succeeded, which were missing (acked), which are
-- force-cancel-flagged (cancelled), and which had a different attempts count
-- (stolen).
setVisibilityTimeoutBatchSQL :: Text -> Text -> Text -> Text
setVisibilityTimeoutBatchSQL schema tableName valuesPlaceholder =
  let tbl = jobQueueTable schema tableName
   in [text|
        WITH input_jobs AS (
          SELECT v.id::bigint AS id, v.expected_attempts::int AS expected_attempts
          FROM (VALUES ${valuesPlaceholder}) AS v(id, expected_attempts)
        ),
        updated AS (
          UPDATE ${tbl} j
          SET not_visible_until = CASE WHEN ?::double precision <= 0 THEN NULL ELSE NOW() + (?::double precision * interval '1 second') END,
              updated_at = NOW()
          FROM input_jobs ij
          WHERE j.id = ij.id AND j.attempts = ij.expected_attempts
          RETURNING j.id
        )
        SELECT
          ij.id,
          (u.id IS NOT NULL) as was_heartbeated,
          j.attempts as current_db_attempts,
          (j.cancel_requested_at IS NOT NULL) as cancel_requested
        FROM input_jobs ij
        LEFT JOIN updated u ON ij.id = u.id
        LEFT JOIN ${tbl} j ON j.id = ij.id
      |]

-- | SQL template for updating job for retry
--
-- Parameters: backoff, error, job_id, attempts
--
-- Uses optimistic locking (attempts check) to prevent race conditions when
-- a job's visibility timeout expires and another worker claims it before
-- the retry update completes.
updateJobForRetrySQL :: Text -> Text -> Text
updateJobForRetrySQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET not_visible_until = NOW() + (? * interval '1 second'),
            last_error = ?,
            updated_at = NOW(),
            claimed_by = NULL
        WHERE id = ? AND attempts = ? AND NOT suspended
      |]

-- | SQL template for a soft nack: give back the attempt the claim consumed so
-- the reprocess is free, without recording a failure.
--
-- Parameters: job_id, attempts
--
-- Leaves not_visible_until untouched so the job becomes visible again when the
-- claim's visibility timeout lapses, matching the documented nack semantics.
-- Uses optimistic locking (attempts check) so a job reclaimed by another worker
-- is left alone. The GREATEST floor keeps attempts non-negative.
nackJobSQL :: Text -> Text -> Text
nackJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET attempts = GREATEST(attempts - 1, 0),
            updated_at = NOW()
        WHERE id = ? AND attempts = ? AND NOT suspended
      |]

-- | Promote a delayed or retrying job to be immediately visible.
--
-- Refuses in-flight jobs (attempts > 0 with no last_error).
-- Returns 0 if job doesn't exist, is already visible, or is in-flight.
promoteJobSQL :: Text -> Text -> Text
promoteJobSQL schema tableName =
  let tbl = jobQueueTable schema tableName
   in [text|
        UPDATE ${tbl}
        SET not_visible_until = NULL,
            updated_at = NOW()
        WHERE id = ?
          AND not_visible_until IS NOT NULL
          AND not_visible_until > NOW()
          AND (attempts = 0 OR last_error IS NOT NULL)
      |]

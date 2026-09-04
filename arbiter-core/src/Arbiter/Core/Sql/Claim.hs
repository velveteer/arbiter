{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Claim-time SQL: candidate selection, admission gates, and the batched claim.
module Arbiter.Core.Sql.Claim
  ( ClaimAdmission (..)
  , claimJobsBatchedSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import NeatInterpolation (text)

import Arbiter.Core.Admission (effectivePolicyCol)
import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyPoliciesTable, arbiterConcurrencyTable)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Job.Types (defaultMaxAttempts)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTable, arbiterRateLimitsTable, bucketSeedInsert)
import Arbiter.Core.Sql.Jobs (jobColumns)
import Arbiter.Core.Sql.Query (mwhen)
import Arbiter.Core.Sql.RateLimit (defaultThrottleWaitSeconds, refilledExpr)

-- | Which admission filters the claim SQL renders. A payload type that declares
-- no policy of a kind gets no filter for it.
data ClaimAdmission = ClaimAdmission
  { admitRateLimited :: Bool
  , admitConcurrent :: Bool
  }
  deriving stock (Eq, Show)

-- | Group-FIFO cut over a @<prefix>_self@ CTE of (id, group_key, grp_rank,
-- self_ok), emitting @<prefix>_group_cut@ and @<prefix>_admitted@.
groupFifoCutCtes :: Text -> Text
groupFifoCutCtes prefix =
  [text|
    ${prefix}_group_cut AS MATERIALIZED (
      SELECT group_key, MIN(grp_rank) AS cut_rank
      FROM ${prefix}_self
      WHERE group_key IS NOT NULL AND NOT self_ok
      GROUP BY group_key
    ),
    ${prefix}_admitted AS (
      SELECT verdict.id FROM ${prefix}_self verdict
      LEFT JOIN ${prefix}_group_cut cut ON cut.group_key = verdict.group_key
      WHERE verdict.self_ok
        AND (verdict.grp_rank IS NULL OR cut.cut_rank IS NULL OR verdict.grp_rank < cut.cut_rank)
    ),
  |]

-- | Per-group rank of candidate rows (alias @candidate@), the shape 'groupFifoCutCtes' cuts on.
grpRankExpr :: Text
grpRankExpr =
  [text|
    CASE WHEN candidate.group_key IS NOT NULL
         THEN ROW_NUMBER() OVER (
                PARTITION BY candidate.group_key ORDER BY candidate.priority ASC, candidate.id ASC
              )
         END AS grp_rank
  |]

-- | A job's cost, floored at 0 and capped at the bucket max. Shared by
-- admission, debit, and the deny-wait.
clampedCostExpr :: Text
clampedCostExpr = "LEAST(GREATEST(candidate.rate_limit_cost, 0), bucket.max_tokens)"

-- | Concurrency gate. Locks each referenced count row, reserves a slot per fresh
-- candidate up to the pool headroom, then applies the group-FIFO cut. Emits @conc_admitted@.
concGateCtes :: Text -> Text -> Text
concGateCtes concTbl concPolicies =
  let effLimit = effectivePolicyCol "policy" "limit"
      cut = groupFifoCutCtes "conc"
   in [text|
        -- Whoever locks the count row gates that key this cycle (others skip and wait).
        -- eff_limit is COALESCE(pool override, default). An undeclared prefix leaves it
        -- NULL and runs uncapped.
        conc_locked AS (
          SELECT counts.concurrency_key, counts.in_flight,
                 ${effLimit} AS eff_limit
          FROM ${concTbl} counts
          LEFT JOIN ${concPolicies} policy ON policy.prefix_id = counts.concurrency_prefix
          WHERE counts.concurrency_key IN (
            SELECT concurrency_key FROM locked WHERE concurrency_key IS NOT NULL
          )
          FOR UPDATE OF counts SKIP LOCKED
        ),
        -- Ungated jobs pass straight through. Keyed jobs join their held pool row (a key
        -- locked by another claimer or absent drops out). fresh_rn reserves a slot per
        -- fresh candidate in priority order, admitting up to the pool's headroom.
        conc_self_ok AS MATERIALIZED (
          SELECT id FROM locked WHERE concurrency_key IS NULL
          UNION ALL
          SELECT ranked.id FROM (
            SELECT candidate.id, candidate.claimed_by, pool.in_flight, pool.eff_limit,
              SUM((candidate.claimed_by IS NULL)::int) OVER (
                PARTITION BY candidate.concurrency_key ORDER BY candidate.priority ASC, candidate.id ASC
                ROWS UNBOUNDED PRECEDING
              ) AS fresh_rn
            FROM locked candidate
            JOIN conc_locked pool ON pool.concurrency_key = candidate.concurrency_key
          ) ranked
          WHERE ranked.claimed_by IS NOT NULL
             OR ranked.eff_limit IS NULL
             OR ranked.in_flight + ranked.fresh_rn <= ranked.eff_limit
        ),
        -- Normalize to the shared (id, group_key, grp_rank, self_ok) shape for the cut.
        conc_self AS (
          SELECT candidate.id, candidate.group_key,
            ${grpRankExpr},
            (passed.id IS NOT NULL) AS self_ok
          FROM locked candidate
          LEFT JOIN conc_self_ok passed ON passed.id = candidate.id
        ),
        ${cut}
      |]

-- | Rate-limit admission: lock and refill each referenced bucket, seed a missing one,
-- admit per key in priority order while cumulative cost fits, then the group-FIFO cut.
-- Emits @rl_admitted@.
rlGateCtes :: Text -> Text -> Text
rlGateCtes buckets rlPolicies =
  let maxCol = effectivePolicyCol "policy" "max_tokens"
      refillCol = effectivePolicyCol "policy" "refill_amount"
      intervalCol = effectivePolicyCol "policy" "interval"
      availCol = refilledExpr maxCol "stored_bucket.tokens" "stored_bucket.last_refill" refillCol intervalCol
      seed = bucketSeedInsert buckets rlPolicies "locked n" "n.rate_limit_key IS NOT NULL"
      cut = groupFifoCutCtes "rl"
   in [text|
        rl_locked AS (
          SELECT stored_bucket.rate_limit_key,
                 ${maxCol} AS max_tokens, ${refillCol} AS refill_amount, ${intervalCol} AS refill_interval,
                 ${availCol} AS available
          FROM ${buckets} stored_bucket
          JOIN ${rlPolicies} policy ON policy.prefix_id = stored_bucket.policy_prefix
          WHERE stored_bucket.rate_limit_key IN (
            SELECT rate_limit_key FROM locked WHERE rate_limit_key IS NOT NULL
          )
          FOR UPDATE OF stored_bucket SKIP LOCKED
        ),
        rl_seed AS (
          ${seed}
        ),
        -- Per-key admission in priority order while cumulative cost fits
        -- (keyed jobs only).
        rl_keyed AS MATERIALIZED (
          SELECT candidate.id,
            (bucket.max_tokens > 0 AND
             SUM(${clampedCostExpr}) OVER (
               PARTITION BY candidate.rate_limit_key ORDER BY candidate.priority ASC, candidate.id ASC
               ROWS UNBOUNDED PRECEDING
             ) <= bucket.available) AS key_ok
          FROM locked candidate
          JOIN rl_locked bucket ON bucket.rate_limit_key = candidate.rate_limit_key
        ),
        -- Own verdict plus per-group rank. Unkeyed/unpolicied passes. A keyed
        -- job with no locked bucket fails.
        rl_self AS (
          SELECT candidate.id, candidate.group_key,
            COALESCE(keyed.key_ok,
              candidate.rate_limit_key IS NULL
              OR NOT EXISTS (
                SELECT 1 FROM ${rlPolicies} policy WHERE policy.prefix_id = candidate.rate_limit_prefix
              )
            ) AS self_ok,
            ${grpRankExpr}
          FROM locked candidate
          LEFT JOIN rl_keyed keyed ON keyed.id = candidate.id
        ),
        ${cut}
      |]

-- | Rate-limit spend. Debits each bucket by the cost admitted by every gate, banks
-- the accrued refill, then computes a jittered defer time for denied keyed jobs.
rlSpendCtes :: Text -> Text
rlSpendCtes buckets =
  let fallback = T.pack (show defaultThrottleWaitSeconds)
      -- Time to accrue the token deficit when the policy refills, else one
      -- interval, else the fallback.
      rlWaitExpr =
        [text|
          CASE WHEN bucket.refill_amount > 0 AND bucket.refill_interval > 0 AND bucket.max_tokens > 0
               THEN (bucket.refill_interval / bucket.refill_amount)
                    * GREATEST(1.0, ${clampedCostExpr} - (bucket.available - COALESCE(spend.admitted_cost, 0)))
               ELSE COALESCE(NULLIF(bucket.refill_interval, 0), ${fallback})
          END
        |]
   in [text|
        rl_spend_agg AS (
          SELECT candidate.rate_limit_key,
                 SUM(${clampedCostExpr}) AS admitted_cost
          FROM locked candidate
          JOIN admitted admitted_row ON admitted_row.id = candidate.id
          JOIN rl_locked bucket ON bucket.rate_limit_key = candidate.rate_limit_key
          WHERE candidate.rate_limit_key IS NOT NULL
          GROUP BY candidate.rate_limit_key
        ),
        rl_spent AS (
          UPDATE ${buckets} stored_bucket
          SET tokens = bucket.available - spend.admitted_cost, last_refill = NOW()
          FROM rl_locked bucket
          JOIN rl_spend_agg spend
            ON spend.rate_limit_key = bucket.rate_limit_key AND spend.admitted_cost > 0
          WHERE stored_bucket.rate_limit_key = bucket.rate_limit_key
          RETURNING stored_bucket.rate_limit_key
        ),
        rl_deferred AS (
          SELECT id,
            NOW() + ((wait + random() * LEAST(wait * 0.5, 2.0))
                     * interval '1 second') AS defer_until
          FROM (
            SELECT candidate.id, GREATEST(${rlWaitExpr}, 0) AS wait
            FROM locked candidate
            JOIN rl_locked bucket ON bucket.rate_limit_key = candidate.rate_limit_key
            LEFT JOIN rl_spend_agg spend ON spend.rate_limit_key = candidate.rate_limit_key
            WHERE NOT EXISTS (SELECT 1 FROM rl_admitted admitted_row WHERE admitted_row.id = candidate.id)
          ) denied
        ),
      |]

-- | The batch a gated group would take, reduced to the row the group cut ranks first,
-- and the headroom check on that row. Retried rows rank ahead of fresh ones. Each run
-- comes from an index.
gatedHeadLateral :: Text -> Text -> Text -> Text -> Text
gatedHeadLateral tbl dma batchLimit headGate =
  [text|
    CROSS JOIN LATERAL (
      SELECT head_batch.concurrency_key, head_batch.claimed_by
      FROM (
        SELECT merged.concurrency_key, merged.claimed_by, merged.priority, merged.id
        FROM (
          (
            SELECT job.concurrency_key, job.claimed_by, job.attempts, job.priority, job.id
            FROM ${tbl} job
            WHERE job.group_key = eligible.group_key
              AND NOT job.suspended
              AND job.cancel_requested_at IS NULL
              AND (job.not_visible_until IS NULL OR job.not_visible_until <= NOW())
              AND job.attempts < COALESCE(job.max_attempts, ${dma})
              AND job.attempts > 0
            ORDER BY job.attempts DESC, job.priority ASC, job.id ASC
            LIMIT ${batchLimit}
          )
          UNION ALL
          (
            SELECT job.concurrency_key, job.claimed_by, job.attempts, job.priority, job.id
            FROM ${tbl} job
            WHERE job.group_key = eligible.group_key
              AND NOT job.suspended
              AND job.cancel_requested_at IS NULL
              AND (job.not_visible_until IS NULL OR job.not_visible_until <= NOW())
              AND job.attempts < COALESCE(job.max_attempts, ${dma})
              AND job.attempts = 0
            ORDER BY job.priority ASC, job.id ASC
            LIMIT ${batchLimit}
          )
        ) merged
        ORDER BY merged.attempts DESC, merged.priority ASC, merged.id ASC
        LIMIT ${batchLimit}
      ) head_batch
      ORDER BY head_batch.priority ASC, head_batch.id ASC
      LIMIT 1
    ) gated_head
    WHERE ${headGate}
  |]

-- | Candidate stage one. Groups with ready or due work, locked and reduced
-- to each head row. A gated group is judged on the row its next batch would take.
groupCandidateCtes :: Text -> Text -> Text -> Text -> Text -> Text
groupCandidateCtes groupsTbl tbl overfetch dma gateLateral =
  [text|
    group_candidates AS (
      (
        SELECT group_key FROM ${groupsTbl}
        WHERE ready_count > 0 AND in_flight_until IS NULL
        ORDER BY min_priority ASC, min_id ASC
        LIMIT ${overfetch}
      )
      UNION
      (
        SELECT group_key FROM ${groupsTbl}
        WHERE next_due <= NOW()
        ORDER BY next_due ASC
        LIMIT ${overfetch}
      )
    ),
    eligible_groups AS (
      SELECT summary.group_key FROM ${groupsTbl} summary
      WHERE summary.group_key IN (SELECT group_key FROM group_candidates)
        AND summary.job_count > 0
        AND (summary.in_flight_until IS NULL OR summary.in_flight_until <= NOW())
      ORDER BY summary.min_priority ASC, summary.min_id ASC
      LIMIT ${overfetch}
      FOR UPDATE SKIP LOCKED
    ),
    eligible_heads AS (
      SELECT eligible.group_key, head.min_priority, head.min_id
      FROM eligible_groups eligible
      CROSS JOIN LATERAL (
        SELECT job.priority AS min_priority, job.id AS min_id
        FROM ${tbl} job
        WHERE job.group_key = eligible.group_key
          AND NOT job.suspended
          AND job.cancel_requested_at IS NULL
          AND (job.not_visible_until IS NULL OR job.not_visible_until <= NOW())
          AND job.attempts < COALESCE(job.max_attempts, ${dma})
        ORDER BY job.priority ASC, job.id ASC
        LIMIT 1
      ) head
      ${gateLateral}
    )
  |]

-- | Candidate stage two. The ungrouped ready and due pools, numbered into batches.
ungroupedPoolCtes :: Text -> Text -> Text -> Text -> Text -> Text
ungroupedPoolCtes tbl ungroupedLimit batchLimit dma ccGate =
  [text|
    ungrouped_pool AS (
      (
        SELECT job.id, job.priority
        FROM ${tbl} job
        WHERE job.group_key IS NULL
          AND NOT job.suspended
          AND job.cancel_requested_at IS NULL
          AND job.not_visible_until IS NULL
          AND job.attempts < COALESCE(job.max_attempts, ${dma})
          ${ccGate}
        ORDER BY job.priority ASC, job.id ASC
        LIMIT ${ungroupedLimit}
      )
      UNION ALL
      (
        SELECT job.id, job.priority
        FROM ${tbl} job
        WHERE job.group_key IS NULL
          AND NOT job.suspended
          AND job.cancel_requested_at IS NULL
          AND job.not_visible_until <= NOW()
          AND job.attempts < COALESCE(job.max_attempts, ${dma})
          ${ccGate}
        ORDER BY job.not_visible_until ASC
        LIMIT ${ungroupedLimit}
      )
    ),
    ungrouped_numbered AS (
      SELECT id, priority,
        ((ROW_NUMBER() OVER (ORDER BY priority ASC, id ASC) - 1)
          / ${batchLimit}) + 1 AS batch_num
      FROM ungrouped_pool
      ORDER BY priority ASC, id ASC
      LIMIT ${ungroupedLimit}
    ),
    ungrouped_batch_info AS (
      SELECT batch_num, MIN(priority) AS min_priority,
        (MIN(ARRAY[priority::bigint, id]))[2] AS min_id
      FROM ungrouped_numbered
      GROUP BY batch_num
    )
  |]

-- | Candidate stage three. Interleaves group heads and ungrouped batches by
-- priority, capped at the batch budget.
allocatedSlotCtes :: Text -> Text
allocatedSlotCtes batchBudget =
  [text|
    allocated_slots AS (
      SELECT slot.group_key, slot.ungrouped_batch
      FROM (
        SELECT group_key, NULL::bigint AS ungrouped_batch, min_priority, min_id
        FROM eligible_heads
        UNION ALL
        SELECT NULL::text, batch_num, min_priority, min_id
        FROM ungrouped_batch_info
        ORDER BY min_priority ASC, min_id ASC
      ) slot
      LIMIT ${batchBudget}
    ),
    final_locked_groups AS (
      SELECT group_key FROM allocated_slots WHERE group_key IS NOT NULL
    )
  |]

-- | Candidate stage four. Resolves the allocated slots to rows and locks the claimable set.
lockedCandidateCtes :: Text -> Text -> Text -> Text -> Text
lockedCandidateCtes tbl batchLimit dma ungroupedLimit =
  [text|
    grouped_candidates AS (
      SELECT batch.id, target_group.group_key AS expected_group
      FROM final_locked_groups target_group
      CROSS JOIN LATERAL (
        SELECT id
        FROM ${tbl}
        WHERE group_key = target_group.group_key
          AND NOT suspended
          AND cancel_requested_at IS NULL
          AND (not_visible_until IS NULL OR not_visible_until <= NOW())
          AND attempts < COALESCE(max_attempts, ${dma})
        ORDER BY attempts DESC, priority ASC, id ASC
        LIMIT ${batchLimit}
      ) batch
    ),
    ungrouped_candidates AS (
      SELECT id, NULL::text AS expected_group
      FROM ungrouped_numbered
      WHERE batch_num IN (
        SELECT ungrouped_batch
        FROM allocated_slots
        WHERE ungrouped_batch IS NOT NULL
      )
    ),
    locked AS (
      SELECT job.id, job.priority, job.group_key,
             job.concurrency_key, job.claimed_by,
             job.rate_limit_key, job.rate_limit_prefix, job.rate_limit_cost
      FROM (
        SELECT id, expected_group FROM grouped_candidates
        UNION ALL
        SELECT id, expected_group FROM ungrouped_candidates
      ) selected
      INNER JOIN ${tbl} job ON job.id = selected.id
      WHERE NOT job.suspended
        AND job.cancel_requested_at IS NULL
        AND (job.not_visible_until IS NULL OR job.not_visible_until <= NOW())
        AND job.group_key IS NOT DISTINCT FROM selected.expected_group
        AND job.attempts < COALESCE(job.max_attempts, ${dma})
      ORDER BY job.priority ASC, job.id ASC
      FOR UPDATE OF job SKIP LOCKED
      LIMIT ${ungroupedLimit}
    )
  |]

-- | Concurrency headroom over a candidate row alias. Keeps a full key off the
-- bounded candidate window, in the ungrouped pool and at each group's head.
-- OFFSET 0 keeps the probe correlated. Without it the planner hashes the whole
-- count table once per claim.
concHeadroomPred :: Text -> Text -> Text -> Text
concHeadroomPred concTbl concPolicies alias =
  let effLimit = effectivePolicyCol "policy" "limit"
   in [text|
        (${alias}.claimed_by IS NOT NULL OR ${alias}.concurrency_key IS NULL OR EXISTS (
          SELECT 1 FROM ${concTbl} counts
          LEFT JOIN ${concPolicies} policy ON policy.prefix_id = counts.concurrency_prefix
          WHERE counts.concurrency_key = ${alias}.concurrency_key
            AND (${effLimit} IS NULL
                 OR counts.in_flight < ${effLimit})
          OFFSET 0
        ))
      |]

-- | The claimable set. The intersection of every active gate's admitted ids. The
-- full locked set when no gate is rendered.
admittedCte :: ClaimAdmission -> Text
admittedCte admission =
  [text|
    admitted AS (
      ${body}
    )
  |]
  where
    body = case (admitConcurrent admission, admitRateLimited admission) of
      (True, True) ->
        [text|
          SELECT conc_row.id FROM conc_admitted conc_row
          JOIN rl_admitted rl_row ON rl_row.id = conc_row.id
        |]
      (True, False) -> "SELECT id FROM conc_admitted"
      (False, True) -> "SELECT id FROM rl_admitted"
      (False, False) -> "SELECT id FROM locked"

-- | With rate limiting the claim splits into an admit/defer decision.
decisionCte :: ClaimAdmission -> Text
decisionCte admission =
  mwhen
    (admitRateLimited admission)
    [text|
      decision AS (
        SELECT id, TRUE AS _admit, NULL::timestamptz AS _defer FROM admitted
        ${deferUnion}
      ),
    |]
  where
    -- Denied rate-limited jobs are parked. The decision set carries them with a
    -- defer time alongside the claimed ids. Parking a stale-leased job flips
    -- claimed_by, which makes the update trigger lock its count row. Such a job
    -- is parked when this claim holds that row. Any other such job stays visible
    -- for a later cycle.
    deferUnion
      | admitConcurrent admission =
          [text|
            UNION ALL
            SELECT denied.id, FALSE, denied.defer_until
            FROM rl_deferred denied
            JOIN locked candidate ON candidate.id = denied.id
            WHERE candidate.claimed_by IS NULL
               OR candidate.concurrency_key IS NULL
               OR EXISTS (
                    SELECT 1 FROM conc_locked pool
                    WHERE pool.concurrency_key = candidate.concurrency_key
                  )
          |]
      | otherwise = "UNION ALL SELECT id, FALSE, defer_until FROM rl_deferred"

-- | The @claimed@ UPDATE. Rate limiting splits it into an admit/defer decision.
-- Without it, a straight claim of the admitted ids. A defer clears the holder and
-- moves the token.
claimedCte :: ClaimAdmission -> Text -> Text -> Text
claimedCte admission tbl timeout
  | admitRateLimited admission =
      [text|
        claimed AS (
          UPDATE ${tbl} job
          SET not_visible_until = CASE
                WHEN verdict._admit THEN NOW() + (${timeout} * interval '1 second')
                ELSE verdict._defer
              END,
              attempts = CASE
                WHEN verdict._admit THEN job.attempts + 1
                ELSE job.attempts
              END,
              claim_seq = job.claim_seq + 1,
              last_attempted_at = CASE
                WHEN verdict._admit THEN NOW()
                ELSE job.last_attempted_at
              END,
              updated_at = NOW(),
              throttled_until = CASE
                WHEN verdict._admit THEN NULL
                ELSE verdict._defer
              END,
              claimed_by =
                (CASE WHEN verdict._admit THEN ?::uuid ELSE NULL END)::uuid
          FROM decision verdict
          WHERE job.id = verdict.id
          RETURNING job.*, verdict._admit
        )
      |]
  | otherwise =
      [text|
        claimed AS (
          UPDATE ${tbl} job
          SET not_visible_until = NOW() + (${timeout} * interval '1 second'),
              attempts = job.attempts + 1,
              claim_seq = job.claim_seq + 1,
              last_attempted_at = NOW(),
              updated_at = NOW(),
              claimed_by = ?::uuid
          FROM admitted admitted_row
          WHERE job.id = admitted_row.id
          RETURNING job.*
        )
      |]

-- | The single-CTE batched claim, which at batch size 1 is the single-job claim. Takes
-- any unsuspended visible job, rollup children and woken rollup parents included.
-- Each gate's CTEs render when the payload declares that kind of policy.
claimJobsBatchedSQL :: SchemaName -> TableName -> ClaimAdmission -> Int -> Int -> NominalDiffTime -> Text
claimJobsBatchedSQL schema tableName admission batchSize maxBatches timeoutSeconds =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      buckets = arbiterRateLimitsTable schema
      concTbl = arbiterConcurrencyTable schema
      concPolicies = arbiterConcurrencyPoliciesTable schema
      rlPolicies = arbiterRateLimitPoliciesTable schema
      batchLimit = T.pack (show batchSize)
      batchBudget = T.pack (show maxBatches)
      timeout = T.pack (show (realToFrac timeoutSeconds :: Double))
      ungroupedLimit = T.pack (show (maxBatches * batchSize))
      overfetch = T.pack (show (maxBatches * 10))
      dma = T.pack (show defaultMaxAttempts)
      hasRateLimit = admitRateLimited admission
      hasConcurrency = admitConcurrent admission
      jobHeadroom = concHeadroomPred concTbl concPolicies "job"
      headHeadroom = concHeadroomPred concTbl concPolicies "gated_head"
      ccGate = mwhen hasConcurrency [text|AND ${jobHeadroom}|]
      gateLateral = mwhen hasConcurrency (gatedHeadLateral tbl dma batchLimit headHeadroom)
      groupCandidates = groupCandidateCtes groupsTbl tbl overfetch dma gateLateral
      ungroupedPool = ungroupedPoolCtes tbl ungroupedLimit batchLimit dma ccGate
      allocatedSlots = allocatedSlotCtes batchBudget
      lockedCandidates = lockedCandidateCtes tbl batchLimit dma ungroupedLimit
      concGate = mwhen hasConcurrency (concGateCtes concTbl concPolicies)
      rlGate = mwhen hasRateLimit (rlGateCtes buckets rlPolicies)
      admitted = admittedCte admission
      rlSpend = mwhen hasRateLimit (rlSpendCtes buckets)
      decision = decisionCte admission
      claimed = claimedCte admission tbl timeout
      admitFilter = mwhen hasRateLimit " WHERE _admit"
   in [text|
        WITH
        ${groupCandidates},
        ${ungroupedPool},
        ${allocatedSlots},
        ${lockedCandidates},
        ${concGate}
        ${rlGate}
        ${admitted},
        ${rlSpend}
        ${decision}
        ${claimed}
        SELECT ${jobColumns} FROM claimed${admitFilter} ORDER BY priority ASC, id ASC
      |]

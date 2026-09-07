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
import Data.UUID.Types (UUID)
import NeatInterpolation (text)

import Arbiter.Core.Admission (effectivePolicyCol)
import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyPoliciesTable, arbiterConcurrencyTable)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Job.Types (defaultMaxAttempts)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTable, arbiterRateLimitsTable, bucketSeedInsert)
import Arbiter.Core.Sql.Jobs (jobColumns)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, mwhen)
import Arbiter.Core.Sql.RateLimit (defaultThrottleWaitSeconds, refilledExpr)

-- | Which admission filters the claim SQL renders. A payload type that declares
-- no policy of a kind gets no filter for it.
data ClaimAdmission = ClaimAdmission
  { admitRateLimited :: Bool
  , admitConcurrent :: Bool
  }
  deriving stock (Eq, Show)

-- | Per-group rank of candidate rows (alias @candidate@), which each gate's group cut ranks by.
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

-- | Lock each referenced count row. Whoever holds the row gates that key this cycle.
-- eff_limit is COALESCE(pool override, default). An undeclared prefix leaves it NULL
-- and runs uncapped. Emits @conc_locked@.
concLockedCte :: Text -> Text -> Text
concLockedCte concTbl concPolicies =
  let effLimit = effectivePolicyCol "policy" "limit"
   in [text|
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
      |]

-- | Seed a bucket for each candidate key without one. This claim does not see it.
rlSeedCte :: Text -> Text -> Text
rlSeedCte buckets rlPolicies =
  let seed = bucketSeedInsert buckets rlPolicies "locked n" "n.rate_limit_key IS NOT NULL"
   in [text|
        rl_seed AS (
          ${seed}
        ),
      |]

-- | The candidate's bucket, locked and refilled under its policy. Absent when another
-- claimer holds it.
rlBucketLateral :: Text -> Text -> Text
rlBucketLateral buckets rlPolicies =
  let maxCol = effectivePolicyCol "policy" "max_tokens"
      refillCol = effectivePolicyCol "policy" "refill_amount"
      intervalCol = effectivePolicyCol "policy" "interval"
      availCol = refilledExpr maxCol "stored_bucket.tokens" "stored_bucket.last_refill" refillCol intervalCol
   in [text|
        LEFT JOIN LATERAL (
          SELECT stored_bucket.rate_limit_key,
                 ${maxCol} AS max_tokens, ${refillCol} AS refill_amount, ${intervalCol} AS refill_interval,
                 ${availCol} AS available
          FROM ${buckets} stored_bucket
          JOIN ${rlPolicies} policy ON policy.prefix_id = stored_bucket.policy_prefix
          WHERE stored_bucket.rate_limit_key = candidate.rate_limit_key
          FOR UPDATE OF stored_bucket SKIP LOCKED
        ) bucket ON TRUE
      |]

-- | The conjunction of every rendered gate's admission column.
admitExpr :: ClaimAdmission -> Text
admitExpr admission =
  T.intercalate " AND " $
    ["conc_admit" | admitConcurrent admission] <> ["rl_admit" | admitRateLimited admission]

-- | Every rendered gate's verdict on every locked row in one windowed pass. A row
-- carries its rank in its group, its own admission per gate, the group cut per gate,
-- and the cost its key admits. Emits @judged@.
judgedCte :: ClaimAdmission -> Text -> Text -> Text -> Text
judgedCte admission rlPolicies concJoin rlJoin =
  let cc = admitConcurrent admission
      rl = admitRateLimited admission
      admit = admitExpr admission
      concRow =
        mwhen
          cc
          [text|,
            (pool.concurrency_key IS NOT NULL) AS pool_held,
            (candidate.concurrency_key IS NULL
              OR (pool.concurrency_key IS NOT NULL
                  AND (candidate.claimed_by IS NOT NULL OR pool.eff_limit IS NULL
                       OR pool.in_flight + SUM((candidate.claimed_by IS NULL)::int) OVER (
                            PARTITION BY candidate.concurrency_key ORDER BY candidate.priority ASC, candidate.id ASC
                            ROWS UNBOUNDED PRECEDING
                          ) <= pool.eff_limit))) AS conc_ok
          |]
      rlRow =
        mwhen
          rl
          [text|,
            (bucket.rate_limit_key IS NOT NULL) AS bucket_held,
            bucket.max_tokens, bucket.refill_amount, bucket.refill_interval, bucket.available,
            ${clampedCostExpr} AS clamped_cost,
            CASE WHEN bucket.rate_limit_key IS NOT NULL
                 THEN bucket.max_tokens > 0
                      AND SUM(${clampedCostExpr}) OVER (
                            PARTITION BY candidate.rate_limit_key ORDER BY candidate.priority ASC, candidate.id ASC
                            ROWS UNBOUNDED PRECEDING
                          ) <= bucket.available
                 ELSE candidate.rate_limit_key IS NULL
                      OR NOT EXISTS (
                        SELECT 1 FROM ${rlPolicies} policy WHERE policy.prefix_id = candidate.rate_limit_prefix
                      )
            END AS rl_ok
          |]
      concCut = mwhen cc ",\n MIN(CASE WHEN NOT conc_ok THEN grp_rank END) OVER (PARTITION BY group_key) AS conc_cut"
      rlCut = mwhen rl ",\n MIN(CASE WHEN NOT rl_ok THEN grp_rank END) OVER (PARTITION BY group_key) AS rl_cut"
      admitCols =
        T.intercalate ",\n" $
          ["(conc_ok AND (grp_rank IS NULL OR conc_cut IS NULL OR grp_rank < conc_cut)) AS conc_admit" | cc]
            <> ["(rl_ok AND (grp_rank IS NULL OR rl_cut IS NULL OR grp_rank < rl_cut)) AS rl_admit" | rl]
      keyCost =
        mwhen
          rl
          [text|,
            SUM(CASE WHEN ${admit} THEN clamped_cost ELSE 0 END) OVER (PARTITION BY rate_limit_key) AS key_admitted_cost
          |]
   in [text|
        judged AS MATERIALIZED (
          SELECT judged_admit.*${keyCost}
          FROM (
            SELECT judged_cut.*,
              ${admitCols}
            FROM (
              SELECT judged_row.*${concCut}${rlCut}
              FROM (
                SELECT candidate.id, candidate.priority, candidate.group_key, candidate.claimed_by,
                       candidate.concurrency_key, candidate.rate_limit_key, candidate.rate_limit_prefix,
                       candidate.rate_limit_cost,
                       ${grpRankExpr}${concRow}${rlRow}
                FROM locked candidate
                ${concJoin}
                ${rlJoin}
              ) judged_row
            ) judged_cut
          ) judged_admit
        ),
      |]

-- | Rate-limit spend. Debits each held bucket by the cost admitted by every gate, banks
-- the accrued refill, then computes a jittered defer time for denied keyed jobs. A
-- stale-leased job on a count row this claim does not hold stays visible for a later
-- cycle, since parking it would make the update trigger lock that row.
rlSpendCtes :: ClaimAdmission -> Text -> Text
rlSpendCtes admission buckets =
  let fallback = T.pack (show defaultThrottleWaitSeconds)
      admit = admitExpr admission
      deferGuard = mwhen (admitConcurrent admission) " AND (claimed_by IS NULL OR concurrency_key IS NULL OR pool_held)"
      -- Time to accrue the token deficit when the policy refills, else one
      -- interval, else the fallback.
      rlWaitExpr =
        [text|
          CASE WHEN judged.refill_amount > 0 AND judged.refill_interval > 0 AND judged.max_tokens > 0
               THEN (judged.refill_interval / judged.refill_amount)
                    * GREATEST(1.0, judged.clamped_cost - (judged.available - judged.key_admitted_cost))
               ELSE COALESCE(NULLIF(judged.refill_interval, 0), ${fallback})
          END
        |]
   in [text|
        rl_spend_agg AS (
          SELECT rate_limit_key, MAX(available) AS available, SUM(clamped_cost) AS admitted_cost
          FROM judged
          WHERE ${admit} AND rate_limit_key IS NOT NULL AND bucket_held
          GROUP BY rate_limit_key
        ),
        rl_spent AS (
          UPDATE ${buckets} stored_bucket
          SET tokens = spend.available - spend.admitted_cost, last_refill = NOW()
          FROM rl_spend_agg spend
          WHERE stored_bucket.rate_limit_key = spend.rate_limit_key AND spend.admitted_cost > 0
          RETURNING stored_bucket.rate_limit_key
        ),
        rl_deferred AS (
          SELECT id,
            NOW() + ((wait + random() * LEAST(wait * 0.5, 2.0))
                     * interval '1 second') AS defer_until
          FROM (
            SELECT judged.id, GREATEST(${rlWaitExpr}, 0) AS wait
            FROM judged
            WHERE NOT rl_admit AND bucket_held${deferGuard}
          ) denied
        ),
      |]

-- | The claimable set. The rows every rendered gate admits, or the full locked set
-- when no gate is rendered.
admittedCte :: ClaimAdmission -> Text
admittedCte admission =
  [text|
    admitted AS (
      ${body}
    ),
  |]
  where
    body
      | admitConcurrent admission || admitRateLimited admission =
          let admit = admitExpr admission
           in [text|SELECT id FROM judged WHERE ${admit}|]
      | otherwise = "SELECT id FROM locked"

-- | With rate limiting the claim splits into an admit/defer decision. Denied keyed jobs
-- are parked with a defer time alongside the claimed ids.
decisionCte :: ClaimAdmission -> Text
decisionCte admission =
  mwhen
    (admitRateLimited admission)
    [text|
      decision AS (
        SELECT id, TRUE AS _admit, NULL::timestamptz AS _defer FROM admitted
        UNION ALL
        SELECT id, FALSE, defer_until FROM rl_deferred
      ),
    |]

-- | The @claimed@ UPDATE and the final SELECT, over the CTEs rendered before them.
claimStatement :: ClaimAdmission -> Text -> Text -> Text -> UUID -> Query ()
claimStatement admission tbl timeout ctes claimant =
  let claimed = claimedCte admission tbl timeout claimant
      admitFilter = mwhen (admitRateLimited admission) (" WHERE _admit" :: Text)
   in [sql|
        WITH
        ${ctes}
        ${claimed}
        SELECT ${jobColumns} FROM claimed${admitFilter} ORDER BY priority ASC, id ASC
      |]

-- | The @claimed@ UPDATE. Under rate limiting a deferred row moves its claim token
-- and parks, taking no attempt.
claimedCte :: ClaimAdmission -> Text -> Text -> UUID -> Query ()
claimedCte admission tbl timeout claimant
  | admitRateLimited admission =
      [sql|
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
                (CASE WHEN verdict._admit THEN #{claimant :: CUuid} ELSE NULL END)::uuid
          FROM decision verdict
          WHERE job.id = verdict.id
          RETURNING job.*, verdict._admit
        )
      |]
  | otherwise =
      [sql|
        claimed AS (
          UPDATE ${tbl} job
          SET not_visible_until = NOW() + (${timeout} * interval '1 second'),
              attempts = job.attempts + 1,
              claim_seq = job.claim_seq + 1,
              last_attempted_at = NOW(),
              updated_at = NOW(),
              claimed_by = #{claimant :: CUuid}::uuid
          FROM admitted admitted_row
          WHERE job.id = admitted_row.id
          RETURNING job.*
        )
      |]

-- | The single-CTE batched claim, which at batch size 1 is the single-job claim. Takes
-- any unsuspended visible job, rollup children and woken rollup parents included.
-- Each gate's CTEs render when the payload declares that kind of policy.
claimJobsBatchedSQL :: SchemaName -> TableName -> ClaimAdmission -> Int -> Int -> NominalDiffTime -> UUID -> Query ()
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
      concLocked = mwhen hasConcurrency (concLockedCte concTbl concPolicies)
      rlSeed = mwhen hasRateLimit (rlSeedCte buckets rlPolicies)
      concJoin = mwhen hasConcurrency "LEFT JOIN conc_locked pool ON pool.concurrency_key = candidate.concurrency_key"
      rlJoin = mwhen hasRateLimit (rlBucketLateral buckets rlPolicies)
      judged = mwhen (hasConcurrency || hasRateLimit) (judgedCte admission rlPolicies concJoin rlJoin)
      admitted = admittedCte admission
      rlSpend = mwhen hasRateLimit (rlSpendCtes admission buckets)
      decision = decisionCte admission
      ctes =
        [text|
          ${groupCandidates},
          ${ungroupedPool},
          ${allocatedSlots},
          ${lockedCandidates},
          ${concLocked}
          ${rlSeed}
          ${judged}
          ${admitted}
          ${rlSpend}
          ${decision}
        |]
   in claimStatement admission tbl timeout ctes

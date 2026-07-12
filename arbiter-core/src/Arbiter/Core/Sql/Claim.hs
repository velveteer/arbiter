{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Claim-time SQL: candidate selection, admission gates, and the batched claim.
module Arbiter.Core.Sql.Claim
  ( ClaimAdmission (..)
  , claimJobsBatchedSQL
  ) where

import Data.Foldable (fold)
import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Admission (effectivePolicyCol, unpolicied)
import Arbiter.Core.Concurrency.Schema (arbiterConcurrencyPoliciesTable, arbiterConcurrencyTable)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueGroupsTable, jobQueueTable)
import Arbiter.Core.Job.Types (defaultMaxAttempts)
import Arbiter.Core.Queues (arbiterQueuesTable)
import Arbiter.Core.RateLimit.Schema (arbiterRateLimitPoliciesTable, arbiterRateLimitsTable, bucketSeedInsert)
import Arbiter.Core.Sql.Jobs (jobColumns)
import Arbiter.Core.Sql.RateLimit (defaultThrottleWaitSeconds, refilledExpr)
import Arbiter.Core.SqlLiterals (textLiteral)

-- | Which admission filters the claim SQL renders, and so which policies this process
-- enforces. An unrendered kind refuses the jobs its policies govern rather than admit
-- them ungated, so a stale value costs throughput, never a breached limit.
data ClaimAdmission = ClaimAdmission
  { admitRateLimited :: Bool
  , admitConcurrent :: Bool
  }
  deriving stock (Eq, Show)

-- | Join non-empty CTE fragments into a WITH body, so each fragment stays
-- comma-free and an optional one drops out cleanly.
cteList :: [Text] -> Text
cteList = T.intercalate ",\n" . filter (not . T.null . T.strip)

-- | Group-FIFO cut over a @<p>_self@ CTE of (id, group_key, grp_rank, self_ok), emitting @<p>_group_cut@ and @<p>_admitted@.
groupFifoCutCtes :: Text -> Text
groupFifoCutCtes p =
  [text|
    ${p}_group_cut AS (
      SELECT group_key, MIN(grp_rank) AS cut_rank
      FROM ${p}_self
      WHERE group_key IS NOT NULL AND NOT self_ok
      GROUP BY group_key
    ),
    ${p}_admitted AS (
      SELECT s.id FROM ${p}_self s
      LEFT JOIN ${p}_group_cut gc ON gc.group_key = s.group_key
      WHERE s.self_ok
        AND (s.grp_rank IS NULL OR gc.cut_rank IS NULL OR s.grp_rank < gc.cut_rank)
    )
  |]

-- | Per-group rank of candidate rows (alias @l@), the shape 'groupFifoCutCtes' cuts on.
grpRankExpr :: Text
grpRankExpr =
  [text|
    CASE WHEN l.group_key IS NOT NULL
         THEN ROW_NUMBER() OVER (PARTITION BY l.group_key ORDER BY l.priority ASC, l.id ASC)
         END AS grp_rank
  |]

-- | A job's cost, floored at 0 and capped at the bucket max. Used identically by
-- admission, debit, and the deny-wait so they never disagree.
clampedCostExpr :: Text
clampedCostExpr = "LEAST(GREATEST(l.rate_limit_cost, 0), rl.mx)"

-- | Concurrency gate: lock each referenced count row, reserve a slot per fresh
-- candidate up to the pool headroom, then the group-FIFO cut. Emits @conc_admitted@.
concGateCtes :: Text -> Text -> Text
concGateCtes concTbl concPolicies =
  let effLimit = effectivePolicyCol "p" "limit"
   in [text|
        -- Whoever locks the count row gates that key this cycle (others skip and wait).
        -- eff_limit is COALESCE(pool override, default). An undeclared prefix leaves it
        -- NULL and runs uncapped.
        conc_locked AS (
          SELECT c.concurrency_key, c.in_flight,
                 ${effLimit} AS eff_limit
          FROM ${concTbl} c
          LEFT JOIN ${concPolicies} p ON p.prefix_id = c.concurrency_prefix
          WHERE c.concurrency_key IN (SELECT concurrency_key FROM locked WHERE concurrency_key IS NOT NULL)
          FOR UPDATE OF c SKIP LOCKED
        ),
        -- Ungated jobs pass straight through. Keyed jobs join their held pool row (a key
        -- locked by another claimer or absent drops out). fresh_rn reserves a slot per
        -- fresh candidate in priority order, admitting up to the pool's headroom.
        conc_self_ok AS (
          SELECT id FROM locked WHERE concurrency_key IS NULL
          UNION ALL
          SELECT z.id FROM (
            SELECT l.id, l.claimed_by, cl.in_flight, cl.eff_limit,
              SUM((l.claimed_by IS NULL)::int) OVER
                (PARTITION BY l.concurrency_key ORDER BY l.priority ASC, l.id ASC ROWS UNBOUNDED PRECEDING) AS fresh_rn
            FROM locked l
            JOIN conc_locked cl ON cl.concurrency_key = l.concurrency_key
          ) z
          WHERE z.claimed_by IS NOT NULL
             OR z.eff_limit IS NULL
             OR z.in_flight + z.fresh_rn <= z.eff_limit
        ),
        -- Normalize to the shared (id, group_key, grp_rank, self_ok) shape for the cut.
        conc_self AS (
          SELECT l.id, l.group_key,
            ${grpRankExpr},
            (so.id IS NOT NULL) AS self_ok
          FROM locked l
          LEFT JOIN conc_self_ok so ON so.id = l.id
        ),
      |]
        <> groupFifoCutCtes "conc"

-- | Rate-limit admission: lock and refill each referenced bucket, seed a missing one,
-- admit per key in priority order while cumulative cost fits, then the group-FIFO cut.
-- Emits @rl_admitted@.
rlGateCtes :: Text -> Text -> Text
rlGateCtes buckets rlPolicies =
  let mxCol = effectivePolicyCol "p" "max_tokens"
      rfCol = effectivePolicyCol "p" "refill_amount"
      ivCol = effectivePolicyCol "p" "interval"
      availCol = refilledExpr mxCol "b.tokens" "b.last_refill" rfCol ivCol
      seed = bucketSeedInsert buckets rlPolicies "locked n" "n.rate_limit_key IS NOT NULL"
      unpoliciedRl = unpolicied rlPolicies "l" "rate_limit"
   in [text|
        rl_locked AS (
          SELECT b.rate_limit_key,
                 ${mxCol} AS mx, ${rfCol} AS rf, ${ivCol} AS iv,
                 ${availCol} AS available
          FROM ${buckets} b
          JOIN ${rlPolicies} p ON p.prefix_id = b.policy_prefix
          WHERE b.rate_limit_key IN (SELECT rate_limit_key FROM locked WHERE rate_limit_key IS NOT NULL)
          FOR UPDATE OF b SKIP LOCKED
        ),
        rl_seed AS (
          ${seed}
        ),
        -- Per-key admission in priority order while cumulative cost fits (keyed jobs only).
        rl_keyed AS (
          SELECT l.id,
            (rl.mx > 0 AND
             SUM(${clampedCostExpr}) OVER
               (PARTITION BY l.rate_limit_key ORDER BY l.priority ASC, l.id ASC ROWS UNBOUNDED PRECEDING)
               <= rl.available) AS key_ok
          FROM locked l
          JOIN rl_locked rl ON rl.rate_limit_key = l.rate_limit_key
        ),
        -- Own verdict plus per-group rank. Unkeyed/unpolicied passes. A keyed job with no locked bucket fails.
        rl_self AS (
          SELECT l.id, l.group_key,
            COALESCE(k.key_ok, ${unpoliciedRl}) AS self_ok,
            ${grpRankExpr}
          FROM locked l
          LEFT JOIN rl_keyed k ON k.id = l.id
        ),
      |]
        <> groupFifoCutCtes "rl"

-- | Rate-limit spend: debit each bucket by the cost actually admitted by every gate
-- and bank the accrued refill, then compute a jittered defer time for denied keyed jobs.
rlSpendCtes :: Text -> Text
rlSpendCtes buckets =
  let fallback = T.pack (show defaultThrottleWaitSeconds)
      -- Time to accrue the token deficit when the policy refills, else one interval, else the fallback.
      rlWaitExpr =
        "CASE WHEN rl.rf > 0 AND rl.iv > 0 AND rl.mx > 0 THEN (rl.iv / rl.rf) * GREATEST(1.0, "
          <> clampedCostExpr
          <> " - (rl.available - COALESCE(sa.admitted_cost, 0))) ELSE COALESCE(NULLIF(rl.iv, 0), "
          <> fallback
          <> ") END"
   in [text|
        rl_spend_agg AS (
          SELECT l.rate_limit_key,
                 SUM(${clampedCostExpr}) AS admitted_cost
          FROM locked l
          JOIN admitted a ON a.id = l.id
          JOIN rl_locked rl ON rl.rate_limit_key = l.rate_limit_key
          WHERE l.rate_limit_key IS NOT NULL
          GROUP BY l.rate_limit_key
        ),
        rl_spent AS (
          UPDATE ${buckets} b
          SET tokens = rl.available - sa.admitted_cost, last_refill = NOW()
          FROM rl_locked rl
          JOIN rl_spend_agg sa ON sa.rate_limit_key = rl.rate_limit_key AND sa.admitted_cost > 0
          WHERE b.rate_limit_key = rl.rate_limit_key
          RETURNING b.rate_limit_key
        ),
        rl_deferred AS (
          SELECT id,
            NOW() + ((wait + random() * LEAST(wait * 0.5, 2.0)) * interval '1 second') AS defer_until
          FROM (
            SELECT l.id, GREATEST(${rlWaitExpr}, 0) AS wait
            FROM locked l
            JOIN rl_locked rl ON rl.rate_limit_key = l.rate_limit_key
            LEFT JOIN rl_spend_agg sa ON sa.rate_limit_key = l.rate_limit_key
            WHERE NOT EXISTS (SELECT 1 FROM rl_admitted a WHERE a.id = l.id)
          ) d
        )
      |]

-- | Candidate stage one: groups with ready or due work, locked and reduced to each head row.
groupCandidateCtes :: Text -> Text -> Text -> Text -> Text
groupCandidateCtes groupsTbl tbl overfetch dma =
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
      SELECT g.group_key FROM ${groupsTbl} g
      WHERE g.group_key IN (SELECT group_key FROM group_candidates)
        AND g.job_count > 0
        AND (g.in_flight_until IS NULL OR g.in_flight_until <= NOW())
      ORDER BY g.min_priority ASC, g.min_id ASC
      LIMIT ${overfetch}
      FOR UPDATE SKIP LOCKED
    ),
    eligible_heads AS (
      SELECT el.group_key, h.min_priority, h.min_id
      FROM eligible_groups el
      CROSS JOIN LATERAL (
        SELECT t.priority AS min_priority, t.id AS min_id
        FROM ${tbl} t
        WHERE t.group_key = el.group_key
          AND NOT t.suspended
          AND (t.not_visible_until IS NULL OR t.not_visible_until <= NOW())
          AND t.attempts < COALESCE(t.max_attempts, ${dma})
        ORDER BY t.priority ASC, t.id ASC
        LIMIT 1
      ) h
    )
  |]

-- | Candidate stage two: the ungrouped ready and due pools, numbered into batches.
ungroupedPoolCtes :: Text -> Text -> Text -> Text -> Text -> Text
ungroupedPoolCtes tbl ungroupedLimit bs dma poolGate =
  [text|
    ungrouped_pool AS (
      (
        SELECT j.id, j.priority
        FROM ${tbl} j
        WHERE j.group_key IS NULL
          AND NOT j.suspended
          AND j.not_visible_until IS NULL
          AND j.attempts < COALESCE(j.max_attempts, ${dma})
          ${poolGate}
        ORDER BY j.priority ASC, j.id ASC
        LIMIT ${ungroupedLimit}
      )
      UNION ALL
      (
        SELECT j.id, j.priority
        FROM ${tbl} j
        WHERE j.group_key IS NULL
          AND NOT j.suspended
          AND j.not_visible_until IS NOT NULL
          AND j.not_visible_until <= NOW()
          AND j.attempts < COALESCE(j.max_attempts, ${dma})
          ${poolGate}
        ORDER BY j.not_visible_until ASC
        LIMIT ${ungroupedLimit}
      )
    ),
    ungrouped_numbered AS (
      SELECT id, priority,
        ((ROW_NUMBER() OVER (ORDER BY priority ASC, id ASC) - 1)
          / ${bs}) + 1 AS batch_num
      FROM ungrouped_pool
      ORDER BY priority ASC, id ASC
      LIMIT ${ungroupedLimit}
    ),
    ungrouped_batch_info AS (
      SELECT batch_num, MIN(priority) AS min_priority, MIN(id) AS min_id
      FROM ungrouped_numbered
      GROUP BY batch_num
    )
  |]

-- | Candidate stage three: interleave group heads and ungrouped batches by priority, capped at the batch budget.
allocatedSlotCtes :: Text -> Text
allocatedSlotCtes mb =
  [text|
    allocated_slots AS (
      SELECT s.group_key, s.ungrouped_batch
      FROM (
        SELECT group_key, NULL::bigint AS ungrouped_batch, min_priority, min_id
        FROM eligible_heads
        UNION ALL
        SELECT NULL::text, batch_num, min_priority, min_id
        FROM ungrouped_batch_info
        ORDER BY min_priority ASC, min_id ASC
      ) s
      LIMIT ${mb}
    ),
    final_locked_groups AS (
      SELECT group_key FROM allocated_slots WHERE group_key IS NOT NULL
    )
  |]

-- | Candidate stage four: resolve the allocated slots to rows and lock the claimable set.
lockedCandidateCtes :: Text -> Text -> Text -> Text -> Text -> Text -> Text -> Text
lockedCandidateCtes tbl bs dma lockedCols ungroupedLimit notPaused lockedGate =
  [text|
    grouped_candidates AS (
      SELECT j.id, flg.group_key AS expected_group
      FROM final_locked_groups flg
      CROSS JOIN LATERAL (
        SELECT id
        FROM ${tbl}
        WHERE group_key = flg.group_key
          AND NOT suspended
          AND (not_visible_until IS NULL OR not_visible_until <= NOW())
          AND attempts < COALESCE(max_attempts, ${dma})
        ORDER BY attempts DESC, priority ASC, id ASC
        LIMIT ${bs}
      ) j
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
      SELECT ${lockedCols}
      FROM (
        SELECT id, expected_group FROM grouped_candidates
        UNION ALL
        SELECT id, expected_group FROM ungrouped_candidates
      ) i
      INNER JOIN ${tbl} j ON j.id = i.id
      WHERE NOT j.suspended
        AND ${notPaused}
        AND (j.not_visible_until IS NULL OR j.not_visible_until <= NOW())
        AND j.group_key IS NOT DISTINCT FROM i.expected_group
        AND j.attempts < COALESCE(j.max_attempts, ${dma})
        ${lockedGate}
      ORDER BY j.priority ASC, j.id ASC
      FOR UPDATE OF j SKIP LOCKED
      LIMIT ${ungroupedLimit}
    )
  |]

-- | Batched single-CTE claim. Also serves the single-job claim at batch size 1.
--
-- Claims any unsuspended visible job, including rollup children and woken
-- rollup parents. Binds the lease timeout, then the claimant.
claimJobsBatchedSQL :: SchemaName -> TableName -> ClaimAdmission -> Int -> Int -> Text
claimJobsBatchedSQL schema tableName admission batchSize maxBatches =
  let tbl = jobQueueTable schema tableName
      groupsTbl = jobQueueGroupsTable schema tableName
      buckets = arbiterRateLimitsTable schema
      concTbl = arbiterConcurrencyTable schema
      concPolicies = arbiterConcurrencyPoliciesTable schema
      rlPolicies = arbiterRateLimitPoliciesTable schema
      columns = jobColumns Nothing
      bs = T.pack (show batchSize)
      mb = T.pack (show maxBatches)
      ungroupedLimit = T.pack (show (maxBatches * batchSize))
      overfetch = T.pack (show (maxBatches * 10))
      dma = T.pack (show defaultMaxAttempts)
      queuesTbl = arbiterQueuesTable schema
      hasRateLimit = admitRateLimited admission
      hasConcurrency = admitConcurrent admission
      queueLit = textLiteral tableName
      notPaused =
        [text|NOT EXISTS (SELECT 1 FROM ${queuesTbl} q WHERE q.queue_name = ${queueLit} AND q.paused)|]
      -- Ungrouped-pool concurrency headroom pre-filter, rendered only for a type that
      -- declares a pool. Keeps a full key off the bounded candidate window.
      effLimit = effectivePolicyCol "p" "limit"
      ccHeadroom
        | hasConcurrency =
            [text|
              AND (j.claimed_by IS NOT NULL OR j.concurrency_key IS NULL OR EXISTS (
                SELECT 1 FROM ${concTbl} c
                LEFT JOIN ${concPolicies} p ON p.prefix_id = c.concurrency_prefix
                WHERE c.concurrency_key = j.concurrency_key
                  AND (${effLimit} IS NULL
                       OR c.in_flight < ${effLimit})
              ))
            |]
        | otherwise = ""
      -- An unrendered gate refuses the jobs its policies govern, so an admission that
      -- predates a policy cannot admit past it. An unpolicied key runs uncapped either way.
      refuse policies stem = "AND " <> unpolicied policies "j" stem
      lockedGate =
        fold
          [ if hasConcurrency then "" else refuse concPolicies "concurrency"
          , if hasRateLimit then "" else refuse rlPolicies "rate_limit"
          ]
      poolGate = ccHeadroom <> lockedGate
      -- The locked CTE projects only what the rendered gates read, so a gateless
      -- claim materializes bare ids.
      lockedCols =
        T.intercalate ", " $
          concat
            [ ["j.id"]
            , if hasConcurrency || hasRateLimit then ["j.priority", "j.group_key"] else []
            , if hasConcurrency then ["j.concurrency_key", "j.claimed_by"] else []
            , if hasRateLimit then ["j.rate_limit_key", "j.rate_limit_prefix", "j.rate_limit_cost"] else []
            ]
      -- The claimable set: the intersection of every active gate's admitted ids, or the
      -- full locked set when neither gate is rendered.
      admittedBody
        | hasConcurrency && hasRateLimit = "SELECT c.id FROM conc_admitted c JOIN rl_admitted r ON r.id = c.id"
        | hasConcurrency = "SELECT id FROM conc_admitted"
        | hasRateLimit = "SELECT id FROM rl_admitted"
        | otherwise = "SELECT id FROM locked"
      -- Denied rate-limited jobs are parked (not left for retry), so the decision set
      -- carries them with a defer time alongside the claimed ids. Parking a stale-leased
      -- job flips claimed_by, which makes the update trigger lock its count row. Only
      -- park such a job when this claim holds that row, else two claims can form a lock
      -- cycle. It stays visible for a later cycle.
      deferUnion
        | hasRateLimit && hasConcurrency =
            " UNION ALL SELECT d.id, FALSE, d.defer_until FROM rl_deferred d JOIN locked l ON l.id = d.id WHERE l.claimed_by IS NULL OR l.concurrency_key IS NULL OR EXISTS (SELECT 1 FROM conc_locked cl WHERE cl.concurrency_key = l.concurrency_key)"
        | hasRateLimit = " UNION ALL SELECT id, FALSE, defer_until FROM rl_deferred"
        | otherwise = ""
      admittedCte =
        [text|
          admitted AS (
            ${admittedBody}
          )
        |]
      -- Rate limiting can park a denied job, so the claim splits into an admit/defer decision.
      decisionCte
        | hasRateLimit =
            [text|
              decision AS (
                SELECT id, TRUE AS _admit, NULL::timestamptz AS _defer FROM admitted${deferUnion}
              )
            |]
        | otherwise = ""
      claimedCte
        | hasRateLimit =
            [text|
              claimed AS (
                UPDATE ${tbl} j
                SET not_visible_until = CASE WHEN dc._admit THEN NOW() + (?::float8 * interval '1 second') ELSE dc._defer END,
                    attempts = CASE WHEN dc._admit THEN j.attempts + 1 ELSE j.attempts END,
                    last_attempted_at = CASE WHEN dc._admit THEN NOW() ELSE j.last_attempted_at END,
                    updated_at = NOW(),
                    throttled_until = CASE WHEN dc._admit THEN NULL ELSE dc._defer END,
                    claimed_by = CASE WHEN dc._admit THEN ?::uuid ELSE NULL END
                FROM decision dc
                WHERE j.id = dc.id
                RETURNING j.*, dc._admit
              )
            |]
        | otherwise =
            [text|
              claimed AS (
                UPDATE ${tbl} j
                SET not_visible_until = NOW() + (?::float8 * interval '1 second'),
                    attempts = j.attempts + 1,
                    last_attempted_at = NOW(),
                    updated_at = NOW(),
                    claimed_by = ?::uuid
                FROM admitted a
                WHERE j.id = a.id
                RETURNING j.*
              )
            |]
      admitFilter = if hasRateLimit then " WHERE _admit" else ""
      -- Gate CTEs render only when the payload declares that kind of policy. cteList
      -- stitches the non-empty ones, so no fragment carries a trailing comma.
      withCtes =
        cteList
          [ groupCandidateCtes groupsTbl tbl overfetch dma
          , ungroupedPoolCtes tbl ungroupedLimit bs dma poolGate
          , allocatedSlotCtes mb
          , lockedCandidateCtes tbl bs dma lockedCols ungroupedLimit notPaused lockedGate
          , if hasConcurrency then concGateCtes concTbl concPolicies else ""
          , if hasRateLimit then rlGateCtes buckets rlPolicies else ""
          , admittedCte
          , if hasRateLimit then rlSpendCtes buckets else ""
          , decisionCte
          , claimedCte
          ]
   in [text|
        WITH
        ${withCtes}
        SELECT ${columns} FROM claimed${admitFilter} ORDER BY priority ASC, id ASC
      |]

ALTER TABLE "arbiter"."golden_jobs_groups" ADD COLUMN IF NOT EXISTS ready_count INT NOT NULL DEFAULT 0;
ALTER TABLE "arbiter"."golden_jobs_groups" ADD COLUMN IF NOT EXISTS next_due TIMESTAMPTZ;
UPDATE "arbiter"."golden_jobs_groups" g SET
  min_priority = sub.mp, min_id = sub.mi, ready_count = COALESCE(sub.rc, 0), next_due = sub.nd
FROM (
  SELECT group_key,
    MIN(priority) AS mp,
    MIN(id) AS mi,
    COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended) AS rc,
    MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended) AS nd
  FROM "arbiter"."golden_jobs" WHERE group_key IS NOT NULL GROUP BY group_key
) sub WHERE g.group_key = sub.group_key;
DROP INDEX IF EXISTS "arbiter"."idx_golden_jobs_groups_ranking";
CREATE INDEX IF NOT EXISTS "idx_golden_jobs_groups_ranking" ON "arbiter"."golden_jobs_groups" (min_priority ASC, min_id ASC) WHERE ready_count > 0 AND in_flight_until IS NULL;
CREATE INDEX IF NOT EXISTS "idx_golden_jobs_groups_next_due" ON "arbiter"."golden_jobs_groups" (next_due ASC) WHERE next_due IS NOT NULL;

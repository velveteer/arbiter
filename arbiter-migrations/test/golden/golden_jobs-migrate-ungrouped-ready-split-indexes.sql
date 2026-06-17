DROP INDEX IF EXISTS "arbiter"."idx_golden_jobs_ungrouped_ranking";
CREATE INDEX IF NOT EXISTS "idx_golden_jobs_ungrouped_ready_ranking"
ON "arbiter"."golden_jobs" (priority ASC, id ASC)
WHERE group_key IS NULL AND not_visible_until IS NULL AND NOT suspended;

CREATE INDEX IF NOT EXISTS "idx_golden_jobs_ungrouped_due"
ON "arbiter"."golden_jobs" (not_visible_until ASC)
WHERE group_key IS NULL AND not_visible_until IS NOT NULL AND NOT suspended;


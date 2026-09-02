CREATE INDEX IF NOT EXISTS "idx_golden_jobs_grouped_due"
ON "arbiter"."golden_jobs" (group_key, not_visible_until ASC)
WHERE group_key IS NOT NULL AND not_visible_until IS NOT NULL AND NOT suspended;

CREATE INDEX IF NOT EXISTS "idx_golden_jobs_group_key"
ON "arbiter"."golden_jobs" (group_key, priority ASC, id ASC)
WHERE group_key IS NOT NULL;

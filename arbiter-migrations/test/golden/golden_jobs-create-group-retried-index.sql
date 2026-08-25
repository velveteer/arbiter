CREATE INDEX IF NOT EXISTS "idx_golden_jobs_group_retried"
ON "arbiter"."golden_jobs" (group_key, attempts DESC, priority ASC, id ASC)
WHERE group_key IS NOT NULL AND attempts > 0;

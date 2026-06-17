CREATE INDEX IF NOT EXISTS "idx_golden_jobs_parent_id"
ON "arbiter"."golden_jobs" (parent_id)
WHERE parent_id IS NOT NULL;

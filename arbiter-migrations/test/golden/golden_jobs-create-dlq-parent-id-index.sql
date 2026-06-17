CREATE INDEX IF NOT EXISTS "idx_golden_jobs_dlq_parent_id"
ON "arbiter"."golden_jobs_dlq" (parent_id)
WHERE parent_id IS NOT NULL;

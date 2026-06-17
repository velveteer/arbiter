CREATE INDEX IF NOT EXISTS "idx_golden_jobs_dlq_group_key"
ON "arbiter"."golden_jobs_dlq" (group_key);

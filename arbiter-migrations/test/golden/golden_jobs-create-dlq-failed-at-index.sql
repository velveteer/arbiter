CREATE INDEX IF NOT EXISTS "idx_golden_jobs_dlq_failed_at"
ON "arbiter"."golden_jobs_dlq" (failed_at DESC);

CREATE INDEX IF NOT EXISTS "idx_golden_jobs_concurrency"
ON "arbiter"."golden_jobs" (concurrency_key)
WHERE concurrency_key IS NOT NULL;

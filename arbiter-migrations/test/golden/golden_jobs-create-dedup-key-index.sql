CREATE UNIQUE INDEX IF NOT EXISTS "idx_golden_jobs_dedup_key"
ON "arbiter"."golden_jobs" (dedup_key)
WHERE dedup_key IS NOT NULL;

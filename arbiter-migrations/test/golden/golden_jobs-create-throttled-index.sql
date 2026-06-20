CREATE INDEX IF NOT EXISTS "idx_golden_jobs_throttled"
ON "arbiter"."golden_jobs" (rate_limit_prefix, rate_limit_key)
WHERE throttled_until IS NOT NULL;

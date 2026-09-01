CREATE INDEX IF NOT EXISTS "idx_golden_jobs_group_in_flight"
ON "arbiter"."golden_jobs" (group_key, not_visible_until DESC NULLS LAST)
WHERE group_key IS NOT NULL AND not_visible_until IS NOT NULL AND NOT suspended AND (attempts > 0 OR throttled_until IS NOT NULL);

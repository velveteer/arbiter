CREATE INDEX IF NOT EXISTS "idx_golden_jobs_groups_emptied"
ON "arbiter"."golden_jobs_groups" (group_key)
WHERE job_count = 0;

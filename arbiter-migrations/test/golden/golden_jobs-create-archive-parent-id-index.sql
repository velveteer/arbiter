CREATE INDEX IF NOT EXISTS "idx_golden_jobs_archive_parent_id"
ON "arbiter"."golden_jobs_archive" (parent_id)
WHERE parent_id IS NOT NULL;

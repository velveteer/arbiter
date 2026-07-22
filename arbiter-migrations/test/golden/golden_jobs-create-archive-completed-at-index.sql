CREATE INDEX IF NOT EXISTS "idx_golden_jobs_archive_completed_at"
ON "arbiter"."golden_jobs_archive" (completed_at DESC);

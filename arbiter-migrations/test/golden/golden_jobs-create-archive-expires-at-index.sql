CREATE INDEX IF NOT EXISTS "idx_golden_jobs_archive_expires_at"
ON "arbiter"."golden_jobs_archive" (archive_expires_at);

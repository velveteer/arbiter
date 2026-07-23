ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS archive_for INT;
ALTER TABLE "arbiter"."golden_jobs_dlq" ADD COLUMN IF NOT EXISTS archive_for INT;

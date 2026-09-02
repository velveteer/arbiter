ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS kind TEXT;
ALTER TABLE "arbiter"."golden_jobs_dlq" ADD COLUMN IF NOT EXISTS kind TEXT;
ALTER TABLE "arbiter"."golden_jobs_archive" ADD COLUMN IF NOT EXISTS kind TEXT;

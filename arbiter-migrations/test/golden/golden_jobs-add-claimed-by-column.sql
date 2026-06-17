ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS claimed_by UUID;
ALTER TABLE "arbiter"."golden_jobs_dlq" ADD COLUMN IF NOT EXISTS claimed_by UUID;

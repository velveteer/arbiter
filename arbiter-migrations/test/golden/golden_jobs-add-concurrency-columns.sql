ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS concurrency_key TEXT;
ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS concurrency_prefix TEXT;
ALTER TABLE "arbiter"."golden_jobs_dlq" ADD COLUMN IF NOT EXISTS concurrency_key TEXT;
ALTER TABLE "arbiter"."golden_jobs_dlq" ADD COLUMN IF NOT EXISTS concurrency_prefix TEXT;

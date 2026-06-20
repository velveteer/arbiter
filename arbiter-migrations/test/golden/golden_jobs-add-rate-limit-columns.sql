ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS rate_limit_key TEXT;
ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS rate_limit_prefix TEXT;
ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS throttled_until TIMESTAMPTZ;
ALTER TABLE "arbiter"."golden_jobs_dlq" ADD COLUMN IF NOT EXISTS rate_limit_key TEXT;
ALTER TABLE "arbiter"."golden_jobs_dlq" ADD COLUMN IF NOT EXISTS rate_limit_prefix TEXT;

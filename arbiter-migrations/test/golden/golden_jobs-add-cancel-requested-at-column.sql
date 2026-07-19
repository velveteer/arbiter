ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS cancel_requested_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS "idx_golden_jobs_cancel_requested" ON "arbiter"."golden_jobs" (id ASC) WHERE cancel_requested_at IS NOT NULL;

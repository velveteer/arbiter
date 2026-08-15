ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS claim_seq BIGINT NOT NULL DEFAULT 0;
ALTER TABLE "arbiter"."golden_jobs_dlq" ADD COLUMN IF NOT EXISTS claim_seq BIGINT NOT NULL DEFAULT 0;
ALTER TABLE "arbiter"."golden_jobs_archive" ADD COLUMN IF NOT EXISTS claim_seq BIGINT NOT NULL DEFAULT 0;

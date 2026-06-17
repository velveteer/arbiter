UPDATE "arbiter"."golden_jobs" SET max_attempts = 10 WHERE max_attempts IS NULL;
ALTER TABLE "arbiter"."golden_jobs" ALTER COLUMN max_attempts SET DEFAULT 10;
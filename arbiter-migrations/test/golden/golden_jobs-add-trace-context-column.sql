ALTER TABLE "arbiter"."golden_jobs" ADD COLUMN IF NOT EXISTS traceparent TEXT, ADD COLUMN IF NOT EXISTS tracestate TEXT;
ALTER TABLE "arbiter"."golden_jobs_dlq" ADD COLUMN IF NOT EXISTS traceparent TEXT, ADD COLUMN IF NOT EXISTS tracestate TEXT;
ALTER TABLE "arbiter"."golden_jobs_archive" ADD COLUMN IF NOT EXISTS traceparent TEXT, ADD COLUMN IF NOT EXISTS tracestate TEXT;

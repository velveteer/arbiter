DROP TRIGGER IF EXISTS "notify_job_insert" ON "arbiter"."golden_jobs";
DROP TRIGGER IF EXISTS "notify_job_update" ON "arbiter"."golden_jobs";
DROP TRIGGER IF EXISTS "notify_job_delete" ON "arbiter"."golden_jobs";
DROP TRIGGER IF EXISTS "notify_dlq_insert" ON "arbiter"."golden_jobs_dlq";

DROP TRIGGER IF EXISTS "notify_job_event_golden_jobs" ON "arbiter"."golden_jobs";
CREATE TRIGGER "notify_job_event_golden_jobs"
AFTER INSERT OR UPDATE OR DELETE ON "arbiter"."golden_jobs"
FOR EACH ROW EXECUTE FUNCTION "arbiter"."notify_job_event"();

DROP TRIGGER IF EXISTS "notify_job_event_golden_jobs_dlq" ON "arbiter"."golden_jobs_dlq";
CREATE TRIGGER "notify_job_event_golden_jobs_dlq"
AFTER INSERT ON "arbiter"."golden_jobs_dlq"
FOR EACH ROW EXECUTE FUNCTION "arbiter"."notify_job_event"();

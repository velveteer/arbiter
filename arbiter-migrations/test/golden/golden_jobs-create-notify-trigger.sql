DROP TRIGGER IF EXISTS "golden_jobs_notify_trigger" ON "arbiter"."golden_jobs";
CREATE TRIGGER "golden_jobs_notify_trigger"
AFTER INSERT ON "arbiter"."golden_jobs"
FOR EACH ROW
EXECUTE FUNCTION "arbiter"."notify_golden_jobs_created"();

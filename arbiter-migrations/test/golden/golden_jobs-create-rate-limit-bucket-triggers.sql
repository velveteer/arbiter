DROP TRIGGER IF EXISTS "ensure_golden_jobs_rate_limit_buckets_insert" ON "arbiter"."golden_jobs";
CREATE TRIGGER "ensure_golden_jobs_rate_limit_buckets_insert"
AFTER INSERT ON "arbiter"."golden_jobs"
REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT EXECUTE FUNCTION "arbiter"."ensure_golden_jobs_rate_limit_buckets_insert"();

DROP TRIGGER IF EXISTS "ensure_golden_jobs_rate_limit_buckets_update" ON "arbiter"."golden_jobs";
CREATE TRIGGER "ensure_golden_jobs_rate_limit_buckets_update"
AFTER UPDATE ON "arbiter"."golden_jobs"
REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
FOR EACH STATEMENT EXECUTE FUNCTION "arbiter"."ensure_golden_jobs_rate_limit_buckets_update"();

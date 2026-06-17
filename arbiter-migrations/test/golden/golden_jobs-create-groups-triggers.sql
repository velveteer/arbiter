DROP TRIGGER IF EXISTS "maintain_golden_jobs_groups_insert" ON "arbiter"."golden_jobs";
CREATE TRIGGER "maintain_golden_jobs_groups_insert"
AFTER INSERT ON "arbiter"."golden_jobs"
REFERENCING NEW TABLE AS new_table
FOR EACH STATEMENT EXECUTE FUNCTION "arbiter"."maintain_golden_jobs_groups_insert"();

DROP TRIGGER IF EXISTS "maintain_golden_jobs_groups_delete" ON "arbiter"."golden_jobs";
CREATE TRIGGER "maintain_golden_jobs_groups_delete"
AFTER DELETE ON "arbiter"."golden_jobs"
REFERENCING OLD TABLE AS old_table
FOR EACH STATEMENT EXECUTE FUNCTION "arbiter"."maintain_golden_jobs_groups_delete"();

DROP TRIGGER IF EXISTS "maintain_golden_jobs_groups_update" ON "arbiter"."golden_jobs";
CREATE TRIGGER "maintain_golden_jobs_groups_update"
AFTER UPDATE ON "arbiter"."golden_jobs"
REFERENCING OLD TABLE AS old_table NEW TABLE AS new_table
FOR EACH STATEMENT EXECUTE FUNCTION "arbiter"."maintain_golden_jobs_groups_update"();

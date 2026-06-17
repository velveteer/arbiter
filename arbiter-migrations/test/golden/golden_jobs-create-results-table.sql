CREATE TABLE IF NOT EXISTS "arbiter"."golden_jobs_results" (
  parent_id BIGINT NOT NULL REFERENCES "arbiter"."golden_jobs"(id) ON DELETE CASCADE,
  child_id BIGINT NOT NULL,
  result JSONB NOT NULL,
  PRIMARY KEY (parent_id, child_id)
);

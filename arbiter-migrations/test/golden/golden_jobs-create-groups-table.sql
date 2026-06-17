CREATE TABLE IF NOT EXISTS "arbiter"."golden_jobs_groups" (
  group_key TEXT PRIMARY KEY,
  min_priority INT NOT NULL DEFAULT 0,
  min_id BIGINT NOT NULL DEFAULT 0,
  job_count INT NOT NULL DEFAULT 0,
  in_flight_until TIMESTAMPTZ DEFAULT NULL
);

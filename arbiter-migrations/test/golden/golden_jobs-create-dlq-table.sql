CREATE TABLE IF NOT EXISTS "arbiter"."golden_jobs_dlq" (
  id BIGSERIAL PRIMARY KEY,
  failed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  job_id BIGINT NOT NULL,
  payload JSONB NOT NULL,
  group_key TEXT,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ,
  last_attempted_at TIMESTAMPTZ,
  not_visible_until TIMESTAMPTZ,
  attempts INT NOT NULL DEFAULT 0,
  last_error TEXT,
  priority INT NOT NULL DEFAULT 0,
  dedup_key TEXT,
  dedup_strategy TEXT,
  max_attempts INT,
  parent_id BIGINT,
  parent_state JSONB,
  suspended BOOLEAN NOT NULL DEFAULT FALSE

);

CREATE TABLE IF NOT EXISTS "arbiter".arbiter_gates (
  task_name TEXT PRIMARY KEY,
  last_run_at TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01'::timestamptz
);

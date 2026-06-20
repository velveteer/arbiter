CREATE UNLOGGED TABLE IF NOT EXISTS "arbiter".arbiter_concurrency (
  concurrency_key TEXT PRIMARY KEY,
  concurrency_prefix TEXT NOT NULL,
  in_flight INTEGER NOT NULL DEFAULT 0
) WITH (fillfactor = 80);

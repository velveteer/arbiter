CREATE TABLE IF NOT EXISTS "arbiter".arbiter_concurrency_policies (
  prefix_id TEXT PRIMARY KEY,
  default_limit INTEGER NOT NULL CHECK (default_limit > 0),
  override_limit INTEGER CHECK (override_limit >= 0)
);

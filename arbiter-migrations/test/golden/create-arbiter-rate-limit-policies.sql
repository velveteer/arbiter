CREATE TABLE IF NOT EXISTS "arbiter".arbiter_rate_limit_policies (
  prefix_id TEXT PRIMARY KEY,
  default_max_tokens DOUBLE PRECISION NOT NULL CHECK (default_max_tokens >= 0),
  default_refill_amount DOUBLE PRECISION NOT NULL CHECK (default_refill_amount >= 0),
  default_interval DOUBLE PRECISION NOT NULL CHECK (default_interval > 0),
  override_max_tokens DOUBLE PRECISION CHECK (override_max_tokens >= 0),
  override_refill_amount DOUBLE PRECISION CHECK (override_refill_amount >= 0),
  override_interval DOUBLE PRECISION CHECK (override_interval > 0)
);

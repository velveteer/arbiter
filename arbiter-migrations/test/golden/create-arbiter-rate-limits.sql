CREATE UNLOGGED TABLE IF NOT EXISTS "arbiter".arbiter_rate_limits (
  rate_limit_key TEXT PRIMARY KEY,
  policy_prefix TEXT NOT NULL,
  tokens DOUBLE PRECISION NOT NULL,
  last_refill TIMESTAMPTZ NOT NULL
) WITH (fillfactor = 80);

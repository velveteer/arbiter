CREATE TABLE IF NOT EXISTS "arbiter".arbiter_queues (
  queue_name TEXT PRIMARY KEY,
  paused BOOLEAN NOT NULL DEFAULT FALSE,
  paused_at TIMESTAMPTZ,
  metadata JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

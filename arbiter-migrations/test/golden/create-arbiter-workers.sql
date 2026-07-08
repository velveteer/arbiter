CREATE TABLE IF NOT EXISTS "arbiter".arbiter_workers (
  worker_id UUID PRIMARY KEY,
  queue_name TEXT NOT NULL,
  host_name TEXT,
  worker_count INT,
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_heartbeat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  shutting_down BOOLEAN NOT NULL DEFAULT FALSE,
  paused BOOLEAN NOT NULL DEFAULT FALSE,
  stale_threshold_secs DOUBLE PRECISION NOT NULL DEFAULT 300,
  metadata JSONB
);

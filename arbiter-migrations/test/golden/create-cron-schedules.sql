CREATE TABLE IF NOT EXISTS "arbiter".cron_schedules (
  name TEXT PRIMARY KEY,
  default_expression TEXT NOT NULL,
  default_overlap TEXT NOT NULL CHECK (default_overlap IN ('SkipOverlap', 'AllowOverlap')),
  override_expression TEXT,
  override_overlap TEXT CHECK (override_overlap IS NULL OR override_overlap IN ('SkipOverlap', 'AllowOverlap')),
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  last_fired_at TIMESTAMPTZ,
  last_checked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

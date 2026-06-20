ALTER TABLE "arbiter".cron_schedules ADD COLUMN IF NOT EXISTS default_timezone TEXT;
ALTER TABLE "arbiter".cron_schedules ADD COLUMN IF NOT EXISTS override_timezone TEXT;

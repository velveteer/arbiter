CREATE OR REPLACE FUNCTION "arbiter"."notify_golden_jobs_created"()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify('golden_jobs_created', '');
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION "arbiter"."notify_job_event"() RETURNS trigger AS $$
DECLARE
  event_type text;
  job_id bigint;
  queue_name text := TG_ARGV[0];
  is_dlq boolean := TG_ARGV[1]::boolean;
BEGIN
  CASE TG_OP
    WHEN 'INSERT' THEN
      event_type := CASE WHEN is_dlq THEN 'job_dlq' ELSE 'job_inserted' END;
      job_id := NEW.id;
    WHEN 'UPDATE' THEN
      event_type := 'job_updated';
      job_id := NEW.id;
    WHEN 'DELETE' THEN
      event_type := 'job_deleted';
      job_id := OLD.id;
  END CASE;

  PERFORM pg_notify('arbiter_job_events',
    json_build_object(
      'event', event_type,
      'table', queue_name,
      'job_id', job_id
    )::text);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

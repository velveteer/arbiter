CREATE OR REPLACE FUNCTION "arbiter"."maintain_golden_jobs_groups_insert"()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM new_table WHERE group_key IS NOT NULL LIMIT 1) THEN
    RETURN NULL;
  END IF;

  PERFORM 1 FROM "arbiter"."golden_jobs_groups" g
  WHERE g.group_key IN (SELECT group_key FROM new_table WHERE group_key IS NOT NULL)
  ORDER BY g.group_key FOR UPDATE;

  INSERT INTO "arbiter"."golden_jobs_groups" (group_key, min_priority, min_id, job_count, ready_count, next_due)
  SELECT group_key,
    MIN(priority) AS min_priority,
    (MIN(ARRAY[priority::bigint, id]))[2] AS min_id,
    COUNT(*) AS job_count,
    COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended) AS ready_count,
    MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended) AS next_due
  FROM new_table
  WHERE group_key IS NOT NULL
  GROUP BY group_key
  ORDER BY group_key
  ON CONFLICT (group_key) DO UPDATE SET
    min_priority = CASE WHEN "arbiter"."golden_jobs_groups".job_count = 0 OR (EXCLUDED.min_priority, EXCLUDED.min_id) < ("arbiter"."golden_jobs_groups".min_priority, "arbiter"."golden_jobs_groups".min_id) THEN EXCLUDED.min_priority
      ELSE "arbiter"."golden_jobs_groups".min_priority END,
    min_id = CASE WHEN "arbiter"."golden_jobs_groups".job_count = 0 OR (EXCLUDED.min_priority, EXCLUDED.min_id) < ("arbiter"."golden_jobs_groups".min_priority, "arbiter"."golden_jobs_groups".min_id) THEN EXCLUDED.min_id
      ELSE "arbiter"."golden_jobs_groups".min_id END,
    job_count = "arbiter"."golden_jobs_groups".job_count + EXCLUDED.job_count,
    ready_count = "arbiter"."golden_jobs_groups".ready_count + EXCLUDED.ready_count,
    next_due = LEAST("arbiter"."golden_jobs_groups".next_due, EXCLUDED.next_due),
    in_flight_until = CASE WHEN "arbiter"."golden_jobs_groups".in_flight_until <= NOW()
      THEN NULL ELSE "arbiter"."golden_jobs_groups".in_flight_until END;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION "arbiter"."maintain_golden_jobs_groups_delete"()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM old_table WHERE group_key IS NOT NULL LIMIT 1) THEN
    RETURN NULL;
  END IF;

  PERFORM 1 FROM "arbiter"."golden_jobs_groups" g
  WHERE g.group_key IN (SELECT group_key FROM old_table WHERE group_key IS NOT NULL)
  ORDER BY g.group_key FOR UPDATE;
  WITH t AS (
    SELECT g0.group_key,
      CASE WHEN hp.priority IS NULL THEN 0 ELSE GREATEST(0, g0.job_count + d.count_delta) END AS job_count,
      CASE WHEN hp.priority IS NULL THEN 0 ELSE GREATEST(0, g0.ready_count + d.ready_delta) END AS ready_count,
      COALESCE(hp.priority, 0) AS min_priority,
      COALESCE(hp.id, 0) AS min_id,
      nd.not_visible_until AS next_due,
      fi.not_visible_until AS in_flight_until
    FROM (
      SELECT group_key, (-COUNT(*))::int AS count_delta,
        (-COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended))::int AS ready_delta
      FROM old_table WHERE group_key IS NOT NULL GROUP BY group_key
    ) d
    JOIN "arbiter"."golden_jobs_groups" g0 ON g0.group_key = d.group_key
    LEFT JOIN LATERAL (
      SELECT q.priority, q.id FROM "arbiter"."golden_jobs" q
      WHERE q.group_key = d.group_key
      ORDER BY q.priority ASC, q.id ASC LIMIT 1
    ) hp ON TRUE
    LEFT JOIN LATERAL (
      SELECT q.not_visible_until FROM "arbiter"."golden_jobs" q
      WHERE q.group_key = d.group_key AND q.not_visible_until IS NOT NULL AND NOT q.suspended
      ORDER BY q.not_visible_until ASC LIMIT 1
    ) nd ON TRUE
    LEFT JOIN LATERAL (
      SELECT q.not_visible_until FROM "arbiter"."golden_jobs" q
      WHERE q.group_key = d.group_key AND q.not_visible_until > NOW() AND NOT q.suspended AND (q.attempts > 0 OR q.throttled_until > NOW())
      ORDER BY q.not_visible_until DESC NULLS LAST LIMIT 1
    ) fi ON TRUE
  )
  UPDATE "arbiter"."golden_jobs_groups" g
  SET job_count = t.job_count,
    ready_count = t.ready_count,
    min_priority = t.min_priority,
    min_id = t.min_id,
    next_due = t.next_due,
    in_flight_until = t.in_flight_until
  FROM t
  WHERE g.group_key = t.group_key
    AND (g.job_count, g.ready_count, g.min_priority, g.min_id, g.next_due, g.in_flight_until)
        IS DISTINCT FROM (t.job_count, t.ready_count, t.min_priority, t.min_id, t.next_due, t.in_flight_until);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION "arbiter"."maintain_golden_jobs_groups_update"()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM new_table WHERE group_key IS NOT NULL LIMIT 1)
     AND NOT EXISTS (SELECT 1 FROM old_table WHERE group_key IS NOT NULL LIMIT 1) THEN
    RETURN NULL;
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE (n.group_key IS NOT NULL OR o.group_key IS NOT NULL)
      AND (n.group_key IS DISTINCT FROM o.group_key OR n.priority IS DISTINCT FROM o.priority OR n.not_visible_until IS DISTINCT FROM o.not_visible_until OR n.suspended IS DISTINCT FROM o.suspended OR n.attempts IS DISTINCT FROM o.attempts OR n.throttled_until IS DISTINCT FROM o.throttled_until)
    LIMIT 1
  ) THEN
    RETURN NULL;
  END IF;

  PERFORM 1 FROM "arbiter"."golden_jobs_groups" g
  WHERE g.group_key IN (SELECT group_key
  FROM new_table
  WHERE group_key IS NOT NULL
  UNION
  SELECT group_key
  FROM old_table
  WHERE group_key IS NOT NULL)
  ORDER BY g.group_key FOR UPDATE;
  IF EXISTS (
    SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE o.group_key IS DISTINCT FROM n.group_key LIMIT 1
  ) THEN
    WITH t AS (
      SELECT g0.group_key,
        CASE WHEN hp.priority IS NULL THEN 0 ELSE GREATEST(0, g0.job_count + d.count_delta) END AS job_count,
        CASE WHEN hp.priority IS NULL THEN 0 ELSE GREATEST(0, g0.ready_count + d.ready_delta) END AS ready_count,
        COALESCE(hp.priority, 0) AS min_priority,
        COALESCE(hp.id, 0) AS min_id,
        nd.not_visible_until AS next_due,
        fi.not_visible_until AS in_flight_until
      FROM (
        SELECT o.group_key, (-COUNT(*))::int AS count_delta,
          (-COUNT(*) FILTER (WHERE o.not_visible_until IS NULL AND NOT o.suspended))::int AS ready_delta
        FROM old_table o JOIN new_table n ON n.id = o.id
        WHERE o.group_key IS NOT NULL AND o.group_key IS DISTINCT FROM n.group_key
        GROUP BY o.group_key
      ) d
      JOIN "arbiter"."golden_jobs_groups" g0 ON g0.group_key = d.group_key
      LEFT JOIN LATERAL (
        SELECT q.priority, q.id FROM "arbiter"."golden_jobs" q
        WHERE q.group_key = d.group_key
        ORDER BY q.priority ASC, q.id ASC LIMIT 1
      ) hp ON TRUE
      LEFT JOIN LATERAL (
        SELECT q.not_visible_until FROM "arbiter"."golden_jobs" q
        WHERE q.group_key = d.group_key AND q.not_visible_until IS NOT NULL AND NOT q.suspended
        ORDER BY q.not_visible_until ASC LIMIT 1
      ) nd ON TRUE
      LEFT JOIN LATERAL (
        SELECT q.not_visible_until FROM "arbiter"."golden_jobs" q
        WHERE q.group_key = d.group_key AND q.not_visible_until > NOW() AND NOT q.suspended AND (q.attempts > 0 OR q.throttled_until > NOW())
        ORDER BY q.not_visible_until DESC NULLS LAST LIMIT 1
      ) fi ON TRUE
    )
    UPDATE "arbiter"."golden_jobs_groups" g
    SET job_count = t.job_count,
      ready_count = t.ready_count,
      min_priority = t.min_priority,
      min_id = t.min_id,
      next_due = t.next_due,
      in_flight_until = t.in_flight_until
    FROM t
    WHERE g.group_key = t.group_key
      AND (g.job_count, g.ready_count, g.min_priority, g.min_id, g.next_due, g.in_flight_until)
          IS DISTINCT FROM (t.job_count, t.ready_count, t.min_priority, t.min_id, t.next_due, t.in_flight_until);

    INSERT INTO "arbiter"."golden_jobs_groups" (group_key, min_priority, min_id, job_count, ready_count, next_due)
    SELECT n.group_key,
      MIN(n.priority) AS min_priority,
      (MIN(ARRAY[n.priority::bigint, n.id]))[2] AS min_id,
      COUNT(*) AS job_count,
      COUNT(*) FILTER (WHERE n.not_visible_until IS NULL AND NOT n.suspended) AS ready_count,
      MIN(n.not_visible_until) FILTER (WHERE n.not_visible_until IS NOT NULL AND NOT n.suspended) AS next_due
    FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE n.group_key IS NOT NULL AND o.group_key IS DISTINCT FROM n.group_key
    GROUP BY n.group_key ORDER BY n.group_key
    ON CONFLICT (group_key) DO UPDATE SET min_priority = CASE WHEN "arbiter"."golden_jobs_groups".job_count = 0 OR (EXCLUDED.min_priority, EXCLUDED.min_id) < ("arbiter"."golden_jobs_groups".min_priority, "arbiter"."golden_jobs_groups".min_id) THEN EXCLUDED.min_priority
      ELSE "arbiter"."golden_jobs_groups".min_priority END,
    min_id = CASE WHEN "arbiter"."golden_jobs_groups".job_count = 0 OR (EXCLUDED.min_priority, EXCLUDED.min_id) < ("arbiter"."golden_jobs_groups".min_priority, "arbiter"."golden_jobs_groups".min_id) THEN EXCLUDED.min_id
      ELSE "arbiter"."golden_jobs_groups".min_id END,
    job_count = "arbiter"."golden_jobs_groups".job_count + EXCLUDED.job_count,
    ready_count = "arbiter"."golden_jobs_groups".ready_count + EXCLUDED.ready_count,
    next_due = LEAST("arbiter"."golden_jobs_groups".next_due, EXCLUDED.next_due);

    WITH t AS (
      SELECT g0.group_key,
        CASE WHEN hp.priority IS NULL THEN 0 ELSE GREATEST(0, g0.job_count + d.count_delta) END AS job_count,
        CASE WHEN hp.priority IS NULL THEN 0 ELSE GREATEST(0, g0.ready_count + d.ready_delta) END AS ready_count,
        COALESCE(hp.priority, 0) AS min_priority,
        COALESCE(hp.id, 0) AS min_id,
        nd.not_visible_until AS next_due,
        fi.not_visible_until AS in_flight_until
      FROM (
        SELECT n.group_key, 0 AS count_delta, 0 AS ready_delta
        FROM new_table n JOIN old_table o ON o.id = n.id
        WHERE n.group_key IS NOT NULL AND o.group_key IS DISTINCT FROM n.group_key
        GROUP BY n.group_key
      ) d
      JOIN "arbiter"."golden_jobs_groups" g0 ON g0.group_key = d.group_key
      LEFT JOIN LATERAL (
        SELECT q.priority, q.id FROM "arbiter"."golden_jobs" q
        WHERE q.group_key = d.group_key
        ORDER BY q.priority ASC, q.id ASC LIMIT 1
      ) hp ON TRUE
      LEFT JOIN LATERAL (
        SELECT q.not_visible_until FROM "arbiter"."golden_jobs" q
        WHERE q.group_key = d.group_key AND q.not_visible_until IS NOT NULL AND NOT q.suspended
        ORDER BY q.not_visible_until ASC LIMIT 1
      ) nd ON TRUE
      LEFT JOIN LATERAL (
        SELECT q.not_visible_until FROM "arbiter"."golden_jobs" q
        WHERE q.group_key = d.group_key AND q.not_visible_until > NOW() AND NOT q.suspended AND (q.attempts > 0 OR q.throttled_until > NOW())
        ORDER BY q.not_visible_until DESC NULLS LAST LIMIT 1
      ) fi ON TRUE
    )
    UPDATE "arbiter"."golden_jobs_groups" g
    SET job_count = t.job_count,
      ready_count = t.ready_count,
      min_priority = t.min_priority,
      min_id = t.min_id,
      next_due = t.next_due,
      in_flight_until = t.in_flight_until
    FROM t
    WHERE g.group_key = t.group_key
      AND (g.job_count, g.ready_count, g.min_priority, g.min_id, g.next_due, g.in_flight_until)
          IS DISTINCT FROM (t.job_count, t.ready_count, t.min_priority, t.min_id, t.next_due, t.in_flight_until);
  END IF;
  WITH t AS (
    SELECT g0.group_key,
      CASE WHEN hp.priority IS NULL THEN 0 ELSE GREATEST(0, g0.job_count + d.count_delta) END AS job_count,
      CASE WHEN hp.priority IS NULL THEN 0 ELSE GREATEST(0, g0.ready_count + d.ready_delta) END AS ready_count,
      COALESCE(hp.priority, 0) AS min_priority,
      COALESCE(hp.id, 0) AS min_id,
      nd.not_visible_until AS next_due,
      fi.not_visible_until AS in_flight_until
    FROM (
      SELECT n.group_key, 0 AS count_delta,
        SUM((n.not_visible_until IS NULL AND NOT n.suspended)::int - (o.not_visible_until IS NULL AND NOT o.suspended)::int)::int AS ready_delta
      FROM new_table n JOIN old_table o ON o.id = n.id
      WHERE n.group_key IS NOT NULL
        AND n.group_key IS NOT DISTINCT FROM o.group_key
        AND (n.priority IS DISTINCT FROM o.priority OR n.not_visible_until IS DISTINCT FROM o.not_visible_until OR n.suspended IS DISTINCT FROM o.suspended OR n.attempts IS DISTINCT FROM o.attempts OR n.throttled_until IS DISTINCT FROM o.throttled_until)
      GROUP BY n.group_key
    ) d
    JOIN "arbiter"."golden_jobs_groups" g0 ON g0.group_key = d.group_key
    LEFT JOIN LATERAL (
      SELECT q.priority, q.id FROM "arbiter"."golden_jobs" q
      WHERE q.group_key = d.group_key
      ORDER BY q.priority ASC, q.id ASC LIMIT 1
    ) hp ON TRUE
    LEFT JOIN LATERAL (
      SELECT q.not_visible_until FROM "arbiter"."golden_jobs" q
      WHERE q.group_key = d.group_key AND q.not_visible_until IS NOT NULL AND NOT q.suspended
      ORDER BY q.not_visible_until ASC LIMIT 1
    ) nd ON TRUE
    LEFT JOIN LATERAL (
      SELECT q.not_visible_until FROM "arbiter"."golden_jobs" q
      WHERE q.group_key = d.group_key AND q.not_visible_until > NOW() AND NOT q.suspended AND (q.attempts > 0 OR q.throttled_until > NOW())
      ORDER BY q.not_visible_until DESC NULLS LAST LIMIT 1
    ) fi ON TRUE
  )
  UPDATE "arbiter"."golden_jobs_groups" g
  SET job_count = t.job_count,
    ready_count = t.ready_count,
    min_priority = t.min_priority,
    min_id = t.min_id,
    next_due = t.next_due,
    in_flight_until = t.in_flight_until
  FROM t
  WHERE g.group_key = t.group_key
    AND (g.job_count, g.ready_count, g.min_priority, g.min_id, g.next_due, g.in_flight_until)
        IS DISTINCT FROM (t.job_count, t.ready_count, t.min_priority, t.min_id, t.next_due, t.in_flight_until);
  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

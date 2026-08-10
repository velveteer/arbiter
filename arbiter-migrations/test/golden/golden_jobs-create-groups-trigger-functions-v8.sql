CREATE OR REPLACE FUNCTION "arbiter"."maintain_golden_jobs_groups_insert"()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM new_table WHERE group_key IS NOT NULL LIMIT 1) THEN
    RETURN NULL;
  END IF;

  -- Lock group rows in group_key order to avoid deadlock with concurrent triggers.
  PERFORM 1 FROM "arbiter"."golden_jobs_groups" g
  WHERE g.group_key IN (SELECT group_key FROM new_table WHERE group_key IS NOT NULL)
  ORDER BY g.group_key
  FOR UPDATE;

  INSERT INTO "arbiter"."golden_jobs_groups" (group_key, min_priority, min_id, job_count, ready_count, next_due)
  SELECT group_key, MIN(priority) AS min_priority, MIN(id) AS min_id, COUNT(*) AS job_count, COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended) AS ready_count, MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended) AS next_due
  FROM new_table
  WHERE group_key IS NOT NULL
  GROUP BY group_key
  ORDER BY group_key
  ON CONFLICT (group_key) DO UPDATE SET
    min_priority = CASE WHEN "arbiter"."golden_jobs_groups".job_count = 0 THEN EXCLUDED.min_priority
      ELSE LEAST("arbiter"."golden_jobs_groups".min_priority, EXCLUDED.min_priority) END,
    min_id = CASE WHEN "arbiter"."golden_jobs_groups".job_count = 0 THEN EXCLUDED.min_id
      ELSE LEAST("arbiter"."golden_jobs_groups".min_id, EXCLUDED.min_id) END,
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

  -- Lock group rows in group_key order to avoid deadlock with concurrent triggers.
  PERFORM 1 FROM "arbiter"."golden_jobs_groups" g
  WHERE g.group_key IN (SELECT group_key FROM old_table WHERE group_key IS NOT NULL)
  ORDER BY g.group_key
  FOR UPDATE;

  UPDATE "arbiter"."golden_jobs_groups" g
  SET job_count = CASE WHEN sub.new_min_id IS NULL THEN 0
        ELSE GREATEST(0, g.job_count - sub.removed_count) END,
      min_priority = CASE WHEN sub.new_min_id IS NULL THEN 0
        ELSE sub.new_min_priority END,
      min_id = CASE WHEN sub.new_min_id IS NULL THEN 0
        ELSE sub.new_min_id END,
      ready_count = CASE WHEN sub.new_min_id IS NULL THEN 0
        ELSE GREATEST(0, g.ready_count - sub.removed_ready_count) END,
      next_due = CASE WHEN sub.new_min_id IS NULL THEN NULL
        ELSE sub.new_next_due END,
      in_flight_until = CASE
        WHEN sub.new_min_id IS NULL THEN NULL
        WHEN sub.had_inflight THEN sub.surviving_ift
        ELSE g.in_flight_until
      END
  FROM (
    SELECT d.group_key, d.removed_count, d.removed_ready_count, d.had_inflight,
      MIN(t.priority) AS new_min_priority,
      MIN(t.id) AS new_min_id,
      MIN(t.not_visible_until) FILTER (WHERE t.not_visible_until IS NOT NULL AND NOT t.suspended) AS new_next_due,
      MAX(t.not_visible_until) FILTER (WHERE t.not_visible_until > NOW() AND NOT t.suspended AND (t.attempts > 0 OR t.throttled_until > NOW())) AS surviving_ift
    FROM (
      SELECT group_key, COUNT(*) AS removed_count,
        COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended) AS removed_ready_count,
        bool_or(not_visible_until > NOW() AND NOT suspended AND (attempts > 0 OR throttled_until > NOW())) AS had_inflight
      FROM old_table
      WHERE group_key IS NOT NULL
      GROUP BY group_key
    ) d
    LEFT JOIN "arbiter"."golden_jobs" t ON t.group_key = d.group_key
    GROUP BY d.group_key, d.removed_count, d.removed_ready_count, d.had_inflight
  ) sub
  WHERE g.group_key = sub.group_key;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION "arbiter"."maintain_golden_jobs_groups_update"()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM new_table WHERE group_key IS NOT NULL LIMIT 1
  ) AND NOT EXISTS (
    SELECT 1 FROM old_table WHERE group_key IS NOT NULL LIMIT 1
  ) THEN
    RETURN NULL;
  END IF;

  -- Lock group rows (old and new) in group_key order to avoid deadlock with concurrent triggers.
  PERFORM 1 FROM "arbiter"."golden_jobs_groups" g
  WHERE g.group_key IN (
    SELECT group_key FROM new_table WHERE group_key IS NOT NULL
    UNION
    SELECT group_key FROM old_table WHERE group_key IS NOT NULL
  )
  ORDER BY g.group_key
  FOR UPDATE;

  -- Step 1: Full rescan - recompute in_flight_until when not_visible_until decreases or suspended changes
  UPDATE "arbiter"."golden_jobs_groups" g
  SET in_flight_until = sub.new_ift
  FROM (
    SELECT t.group_key,
      MAX(t.not_visible_until) FILTER (
        WHERE t.not_visible_until > NOW() AND NOT t.suspended AND (t.attempts > 0 OR t.throttled_until > NOW())
      ) AS new_ift
    FROM "arbiter"."golden_jobs" t
    WHERE t.group_key IN (
      SELECT n.group_key FROM new_table n
      JOIN old_table o ON o.id = n.id
      WHERE n.group_key IS NOT NULL
        AND (o.not_visible_until IS DISTINCT FROM n.not_visible_until
             OR o.suspended IS DISTINCT FROM n.suspended
             OR o.attempts IS DISTINCT FROM n.attempts)
        AND (
          n.not_visible_until > NOW() AND NOT n.suspended AND n.attempts > 0
          AND (o.not_visible_until IS NULL OR o.not_visible_until <= NOW()
               OR n.not_visible_until > o.not_visible_until)
        ) IS NOT TRUE
    )
    GROUP BY t.group_key
  ) sub
  WHERE g.group_key = sub.group_key
    AND g.in_flight_until IS DISTINCT FROM sub.new_ift;

  IF EXISTS (
    SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE o.group_key IS DISTINCT FROM n.group_key
    LIMIT 1
  ) THEN
    -- Step 2: group_key change (dedup replace) - remove from old group
    UPDATE "arbiter"."golden_jobs_groups" g
    SET job_count = CASE WHEN sub.new_min_id IS NULL THEN 0
          ELSE GREATEST(0, g.job_count - sub.removed_count) END,
        min_priority = CASE WHEN sub.new_min_id IS NULL THEN 0
          ELSE sub.new_min_priority END,
        min_id = CASE WHEN sub.new_min_id IS NULL THEN 0
          ELSE sub.new_min_id END,
        ready_count = CASE WHEN sub.new_min_id IS NULL THEN 0
          ELSE GREATEST(0, g.ready_count - sub.removed_ready_count) END,
        next_due = CASE WHEN sub.new_min_id IS NULL THEN NULL
          ELSE sub.new_next_due END,
        in_flight_until = CASE
          WHEN sub.new_min_id IS NULL THEN NULL
          WHEN sub.had_inflight THEN sub.surviving_ift
          ELSE g.in_flight_until
        END
    FROM (
      SELECT d.group_key, d.removed_count, d.removed_ready_count, d.had_inflight,
        MIN(t.priority) AS new_min_priority,
        MIN(t.id) AS new_min_id,
        MIN(t.not_visible_until) FILTER (WHERE t.not_visible_until IS NOT NULL AND NOT t.suspended) AS new_next_due,
        MAX(t.not_visible_until) FILTER (WHERE t.not_visible_until > NOW() AND NOT t.suspended AND (t.attempts > 0 OR t.throttled_until > NOW())) AS surviving_ift
      FROM (
        SELECT o.group_key, COUNT(*) AS removed_count,
          COUNT(*) FILTER (WHERE o.not_visible_until IS NULL AND NOT o.suspended) AS removed_ready_count,
          bool_or(o.not_visible_until > NOW() AND NOT o.suspended AND (o.attempts > 0 OR o.throttled_until > NOW())) AS had_inflight
        FROM old_table o
        JOIN new_table n ON o.id = n.id
        WHERE o.group_key IS NOT NULL
          AND o.group_key IS DISTINCT FROM n.group_key
        GROUP BY o.group_key
      ) d
      LEFT JOIN "arbiter"."golden_jobs" t ON t.group_key = d.group_key
      GROUP BY d.group_key, d.removed_count, d.removed_ready_count, d.had_inflight
    ) sub
    WHERE g.group_key = sub.group_key;

    -- Step 3: group_key change - add to new group
    INSERT INTO "arbiter"."golden_jobs_groups" (group_key, min_priority, min_id, job_count, ready_count, next_due)
    SELECT n.group_key, MIN(n.priority) AS min_priority, MIN(n.id) AS min_id, COUNT(*) AS job_count, COUNT(*) FILTER (WHERE n.not_visible_until IS NULL AND NOT n.suspended) AS ready_count, MIN(n.not_visible_until) FILTER (WHERE n.not_visible_until IS NOT NULL AND NOT n.suspended) AS next_due
    FROM new_table n
    JOIN old_table o ON o.id = n.id
    WHERE n.group_key IS NOT NULL
      AND o.group_key IS DISTINCT FROM n.group_key
    GROUP BY n.group_key
    ORDER BY n.group_key
    ON CONFLICT (group_key) DO UPDATE SET
      min_priority = CASE WHEN "arbiter"."golden_jobs_groups".job_count = 0 THEN EXCLUDED.min_priority
        ELSE LEAST("arbiter"."golden_jobs_groups".min_priority, EXCLUDED.min_priority) END,
      min_id = CASE WHEN "arbiter"."golden_jobs_groups".job_count = 0 THEN EXCLUDED.min_id
        ELSE LEAST("arbiter"."golden_jobs_groups".min_id, EXCLUDED.min_id) END,
      job_count = "arbiter"."golden_jobs_groups".job_count + EXCLUDED.job_count,
      ready_count = "arbiter"."golden_jobs_groups".ready_count + EXCLUDED.ready_count,
      next_due = LEAST("arbiter"."golden_jobs_groups".next_due, EXCLUDED.next_due);
  END IF;

  -- Step 4: same-group ordering and visibility recompute, in-flight extend and ready delta in one write.
  UPDATE "arbiter"."golden_jobs_groups" g
  SET min_priority = CASE WHEN m.recompute THEN m.new_min_priority ELSE g.min_priority END,
      min_id = CASE WHEN m.recompute THEN m.new_min_id ELSE g.min_id END,
      next_due = CASE WHEN m.recompute THEN m.new_next_due ELSE g.next_due END,
      in_flight_until = GREATEST(g.in_flight_until, m.new_ift),
      ready_count = GREATEST(0, g.ready_count + COALESCE(m.delta, 0))
  FROM (
    SELECT COALESCE(sub.group_key, s.group_key) AS group_key,
      sub.group_key IS NOT NULL AS recompute,
      sub.new_min_priority, sub.new_min_id, sub.new_next_due,
      s.new_ift, s.delta
    FROM (
      SELECT d.group_key,
        MIN(t.priority) AS new_min_priority,
        MIN(t.id) AS new_min_id,
        MIN(t.not_visible_until) FILTER (WHERE t.not_visible_until IS NOT NULL AND NOT t.suspended) AS new_next_due
      FROM (
        SELECT DISTINCT n.group_key
        FROM new_table n
        JOIN old_table o ON o.id = n.id
        WHERE n.group_key IS NOT NULL
          AND n.group_key IS NOT DISTINCT FROM o.group_key
          AND (n.priority IS DISTINCT FROM o.priority
               OR o.not_visible_until IS DISTINCT FROM n.not_visible_until
               OR o.suspended IS DISTINCT FROM n.suspended)
      ) d
      LEFT JOIN "arbiter"."golden_jobs" t ON t.group_key = d.group_key
      GROUP BY d.group_key
    ) sub
    FULL OUTER JOIN (
      SELECT COALESCE(ift.group_key, rc.group_key) AS group_key, ift.new_ift, rc.delta
      FROM (
        SELECT n.group_key, MAX(n.not_visible_until) AS new_ift
        FROM new_table n
        JOIN old_table o ON o.id = n.id
        WHERE n.group_key IS NOT NULL
          AND n.not_visible_until > NOW()
          AND NOT n.suspended
          AND n.attempts > 0
          AND (o.not_visible_until IS NULL OR o.not_visible_until <= NOW()
               OR n.not_visible_until > o.not_visible_until)
        GROUP BY n.group_key
      ) ift
      FULL OUTER JOIN (
        SELECT group_key, delta FROM (
          SELECT n.group_key,
            SUM(
              (CASE WHEN n.not_visible_until IS NULL AND NOT n.suspended THEN 1 ELSE 0 END)
              - (CASE WHEN o.not_visible_until IS NULL AND NOT o.suspended THEN 1 ELSE 0 END)
            )::int AS delta
          FROM new_table n
          JOIN old_table o ON o.id = n.id
          WHERE n.group_key IS NOT NULL
            AND n.group_key IS NOT DISTINCT FROM o.group_key
          GROUP BY n.group_key
        ) z
        WHERE delta <> 0
      ) rc ON ift.group_key = rc.group_key
    ) s ON s.group_key = sub.group_key
  ) m
  WHERE g.group_key = m.group_key
    AND ((m.recompute
          AND (g.min_priority IS DISTINCT FROM m.new_min_priority
               OR g.min_id IS DISTINCT FROM m.new_min_id
               OR g.next_due IS DISTINCT FROM m.new_next_due))
         OR g.in_flight_until IS DISTINCT FROM GREATEST(g.in_flight_until, m.new_ift)
         OR COALESCE(m.delta, 0) <> 0);

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

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
  SELECT group_key,
    MIN(priority),
    MIN(id),
    COUNT(*),
    COUNT(*) FILTER (WHERE not_visible_until IS NULL AND NOT suspended),
    MIN(not_visible_until) FILTER (WHERE not_visible_until IS NOT NULL AND NOT suspended)
  FROM new_table
  WHERE group_key IS NOT NULL
  GROUP BY group_key
  ORDER BY group_key
  ON CONFLICT (group_key) DO UPDATE SET
    min_priority = LEAST("arbiter"."golden_jobs_groups".min_priority, EXCLUDED.min_priority),
    min_id = LEAST("arbiter"."golden_jobs_groups".min_id, EXCLUDED.min_id),
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
  SET job_count = g.job_count - sub.removed_count,
      min_priority = COALESCE(sub.new_min_priority, g.min_priority),
      min_id = COALESCE(sub.new_min_id, g.min_id),
      ready_count = GREATEST(0, g.ready_count - sub.removed_ready_count),
      next_due = sub.new_next_due,
      in_flight_until = CASE
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

  DELETE FROM "arbiter"."golden_jobs_groups"
  WHERE job_count <= 0
    AND group_key IN (SELECT group_key FROM old_table WHERE group_key IS NOT NULL);

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

  -- Step 2: group_key change (dedup replace) - remove from old group
  UPDATE "arbiter"."golden_jobs_groups" g
  SET job_count = g.job_count - sub.cnt,
      min_priority = COALESCE(sub.new_min_priority, g.min_priority),
      min_id = COALESCE(sub.new_min_id, g.min_id),
      ready_count = GREATEST(0, g.ready_count - sub.removed_ready_count),
      next_due = sub.new_next_due,
      in_flight_until = CASE
        WHEN sub.had_inflight THEN sub.surviving_ift
        ELSE g.in_flight_until
      END
  FROM (
    SELECT d.group_key, d.cnt, d.removed_ready_count, d.had_inflight,
      MIN(t.priority) AS new_min_priority, MIN(t.id) AS new_min_id,
      MIN(t.not_visible_until) FILTER (WHERE t.not_visible_until IS NOT NULL AND NOT t.suspended) AS new_next_due,
      MAX(t.not_visible_until) FILTER (WHERE t.not_visible_until > NOW() AND NOT t.suspended AND (t.attempts > 0 OR t.throttled_until > NOW())) AS surviving_ift
    FROM (
      SELECT o.group_key, COUNT(*) AS cnt,
        COUNT(*) FILTER (WHERE o.not_visible_until IS NULL AND NOT o.suspended) AS removed_ready_count,
        bool_or(o.not_visible_until > NOW() AND NOT o.suspended AND (o.attempts > 0 OR o.throttled_until > NOW())) AS had_inflight
      FROM old_table o
      JOIN new_table n ON o.id = n.id
      WHERE o.group_key IS NOT NULL
        AND o.group_key IS DISTINCT FROM n.group_key
      GROUP BY o.group_key
    ) d
    LEFT JOIN "arbiter"."golden_jobs" t ON t.group_key = d.group_key
    GROUP BY d.group_key, d.cnt, d.removed_ready_count, d.had_inflight
  ) sub
  WHERE g.group_key = sub.group_key;

  DELETE FROM "arbiter"."golden_jobs_groups"
  WHERE job_count <= 0
    AND group_key IN (
      SELECT o.group_key FROM old_table o
      JOIN new_table n ON o.id = n.id
      WHERE o.group_key IS NOT NULL
        AND o.group_key IS DISTINCT FROM n.group_key
    );

  -- Step 3: group_key change - add to new group
  INSERT INTO "arbiter"."golden_jobs_groups" (group_key, min_priority, min_id, job_count, ready_count, next_due)
  SELECT n.group_key, MIN(n.priority), MIN(n.id), COUNT(*),
    COUNT(*) FILTER (WHERE n.not_visible_until IS NULL AND NOT n.suspended),
    MIN(n.not_visible_until) FILTER (WHERE n.not_visible_until IS NOT NULL AND NOT n.suspended)
  FROM new_table n
  JOIN old_table o ON o.id = n.id
  WHERE n.group_key IS NOT NULL
    AND o.group_key IS DISTINCT FROM n.group_key
  GROUP BY n.group_key
  ORDER BY n.group_key
  ON CONFLICT (group_key) DO UPDATE SET
    min_priority = LEAST("arbiter"."golden_jobs_groups".min_priority, EXCLUDED.min_priority),
    min_id = LEAST("arbiter"."golden_jobs_groups".min_id, EXCLUDED.min_id),
    job_count = "arbiter"."golden_jobs_groups".job_count + EXCLUDED.job_count,
    ready_count = "arbiter"."golden_jobs_groups".ready_count + EXCLUDED.ready_count,
    next_due = LEAST("arbiter"."golden_jobs_groups".next_due, EXCLUDED.next_due);

  -- Step 4: same-group ordering/visibility change - recompute min and next_due.
  UPDATE "arbiter"."golden_jobs_groups" g
  SET min_priority = sub.new_min_priority,
      min_id = sub.new_min_id,
      next_due = sub.new_next_due
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
  WHERE g.group_key = sub.group_key
    AND (g.min_priority IS DISTINCT FROM sub.new_min_priority
         OR g.min_id IS DISTINCT FROM sub.new_min_id
         OR g.next_due IS DISTINCT FROM sub.new_next_due);

  -- Step 5: commutative in_flight_until extend and ready_count delta in one write.
  UPDATE "arbiter"."golden_jobs_groups" g
  SET in_flight_until = GREATEST(g.in_flight_until, s.new_ift),
      ready_count = GREATEST(0, g.ready_count + COALESCE(s.delta, 0))
  FROM (
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
  ) s
  WHERE g.group_key = s.group_key;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

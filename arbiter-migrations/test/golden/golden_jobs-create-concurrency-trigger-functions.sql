CREATE OR REPLACE FUNCTION "arbiter"."maintain_golden_jobs_concurrency_insert"()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM new_table WHERE concurrency_key IS NOT NULL LIMIT 1) THEN
    RETURN NULL;
  END IF;

  PERFORM pg_advisory_xact_lock_shared(hashtextextended('arbiter_conc:' || t.k, 0))
  FROM (SELECT DISTINCT concurrency_key AS k FROM new_table WHERE concurrency_key IS NOT NULL) t;

  INSERT INTO "arbiter".arbiter_concurrency (concurrency_key, concurrency_prefix)
  SELECT n.concurrency_key, MAX(n.concurrency_prefix)
  FROM new_table n
  WHERE n.concurrency_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM "arbiter".arbiter_concurrency c WHERE c.concurrency_key = n.concurrency_key)
  GROUP BY n.concurrency_key
  ORDER BY n.concurrency_key
  ON CONFLICT (concurrency_key) DO NOTHING;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION "arbiter"."maintain_golden_jobs_concurrency_delete"()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM old_table WHERE concurrency_key IS NOT NULL AND claimed_by IS NOT NULL LIMIT 1) THEN
    RETURN NULL;
  END IF;

  -- Lock affected count rows in key order to avoid deadlock with concurrent triggers.
  -- Only claimed rows shift in_flight, so unclaimed deletions touch no count row.
  PERFORM 1 FROM "arbiter".arbiter_concurrency a
  WHERE a.concurrency_key IN (SELECT concurrency_key FROM old_table WHERE concurrency_key IS NOT NULL AND claimed_by IS NOT NULL)
  ORDER BY a.concurrency_key
  FOR UPDATE;

  WITH deltas AS (
    SELECT concurrency_key AS key,
           COUNT(*) AS inflight_delta
    FROM old_table
    WHERE concurrency_key IS NOT NULL AND claimed_by IS NOT NULL
    GROUP BY concurrency_key
  )
  UPDATE "arbiter".arbiter_concurrency a
  SET in_flight = GREATEST(0, a.in_flight - d.inflight_delta)
  FROM deltas d
  WHERE a.concurrency_key = d.key;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION "arbiter"."maintain_golden_jobs_concurrency_update"()
RETURNS TRIGGER AS $$
BEGIN
  -- Only a claimed_by flip (shifts in_flight) or a concurrency_key move (a dedup
  -- replace, shifts in_flight between keys) touches in_flight. A heartbeat or other
  -- update leaves it unchanged, so skip it before locking.
  IF NOT EXISTS (
    SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE (n.concurrency_key IS NOT NULL OR o.concurrency_key IS NOT NULL)
      AND (n.claimed_by IS DISTINCT FROM o.claimed_by
           OR n.concurrency_key IS DISTINCT FROM o.concurrency_key)
    LIMIT 1
  ) THEN
    RETURN NULL;
  END IF;

  -- Lock old and new keys' count rows in key order to avoid deadlock, but only
  -- for rows that shift in_flight. An updated row with no claimed_by flip or key
  -- move (a claim's throttle deferral) may reference a key another claimer holds,
  -- and blocking on it here would invert the claim's lock order.
  PERFORM 1 FROM "arbiter".arbiter_concurrency a
  WHERE a.concurrency_key IN (
    SELECT o.concurrency_key FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE o.concurrency_key IS NOT NULL
      AND (n.claimed_by IS DISTINCT FROM o.claimed_by
           OR n.concurrency_key IS DISTINCT FROM o.concurrency_key)
    UNION
    SELECT n.concurrency_key FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE n.concurrency_key IS NOT NULL
      AND (n.claimed_by IS DISTINCT FROM o.claimed_by
           OR n.concurrency_key IS DISTINCT FROM o.concurrency_key)
  )
  ORDER BY a.concurrency_key
  FOR UPDATE;

  -- Same key: only in_flight shifts by the claimed_by delta.
  WITH deltas AS (
    SELECT n.concurrency_key AS key,
           SUM((n.claimed_by IS NOT NULL)::int - (o.claimed_by IS NOT NULL)::int) AS inflight_delta
    FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE n.concurrency_key IS NOT NULL
      AND n.concurrency_key IS NOT DISTINCT FROM o.concurrency_key
    GROUP BY n.concurrency_key
  )
  UPDATE "arbiter".arbiter_concurrency a
  SET in_flight = GREATEST(0, a.in_flight + d.inflight_delta)
  FROM deltas d
  WHERE a.concurrency_key = d.key AND d.inflight_delta <> 0;

  -- Skip both key-move branches when no key changed.
  IF EXISTS (
    SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE n.concurrency_key IS DISTINCT FROM o.concurrency_key
      AND (n.concurrency_key IS NOT NULL OR o.concurrency_key IS NOT NULL)
    LIMIT 1
  ) THEN
    -- Key move: remove the row's in_flight from the old key.
    WITH deltas AS (
      SELECT o.concurrency_key AS key,
             SUM((o.claimed_by IS NOT NULL)::int) AS inflight_delta
      FROM old_table o JOIN new_table n ON o.id = n.id
      WHERE o.concurrency_key IS NOT NULL
        AND o.concurrency_key IS DISTINCT FROM n.concurrency_key
      GROUP BY o.concurrency_key
    )
    UPDATE "arbiter".arbiter_concurrency a
    SET in_flight = GREATEST(0, a.in_flight - d.inflight_delta)
    FROM deltas d
    WHERE a.concurrency_key = d.key;

    -- Key move: add the row's in_flight to the new key, creating its row if absent.
    INSERT INTO "arbiter".arbiter_concurrency (concurrency_key, concurrency_prefix, in_flight)
    SELECT n.concurrency_key,
           MAX(n.concurrency_prefix),
           SUM((n.claimed_by IS NOT NULL)::int)
    FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE n.concurrency_key IS NOT NULL
      AND n.concurrency_key IS DISTINCT FROM o.concurrency_key
    GROUP BY n.concurrency_key
    ORDER BY n.concurrency_key
    ON CONFLICT (concurrency_key) DO UPDATE SET
      in_flight = "arbiter".arbiter_concurrency.in_flight + EXCLUDED.in_flight;
  END IF;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

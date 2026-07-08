CREATE OR REPLACE FUNCTION "arbiter"."ensure_golden_jobs_rate_limit_buckets_insert"()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM new_table WHERE rate_limit_key IS NOT NULL LIMIT 1) THEN
    RETURN NULL;
  END IF;

  INSERT INTO "arbiter".arbiter_rate_limits (rate_limit_key, policy_prefix, tokens, last_refill)
  SELECT n.rate_limit_key, MAX(n.rate_limit_prefix),
         MAX(COALESCE(p.override_max_tokens, p.default_max_tokens)), NOW()
  FROM new_table n
  JOIN "arbiter".arbiter_rate_limit_policies p ON p.prefix_id = n.rate_limit_prefix
  WHERE n.rate_limit_key IS NOT NULL
    AND NOT EXISTS (SELECT 1 FROM "arbiter".arbiter_rate_limits b WHERE b.rate_limit_key = n.rate_limit_key)
  GROUP BY n.rate_limit_key
  ORDER BY n.rate_limit_key
  ON CONFLICT (rate_limit_key) DO NOTHING;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;
CREATE OR REPLACE FUNCTION "arbiter"."ensure_golden_jobs_rate_limit_buckets_update"()
RETURNS TRIGGER AS $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM new_table n JOIN old_table o ON o.id = n.id
    WHERE n.rate_limit_key IS NOT NULL AND n.rate_limit_key IS DISTINCT FROM o.rate_limit_key
    LIMIT 1
  ) THEN
    RETURN NULL;
  END IF;

  INSERT INTO "arbiter".arbiter_rate_limits (rate_limit_key, policy_prefix, tokens, last_refill)
  SELECT n.rate_limit_key, MAX(n.rate_limit_prefix),
         MAX(COALESCE(p.override_max_tokens, p.default_max_tokens)), NOW()
  FROM new_table n JOIN old_table o ON o.id = n.id
  JOIN "arbiter".arbiter_rate_limit_policies p ON p.prefix_id = n.rate_limit_prefix
  WHERE n.rate_limit_key IS NOT NULL AND n.rate_limit_key IS DISTINCT FROM o.rate_limit_key
    AND NOT EXISTS (SELECT 1 FROM "arbiter".arbiter_rate_limits b WHERE b.rate_limit_key = n.rate_limit_key)
  GROUP BY n.rate_limit_key
  ORDER BY n.rate_limit_key
  ON CONFLICT (rate_limit_key) DO NOTHING;

  RETURN NULL;
END;
$$ LANGUAGE plpgsql;

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Gates SQL templates.
module Arbiter.Core.Sql.Gates
  ( ensureGateRowSQL
  , checkGateSQL
  , tryClaimGateSQL
  , claimOrReadGateSQL
  , releaseGateSQL
  , gateNameDigestSQL
  , bumpGateSQL
  , setGateMetadataSQL
  ) where

import Data.Aeson (Value)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Time (UTCTime)

import Arbiter.Core.Gates (arbiterGatesTable)
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query)

-- | Idempotently create the gate row for a task.
ensureGateRowSQL :: SchemaName -> Text -> Query ()
ensureGateRowSQL schemaName task =
  let tbl = arbiterGatesTable schemaName
   in [sql|
        INSERT INTO ${tbl} (task_name) VALUES (#{task :: CText})
        ON CONFLICT (task_name) DO NOTHING
      |]

-- | Cheap read-only pre-transaction check: TRUE if last_run_at is older than the interval.
checkGateSQL :: SchemaName -> Double -> Text -> Query Bool
checkGateSQL schemaName intervalSecs task =
  let tbl = arbiterGatesTable schemaName
   in [sql|
        SELECT (last_run_at < NOW() - (#{intervalSecs :: CFloat8}::double precision * interval '1 second'))
          AS @{result :: CBool}
        FROM ${tbl}
        WHERE task_name = #{task :: CText}
      |]

-- | Atomically claim the gate row iff the interval elapsed and no concurrent tx holds it.
tryClaimGateSQL :: SchemaName -> Text -> Double -> Query Int64
tryClaimGateSQL schemaName task intervalSecs =
  let tbl = arbiterGatesTable schemaName
   in [sql|
        SELECT 1::bigint AS @{result :: CInt8} FROM ${tbl}
        WHERE task_name = #{task :: CText}
          AND last_run_at < NOW() - (#{intervalSecs :: CFloat8}::double precision * interval '1 second')
        FOR UPDATE SKIP LOCKED
      |]

-- | An md5 digest, for a gate name too long to be a key itself.
gateNameDigestSQL :: Text -> Query Text
gateNameDigestSQL name = [sql|SELECT md5(#{name :: CText}) AS @{digest :: CText}|]

-- | Claim the gate, or read what the last winner published. NULL timestamps mean lost.
claimOrReadGateSQL
  :: SchemaName -> Text -> Double -> Double -> Query (Maybe UTCTime, Maybe UTCTime, Maybe Value, Maybe Double)
claimOrReadGateSQL schemaName task intervalSecs maxAgeSecs =
  let tbl = arbiterGatesTable schemaName
   in [sql|
        WITH seeded AS (
          INSERT INTO ${tbl} (task_name, last_run_at)
          SELECT #{task :: CText}, NOW()
          WHERE NOT EXISTS (SELECT 1 FROM ${tbl} WHERE task_name = #{task :: CText})
          ON CONFLICT (task_name) DO NOTHING
          RETURNING NOW() AS claimed_at, '1970-01-01'::timestamptz AS previous_run_at
        ),
        claimed AS (
          UPDATE ${tbl} g SET last_run_at = NOW()
          FROM ${tbl} old
          WHERE g.task_name = #{task :: CText}
            AND old.task_name = g.task_name
            AND g.last_run_at < NOW() - (#{intervalSecs :: CFloat8}::double precision * interval '1 second')
          RETURNING NOW() AS claimed_at, old.last_run_at AS previous_run_at
        ),
        claim AS (
          SELECT claimed_at, previous_run_at FROM seeded
          UNION ALL
          SELECT claimed_at, previous_run_at FROM claimed
        ),
        published AS (
          SELECT metadata -> 'payload' AS payload,
                 EXTRACT(EPOCH FROM NOW() - (metadata ->> 'at')::timestamptz)::float8 AS age_seconds
          FROM ${tbl}
          WHERE task_name = #{task :: CText}
            AND metadata -> 'payload' IS NOT NULL
            AND (metadata ->> 'at')::timestamptz > NOW() - (#{maxAgeSecs :: CFloat8}::double precision * interval '1 second')
        )
        SELECT (SELECT claimed_at FROM claim) AS @{claimed_at :: Maybe CTimestamptz},
               (SELECT previous_run_at FROM claim) AS @{previous_run_at :: Maybe CTimestamptz},
               (SELECT payload FROM published) AS @{payload :: Maybe CJsonb},
               (SELECT age_seconds FROM published) AS @{age_seconds :: Maybe CFloat8}
      |]

-- | Put a claim's watermark back, for a winner whose work never published. Scoped to
-- the value that claim wrote, so a later run's bump stands.
releaseGateSQL :: SchemaName -> Text -> UTCTime -> UTCTime -> Query ()
releaseGateSQL schemaName task claimedAt previous =
  let tbl = arbiterGatesTable schemaName
   in [sql|
        UPDATE ${tbl} SET last_run_at = #{previous :: CTimestamptz}
        WHERE task_name = #{task :: CText} AND last_run_at = #{claimedAt :: CTimestamptz}
      |]

-- | Bump last_run_at to NOW(), inside the claim transaction so it commits with the task's work.
bumpGateSQL :: SchemaName -> Text -> Query ()
bumpGateSQL schemaName task =
  let tbl = arbiterGatesTable schemaName
   in [sql|UPDATE ${tbl} SET last_run_at = NOW() WHERE task_name = #{task :: CText}|]

-- | Publish what the task computed, stamped with the claim the work started from, which
-- is what a reader ages the payload by. The gate's interval restarts from the publish.
setGateMetadataSQL :: SchemaName -> Value -> UTCTime -> Text -> Query ()
setGateMetadataSQL schemaName metadata claimedAt task =
  let tbl = arbiterGatesTable schemaName
   in [sql|
        UPDATE ${tbl}
        SET last_run_at = NOW(),
            metadata = jsonb_build_object('at', to_jsonb(#{claimedAt :: CTimestamptz}::timestamptz), 'payload', #{metadata :: CJsonb}::jsonb)
        WHERE task_name = #{task :: CText}
          AND (metadata ->> 'at' IS NULL OR (metadata ->> 'at')::timestamptz < #{claimedAt :: CTimestamptz})
      |]

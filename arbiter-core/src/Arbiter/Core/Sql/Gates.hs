{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Gates SQL templates.
module Arbiter.Core.Sql.Gates
  ( ensureGateRowSQL
  , checkGateSQL
  , tryClaimGateSQL
  , gateClaimedAtSQL
  , gateNameDigestSQL
  , bumpGateSQL
  , setGateMetadataSQL
  , gateMetadataSQL
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

-- | The claiming transaction's timestamp, which is the value its 'bumpGateSQL' writes.
gateClaimedAtSQL :: Query UTCTime
gateClaimedAtSQL = [sql|SELECT NOW() AS @{claimed_at :: CTimestamptz}|]

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
      |]

-- | What the task published, with its age in seconds, if it is still fresh.
gateMetadataSQL :: SchemaName -> Text -> Double -> Query (Value, Double)
gateMetadataSQL schemaName task maxAgeSecs =
  let tbl = arbiterGatesTable schemaName
   in [sql|
        SELECT metadata -> 'payload' AS @{metadata :: CJsonb},
               EXTRACT(EPOCH FROM NOW() - (metadata ->> 'at')::timestamptz)::float8 AS @{age_seconds :: CFloat8}
        FROM ${tbl}
        WHERE task_name = #{task :: CText}
          AND metadata -> 'payload' IS NOT NULL
          AND (metadata ->> 'at')::timestamptz > NOW() - (#{maxAgeSecs :: CFloat8}::double precision * interval '1 second')
      |]

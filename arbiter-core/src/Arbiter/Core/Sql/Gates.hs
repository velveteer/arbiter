{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Gates SQL templates.
module Arbiter.Core.Sql.Gates
  ( ensureGateRowSQL
  , checkGateSQL
  , tryClaimGateSQL
  , bumpGateSQL
  ) where

import Data.Int (Int64)
import Data.Text (Text)

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

-- | Bump last_run_at to NOW(), inside the claim transaction so it commits with the task's work.
bumpGateSQL :: SchemaName -> Text -> Query ()
bumpGateSQL schemaName task =
  let tbl = arbiterGatesTable schemaName
   in [sql|UPDATE ${tbl} SET last_run_at = NOW() WHERE task_name = #{task :: CText}|]

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Gates SQL templates.
module Arbiter.Core.Sql.Gates
  ( ensureGateRowSQL
  , checkGateSQL
  , tryClaimGateSQL
  , bumpGateSQL
  , setGateMetadataSQL
  , gateMetadataSQL
  ) where

import Data.Text (Text)
import NeatInterpolation (text)

import Arbiter.Core.Gates (arbiterGatesTable)
import Arbiter.Core.Job.Schema (SchemaName)

-- | Idempotently create the gate row for a task. Parameters: task_name.
ensureGateRowSQL :: SchemaName -> Text
ensureGateRowSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|
        INSERT INTO ${tbl} (task_name) VALUES (?)
        ON CONFLICT (task_name) DO NOTHING
      |]

-- | Cheap read-only pre-transaction check: TRUE if last_run_at is older than the interval.
-- Parameters: interval seconds, task_name.
checkGateSQL :: SchemaName -> Text
checkGateSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|
        SELECT (last_run_at < NOW() - (?::double precision * interval '1 second'))
          AS result
        FROM ${tbl}
        WHERE task_name = ?
      |]

-- | Atomically claim the gate row iff the interval elapsed and no concurrent tx holds it.
-- Parameters: task_name, interval seconds.
tryClaimGateSQL :: SchemaName -> Text
tryClaimGateSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|
        SELECT 1::bigint AS result FROM ${tbl}
        WHERE task_name = ?
          AND last_run_at < NOW() - (?::double precision * interval '1 second')
        FOR UPDATE SKIP LOCKED
      |]

-- | Bump last_run_at to NOW(), inside the claim transaction so it commits with the task's work.
-- Parameters: task_name.
bumpGateSQL :: SchemaName -> Text
bumpGateSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|UPDATE ${tbl} SET last_run_at = NOW() WHERE task_name = ?|]

-- | Publish what the task computed, for the callers that lost its gate.
-- Parameters: metadata, task_name.
setGateMetadataSQL :: SchemaName -> Text
setGateMetadataSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|UPDATE ${tbl} SET metadata = ?::jsonb WHERE task_name = ?|]

-- | Read what the task published, if it is still fresh. Published in the run that bumped
-- last_run_at, so that is its age. Parameters: task_name, max age in seconds.
gateMetadataSQL :: SchemaName -> Text
gateMetadataSQL schemaName =
  let tbl = arbiterGatesTable schemaName
   in [text|
        SELECT metadata FROM ${tbl}
        WHERE task_name = ?
          AND metadata IS NOT NULL
          AND last_run_at > NOW() - (?::double precision * interval '1 second')
      |]

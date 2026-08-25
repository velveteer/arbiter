{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Workers SQL templates.
module Arbiter.Core.Sql.Workers
  ( workerColumnList
  , workerHealthCaseSQL
  , upsertWorkerSQL
  , heartbeatWorkerSQL
  , setWorkerPausedSQL
  , markWorkerShuttingDownSQL
  , deleteWorkerSQL
  , workerRegisteredSQL
  , listWorkersSQL
  , deleteStaleWorkersSQL
  ) where

import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Text qualified as T
import Data.UUID.Types (UUID)

import Arbiter.Core.Codec (codecColumns, workerRowWithHealthCodec)
import Arbiter.Core.Job.Schema (SchemaName, pauseNotifyChannelPrefix)
import Arbiter.Core.Queues (arbiterQueuesTable)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)
import Arbiter.Core.Worker (WorkerRow, arbiterWorkersTable)

-- | Worker SELECT columns from the codec, with @health@ rendered as a computed expression.
workerColumnList :: Text
workerColumnList = T.intercalate ", " (map render (codecColumns workerRowWithHealthCodec))
  where
    render "health" = workerHealthCaseSQL <> " AS health"
    render c = c

-- | Heartbeat-derived health, computed against the DB clock.
workerHealthCaseSQL :: Text
workerHealthCaseSQL =
  "CASE WHEN last_heartbeat < NOW() - stale_threshold_secs * interval '1 second' THEN 'stale' WHEN shutting_down THEN 'draining' ELSE 'live' END"

-- | Upsert a worker registration and return its effective paused state (worker OR queue).
upsertWorkerSQL :: SchemaName -> UUID -> Text -> Maybe Text -> Maybe Int32 -> Double -> Maybe Value -> Query Bool
upsertWorkerSQL schemaName workerId queue host threads staleThreshold metadata =
  let tbl = arbiterWorkersTable schemaName
      qmTbl = arbiterQueuesTable schemaName
   in [sql|
        WITH upserted AS (
          INSERT INTO ${tbl}
            (worker_id, queue_name, host_name, worker_count, stale_threshold_secs, metadata, started_at, last_heartbeat, shutting_down)
          VALUES (#{workerId :: CUuid}, #{queue :: CText}, #{host :: Maybe CText}, #{threads :: Maybe CInt4}, #{staleThreshold :: CFloat8}, #{metadata :: Maybe CJsonb}, NOW(), NOW(), FALSE)
          ON CONFLICT (worker_id) DO UPDATE
            SET queue_name = EXCLUDED.queue_name,
                host_name = EXCLUDED.host_name,
                worker_count = EXCLUDED.worker_count,
                stale_threshold_secs = EXCLUDED.stale_threshold_secs,
                metadata = EXCLUDED.metadata,
                last_heartbeat = NOW(),
                shutting_down = FALSE
          RETURNING queue_name, paused
        )
        SELECT u.paused OR COALESCE(qm.paused, FALSE) AS @{effective_paused :: CBool}
        FROM upserted u
        LEFT JOIN ${qmTbl} qm ON qm.queue_name = u.queue_name
      |]

-- | Whether the worker registry holds this identity.
workerRegisteredSQL :: SchemaName -> UUID -> Query Bool
workerRegisteredSQL schemaName workerId =
  let tbl = arbiterWorkersTable schemaName
   in [sql|
        SELECT EXISTS (SELECT 1 FROM ${tbl} WHERE worker_id = #{workerId :: CUuid}) AS @{registered :: CBool}
      |]

-- | Bump last_heartbeat and return the worker's effective paused state (worker OR queue).
heartbeatWorkerSQL :: SchemaName -> UUID -> Query Bool
heartbeatWorkerSQL schemaName workerId =
  let tbl = arbiterWorkersTable schemaName
      qmTbl = arbiterQueuesTable schemaName
   in [sql|
        WITH updated AS (
          UPDATE ${tbl} SET last_heartbeat = NOW()
          WHERE worker_id = #{workerId :: CUuid}
          RETURNING queue_name, paused
        )
        SELECT updated.paused OR COALESCE(qm.paused, FALSE) AS @{effective_paused :: CBool}
        FROM updated
        LEFT JOIN ${qmTbl} qm ON qm.queue_name = updated.queue_name
      |]

-- | Set @paused@ for a worker and NOTIFY its effective pause state.
setWorkerPausedSQL :: SchemaName -> Bool -> UUID -> Query Int64
setWorkerPausedSQL schemaName paused workerId =
  let tbl = arbiterWorkersTable schemaName
      qmTbl = arbiterQueuesTable schemaName
      chanPrefix = pauseNotifyChannelPrefix schemaName
   in [sql|
        WITH updated AS (
          UPDATE ${tbl} SET paused = #{paused :: CBool} WHERE worker_id = #{workerId :: CUuid}
          RETURNING worker_id, queue_name, paused
        ),
        notif AS (
          SELECT pg_notify(
            LEFT('${chanPrefix}' || u.queue_name, 63),
            json_build_object(
              'worker_id', u.worker_id,
              'paused', u.paused OR COALESCE(qm.paused, FALSE)
            )::text
          )
          FROM updated u
          LEFT JOIN ${qmTbl} qm ON qm.queue_name = u.queue_name
        )
        SELECT count(*)::int8 AS @{count :: CInt8} FROM updated
        WHERE (SELECT count(*) FROM notif) >= 0
      |]

-- | Mark a worker as gracefully draining.
markWorkerShuttingDownSQL :: SchemaName -> UUID -> Query ()
markWorkerShuttingDownSQL schemaName workerId =
  let tbl = arbiterWorkersTable schemaName
   in [sql|UPDATE ${tbl} SET shutting_down = TRUE, last_heartbeat = NOW() WHERE worker_id = #{workerId :: CUuid}|]

-- | Remove a worker row outright (clean shutdown, rather than waiting for the stale-sweeper).
deleteWorkerSQL :: SchemaName -> UUID -> Query ()
deleteWorkerSQL schemaName workerId =
  let tbl = arbiterWorkersTable schemaName
   in [sql|DELETE FROM ${tbl} WHERE worker_id = #{workerId :: CUuid}|]

-- | List workers, with NULL parameters short-circuiting the corresponding filter.
listWorkersSQL :: SchemaName -> Maybe Text -> Maybe Double -> Query (WorkerRow, Text)
listWorkersSQL schemaName queue liveSecs =
  let tbl = arbiterWorkersTable schemaName
      cols = workerColumnList
   in rows
        workerRowWithHealthCodec
        [sql|
          WITH filt AS (SELECT #{queue :: Maybe CText}::text AS queue, #{liveSecs :: Maybe CFloat8}::float8 AS live_secs)
          SELECT ${cols} FROM ${tbl}, filt
          WHERE (filt.queue IS NULL OR queue_name = filt.queue)
            AND (filt.live_secs IS NULL
                 OR last_heartbeat > NOW() - filt.live_secs * interval '1 second')
          ORDER BY queue_name, started_at DESC
        |]

-- | Delete worker rows older than their own @stale_threshold_secs@.
deleteStaleWorkersSQL :: SchemaName -> Query ()
deleteStaleWorkersSQL schemaName =
  let tbl = arbiterWorkersTable schemaName
   in [sql|
        DELETE FROM ${tbl}
        WHERE last_heartbeat < NOW() - (stale_threshold_secs * interval '1 second')
      |]

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Queues SQL templates.
module Arbiter.Core.Sql.Queues
  ( queueColumnList
  , ensureQueueSQL
  , setQueuePausedSQL
  , getQueueSQL
  , listQueuesSQL
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Codec (codecColumns, queueRowCodec)
import Arbiter.Core.Job.Schema (SchemaName, pauseNotifyChannelPrefix)
import Arbiter.Core.Queues (arbiterQueuesTable)
import Arbiter.Core.Worker (arbiterWorkersTable)

queueColumnList :: Text
queueColumnList = T.intercalate ", " (codecColumns queueRowCodec)

-- | Insert an arbiter_queues row with defaults if one doesn't already exist.
--
-- Parameters: queue_name
ensureQueueSQL :: SchemaName -> Text
ensureQueueSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
   in [text|
        INSERT INTO ${tbl} (queue_name) VALUES (?)
        ON CONFLICT (queue_name) DO NOTHING
      |]

-- | Upsert the queue's paused flag and fan one NOTIFY per worker in the queue.
-- @paused_at@ is set on first pause, cleared on resume, untouched on re-pause.
-- Parameters: queue_name, paused, paused (repeated for the CASE expression)
setQueuePausedSQL :: SchemaName -> Text
setQueuePausedSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
      wTbl = arbiterWorkersTable schemaName
      chanPrefix = pauseNotifyChannelPrefix schemaName
   in [text|
        WITH upsert AS (
          INSERT INTO ${tbl} (queue_name, paused, paused_at)
            VALUES (?, ?, CASE WHEN ?::boolean THEN NOW() ELSE NULL END)
          ON CONFLICT (queue_name) DO UPDATE
            SET paused = EXCLUDED.paused,
                paused_at = CASE
                  WHEN EXCLUDED.paused AND NOT arbiter_queues.paused THEN NOW()
                  WHEN NOT EXCLUDED.paused THEN NULL
                  ELSE arbiter_queues.paused_at
                END,
                updated_at = NOW()
          RETURNING queue_name, paused
        ),
        notif AS (
          SELECT pg_notify(
            LEFT('${chanPrefix}' || w.queue_name, 63),
            json_build_object(
              'worker_id', w.worker_id,
              'paused', w.paused OR u.paused
            )::text
          )
          FROM upsert u
          JOIN ${wTbl} w ON w.queue_name = u.queue_name
        )
        SELECT count(*)::int8 AS count FROM upsert
        WHERE (SELECT count(*) FROM notif) >= 0
      |]

-- | Get the arbiter_queues row for a single queue.
--
-- Parameters: queue_name
getQueueSQL :: SchemaName -> Text
getQueueSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
      cols = queueColumnList
   in [text|SELECT ${cols} FROM ${tbl} WHERE queue_name = ?|]

-- | List all arbiter_queues rows.
listQueuesSQL :: SchemaName -> Text
listQueuesSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
      cols = queueColumnList
   in [text|SELECT ${cols} FROM ${tbl} ORDER BY queue_name|]

-- ---------------------------------------------------------------------------
-- Global Gate Operations
-- ---------------------------------------------------------------------------

-- | Idempotently create the gate row for a task. Parameters: task_name.

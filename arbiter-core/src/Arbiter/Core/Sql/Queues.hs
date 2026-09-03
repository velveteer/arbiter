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

import Data.Int (Int64)
import Data.Text (Text)

import Arbiter.Core.Codec (queueRowCodec)
import Arbiter.Core.Job.Schema (SchemaName, pauseNotifyChannelPrefix)
import Arbiter.Core.Queues (QueueRow, arbiterQueuesTable)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)
import Arbiter.Core.SqlLiterals (textLiteral)
import Arbiter.Core.Worker (arbiterWorkersTable)

-- | The @arbiter_queues@ read columns, in codec order.
queueColumnList :: Text
queueColumnList = "queue_name, paused, paused_at, metadata, created_at, updated_at"

-- | Insert an arbiter_queues row with defaults if one doesn't already exist.
ensureQueueSQL :: SchemaName -> Text -> Query ()
ensureQueueSQL schemaName queue =
  let tbl = arbiterQueuesTable schemaName
   in [sql|
        INSERT INTO ${tbl} (queue_name) VALUES (#{queue :: CText})
        ON CONFLICT (queue_name) DO NOTHING
      |]

-- | Upsert the queue's paused flag and fan one NOTIFY per worker in the queue.
-- @paused_at@ is set on first pause, cleared on resume, untouched on re-pause.
setQueuePausedSQL :: SchemaName -> Text -> Bool -> Query Int64
setQueuePausedSQL schemaName queue paused =
  let tbl = arbiterQueuesTable schemaName
      workersTbl = arbiterWorkersTable schemaName
      chanPrefix = textLiteral (pauseNotifyChannelPrefix schemaName)
   in [sql|
        WITH upsert AS (
          INSERT INTO ${tbl} (queue_name, paused, paused_at)
            VALUES (#{queue :: CText}, #{paused :: CBool},
                    CASE WHEN #{paused :: CBool}::boolean THEN NOW() ELSE NULL END)
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
            LEFT(${chanPrefix} || worker.queue_name, 63),
            json_build_object(
              'worker_id', worker.worker_id,
              'paused', worker.paused OR upserted.paused
            )::text
          )
          FROM upsert upserted
          JOIN ${workersTbl} worker ON worker.queue_name = upserted.queue_name
        )
        SELECT count(*)::int8 AS @{count :: CInt8} FROM upsert
        WHERE (SELECT count(*) FROM notif) >= 0
      |]

-- | Get the arbiter_queues row for a single queue.
getQueueSQL :: SchemaName -> Text -> Query QueueRow
getQueueSQL schemaName queue =
  let tbl = arbiterQueuesTable schemaName
   in rows queueRowCodec [sql|SELECT ${queueColumnList} FROM ${tbl} WHERE queue_name = #{queue :: CText}|]

-- | List all arbiter_queues rows.
listQueuesSQL :: SchemaName -> Query QueueRow
listQueuesSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
   in rows queueRowCodec [sql|SELECT ${queueColumnList} FROM ${tbl} ORDER BY queue_name|]

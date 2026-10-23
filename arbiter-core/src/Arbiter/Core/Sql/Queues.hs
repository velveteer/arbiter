{-# LANGUAGE QuasiQuotes #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Queues SQL templates.
module Arbiter.Core.Sql.Queues
  ( ensureQueueSQL
  , setQueuePausedSQL
  , setQueueDLQRetentionSQL
  , queueDLQRetentionsSQL
  , existingQueuesSQL
  , getQueueSQL
  , listQueuesSQL
  ) where

import Data.Int (Int64)
import Data.Text (Text)

import Arbiter.Core.Codec (codecColumns, joinColumns, queueRowCodec)
import Arbiter.Core.Job.Schema (SchemaName, dlqSuffix, pauseNotifyChannelPrefix)
import Arbiter.Core.Queues (QueueRow, arbiterQueuesTable)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)
import Arbiter.Core.SqlLiterals (intLiteral, maxIdentifierBytes, textLiteral)
import Arbiter.Core.Worker (arbiterWorkersTable)

-- | The @arbiter_queues@ read columns, in codec order.
queueColumnList :: Text
queueColumnList = joinColumns (codecColumns queueRowCodec)

-- | Insert an @arbiter_queues@ row with defaults if one does not already exist.
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
      maxIdent = intLiteral maxIdentifierBytes
   in [sql|
        WITH upsert AS (
          INSERT INTO ${tbl} AS queue_row (queue_name, paused, paused_at)
            VALUES (#{queue :: CText}, #{paused :: CBool},
                    CASE WHEN #{paused :: CBool}::boolean THEN NOW() ELSE NULL END)
          ON CONFLICT (queue_name) DO UPDATE
            SET paused = EXCLUDED.paused,
                paused_at = CASE
                  WHEN EXCLUDED.paused AND NOT queue_row.paused THEN NOW()
                  WHEN NOT EXCLUDED.paused THEN NULL
                  ELSE queue_row.paused_at
                END,
                updated_at = NOW()
          RETURNING queue_name, paused
        ),
        notif AS (
          SELECT pg_notify(
            LEFT(${chanPrefix} || worker.queue_name, ${maxIdent}),
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

-- | Upsert the queue's DLQ retention in seconds. 'Nothing' keeps its DLQ jobs.
setQueueDLQRetentionSQL :: SchemaName -> Text -> Maybe Int64 -> Query ()
setQueueDLQRetentionSQL schemaName queue retention =
  let tbl = arbiterQueuesTable schemaName
   in [sql|
        INSERT INTO ${tbl} (queue_name, dlq_retention_secs)
          VALUES (#{queue :: CText}, #{retention :: Maybe CInt8}::bigint)
        ON CONFLICT (queue_name) DO UPDATE
          SET dlq_retention_secs = EXCLUDED.dlq_retention_secs, updated_at = NOW()
      |]

-- | The queues with a DLQ retention, with the retention in seconds.
queueDLQRetentionsSQL :: SchemaName -> Query (Text, Int64)
queueDLQRetentionsSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
   in [sql|
        SELECT @{queue_name :: CText}, @{dlq_retention_secs :: CInt8}
        FROM ${tbl}
        WHERE dlq_retention_secs IS NOT NULL
        ORDER BY queue_name
      |]

-- | The @arbiter_queues@ names and the given names whose job and DLQ tables exist in
-- the schema.
existingQueuesSQL :: SchemaName -> [Text] -> Query Text
existingQueuesSQL schemaName queues =
  let tbl = arbiterQueuesTable schemaName
      schemaLit = textLiteral schemaName
      dlqLit = textLiteral dlqSuffix
   in [sql|
        SELECT @{queue_name :: CText}
        FROM (
          SELECT queue_name FROM ${tbl}
          UNION
          SELECT unnest(#{queues :: [CText]}::text[])
        ) known(queue_name)
        WHERE to_regclass(format('%I.%I', ${schemaLit}::text, queue_name)) IS NOT NULL
          AND to_regclass(format('%I.%I', ${schemaLit}::text, queue_name || ${dlqLit})) IS NOT NULL
        ORDER BY queue_name
      |]

-- | Get the @arbiter_queues@ row for a single queue.
getQueueSQL :: SchemaName -> Text -> Query QueueRow
getQueueSQL schemaName queue =
  let tbl = arbiterQueuesTable schemaName
   in rows queueRowCodec [sql|SELECT ${queueColumnList} FROM ${tbl} WHERE queue_name = #{queue :: CText}|]

-- | List all @arbiter_queues@ rows.
listQueuesSQL :: SchemaName -> Query QueueRow
listQueuesSQL schemaName =
  let tbl = arbiterQueuesTable schemaName
   in rows queueRowCodec [sql|SELECT ${queueColumnList} FROM ${tbl} ORDER BY queue_name|]

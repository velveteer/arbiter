{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Completed-job archive SQL templates.
module Arbiter.Core.Sql.Archive
  ( archiveAckCte
  , updateArchiveResultSQL
  , updateArchiveResultsBatchSQL
  , purgeArchiveSQL
  , archivePurgeBatch
  , listArchiveFilteredSQL
  , countArchiveFilteredSQL
  , deleteArchiveJobSQL
  , deleteArchiveJobsBatchSQL
  , reEnqueueFromArchiveSQL
  , allArchiveColumns
  ) where

import Data.Aeson (Value)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)
import NeatInterpolation (text)

import Arbiter.Core.Codec (archiveRowCodec, jobRowCodec)
import Arbiter.Core.Job.Schema (jobQueueArchiveTable, jobQueueTable)
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.Sql.Jobs (enqueuedAgainCols, jobColsExceptId, jobColumns)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)

-- | The archive read columns, in codec order. The archive uses @job_id@ for the
-- main-table @id@.
allArchiveColumns :: Text
allArchiveColumns =
  [text|
    id, completed_at, job_id, payload, group_key, inserted_at, updated_at, attempts, last_error, priority,
    last_attempted_at, not_visible_until, dedup_key, dedup_strategy, max_attempts,
    parent_id, parent_state, traceparent, tracestate, suspended, claimed_by, claim_seq,
    archive_for, kind, rate_limit_key, rate_limit_prefix, concurrency_key, concurrency_prefix,
    result
  |]

-- | The @archived@ CTE teeing rows from the named @ack@ CTE into the archive, per-row
-- on @archive_for@. @archive_expires_at@ is precomputed. Shared by single and batch ack.
archiveAckCte :: Text -> Text -> Text -> Text
archiveAckCte schema tableName ackCte =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [text|
        archived AS (
          INSERT INTO ${archiveTbl} (job_id, ${jobColsExceptId}, rate_limit_cost, completed_at, archive_expires_at)
          SELECT id, ${jobColsExceptId}, rate_limit_cost, NOW(), NOW() + (archive_for * interval '1 second')
          FROM ${ackCte}
          WHERE archive_for > 0
        ),
      |]

-- | Set a completed root job's stored @result@ on its archive row. No-ops without
-- an archive row.
updateArchiveResultSQL :: Text -> Text -> Value -> Int64 -> Query ()
updateArchiveResultSQL schema tableName result jobId =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|UPDATE ${archiveTbl} SET result = #{result :: CJsonb} WHERE job_id = #{jobId :: CInt8}|]

-- | 'updateArchiveResultSQL' for several jobs in one statement.
updateArchiveResultsBatchSQL :: Text -> Text -> [Int64] -> [Value] -> Query ()
updateArchiveResultsBatchSQL schema tableName jobIds results =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|
        UPDATE ${archiveTbl} archive_row SET result = src.result
        FROM (
          SELECT unnest(#{jobIds :: [CInt8]}::bigint[]) AS job_id,
                 unnest(#{results :: [CJsonb]}::jsonb[]) AS result
        ) src
        WHERE archive_row.job_id = src.job_id
      |]

-- | Per-queue cap on archived jobs purged in one reaper pass.
archivePurgeBatch :: Int
archivePurgeBatch = 10000

-- | Delete up to @archivePurgeBatch@ archived jobs whose per-row
-- @archive_expires_at@ has passed.
purgeArchiveSQL :: Text -> Text -> Text
purgeArchiveSQL schema tableName =
  let archiveTbl = jobQueueArchiveTable schema tableName
      lim = T.pack (show archivePurgeBatch)
   in [text|
        DELETE FROM ${archiveTbl}
        WHERE ctid IN (
          SELECT ctid FROM ${archiveTbl}
          WHERE archive_expires_at < NOW()
          LIMIT ${lim}
        )
      |]

-- | List archived jobs under a dynamic WHERE.
listArchiveFilteredSQL
  :: Text -> Text -> Query () -> Text -> Int64 -> Int64 -> Query (Int64, UTCTime, JobRead Value, Maybe Value)
listArchiveFilteredSQL schema tableName whereFrag orderBy limit offset =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in rows
        (archiveRowCodec tableName)
        [sql|
          SELECT ${allArchiveColumns}
          FROM ${archiveTbl}
          ${whereFrag}
          ORDER BY ${orderBy}
          LIMIT #{limit :: CInt8} OFFSET #{offset :: CInt8}
        |]

-- | Count archived jobs under a dynamic WHERE.
countArchiveFilteredSQL :: Text -> Text -> Query () -> Query Int64
countArchiveFilteredSQL schema tableName whereFrag =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|SELECT COUNT(*) AS @{count :: CInt8} FROM ${archiveTbl} ${whereFrag}|]

-- | Delete one archived job by its archive primary key.
deleteArchiveJobSQL :: Text -> Text -> Int64 -> Query ()
deleteArchiveJobSQL schema tableName archiveId =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|DELETE FROM ${archiveTbl} WHERE id = #{archiveId :: CInt8}|]

-- | Delete archived jobs by archive primary key.
deleteArchiveJobsBatchSQL :: Text -> Text -> [Int64] -> Query ()
deleteArchiveJobsBatchSQL schema tableName archiveIds =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|DELETE FROM ${archiveTbl} WHERE id = ANY(#{archiveIds :: [CInt8]})|]

-- | Re-enqueue an archived job as a fresh standalone job, keeping the archive
-- row. Carries 'enqueuedAgainCols' and resets the other columns to their defaults.
reEnqueueFromArchiveSQL :: Text -> Text -> Int64 -> Query (JobRead Value)
reEnqueueFromArchiveSQL schema tableName archiveId =
  let archiveTbl = jobQueueArchiveTable schema tableName
      tbl = jobQueueTable schema tableName
   in rows
        (jobRowCodec tableName)
        [sql|
          INSERT INTO ${tbl} (${enqueuedAgainCols})
          SELECT ${enqueuedAgainCols}
          FROM ${archiveTbl}
          WHERE id = #{archiveId :: CInt8}
          RETURNING ${jobColumns}
        |]

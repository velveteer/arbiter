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

import Arbiter.Core.Codec (archiveRowCodec, codecColumns, jobRowCodec)
import Arbiter.Core.Job.Schema (jobQueueArchiveTable, jobQueueTable)
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.Sql.Jobs (enqueuedAgainCols, jobColsExceptId, jobColumns)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)

-- | Archive columns: archive-specific fields + all Job read fields (with job_id
-- instead of id). Used as the SELECT list for archive listings.
allArchiveColumns :: [Text]
allArchiveColumns = codecColumns (archiveRowCodec "")

-- | The @, archived AS (...)@ CTE teeing rows from the named @ack@ CTE into the
-- archive, per-row on @archive_for@ (rows with NULL retention are not archived).
-- @archive_expires_at@ is precomputed so the purge is a self-contained sweep.
-- Shared by single and batch ack.
archiveAckCte :: Text -> Text -> Text -> Text
archiveAckCte schema tableName ackCte =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [text|,
        archived AS (
          INSERT INTO ${archiveTbl} (job_id, ${jobColsExceptId}, rate_limit_cost, completed_at, archive_expires_at)
          SELECT id, ${jobColsExceptId}, rate_limit_cost, NOW(), NOW() + (archive_for * interval '1 second')
          FROM ${ackCte}
          WHERE archive_for > 0
        )|]

-- | Set a completed root job's stored @result@ on its archive row. No-ops when
-- the job was not archived (no row for @job_id@).
updateArchiveResultSQL :: Text -> Text -> Value -> Int64 -> Query ()
updateArchiveResultSQL schema tableName result jobId =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|UPDATE ${archiveTbl} SET result = #{result :: CJsonb} WHERE job_id = #{jobId :: CInt8}|]

-- | 'updateArchiveResultSQL' for several jobs in one statement.
updateArchiveResultsBatchSQL :: Text -> Text -> [Int64] -> [Value] -> Query ()
updateArchiveResultsBatchSQL schema tableName jobIds results =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|
        UPDATE ${archiveTbl} a SET result = src.result
        FROM (
          SELECT unnest(#{jobIds :: [CInt8]}::bigint[]) AS job_id,
                 unnest(#{results :: [CJsonb]}::jsonb[]) AS result
        ) src
        WHERE a.job_id = src.job_id
      |]

-- | Per-queue cap on archived jobs purged in one reaper pass.
archivePurgeBatch :: Int
archivePurgeBatch = 10000

-- | Delete up to @archivePurgeBatch@ archived jobs whose per-row
-- @archive_expires_at@ has passed. Capped per call so a large backlog drains over
-- several reaper ticks instead of one unbounded DELETE that stalls the loop.
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
      columns = T.intercalate ", " allArchiveColumns
   in rows
        (archiveRowCodec tableName)
        [sql|
          SELECT ${columns}
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
-- row. Carries payload, group, priority, max_attempts, admission keys/cost,
-- archive_for, and the trace context it was enqueued with. Resets everything
-- else (attempts, error, parent, dedup) to column defaults.
reEnqueueFromArchiveSQL :: Text -> Text -> Int64 -> Query (JobRead Value)
reEnqueueFromArchiveSQL schema tableName archiveId =
  let archiveTbl = jobQueueArchiveTable schema tableName
      tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
      carried = enqueuedAgainCols
   in rows
        (jobRowCodec tableName)
        [sql|
          INSERT INTO ${tbl} (${carried})
          SELECT ${carried}
          FROM ${archiveTbl}
          WHERE id = #{archiveId :: CInt8}
          RETURNING ${columns}
        |]

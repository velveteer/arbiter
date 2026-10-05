{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
--
-- Completed-job archive SQL templates.
module Arbiter.Core.Sql.Archive
  ( archiveAckCte
  , updateArchiveResultSQL
  , updateArchiveResultsBatchSQL
  , purgeArchiveSQL
  , listArchiveFilteredSQL
  , countArchiveFilteredSQL
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

import Arbiter.Core.Codec (archiveRowCodec, codecColumns, jobRowCodec, joinColumns)
import Arbiter.Core.Job.Schema (SchemaName, TableName, jobQueueArchiveTable, jobQueueTable)
import Arbiter.Core.Job.Types (JobRead, Stored)
import Arbiter.Core.Sql.Insert (RowEdit (..), editJoin)
import Arbiter.Core.Sql.Jobs (enqueuedAgainCols, enqueuedAgainValsEditing, jobColsExceptId, jobColumns)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)

-- | The archive read columns, in codec order. The archive uses @job_id@ for the
-- main-table @id@.
allArchiveColumns :: Text
allArchiveColumns = joinColumns (codecColumns (archiveRowCodec ""))

-- | The @archived@ CTE teeing rows from the named @ack@ CTE into the archive, per-row
-- on @archive_for@. @archive_expires_at@ is precomputed.
-- The fragment ends with a comma, so another CTE must follow it.
archiveAckCte :: SchemaName -> TableName -> Text -> Text
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
updateArchiveResultSQL :: SchemaName -> TableName -> Value -> Int64 -> Query ()
updateArchiveResultSQL schema tableName result jobId =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|UPDATE ${archiveTbl} SET result = #{result :: CJsonb} WHERE job_id = #{jobId :: CInt8}|]

-- | 'updateArchiveResultSQL' for several jobs in one statement.
updateArchiveResultsBatchSQL :: SchemaName -> TableName -> [Int64] -> [Value] -> Query ()
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

-- | Delete a bounded batch of archived jobs whose per-row @archive_expires_at@ has passed.
purgeArchiveSQL :: SchemaName -> TableName -> Text
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
  :: SchemaName
  -> TableName
  -> Query ()
  -> Text
  -> Int64
  -> Int64
  -> Query (Int64, UTCTime, JobRead (Stored payload), Maybe Value)
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
countArchiveFilteredSQL :: SchemaName -> TableName -> Query () -> Query Int64
countArchiveFilteredSQL schema tableName whereFrag =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|SELECT COUNT(*) AS @{count :: CInt8} FROM ${archiveTbl} ${whereFrag}|]

-- | Delete archived jobs by archive primary key.
deleteArchiveJobsBatchSQL :: SchemaName -> TableName -> [Int64] -> Query ()
deleteArchiveJobsBatchSQL schema tableName archiveIds =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [sql|DELETE FROM ${archiveTbl} WHERE id = ANY(#{archiveIds :: [CInt8]})|]

-- | Re-enqueue an archived job as a fresh standalone job, keeping the archive
-- row. Carries 'enqueuedAgainCols' and resets the other columns to their defaults.
-- An @edit@ replaces its columns.
reEnqueueFromArchiveSQL :: SchemaName -> TableName -> Int64 -> Maybe RowEdit -> Query (JobRead (Stored payload))
reEnqueueFromArchiveSQL schema tableName archiveId edit =
  let archiveTbl = jobQueueArchiveTable schema tableName
      tbl = jobQueueTable schema tableName
      joinedEdit = editJoin edit
      vals = enqueuedAgainValsEditing (foldMap editColumns edit) ("edit." <>)
   in rows
        (jobRowCodec tableName)
        [sql|
          INSERT INTO ${tbl} (${enqueuedAgainCols})
          SELECT ${vals}
          FROM ${archiveTbl} ${joinedEdit}
          WHERE id = #{archiveId :: CInt8}
          RETURNING ${jobColumns}
        |]

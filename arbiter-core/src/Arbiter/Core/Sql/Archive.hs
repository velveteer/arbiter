{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | Completed-job archive SQL templates.
module Arbiter.Core.Sql.Archive
  ( archiveAckCte
  , updateArchiveResultSQL
  , purgeArchiveSQL
  , archivePurgeBatch
  , listArchiveFilteredSQL
  , countArchiveFilteredSQL
  , deleteArchiveJobSQL
  , deleteArchiveJobsBatchSQL
  , reEnqueueFromArchiveSQL
  , allArchiveColumns
  ) where

import Data.Text (Text)
import Data.Text qualified as T
import NeatInterpolation (text)

import Arbiter.Core.Codec (archiveRowCodec, codecColumns)
import Arbiter.Core.Job.Schema (jobQueueArchiveTable, jobQueueTable)
import Arbiter.Core.Sql.Jobs (jobColsExceptId, jobColumns)

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
-- the job was not archived (no row for @job_id@). Parameters: result, job_id
updateArchiveResultSQL :: Text -> Text -> Text
updateArchiveResultSQL schema tableName =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [text|UPDATE ${archiveTbl} SET result = ? WHERE job_id = ?|]

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

-- | Generic SQL for listing archived jobs with a dynamic WHERE clause.
listArchiveFilteredSQL :: Text -> Text -> Text -> Text -> Text
listArchiveFilteredSQL schema tableName whereClause orderBy =
  let archiveTbl = jobQueueArchiveTable schema tableName
      columns = T.intercalate ", " allArchiveColumns
   in [text|
        SELECT ${columns}
        FROM ${archiveTbl}
        ${whereClause}
        ORDER BY ${orderBy}
        LIMIT ? OFFSET ?
      |]

-- | Generic SQL for counting archived jobs with a dynamic WHERE clause.
countArchiveFilteredSQL :: Text -> Text -> Text -> Text
countArchiveFilteredSQL schema tableName whereClause =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [text|SELECT COUNT(*) FROM ${archiveTbl} ${whereClause}|]

-- | Delete one archived job by its archive primary key. Parameters: id
deleteArchiveJobSQL :: Text -> Text -> Text
deleteArchiveJobSQL schema tableName =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [text|DELETE FROM ${archiveTbl} WHERE id = ?|]

-- | Delete archived jobs by archive primary key. Parameters: id[]
deleteArchiveJobsBatchSQL :: Text -> Text -> Text
deleteArchiveJobsBatchSQL schema tableName =
  let archiveTbl = jobQueueArchiveTable schema tableName
   in [text|DELETE FROM ${archiveTbl} WHERE id = ANY(?)|]

-- | Re-enqueue an archived job as a fresh standalone job, keeping the archive
-- row. Carries payload, group, priority, max_attempts, admission keys/cost, and
-- archive_for. Resets everything else (attempts, error, parent, dedup) to
-- column defaults. Parameters: id (archive primary key)
reEnqueueFromArchiveSQL :: Text -> Text -> Text
reEnqueueFromArchiveSQL schema tableName =
  let archiveTbl = jobQueueArchiveTable schema tableName
      tbl = jobQueueTable schema tableName
      columns = jobColumns Nothing
   in [text|
        INSERT INTO ${tbl} (payload, group_key, priority, max_attempts, archive_for, rate_limit_key, rate_limit_prefix, rate_limit_cost, concurrency_key, concurrency_prefix)
        SELECT payload, group_key, priority, max_attempts, archive_for, rate_limit_key, rate_limit_prefix, rate_limit_cost, concurrency_key, concurrency_prefix
        FROM ${archiveTbl}
        WHERE id = ?
        RETURNING ${columns}
      |]

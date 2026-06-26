{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot #-}

-- | High-level API for job queue operations.
--
-- Table names are automatically extracted from the payload type using the
-- registry. Compile-time checks ensure payloads are only used with registered tables.
module Arbiter.Core.HighLevel
  ( -- * Constraint Aliases
    QueueOperation
  , JobOperation

    -- * Job Operations
  , insertJob
  , insertJobsBatch
  , insertJobsBatch_
  , claimNextVisibleJobs
  , claimNextVisibleJobsAs
  , claimNextVisibleJobsBatched
  , claimNextVisibleJobsBatchedAs
  , ackJob
  , ackJobsBatch
  , updateJobForRetry
  , nackJob
  , setVisibilityTimeout
  , setVisibilityTimeoutBatch
  , SetVisibilityResult (..)

    -- * Filtered Query Operations
  , Ops.JobFilter (..)
  , listJobsFiltered
  , countJobsFiltered
  , listDLQFiltered
  , countDLQFiltered

    -- * Dead Letter Queue Operations
  , moveToDLQ
  , moveToDLQBatch
  , listDLQJobs
  , retryFromDLQ
  , dlqJobExists
  , deleteDLQJob
  , deleteDLQJobsBatch

    -- * Admin Operations
  , listJobs
  , getJobById
  , getJobsByGroup
  , getJobsByParent
  , cancelJob
  , cancelJobsBatch
  , forceCancelJob
  , promoteJob
  , Ops.QueueStats (..)
  , getQueueStats

    -- * Count Operations
  , countJobs
  , countJobsByGroup
  , countJobsByParent
  , countDLQJobs
  , countChildrenBatch
  , countDLQChildren
  , countDLQChildrenBatch

    -- * Job Dependency Operations
  , pauseChildren
  , resumeChildren
  , cancelJobCascade

    -- * Suspend/Resume Operations
  , suspendJob
  , resumeJob

    -- * Results Table Operations
  , insertResult
  , getResultsByParent
  , getDLQChildErrorsByParent
  , readChildResultsRaw
  , Ops.mergeRawChildResults
  , persistParentState
  , getParentStateSnapshot

    -- * Groups Table Operations
  , refreshAllGroups

    -- * Worker Registry Operations
  , registerWorker
  , heartbeatWorker
  , setWorkerPaused
  , markWorkerShuttingDown
  , deregisterWorker
  , listWorkers
  , sweepStaleWorkers
  , WorkerRow (..)

    -- * Queue Operations
  , ensureQueue
  , setQueuePaused
  , getQueue
  , listQueues
  , QueueRow (..)

    -- * Global Gate
  , runGated

    -- * Job Tree DSL
  , insertJobTree

    -- * Re-exports
  , getSchema
  ) where

import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Data.UUID.Types (UUID)
import GHC.TypeLits (KnownSymbol, symbolVal)
import UnliftIO (MonadUnliftIO)

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema (..))
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Types (Job (..), JobPayload, JobRead, JobWrite)
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables (..), TableForPayload)
import Arbiter.Core.Queues (QueueRow (..))
import Arbiter.Core.Worker (WorkerRow (..))

-- | Constraints for queue operations (requires table name lookup from registry).
type QueueOperation m registry payload =
  ( HasArbiterSchema m registry
  , JobPayload payload
  , KnownSymbol (TableForPayload payload registry)
  , MonadArbiter m
  )

-- | Constraints for job operations (table name stored in job).
type JobOperation m registry payload =
  ( HasArbiterSchema m registry
  , JobPayload payload
  , MonadArbiter m
  )

-- | Insert a job. Returns the inserted job, or @Nothing@ if skipped by dedup
-- ('IgnoreDuplicate') or if @parentId@ references a non-existent job.
insertJob
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => JobWrite payload
  -> m (Maybe (JobRead payload))
insertJob job = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.insertJob schemaName tableName job

-- | Insert multiple jobs in one round-trip. Returns only the jobs that were
-- actually inserted (dedup'd jobs are excluded). Does not validate @parentId@ -
-- use 'insertJobTree' for parent-child relationships.
insertJobsBatch
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [JobWrite payload]
  -> m [JobRead payload]
insertJobsBatch jobs = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.insertJobsBatch schemaName tableName jobs

-- | Like 'insertJobsBatch' but returns only the count of inserted rows.
insertJobsBatch_
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [JobWrite payload]
  -> m Int64
insertJobsBatch_ jobs = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.insertJobsBatch_ schemaName tableName jobs

-- | Claim visible jobs (at most one per group). May return fewer than the
-- limit if groups are exhausted.
claimNextVisibleJobs
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int
  -- ^ Maximum number of jobs to claim.
  -> NominalDiffTime
  -- ^ How long the claimed jobs should remain invisible (in seconds).
  -> m [JobRead payload]
claimNextVisibleJobs limit timeout = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.claimNextVisibleJobs schemaName tableName limit timeout

-- | Variant of 'claimNextVisibleJobs' that stamps @claimed_by@ on every claimed
-- row. Used by the worker dispatcher for claim attribution.
claimNextVisibleJobsAs
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int
  -> NominalDiffTime
  -> UUID
  -- ^ Worker UUID stamped on each claimed row.
  -> m [JobRead payload]
claimNextVisibleJobsAs limit timeout workerId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.claimNextVisibleJobsAs schemaName tableName limit timeout workerId

-- | Claims multiple jobs per group. Unlike 'claimNextVisibleJobs', this can
-- claim up to @batchSize@ jobs from each group while still respecting
-- per-group ordering between batches.
claimNextVisibleJobsBatched
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int
  -- ^ Batch size: maximum number of jobs to claim per group.
  -> Int
  -- ^ Max groups: maximum number of groups/batches to claim.
  -> NominalDiffTime
  -- ^ How long the claimed jobs should remain invisible (in seconds).
  -> m [NonEmpty (JobRead payload)]
claimNextVisibleJobsBatched batchSize maxGroups timeout = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.claimNextVisibleJobsBatched schemaName tableName batchSize maxGroups timeout

-- | Variant of 'claimNextVisibleJobsBatched' that stamps @claimed_by@ on every
-- claimed row.
claimNextVisibleJobsBatchedAs
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int
  -> Int
  -> NominalDiffTime
  -> UUID
  -- ^ Worker UUID stamped on each claimed row.
  -> m [NonEmpty (JobRead payload)]
claimNextVisibleJobsBatchedAs batchSize maxGroups timeout workerId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.claimNextVisibleJobsBatchedAs schemaName tableName batchSize maxGroups timeout workerId

-- | Acknowledge a job as complete. Deletes it from the queue, or suspends it
-- if it's a parent with unfinished children. Returns 1 on success, 0 if gone.
ackJob
  :: forall m registry payload
   . (JobOperation m registry payload)
  => JobRead payload
  -> m Int64
ackJob job = do
  schemaName <- getSchema
  let tableName = job.queueName
  Ops.ackJob schemaName tableName job

-- | Acknowledges multiple jobs as complete in one statement (parent-aware:
-- finalizers suspend, parents wake). All jobs must be from the same queue.
-- Returns the ids actually acked. Reclaimed jobs are absent.
ackJobsBatch
  :: forall m registry payload
   . (JobOperation m registry payload)
  => [JobRead payload]
  -> m [Int64]
ackJobsBatch [] = pure []
ackJobsBatch jobs@(firstJob : _) = do
  schemaName <- getSchema
  let tableName = firstJob.queueName
  Ops.ackJobsBatch schemaName tableName jobs

-- | Marks a failed job for retry at a later time.
--
-- Returns the number of rows updated (0 if job was already claimed by another worker).
updateJobForRetry
  :: forall m registry payload
   . (JobOperation m registry payload)
  => NominalDiffTime
  -- ^ The delay before this job becomes visible again for retry.
  -> Text
  -- ^ An error message to store with the job.
  -> JobRead payload
  -> m Int64
updateJobForRetry delay errorMsg job = do
  schemaName <- getSchema
  let tableName = job.queueName
  Ops.updateJobForRetry schemaName tableName delay errorMsg job

-- | Soft-nack a job so it is reprocessed after its visibility timeout without
-- recording a failure or consuming a retry attempt.
--
-- Returns the number of rows updated (0 if job was already claimed by another worker).
nackJob
  :: forall m registry payload
   . (JobOperation m registry payload)
  => JobRead payload
  -> m Int64
nackJob job = do
  schemaName <- getSchema
  let tableName = job.queueName
  Ops.nackJob schemaName tableName job

-- | Manually extends a job's visibility timeout, useful for long-running jobs.
--
-- Returns the number of rows updated (0 if job was already reclaimed by another worker).
setVisibilityTimeout
  :: forall m registry payload
   . (JobOperation m registry payload)
  => NominalDiffTime
  -- ^ The new visibility timeout (in seconds) from the current time.
  -> JobRead payload
  -> m Int64
setVisibilityTimeout timeout job = do
  schemaName <- getSchema
  let tableName = job.queueName
  Ops.setVisibilityTimeout schemaName tableName timeout job

-- | Result of setting visibility timeout for a single job in a batch.
data SetVisibilityResult
  = -- | Visibility timeout was successfully extended. Contains job ID.
    VisibilityExtended Int64
  | -- | Job no longer exists (was deleted/acked). Contains job ID.
    JobGone Int64
  | -- | Job was reclaimed by another worker (attempts count changed).
    -- Contains: job ID, expected attempts, actual attempts.
    JobReclaimed Int64 Int32 Int32
  deriving stock (Eq, Show)

-- | Extends visibility timeout for multiple jobs. All jobs must be from
-- the same queue.
setVisibilityTimeoutBatch
  :: forall m registry payload
   . (JobOperation m registry payload)
  => NominalDiffTime
  -- ^ The new visibility timeout (in seconds) from the current time.
  -> [JobRead payload]
  -- ^ Jobs to heartbeat (all must be from the same queue)
  -> m [SetVisibilityResult]
setVisibilityTimeoutBatch _ [] = pure []
setVisibilityTimeoutBatch timeout jobs@(firstJob : _) = do
  schemaName <- getSchema
  let tableName = firstJob.queueName
  infos <- Ops.setVisibilityTimeoutBatch schemaName tableName timeout jobs
  let jobMap = Map.fromList [(primaryKey j, j) | j <- jobs]
      toResult info = case info of
        Ops.VisibilityUpdateInfo jobId True _ -> VisibilityExtended jobId
        Ops.VisibilityUpdateInfo jobId False Nothing -> JobGone jobId
        Ops.VisibilityUpdateInfo jobId False (Just actual) ->
          let jobAttempts = maybe 0 attempts (Map.lookup jobId jobMap)
           in JobReclaimed jobId jobAttempts actual
  pure $ map toResult infos

-- | Move a job to the DLQ. Returns 0 if already claimed by another worker.
moveToDLQ
  :: forall m registry payload
   . (JobOperation m registry payload)
  => Text
  -- ^ Error message (the final error that caused the DLQ move)
  -> JobRead payload
  -> m Int64
moveToDLQ errorMsg job = do
  schemaName <- getSchema
  let tableName = job.queueName
  Ops.moveToDLQ schemaName tableName errorMsg job

-- | Lists jobs in the dead-letter queue with pagination.
listDLQJobs
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int
  -- ^ The maximum number of jobs to return.
  -> Int
  -- ^ The number of jobs to skip (for pagination).
  -> m [DLQ.DLQJob payload]
listDLQJobs limit offset = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.listDLQJobs schemaName tableName limit offset

-- | Retry a DLQ job (re-insert into main queue with attempts reset).
-- Returns @Nothing@ if the DLQ job no longer exists.
retryFromDLQ
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ DLQ job ID
  -> m (Maybe (JobRead payload))
retryFromDLQ dlqId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.retryFromDLQ schemaName tableName dlqId

-- | Check whether a DLQ job exists by ID.
dlqJobExists
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -> m Bool
dlqJobExists dlqId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.dlqJobExists schemaName tableName dlqId

-- | Permanently deletes a job from the dead-letter queue.
--
-- Returns the number of rows deleted (0 if the DLQ job no longer exists).
deleteDLQJob
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ DLQ job ID
  -> m Int64
deleteDLQJob dlqId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.deleteDLQJob schemaName tableName dlqId

-- | Move multiple jobs to the DLQ. Jobs already claimed by another worker are
-- skipped. Returns the count of jobs moved.
moveToDLQBatch
  :: forall m registry payload
   . (JobOperation m registry payload)
  => [(JobRead payload, Text)]
  -- ^ List of (job, error message) pairs. All jobs must be from the same queue.
  -> m Int64
moveToDLQBatch [] = pure 0
moveToDLQBatch jobsWithErrors@((firstJob, _) : _) = do
  schemaName <- getSchema
  let tableName = firstJob.queueName
  Ops.moveToDLQBatch schemaName tableName jobsWithErrors

-- | Permanently deletes multiple jobs from the dead-letter queue.
--
-- Returns the total number of DLQ jobs deleted.
deleteDLQJobsBatch
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [Int64]
  -- ^ DLQ job IDs
  -> m Int64
deleteDLQJobsBatch dlqIds = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.deleteDLQJobsBatch schemaName tableName dlqIds

-- ---------------------------------------------------------------------------
-- Filtered Query Operations
-- ---------------------------------------------------------------------------

-- | Lists jobs with composable filters.
--
-- Returns jobs ordered by ID (descending, newest first).
listJobsFiltered
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [Ops.JobFilter]
  -- ^ Composable filters
  -> Int
  -- ^ Maximum number of jobs to return.
  -> Int
  -- ^ Number of jobs to skip (for pagination).
  -> m [JobRead payload]
listJobsFiltered filters limit offset = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.listJobsFiltered schemaName tableName filters limit offset

-- | Counts jobs with composable filters.
countJobsFiltered
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [Ops.JobFilter]
  -- ^ Composable filters
  -> m Int64
countJobsFiltered filters = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.countJobsFiltered schemaName tableName filters

-- | Lists DLQ jobs with composable filters.
--
-- Returns jobs ordered by failed_at (most recent first).
listDLQFiltered
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [Ops.JobFilter]
  -- ^ Composable filters
  -> Int
  -- ^ Maximum number of jobs to return.
  -> Int
  -- ^ Number of jobs to skip (for pagination).
  -> m [DLQ.DLQJob payload]
listDLQFiltered filters limit offset = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.listDLQFiltered schemaName tableName filters limit offset

-- | Counts DLQ jobs with composable filters.
countDLQFiltered
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [Ops.JobFilter]
  -- ^ Composable filters
  -> m Int64
countDLQFiltered filters = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.countDLQFiltered schemaName tableName filters

-- ---------------------------------------------------------------------------
-- Admin Operations
-- ---------------------------------------------------------------------------

-- | Lists jobs in the queue with pagination.
--
-- Returns jobs ordered by ID (descending, newest first).
listJobs
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int
  -- ^ Maximum number of jobs to return.
  -> Int
  -- ^ Number of jobs to skip (for pagination).
  -> m [JobRead payload]
listJobs limit offset = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.listJobs schemaName tableName limit offset

-- | Gets a single job by its ID.
--
-- Returns @Nothing@ if the job doesn't exist.
getJobById
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Job ID
  -> m (Maybe (JobRead payload))
getJobById jobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.getJobById schemaName tableName jobId

-- | Gets all jobs for a specific group key with pagination.
getJobsByGroup
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Text
  -- ^ Group key to filter by
  -> Int
  -- ^ Maximum number of jobs to return.
  -> Int
  -- ^ Number of jobs to skip (for pagination).
  -> m [JobRead payload]
getJobsByGroup groupKey limit offset = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.getJobsByGroup schemaName tableName groupKey limit offset

-- | Gets all jobs for a specific parent ID with pagination.
getJobsByParent
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Parent ID to filter by
  -> Int
  -- ^ Maximum number of jobs to return.
  -> Int
  -- ^ Number of jobs to skip (for pagination).
  -> m [JobRead payload]
getJobsByParent pid limit offset = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.getJobsByParent schemaName tableName pid limit offset

-- | Cancels (deletes) a job by ID.
--
-- Returns 0 if the job has children - use 'cancelJobCascade' to delete
-- a parent and all its descendants.
cancelJob
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Job ID
  -> m Int64
cancelJob jobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.cancelJob schemaName tableName jobId

-- | Force-cancel a job and its descendants, also interrupting any handlers
-- currently running the deleted jobs via NOTIFY on the cancel channel.
forceCancelJob
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Root job ID
  -> m Int64
forceCancelJob jobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.forceCancelJob schemaName tableName jobId

-- | Cancels (deletes) multiple jobs by ID.
--
-- Returns the total number of jobs deleted.
cancelJobsBatch
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [Int64]
  -- ^ Job IDs
  -> m Int64
cancelJobsBatch jobIds = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.cancelJobsBatch schemaName tableName jobIds

-- | Promote a delayed or retrying job to be immediately visible.
--
-- Refuses in-flight jobs (attempts > 0 with no last_error).
-- Returns 1 on success, 0 if not found, already visible, or in-flight.
promoteJob
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Job ID
  -> m Int64
promoteJob jobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.promoteJob schemaName tableName jobId

-- | Gets statistics about the job queue.
getQueueStats
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => m Ops.QueueStats
getQueueStats = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.getQueueStats schemaName tableName

-- ---------------------------------------------------------------------------
-- Count Operations
-- ---------------------------------------------------------------------------

-- | Counts all jobs in the queue.
countJobs
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => m Int64
countJobs = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.countJobs schemaName tableName

-- | Counts jobs matching a group key.
countJobsByGroup
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Text
  -- ^ Group key to count
  -> m Int64
countJobsByGroup groupKey = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.countJobsByGroup schemaName tableName groupKey

-- | Counts jobs matching a parent ID.
countJobsByParent
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Parent ID to count children of
  -> m Int64
countJobsByParent pid = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.countJobsByParent schemaName tableName pid

-- | Counts jobs in the dead-letter queue.
countDLQJobs
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => m Int64
countDLQJobs = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.countDLQJobs schemaName tableName

-- | Counts children for a batch of potential parent IDs.
--
-- Returns a Map from parent_id to @(total, paused)@ counts (only non-zero entries).
countChildrenBatch
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [Int64]
  -> m (Map Int64 (Int64, Int64))
countChildrenBatch ids = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.countChildrenBatch schemaName tableName ids

-- | Count how many children of a parent are in the DLQ.
-- Useful inside finalizer handlers to detect failed children.
countDLQChildren
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Parent job ID
  -> m Int64
countDLQChildren parentJobId = do
  m <- countDLQChildrenBatch @m @registry @payload [parentJobId]
  pure $ fromMaybe 0 (Map.lookup parentJobId m)

-- | Counts children in the DLQ for a batch of potential parent IDs.
--
-- Returns a Map from parent_id to DLQ child count (only non-zero entries).
countDLQChildrenBatch
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [Int64]
  -> m (Map Int64 Int64)
countDLQChildrenBatch ids = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.countDLQChildrenBatch schemaName tableName ids

-- ---------------------------------------------------------------------------
-- Job Dependency Operations
-- ---------------------------------------------------------------------------

-- | Pause all visible children of a parent job, making them unclaimable.
--
-- Only affects children that are currently claimable. In-flight children
-- are left alone so their visibility timeout can expire normally if the
-- worker crashes.
--
-- Returns the number of children paused.
pauseChildren
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Parent job ID
  -> m Int64
pauseChildren parentJobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.pauseChildren schemaName tableName parentJobId

-- | Resume all suspended children of a parent job.
--
-- Returns the number of children resumed.
resumeChildren
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Parent job ID
  -> m Int64
resumeChildren parentJobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.resumeChildren schemaName tableName parentJobId

-- | Cancel a job and all its descendants recursively.
--
-- Returns the total number of jobs deleted (parent + all descendants).
cancelJobCascade
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Root job ID
  -> m Int64
cancelJobCascade jobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.cancelJobCascade schemaName tableName jobId

-- ---------------------------------------------------------------------------
-- Suspend/Resume Operations
-- ---------------------------------------------------------------------------

-- | Suspend a job, making it unclaimable.
--
-- Only suspends non-in-flight jobs (not currently being processed by workers).
-- Returns the number of rows updated (0 if job doesn't exist, is in-flight,
-- or already suspended).
suspendJob
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Job ID
  -> m Int64
suspendJob jobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.suspendJob schemaName tableName jobId

-- | Resume a suspended job, making it claimable again.
--
-- Returns the number of rows updated (0 if job doesn't exist or isn't suspended).
resumeJob
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Job ID
  -> m Int64
resumeJob jobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.resumeJob schemaName tableName jobId

-- ---------------------------------------------------------------------------
-- Results Table Operations
-- ---------------------------------------------------------------------------

-- | Insert a child's result into the results table.
--
-- Each child gets its own row keyed by @(parent_id, child_id)@.
-- The FK @ON DELETE CASCADE@ ensures cleanup when the parent is acked.
--
-- Returns the number of rows inserted (1 on success).
insertResult
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Parent job ID
  -> Int64
  -- ^ Child job ID
  -> Value
  -- ^ Encoded result value
  -> m Int64
insertResult parentJobId childId result = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.insertResult schemaName tableName parentJobId childId result

-- | Get all child results for a parent from the results table.
getResultsByParent
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Parent job ID
  -> m (Map Int64 Value)
getResultsByParent parentJobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.getResultsByParent schemaName tableName parentJobId

-- | Get DLQ child errors for a parent.
--
-- Returns a 'Map' from child job ID to the last error message.
getDLQChildErrorsByParent
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Parent job ID
  -> m (Map Int64 Text)
getDLQChildErrorsByParent parentJobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.getDLQChildErrorsByParent schemaName tableName parentJobId

-- | Read all child data for a rollup finalizer. Returns
-- @(childId->result, childId->error, parentStateSnapshot, dlqPK->error)@.
readChildResultsRaw
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -- ^ Parent job ID
  -> m (Map Int64 Value, Map Int64 Text, Maybe Value, Map Int64 Text)
readChildResultsRaw parentJobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.readChildResultsRaw schemaName tableName parentJobId

-- | Snapshot results into @parent_state@ before DLQ move.
persistParentState
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -> Value
  -> m Int64
persistParentState jobId state = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.persistParentState schemaName tableName jobId state

-- | Read raw @parent_state@ snapshot from the DB.
getParentStateSnapshot
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int64
  -> m (Maybe Value)
getParentStateSnapshot jobId = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.getParentStateSnapshot schemaName tableName jobId

-- ---------------------------------------------------------------------------
-- Groups Table Operations
-- ---------------------------------------------------------------------------

-- | Schema-wide groups-table refresh. Iterates all registered queues and
-- corrects drift in @job_count@, @min_priority@, @min_id@, and
-- @in_flight_until@ for each. Returns the queue names that failed.
--
-- Intended for the reaper loop, which wraps it in 'Ops.runGated' so only
-- one pool runs it per interval.
refreshAllGroups
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m, MonadUnliftIO m, RegistryTables registry)
  => m [Text]
refreshAllGroups = do
  schemaName <- getSchema
  Ops.refreshAllGroups schemaName (registryTableNames (Proxy @registry))

-- ---------------------------------------------------------------------------
-- Worker Registry Operations
-- ---------------------------------------------------------------------------

-- | Register a worker pool. See 'Ops.registerWorker'.
registerWorker
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => UUID
  -> Text
  -- ^ Queue name
  -> Maybe Text
  -- ^ Host name
  -> Maybe Int32
  -- ^ Worker thread count
  -> NominalDiffTime
  -- ^ Stale threshold seconds
  -> Maybe Value
  -- ^ Extra JSONB metadata
  -> m (Maybe Bool)
registerWorker workerId queue host threads staleThreshold metadata = do
  schemaName <- getSchema
  Ops.registerWorker schemaName workerId queue host threads staleThreshold metadata

-- | Bump a worker's heartbeat. See 'Ops.heartbeatWorker'.
heartbeatWorker
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => UUID
  -> m (Maybe Bool)
heartbeatWorker workerId = do
  schemaName <- getSchema
  Ops.heartbeatWorker schemaName workerId

-- | Set the @paused@ flag for a registered worker.
setWorkerPaused
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => UUID
  -> Bool
  -> m Int64
setWorkerPaused workerId p = do
  schemaName <- getSchema
  Ops.setWorkerPaused schemaName workerId p

-- | Mark a worker as gracefully draining.
markWorkerShuttingDown
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => UUID
  -> m Int64
markWorkerShuttingDown workerId = do
  schemaName <- getSchema
  Ops.markWorkerShuttingDown schemaName workerId

-- | Remove a worker row.
deregisterWorker
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => UUID
  -> m Int64
deregisterWorker workerId = do
  schemaName <- getSchema
  Ops.deregisterWorker schemaName workerId

-- | List workers, optionally scoped to a queue and a heartbeat-age threshold.
listWorkers
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => Maybe Text
  -- ^ Queue name. 'Nothing' returns workers from all queues.
  -> Maybe NominalDiffTime
  -- ^ Liveness threshold in seconds. 'Nothing' returns workers regardless of heartbeat age.
  -> m [WorkerRow]
listWorkers mQueue mLiveSecs = do
  schemaName <- getSchema
  Ops.listWorkers schemaName mQueue mLiveSecs

-- | Delete worker rows older than each row's own @stale_threshold_secs@.
sweepStaleWorkers
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => m Int64
sweepStaleWorkers = do
  schemaName <- getSchema
  Ops.sweepStaleWorkers schemaName

-- ---------------------------------------------------------------------------
-- Queue Operations
-- ---------------------------------------------------------------------------

-- | Insert an @arbiter_queues@ row with defaults if one doesn't already exist.
ensureQueue
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => Text
  -- ^ Queue name
  -> m Int64
ensureQueue queue = do
  schemaName <- getSchema
  Ops.ensureQueue schemaName queue

-- | Set the queue's @paused@ flag. Fans out a NOTIFY to each registered worker
-- on the queue.
setQueuePaused
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => Text
  -- ^ Queue name
  -> Bool
  -> m Int64
setQueuePaused queue p = do
  schemaName <- getSchema
  Ops.setQueuePaused schemaName queue p

-- | Get the queue's row. 'Nothing' if absent.
getQueue
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => Text
  -- ^ Queue name
  -> m (Maybe QueueRow)
getQueue queue = do
  schemaName <- getSchema
  Ops.getQueue schemaName queue

-- | List all queues registered in this schema.
listQueues
  :: forall m registry
   . (HasArbiterSchema m registry, MonadArbiter m)
  => m [QueueRow]
listQueues = do
  schemaName <- getSchema
  Ops.listQueues schemaName

-- ---------------------------------------------------------------------------
-- Global Gate
-- ---------------------------------------------------------------------------

-- | Run @work@ at most once per @interval@ across every worker pool sharing
-- the same schema. See 'Ops.runGated'.
runGated
  :: forall m registry a
   . (HasArbiterSchema m registry, MonadArbiter m)
  => Text
  -- ^ Task identifier
  -> NominalDiffTime
  -> m a
  -> m (Maybe a)
runGated task interval work = do
  schemaName <- getSchema
  Ops.runGated schemaName task interval work

-- ---------------------------------------------------------------------------
-- Job Tree DSL
-- ---------------------------------------------------------------------------

-- | Insert a 'JT.JobTree' atomically. Returns all inserted jobs (pre-order),
-- or @Left@ if the root has a dedup conflict. Rolls back on any failure.
insertJobTree
  :: forall m registry payload
   . (MonadUnliftIO m, QueueOperation m registry payload)
  => JT.JobTree payload
  -> m (Either Text (NonEmpty (JobRead payload)))
insertJobTree tree = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  JT.insertJobTree schemaName tableName tree

{-# LANGUAGE AllowAmbiguousTypes #-}
{-# OPTIONS_GHC -Wno-redundant-constraints #-}

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
  , claimNextVisibleJobsBatched
  , ackJob
  , ackJobsBatch
  , ackJobsBulk
  , updateJobForRetry
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
  , getInFlightJobs
  , cancelJob
  , cancelJobsBatch
  , promoteJob
  , Ops.QueueStats (..)
  , getQueueStats

    -- * Count Operations
  , countJobs
  , countJobsByGroup
  , countJobsByParent
  , countInFlightJobs
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
  , refreshGroups

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
import GHC.TypeLits (KnownSymbol, symbolVal)
import UnliftIO (MonadUnliftIO)

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema (..))
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Types (Job (..), JobPayload, JobRead, JobWrite)
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (TableForPayload)

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

-- | Inserts a job into the queue.
--
-- Returns @Nothing@ if a job with the same deduplication key already
-- exists (with @IgnoreDuplicate@ strategy).
insertJob
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => JobWrite payload
  -> m (Maybe (JobRead payload))
insertJob job = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.insertJob schemaName tableName job

-- | Insert multiple jobs in a single batch operation.
--
-- Supports dedup keys: within the batch, duplicate keys are resolved
-- (last 'ReplaceDuplicate' wins), and against existing rows via
-- @ON CONFLICT@.
--
-- Does not validate @parentId@. Use @insertJobTree@ for parent-child relationships.
insertJobsBatch
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [JobWrite payload]
  -> m [JobRead payload]
insertJobsBatch jobs = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.insertJobsBatch schemaName tableName jobs

insertJobsBatch_
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => [JobWrite payload]
  -> m Int64
insertJobsBatch_ jobs = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.insertJobsBatch_ schemaName tableName jobs

-- | Claims visible jobs from the queue. At most one job per group is claimed
-- to enforce head-of-line blocking.
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

-- | Claims multiple jobs per group. Unlike 'claimNextVisibleJobs', this can
-- claim up to @batchSize@ jobs from each group while still respecting
-- head-of-line blocking between batches.
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

-- | Acknowledges a job as complete, permanently deleting it from the queue.
--
-- Returns 1 on success (job deleted or parent suspended), 0 if already gone.
ackJob
  :: forall m registry payload
   . (JobOperation m registry payload)
  => JobRead payload
  -> m Int64
ackJob job = do
  schemaName <- getSchema
  let tableName = queueName job
  Ops.ackJob schemaName tableName job

-- | Acknowledges multiple jobs as complete. All jobs must be from the same
-- queue. Returns the total number of rows deleted.
ackJobsBatch
  :: forall m registry payload
   . (JobOperation m registry payload)
  => [JobRead payload]
  -> m Int64
ackJobsBatch [] = pure 0
ackJobsBatch jobs@(firstJob : _) = do
  schemaName <- getSchema
  let tableName = queueName firstJob
  Ops.ackJobsBatch schemaName tableName jobs

-- | Bulk ack for standalone jobs (no parent, no tree logic).
--
-- A single DELETE with unnest — one round trip for N jobs.
-- Only valid for jobs claimed in 'BatchedJobsMode'.
--
-- Returns the number of rows deleted.
ackJobsBulk
  :: forall m registry payload
   . (JobOperation m registry payload)
  => [JobRead payload]
  -> m Int64
ackJobsBulk [] = pure 0
ackJobsBulk jobs@(firstJob : _) = do
  schemaName <- getSchema
  let tableName = queueName firstJob
  Ops.ackJobsBulk schemaName tableName jobs

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
  let tableName = queueName job
  Ops.updateJobForRetry schemaName tableName delay errorMsg job

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
  let tableName = queueName job
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
  let tableName = queueName firstJob
  infos <- Ops.setVisibilityTimeoutBatch schemaName tableName timeout jobs
  let jobMap = Map.fromList [(primaryKey j, j) | j <- jobs]
      toResult info = case info of
        Ops.VisibilityUpdateInfo jobId True _ -> VisibilityExtended jobId
        Ops.VisibilityUpdateInfo jobId False Nothing -> JobGone jobId
        Ops.VisibilityUpdateInfo jobId False (Just actual) ->
          let jobAttempts = maybe 0 attempts (Map.lookup jobId jobMap)
           in JobReclaimed jobId jobAttempts actual
  pure $ map toResult infos

-- | Moves a job from the main queue to the dead-letter queue (DLQ).
--
-- Returns the number of rows deleted from main queue (0 if job was already
-- claimed by another worker).
moveToDLQ
  :: forall m registry payload
   . (JobOperation m registry payload)
  => Text
  -- ^ Error message (the final error that caused the DLQ move)
  -> JobRead payload
  -> m Int64
moveToDLQ errorMsg job = do
  schemaName <- getSchema
  let tableName = queueName job
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

-- | Moves a job from the dead-letter queue back into the main queue to be retried.
--
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

-- | Moves multiple jobs to the dead-letter queue.
--
-- Each job is moved with its own error message. Jobs that have already been
-- claimed by another worker (attempts mismatch) are silently skipped.
--
-- Returns the total number of jobs moved to DLQ.
moveToDLQBatch
  :: forall m registry payload
   . (JobOperation m registry payload)
  => [(JobRead payload, Text)]
  -- ^ List of (job, error message) pairs. All jobs must be from the same queue.
  -> m Int64
moveToDLQBatch [] = pure 0
moveToDLQBatch jobsWithErrors@((firstJob, _) : _) = do
  schemaName <- getSchema
  let tableName = queueName firstJob
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
--
-- Useful for debugging or admin UI to see all jobs for a specific entity.
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

-- | Gets all in-flight jobs (currently being processed by workers).
--
-- A job is considered in-flight if it has been claimed (attempts > 0) and
-- its visibility timeout hasn't expired yet.
--
-- Useful for monitoring active work and detecting stuck jobs.
getInFlightJobs
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int
  -- ^ Maximum number of jobs to return.
  -> Int
  -- ^ Number of jobs to skip (for pagination).
  -> m [JobRead payload]
getInFlightJobs limit offset = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.getInFlightJobs schemaName tableName limit offset

-- | Cancels (deletes) a job by ID.
--
-- Returns 0 if the job has children — use 'cancelJobCascade' to delete
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

-- | Counts in-flight jobs (currently being processed by workers).
countInFlightJobs
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => m Int64
countInFlightJobs = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.countInFlightJobs schemaName tableName

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

-- | Read child results, DLQ errors, parent_state snapshot, and DLQ failures
-- for a rollup finalizer in a single query.
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

-- | Full recompute of the groups table from the main queue.
--
-- Corrects any drift in job_count, min_priority, min_id, and in_flight_until.
refreshGroups
  :: forall m registry payload
   . (QueueOperation m registry payload)
  => Int
  -- ^ Minimum interval between runs (seconds)
  -> m ()
refreshGroups intervalSecs = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  Ops.refreshGroups schemaName tableName intervalSecs

-- ---------------------------------------------------------------------------
-- Job Tree DSL
-- ---------------------------------------------------------------------------

-- | Insert a 'JobTree' atomically in a single transaction.
--
-- HighLevel wrapper that resolves schema/table from the registry.
-- See 'Arbiter.Core.JobTree.insertJobTree' for details.
insertJobTree
  :: forall m registry payload
   . (MonadUnliftIO m, QueueOperation m registry payload)
  => JT.JobTree payload
  -> m (Either Text (NonEmpty (JobRead payload)))
insertJobTree tree = do
  schemaName <- getSchema
  let tableName = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))
  JT.insertJobTree schemaName tableName tree

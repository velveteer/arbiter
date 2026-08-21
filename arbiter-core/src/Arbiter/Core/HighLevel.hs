{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DuplicateRecordFields #-}

-- | The job queue operations with their table names resolved from the payload type, so a
-- payload can only reach the queue its registry entry declares.
module Arbiter.Core.HighLevel
  ( -- * Constraint Aliases
    QueueOperation
  , JobOperation
  , queueTable
  , RegistryAdmissionPolicies

    -- * Job Operations
  , insertJob
  , insertJobsBatch
  , insertJobsBatch_
  , claimNextVisibleJobs
  , claimNextVisibleJobsAs
  , claimNextVisibleJobsBatched
  , mkClaimSql
  , addRateLimitTokens
  , pruneRateLimitBuckets
  , resetRateLimitBuckets
  , listRateLimitPolicies
  , getRateLimitPolicy
  , rateLimitPolicyExists
  , listRateLimitBuckets
  , listConcurrencyPolicies
  , getConcurrencyPolicy
  , listConcurrencyKeys
  , updateRateLimitPolicyOverrides
  , updateConcurrencyPolicyOverrides
  , pruneConcurrencyKeys
  , reconcileConcurrencyCounts
  , reconcileConcurrencyCountsIfStale
  , reconcileAndPruneConcurrency
  , ackJob
  , ackJobsBatch
  , updateJobForRetry
  , nackJob
  , nackJobsBatch
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
  , listArchiveJobs
  , getArchivedJobById
  , listArchivedJobsByGroupKey
  , deleteArchiveJob
  , deleteArchiveJobsBatch
  , reEnqueueFromArchive
  , retryFromDLQ
  , dlqJobExists
  , deleteDLQJob
  , deleteDLQJobsBatch

    -- * Admin Operations
  , listJobs
  , getJobById
  , jobExists
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
  , insertResultUnsafe
  , getResultsByParent
  , getDLQChildErrorsByParent
  , readChildResultsRaw
  , Ops.mergeRawChildResults
  , persistParentState
  , getParentStateSnapshot

    -- * Groups Table Operations
  , refreshAllGroupsFully

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

    -- * Cron Schedule Operations
  , listCronSchedules
  , getCronScheduleByName
  , updateCronScheduleUnchecked

    -- * Global Gate
  , runGated

    -- * Job Tree DSL
  , insertJobTree

    -- * Re-exports
  , getSchema
  ) where

import Control.Monad (void, when)
import Data.Aeson (Value)
import Data.Int (Int64)
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

import Arbiter.Core.Concurrency.Stats (ConcurrencyKeyView, ConcurrencyPolicyUpdate, ConcurrencyPolicyView)
import Arbiter.Core.CronSchedule (CronScheduleRow, CronScheduleUpdate)
import Arbiter.Core.HighLevel.Runtime
  ( deregisterWorker
  , ensureQueue
  , getQueue
  , heartbeatWorker
  , listQueues
  , listWorkers
  , markWorkerShuttingDown
  , registerWorker
  , setQueuePaused
  , setWorkerPaused
  , sweepStaleWorkers
  )
import Arbiter.Core.Job.Archive qualified as Archive
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Types
  ( ClaimSeq
  , JobId
  , JobPayload
  , JobRead
  , JobWrite
  , RegistryAdmissionPolicies
  , claimSeq
  , claimedBy
  , primaryKey
  )
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobResult (EncodeJobResult, encodeJobResult)
import Arbiter.Core.JobTree qualified as JT
import Arbiter.Core.MonadArbiter (MonadArbiter (..), ResultOf)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables (..), TableForPayload)
import Arbiter.Core.Queues (QueueRow (..))
import Arbiter.Core.RateLimit.Spec (RateLimitKey (..))
import Arbiter.Core.RateLimit.Stats (RateLimitBucketView, RateLimitPolicyUpdate, RateLimitPolicyView)
import Arbiter.Core.Trace (withPublishSpan)
import Arbiter.Core.Worker (WorkerRow (..))

-- | Constraints for queue operations (requires table name lookup from registry).
type QueueOperation m payload =
  ( JobPayload payload
  , KnownSymbol (TableForPayload payload (RegistryOf m))
  , MonadArbiter m
  )

-- | Constraints for job operations (table name stored in job).
type JobOperation m payload =
  ( JobPayload payload
  , MonadArbiter m
  )

-- | The table name @payload@'s entry in this monad's registry declares.
queueTable :: forall payload m. (KnownSymbol (TableForPayload payload (RegistryOf m))) => Text
queueTable = T.pack $ symbolVal (Proxy @(TableForPayload payload (RegistryOf m)))

publishSpan
  :: forall payload m a
   . (KnownSymbol (TableForPayload payload (RegistryOf m)), MonadUnliftIO m)
  => [JobWrite payload]
  -> m a
  -> m a
publishSpan = withPublishSpan (queueTable @payload @m)

-- | Insert a job. Returns the inserted job, or @Nothing@ if skipped by dedup
-- ('Arbiter.Core.Job.Dedup.IgnoreDuplicate').
insertJob
  :: forall payload m
   . (QueueOperation m payload)
  => JobWrite payload
  -> m (Maybe (JobRead payload))
insertJob job = publishSpan @payload [job] $ do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.insertJob schemaName tableName job

-- | Insert multiple jobs in one round-trip. Returns only the jobs that were
-- actually inserted (dedup'd jobs are excluded). Use 'insertJobTree' for
-- parent-child relationships.
insertJobsBatch
  :: forall payload m
   . (QueueOperation m payload)
  => [JobWrite payload]
  -> m [JobRead payload]
insertJobsBatch [] = pure []
insertJobsBatch jobs = publishSpan @payload jobs $ do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.insertJobsBatch schemaName tableName jobs

-- | Like 'insertJobsBatch' but returns only the count of inserted rows.
insertJobsBatch_
  :: forall payload m
   . (QueueOperation m payload)
  => [JobWrite payload]
  -> m Int64
insertJobsBatch_ [] = pure 0
insertJobsBatch_ jobs = publishSpan @payload jobs $ do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.insertJobsBatch_ schemaName tableName jobs

-- | Claim visible jobs, at most one per group, and fewer than the limit once the groups
-- run out. Leaves @claimed_by@ NULL, so no concurrency pool caps this path.
-- 'claimNextVisibleJobsAs' is the capped one.
claimNextVisibleJobs
  :: forall payload m
   . (QueueOperation m payload)
  => Int
  -- ^ Maximum number of jobs to claim.
  -> NominalDiffTime
  -- ^ How long the claimed jobs should remain invisible (in seconds).
  -> m [JobRead payload]
claimNextVisibleJobs limit timeout = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.claimNextVisibleJobs schemaName tableName limit timeout

-- | Add tokens to a key's bucket, capped at max, and wake any of its jobs parked
-- mid-wait. A no-op without a policy.
addRateLimitTokens
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => RateLimitKey
  -> Double
  -> m ()
addRateLimitTokens key amount = withDbTransaction $ do
  schemaName <- getSchema
  Ops.addRateLimitTokens schemaName key amount
  let queues = registryTableNames (Proxy @(RegistryOf m))
  void $ Ops.wakeThrottledJobsForKey schemaName queues key

-- | Delete reclaimable idle (full) buckets. Returns the number pruned. The worker
-- reaper runs this. A full bucket re-seeds at full on next use, so pruning introduces
-- no burst.
pruneRateLimitBuckets
  :: forall m
   . (MonadArbiter m)
  => NominalDiffTime
  -> m Int64
pruneRateLimitBuckets idle = do
  schemaName <- getSchema
  Ops.pruneRateLimitBuckets schemaName idle

-- | Reset every bucket for a prefix to full and wake its throttled jobs. Returns
-- the number of buckets reset. A manual (0-refill) policy plus a cron calling this
-- at the boundary is a fixed window.
resetRateLimitBuckets
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => Text
  -> m Int64
resetRateLimitBuckets prefix = withDbTransaction $ do
  schemaName <- getSchema
  n <- Ops.resetRateLimitBuckets schemaName prefix
  let queues = registryTableNames (Proxy @(RegistryOf m))
  _ <- Ops.wakeThrottledJobs schemaName queues prefix
  pure n

-- | List every rate-limit policy with its params, bucket stats, and a live count
-- of currently-throttled jobs per prefix across the registry's queues.
listRateLimitPolicies
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => m [RateLimitPolicyView]
listRateLimitPolicies = do
  schemaName <- getSchema
  Ops.listRateLimitPolicies schemaName (registryTableNames (Proxy @(RegistryOf m)))

-- | One prefix's rate-limit policy with its params, bucket stats, and live throttled
-- count. 'Nothing' when the prefix has no policy.
getRateLimitPolicy
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => Text
  -> m (Maybe RateLimitPolicyView)
getRateLimitPolicy prefix = do
  schemaName <- getSchema
  Ops.getRateLimitPolicy schemaName (registryTableNames (Proxy @(RegistryOf m))) prefix

-- | Whether a rate-limit policy exists for a prefix.
rateLimitPolicyExists
  :: forall m
   . (MonadArbiter m)
  => Text
  -> m Bool
rateLimitPolicyExists prefix = do
  schemaName <- getSchema
  Ops.rateLimitPolicyExists schemaName prefix

-- | List a prefix's buckets with fill levels, paginated.
listRateLimitBuckets
  :: forall m
   . (MonadArbiter m)
  => Text
  -> Int
  -> Int
  -> m [RateLimitBucketView]
listRateLimitBuckets prefix limit offset = do
  schemaName <- getSchema
  Ops.listRateLimitBuckets schemaName prefix limit offset

-- | Set or clear a policy's override params and wake the prefix's parked jobs. Returns
-- rows affected (0 if absent).
updateRateLimitPolicyOverrides
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => Text
  -> RateLimitPolicyUpdate
  -> m Int64
updateRateLimitPolicyOverrides prefix upd = do
  schemaName <- getSchema
  n <- Ops.updateRateLimitPolicyOverrides schemaName prefix upd
  -- Wake after the override commits, so a wake failure cannot roll the override back.
  when (n > 0) $ do
    let queues = registryTableNames (Proxy @(RegistryOf m))
    void $ Ops.wakeThrottledJobs schemaName queues prefix
  pure n

-- | List every concurrency pool with its default/override limit and live key and
-- in-flight aggregates.
listConcurrencyPolicies
  :: forall m
   . (MonadArbiter m)
  => m [ConcurrencyPolicyView]
listConcurrencyPolicies = do
  schemaName <- getSchema
  Ops.listConcurrencyPolicies schemaName

-- | One prefix's concurrency pool with its default/override limit and live aggregates.
-- 'Nothing' when the prefix has no pool.
getConcurrencyPolicy
  :: forall m
   . (MonadArbiter m)
  => Text
  -> m (Maybe ConcurrencyPolicyView)
getConcurrencyPolicy prefix = do
  schemaName <- getSchema
  Ops.getConcurrencyPolicy schemaName prefix

-- | List a prefix's keys with effective cap and in-flight fill fraction, paginated.
listConcurrencyKeys
  :: forall m
   . (MonadArbiter m)
  => Text
  -> Int
  -> Int
  -> m [ConcurrencyKeyView]
listConcurrencyKeys prefix limit offset = do
  schemaName <- getSchema
  Ops.listConcurrencyKeys schemaName prefix limit offset

-- | Apply a pool's override-limit patch on its policy row, retuning every key under
-- the prefix live. Lowering it does not preempt in-flight jobs until they drain.
-- Returns rows affected.
updateConcurrencyPolicyOverrides
  :: forall m
   . (MonadArbiter m)
  => Text
  -> ConcurrencyPolicyUpdate
  -> m Int64
updateConcurrencyPolicyOverrides prefix upd = do
  schemaName <- getSchema
  Ops.updateConcurrencyPolicyOverrides schemaName prefix upd

-- | Delete drained concurrency rows with no live job. The reaper runs this.
pruneConcurrencyKeys
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => m Int64
pruneConcurrencyKeys = do
  schemaName <- getSchema
  Ops.pruneConcurrencyKeys schemaName (registryTableNames (Proxy @(RegistryOf m)))

-- | Recompute the concurrency counts from live jobs, repairing any trigger drift.
reconcileConcurrencyCounts
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => m Int64
reconcileConcurrencyCounts = do
  schemaName <- getSchema
  Ops.reconcileConcurrencyCounts schemaName (registryTableNames (Proxy @(RegistryOf m)))

-- | Rebuild the concurrency counts only if a crash truncated the UNLOGGED table. The
-- reaper runs this periodically.
reconcileConcurrencyCountsIfStale
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => m Int64
reconcileConcurrencyCountsIfStale = do
  schemaName <- getSchema
  Ops.reconcileConcurrencyCountsIfStale schemaName (registryTableNames (Proxy @(RegistryOf m)))

-- | Reconcile then prune. The reaper runs this.
reconcileAndPruneConcurrency
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => m Int64
reconcileAndPruneConcurrency = do
  schemaName <- getSchema
  Ops.reconcileAndPruneConcurrency schemaName (registryTableNames (Proxy @(RegistryOf m)))

-- | Assemble a pool's claim statements once (see 'Ops.mkClaimSql'). Batch size 1
-- is the single-job claim.
mkClaimSql
  :: forall payload m
   . (QueueOperation m payload)
  => Int
  -> Int
  -> NominalDiffTime
  -> Maybe UUID
  -> m Ops.ClaimSql
mkClaimSql batchSize poolSize timeout mWorkerId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  pure $ Ops.mkClaimSql (Proxy @payload) schemaName tableName batchSize poolSize timeout mWorkerId

-- | 'claimNextVisibleJobs' stamping @claimed_by@ on every row it claims, which is how
-- the dispatcher attributes a claim.
claimNextVisibleJobsAs
  :: forall payload m
   . (QueueOperation m payload)
  => Int
  -> NominalDiffTime
  -> UUID
  -- ^ Worker UUID stamped on each claimed row.
  -> m [JobRead payload]
claimNextVisibleJobsAs limit timeout workerId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.claimNextVisibleJobsAs schemaName tableName limit timeout workerId

-- | 'claimNextVisibleJobs' claiming up to @batchSize@ jobs from each group, still in
-- per-group order.
claimNextVisibleJobsBatched
  :: forall payload m
   . (QueueOperation m payload)
  => Int
  -- ^ Batch size: maximum number of jobs to claim per group.
  -> Int
  -- ^ Max groups: maximum number of groups/batches to claim.
  -> NominalDiffTime
  -- ^ How long the claimed jobs should remain invisible (in seconds).
  -> m [NonEmpty (JobRead payload)]
claimNextVisibleJobsBatched batchSize maxGroups timeout = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.claimNextVisibleJobsBatched schemaName tableName batchSize maxGroups timeout

-- | Ack a completed job, deleting it or suspending it when its children are still
-- running. Returns 1, or 0 for a job already gone.
ackJob
  :: forall payload m
   . (JobOperation m payload)
  => JobRead payload
  -> m Int64
ackJob job = do
  schemaName <- getSchema
  let tableName = Job.queueName job
  Ops.ackJob schemaName tableName job

-- | 'ackJob' over a batch from one queue in one statement, returning the ids acked.
-- Reclaimed jobs are absent.
ackJobsBatch
  :: forall payload m
   . (JobOperation m payload)
  => [JobRead payload]
  -> m [Int64]
ackJobsBatch [] = pure []
ackJobsBatch jobs@(firstJob : _) = do
  schemaName <- getSchema
  let tableName = Job.queueName firstJob
  Ops.ackJobsBatch schemaName tableName jobs

-- | Park a failed job for its retry backoff. Returns 0 for a job another worker holds.
updateJobForRetry
  :: forall payload m
   . (JobOperation m payload)
  => NominalDiffTime
  -- ^ The delay before this job becomes visible again for retry.
  -> Text
  -- ^ An error message to store with the job.
  -> JobRead payload
  -> m Int64
updateJobForRetry delay errorMsg job = do
  schemaName <- getSchema
  let tableName = Job.queueName job
  Ops.updateJobForRetry schemaName tableName delay errorMsg job

-- | Soft-nack a job so it is reprocessed once its visibility timeout lapses, recording
-- no failure and consuming no attempt. Returns 0 for a job another worker holds.
nackJob
  :: forall payload m
   . (JobOperation m payload)
  => JobRead payload
  -> m Int64
nackJob job = do
  schemaName <- getSchema
  let tableName = Job.queueName job
  Ops.nackJob schemaName tableName job

-- | 'nackJob' over a batch from one queue in one statement, returning the ids nacked.
-- Jobs another worker holds are absent.
nackJobsBatch
  :: forall payload m
   . (JobOperation m payload)
  => [JobRead payload]
  -> m [Int64]
nackJobsBatch [] = pure []
nackJobsBatch jobs@(firstJob : _) = do
  schemaName <- getSchema
  Ops.nackJobsBatch schemaName (Job.queueName firstJob) jobs

-- | Extend a job's visibility timeout by hand, for a long-running job. Returns 0 for a
-- job that is gone, reclaimed or suspended, which 'setVisibilityTimeoutBatch' tells
-- apart.
setVisibilityTimeout
  :: forall payload m
   . (JobOperation m payload)
  => NominalDiffTime
  -- ^ The new visibility timeout (in seconds) from the current time.
  -> JobRead payload
  -> m Int64
setVisibilityTimeout timeout job = do
  schemaName <- getSchema
  let tableName = Job.queueName job
  Ops.setVisibilityTimeout schemaName tableName timeout job

-- | Result of setting visibility timeout for a single job in a batch.
data SetVisibilityResult
  = -- | Visibility timeout was successfully extended.
    VisibilityExtended JobId
  | -- | Job no longer exists (was deleted/acked).
    JobGone JobId
  | -- | Job was reclaimed by another worker: this claim's token, then the row's
    -- token now.
    JobReclaimed JobId ClaimSeq ClaimSeq
  | -- | Job was force-cancel-flagged.
    JobCancelled JobId
  | -- | Job is a finalizer waiting on its children, so it holds no lease.
    JobSuspended JobId
  | -- | The row changed under this claim mid-statement, so nothing was extended.
    VisibilityUnchanged JobId
  deriving stock (Eq, Show)

-- | 'setVisibilityTimeout' over a batch from one queue.
setVisibilityTimeoutBatch
  :: forall payload m
   . (JobOperation m payload)
  => NominalDiffTime
  -- ^ The new visibility timeout (in seconds) from the current time.
  -> [JobRead payload]
  -- ^ Jobs to heartbeat (all must be from the same queue)
  -> m [SetVisibilityResult]
setVisibilityTimeoutBatch _ [] = pure []
setVisibilityTimeoutBatch timeout jobs@(firstJob : _) = do
  schemaName <- getSchema
  let tableName = Job.queueName firstJob
  infos <- Ops.setVisibilityTimeoutBatch schemaName tableName timeout jobs
  let jobMap = Map.fromList [(primaryKey j, j) | j <- jobs]
      toResult (Ops.VisibilityUpdateInfo jobId heartbeated mActual cancelled suspended holder) =
        let mJob = Map.lookup jobId jobMap
            expected = maybe 0 claimSeq mJob
            heldHere = maybe False (\j -> claimedBy j == holder) mJob
         in case mActual of
              Nothing -> JobGone jobId
              Just actual
                | cancelled, heldHere, actual == expected + 1 -> JobCancelled jobId
                | actual /= expected -> JobReclaimed jobId expected actual
                | suspended -> JobSuspended jobId
                | heartbeated -> VisibilityExtended jobId
                | otherwise -> VisibilityUnchanged jobId
  pure $ map toResult infos

-- | Move a job to the DLQ. Returns 0 for a job another worker holds.
moveToDLQ
  :: forall payload m
   . (JobOperation m payload)
  => Text
  -- ^ Error message (the final error that caused the DLQ move)
  -> JobRead payload
  -> m Int64
moveToDLQ errorMsg job = do
  schemaName <- getSchema
  let tableName = Job.queueName job
  Ops.moveToDLQ Ops.TakeLocks schemaName tableName errorMsg job

-- | List DLQ jobs, most recently failed first.
listDLQJobs
  :: forall payload m
   . (QueueOperation m payload)
  => Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [DLQ.DLQJob payload]
listDLQJobs limit offset = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.listDLQJobs schemaName tableName limit offset

-- | List archived jobs, most recently completed first.
listArchiveJobs
  :: forall payload m
   . (QueueOperation m payload)
  => Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [Archive.ArchiveJob payload]
listArchiveJobs limit offset = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.listArchiveJobs schemaName tableName limit offset

-- | Fetch a single archived job by its original job id.
getArchivedJobById
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Original job id
  -> m (Maybe (Archive.ArchiveJob payload))
getArchivedJobById jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.getArchivedJobById schemaName tableName jobId

-- | List archived jobs in a group, most recent first, with pagination.
listArchivedJobsByGroupKey
  :: forall payload m
   . (QueueOperation m payload)
  => Text
  -- ^ Group key
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [Archive.ArchiveJob payload]
listArchivedJobsByGroupKey groupKey limit offset = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.listArchivedJobsByGroupKey schemaName tableName groupKey limit offset

-- | Delete one archived job by its archive primary key. Returns rows deleted.
deleteArchiveJob
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Archive primary key
  -> m Int64
deleteArchiveJob archiveId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.deleteArchiveJob schemaName tableName archiveId

-- | Delete archived jobs by archive primary key. Returns rows deleted.
deleteArchiveJobsBatch
  :: forall payload m
   . (QueueOperation m payload)
  => [Int64]
  -- ^ Archive primary keys
  -> m Int64
deleteArchiveJobsBatch archiveIds = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.deleteArchiveJobsBatch schemaName tableName archiveIds

-- | Re-enqueue an archived job as a fresh standalone job, keeping the archive
-- row. Returns the new job, or @Nothing@ if the archive row no longer exists.
reEnqueueFromArchive
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Archive primary key
  -> m (Maybe (JobRead payload))
reEnqueueFromArchive archiveId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.reEnqueueFromArchive schemaName tableName archiveId

-- | Retry a DLQ job, re-inserting it into the queue with a fresh attempt count.
-- 'Nothing' when the DLQ row is gone.
retryFromDLQ
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ DLQ job id
  -> m (Maybe (JobRead payload))
retryFromDLQ dlqId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.retryFromDLQ schemaName tableName dlqId

-- | Whether a DLQ job with the given id exists.
dlqJobExists
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -> m Bool
dlqJobExists dlqId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.dlqJobExists schemaName tableName dlqId

-- | Delete a DLQ job. Returns 0 when the row is already gone.
deleteDLQJob
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ DLQ job id
  -> m Int64
deleteDLQJob dlqId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.deleteDLQJob schemaName tableName dlqId

-- | Move a batch of jobs to the DLQ, skipping any another worker holds. Returns the
-- number moved.
moveToDLQBatch
  :: forall payload m
   . (JobOperation m payload)
  => [(JobRead payload, Text)]
  -- ^ List of (job, error message) pairs. All jobs must be from the same queue.
  -> m Int64
moveToDLQBatch [] = pure 0
moveToDLQBatch jobsWithErrors@((firstJob, _) : _) = do
  schemaName <- getSchema
  let tableName = Job.queueName firstJob
  Ops.moveToDLQBatch schemaName tableName jobsWithErrors

-- | Delete DLQ jobs by id. Returns the number deleted.
deleteDLQJobsBatch
  :: forall payload m
   . (QueueOperation m payload)
  => [Int64]
  -- ^ DLQ job ids
  -> m Int64
deleteDLQJobsBatch dlqIds = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.deleteDLQJobsBatch schemaName tableName dlqIds

-- ---------------------------------------------------------------------------
-- Filtered Query Operations
-- ---------------------------------------------------------------------------

-- | List filtered jobs, newest first.
listJobsFiltered
  :: forall payload m
   . (QueueOperation m payload)
  => [Ops.JobFilter]
  -- ^ Composable filters
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
listJobsFiltered filters limit offset = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.listJobsFiltered schemaName tableName filters limit offset

-- | Count filtered jobs.
countJobsFiltered
  :: forall payload m
   . (QueueOperation m payload)
  => [Ops.JobFilter]
  -- ^ Composable filters
  -> m Int64
countJobsFiltered filters = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.countJobsFiltered schemaName tableName filters

-- | List filtered DLQ jobs, most recently failed first.
listDLQFiltered
  :: forall payload m
   . (QueueOperation m payload)
  => [Ops.JobFilter]
  -- ^ Composable filters
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [DLQ.DLQJob payload]
listDLQFiltered filters limit offset = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.listDLQFiltered schemaName tableName filters limit offset

-- | Count filtered DLQ jobs.
countDLQFiltered
  :: forall payload m
   . (QueueOperation m payload)
  => [Ops.JobFilter]
  -- ^ Composable filters
  -> m Int64
countDLQFiltered filters = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.countDLQFiltered schemaName tableName filters

-- ---------------------------------------------------------------------------
-- Admin Operations
-- ---------------------------------------------------------------------------

-- | List jobs, newest first.
listJobs
  :: forall payload m
   . (QueueOperation m payload)
  => Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
listJobs limit offset = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.listJobs schemaName tableName limit offset

-- | Fetch a job by id.
getJobById
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Job id
  -> m (Maybe (JobRead payload))
getJobById jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.getJobById schemaName tableName jobId

-- | Whether a job with the given id exists in this payload's queue table.
jobExists
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Job id
  -> m Bool
jobExists jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.jobExists schemaName tableName jobId

-- | List a group's jobs.
getJobsByGroup
  :: forall payload m
   . (QueueOperation m payload)
  => Text
  -- ^ Group key
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
getJobsByGroup groupKey limit offset = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.getJobsByGroup schemaName tableName groupKey limit offset

-- | List a parent's children.
getJobsByParent
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Parent id
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
getJobsByParent pid limit offset = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.getJobsByParent schemaName tableName pid limit offset

-- | Delete a job by id. Returns 0 for a job with children, which 'cancelJobCascade'
-- takes instead.
cancelJob
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Job id
  -> m Int64
cancelJob jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.cancelJob schemaName tableName jobId

-- | Delete a job and its descendants, interrupting any handler running one of them by
-- NOTIFY on the cancel channel.
forceCancelJob
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Root job id
  -> m Int64
forceCancelJob jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.forceCancelJob schemaName tableName jobId

-- | Delete jobs by id. Returns the number deleted.
cancelJobsBatch
  :: forall payload m
   . (QueueOperation m payload)
  => [Int64]
  -- ^ Job ids
  -> m Int64
cancelJobsBatch jobIds = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.cancelJobsBatch schemaName tableName jobIds

-- | Make a delayed or retrying job immediately visible. Refuses an in-flight job.
promoteJob
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Job id
  -> m Int64
promoteJob jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.promoteJob schemaName tableName jobId

-- | A queue's per-status counts and backlog ages.
getQueueStats
  :: forall payload m
   . (QueueOperation m payload)
  => m Ops.QueueStats
getQueueStats = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.getQueueStats schemaName tableName

-- ---------------------------------------------------------------------------
-- Count Operations
-- ---------------------------------------------------------------------------

-- | Count every job in the queue.
countJobs
  :: forall payload m
   . (QueueOperation m payload)
  => m Int64
countJobs = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.countJobs schemaName tableName

-- | Count a group's jobs.
countJobsByGroup
  :: forall payload m
   . (QueueOperation m payload)
  => Text
  -- ^ Group key
  -> m Int64
countJobsByGroup groupKey = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.countJobsByGroup schemaName tableName groupKey

-- | Count a parent's children.
countJobsByParent
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Parent id
  -> m Int64
countJobsByParent pid = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.countJobsByParent schemaName tableName pid

-- | Count the queue's DLQ jobs.
countDLQJobs
  :: forall payload m
   . (QueueOperation m payload)
  => m Int64
countDLQJobs = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.countDLQJobs schemaName tableName

-- | Child counts as @(total, paused)@ per parent id, over a batch. Parents with none
-- are absent.
countChildrenBatch
  :: forall payload m
   . (QueueOperation m payload)
  => [Int64]
  -> m (Map Int64 (Int64, Int64))
countChildrenBatch ids = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.countChildrenBatch schemaName tableName ids

-- | Count a parent's DLQ'd children, which a finalizer handler reads to spot failures.
countDLQChildren
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Parent job id
  -> m Int64
countDLQChildren parentJobId = do
  m <- countDLQChildrenBatch @payload [parentJobId]
  pure $ fromMaybe 0 (Map.lookup parentJobId m)

-- | DLQ child counts per parent id, over a batch. Parents with none are absent.
countDLQChildrenBatch
  :: forall payload m
   . (QueueOperation m payload)
  => [Int64]
  -> m (Map Int64 Int64)
countDLQChildrenBatch ids = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.countDLQChildrenBatch schemaName tableName ids

-- ---------------------------------------------------------------------------
-- Job Dependency Operations
-- ---------------------------------------------------------------------------

-- | Suspend a parent's claimable children. In-flight ones are left alone, so their
-- visibility timeout can lapse normally if the worker crashes. Returns the number
-- suspended.
pauseChildren
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Parent job id
  -> m Int64
pauseChildren parentJobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.pauseChildren schemaName tableName parentJobId

-- | Resume a parent's suspended children. Returns the number resumed.
resumeChildren
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Parent job id
  -> m Int64
resumeChildren parentJobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.resumeChildren schemaName tableName parentJobId

-- | Delete a job and every descendant under it. Returns the number deleted.
cancelJobCascade
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Root job id
  -> m Int64
cancelJobCascade jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.cancelJobCascade schemaName tableName jobId

-- ---------------------------------------------------------------------------
-- Suspend/Resume Operations
-- ---------------------------------------------------------------------------

-- | Suspend a job, making it unclaimable. Refuses an in-flight job.
suspendJob
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Job id
  -> m Int64
suspendJob jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.suspendJob schemaName tableName jobId

-- | Resume a suspended job, making it claimable again.
resumeJob
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Job id
  -> m Int64
resumeJob jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.resumeJob schemaName tableName jobId

-- ---------------------------------------------------------------------------
-- Results Table Operations
-- ---------------------------------------------------------------------------

-- | Insert a child's result, encoded as the queue's declared result type and keyed by
-- @(parent_id, child_id)@. Its foreign key cascades, so acking the parent clears it.
-- Returns 0 for a result that stores nothing.
insertResult
  :: forall payload m
   . (EncodeJobResult (ResultOf m payload), QueueOperation m payload)
  => Int64
  -- ^ Parent job id
  -> Int64
  -- ^ Child job id
  -> ResultOf m payload
  -- ^ Result value
  -> m Int64
insertResult parentJobId childId result =
  maybe (pure 0) (insertResultUnsafe @payload parentJobId childId) (encodeJobResult result)

-- | 'insertResult' with a raw JSON value, bypassing the queue's declared result
-- type. A value 'Arbiter.Worker.childResults' cannot decode surfaces there as a
-- 'Left'.
insertResultUnsafe
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Parent job id
  -> Int64
  -- ^ Child job id
  -> Value
  -- ^ Encoded result value
  -> m Int64
insertResultUnsafe parentJobId childId result = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.insertResult schemaName tableName parentJobId childId result

-- | A parent's child results, keyed by child id.
getResultsByParent
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Parent job id
  -> m (Map Int64 Value)
getResultsByParent parentJobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.getResultsByParent schemaName tableName parentJobId

-- | A parent's DLQ'd children's last errors, keyed by child id.
getDLQChildErrorsByParent
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Parent job id
  -> m (Map Int64 Text)
getDLQChildErrorsByParent parentJobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.getDLQChildErrorsByParent schemaName tableName parentJobId

-- | Read all child data for a rollup finalizer. Returns
-- @(childId->result, childId->error, parentStateSnapshot, dlqRowId->error)@.
readChildResultsRaw
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -- ^ Parent job id
  -> m (Map Int64 Value, Map Int64 Text, Maybe Value, Map Int64 Text)
readChildResultsRaw parentJobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.readChildResultsRaw schemaName tableName parentJobId

-- | Snapshot results into @parent_state@ before DLQ move.
persistParentState
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -> Value
  -> m Int64
persistParentState jobId state = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.persistParentState schemaName tableName jobId state

-- | Read raw @parent_state@ snapshot from the DB.
getParentStateSnapshot
  :: forall payload m
   . (QueueOperation m payload)
  => Int64
  -> m (Maybe Value)
getParentStateSnapshot jobId = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  Ops.getParentStateSnapshot schemaName tableName jobId

-- ---------------------------------------------------------------------------
-- Groups Table Operations
-- ---------------------------------------------------------------------------

-- | Schema-wide groups-table refresh, correcting every registered queue's summary drift.
-- Walks each groups table to the end, one bounded batch and one transaction at a time.
-- A deliberate repair rather than a hot path: the reaper runs 'Ops.refreshAllGroups' for
-- a single batch per tick. Returns the rows rewritten and the queue names that failed.
refreshAllGroupsFully
  :: forall m
   . (MonadArbiter m, RegistryTables (RegistryOf m))
  => m (Int64, [Text])
refreshAllGroupsFully = do
  schemaName <- getSchema
  Ops.refreshAllGroupsFully schemaName (registryTableNames (Proxy @(RegistryOf m)))

-- ---------------------------------------------------------------------------
-- Cron Schedules
-- ---------------------------------------------------------------------------

-- | List cron schedules ordered by name, optionally filtered by queue.
listCronSchedules
  :: forall m
   . (MonadArbiter m)
  => Maybe Text
  -- ^ Queue filter. 'Nothing' returns schedules for all queues.
  -> m [CronScheduleRow]
listCronSchedules mQueue = do
  schemaName <- getSchema
  Ops.listCronSchedules schemaName mQueue

-- | Get a single cron schedule by name.
getCronScheduleByName
  :: forall m
   . (MonadArbiter m)
  => Text
  -- ^ Schedule name
  -> m (Maybe CronScheduleRow)
getCronScheduleByName scheduleName = do
  schemaName <- getSchema
  Ops.getCronScheduleByName schemaName scheduleName

-- | Patch a cron schedule, writing the overrides as given. Returns rows affected, 0 for
-- a name that is not there. @Arbiter.Worker.updateCronScheduleChecked@ rejects overrides
-- the scheduler cannot parse.
updateCronScheduleUnchecked
  :: forall m
   . (MonadArbiter m)
  => Text
  -- ^ Schedule name
  -> CronScheduleUpdate
  -> m Int64
updateCronScheduleUnchecked scheduleName upd = do
  schemaName <- getSchema
  Ops.updateCronSchedule schemaName scheduleName upd

-- ---------------------------------------------------------------------------
-- Global Gate
-- ---------------------------------------------------------------------------

-- | Run @work@ at most once per @interval@ across every worker pool sharing
-- the same schema. See 'Ops.runGated'.
runGated
  :: forall m a
   . (MonadArbiter m)
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
  :: forall payload m
   . (QueueOperation m payload)
  => JT.JobTree payload
  -> m (Either Text (NonEmpty (JobRead payload)))
insertJobTree tree = do
  schemaName <- getSchema
  let tableName = queueTable @payload @m
  JT.insertJobTree schemaName tableName tree

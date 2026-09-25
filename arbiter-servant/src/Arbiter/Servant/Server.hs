{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | REST API server for the Arbiter job queue, over any 'MonadArbiter' backend.
--
-- __Security:__ No built-in authentication. All endpoints are publicly
-- accessible. Add auth middleware before exposing to untrusted networks.
module Arbiter.Servant.Server
  ( -- * Server handlers
    arbiterServer
  , arbiterServerHoisted
  , arbiterApp
  , runArbiterAPI
  , ArbiterServerConfig (..)
  , initArbiterServer
  , defaultQueueStatsCacheTtl
  , defaultMaintenanceInterval
  , defaultMaintenanceBucketIdle
  , defaultMaintenanceSparseInterval
  , defaultMaintenanceTimeout
  , BuildServer (..)
  ) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Exceptions (throwParsing)
import Arbiter.Core.Health qualified as Health
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types (DedupKey (..), JobPayload, JobStatus, isRollup, kindsFor)
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobResult (EncodeJobResult, encodeJobResult)
import Arbiter.Core.Listen (Notification (..), withChannels)
import Arbiter.Core.MonadArbiter (HasRegistry, getListener, getSchema, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (JobPayloadRegistry, RegistryTables (..), SpecName, SpecPayload, SpecResult)
import Arbiter.Core.Queues qualified as Queues
import Arbiter.Core.Sql.Jobs (ArchiveSortColumn, DLQSortColumn, JobFilter (..), JobSortColumn, SortDir)
import Arbiter.Core.Trace (withPublishSpan)
import Arbiter.Worker (MaintenancePace (..), runMaintenancePass, storeEncodedResult)
import Arbiter.Worker.Config (maintenanceOpName)
import Arbiter.Worker.Cron (nextRunFromExpression, updateCronScheduleChecked)
import Arbiter.Worker.Logger
  ( FailureGates
  , LogConfig
  , LogLevel (..)
  , defaultLogConfig
  , hubLogFor
  , newFailureGates
  , tryReportedOn
  )
import Control.Concurrent.STM
  ( TVar
  , atomically
  , check
  , modifyTVar'
  , newTChanIO
  , newTVarIO
  , readTChan
  , readTVar
  , readTVarIO
  , writeTChan
  )
import Control.Exception (bracket_)
import Control.Monad (guard, join, mfilter, unless, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (encode)
import Data.ByteString (ByteString)
import Data.ByteString.Builder qualified as Builder
import Data.ByteString.Lazy qualified as LBS
import Data.Either (fromRight)
import Data.Foldable (traverse_)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.Kind (Type)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust)
import Data.Ord (clamp)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Data.Time (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Data.UUID.Types (UUID)
import Data.UUID.V4 qualified as UUID
import GHC.TypeLits (KnownSymbol, symbolVal)
import Network.HTTP.Types (status200)
import Network.Wai (responseStream)
import Network.Wai.Handler.Warp (Port, defaultSettings, runSettings, setPort)
import Servant
import Servant.Server.Generic (AsServerT)
import System.Timeout (timeout)
import UnliftIO.Exception (handleAny, tryAny)

import Arbiter.Servant.API
  ( ArbiterAPI
  , ArchiveAPI (..)
  , ConcurrencyAPI (..)
  , CronAPI (..)
  , DLQAPI (..)
  , HealthAPI (..)
  , JobsAPI (..)
  , MaintenanceAPI (..)
  , QueuesAPI (..)
  , RateLimitsAPI (..)
  , RegistryToAPI
  , SharedAPI
  , StatsAPI (..)
  , TableAPI (..)
  , WorkersAPI (..)
  )
import Arbiter.Servant.Types

-- | Configuration for the API server. @m@ is the backend monad every handler's
-- statements run in.
data ArbiterServerConfig m (registry :: JobPayloadRegistry) = ArbiterServerConfig
  { serverRun :: forall a. m a -> IO a
  -- ^ Backend runner, e.g. @runSimpleDb env@ or @runHasqlDb env@.
  , serverSchema :: Text
  -- ^ The schema every handler's statements run against.
  , enableSSE :: Bool
  -- ^ Enable the Server-Sent Events streaming endpoint. When 'False', the
  -- @\/events\/stream@ endpoint returns one \"disabled\" event and closes.
  -- The admin UI then polls. A backend with no listener answers the same way.
  -- Default: 'True'.
  , rateLimitPoliciesCache :: CacheCell RateLimitPoliciesResponse
  -- ^ Short-TTL cache for the rate-limit policy list.
  , concurrencyPoliciesCache :: CacheCell ConcurrencyPoliciesResponse
  -- ^ Short-TTL cache for the concurrency policy list.
  , allQueueStatsCache :: CacheCell AllStatsResponse
  -- ^ Short-TTL cache for the all-queues overview aggregate.
  , queueStatsCache :: CacheCell StatsResponse
  -- ^ Per-queue stats cache.
  , queueStatsCacheTtl :: NominalDiffTime
  -- ^ Per-queue stats staleness, or zero to always hit the database.
  -- Default: 'defaultQueueStatsCacheTtl'.
  , healthCache :: CacheCell HealthResponse
  -- ^ Short-TTL cache for the readiness probe.
  , maintenanceInterval :: NominalDiffTime
  -- ^ Minimum gap between runs of one maintenance operation. Zero runs every
  -- operation on every call. Default: 'defaultMaintenanceInterval'.
  , maintenanceSparseInterval :: NominalDiffTime
  -- ^ Gap between runs of one whole-schema operation, independent of
  -- 'maintenanceInterval'. Default: 'defaultMaintenanceSparseInterval'.
  , maintenanceBucketIdle :: NominalDiffTime
  -- ^ Idle age at which a pass prunes a rate-limit bucket.
  -- Default: 'defaultMaintenanceBucketIdle'.
  , maintenanceTimeout :: NominalDiffTime
  -- ^ Abort any single maintenance statement that runs longer than this.
  -- Default: 'defaultMaintenanceTimeout'.
  , serverLogConfig :: LogConfig
  -- ^ Where the server reports maintenance and dead-letter failures.
  -- Default: 'defaultLogConfig'.
  , deadLetterGates :: FailureGates
  -- ^ Per-queue failure gates for dead-letter reports.
  }

-- | Run a statement on the backend.
runDb :: (MonadIO n) => ArbiterServerConfig m registry -> m a -> n a
runDb config = liftIO . serverRun config

-- | 'NoContent' when a statement touched a row, 404 otherwise.
rowsOr404 :: LBS.ByteString -> Int64 -> Handler NoContent
rowsOr404 missing rowsAffected
  | rowsAffected > 0 = pure NoContent
  | otherwise = throwError err404 {errBody = missing}

-- | Answer a handler that decided its own error.
noContentOr :: Either ServerError () -> Handler NoContent
noContentOr = either throwError (const (pure NoContent))

-- | Run a job mutation. When it touches no row, re-read the job and answer 404, or
-- the 409 that @refuse@ derives from the job's state.
mutateJob
  :: forall (payload :: Type) registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> (Text -> m Int64)
  -> (Job.JobRead (Job.Stored payload) -> LBS.ByteString)
  -> Handler NoContent
mutateJob tableName config jobId mutate refuse =
  noContentOr =<< runDb config (mutate schemaName >>= diagnose)
  where
    schemaName = serverSchema config
    diagnose rowsAffected
      | rowsAffected > 0 = pure (Right ())
      | otherwise =
          maybe (Left err404 {errBody = "Job not found"}) (\job -> Left err409 {errBody = refuse job})
            <$> Ops.getJobById @_ @payload schemaName tableName jobId

-- | Create an 'ArbiterServerConfig' over a backend runner. SSE needs the
-- event-streaming triggers, which 'Arbiter.Migrations.runMigrationsForRegistry' installs
-- when @enableEventStreaming@ is set, and a backend with a listener.
initArbiterServer
  :: forall m registry
   . (HasRegistry m registry)
  => (forall a. m a -> IO a)
  -> IO (ArbiterServerConfig m registry)
initArbiterServer run = do
  schemaName <- run getSchema
  rlCache <- newCacheCell
  ccCache <- newCacheCell
  statsCache <- newCacheCell
  perQueueCache <- newCacheCell
  healthCell <- newCacheCell
  gates <- newFailureGates
  pure
    ArbiterServerConfig
      { serverRun = run
      , serverSchema = schemaName
      , enableSSE = True
      , rateLimitPoliciesCache = rlCache
      , concurrencyPoliciesCache = ccCache
      , allQueueStatsCache = statsCache
      , queueStatsCache = perQueueCache
      , queueStatsCacheTtl = defaultQueueStatsCacheTtl
      , healthCache = healthCell
      , maintenanceInterval = defaultMaintenanceInterval
      , maintenanceSparseInterval = defaultMaintenanceSparseInterval
      , maintenanceBucketIdle = defaultMaintenanceBucketIdle
      , maintenanceTimeout = defaultMaintenanceTimeout
      , serverLogConfig = defaultLogConfig
      , deadLetterGates = gates
      }

-- | Jobs API handlers for a specific table.
jobsServer
  :: forall registry payload result m
   . (EncodeJobResult result, HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> JobsAPI payload result (AsServerT Handler)
jobsServer table config =
  JobsAPI
    { listJobs = listJobsHandler @registry @payload table config
    , insertJob = insertJobHandler @registry @payload table config
    , insertJobsBatch = insertJobsBatchHandler @registry @payload table config
    , getJob = getJobHandler @registry @payload table config
    , cancelJob = cancelJobHandler @registry table config
    , forceCancelJob = forceCancelJobHandler @registry table config
    , promoteJob = promoteJobHandler @registry table config
    , moveToDLQ = moveToDLQHandler @registry table config
    , pauseChildren = pauseChildrenHandler @registry table config
    , resumeChildren = resumeChildrenHandler @registry table config
    , suspendJob = suspendJobHandler @registry table config
    , resumeJob = resumeJobHandler @registry table config
    , ackClaimedJob = ackClaimedJobHandler @registry @result table config
    , nackClaimedJob = nackClaimedJobHandler @registry table config
    , extendClaimedJob = extendClaimedJobHandler @registry table config
    }

-- | List jobs with pagination and composable filters.
listJobsHandler
  :: forall registry (payload :: Type) m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Maybe Int
  -> Maybe Int
  -> Maybe Text
  -> Maybe Int64
  -> Maybe Int64
  -> Bool
  -> Maybe JobStatus
  -> Maybe UUID
  -> Maybe Text
  -> Maybe Text
  -> Maybe Text
  -> Maybe Text
  -> Maybe JobSortColumn
  -> Maybe SortDir
  -> Handler (JobsResponse payload)
listJobsHandler tableName config mLimit mOffset mGroupKey mParentId mJobId rootsOnly mStatus mClaimedBy mKind mPayload mRatePrefix mConcPrefix mSortBy mSortDir = liftIO $ do
  let (limit, offset) = validatePagination 50 mLimit mOffset
      schemaName = serverSchema config
      filters =
        catMaybes
          [ FilterGroupKey <$> mGroupKey
          , FilterParentId <$> mParentId
          , FilterId <$> mJobId
          , FilterRootsOnly <$ guard rootsOnly
          , FilterStatus <$> mStatus
          , FilterClaimedBy <$> mClaimedBy
          , FilterKind <$> nonBlank mKind
          , FilterPayloadText <$> nonBlank mPayload
          , FilterRateLimitPrefix <$> mRatePrefix
          , FilterConcurrencyPrefix <$> mConcPrefix
          ]

  (jobs, total, combined, dlqCounts) <- runDb config $ withDbTransaction $ do
    page <- Ops.listJobsWithStatus schemaName tableName filters mSortBy mSortDir limit offset
    matching <- Ops.countJobsFiltered schemaName tableName filters
    -- Every parent is a rollup finalizer. A page without one skips the count queries.
    let jobIds = map (Job.primaryKey . fst) page
        hasParents = any (isRollup . fst) page
    if hasParents
      then do
        children <- Ops.countChildrenBatch schemaName tableName jobIds
        dlqChildren <- Ops.countDLQChildrenBatch schemaName tableName jobIds
        pure (page, matching, children, dlqChildren)
      else pure (page, matching, Map.empty, Map.empty)

  let childCounts = fmap fst combined
      pausedParents = Map.keys $ Map.filter (\(childTotal, childPaused) -> childPaused == childTotal) combined
      apiJobs = map (uncurry ApiJobWithStatus) jobs
  pure $
    JobsResponse
      { jobs = apiJobs
      , jobsTotal = fromIntegral total
      , jobsOffset = offset
      , jobsLimit = limit
      , childCounts = childCounts
      , pausedParents = pausedParents
      , dlqChildCounts = dlqCounts
      }

-- | Insert a new job into the queue.
insertJobHandler
  :: forall registry payload m
   . (HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> ApiJobWrite payload
  -> Handler (JobResponse (Job.JobRead payload))
insertJobHandler tableName config (ApiJobWrite jobWrite) = do
  let schemaName = serverSchema config
  mJob <- runDb config $ withPublishSpan tableName [jobWrite] $ do
    inserted <- Ops.insertJob schemaName tableName jobWrite
    case (inserted, Job.dedupKey jobWrite) of
      (Just fresh, _) -> pure (Just fresh)
      (Nothing, Just (IgnoreDuplicate duplicateKey)) ->
        Ops.getJobByDedupKey schemaName tableName duplicateKey >>= traverse (either throwParsing pure . Ops.decodeRow)
      _ -> pure Nothing
  case mJob of
    Just found -> pure $ JobResponse found
    Nothing ->
      throwError err409 {errBody = "Replace blocked: existing job is actively claimed, force-cancel flagged, or has children"}

-- | Insert multiple jobs in a single batch operation.
insertJobsBatchHandler
  :: forall registry payload m
   . (HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> BatchInsertRequest payload
  -> Handler (BatchInsertResponse payload)
insertJobsBatchHandler tableName config (BatchInsertRequest jobWrites) = do
  let schemaName = serverSchema config
      writes = map unApiJobWrite jobWrites

  inserted <-
    runDb config
      $ withPublishSpan tableName writes
      $ Ops.insertJobsBatch schemaName tableName writes
  pure $ BatchInsertResponse {inserted = inserted, insertedCount = length inserted}

-- | Fetch a job by id.
getJobHandler
  :: forall registry (payload :: Type) m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler (JobResponse (ApiJobWithStatus (Job.Stored payload)))
getJobHandler tableName config jobId = do
  let schemaName = serverSchema config
  mJob <- runDb config $ Ops.getJobByIdWithStatus schemaName tableName jobId
  case mJob of
    Nothing -> throwError err404 {errBody = "Job not found"}
    Just (found, jobStatus) -> pure $ JobResponse {job = ApiJobWithStatus found jobStatus}

-- | Cancel a job (delete it from the queue).
cancelJobHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
cancelJobHandler tableName config jobId = do
  let schemaName = serverSchema config
  runDb config (Ops.cancelJobCascade schemaName tableName jobId) >>= rowsOr404 "Job not found"

-- | Cascade-cancel a job and async-cancel any in-flight handlers via NOTIFY.
forceCancelJobHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
forceCancelJobHandler tableName config jobId = do
  let schemaName = serverSchema config
  runDb config (Ops.forceCancelJob schemaName tableName jobId) >>= rowsOr404 "Job not found"

-- | Promote a job (make it immediately visible).
promoteJobHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
promoteJobHandler tableName config jobId =
  mutateJob tableName config jobId (\schemaName -> Ops.promoteJob schemaName tableName jobId) refuse
  where
    refuse job
      | Job.suspended job = "Job is suspended - use resume endpoint"
      | isJust (Job.claimedBy job) = "Job is in flight - wait for its lease to lapse"
      | otherwise = "Job is already visible"

-- | Move a job to the dead letter queue.
moveToDLQHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
moveToDLQHandler tableName config jobId =
  noContentOr =<< runDb config (withDbTransaction moved)
  where
    schemaName = serverSchema config
    moved = Ops.getJobById schemaName tableName jobId >>= maybe (pure notFound) move
    notFound = Left err404 {errBody = "Job not found"}
    move job = decide <$> Ops.moveToDLQ Ops.TakeLocks schemaName tableName "Manually moved to DLQ via admin API" job
    decide rowsAffected
      | rowsAffected > 0 = Right ()
      | otherwise = Left err409 {errBody = "Job was concurrently modified"}

-- | Pause all children of a parent job.
pauseChildrenHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
pauseChildrenHandler tableName config jobId =
  -- Pausing nothing is a success. The children may be in flight, suspended or done.
  NoContent <$ runDb config (Ops.pauseChildren (serverSchema config) tableName jobId)

-- | Resume all suspended children of a parent job.
resumeChildrenHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
resumeChildrenHandler tableName config jobId =
  -- Resuming nothing is a success. The children may be unsuspended or done.
  NoContent <$ runDb config (Ops.resumeChildren (serverSchema config) tableName jobId)

-- | Suspend a job (make it unclaimable).
suspendJobHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
suspendJobHandler tableName config jobId =
  mutateJob tableName config jobId (\schemaName -> Ops.suspendJob schemaName tableName jobId) refuse
  where
    refuse job
      | Job.suspended job = "Job is already suspended"
      | otherwise = "Job is in-flight - cannot suspend"

-- | Resume a suspended job, making it claimable again. Refuses a finalizer with children
-- still running.
resumeJobHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
resumeJobHandler tableName config jobId =
  mutateJob tableName config jobId (\schemaName -> Ops.resumeJob schemaName tableName jobId) refuse
  where
    refuse job
      | not (Job.suspended job) = "Job is not suspended"
      | isRollup job = "Cannot resume a rollup finalizer with active children"
      | otherwise = "Job could not be resumed (concurrent modification)"

-- | DLQ API handlers for a specific table.
dlqServer
  :: forall registry payload m
   . (HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> DLQAPI payload (AsServerT Handler)
dlqServer table config =
  DLQAPI
    { listDLQ = listDLQHandler @registry @payload table config
    , retryFromDLQ = retryFromDLQHandler @registry @payload table config
    , deleteDLQ = deleteDLQHandler @registry table config
    , deleteDLQBatch = deleteDLQBatchHandler @registry table config
    }

-- | List DLQ jobs with pagination and composable filters.
listDLQHandler
  :: forall registry (payload :: Type) m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Maybe Int
  -> Maybe Int
  -> Maybe Int64
  -> Maybe Int64
  -> Maybe Text
  -> Maybe Text
  -> Maybe DLQSortColumn
  -> Maybe SortDir
  -> Handler (DLQResponse payload)
listDLQHandler tableName config mLimit mOffset mParentId mJobId mGroupKey mKind mSortBy mSortDir = do
  let (limit, offset) = validatePagination 50 mLimit mOffset
      schemaName = serverSchema config
      filters =
        catMaybes
          [ FilterParentId <$> mParentId
          , FilterJobId <$> mJobId
          , FilterGroupKey <$> mGroupKey
          , FilterKind <$> nonBlank mKind
          ]

  (entries, total) <- runDb config $ withDbTransaction $ do
    page <- Ops.listDLQFilteredOrdered schemaName tableName filters mSortBy mSortDir limit offset
    matching <- Ops.countDLQFiltered schemaName tableName filters
    pure (page, matching)

  pure $
    DLQResponse
      { dlqJobs = entries
      , dlqTotal = fromIntegral total
      , dlqOffset = offset
      , dlqLimit = limit
      }

-- | Retry a DLQ job back into the main queue. 409 when its parent is gone.
retryFromDLQHandler
  :: forall registry (payload :: Type) m
   . (HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
retryFromDLQHandler tableName config dlqId =
  noContentOr =<< runDb config (withDbTransaction retried)
  where
    schemaName = serverSchema config
    retried =
      Ops.retryFromDLQ schemaName tableName dlqId
        >>= traverse (Ops.typedRow @payload)
        >>= maybe missing (const (pure (Right ())))
    missing = refuse <$> Ops.dlqJobExists schemaName tableName dlqId
    refuse exists
      | exists = Left err409 {errBody = "Cannot retry: parent job no longer exists (not in queue or DLQ)"}
      | otherwise = Left err404 {errBody = "DLQ job not found"}

-- | Delete a job from DLQ permanently.
deleteDLQHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
deleteDLQHandler tableName config dlqId = do
  let schemaName = serverSchema config
  runDb config (Ops.deleteDLQJob schemaName tableName dlqId) >>= rowsOr404 "DLQ job not found"

-- | Batch delete jobs from DLQ permanently.
deleteDLQBatchHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> BatchDeleteRequest
  -> Handler BatchDeleteResponse
deleteDLQBatchHandler tableName config (BatchDeleteRequest dlqIds) = do
  let schemaName = serverSchema config
  rowsDeleted <- runDb config $ Ops.deleteDLQJobsBatch schemaName tableName dlqIds
  pure $ BatchDeleteResponse {deleted = rowsDeleted}

-- | Archive API handler for a specific table.
archiveServer
  :: forall registry payload m
   . (HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> ArchiveAPI payload (AsServerT Handler)
archiveServer table config =
  ArchiveAPI
    { listArchive = listArchiveHandler @registry @payload table config
    , reEnqueueArchive = reEnqueueArchiveHandler @registry @payload table config
    , deleteArchive = deleteArchiveHandler @registry table config
    , deleteArchiveBatch = deleteArchiveBatchHandler @registry table config
    }

-- | List archived jobs with pagination and composable filters.
listArchiveHandler
  :: forall registry (payload :: Type) m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Maybe Int
  -> Maybe Int
  -> Maybe Int64
  -> Maybe Int64
  -> Maybe Text
  -> Maybe Text
  -> Maybe UTCTime
  -> Maybe UTCTime
  -> Maybe ArchiveSortColumn
  -> Maybe SortDir
  -> Handler (ArchiveResponse payload)
listArchiveHandler tableName config mLimit mOffset mParentId mJobId mGroupKey mKind mCompletedAfter mCompletedBefore mSortBy mSortDir = do
  let (limit, offset) = validatePagination 50 mLimit mOffset
      schemaName = serverSchema config
      filters =
        catMaybes
          [ FilterParentId <$> mParentId
          , FilterJobId <$> mJobId
          , FilterGroupKey <$> mGroupKey
          , FilterKind <$> nonBlank mKind
          , FilterCompletedAfter <$> mCompletedAfter
          , FilterCompletedBefore <$> mCompletedBefore
          ]

  (archived, total) <- runDb config $ withDbTransaction $ do
    page <- Ops.listArchiveFiltered schemaName tableName filters mSortBy mSortDir limit offset
    matching <- Ops.countArchiveFiltered schemaName tableName filters
    pure (page, matching)

  pure $
    ArchiveResponse
      { archiveJobs = archived
      , archiveTotal = fromIntegral total
      , archiveOffset = offset
      , archiveLimit = limit
      }

-- | Re-enqueue an archived job as a fresh job. 404 if the archive row is gone.
reEnqueueArchiveHandler
  :: forall registry (payload :: Type) m
   . (HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
reEnqueueArchiveHandler tableName config archiveId = do
  let schemaName = serverSchema config
  mJob <-
    runDb config $
      withDbTransaction (Ops.reEnqueueFromArchive schemaName tableName archiveId >>= traverse (Ops.typedRow @payload))
  case mJob of
    Just _ -> pure NoContent
    Nothing -> throwError err404 {errBody = "Archived job not found"}

-- | Purge one archived job by its archive primary key.
deleteArchiveHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> Handler NoContent
deleteArchiveHandler tableName config archiveId = do
  let schemaName = serverSchema config
  runDb config (Ops.deleteArchiveJob schemaName tableName archiveId) >>= rowsOr404 "Archived job not found"

-- | Bulk-purge archived jobs by archive primary key.
deleteArchiveBatchHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> BatchDeleteRequest
  -> Handler BatchDeleteResponse
deleteArchiveBatchHandler tableName config (BatchDeleteRequest archiveIds) = do
  let schemaName = serverSchema config
  rowsDeleted <- runDb config $ Ops.deleteArchiveJobsBatch schemaName tableName archiveIds
  pure $ BatchDeleteResponse {deleted = rowsDeleted}

-- | Stats API handler for a specific table.
statsServer
  :: forall registry payload m
   . (HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> StatsAPI (AsServerT Handler)
statsServer tableName config =
  StatsAPI
    { getStats = getStatsHandler @registry tableName (kindsFor @payload) config
    }

-- | Get queue statistics.
getStatsHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> [Text]
  -> ArbiterServerConfig m registry
  -> Handler StatsResponse
getStatsHandler tableName kinds config =
  liftIO $ cachedForKey (queueStatsCacheTtl config) (queueStatsCache config) tableName $ do
    let schemaName = serverSchema config

    queueStats <- runDb config $ Ops.getQueueStats schemaName tableName kinds
    now <- getCurrentTime
    let timestamp = T.pack $ formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%z" now

    pure $ StatsResponse {stats = queueStats, timestamp = timestamp}

-- | Every queue's stats in one request, for the landing overview.
getAllStatsHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> [(Text, [Text])]
  -> Handler AllStatsResponse
getAllStatsHandler config queueKinds =
  liftIO $ cachedFor overviewStatsCacheTtl (allQueueStatsCache config) $ do
    let schemaName = serverSchema config
    AllStatsResponse <$> runDb config (Ops.getAllQueueStats schemaName queueKinds)

-- | Table API handlers for a specific table.
tableServer
  :: forall registry payload result m
   . (EncodeJobResult result, HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> TableAPI payload result (AsServerT Handler)
tableServer table config =
  TableAPI
    { jobs = jobsServer @registry @payload @result table config
    , claimJobs = claimJobsHandler @registry @payload table config
    , dlq = dlqServer @registry @payload table config
    , archive = archiveServer @registry @payload table config
    , stats = statsServer @registry @payload table config
    , listKinds = pure (kindsFor @payload)
    }

-- | Lease visible jobs to a consumer outside a worker pool. Each returned job
-- contains the claim sequence and claimant required for finalization.
claimJobsHandler
  :: forall registry payload m
   . (HasRegistry m registry, JobPayload payload)
  => Text
  -> ArbiterServerConfig m registry
  -> ClaimRequest
  -> Handler (ClaimResponse payload)
claimJobsHandler tableName config req = liftIO $ do
  let schemaName = serverSchema config
      wanted = clamp claimJobsRange (fromMaybe defaultClaimJobs (maxJobs req))
      leaseSecs = realToFrac (clamp leaseSecondsRange (fromMaybe defaultLeaseSeconds (leaseSeconds req)))
  claimant <- UUID.nextRandom
  claimed <- runDb config $ do
    mQueue <- Ops.getQueue schemaName tableName
    if any Queues.paused mQueue
      then pure []
      else do
        (jobs, rejected) <-
          Ops.claimJobsCached (Ops.mkJobStatements @payload schemaName tableName 1 0 leaseSecs claimant) wanted
        traverse_ deadLetter rejected
        pure jobs
  pure $ ClaimResponse claimed
  where
    deadLetter rejected =
      tryReportedOn
        (serverLogConfig config)
        Error
        (deadLetterGates config)
        ("Dead-letter undecodable job in " <> tableName)
        (Ops.deadLetterRejected rejected)

-- | Complete a job that the caller holds. Store an optional result in the
-- parent rollup or archive entry, as worker @ackWith@ does.
ackClaimedJobHandler
  :: forall registry result m
   . (EncodeJobResult result, HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> AckRequest result
  -> Handler NoContent
ackClaimedJobHandler tableName config jobId req =
  withHeldJob @registry tableName config jobId (arLease req) $ \schemaName job ->
    withDbTransaction $ do
      rows <- Ops.ackJob schemaName tableName job
      when (rows > 0) $ storeEncodedResult schemaName job (arResult req >>= encodeJobResult)
      pure rows

-- | Restore the attempt used by a claim. The job becomes available when its
-- lease expires.
nackClaimedJobHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> JobLease
  -> Handler NoContent
nackClaimedJobHandler tableName config jobId lease =
  withHeldJob @registry tableName config jobId lease $ \schemaName job ->
    Ops.nackJob schemaName tableName job

-- | Extend a held HTTP lease, equivalent to a worker heartbeat.
extendClaimedJobHandler
  :: forall registry m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> ExtendRequest
  -> Handler NoContent
extendClaimedJobHandler tableName config jobId req =
  withHeldJob @registry tableName config jobId (erLease req) $ \schemaName job ->
    Ops.setVisibilityTimeout schemaName tableName (realToFrac (clamp leaseSecondsRange (erSeconds req))) job

-- | Finalize the job identified by a lease. Refuse a lease that the caller no
-- longer holds or a lease held by a worker pool. Each statement checks the claim
-- sequence and writes no change after a lease is lost.
withHeldJob
  :: forall registry (payload :: Type) m
   . (HasRegistry m registry)
  => Text
  -> ArbiterServerConfig m registry
  -> Int64
  -> JobLease
  -> (Text -> Job.JobRead (Job.Stored payload) -> m Int64)
  -> Handler NoContent
withHeldJob tableName config jobId lease finalize = do
  let schemaName = serverSchema config
      held job = Job.claimedBy job == Just (jlClaimedBy lease) && Job.claimSeq job == jlClaimSeq lease
      refuse body = Left err409 {errBody = body}

  result <- runDb config $ do
    mJob <- Ops.getJobById @_ @payload schemaName tableName jobId
    case mJob of
      Nothing -> pure $ Left err404 {errBody = "Job not found"}
      Just job
        | not (held job) -> pure $ refuse "Job is not held by this lease"
        | otherwise -> do
            pooled <- Ops.workerRegistered schemaName (jlClaimedBy lease)
            if pooled
              then pure $ refuse "Job is held by a worker pool"
              else do
                rowsAffected <- finalize schemaName job
                pure $
                  if rowsAffected > 0
                    then Right ()
                    else refuse (if Job.suspended job then "Job is suspended" else "Lease no longer held")

  noContentOr result

-- | Maintenance API handler.
maintenanceServer
  :: forall registry m
   . (HL.RegistryAdmissionPolicies registry, HasRegistry m registry, RegistryTables registry)
  => ArbiterServerConfig m registry
  -> MaintenanceAPI (AsServerT Handler)
maintenanceServer config = MaintenanceAPI {runMaintenance = maintenanceHandler @registry config}

-- | Run one maintenance pass. A worker pool's reaper does the same work. Operations
-- exclude each other across callers. An operation another caller is running is
-- skipped and absent from the response.
maintenanceHandler
  :: forall registry m
   . (HL.RegistryAdmissionPolicies registry, HasRegistry m registry, RegistryTables registry)
  => ArbiterServerConfig m registry
  -> Handler MaintenanceResponse
maintenanceHandler config = liftIO $ do
  touched <- newIORef Map.empty
  let report operation rows = liftIO $ modifyIORef' touched (Map.insertWith (+) (maintenanceOpName operation) rows)
      pace =
        MaintenancePace
          { paceWindow = maintenanceInterval config
          , paceSparseWindow = maintenanceSparseInterval config
          , paceBucketIdle = maintenanceBucketIdle config
          }
  failed <-
    runDb config $
      runMaintenancePass (serverLogConfig config) report pace (maintenanceTimeout config)
  ops <- readIORef touched
  pure $ MaintenanceResponse ops (map maintenanceOpName failed)

-- | Bounds on jobs per claim.
claimJobsRange :: (Int, Int)
claimJobsRange = (1, 1000)

-- | Jobs per claim when the request omits it.
defaultClaimJobs :: Int
defaultClaimJobs = 1

-- | Bounds on lease seconds.
leaseSecondsRange :: (Double, Double)
leaseSecondsRange = (1, 3600)

-- | Lease seconds when the request omits it.
defaultLeaseSeconds :: Double
defaultLeaseSeconds = 60

-- | Bounds on page size.
pageLimitRange :: (Int, Int)
pageLimitRange = (1, 1000)

-- | Queues API handler.
queuesServer
  :: forall registry m
   . (HasRegistry m registry, RegistryTables registry)
  => Proxy registry
  -> ArbiterServerConfig m registry
  -> QueuesAPI (AsServerT Handler)
queuesServer registryProxy config =
  let known = registryTableNames registryProxy
   in QueuesAPI
        { listQueues = pure $ QueuesResponse {queues = known}
        , getAllStats = getAllStatsHandler config (registryQueueKinds registryProxy)
        , getDetails = getQueueDetailsHandler config
        , pauseQueue = setQueuePausedHandler config known True
        , resumeQueue = setQueuePausedHandler config known False
        }

-- | Get a queue's operator config.
getQueueDetailsHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Text
  -> Handler (Maybe QueueRow)
getQueueDetailsHandler config queue = do
  let schemaName = serverSchema config
  runDb config $ Ops.getQueue schemaName queue

-- | Flip the @paused@ flag for a queue, validated against the registry. The
-- @arbiter_queues@ row is created lazily on first pause.
setQueuePausedHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> [Text]
  -> Bool
  -> Text
  -> Handler NoContent
setQueuePausedHandler config knownQueues pauseFlag queue = do
  unless (queue `elem` knownQueues) $
    throwError err404 {errBody = "Unknown queue"}
  let schemaName = serverSchema config
  void . runDb config $ Ops.setQueuePaused schemaName queue pauseFlag
  -- The landing overview shows each queue's paused flag.
  invalidate (allQueueStatsCache config)
  NoContent <$ invalidate (queueStatsCache config)

-- | Serve the SSE stream as a raw WAI application. Each client registers on the
-- backend's shared listener for the response's lifetime and gets a @connected@
-- event once its channel is subscribed. If 'enableSSE' is false or the backend
-- has no listener, send one @disabled@ event and close the stream. The admin UI
-- then stops reconnection attempts.
eventsServer
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Tagged Handler Application
eventsServer config = Tagged $ \_req sendResponse -> do
  mListener <- if enableSSE config then runDb config getListener else pure Nothing
  case mListener of
    Nothing -> sendResponse $ responseStream status200 sseHeaders $ \write flush -> do
      write "data: {\"event\":\"disabled\"}\n\n"
      flush
    Just listener -> do
      events <- newTChanIO
      let deliver = atomically . writeTChan events . notificationData
      -- The registration wraps the whole response. The hub is released when the
      -- streaming body never runs.
      withChannels listener (hubLogFor (serverLogConfig config)) [(eventStreamingChannel, deliver)] $ \ready ->
        sendResponse $ responseStream status200 sseHeaders $ \write flush ->
          -- A failed write (client gone) ends the stream. The keepalive comment
          -- every 15s is how a gone client is noticed.
          handleAny (const (pure ())) $ do
            let keepalive = write ": keepalive\n\n" >> flush
                event payload = write ("data: " <> Builder.byteString payload <> "\n\n") >> flush
                awaitReady = timeout sseKeepaliveMicros (atomically (ready >>= check)) >>= maybe (keepalive >> awaitReady) pure
                pump = timeout sseKeepaliveMicros (atomically (readTChan events)) >>= maybe keepalive event >> pump
            -- The connected event says the channel is subscribed.
            awaitReady
            event "{\"event\":\"connected\",\"message\":\"Stream connected\"}"
            pump
  where
    sseHeaders =
      [ ("Content-Type", "text/event-stream")
      , ("Cache-Control", "no-cache")
      , ("Connection", "keep-alive")
      , ("X-Accel-Buffering", "no")
      ]

-- | Idle gap before an SSE client gets a keepalive comment.
sseKeepaliveMicros :: Int
sseKeepaliveMicros = 15_000_000

-- | The event-streaming channel as the hub names it.
eventStreamingChannel :: ByteString
eventStreamingChannel = encodeUtf8 Schema.eventStreamingChannel

-- | Cron API handlers.
cronServer
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> CronAPI (AsServerT Handler)
cronServer config =
  CronAPI
    { listSchedules = listCronSchedulesHandler config
    , updateSchedule = updateCronScheduleHandler config
    , runSchedule = runCronScheduleHandler config
    }

-- | List cron schedules, optionally scoped to a queue.
listCronSchedulesHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Maybe Text
  -> Handler CronSchedulesResponse
listCronSchedulesHandler config mQueue = do
  let schemaName = serverSchema config
  rows <- runDb config $ Ops.listCronSchedules schemaName mQueue
  now <- liftIO getCurrentTime
  pure $ CronSchedulesResponse {cronSchedules = map (cronScheduleView now) rows}

-- | A schedule row with the next tick it fires at. A disabled schedule has none.
cronScheduleView :: UTCTime -> CronScheduleRow -> CronScheduleView
cronScheduleView now row@CS.CronScheduleRow {CS.enabled = isEnabled} =
  CronScheduleView
    { schedule = row
    , nextRunAt = do
        guard isEnabled
        nextRunFromExpression (CS.effectiveTimezone row) (CS.effectiveExpression row) now
    }

-- | Update a cron schedule.
updateCronScheduleHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Text
  -> CronScheduleUpdate
  -> Handler CronScheduleView
updateCronScheduleHandler config name update = do
  let schemaName = serverSchema config
  result <- runDb config $ withDbTransaction $ do
    outcome <- updateCronScheduleChecked name update
    traverse (const (Ops.getCronScheduleByName schemaName name)) outcome

  case result of
    Left err -> throwError err400 {errBody = LBS.fromStrict (encodeUtf8 err)}
    Right Nothing -> throwError err404 {errBody = "Cron schedule not found"}
    Right (Just row) -> flip cronScheduleView row <$> liftIO getCurrentTime

-- | Request an out-of-band run of a cron schedule. A disabled schedule is
-- refused. A schedule with a run already pending is refused.
runCronScheduleHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Text
  -> Handler NoContent
runCronScheduleHandler config name = do
  let schemaName = serverSchema config
  outcome <- runDb config $ Ops.requestCronRun schemaName name
  case outcome of
    Ops.RunReqNotFound -> throwError err404 {errBody = "Cron schedule not found"}
    Ops.RunReqDisabled -> throwError err409 {errBody = "Cron schedule is disabled"}
    Ops.RunReqPending -> throwError err409 {errBody = "Cron schedule already has a run pending"}
    Ops.RunReqStamped -> pure NoContent

-- | Workers API handlers.
workersServer
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> WorkersAPI (AsServerT Handler)
workersServer config =
  WorkersAPI
    { listWorkers = listWorkersHandler config
    , pauseWorker = setWorkerPausedHandler config True
    , resumeWorker = setWorkerPausedHandler config False
    }

-- | List workers, optionally scoped to a queue and/or to recent heartbeats.
listWorkersHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Maybe Text
  -> Maybe Double
  -> Handler WorkersResponse
listWorkersHandler config mQueue mLiveSecs = do
  let schemaName = serverSchema config
  rows <- runDb config $ Ops.listWorkers schemaName mQueue (realToFrac <$> mLiveSecs)
  pure $ WorkersResponse {workers = rows}

-- | Set a worker's @paused@ flag. The worker reconciles its local state on
-- the next heartbeat.
setWorkerPausedHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Bool
  -> UUID
  -> Handler NoContent
setWorkerPausedHandler config pauseFlag workerId = do
  let schemaName = serverSchema config
  runDb config (Ops.setWorkerPaused schemaName workerId pauseFlag) >>= rowsOr404 "Worker not found"

-- | Rate-limit management/observability handlers.
rateLimitsServer
  :: forall registry m
   . (HasRegistry m registry, RegistryTables registry)
  => ArbiterServerConfig m registry
  -> RateLimitsAPI (AsServerT Handler)
rateLimitsServer config =
  RateLimitsAPI
    { listRateLimits = listRateLimitsHandler config
    , listRateLimitBuckets = listRateLimitBucketsHandler config
    , updateRateLimitPolicy = updateRateLimitPolicyHandler config
    , resetRateLimitBuckets = resetRateLimitBucketsHandler config
    }

-- | Liveness and readiness handlers.
healthServer
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> HealthAPI (AsServerT Handler)
healthServer config =
  HealthAPI
    { getHealth = healthHandler config
    , getLiveness = pure LivenessResponse {alive = True}
    }

-- | Readiness for probes and the dashboard. An unreachable database is a 503.
-- Both answers carry the same body.
healthHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Handler HealthResponse
healthHandler config = do
  report <- liftIO (probeHealth config)
  case status report of
    Ok -> pure report
    Down ->
      throwError
        err503
          { errBody = encode report
          , errHeaders = [("Content-Type", "application/json;charset=utf-8")]
          }

-- | Timed database health probe. Cancellation propagates.
probeHealth
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> IO HealthResponse
probeHealth config = cachedFor healthCacheTtl (healthCache config) $ do
  let schemaName = serverSchema config
  started <- getCurrentTime
  probed <- tryAny (timeout healthProbeMicros (runDb config Health.getPgDbHealth))
  finished <- getCurrentTime
  let elapsedMs = realToFrac (diffUTCTime finished started) * 1000
      reached = fromRight Nothing probed
  pure
    HealthResponse
      { status = maybe Down (const Ok) reached
      , schemaName = schemaName
      , checkedAt = finished
      , dbLatencyMs = elapsedMs <$ reached
      , db = join reached
      }

-- | Poll-collapsing TTL for the readiness probe.
healthCacheTtl :: NominalDiffTime
healthCacheTtl = 2

-- | Probe time limit. A pool with no available connection reports @down@ when it
-- lapses. A connect blocked in the driver is not interruptible. The connection
-- string needs its own @connect_timeout@.
healthProbeMicros :: Int
healthProbeMicros = 5_000_000

-- | Poll-collapsing TTL for the dashboard list-policy stats.
policyStatsCacheTtl :: NominalDiffTime
policyStatsCacheTtl = 10

-- | Shorter TTL for the faster-polling all-queues overview.
overviewStatsCacheTtl :: NominalDiffTime
overviewStatsCacheTtl = 5

-- | Default floor between per-queue stats scans.
defaultQueueStatsCacheTtl :: NominalDiffTime
defaultQueueStatsCacheTtl = 2

-- | No minimum gap. An explicit maintenance call runs every operation.
-- Concurrent callers exclude each other on the gate.
defaultMaintenanceInterval :: NominalDiffTime
defaultMaintenanceInterval = 0

-- | Gap the whole-schema operations keep, matching a worker pool's reaper.
defaultMaintenanceSparseInterval :: NominalDiffTime
defaultMaintenanceSparseInterval = 3600

-- | Bucket idle age, matching a worker pool's reaper.
defaultMaintenanceBucketIdle :: NominalDiffTime
defaultMaintenanceBucketIdle = 300

-- | Statement timeout for one maintenance operation.
defaultMaintenanceTimeout :: NominalDiffTime
defaultMaintenanceTimeout = 300

-- | Keyed TTL cache under an epoch bumped by 'invalidate'.
data CacheCell a = CacheCell
  { cacheEntries :: TVar (Word, Map.Map Text (UTCTime, a))
  , cacheFilling :: TVar (Set.Set Text)
  }

newCacheCell :: IO (CacheCell a)
newCacheCell = CacheCell <$> newTVarIO (0, Map.empty) <*> newTVarIO Set.empty

-- | Serve the sole entry of a single-key cell.
cachedFor :: NominalDiffTime -> CacheCell a -> IO a -> IO a
cachedFor ttl cell = cachedForKey ttl cell ""

-- | Serve one key. Concurrent misses on a key collapse onto one @produce@. Its
-- write is skipped when 'invalidate' bumped the epoch meanwhile.
cachedForKey :: NominalDiffTime -> CacheCell a -> Text -> IO a -> IO a
cachedForKey ttl cell key produce
  | ttl <= 0 = produce
  | otherwise = fresh >>= maybe fill pure
  where
    fresh = do
      now <- getCurrentTime
      (_, entries) <- readTVarIO (cacheEntries cell)
      pure $ do
        (storedAt, value) <- Map.lookup key entries
        guard (diffUTCTime now storedAt < ttl)
        pure value
    fill = bracket_ acquire release (fresh >>= maybe store pure)
    acquire = atomically $ do
      inflight <- readTVar (cacheFilling cell)
      check (Set.notMember key inflight)
      modifyTVar' (cacheFilling cell) (Set.insert key)
    release = atomically $ modifyTVar' (cacheFilling cell) (Set.delete key)
    store = do
      (epoch, _) <- readTVarIO (cacheEntries cell)
      value <- produce
      now <- getCurrentTime
      atomically $ modifyTVar' (cacheEntries cell) $ \(current, cached) ->
        if current == epoch then (current, Map.insert key (now, value) cached) else (current, cached)
      pure value

-- | Increment a cache cell's epoch and clear its entries after an operator mutation.
invalidate :: CacheCell a -> Handler ()
invalidate cell = liftIO $ atomically $ modifyTVar' (cacheEntries cell) $ \(epoch, _) -> (epoch + 1, Map.empty)

-- | List policies with bucket stats and currently-throttled job counts.
listRateLimitsHandler
  :: forall registry m
   . (HasRegistry m registry, RegistryTables registry)
  => ArbiterServerConfig m registry
  -> Handler RateLimitPoliciesResponse
listRateLimitsHandler config =
  liftIO $ cachedFor policyStatsCacheTtl (rateLimitPoliciesCache config) $ do
    views <- runDb config HL.listRateLimitPolicies
    pure $ RateLimitPoliciesResponse {policies = views}

-- | List a prefix's buckets with fill levels, paginated (default 100, max 1000).
listRateLimitBucketsHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Text
  -> Maybe Int
  -> Maybe Int
  -> Handler RateLimitBucketsResponse
listRateLimitBucketsHandler config prefix mLimit mOffset = do
  let (limit, offset) = validatePagination 100 mLimit mOffset
  rows <- runDb config (HL.listRateLimitBuckets prefix limit offset)
  pure $ RateLimitBucketsResponse {buckets = rows}

updateThenView
  :: ArbiterServerConfig m registry
  -> m (Maybe a)
  -> LBS.ByteString
  -> Handler a
updateThenView config action notFound = do
  mView <- runDb config action
  maybe (throwError err404 {errBody = notFound}) pure mView

-- | Set or clear a policy's override params, then return the updated view.
updateRateLimitPolicyHandler
  :: forall registry m
   . (HasRegistry m registry, RegistryTables registry)
  => ArbiterServerConfig m registry
  -> Text
  -> RateLimitPolicyUpdate
  -> Handler RateLimitPolicyView
updateRateLimitPolicyHandler config prefix upd@(RateLimitPolicyUpdate mMax mRefill mInterval) = do
  let invalid
        | any (< 0) (join mMax) = Just "override max tokens must be >= 0"
        | any (< 0) (join mRefill) = Just "override refill amount must be >= 0"
        | any (<= 0) (join mInterval) = Just "override interval must be > 0"
        | otherwise = Nothing
  traverse_ (\msg -> throwError err400 {errBody = msg}) invalid
  -- An all-absent patch reads the view without rewriting the row.
  let update = case (mMax, mRefill, mInterval) of
        (Nothing, Nothing, Nothing) -> pure ()
        _ -> void $ HL.updateRateLimitPolicyOverrides prefix upd
  view <- updateThenView config (update >> HL.getRateLimitPolicy prefix) "Rate-limit policy not found"
  invalidate (rateLimitPoliciesCache config)
  pure view

-- | Clear every bucket for a prefix. Returns the number reset. 404s an unknown prefix.
resetRateLimitBucketsHandler
  :: forall registry m
   . (HasRegistry m registry, RegistryTables registry)
  => ArbiterServerConfig m registry
  -> Text
  -> Handler RateLimitResetResponse
resetRateLimitBucketsHandler config prefix = do
  let action =
        HL.rateLimitPolicyExists prefix >>= \exists -> if exists then Just <$> HL.resetRateLimitBuckets prefix else pure Nothing
  count <- updateThenView config action "Rate-limit policy not found"
  invalidate (rateLimitPoliciesCache config)
  pure $ RateLimitResetResponse {reset = count}

-- | Concurrency management/observability handlers.
concurrencyServer
  :: forall registry m
   . (HasRegistry m registry, RegistryTables registry)
  => ArbiterServerConfig m registry
  -> ConcurrencyAPI (AsServerT Handler)
concurrencyServer config =
  ConcurrencyAPI
    { listConcurrency = listConcurrencyHandler config
    , listConcurrencyKeys = listConcurrencyKeysHandler config
    , updateConcurrencyPolicy = updateConcurrencyPolicyHandler config
    , reconcileConcurrency = reconcileConcurrencyHandler config
    }

-- | List pools with their default/override limit and live key/in-flight stats.
listConcurrencyHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Handler ConcurrencyPoliciesResponse
listConcurrencyHandler config =
  liftIO $ cachedFor policyStatsCacheTtl (concurrencyPoliciesCache config) $ do
    views <- runDb config HL.listConcurrencyPolicies
    pure $ ConcurrencyPoliciesResponse {policies = views}

-- | List a prefix's keys with in-flight fill levels, paginated (default 100, max 1000).
listConcurrencyKeysHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Text
  -> Maybe Int
  -> Maybe Int
  -> Handler ConcurrencyKeysResponse
listConcurrencyKeysHandler config prefix mLimit mOffset = do
  let (limit, offset) = validatePagination 100 mLimit mOffset
  rows <- runDb config (HL.listConcurrencyKeys prefix limit offset)
  pure $ ConcurrencyKeysResponse {keys = rows}

-- | Set or clear a pool's override limit, then return the updated view.
updateConcurrencyPolicyHandler
  :: forall registry m
   . (HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> Text
  -> ConcurrencyPolicyUpdate
  -> Handler ConcurrencyPolicyView
updateConcurrencyPolicyHandler config prefix upd@(ConcurrencyPolicyUpdate mLimit) = do
  when (any (< 0) (join mLimit)) $ throwError err400 {errBody = "override limit must be >= 0"}
  -- An absent overrideLimit reads the view without rewriting the row.
  let action = case mLimit of
        Nothing -> HL.getConcurrencyPolicy prefix
        Just _ -> HL.updateConcurrencyPolicyOverrides prefix upd >> HL.getConcurrencyPolicy prefix
  view <- updateThenView config action "Concurrency pool not found"
  invalidate (concurrencyPoliciesCache config)
  pure view

-- | Recompute every key's in-flight count from live jobs. Returns rows repaired.
reconcileConcurrencyHandler
  :: forall registry m
   . (HasRegistry m registry, RegistryTables registry)
  => ArbiterServerConfig m registry
  -> Handler ConcurrencyReconcileResponse
reconcileConcurrencyHandler config = do
  repaired <- runDb config HL.reconcileConcurrencyCounts
  invalidate (concurrencyPoliciesCache config)
  pure $ ConcurrencyReconcileResponse {reconciled = repaired}

-- | Server for the shared top-level routes.
sharedServer
  :: forall registry m
   . (HL.RegistryAdmissionPolicies registry, HasRegistry m registry, RegistryTables registry)
  => ArbiterServerConfig m registry
  -> ServerT SharedAPI Handler
sharedServer config =
  queuesServer @registry (Proxy @registry) config
    :<|> maintenanceServer @registry config
    :<|> eventsServer config
    :<|> cronServer config
    :<|> workersServer config
    :<|> rateLimitsServer config
    :<|> concurrencyServer config
    :<|> healthServer config

-- | Builds a registry's per-queue server implementations.
class BuildServer registry (reg :: JobPayloadRegistry) where
  buildServer :: (HasRegistry m registry) => ArbiterServerConfig m registry -> ServerT (RegistryToAPI reg) Handler

-- The empty registry builds the shared top-level routes alone.
instance
  (HL.RegistryAdmissionPolicies registry, RegistryTables registry)
  => BuildServer registry '[]
  where
  buildServer = sharedServer

-- One table builds its endpoints, then the rest of the registry.
instance
  ( BuildServer registry rest
  , EncodeJobResult (SpecResult spec)
  , JobPayload (SpecPayload spec)
  , KnownSymbol (SpecName spec)
  )
  => BuildServer registry (spec ': rest)
  where
  buildServer config =
    let tableName = T.pack $ symbolVal (Proxy @(SpecName spec))
     in tableServer @registry @(SpecPayload spec) @(SpecResult spec) tableName config
          :<|> buildServer @registry @rest config

-- | Complete Arbiter server at @\/api\/v1\/...@
arbiterServer
  :: forall registry m
   . (BuildServer registry registry, HasRegistry m registry)
  => ArbiterServerConfig m registry
  -> ServerT (ArbiterAPI registry) Handler
arbiterServer = buildServer @registry @registry

-- | Hoisted server for integration into a route tree using a custom monad.
arbiterServerHoisted
  :: forall registry m n
   . ( BuildServer registry registry
     , HasRegistry m registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => (forall x. Handler x -> n x)
  -> ArbiterServerConfig m registry
  -> ServerT (ArbiterAPI registry) n
arbiterServerHoisted natTrans config =
  hoistServer (Proxy @(ArbiterAPI registry)) natTrans (arbiterServer config)

-- | Convert to WAI Application. Each 'QueueWithResult' result type needs
-- @FromJSON@ and @ToJSON@.
arbiterApp
  :: forall registry m
   . ( BuildServer registry registry
     , HasRegistry m registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => ArbiterServerConfig m registry
  -> Application
arbiterApp config =
  serve (Proxy @(ArbiterAPI registry)) (arbiterServer config)

-- | Run the API server on a port.
runArbiterAPI
  :: forall registry m
   . ( BuildServer registry registry
     , HasRegistry m registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => Port
  -> ArbiterServerConfig m registry
  -> IO ()
runArbiterAPI port config = do
  putStrLn $ "Starting Arbiter API server on port " <> show port
  let settings = setPort port defaultSettings
  runSettings settings (arbiterApp config)

-- | Remove an empty search parameter.
nonBlank :: Maybe Text -> Maybe Text
nonBlank = mfilter (not . T.null . T.strip)

-- | Clamp pagination parameters to a limit of 1 to 1000 and a non-negative offset.
validatePagination :: Int -> Maybe Int -> Maybe Int -> (Int, Int)
validatePagination defLimit mLimit mOffset =
  (clamp pageLimitRange (fromMaybe defLimit mLimit), max 0 (fromMaybe 0 mOffset))

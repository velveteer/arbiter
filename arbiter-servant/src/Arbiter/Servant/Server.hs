{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | REST API server for the Arbiter job queue (SimpleDb backend).
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
import Arbiter.Core.Health qualified as Health
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types (DedupKey (..), JobPayload, JobStatus, isRollup, kindsFor)
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobResult (EncodeJobResult, encodeJobResult)
import Arbiter.Core.MonadArbiter (withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.QueueRegistry (JobPayloadRegistry, RegistryTables (..), SpecName, SpecPayload, SpecResult)
import Arbiter.Core.Queues qualified as Queues
import Arbiter.Core.Sql.Jobs (ArchiveSortColumn, DLQSortColumn, JobFilter (..), JobSortColumn, SortDir)
import Arbiter.Core.Trace (withPublishSpan)
import Arbiter.Simple (SimpleConnectionPool (..), SimpleDb, SimpleEnv (..), createSimpleEnvWithConfig, runSimpleDb)
import Arbiter.Worker (MaintenancePace (..), runMaintenancePass, storeEncodedResult)
import Arbiter.Worker.Config (maintenanceOpName)
import Arbiter.Worker.Cron (nextRunFromExpression, updateCronScheduleChecked)
import Arbiter.Worker.Logger (defaultLogConfig)
import Control.Concurrent (forkIOWithUnmask, threadDelay)
import Control.Concurrent.Async (race_)
import Control.Concurrent.MVar (MVar, modifyMVar, modifyMVar_, newMVar)
import Control.Concurrent.STM
  ( TChan
  , TVar
  , atomically
  , check
  , dupTChan
  , modifyTVar'
  , newBroadcastTChanIO
  , newTVarIO
  , readTChan
  , readTVar
  , readTVarIO
  , writeTChan
  )
import Control.Exception (SomeAsyncException, SomeException, bracket, bracket_, fromException, handle, throwIO, try)
import Control.Monad (forever, guard, join, mfilter, unless, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (encode)
import Data.ByteString (ByteString)
import Data.ByteString.Builder qualified as Builder
import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Lazy qualified as LBS
import Data.IORef (modifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust)
import Data.Pool qualified as Pool
import Data.Set qualified as Set
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Data.Time (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Data.UUID.Types (UUID)
import Data.UUID.V4 qualified as UUID
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Notification (Notification (..), getNotification)
import GHC.TypeLits (KnownSymbol, symbolVal)
import Network.HTTP.Types (status200)
import Network.Wai (responseStream)
import Network.Wai.Handler.Warp (Port, defaultSettings, runSettings, setPort)
import Servant
import Servant.Server.Generic (AsServerT)
import System.IO (stderr)
import System.Timeout (timeout)

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

-- | Configuration for the API server.
data ArbiterServerConfig (registry :: JobPayloadRegistry) = ArbiterServerConfig
  { serverEnv :: SimpleEnv registry
  -- ^ The SimpleEnv containing schema and connection pool
  , enableSSE :: Bool
  -- ^ Enable the Server-Sent Events streaming endpoint. When 'False', the
  -- @\/events\/stream@ endpoint returns one \"disabled\" event and closes.
  -- The admin UI then polls. Default: 'True'.
  , sseHub :: MVar (Maybe SSEHub)
  -- ^ Lazily started SSE broadcast hub, shared by all clients. The first
  -- subscriber starts it. The last disconnect tears it down and releases its
  -- @LISTEN@ connection.
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
  }

-- | A running SSE broadcast hub: the channel every client duplicates, and the
-- live subscriber count the listener watches to release itself at zero.
data SSEHub = SSEHub
  { hubChan :: TChan ByteString
  , hubRefs :: TVar Int
  }

-- | The schema every handler's statements run against.
serverSchema :: ArbiterServerConfig registry -> Text
serverSchema = schema . serverEnv

-- | Run a statement on the server's own pool.
runDb :: (MonadIO n) => ArbiterServerConfig registry -> SimpleDb registry IO a -> n a
runDb config = liftIO . runSimpleDb (serverEnv config)

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
  :: forall payload registry
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> (Text -> SimpleDb registry IO Int64)
  -> (Job.JobRead payload -> LBS.ByteString)
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

-- | Small pool configuration for admin API traffic.
serverPoolConfig :: PoolConfig
serverPoolConfig =
  PoolConfig
    { poolSize = 10
    , poolIdleTimeout = 60
    , poolStripes = Just 1
    }

-- | Create an 'ArbiterServerConfig' with a connection pool of its own. SSE needs the
-- event-streaming triggers, which 'Arbiter.Migrations.runMigrationsForRegistry' installs
-- when @enableEventStreaming@ is set.
initArbiterServer
  :: forall registry
   . Proxy registry
  -> ByteString
  -> Text
  -> IO (ArbiterServerConfig registry)
initArbiterServer _proxy connStr schemaName = do
  env <- createSimpleEnvWithConfig (Proxy @registry) connStr schemaName serverPoolConfig
  hub <- newMVar Nothing
  rlCache <- newCacheCell
  ccCache <- newCacheCell
  statsCache <- newCacheCell
  perQueueCache <- newCacheCell
  healthCell <- newCacheCell
  pure
    ArbiterServerConfig
      { serverEnv = env
      , enableSSE = True
      , sseHub = hub
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
      }

-- | Jobs API handlers for a specific table.
jobsServer
  :: forall registry payload result
   . (EncodeJobResult result, JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> JobsAPI payload result (AsServerT Handler)
jobsServer table config =
  JobsAPI
    { listJobs = listJobsHandler @registry @payload table config
    , insertJob = insertJobHandler @registry @payload table config
    , insertJobsBatch = insertJobsBatchHandler @registry @payload table config
    , getJob = getJobHandler @registry @payload table config
    , cancelJob = cancelJobHandler @registry table config
    , forceCancelJob = forceCancelJobHandler @registry table config
    , promoteJob = promoteJobHandler @registry @payload table config
    , moveToDLQ = moveToDLQHandler @registry @payload table config
    , pauseChildren = pauseChildrenHandler @registry table config
    , resumeChildren = resumeChildrenHandler @registry table config
    , suspendJob = suspendJobHandler @registry @payload table config
    , resumeJob = resumeJobHandler @registry @payload table config
    , ackClaimedJob = ackClaimedJobHandler @registry @payload @result table config
    , nackClaimedJob = nackClaimedJobHandler @registry @payload table config
    , extendClaimedJob = extendClaimedJobHandler @registry @payload table config
    }

-- | List jobs with pagination and composable filters.
listJobsHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
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
    if null page || not hasParents
      then pure (page, matching, Map.empty, Map.empty)
      else do
        children <- Ops.countChildrenBatch schemaName tableName jobIds
        dlqChildren <- Ops.countDLQChildrenBatch schemaName tableName jobIds
        pure (page, matching, children, dlqChildren)

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
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> ApiJobWrite payload
  -> Handler (JobResponse (ApiJob payload))
insertJobHandler tableName config (ApiJobWrite jobWrite) = do
  let schemaName = serverSchema config
  mJob <- runDb config $ withPublishSpan tableName [jobWrite] $ do
    inserted <- Ops.insertJob schemaName tableName jobWrite
    case (inserted, Job.dedupKey jobWrite) of
      (Just fresh, _) -> pure (Just fresh)
      (Nothing, Just (IgnoreDuplicate duplicateKey)) -> Ops.getJobByDedupKey schemaName tableName duplicateKey
      _ -> pure Nothing
  case mJob of
    Just found -> pure $ JobResponse (ApiJob found)
    Nothing ->
      throwError err409 {errBody = "Replace blocked: existing job is actively claimed, force-cancel flagged, or has children"}

-- | Insert multiple jobs in a single batch operation.
insertJobsBatchHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> BatchInsertRequest payload
  -> Handler (BatchInsertResponse payload)
insertJobsBatchHandler tableName config (BatchInsertRequest jobWrites) = do
  let schemaName = serverSchema config
      writes = map unApiJobWrite jobWrites

  inserted <-
    runDb config
      $ withPublishSpan tableName writes
      $ Ops.insertJobsBatch schemaName tableName writes
  let apiJobs = map ApiJob inserted
  pure $ BatchInsertResponse {inserted = apiJobs, insertedCount = length apiJobs}

-- | Fetch a job by id.
getJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler (JobResponse (ApiJobWithStatus payload))
getJobHandler tableName config jobId = do
  let schemaName = serverSchema config
  mJob <- runDb config $ Ops.getJobByIdWithStatus schemaName tableName jobId
  case mJob of
    Nothing -> throwError err404 {errBody = "Job not found"}
    Just (found, jobStatus) -> pure $ JobResponse {job = ApiJobWithStatus found jobStatus}

-- | Cancel a job (delete it from the queue).
cancelJobHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
cancelJobHandler tableName config jobId = do
  let schemaName = serverSchema config
  runDb config (Ops.cancelJobCascade schemaName tableName jobId) >>= rowsOr404 "Job not found"

-- | Cascade-cancel a job and async-cancel any in-flight handlers via NOTIFY.
forceCancelJobHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
forceCancelJobHandler tableName config jobId = do
  let schemaName = serverSchema config
  runDb config (Ops.forceCancelJob schemaName tableName jobId) >>= rowsOr404 "Job not found"

-- | Promote a job (make it immediately visible).
promoteJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
promoteJobHandler tableName config jobId =
  mutateJob @payload tableName config jobId (\schemaName -> Ops.promoteJob schemaName tableName jobId) refuse
  where
    refuse job
      | Job.suspended job = "Job is suspended - use resume endpoint"
      | isJust (Job.claimedBy job) = "Job is in flight - wait for its lease to lapse"
      | otherwise = "Job is already visible"

-- | Move a job to the dead letter queue.
moveToDLQHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
moveToDLQHandler tableName config jobId = do
  let schemaName = serverSchema config
  result <- runDb config $ withDbTransaction $ do
    mJob <- Ops.getJobById @_ @payload schemaName tableName jobId
    case mJob of
      Nothing -> pure Nothing
      Just job ->
        Just <$> Ops.moveToDLQ Ops.TakeLocks schemaName tableName "Manually moved to DLQ via admin API" job

  case result of
    Nothing -> throwError err404 {errBody = "Job not found"}
    Just 0 -> throwError err409 {errBody = "Job was concurrently modified"}
    Just _ -> pure NoContent

-- | Pause all children of a parent job.
pauseChildrenHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
pauseChildrenHandler tableName config jobId = do
  let schemaName = serverSchema config
  void . runDb config $
    Ops.pauseChildren schemaName tableName jobId

  -- Pausing nothing is a success. The children may be in flight, suspended or done.
  pure NoContent

-- | Resume all suspended children of a parent job.
resumeChildrenHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
resumeChildrenHandler tableName config jobId = do
  let schemaName = serverSchema config
  void . runDb config $
    Ops.resumeChildren schemaName tableName jobId

  -- Resuming nothing is a success. The children may be unsuspended or done.
  pure NoContent

-- | Suspend a job (make it unclaimable).
suspendJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
suspendJobHandler tableName config jobId =
  mutateJob @payload tableName config jobId (\schemaName -> Ops.suspendJob schemaName tableName jobId) refuse
  where
    refuse job
      | Job.suspended job = "Job is already suspended"
      | otherwise = "Job is in-flight - cannot suspend"

-- | Resume a suspended job, making it claimable again. Refuses a finalizer with children
-- still running.
resumeJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
resumeJobHandler tableName config jobId =
  mutateJob @payload tableName config jobId (\schemaName -> Ops.resumeJob schemaName tableName jobId) refuse
  where
    refuse job
      | not (Job.suspended job) = "Job is not suspended"
      | isRollup job = "Cannot resume a rollup finalizer with active children"
      | otherwise = "Job could not be resumed (concurrent modification)"

-- | DLQ API handlers for a specific table.
dlqServer
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
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
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
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

  (dlqJobs, total) <- runDb config $ withDbTransaction $ do
    page <- Ops.listDLQFilteredOrdered schemaName tableName filters mSortBy mSortDir limit offset
    matching <- Ops.countDLQFiltered schemaName tableName filters
    pure (page, matching)

  let apiDlqJobs = map ApiDLQJob dlqJobs
  pure $
    DLQResponse
      { dlqJobs = apiDlqJobs
      , dlqTotal = fromIntegral total
      , dlqOffset = offset
      , dlqLimit = limit
      }

-- | Retry a DLQ job back into the main queue. 409 when its parent is gone.
retryFromDLQHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
retryFromDLQHandler tableName config dlqId = do
  let schemaName = serverSchema config
  result <- runDb config $ withDbTransaction $ do
    mJob <- Ops.retryFromDLQ @_ @payload schemaName tableName dlqId
    case mJob of
      Just _ -> pure (Right ())
      Nothing -> Left <$> Ops.dlqJobExists schemaName tableName dlqId

  case result of
    Right () -> pure NoContent
    Left True -> throwError err409 {errBody = "Cannot retry: parent job no longer exists (not in queue or DLQ)"}
    Left False -> throwError err404 {errBody = "DLQ job not found"}

-- | Delete a job from DLQ permanently.
deleteDLQHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
deleteDLQHandler tableName config dlqId = do
  let schemaName = serverSchema config
  runDb config (Ops.deleteDLQJob schemaName tableName dlqId) >>= rowsOr404 "DLQ job not found"

-- | Batch delete jobs from DLQ permanently.
deleteDLQBatchHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> BatchDeleteRequest
  -> Handler BatchDeleteResponse
deleteDLQBatchHandler tableName config (BatchDeleteRequest dlqIds) = do
  let schemaName = serverSchema config
  rowsDeleted <- runDb config $ Ops.deleteDLQJobsBatch schemaName tableName dlqIds
  pure $ BatchDeleteResponse {deleted = rowsDeleted}

-- | Archive API handler for a specific table.
archiveServer
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
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
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
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
      { archiveJobs = map ApiArchiveJob archived
      , archiveTotal = fromIntegral total
      , archiveOffset = offset
      , archiveLimit = limit
      }

-- | Re-enqueue an archived job as a fresh job. 404 if the archive row is gone.
reEnqueueArchiveHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
reEnqueueArchiveHandler tableName config archiveId = do
  let schemaName = serverSchema config
  mJob <- runDb config $ Ops.reEnqueueFromArchive @_ @payload schemaName tableName archiveId
  case mJob of
    Just _ -> pure NoContent
    Nothing -> throwError err404 {errBody = "Archived job not found"}

-- | Purge one archived job by its archive primary key.
deleteArchiveHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
deleteArchiveHandler tableName config archiveId = do
  let schemaName = serverSchema config
  runDb config (Ops.deleteArchiveJob schemaName tableName archiveId) >>= rowsOr404 "Archived job not found"

-- | Bulk-purge archived jobs by archive primary key.
deleteArchiveBatchHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> BatchDeleteRequest
  -> Handler BatchDeleteResponse
deleteArchiveBatchHandler tableName config (BatchDeleteRequest archiveIds) = do
  let schemaName = serverSchema config
  rowsDeleted <- runDb config $ Ops.deleteArchiveJobsBatch schemaName tableName archiveIds
  pure $ BatchDeleteResponse {deleted = rowsDeleted}

-- | Stats API handler for a specific table.
statsServer
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> StatsAPI (AsServerT Handler)
statsServer tableName config =
  StatsAPI
    { getStats = getStatsHandler @registry tableName (kindsFor @payload) config
    }

-- | Get queue statistics.
getStatsHandler
  :: forall registry
   . Text
  -> [Text]
  -> ArbiterServerConfig registry
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
  :: forall registry
   . ArbiterServerConfig registry
  -> [(Text, [Text])]
  -> Handler AllStatsResponse
getAllStatsHandler config queueKinds =
  liftIO $ cachedFor overviewStatsCacheTtl (allQueueStatsCache config) $ do
    let schemaName = serverSchema config
    AllStatsResponse <$> runDb config (Ops.getAllQueueStats schemaName queueKinds)

-- | Table API handlers for a specific table.
tableServer
  :: forall registry payload result
   . (EncodeJobResult result, JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
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
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> ClaimRequest
  -> Handler (ClaimResponse payload)
claimJobsHandler tableName config req = liftIO $ do
  let schemaName = serverSchema config
      maxJobs = clamp 1 1000 (fromMaybe 1 (crMaxJobs req))
      leaseSecs = realToFrac (clamp 1 3600 (fromMaybe 60 (crLeaseSeconds req)))
  claimant <- UUID.nextRandom
  jobs <- runDb config $ do
    mQueue <- Ops.getQueue schemaName tableName
    if maybe False Queues.paused mQueue
      then pure []
      else Ops.claimNextVisibleJobsAs @_ @payload schemaName tableName maxJobs leaseSecs claimant
  pure $ ClaimResponse (map ApiJob jobs)

-- | Complete a job that the caller holds. Store an optional result in the
-- parent rollup or archive entry, as worker @ackWith@ does.
ackClaimedJobHandler
  :: forall registry payload result
   . (EncodeJobResult result, JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> AckRequest result
  -> Handler NoContent
ackClaimedJobHandler tableName config jobId req =
  withHeldJob @registry @payload tableName config jobId (arLease req) $ \schemaName job ->
    withDbTransaction $ do
      rows <- Ops.ackJob schemaName tableName job
      when (rows > 0) $ storeEncodedResult schemaName job (arResult req >>= encodeJobResult)
      pure rows

-- | Restore the attempt used by a claim. The job becomes available when its
-- lease expires.
nackClaimedJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> JobLease
  -> Handler NoContent
nackClaimedJobHandler tableName config jobId lease =
  withHeldJob @registry @payload tableName config jobId lease $ \schemaName job ->
    Ops.nackJob schemaName tableName job

-- | Extend a held lease. This is the HTTP consumer equivalent of a worker heartbeat.
extendClaimedJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> ExtendRequest
  -> Handler NoContent
extendClaimedJobHandler tableName config jobId req =
  withHeldJob @registry @payload tableName config jobId (erLease req) $ \schemaName job ->
    Ops.setVisibilityTimeout schemaName tableName (realToFrac (clamp 1 3600 (erSeconds req))) job

-- | Finalize the job identified by a lease. Refuse a lease that the caller no
-- longer holds or a lease held by a worker pool. Each statement checks the claim
-- sequence and writes no change after a lease is lost.
withHeldJob
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> JobLease
  -> (Text -> Job.JobRead payload -> SimpleDb registry IO Int64)
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
  :: forall registry
   . (HL.RegistryAdmissionPolicies registry, RegistryTables registry)
  => ArbiterServerConfig registry
  -> MaintenanceAPI (AsServerT Handler)
maintenanceServer config = MaintenanceAPI {runMaintenance = maintenanceHandler @registry config}

-- | Run one maintenance pass. A worker pool's reaper does the same work. Operations
-- exclude each other across callers. An operation another caller is running is
-- skipped and absent from the response.
maintenanceHandler
  :: forall registry
   . (HL.RegistryAdmissionPolicies registry, RegistryTables registry)
  => ArbiterServerConfig registry
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
      runMaintenancePass defaultLogConfig report pace (maintenanceTimeout config)
  ops <- readIORef touched
  pure $ MaintenanceResponse ops (map maintenanceOpName failed)

-- | Hold a caller-supplied value inside the range an endpoint accepts.
clamp :: (Ord a) => a -> a -> a -> a
clamp lower upper = max lower . min upper

-- | Queues API handler.
queuesServer
  :: forall registry
   . (RegistryTables registry)
  => Proxy registry
  -> ArbiterServerConfig registry
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
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Handler (Maybe QueueRow)
getQueueDetailsHandler config queue = do
  let schemaName = serverSchema config
  runDb config $ Ops.getQueue schemaName queue

-- | Flip the @paused@ flag for a queue, validated against the registry. The
-- @arbiter_queues@ row is created lazily on first pause.
setQueuePausedHandler
  :: forall registry
   . ArbiterServerConfig registry
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
  invalidate (queueStatsCache config)
  pure NoContent

-- | Serve the SSE stream as a raw WAI application. Flush after each event and
-- send a keepalive comment every 15 seconds. Each client reads a duplicate of
-- the shared broadcast hub and does not use a PostgreSQL pool connection. If
-- 'enableSSE' is false, send one @disabled@ event and close the stream. The
-- admin UI then stops reconnection attempts.
eventsServer
  :: forall registry
   . ArbiterServerConfig registry
  -> Tagged Handler Application
eventsServer config = Tagged $ \_req sendResponse ->
  if not (enableSSE config)
    then sendResponse $ responseStream status200 sseHeaders $ \write flush -> do
      write "data: {\"event\":\"disabled\"}\n\n"
      flush
    else
      -- 'bracket' pairs the refcount increment with its decrement around the
      -- whole response. The hub is released when the streaming body never runs.
      bracket (subscribeSSE config) (maybe (pure ()) (const (unsubscribeSSE config))) $ \mSub ->
        case mSub of
          Nothing -> sendResponse $ responseStream status200 sseHeaders $ \write flush -> do
            write "data: {\"event\":\"error\",\"message\":\"No connection pool available\"}\n\n"
            flush
          Just sub -> sendResponse $ responseStream status200 sseHeaders $ \write flush ->
            -- A failed write (client gone) ends the stream. The enclosing bracket
            -- then drops this subscriber's refcount.
            handle swallowSync $ do
              write "data: {\"event\":\"connected\",\"message\":\"Stream connected\"}\n\n"
              flush
              -- Read this client's channel with a 15s keepalive heartbeat.
              let go = do
                    mPayload <- timeout 15_000_000 (atomically (readTChan sub))
                    case mPayload of
                      Just payload -> do
                        write ("data: " <> Builder.byteString payload <> "\n\n")
                        flush
                        go
                      Nothing -> do
                        write ": keepalive\n\n"
                        flush
                        go
              go
  where
    sseHeaders =
      [ ("Content-Type", "text/event-stream")
      , ("Cache-Control", "no-cache")
      , ("Connection", "keep-alive")
      , ("X-Accel-Buffering", "no")
      ]

swallowSync :: SomeException -> IO ()
swallowSync exception = case fromException exception of
  Just (_ :: SomeAsyncException) -> throwIO exception
  Nothing -> pure ()

-- | Subscribe to the shared SSE hub, returning a duplicated channel to stream
-- from. The first subscriber starts the hub (one @LISTEN@ connection). Later
-- subscribers bump the refcount. 'Nothing' when there is no pool.
subscribeSSE :: ArbiterServerConfig registry -> IO (Maybe (TChan ByteString))
subscribeSSE config =
  modifyMVar (sseHub config) $ \mhub -> case mhub of
    Just hub -> do
      sub <- atomically $ do
        modifyTVar' (hubRefs hub) (+ 1)
        dupTChan (hubChan hub)
      pure (Just hub, Just sub)
    Nothing -> case connectionPool (simplePool (serverEnv config)) of
      Nothing -> pure (Nothing, Nothing)
      Just pool -> do
        broadcast <- newBroadcastTChanIO
        refs <- newTVarIO 1
        sub <- atomically (dupTChan broadcast)
        -- subscribeSSE runs masked as a 'bracket' acquire. The listener is
        -- forked with an explicit unmask.
        void $ forkIOWithUnmask $ \unmask -> unmask (sseListenerLoop pool broadcast refs)
        pure (Just (SSEHub broadcast refs), Just sub)

-- | Drop one subscriber. When the count reaches zero the hub is removed from
-- the config. The listener observes the same count and releases its connection
-- (see 'sseListenerLoop').
unsubscribeSSE :: ArbiterServerConfig registry -> IO ()
unsubscribeSSE config =
  modifyMVar_ (sseHub config) $ \mhub -> case mhub of
    Nothing -> pure Nothing
    Just hub -> do
      remaining <- atomically $ do
        modifyTVar' (hubRefs hub) (subtract 1)
        readTVar (hubRefs hub)
      pure $ if remaining <= 0 then Nothing else Just hub

-- | The single listener. It runs @LISTEN@ on one borrowed connection and fans
-- every notification into the broadcast channel, raced against the subscriber
-- count reaching zero. On connection loss it destroys the dead resource and
-- reconnects. When the count hits zero the race ends, the 'bracket' releases
-- the connection, and the thread exits.
sseListenerLoop :: Pool.Pool PG.Connection -> TChan ByteString -> TVar Int -> IO ()
sseListenerLoop pool broadcast refs = do
  backoff <- newIORef baseBackoff
  race_ waitForIdle (pump backoff)
  where
    baseBackoff = 1_000_000 -- 1s
    maxBackoff = 30_000_000 -- 30s
    waitForIdle = atomically $ do
      subscribers <- readTVar refs
      check (subscribers == 0)
    pump backoff =
      forever
        $ handle (onError backoff)
        $ bracket
          (Pool.takeResource pool)
          (\(conn, localPool) -> Pool.destroyResource pool localPool conn)
        $ \(conn, _) -> do
          _ <- PG.execute_ conn $ "LISTEN " <> fromString (T.unpack Schema.eventStreamingChannel)
          writeIORef backoff baseBackoff -- reset on connect
          forever $ do
            notification <- getNotification conn
            atomically $ writeTChan broadcast (notificationData notification)
    -- Re-raise async exceptions. On a sync error log it and retry with capped
    -- exponential backoff.
    onError backoff exception = case fromException exception of
      Just (_ :: SomeAsyncException) -> throwIO exception
      Nothing -> do
        delay <- readIORef backoff
        BS8.hPutStr stderr . encodeUtf8 $
          "[arbiter:sse] listener error, retrying in "
            <> T.pack (show (delay `div` 1_000_000))
            <> "s: "
            <> T.pack (show exception)
            <> "\n"
        threadDelay delay
        writeIORef backoff (min maxBackoff (delay * 2))

-- | Cron API handlers.
cronServer
  :: forall registry
   . ArbiterServerConfig registry
  -> CronAPI (AsServerT Handler)
cronServer config =
  CronAPI
    { listSchedules = listCronSchedulesHandler config
    , updateSchedule = updateCronScheduleHandler config
    , runSchedule = runCronScheduleHandler config
    }

-- | List cron schedules, optionally scoped to a queue.
listCronSchedulesHandler
  :: forall registry
   . ArbiterServerConfig registry
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
  :: forall registry
   . ArbiterServerConfig registry
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
  :: forall registry
   . ArbiterServerConfig registry
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
  :: forall registry
   . ArbiterServerConfig registry
  -> WorkersAPI (AsServerT Handler)
workersServer config =
  WorkersAPI
    { listWorkers = listWorkersHandler config
    , pauseWorker = setWorkerPausedHandler config True
    , resumeWorker = setWorkerPausedHandler config False
    }

-- | List workers, optionally scoped to a queue and/or to recent heartbeats.
listWorkersHandler
  :: forall registry
   . ArbiterServerConfig registry
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
  :: forall registry
   . ArbiterServerConfig registry
  -> Bool
  -> UUID
  -> Handler NoContent
setWorkerPausedHandler config pauseFlag workerId = do
  let schemaName = serverSchema config
  runDb config (Ops.setWorkerPaused schemaName workerId pauseFlag) >>= rowsOr404 "Worker not found"

-- | Rate-limit management/observability handlers.
rateLimitsServer
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
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
  :: forall registry
   . ArbiterServerConfig registry
  -> HealthAPI (AsServerT Handler)
healthServer config =
  HealthAPI
    { getHealth = healthHandler config
    , getLiveness = pure LivenessResponse {alive = True}
    }

-- | Readiness for probes and the dashboard. An unreachable database is a 503.
-- Both answers carry the same body.
healthHandler
  :: forall registry
   . ArbiterServerConfig registry
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

-- | Time a database round-trip and report what it says about itself. Cancellation
-- still propagates.
probeHealth
  :: forall registry
   . ArbiterServerConfig registry
  -> IO HealthResponse
probeHealth config = cachedFor healthCacheTtl (healthCache config) $ do
  let schemaName = serverSchema config
  started <- getCurrentTime
  probed <- try @SomeException $ timeout healthProbeMicros (runDb config Health.getPgDbHealth)
  either swallowSync (const (pure ())) probed
  finished <- getCurrentTime
  let elapsedMs = realToFrac (diffUTCTime finished started) * 1000
  let reached = join (eitherToMaybe probed)
  pure
    HealthResponse
      { status = maybe Down (const Ok) reached
      , schemaName = schemaName
      , checkedAt = finished
      , dbLatencyMs = elapsedMs <$ reached
      , db = join reached
      }
  where
    eitherToMaybe = either (const Nothing) Just

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

-- | Bump a cache cell's epoch and drop its entries, after an operator mutation.
invalidate :: CacheCell a -> Handler ()
invalidate cell = liftIO $ atomically $ modifyTVar' (cacheEntries cell) $ \(epoch, _) -> (epoch + 1, Map.empty)

-- | List policies with bucket stats and currently-throttled job counts.
listRateLimitsHandler
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
  -> Handler RateLimitPoliciesResponse
listRateLimitsHandler config =
  liftIO $ cachedFor policyStatsCacheTtl (rateLimitPoliciesCache config) $ do
    views <- runDb config HL.listRateLimitPolicies
    pure $ RateLimitPoliciesResponse {policies = views}

-- | List a prefix's buckets with fill levels, paginated (default 100, max 1000).
listRateLimitBucketsHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Maybe Int
  -> Maybe Int
  -> Handler RateLimitBucketsResponse
listRateLimitBucketsHandler config prefix mLimit mOffset = do
  let (limit, offset) = validatePagination 100 mLimit mOffset
  rows <- runDb config (HL.listRateLimitBuckets prefix limit offset)
  pure $ RateLimitBucketsResponse {buckets = rows}

updateThenView
  :: ArbiterServerConfig registry
  -> SimpleDb registry IO (Maybe a)
  -> LBS.ByteString
  -> Handler a
updateThenView config action notFound = do
  mView <- runDb config action
  maybe (throwError err404 {errBody = notFound}) pure mView

-- | Set or clear a policy's override params, then return the updated view.
updateRateLimitPolicyHandler
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
  -> Text
  -> RateLimitPolicyUpdate
  -> Handler RateLimitPolicyView
updateRateLimitPolicyHandler config prefix upd@(RateLimitPolicyUpdate mMax mRefill mInterval) = do
  let invalid
        | maybe False (< 0) (join mMax) = Just "override max tokens must be >= 0"
        | maybe False (< 0) (join mRefill) = Just "override refill amount must be >= 0"
        | maybe False (<= 0) (join mInterval) = Just "override interval must be > 0"
        | otherwise = Nothing
  case invalid of
    Just msg -> throwError err400 {errBody = msg}
    Nothing -> do
      -- An all-absent patch reads the view without rewriting the row.
      let update = case (mMax, mRefill, mInterval) of
            (Nothing, Nothing, Nothing) -> pure ()
            _ -> void $ HL.updateRateLimitPolicyOverrides prefix upd
      view <- updateThenView config (update >> HL.getRateLimitPolicy prefix) "Rate-limit policy not found"
      invalidate (rateLimitPoliciesCache config)
      pure view

-- | Clear every bucket for a prefix. Returns the number reset. 404s an unknown prefix.
resetRateLimitBucketsHandler
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
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
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
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
  :: forall registry
   . ArbiterServerConfig registry
  -> Handler ConcurrencyPoliciesResponse
listConcurrencyHandler config =
  liftIO $ cachedFor policyStatsCacheTtl (concurrencyPoliciesCache config) $ do
    views <- runDb config HL.listConcurrencyPolicies
    pure $ ConcurrencyPoliciesResponse {policies = views}

-- | List a prefix's keys with in-flight fill levels, paginated (default 100, max 1000).
listConcurrencyKeysHandler
  :: forall registry
   . ArbiterServerConfig registry
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
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> ConcurrencyPolicyUpdate
  -> Handler ConcurrencyPolicyView
updateConcurrencyPolicyHandler config prefix upd@(ConcurrencyPolicyUpdate mLimit) = do
  let invalid
        | maybe False (< 0) (join mLimit) = Just "override limit must be >= 0"
        | otherwise = Nothing
  case invalid of
    Just msg -> throwError err400 {errBody = msg}
    Nothing -> do
      -- An absent overrideLimit reads the view without rewriting the row.
      let action = case mLimit of
            Nothing -> HL.getConcurrencyPolicy prefix
            Just _ -> HL.updateConcurrencyPolicyOverrides prefix upd >> HL.getConcurrencyPolicy prefix
      view <- updateThenView config action "Concurrency pool not found"
      invalidate (concurrencyPoliciesCache config)
      pure view

-- | Recompute every key's in-flight count from live jobs. Returns rows repaired.
reconcileConcurrencyHandler
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
  -> Handler ConcurrencyReconcileResponse
reconcileConcurrencyHandler config = do
  repaired <- runDb config HL.reconcileConcurrencyCounts
  invalidate (concurrencyPoliciesCache config)
  pure $ ConcurrencyReconcileResponse {reconciled = repaired}

-- | Server for the shared top-level routes.
sharedServer
  :: forall registry
   . (HL.RegistryAdmissionPolicies registry, RegistryTables registry)
  => ArbiterServerConfig registry
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
  buildServer :: ArbiterServerConfig registry -> ServerT (RegistryToAPI reg) Handler

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
  :: forall registry
   . (BuildServer registry registry)
  => ArbiterServerConfig registry
  -> ServerT (ArbiterAPI registry) Handler
arbiterServer = buildServer @registry @registry

-- | Hoisted server for integration into a route tree using a custom monad.
arbiterServerHoisted
  :: forall registry m
   . ( BuildServer registry registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => (forall x. Handler x -> m x)
  -> ArbiterServerConfig registry
  -> ServerT (ArbiterAPI registry) m
arbiterServerHoisted natTrans config =
  hoistServer (Proxy @(ArbiterAPI registry)) natTrans (arbiterServer config)

-- | Convert to WAI Application. Each 'QueueWithResult' result type needs
-- @FromJSON@ and @ToJSON@.
arbiterApp
  :: forall registry
   . ( BuildServer registry registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => ArbiterServerConfig registry
  -> Application
arbiterApp config =
  serve (Proxy @(ArbiterAPI registry)) (arbiterServer config)

-- | Run the API server on a port.
runArbiterAPI
  :: forall registry
   . ( BuildServer registry registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => Port
  -> ArbiterServerConfig registry
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
  let limit = max 1 $ min 1000 $ fromMaybe defLimit mLimit
      offset = max 0 $ fromMaybe 0 mOffset
   in (limit, offset)

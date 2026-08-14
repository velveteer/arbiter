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
  , BuildServer (..)
  ) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types (DedupKey (..), Job (..), JobPayload, JobStatus, isRollup)
import Arbiter.Core.MonadArbiter (MonadArbiter, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.QueueRegistry (JobPayloadRegistry, RegistryTables (..), SpecName, SpecPayload)
import Arbiter.Core.Sql.Jobs (ArchiveSortColumn, DLQSortColumn, JobFilter (..), JobSortColumn, SortDir)
import Arbiter.Core.Trace (resolveTracer, withPublishSpan)
import Arbiter.Simple (SimpleConnectionPool (..), SimpleDb, SimpleEnv (..), createSimpleEnvWithConfig, runSimpleDb)
import Arbiter.Worker.Cron (updateCronScheduleChecked)
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
import Control.Exception (SomeAsyncException, SomeException, bracket, fromException, handle, throwIO)
import Control.Monad (forever, guard, join, unless, void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.ByteString.Builder qualified as Builder
import Data.ByteString.Lazy qualified as LBS
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe)
import Data.Pool qualified as Pool
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Data.Time (NominalDiffTime, UTCTime, diffUTCTime, getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Data.UUID.Types (UUID)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Notification (Notification (..), getNotification)
import GHC.TypeLits (KnownSymbol, symbolVal)
import Network.HTTP.Types (status200)
import Network.Wai (responseStream)
import Network.Wai.Handler.Warp (Port, defaultSettings, runSettings, setPort, setTimeout)
import Servant
import Servant.Server.Generic (AsServerT)
import System.IO (hPutStrLn, stderr)
import System.Timeout (timeout)

import Arbiter.Servant.API
  ( ArbiterAPI
  , ArchiveAPI (..)
  , ConcurrencyAPI (..)
  , CronAPI (..)
  , DLQAPI (..)
  , JobsAPI (..)
  , QueuesAPI (..)
  , RateLimitsAPI (..)
  , RegistryToAPI
  , SharedAPI
  , StatsAPI (..)
  , TableAPI (..)
  , WorkersAPI (..)
  )
import Arbiter.Servant.Types

-- | Configuration for the API server
data ArbiterServerConfig (registry :: JobPayloadRegistry) = ArbiterServerConfig
  { serverEnv :: SimpleEnv registry
  -- ^ The SimpleEnv containing schema and connection pool
  , enableSSE :: Bool
  -- ^ Enable Server-Sent Events streaming endpoint. When 'False', the
  -- @\/events\/stream@ endpoint returns a single \"disabled\" event and
  -- closes immediately, avoiding long-lived connections. The admin UI
  -- falls back to polling-only mode. Default: 'True'.
  , sseHub :: MVar (Maybe SSEHub)
  -- ^ Lazily started SSE broadcast hub, shared by all clients. Started on the
  -- first subscriber and torn down (its @LISTEN@ connection released) when the
  -- last one disconnects, so an idle server holds no streaming connection.
  , rateLimitPoliciesCache :: CacheCell RateLimitPoliciesResponse
  -- ^ Short-TTL cache for the rate-limit policy list, collapsing dashboard polls.
  , concurrencyPoliciesCache :: CacheCell ConcurrencyPoliciesResponse
  -- ^ Short-TTL cache for the concurrency policy list, collapsing dashboard polls.
  , allQueueStatsCache :: CacheCell AllStatsResponse
  -- ^ Short-TTL cache for the all-queues overview aggregate, collapsing landing polls.
  }

-- | A running SSE broadcast hub: the channel every client duplicates, and the
-- live subscriber count the listener watches to release itself at zero.
data SSEHub = SSEHub
  { hubChan :: TChan ByteString
  , hubRefs :: TVar Int
  }

-- | Small pool configuration for admin API traffic.
serverPoolConfig :: PoolConfig
serverPoolConfig =
  PoolConfig
    { poolSize = 10
    , poolIdleTimeout = 60
    , poolStripes = Just 1
    }

-- | Create an 'ArbiterServerConfig' with its own internal connection pool.
--
-- __Note__: Use 'Arbiter.Migrations.runMigrationsForRegistry' with
-- @enableEventStreaming = True@ to set up the database triggers for SSE.
initArbiterServer
  :: forall registry
   . Proxy registry
  -> ByteString
  -> Text
  -> IO (ArbiterServerConfig registry)
initArbiterServer _proxy connStr schemaName = do
  env <- createSimpleEnvWithConfig (Proxy @registry) connStr schemaName serverPoolConfig
  hub <- newMVar Nothing
  rlCache <- newTVarIO (0, Nothing)
  ccCache <- newTVarIO (0, Nothing)
  statsCache <- newTVarIO (0, Nothing)
  pure
    ArbiterServerConfig
      { serverEnv = env
      , enableSSE = True
      , sseHub = hub
      , rateLimitPoliciesCache = rlCache
      , concurrencyPoliciesCache = ccCache
      , allQueueStatsCache = statsCache
      }

-- | Jobs API handlers for a specific table
jobsServer
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> JobsAPI payload (AsServerT Handler)
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
    }

-- | List jobs with pagination and composable filters
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
  -> Maybe JobSortColumn
  -> Maybe SortDir
  -> Handler (JobsResponse payload)
listJobsHandler tableName config mLimit mOffset mGroupKey mParentId mJobId rootsOnly mStatus mSortBy mSortDir = liftIO $ do
  let (limit, offset) = validatePagination 50 mLimit mOffset
      env = serverEnv config
      schemaName = schema env
      filters =
        catMaybes
          [ FilterGroupKey <$> mGroupKey
          , FilterParentId <$> mParentId
          , FilterId <$> mJobId
          , FilterRootsOnly <$ guard rootsOnly
          , FilterStatus <$> mStatus
          ]

  (jobs, total, combined, dlqCounts) <- runSimpleDb env $ withDbTransaction $ do
    j <- Ops.listJobsWithStatus schemaName tableName filters mSortBy mSortDir limit offset
    c <- Ops.countJobsFiltered schemaName tableName filters
    -- Only query child/DLQ counts if any returned job could be a parent.
    -- All parents are rollup finalizers (isRollup = True), so we
    -- skip the extra queries for queues that don't use job trees.
    let jobIds = map (primaryKey . fst) j
        hasParents = any (isRollup . fst) j
    if null j || not hasParents
      then pure (j, c, Map.empty, Map.empty)
      else do
        cc <- Ops.countChildrenBatch schemaName tableName jobIds
        dc <- Ops.countDLQChildrenBatch schemaName tableName jobIds
        pure (j, c, cc, dc)

  let childCounts = fmap fst combined
      pausedParents = Map.keys $ Map.filter (\(t, p) -> p == t) combined
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

-- | Insert a new job into the queue
insertJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> ApiJobWrite payload
  -> Handler (JobResponse (ApiJob payload))
insertJobHandler tableName config (ApiJobWrite jobWrite) = do
  let env = serverEnv config
      schemaName = schema env

  mJob <- liftIO $ runSimpleDb env $ publishing tableName 1 $ do
    inserted <- Ops.insertJob schemaName tableName jobWrite
    case (inserted, dedupKey jobWrite) of
      (Just j, _) -> pure (Just j)
      (Nothing, Just (IgnoreDuplicate k)) -> Ops.getJobByDedupKey schemaName tableName k
      _ -> pure Nothing
  case mJob of
    Just j -> pure $ JobResponse (ApiJob j)
    Nothing -> throwError err409 {errBody = "Replace blocked: existing job is in-flight on first attempt or has children"}

-- | Insert multiple jobs in a single batch operation
insertJobsBatchHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> BatchInsertRequest payload
  -> Handler (BatchInsertResponse payload)
insertJobsBatchHandler tableName config (BatchInsertRequest jobWrites) = do
  let env = serverEnv config
      schemaName = schema env
      writes = map unApiJobWrite jobWrites

  inserted <-
    liftIO $
      runSimpleDb env $
        publishing tableName (length writes) $
          Ops.insertJobsBatch schemaName tableName writes
  let apiJobs = map ApiJob inserted
  pure $ BatchInsertResponse {inserted = apiJobs, insertedCount = length apiJobs}

-- | Run an enqueue under the @publish \<queue\>@ producer span, named by the runtime queue.
publishing :: (MonadArbiter m) => Text -> Int -> m a -> m a
publishing tableName n action = do
  tracer <- resolveTracer
  withPublishSpan tracer tableName n action

-- | Get a specific job by ID
getJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler (JobResponse (ApiJobWithStatus payload))
getJobHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  mJob <- liftIO $ runSimpleDb env $ Ops.getJobByIdWithStatus schemaName tableName jobId
  case mJob of
    Nothing -> throwError err404 {errBody = "Job not found"}
    Just (j, s) -> pure $ JobResponse {job = ApiJobWithStatus j s}

-- | Cancel a job (delete it from the queue)
cancelJobHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
cancelJobHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  rowsAffected <- liftIO $ runSimpleDb env $ Ops.cancelJobCascade schemaName tableName jobId
  if rowsAffected > 0
    then pure NoContent
    else throwError err404 {errBody = "Job not found"}

-- | Cascade-cancel a job and async-cancel any in-flight handlers via NOTIFY.
forceCancelJobHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
forceCancelJobHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  rowsAffected <- liftIO $ runSimpleDb env $ Ops.forceCancelJob schemaName tableName jobId
  if rowsAffected > 0
    then pure NoContent
    else throwError err404 {errBody = "Job not found"}

-- | Promote a job (make it immediately visible)
promoteJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
promoteJobHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  result <- liftIO $ runSimpleDb env $ do
    rowsAffected <- Ops.promoteJob schemaName tableName jobId
    if rowsAffected > 0
      then pure (Right ())
      else do
        mJob <- Ops.getJobById @_ @payload schemaName tableName jobId
        case mJob of
          Nothing -> pure (Left err404 {errBody = "Job not found"})
          Just job
            | suspended job ->
                pure (Left err409 {errBody = "Job is suspended - use resume endpoint"})
            | otherwise ->
                pure (Left err409 {errBody = "Job is already visible"})

  case result of
    Left err -> throwError err
    Right () -> pure NoContent

-- | Move a job to the dead letter queue
moveToDLQHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
moveToDLQHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  result <- liftIO $ runSimpleDb env $ withDbTransaction $ do
    mJob <- Ops.getJobById @_ @payload schemaName tableName jobId
    case mJob of
      Nothing -> pure Nothing
      Just job ->
        Just <$> Ops.moveToDLQ Ops.TakeLocks schemaName tableName "Manually moved to DLQ via admin API" job

  case result of
    Nothing -> throwError err404 {errBody = "Job not found"}
    Just 0 -> throwError err409 {errBody = "Job was concurrently modified"}
    Just _ -> pure NoContent

-- | Pause all children of a parent job
pauseChildrenHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
pauseChildrenHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  void . liftIO . runSimpleDb env $
    Ops.pauseChildren schemaName tableName jobId

  -- Returns NoContent even if no children were paused (they may all be
  -- in-flight, already suspended, or completed). This is not an error.
  pure NoContent

-- | Resume all suspended children of a parent job.
resumeChildrenHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
resumeChildrenHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  void . liftIO . runSimpleDb env $
    Ops.resumeChildren schemaName tableName jobId

  -- Returns NoContent even if no children were resumed (they may not be
  -- suspended, or may have already completed). This is not an error.
  pure NoContent

-- | Suspend a job (make it unclaimable)
suspendJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
suspendJobHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  result <- liftIO $ runSimpleDb env $ do
    rowsAffected <- Ops.suspendJob schemaName tableName jobId
    if rowsAffected > 0
      then pure (Right ())
      else do
        mJob <- Ops.getJobById @_ @payload schemaName tableName jobId
        case mJob of
          Nothing -> pure (Left err404 {errBody = "Job not found"})
          Just job
            | suspended job ->
                pure (Left err409 {errBody = "Job is already suspended"})
            | otherwise ->
                pure (Left err409 {errBody = "Job is in-flight - cannot suspend"})

  case result of
    Left err -> throwError err
    Right () -> pure NoContent

-- | Resume a suspended job, making it claimable again.
--
-- Refuses to resume a rollup finalizer that still has children, preventing
-- premature handler execution.
resumeJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
resumeJobHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  result <- liftIO $ runSimpleDb env $ do
    rowsAffected <- Ops.resumeJob schemaName tableName jobId
    if rowsAffected > 0
      then pure (Right ())
      else do
        mJob <- Ops.getJobById @_ @payload schemaName tableName jobId
        case mJob of
          Nothing -> pure (Left err404 {errBody = "Job not found"})
          Just job
            | not (suspended job) ->
                pure (Left err409 {errBody = "Job is not suspended"})
            | isRollup job ->
                pure (Left err409 {errBody = "Cannot resume a rollup finalizer with active children"})
            | otherwise ->
                pure (Left err409 {errBody = "Job could not be resumed (concurrent modification)"})

  case result of
    Left err -> throwError err
    Right () -> pure NoContent

-- | DLQ API handlers for a specific table
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

-- | List DLQ jobs with pagination and composable filters
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
  -> Maybe DLQSortColumn
  -> Maybe SortDir
  -> Handler (DLQResponse payload)
listDLQHandler tableName config mLimit mOffset mParentId mJobId mGroupKey mSortBy mSortDir = do
  let (limit, offset) = validatePagination 50 mLimit mOffset
      env = serverEnv config
      schemaName = schema env
      filters =
        catMaybes
          [ FilterParentId <$> mParentId
          , FilterJobId <$> mJobId
          , FilterGroupKey <$> mGroupKey
          ]

  (dlqJobs, total) <- liftIO $ runSimpleDb env $ withDbTransaction $ do
    j <- Ops.listDLQFilteredOrdered schemaName tableName filters mSortBy mSortDir limit offset
    c <- Ops.countDLQFiltered schemaName tableName filters
    pure (j, c)

  let apiDlqJobs = map ApiDLQJob dlqJobs
  pure $
    DLQResponse
      { dlqJobs = apiDlqJobs
      , dlqTotal = fromIntegral total
      , dlqOffset = offset
      , dlqLimit = limit
      }

-- | Retry a job from DLQ (move back to main queue)
--
-- Returns 409 if the job has a parent_id that no longer exists (orphaned child).
retryFromDLQHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
retryFromDLQHandler tableName config dlqId = do
  let env = serverEnv config
      schemaName = schema env

  result <- liftIO $ runSimpleDb env $ withDbTransaction $ do
    mJob <- Ops.retryFromDLQ @_ @payload schemaName tableName dlqId
    case mJob of
      Just _ -> pure (Right ())
      Nothing -> Left <$> Ops.dlqJobExists schemaName tableName dlqId

  case result of
    Right () -> pure NoContent
    Left True -> throwError err409 {errBody = "Cannot retry: parent job no longer exists (not in queue or DLQ)"}
    Left False -> throwError err404 {errBody = "DLQ job not found"}

-- | Delete a job from DLQ permanently
deleteDLQHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
deleteDLQHandler tableName config dlqId = do
  let env = serverEnv config
      schemaName = schema env

  rowsAffected <- liftIO $ runSimpleDb env $ Ops.deleteDLQJob schemaName tableName dlqId
  if rowsAffected > 0
    then pure NoContent
    else throwError err404 {errBody = "DLQ job not found"}

-- | Batch delete jobs from DLQ permanently
deleteDLQBatchHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> BatchDeleteRequest
  -> Handler BatchDeleteResponse
deleteDLQBatchHandler tableName config (BatchDeleteRequest dlqIds) = do
  let env = serverEnv config
      schemaName = schema env
  rowsDeleted <- liftIO $ runSimpleDb env $ Ops.deleteDLQJobsBatch schemaName tableName dlqIds
  pure $ BatchDeleteResponse {deleted = rowsDeleted}

-- | Archive API handler for a specific table
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

-- | List archived jobs with pagination and composable filters
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
  -> Maybe ArchiveSortColumn
  -> Maybe SortDir
  -> Handler (ArchiveResponse payload)
listArchiveHandler tableName config mLimit mOffset mParentId mJobId mGroupKey mSortBy mSortDir = do
  let (limit, offset) = validatePagination 50 mLimit mOffset
      env = serverEnv config
      schemaName = schema env
      filters =
        catMaybes
          [ FilterParentId <$> mParentId
          , FilterJobId <$> mJobId
          , FilterGroupKey <$> mGroupKey
          ]

  (archived, total) <- liftIO $ runSimpleDb env $ withDbTransaction $ do
    j <- Ops.listArchiveFiltered schemaName tableName filters mSortBy mSortDir limit offset
    c <- Ops.countArchiveFiltered schemaName tableName filters
    pure (j, c)

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
  let env = serverEnv config
      schemaName = schema env
  mJob <- liftIO $ runSimpleDb env $ Ops.reEnqueueFromArchive @_ @payload schemaName tableName archiveId
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
  let env = serverEnv config
      schemaName = schema env
  rowsAffected <- liftIO $ runSimpleDb env $ Ops.deleteArchiveJob schemaName tableName archiveId
  if rowsAffected > 0
    then pure NoContent
    else throwError err404 {errBody = "Archived job not found"}

-- | Bulk-purge archived jobs by archive primary key.
deleteArchiveBatchHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> BatchDeleteRequest
  -> Handler BatchDeleteResponse
deleteArchiveBatchHandler tableName config (BatchDeleteRequest archiveIds) = do
  let env = serverEnv config
      schemaName = schema env
  rowsDeleted <- liftIO $ runSimpleDb env $ Ops.deleteArchiveJobsBatch schemaName tableName archiveIds
  pure $ BatchDeleteResponse {deleted = rowsDeleted}

-- | Stats API handler for a specific table
statsServer
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> StatsAPI (AsServerT Handler)
statsServer tableName config =
  StatsAPI
    { getStats = getStatsHandler @registry tableName config
    }

-- | Get queue statistics
getStatsHandler
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Handler StatsResponse
getStatsHandler tableName config = do
  let env = serverEnv config
      schemaName = schema env

  queueStats <- liftIO $ runSimpleDb env $ Ops.getQueueStats schemaName tableName
  now <- liftIO getCurrentTime
  let timestamp = T.pack $ formatTime defaultTimeLocale "%Y-%m-%dT%H:%M:%S%z" now

  pure $ StatsResponse {stats = queueStats, timestamp = timestamp}

-- | Every queue's stats in one request, for the landing overview.
getAllStatsHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> [Text]
  -> Handler AllStatsResponse
getAllStatsHandler config tables =
  liftIO $ cachedFor overviewStatsCacheTtl (allQueueStatsCache config) $ do
    let env = serverEnv config
        schemaName = schema env
    AllStatsResponse <$> runSimpleDb env (Ops.getAllQueueStats schemaName tables)

-- | Table API handlers for a specific table
tableServer
  :: forall registry payload
   . (JobPayload payload)
  => Text -- table
  -> ArbiterServerConfig registry
  -> TableAPI payload (AsServerT Handler)
tableServer table config =
  TableAPI
    { jobs = jobsServer @registry @payload table config
    , dlq = dlqServer @registry @payload table config
    , archive = archiveServer @registry @payload table config
    , stats = statsServer @registry table config
    }

-- | Queues API handler
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
        , getAllStats = getAllStatsHandler config known
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
  let env = serverEnv config
      schemaName = schema env
  liftIO $ runSimpleDb env $ Ops.getQueue schemaName queue

-- | Flip the @paused@ flag for a queue, validated against the registry. The
-- @arbiter_queues@ row is created lazily on first pause.
setQueuePausedHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> [Text]
  -> Bool
  -> Text
  -> Handler NoContent
setQueuePausedHandler config knownQueues p queue = do
  unless (queue `elem` knownQueues) $
    throwError err404 {errBody = "Unknown queue"}
  let env = serverEnv config
      schemaName = schema env
  void . liftIO $ runSimpleDb env $ Ops.setQueuePaused schemaName queue p
  -- The landing overview shows each queue's paused flag, so refresh it promptly.
  invalidate (allQueueStatsCache config)
  pure NoContent

-- | Events server - raw WAI application for SSE streaming.
--
-- Uses WAI 'responseStream' with explicit flush after every event so the
-- browser receives data immediately.  Sends a @: keepalive@ comment every
-- 15 seconds to keep the connection alive through reverse proxies.
--
-- All clients share one broadcast hub (see 'subscribeSSE'): each client is a
-- Warp thread reading a duplicated 'TChan', not a held Postgres connection, so
-- viewer count scales without exhausting the pool. When 'enableSSE' is 'False',
-- sends a single @disabled@ event and closes immediately. The admin UI detects
-- this and skips reconnection.
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
      -- whole response, so the hub is released even if the streaming body never
      -- runs (early client disconnect or an async exception before it starts).
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
              -- Read this client's channel with a 15s keepalive heartbeat that
              -- keeps Warp and any reverse proxies from timing out.
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
swallowSync e = case fromException e of
  Just (_ :: SomeAsyncException) -> throwIO e
  Nothing -> pure ()

-- | Subscribe to the shared SSE hub, returning a duplicated channel to stream
-- from. The first subscriber starts the hub (one @LISTEN@ connection). Later
-- subscribers just bump the refcount. 'Nothing' when there is no pool.
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
        -- subscribeSSE runs masked (as a 'bracket' acquire), so fork the listener
        -- with an explicit unmask to keep it interruptible.
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

-- | The single listener: @LISTEN@ on one borrowed connection and fan every
-- notification into the broadcast channel, raced against the subscriber count
-- reaching zero. On connection loss it destroys the dead resource and
-- reconnects, so a Postgres blip does not kill SSE for every client. When the
-- count hits zero the race ends, the 'bracket' releases the connection, and the
-- thread exits.
sseListenerLoop :: Pool.Pool PG.Connection -> TChan ByteString -> TVar Int -> IO ()
sseListenerLoop pool broadcast refs = do
  backoff <- newIORef baseBackoff
  race_ waitForIdle (pump backoff)
  where
    baseBackoff = 1_000_000 -- 1s
    maxBackoff = 30_000_000 -- 30s
    waitForIdle = atomically $ do
      n <- readTVar refs
      check (n == 0)
    pump backoff =
      forever
        $ handle (onError backoff)
        $ bracket
          (Pool.takeResource pool)
          (\(conn, localPool) -> Pool.destroyResource pool localPool conn)
        $ \(conn, _) -> do
          _ <- PG.execute_ conn $ "LISTEN " <> fromString (T.unpack Schema.eventStreamingChannel)
          writeIORef backoff baseBackoff -- connected: reset for the next failure
          forever $ do
            notification <- getNotification conn
            atomically $ writeTChan broadcast (notificationData notification)
    -- Re-raise async exceptions so the race's cancellation stops the pump. On a
    -- sync error (lost connection or persistent misconfiguration) log it and
    -- retry with capped exponential backoff, so a permanent failure does not spin.
    onError backoff e = case fromException e of
      Just (_ :: SomeAsyncException) -> throwIO e
      Nothing -> do
        delay <- readIORef backoff
        hPutStrLn stderr $
          "[arbiter:sse] listener error, retrying in "
            <> show (delay `div` 1_000_000)
            <> "s: "
            <> show e
        threadDelay delay
        writeIORef backoff (min maxBackoff (delay * 2))

-- | Cron API handlers
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
  let env = serverEnv config
      schemaName = schema env
  rows <- liftIO $ runSimpleDb env $ Ops.listCronSchedules schemaName mQueue
  pure $ CronSchedulesResponse {cronSchedules = rows}

-- | Update a cron schedule
updateCronScheduleHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> CronScheduleUpdate
  -> Handler CronScheduleRow
updateCronScheduleHandler config name update = do
  let env = serverEnv config
      schemaName = schema env

  result <- liftIO $ runSimpleDb env $ withDbTransaction $ do
    outcome <- updateCronScheduleChecked name update
    traverse (const (Ops.getCronScheduleByName schemaName name)) outcome

  case result of
    Left err -> throwError err400 {errBody = LBS.fromStrict (encodeUtf8 err)}
    Right Nothing -> throwError err404 {errBody = "Cron schedule not found"}
    Right (Just row) -> pure row

-- | Request an out-of-band run of a cron schedule. A disabled schedule is
-- refused so a manual run never fires what the schedule itself would not, and
-- so is one whose pending request would be coalesced away.
runCronScheduleHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Handler NoContent
runCronScheduleHandler config name = do
  let env = serverEnv config
      schemaName = schema env
  outcome <- liftIO $ runSimpleDb env $ Ops.requestCronRun schemaName name
  case outcome of
    Ops.RunReqNotFound -> throwError err404 {errBody = "Cron schedule not found"}
    Ops.RunReqDisabled -> throwError err409 {errBody = "Cron schedule is disabled"}
    Ops.RunReqPending -> throwError err409 {errBody = "Cron schedule already has a run pending"}
    Ops.RunReqStamped -> pure NoContent

-- | Workers API handlers
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
  let env = serverEnv config
      schemaName = schema env
  rows <- liftIO $ runSimpleDb env $ Ops.listWorkers schemaName mQueue (realToFrac <$> mLiveSecs)
  pure $ WorkersResponse {workers = rows}

-- | Set a worker's @paused@ flag. The worker reconciles its local state on
-- the next heartbeat.
setWorkerPausedHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Bool
  -> UUID
  -> Handler NoContent
setWorkerPausedHandler config p wid = do
  let env = serverEnv config
      schemaName = schema env
  n <- liftIO $ runSimpleDb env $ Ops.setWorkerPaused schemaName wid p
  if n == 0
    then throwError err404 {errBody = "Worker not found"}
    else pure NoContent

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

-- | Poll-collapsing TTL for the dashboard list-policy stats.
policyStatsCacheTtl :: NominalDiffTime
policyStatsCacheTtl = 10

-- | Shorter TTL for the faster-polling all-queues overview.
overviewStatsCacheTtl :: NominalDiffTime
overviewStatsCacheTtl = 5

-- | A TTL cache cell: an epoch bumped by 'invalidate', plus the current entry.
type CacheCell a = TVar (Word, Maybe (UTCTime, a))

-- | Serve from a short-TTL cache cell, skipping the write when 'invalidate' bumped the epoch mid-produce, so an invalidation is never clobbered.
cachedFor :: NominalDiffTime -> CacheCell a -> IO a -> IO a
cachedFor ttl cell produce = do
  now <- getCurrentTime
  (epoch, cached) <- readTVarIO cell
  case cached of
    Just (ts, v) | diffUTCTime now ts < ttl -> pure v
    _ -> do
      v <- produce
      atomically $ modifyTVar' cell $ \(e, cur) -> if e == epoch then (e, Just (now, v)) else (e, cur)
      pure v

-- | Bump a cache cell's epoch and drop its entry, after an operator mutation.
invalidate :: CacheCell a -> Handler ()
invalidate cell = liftIO $ atomically $ modifyTVar' cell $ \(e, _) -> (e + 1, Nothing)

-- | List policies with bucket stats and currently-throttled job counts.
listRateLimitsHandler
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
  -> Handler RateLimitPoliciesResponse
listRateLimitsHandler config =
  liftIO $ cachedFor policyStatsCacheTtl (rateLimitPoliciesCache config) $ do
    views <- runSimpleDb (serverEnv config) HL.listRateLimitPolicies
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
  rows <- liftIO $ runSimpleDb (serverEnv config) (HL.listRateLimitBuckets prefix limit offset)
  pure $ RateLimitBucketsResponse {buckets = rows}

updateThenView
  :: SimpleEnv registry
  -> SimpleDb registry IO (Maybe a)
  -> LBS.ByteString
  -> Handler a
updateThenView env action notFound = do
  mView <- liftIO $ runSimpleDb env action
  maybe (throwError err404 {errBody = notFound}) pure mView

-- | Set or clear a policy's override params, then return the updated view.
updateRateLimitPolicyHandler
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
  -> Text
  -> RateLimitPolicyUpdate
  -> Handler RateLimitPolicyView
updateRateLimitPolicyHandler config prefix upd@(RateLimitPolicyUpdate mMax mRefill mIv) = do
  let invalid
        | maybe False (< 0) (join mMax) = Just "override max tokens must be >= 0"
        | maybe False (< 0) (join mRefill) = Just "override refill amount must be >= 0"
        | maybe False (<= 0) (join mIv) = Just "override interval must be > 0"
        | otherwise = Nothing
  case invalid of
    Just msg -> throwError err400 {errBody = msg}
    Nothing -> do
      -- An all-absent patch changes nothing, so read the view without rewriting the row and waking jobs.
      let update = case (mMax, mRefill, mIv) of
            (Nothing, Nothing, Nothing) -> pure ()
            _ -> void $ HL.updateRateLimitPolicyOverrides prefix upd
      view <- updateThenView (serverEnv config) (update >> HL.getRateLimitPolicy prefix) "Rate-limit policy not found"
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
  n <- updateThenView (serverEnv config) action "Rate-limit policy not found"
  invalidate (rateLimitPoliciesCache config)
  pure $ RateLimitResetResponse {reset = n}

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
    views <- runSimpleDb (serverEnv config) HL.listConcurrencyPolicies
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
  rows <- liftIO $ runSimpleDb (serverEnv config) (HL.listConcurrencyKeys prefix limit offset)
  pure $ ConcurrencyKeysResponse {keys = rows}

-- | Set or clear a pool's override limit, then return the updated view.
updateConcurrencyPolicyHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> ConcurrencyPolicyUpdate
  -> Handler ConcurrencyPolicyView
updateConcurrencyPolicyHandler config prefix upd@(ConcurrencyPolicyUpdate mLim) = do
  let invalid
        | maybe False (< 0) (join mLim) = Just "override limit must be >= 0"
        | otherwise = Nothing
  case invalid of
    Just msg -> throwError err400 {errBody = msg}
    Nothing -> do
      -- An absent overrideLimit changes nothing, so read the view without rewriting the row.
      let action = case mLim of
            Nothing -> HL.getConcurrencyPolicy prefix
            Just _ -> HL.updateConcurrencyPolicyOverrides prefix upd >> HL.getConcurrencyPolicy prefix
      view <- updateThenView (serverEnv config) action "Concurrency pool not found"
      invalidate (concurrencyPoliciesCache config)
      pure view

-- | Recompute every key's in-flight count from live jobs. Returns rows repaired.
reconcileConcurrencyHandler
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
  -> Handler ConcurrencyReconcileResponse
reconcileConcurrencyHandler config = do
  n <- liftIO $ runSimpleDb (serverEnv config) HL.reconcileConcurrencyCounts
  invalidate (concurrencyPoliciesCache config)
  pure $ ConcurrencyReconcileResponse {reconciled = n}

-- | Server for the shared top-level routes.
sharedServer
  :: forall registry
   . (RegistryTables registry)
  => ArbiterServerConfig registry
  -> ServerT SharedAPI Handler
sharedServer config =
  queuesServer @registry (Proxy @registry) config
    :<|> eventsServer config
    :<|> cronServer config
    :<|> workersServer config
    :<|> rateLimitsServer config
    :<|> concurrencyServer config

-- | Type class to build server implementations for registry entries
class BuildServer registry (reg :: JobPayloadRegistry) where
  buildServer :: ArbiterServerConfig registry -> ServerT (RegistryToAPI reg) Handler

-- Base case: empty registry, just the shared top-level routes
instance
  (RegistryTables registry)
  => BuildServer registry '[]
  where
  buildServer = sharedServer

-- Single table case: table endpoints :<|> shared top-level routes
instance
  ( JobPayload (SpecPayload spec)
  , KnownSymbol (SpecName spec)
  , RegistryTables registry
  )
  => BuildServer registry (spec ': '[])
  where
  buildServer config =
    let tableName = T.pack $ symbolVal (Proxy @(SpecName spec))
     in tableServer @registry @(SpecPayload spec) tableName config
          :<|> sharedServer config

-- Recursive case: table endpoints :<|> rest of tables (at least 2 tables total)
instance
  ( BuildServer registry (nextSpec ': moreRest)
  , JobPayload (SpecPayload spec)
  , KnownSymbol (SpecName spec)
  )
  => BuildServer registry (spec ': (nextSpec ': moreRest))
  where
  buildServer config =
    let tableName = T.pack $ symbolVal (Proxy @(SpecName spec))
     in tableServer @registry @(SpecPayload spec) tableName config
          :<|> buildServer @registry @(nextSpec ': moreRest) config

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
arbiterServerHoisted nt config =
  hoistServer (Proxy @(ArbiterAPI registry)) nt (arbiterServer config)

-- | Convert to WAI Application
arbiterApp
  :: forall registry
   . ( BuildServer registry registry
     , HasServer (ArbiterAPI registry) '[]
     )
  => ArbiterServerConfig registry
  -> Application
arbiterApp config =
  serve (Proxy @(ArbiterAPI registry)) (arbiterServer config)

-- | Run the API server on a specific port.
--
-- Uses @setTimeout 0@ (no idle timeout) so that SSE streaming connections
-- are not killed by Warp.
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
  let settings = setPort port $ setTimeout 0 defaultSettings
  runSettings settings (arbiterApp config)

-- | Validate and sanitize pagination parameters
validatePagination :: Int -> Maybe Int -> Maybe Int -> (Int, Int)
validatePagination defLimit mLimit mOffset =
  let limit = max 1 $ min 1000 $ fromMaybe defLimit mLimit -- Clamp between 1 and 1000
      offset = max 0 $ fromMaybe 0 mOffset -- Must be non-negative
   in (limit, offset)

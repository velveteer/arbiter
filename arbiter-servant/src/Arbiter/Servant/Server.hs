{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

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
  , QueueSpec (..)
  , runtimeQueue
  , initArbiterServer
  , BuildServer (..)

    -- * TTL cache
  , CacheCell
  , newCacheCell
  , cachedFor
  ) where

import Arbiter.Core.Admission (prefixedKeyPartValid)
import Arbiter.Core.Concurrency.Spec (ConcurrencyKey (..))
import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types
  ( DedupKey (..)
  , Job (..)
  , JobPayload
  , JobRead
  , JobStatus
  , JobWrite
  , ObservabilityHooks
  , defaultMaxAttempts
  , defaultObservabilityHooks
  , isRollup
  )
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.MonadArbiter (withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.QueueRegistry (JobPayloadRegistry)
import Arbiter.Core.RateLimit.Spec (RateLimitKey (..))
import Arbiter.Core.Sql.Jobs (DLQSortColumn, JobFilter (..), JobSortColumn, SortDir)
import Arbiter.Simple (SimpleConnectionPool (..), SimpleDb, SimpleEnv (..), createSimpleEnvWithConfig, runSimpleDb)
import Arbiter.Worker.Cron (overlapPolicyFromText, resolveTZ)
import Arbiter.Worker.Outcome qualified as Outcome
import Arbiter.Worker.Trace qualified as Trace
import Control.Concurrent (forkIOWithUnmask, threadDelay)
import Control.Concurrent.Async (race_)
import Control.Concurrent.MVar (MVar, modifyMVar, modifyMVar_, newMVar, withMVar)
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
import Control.Exception (Exception, SomeAsyncException, SomeException, bracket, fromException, handle, throwIO)
import Control.Monad (forever, guard, join, unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (Value)
import Data.ByteString (ByteString)
import Data.ByteString.Builder qualified as Builder
import Data.ByteString.Lazy qualified as LBS
import Data.Foldable (traverse_)
import Data.IORef (newIORef, readIORef, writeIORef)
import Data.Int (Int32, Int64)
import Data.Kind (Type)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe)
import Data.Pool qualified as Pool
import Data.Set qualified as Set
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Data.UUID.Types (UUID)
import Data.UUID.V4 (nextRandom)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Notification (Notification (..), getNotification)
import GHC.Clock (getMonotonicTime)
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Network.HTTP.Types (status200)
import Network.Wai (responseStream)
import Network.Wai.Handler.Warp (Port, defaultSettings, runSettings, setPort, setTimeout)
import Servant
import Servant.Server.Generic (AsServerT)
import System.Cron (parseCronSchedule)
import System.IO (hPutStrLn, stderr)
import System.Timeout (timeout)

import Arbiter.Servant.API
  ( ArbiterAPI
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
  , declaredPrefixesCache :: CacheCell (Set.Set Text, Set.Set Text)
  -- ^ Short-TTL cache of the prefixes that have a policy.
  , serverQueues :: Map.Map Text QueueSpec
  -- ^ Every queue this server serves. 'initArbiterServer' reflects the registry's own,
  -- 'runtimeQueue' adds the rest.
  , serverTracer :: Maybe Trace.Tracer
  -- ^ Tracer resolved once at server start, passed to the producer spans.
  }

-- | What this server knows about one queue.
data QueueSpec = QueueSpec
  { queueAdmission :: Ops.ClaimAdmission
  -- ^ The admission its claim enforces, as its typed workers claim it.
  , queuePayloadAdmission :: Maybe (Value -> Either String Ops.AdmissionColumns)
  -- ^ Derive admission at the queue's payload type. 'Nothing' takes the caller's keys.
  , queueRetry :: Outcome.RetryPolicy
  -- ^ How the fail route retries. Set it to what this queue's worker pool uses.
  , queueHooks :: ObservabilityHooks IO Value
  -- ^ Lifecycle hooks the pull-consumer routes fire.
  }

-- | A queue named at runtime, with no payload type to derive admission from.
runtimeQueue :: Ops.ClaimAdmission -> QueueSpec
runtimeQueue admission =
  QueueSpec
    { queueAdmission = admission
    , queuePayloadAdmission = Nothing
    , queueRetry = Outcome.defaultRetryPolicy
    , queueHooks = defaultObservabilityHooks
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
   . (Ops.RegistryPayloadAdmission registry)
  => Proxy registry
  -> ByteString
  -> Text
  -> IO (ArbiterServerConfig registry)
initArbiterServer proxy connStr schemaName = do
  env <- createSimpleEnvWithConfig (Proxy @registry) connStr schemaName serverPoolConfig
  hub <- newMVar Nothing
  rlCache <- newCacheCell
  ccCache <- newCacheCell
  statsCache <- newCacheCell
  prefixCache <- newCacheCell
  serverTracer <- Trace.resolveTracer
  pure
    ArbiterServerConfig
      { serverEnv = env
      , enableSSE = True
      , sseHub = hub
      , rateLimitPoliciesCache = rlCache
      , concurrencyPoliciesCache = ccCache
      , allQueueStatsCache = statsCache
      , declaredPrefixesCache = prefixCache
      , serverQueues = reflectQueues proxy
      , serverTracer = serverTracer
      }

-- | Reflect each registry queue's declared admission and payload decoder.
reflectQueues
  :: forall registry
   . (Ops.RegistryPayloadAdmission registry)
  => Proxy registry
  -> Map.Map Text QueueSpec
reflectQueues proxy =
  Map.fromList
    [ (table, (runtimeQueue admission) {queuePayloadAdmission = Just readAdmission})
    | (table, admission, readAdmission) <- Ops.registryPayloadAdmission proxy
    ]

-- | Jobs API handlers for a specific table
jobsServer
  :: forall registry payload
   . (JobPayload payload)
  => AdmissionResolver payload
  -> Text
  -> ArbiterServerConfig registry
  -> JobsAPI payload (AsServerT Handler)
jobsServer resolve table config =
  JobsAPI
    { listJobs = listJobsHandler @registry @payload table config
    , insertJob = insertJobHandler @registry @payload resolve table config
    , insertJobsBatch = insertJobsBatchHandler @registry @payload resolve table config
    , getJob = getJobHandler @registry @payload table config
    , cancelJob = cancelJobHandler @registry table config
    , forceCancelJob = forceCancelJobHandler @registry table config
    , promoteJob = promoteJobHandler @registry @payload table config
    , moveToDLQ = moveToDLQHandler @registry table config
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
  -> Bool
  -> Maybe JobStatus
  -> Maybe JobSortColumn
  -> Maybe SortDir
  -> Handler (JobsResponse payload)
listJobsHandler tableName config mLimit mOffset mGroupKey mParentId rootsOnly mStatus mSortBy mSortDir = liftIO $ do
  let (limit, offset) = validatePagination 50 mLimit mOffset
      env = serverEnv config
      schemaName = schema env
      filters =
        catMaybes
          [ FilterGroupKey <$> mGroupKey
          , FilterParentId <$> mParentId
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

-- | Insert in a producer span, re-fetching the existing row on an ignored duplicate.
insertTracedWithDedup
  :: (JobPayload payload)
  => Maybe Trace.Tracer
  -> SimpleEnv registry
  -> Text
  -> Text
  -> Ops.AdmissionColumns
  -> JobWrite payload
  -> IO (Maybe (JobRead payload))
insertTracedWithDedup mTracer env schemaName tableName ac jw =
  runSimpleDb env $ Trace.withPublishSpan mTracer tableName $ do
    inserted <- Ops.insertJobWithAdmission schemaName tableName ac jw
    case (inserted, dedupKey jw) of
      (Just j, _) -> pure (Just j)
      (Nothing, Just (IgnoreDuplicate k)) -> Ops.getJobByDedupKey schemaName tableName k
      _ -> pure Nothing

-- | How a queue's writes get their admission columns.
type AdmissionResolver payload = ApiJobWrite payload -> Handler (Ops.AdmissionColumns, JobWrite payload)

-- | A typed queue derives admission from its payload's selectors. Caller keys are refused.
payloadAdmission :: (JobPayload payload) => AdmissionResolver payload
payloadAdmission (ApiJobWrite jobWrite refs) = do
  rejectSuppliedKeys refs
  pure (Ops.admissionColumns (payload jobWrite), jobWrite)

-- | Refuse admission keys a caller attached to a queue that derives its own.
rejectSuppliedKeys :: AdmissionRefs -> Handler ()
rejectSuppliedKeys refs =
  when (hasAdmissionRefs refs) $
    throwError err400 {errBody = "this queue derives admission from its payload type"}

-- | Insert a new job into the queue.
insertJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => AdmissionResolver payload
  -> Text
  -> ArbiterServerConfig registry
  -> ApiJobWrite payload
  -> Handler (JobResponse (ApiJob payload))
insertJobHandler resolve tableName config write = do
  let env = serverEnv config
      schemaName = schema env
  (ac, jobWrite) <- resolve write

  mJob <- liftIO $ insertTracedWithDedup (serverTracer config) env schemaName tableName ac jobWrite
  case mJob of
    Just j -> pure $ JobResponse (ApiJob j)
    Nothing -> throwError err409 {errBody = "Replace blocked: existing job is in-flight on first attempt or has children"}

-- | Insert multiple jobs in a single batch operation
insertJobsBatchHandler
  :: forall registry payload
   . (JobPayload payload)
  => AdmissionResolver payload
  -> Text
  -> ArbiterServerConfig registry
  -> BatchInsertRequest payload
  -> Handler (BatchInsertResponse payload)
insertJobsBatchHandler resolve tableName config (BatchInsertRequest jobWrites) = do
  let env = serverEnv config
      schemaName = schema env
  admitted <- traverse resolve jobWrites

  inserted <-
    liftIO $
      runSimpleDb env $
        Trace.withPublishSpan (serverTracer config) tableName $
          Ops.insertJobsBatchWithAdmission schemaName tableName admitted
  let apiJobs = map ApiJob inserted
  pure $ BatchInsertResponse {inserted = apiJobs, insertedCount = length apiJobs}

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
    Nothing -> throwError jobNotFoundErr
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
    else throwError jobNotFoundErr

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
    else throwError jobNotFoundErr

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
          Nothing -> pure (Left jobNotFoundErr)
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
  :: forall registry
   . Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler NoContent
moveToDLQHandler tableName config jobId = do
  spec <- requireQueue config tableName
  let env = serverEnv config
      schemaName = schema env
      dlqNow job =
        Outcome.failJob
          schemaName
          (queueRetry spec)
          Nothing
          True
          (attempts job)
          dlqReason
          job

  result <- liftIO $ runSimpleDb env $ withDbTransaction $ do
    mJob <- Ops.getJobById @_ @Value schemaName tableName jobId
    traverse (\job -> (,) job <$> dlqNow job) mJob

  case result of
    Nothing -> throwError jobNotFoundErr
    Just (_, report)
      | Outcome.reportRows report == 0 -> throwError err409 {errBody = "Job was concurrently modified"}
    Just (job, report) ->
      NoContent <$ liftIO (fireFailureHooks (queueHooks spec) job dlqReason report)
  where
    dlqReason = "Manually moved to DLQ via admin API"

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
          Nothing -> pure (Left jobNotFoundErr)
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
          Nothing -> pure (Left jobNotFoundErr)
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
  -> Maybe Text
  -> Maybe DLQSortColumn
  -> Maybe SortDir
  -> Handler (DLQResponse payload)
listDLQHandler tableName config mLimit mOffset mParentId mGroupKey mSortBy mSortDir = do
  let (limit, offset) = validatePagination 50 mLimit mOffset
      env = serverEnv config
      schemaName = schema env
      filters =
        catMaybes
          [ FilterParentId <$> mParentId
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
  -> Handler AllStatsResponse
getAllStatsHandler config =
  liftIO $ cachedFor overviewStatsCacheTtl (allQueueStatsCache config) $ do
    let env = serverEnv config
        schemaName = schema env
    rows <- runSimpleDb env $ Ops.getAllQueueStats schemaName (knownQueues config)
    pure $ AllStatsResponse {queues = map toEntry rows}
  where
    toEntry (Ops.QueueOverview q s qp wl wp) =
      QueueStatsEntry {queue = q, stats = s, paused = qp, workersLive = fromIntegral wl, workersPaused = fromIntegral wp}

-- | Table API handlers for a specific table
tableServer
  :: forall registry payload
   . (JobPayload payload)
  => AdmissionResolver payload
  -> Text -- table
  -> ArbiterServerConfig registry
  -> TableAPI payload (AsServerT Handler)
tableServer resolve table config =
  TableAPI
    { jobs = jobsServer @registry @payload resolve table config
    , dlq = dlqServer @registry @payload table config
    , stats = statsServer @registry table config
    }

-- | Queues API handler
queuesServer
  :: forall registry
   . ArbiterServerConfig registry
  -> QueuesAPI (AsServerT Handler)
queuesServer config =
  QueuesAPI
    { listQueues = pure $ QueuesResponse {queues = knownQueues config}
    , getAllStats = getAllStatsHandler config
    , getDetails = getQueueDetailsHandler config
    , pauseQueue = setQueuePausedHandler config True
    , resumeQueue = setQueuePausedHandler config False
    , enqueue = enqueueRuntimeHandler config
    , claim = claimJobsHandler config claimSqls
    , ackJob = ackJobConsumerHandler config
    , nackJob = nackJobConsumerHandler config
    , extendJob = extendJobConsumerHandler config
    , failJob = failJobConsumerHandler config
    }
  where
    claimSqls = Map.mapWithKey (\q spec -> (spec, claimSqlForQueue config q spec)) (serverQueues config)

-- | The largest capacity a single claim request can ask for.
maxClaimJobs :: Int
maxClaimJobs = 1000

-- | Capacities a claim statement is cached for. A request rounds down to one of these.
claimCapacities :: [Int]
claimCapacities = Ops.upTo 64 <> [96, 128, 192, 256, 384, 512, 768, maxClaimJobs]

claimSqlForQueue :: ArbiterServerConfig registry -> Text -> QueueSpec -> Ops.ClaimSql
claimSqlForQueue config queue spec =
  Ops.mkClaimSqlWith (queueAdmission spec) (schema (serverEnv config)) queue 1 claimCapacities

-- | Every queue this server serves.
knownQueues :: ArbiterServerConfig registry -> [Text]
knownQueues = Map.keys . serverQueues

-- | Enqueue a raw-JSON job. A registry queue's payload must still decode at its type.
enqueueRuntimeHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> ApiJobWrite Value
  -> Handler (JobResponse (ApiJob Value))
enqueueRuntimeHandler config queue =
  insertJobHandler @registry (runtimeAdmission config queue) queue config

-- | A registry queue derives admission from its payload type, a runtime queue takes the caller's keys.
runtimeAdmission
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> AdmissionResolver Value
runtimeAdmission config queue (ApiJobWrite jw refs) = do
  spec <- requireQueue config queue
  case queuePayloadAdmission spec of
    Just readAdmission -> do
      rejectSuppliedKeys refs
      ac <- either rejectPayload pure (readAdmission (Job.payload jw))
      pure (ac, jw)
    Nothing -> do
      validateAdmissionRefs config refs
      let AdmissionRefs rl cc = refs
      pure (Ops.admissionColumnsFor ((\r -> (rlKey r, rlCost r)) <$> rl) cc, jw)

-- | Reject a malformed key, or one whose prefix names no declared policy.
validateAdmissionRefs
  :: forall registry
   . ArbiterServerConfig registry
  -> AdmissionRefs
  -> Handler ()
validateAdmissionRefs config refs@(AdmissionRefs rl cc) = when (hasAdmissionRefs refs) $ do
  let parts = foldMap (keyParts . rlKey) rl <> foldMap (\k -> [ckPrefix k, ckSuffix k]) cc
      keyParts k = [rlkPrefix k, rlkSuffix k]
  unless (all prefixedKeyPartValid parts) $
    throwError err400 {errBody = "admission prefix/suffix must not contain ':'"}
  traverse_ checkCost rl
  (rlDeclared, ccDeclared) <- liftIO (declaredPrefixes config)
  traverse_ (checkPrefix "rate-limit" rlDeclared . rlkPrefix . rlKey) rl
  traverse_ (checkPrefix "concurrency" ccDeclared . ckPrefix) cc
  where
    checkCost r =
      unless (rlCost r > 0) $
        throwError err400 {errBody = "rate-limit cost must be positive"}
    checkPrefix kind declared prefix =
      unless (Set.member prefix declared) $
        throwError
          err400
            { errBody =
                "no " <> kind <> " policy named " <> LBS.fromStrict (TE.encodeUtf8 prefix)
            }

-- | The prefixes that have a policy row, re-read rather than fixed at startup.
declaredPrefixes :: ArbiterServerConfig registry -> IO (Set.Set Text, Set.Set Text)
declaredPrefixes config =
  cachedFor policyPrefixCacheTtl (declaredPrefixesCache config) $
    runSimpleDb env $
      (,)
        <$> (Set.fromList <$> Ops.listRateLimitPrefixes schemaName)
        <*> (Set.fromList <$> Ops.listConcurrencyPrefixes schemaName)
  where
    env = serverEnv config
    schemaName = schema env

-- | The spec for a queue this server serves, rejecting any other path segment.
requireQueue :: ArbiterServerConfig registry -> Text -> Handler QueueSpec
requireQueue config = requireKnown (serverQueues config)

-- | Look a queue up in a per-queue map, 404ing on any name this server does not serve.
requireKnown :: Map.Map Text a -> Text -> Handler a
requireKnown m queue = maybe (throwError err404 {errBody = "Unknown queue"}) pure (Map.lookup queue m)

jobNotFoundErr :: ServerError
jobNotFoundErr = err404 {errBody = "Job not found"}

staleLeaseErr :: ServerError
staleLeaseErr = err409 {errBody = "Lease is stale: not held by this claimant"}

rejectPayload :: String -> Handler a
rejectPayload err =
  throwError
    err400 {errBody = "payload does not match this queue's type: " <> LBS.fromStrict (TE.encodeUtf8 (T.pack err))}

-- | Lease up to @maxJobs@ jobs, payloads as raw JSON. The claimant stamped on each is the lease token.
claimJobsHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Map.Map Text (QueueSpec, Ops.ClaimSql)
  -> Text
  -> ClaimRequest
  -> Handler ClaimResponse
claimJobsHandler config claimSqls queue req = do
  (spec, claimSql) <- requireKnown claimSqls queue
  let env = serverEnv config
      leaseSecs = clampLease (maybe 60 realToFrac (crVisibilitySecs req))
      n = min maxClaimJobs (max 1 (crMaxJobs req))
  jobs <- liftIO $ do
    claimant <- nextRandom
    runSimpleDb
      env
      (Ops.claimJobsCached claimSql n leaseSecs (Just claimant) :: SimpleDb registry IO [JobRead Value])
  unless (null jobs) $ liftIO $ do
    claimedAt <- getCurrentTime
    traverse_ (\j -> fireHook (Job.onJobClaimed (queueHooks spec) j claimedAt)) jobs
  pure $ ClaimResponse (map ApiJob jobs)

-- | Bound a client-supplied lease to [1s, 1d].
clampLease :: NominalDiffTime -> NominalDiffTime
clampLease = min 86400 . max 1

-- | Run @act@ only if the caller still holds the lease, by claimant and attempt.
withLease
  :: forall registry a
   . ArbiterServerConfig registry
  -> Text
  -> Int64
  -> UUID
  -> Int32
  -> (JobRead Value -> SimpleDb registry IO (Int64, a))
  -> Handler a
withLease config queue jobId claimant claimedAttempts act = do
  let env = serverEnv config
      schemaName = schema env
  result <- liftIO $ handle (\StaleLease -> pure (Left staleLeaseErr)) $ runSimpleDb env $ withDbTransaction $ do
    mJob <- Ops.getJobById schemaName queue jobId :: SimpleDb registry IO (Maybe (JobRead Value))
    case mJob of
      Nothing -> pure (Left jobNotFoundErr)
      Just job
        | claimedBy job /= Just claimant || attempts job /= claimedAttempts ->
            pure (Left staleLeaseErr)
        | otherwise -> do
            (n, a) <- act job
            when (n == 0) $ liftIO (throwIO StaleLease)
            pure (Right a)
  either throwError pure result

-- | Rolls a 'withLease' transaction back when the lease turns out to be stale.
data StaleLease = StaleLease
  deriving stock (Show)

instance Exception StaleLease

-- | A finalizer carrying its lease guard in its own @WHERE@, returning the job it wrote.
-- Nothing costs one probe, to tell a missing job apart from a stale lease.
finalizeWithGuard
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Int64
  -> SimpleDb registry IO (Maybe (JobRead Value))
  -> Handler (JobRead Value)
finalizeWithGuard config queue jobId write = do
  void $ requireQueue config queue
  let env = serverEnv config
  written <- liftIO $ runSimpleDb env write
  case written of
    Just job -> pure job
    Nothing -> do
      exists <- liftIO $ runSimpleDb env (Ops.jobExists (schema env) queue jobId)
      throwError (if exists then staleLeaseErr else jobNotFoundErr)

-- | Ack a claimed job, optionally storing a result for its parent rollup. A job standing
-- outside every rollup acks in one guarded statement, the rest take the transactional path.
ackJobConsumerHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Int64
  -> AckRequest
  -> Handler NoContent
ackJobConsumerHandler config queue jobId req = do
  spec <- requireQueue config queue
  let env = serverEnv config
  leaf <-
    liftIO $
      runSimpleDb env (Ops.ackLeasedLeaf (schema env) queue jobId (arClaimedBy req) (arAttempts req))
      :: Handler (Maybe (JobRead Value))
  acked <- maybe rollupAck pure leaf
  liftIO $ do
    now <- getCurrentTime
    fireHook $ Job.onJobSuccess (queueHooks spec) acked (leaseHeldSince acked now) now
  pure NoContent
  where
    rollupAck =
      withLease config queue jobId (arClaimedBy req) (arAttempts req) $ \job -> do
        n <- Outcome.completeJob (schema (serverEnv config)) (arResult req) job
        pure (n, job)

-- | Soft-return a claimed job for reprocessing. Ends the lease, so it is claimable at once.
nackJobConsumerHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Int64
  -> NackRequest
  -> Handler NoContent
nackJobConsumerHandler config queue jobId req =
  NoContent
    <$ finalizeWithGuard
      config
      queue
      jobId
      (Ops.nackLeasedJob (schema (serverEnv config)) queue jobId (nrClaimedBy req) (nrAttempts req))

-- | Extend a claimed job's lease. A client-driven heartbeat, so it fires the queue's.
extendJobConsumerHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Int64
  -> ExtendRequest
  -> Handler NoContent
extendJobConsumerHandler config queue jobId req = do
  spec <- requireQueue config queue
  job <-
    finalizeWithGuard config queue jobId $
      Ops.extendLeasedJob
        (schema (serverEnv config))
        queue
        (clampLease (realToFrac (erVisibilitySecs req)))
        jobId
        (erClaimedBy req)
        (erAttempts req)
  liftIO $ do
    now <- getCurrentTime
    fireHook $ Job.onJobHeartbeat (queueHooks spec) job now (leaseHeldSince job now)
  pure NoContent

-- | Report a claimed job as failed, dead-lettering or retrying it as the worker would.
failJobConsumerHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Int64
  -> FailRequest
  -> Handler FailResponse
failJobConsumerHandler config queue jobId req = do
  spec <- requireQueue config queue
  (job, report) <- withLease config queue jobId (frClaimedBy req) (frAttempts req) $ \job -> do
    let schemaName = schema (serverEnv config)
        maxAtts = fromMaybe defaultMaxAttempts (maxAttempts job)
    report <-
      Outcome.failJob
        schemaName
        (queueRetry spec)
        (clampBackoff . realToFrac <$> frRetryDelaySecs req)
        (frPermanent req)
        maxAtts
        (frError req)
        job
    pure (Outcome.reportRows report, (job, report))
  liftIO $ fireFailureHooks (queueHooks spec) job (frError req) report
  pure $ case Outcome.reportOutcome report of
    Outcome.DeadLettered -> FailResponse {failOutcome = DeadLettered, failRetryInSecs = Nothing}
    Outcome.Retried delay -> FailResponse {failOutcome = Retried, failRetryInSecs = Just (realToFrac delay)}

-- | Run one lifecycle hook, swallowing a synchronous throw.
fireHook :: IO () -> IO ()
fireHook = handle swallowSync

-- | Fire what a failure fires: the queue's failure hook, then its retry or dead-letter hook.
fireFailureHooks :: ObservabilityHooks IO Value -> JobRead Value -> Text -> Outcome.FailureReport -> IO ()
fireFailureHooks hooks job err report = do
  now <- getCurrentTime
  traverse_
    (fireHook . snd)
    (Outcome.failureHookCalls hooks job err (leaseHeldSince job now) now (Outcome.reportOutcome report))

-- | When this lease was taken. Not @updated_at@, which a lease extension rewrites.
leaseHeldSince :: JobRead Value -> UTCTime -> UTCTime
leaseHeldSince job now = fromMaybe now (Job.lastAttemptedAt job)

-- | Bound a client-supplied retry delay to [0s, 1d].
clampBackoff :: NominalDiffTime -> NominalDiffTime
clampBackoff = min 86400 . max 0

-- | Get a queue's operator config.
getQueueDetailsHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Handler (Maybe QueueRow)
getQueueDetailsHandler config queue = do
  void $ requireQueue config queue
  let env = serverEnv config
      schemaName = schema env
  liftIO $ runSimpleDb env $ Ops.getQueue schemaName queue

-- | Flip the @paused@ flag for a queue this server serves.
setQueuePausedHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Bool
  -> Text
  -> Handler NoContent
setQueuePausedHandler config p queue = do
  void $ requireQueue config queue
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
updateCronScheduleHandler config name update@(CS.CronScheduleUpdate mExpr mOverlap mTz _) = do
  let env = serverEnv config
      schemaName = schema env

  -- Validate cron expression if provided
  case mExpr of
    Just (Just expr) ->
      case parseCronSchedule expr of
        Left _ -> throwError err400 {errBody = "Invalid cron expression"}
        Right _ -> pure ()
    _ -> pure ()

  -- Validate overlap policy if provided
  case mOverlap of
    Just (Just ov) ->
      case overlapPolicyFromText ov of
        Nothing ->
          throwError
            err400
              { errBody = "Invalid overlap policy: must be SkipOverlap or AllowOverlap"
              }
        Just _ -> pure ()
    _ -> pure ()

  -- Validate timezone if provided
  case mTz of
    Just (Just tzName) ->
      case resolveTZ tzName of
        Nothing ->
          throwError
            err400
              { errBody = "Invalid timezone: must be an IANA tz name (e.g. America/New_York)"
              }
        Just _ -> pure ()
    _ -> pure ()

  result <- liftIO $ runSimpleDb env $ withDbTransaction $ do
    _ <- Ops.updateCronSchedule schemaName name update
    Ops.getCronScheduleByName schemaName name

  case result of
    Nothing -> throwError err404 {errBody = "Cron schedule not found"}
    Just row -> pure row

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
   . ArbiterServerConfig registry
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

policyPrefixCacheTtl :: NominalDiffTime
policyPrefixCacheTtl = 5

-- | A TTL cache cell: an epoch bumped by 'invalidate', plus the current entry. Stamped
-- monotonically. The lock admits one producer, so an expiry does not stampede the database.
data CacheCell a = CacheCell (MVar ()) (TVar (Word, Maybe (Double, a)))

-- | An empty cache cell.
newCacheCell :: IO (CacheCell a)
newCacheCell = CacheCell <$> newMVar () <*> newTVarIO (0, Nothing)

-- | Serve from a short-TTL cache cell, concurrent misses queueing behind the one produce.
cachedFor :: NominalDiffTime -> CacheCell a -> IO a -> IO a
cachedFor ttl (CacheCell lock cell) produce =
  fresh >>= \case
    Just v -> pure v
    Nothing -> withMVar lock $ \() -> fresh >>= maybe fill pure
  where
    fresh = do
      now <- getMonotonicTime
      (_, cached) <- readTVarIO cell
      pure $ case cached of
        Just (ts, v) | now - ts < realToFrac ttl -> Just v
        _ -> Nothing
    fill = do
      (epoch, _) <- readTVarIO cell
      v <- produce
      now <- getMonotonicTime
      atomically $ modifyTVar' cell $ \(e, cur) -> if e == epoch then (e, Just (now, v)) else (e, cur)
      pure v

-- | Bump a cache cell's epoch and drop its entry, after an operator mutation.
invalidate :: CacheCell a -> Handler ()
invalidate (CacheCell _ cell) = liftIO $ atomically $ modifyTVar' cell $ \(e, _) -> (e + 1, Nothing)

-- | List policies with bucket stats and currently-throttled job counts.
listRateLimitsHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Handler RateLimitPoliciesResponse
listRateLimitsHandler config =
  liftIO $ cachedFor policyStatsCacheTtl (rateLimitPoliciesCache config) $ do
    views <- runSimpleDb env (Ops.listRateLimitPolicies (schema env) (knownQueues config))
    pure $ RateLimitPoliciesResponse {policies = views}
  where
    env = serverEnv config

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
   . ArbiterServerConfig registry
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
            _ -> void $ Ops.updateRateLimitPolicyOverridesAndWake schemaName queues prefix upd
          view = Ops.getRateLimitPolicy schemaName queues prefix
      v <- updateThenView env (update >> view) "Rate-limit policy not found"
      invalidate (rateLimitPoliciesCache config)
      pure v
  where
    env = serverEnv config
    schemaName = schema env
    queues = knownQueues config

-- | Clear every bucket for a prefix. Returns the number reset. 404s an unknown prefix.
resetRateLimitBucketsHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> Handler RateLimitResetResponse
resetRateLimitBucketsHandler config prefix = do
  let action =
        Ops.rateLimitPolicyExists schemaName prefix >>= \exists ->
          if exists
            then Just <$> Ops.resetRateLimitBucketsAndWake schemaName queues prefix
            else pure Nothing
  n <- updateThenView env action "Rate-limit policy not found"
  invalidate (rateLimitPoliciesCache config)
  pure $ RateLimitResetResponse {reset = n}
  where
    env = serverEnv config
    schemaName = schema env
    queues = knownQueues config

-- | Concurrency management/observability handlers.
concurrencyServer
  :: forall registry
   . ArbiterServerConfig registry
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
   . ArbiterServerConfig registry
  -> Handler ConcurrencyReconcileResponse
reconcileConcurrencyHandler config = do
  let env = serverEnv config
  n <- liftIO $ runSimpleDb env $ Ops.reconcileConcurrencyCounts (schema env) (knownQueues config)
  invalidate (concurrencyPoliciesCache config)
  pure $ ConcurrencyReconcileResponse {reconciled = n}

-- | Server for the shared top-level routes.
sharedServer
  :: forall registry
   . ArbiterServerConfig registry
  -> ServerT SharedAPI Handler
sharedServer config =
  queuesServer @registry config
    :<|> eventsServer config
    :<|> cronServer config
    :<|> workersServer config
    :<|> rateLimitsServer config
    :<|> concurrencyServer config
    :<|> runtimeTableServer @registry config

-- | The job, DLQ, and stats routes for a runtime-named queue. A registry queue's own
-- generated routes match first.
runtimeTableServer
  :: forall registry
   . ArbiterServerConfig registry
  -> Text
  -> TableAPI Value (AsServerT Handler)
runtimeTableServer config queue =
  hoistServer
    (Proxy @(NamedRoutes (TableAPI Value)))
    guarded
    (tableServer @registry @Value (runtimeAdmission config queue) queue config)
  where
    guarded :: forall a. Handler a -> Handler a
    guarded h = requireQueue config queue >> h

-- | Type class to build server implementations for registry entries
class BuildServer registry (reg :: [(Symbol, Type)]) where
  buildServer :: ArbiterServerConfig registry -> ServerT (RegistryToAPI reg) Handler

-- Base case: empty registry, just the shared top-level routes
instance BuildServer registry '[] where
  buildServer = sharedServer

-- Single table case: table endpoints :<|> shared top-level routes
instance
  ( JobPayload payload
  , KnownSymbol tableName
  )
  => BuildServer registry ('(tableName, payload) ': '[])
  where
  buildServer config =
    let tableName = T.pack $ symbolVal (Proxy @tableName)
     in tableServer @registry @payload payloadAdmission tableName config
          :<|> sharedServer config

-- Recursive case: table endpoints :<|> rest of tables (at least 2 tables total)
instance
  ( BuildServer registry (nextTable ': moreRest)
  , JobPayload payload
  , KnownSymbol tableName
  )
  => BuildServer registry ('(tableName, payload) ': (nextTable ': moreRest))
  where
  buildServer config =
    let tableName = T.pack $ symbolVal (Proxy @tableName)
     in tableServer @registry @payload payloadAdmission tableName config
          :<|> buildServer @registry @(nextTable ': moreRest) config

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

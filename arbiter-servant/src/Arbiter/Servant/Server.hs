{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DuplicateRecordFields #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Servant server implementation for the Arbiter job queue REST API.
--
-- Hard-coded to use the SimpleDb backend from arbiter-simple.
--
-- __Security:__ This module provides no built-in authentication or
-- authorization. All endpoints (including job deletion, DLQ management, and
-- cron schedule updates) are publicly accessible. Add your own auth
-- middleware before exposing this to untrusted networks.
module Arbiter.Servant.Server
  ( -- * Server handlers
    arbiterServer
  , arbiterApp
  , runArbiterAPI
  , ArbiterServerConfig (..)
  , initArbiterServer
  , BuildServer (..)
  ) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types (Job (..), JobPayload, JobRead)
import Arbiter.Core.MonadArbiter (withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.QueueRegistry (AllQueuesUnique, RegistryTables (..))
import Arbiter.Core.SqlTemplates (JobFilter (..))
import Arbiter.Simple (SimpleConnectionPool (..), SimpleEnv (..), createSimpleEnvWithConfig, runSimpleDb)
import Arbiter.Worker.Cron (overlapPolicyFromText)
import Control.Exception (SomeException, bracket, catch)
import Control.Monad (void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (toJSON)
import Data.ByteString (ByteString)
import Data.ByteString.Builder qualified as Builder
import Data.Int (Int64)
import Data.Kind (Type)
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromJust, fromMaybe)
import Data.Pool qualified as Pool
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (getCurrentTime)
import Data.Time.Format (defaultTimeLocale, formatTime)
import Database.PostgreSQL.Simple qualified as PG
import Database.PostgreSQL.Simple.Notification (Notification (..), getNotification)
import GHC.TypeLits (KnownSymbol, Symbol, symbolVal)
import Network.HTTP.Types (status200)
import Network.Wai (responseStream)
import Network.Wai.Handler.Warp (Port, defaultSettings, runSettings, setPort, setTimeout)
import Servant
import Servant.Server.Generic (AsServerT)
import System.Cron (parseCronSchedule)
import System.Timeout (timeout)

import Arbiter.Servant.API
  ( ArbiterAPI
  , CronAPI (..)
  , DLQAPI (..)
  , JobsAPI (..)
  , QueuesAPI (..)
  , RegistryToAPI
  , StatsAPI (..)
  , TableAPI (..)
  )
import Arbiter.Servant.Types

-- | Configuration for the API server
data ArbiterServerConfig registry = ArbiterServerConfig
  { serverEnv :: SimpleEnv registry
  -- ^ The SimpleEnv containing schema and connection pool
  , enableSSE :: Bool
  -- ^ Enable Server-Sent Events streaming endpoint. When 'False', the
  -- @\/events\/stream@ endpoint returns a single \"disabled\" event and
  -- closes immediately, avoiding long-lived connections. The admin UI
  -- falls back to polling-only mode. Default: 'True'.
  }

-- | Small pool configuration for admin API traffic.
serverPoolConfig :: PoolConfig
serverPoolConfig =
  PoolConfig
    { poolSize = 5
    , poolIdleTimeout = 60
    , poolStripes = Just 1
    }

-- | Create an 'ArbiterServerConfig' with its own internal connection pool.
--
-- __Note__: Use 'Arbiter.Migrations.runMigrationsForRegistry' with
-- @enableEventStreaming = True@ to set up the database triggers for SSE.
initArbiterServer
  :: forall registry
   . (AllQueuesUnique registry)
  => Proxy registry
  -> ByteString
  -> Text
  -> IO (ArbiterServerConfig registry)
initArbiterServer _proxy connStr schemaName = do
  env <- createSimpleEnvWithConfig (Proxy @registry) connStr schemaName serverPoolConfig
  pure ArbiterServerConfig {serverEnv = env, enableSSE = True}

-- | Wrap jobs with current timestamp for status derivation
toApiJobs :: (MonadIO m) => [JobRead payload] -> m [ApiJob payload]
toApiJobs jobs = do
  now <- liftIO getCurrentTime
  pure $ map (`ApiJob` now) jobs

toApiJob :: (MonadIO m) => JobRead payload -> m (ApiJob payload)
toApiJob job = do
  now <- liftIO getCurrentTime
  pure $ ApiJob job now

toApiDLQJobs :: (MonadIO m) => [DLQ.DLQJob payload] -> m [ApiDLQJob payload]
toApiDLQJobs jobs = do
  now <- liftIO getCurrentTime
  pure $ map (`ApiDLQJob` now) jobs

-- | Validate and sanitize pagination parameters
validatePagination :: Maybe Int -> Maybe Int -> (Int, Int)
validatePagination mLimit mOffset =
  let limit = max 1 $ min 1000 $ fromMaybe 50 mLimit -- Clamp between 1 and 1000
      offset = max 0 $ fromMaybe 0 mOffset -- Must be non-negative
   in (limit, offset)

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
    , getInFlightJobs = getInFlightJobsHandler @registry @payload table config
    , cancelJob = cancelJobHandler @registry table config
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
  -> Maybe Bool
  -> Handler (JobsResponse payload)
listJobsHandler tableName config mLimit mOffset mGroupKey mParentId mSuspended = liftIO $ do
  let (limit, offset) = validatePagination mLimit mOffset
      env = serverEnv config
      schemaName = schema env
      filters =
        catMaybes
          [ FilterGroupKey <$> mGroupKey
          , FilterParentId <$> mParentId
          , FilterSuspended <$> mSuspended
          ]

  (jobs, total, combined, dlqCounts) <- runSimpleDb env $ withDbTransaction $ do
    j <- Ops.listJobsFiltered schemaName tableName filters limit offset
    c <- Ops.countJobsFiltered schemaName tableName filters
    -- Only query child/DLQ counts if any returned job could be a parent.
    -- All parents are rollup finalizers (isRollup = True), so we
    -- skip the extra queries for queues that don't use job trees.
    let jobIds = map primaryKey j
        hasParents = any isRollup j
    if null j || not hasParents
      then pure (j, c, Map.empty, Map.empty)
      else do
        cc <- Ops.countChildrenBatch schemaName tableName jobIds
        dc <- Ops.countDLQChildrenBatch schemaName tableName jobIds
        pure (j, c, cc, dc)

  let childCounts = fmap fst combined
      pausedParents = Map.keys $ Map.filter (\(t, p) -> p == t) combined

  apiJobs <- toApiJobs jobs
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
  -> Handler (JobResponse payload)
insertJobHandler tableName config (ApiJobWrite jobWrite) = do
  let env = serverEnv config
      schemaName = schema env

  mJob <- liftIO $ runSimpleDb env $ Ops.insertJob schemaName tableName jobWrite
  case mJob of
    Nothing -> throwError err409 {errBody = "Job insert failed (duplicate dedup key)"}
    Just j -> do
      aj <- toApiJob j
      pure $ JobResponse {job = aj}

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

  inserted <- liftIO $ runSimpleDb env $ Ops.insertJobsBatch schemaName tableName writes
  apiJobs <- toApiJobs inserted
  pure $ BatchInsertResponse {inserted = apiJobs, insertedCount = length apiJobs}

-- | Get a specific job by ID
getJobHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Int64
  -> Handler (JobResponse payload)
getJobHandler tableName config jobId = do
  let env = serverEnv config
      schemaName = schema env

  mJob <- liftIO $ runSimpleDb env $ Ops.getJobById schemaName tableName jobId
  case mJob of
    Nothing -> throwError err404 {errBody = "Job not found"}
    Just j -> do
      aj <- toApiJob j
      pure $ JobResponse {job = aj}

-- | Get all in-flight (invisible) jobs
getInFlightJobsHandler
  :: forall registry payload
   . (JobPayload payload)
  => Text
  -> ArbiterServerConfig registry
  -> Maybe Int
  -> Maybe Int
  -> Handler (JobsResponse payload)
getInFlightJobsHandler tableName config mLimit mOffset = do
  let (limit, offset) = validatePagination mLimit mOffset
      env = serverEnv config
      schemaName = schema env

  (jobs, total, combined, dlqCounts) <- liftIO $ runSimpleDb env $ withDbTransaction $ do
    j <- Ops.getInFlightJobs schemaName tableName limit offset
    c <- Ops.countInFlightJobs schemaName tableName
    let jobIds = map primaryKey j
        hasParents = any isRollup j
    if null j || not hasParents
      then pure (j, c, Map.empty, Map.empty)
      else do
        cc <- Ops.countChildrenBatch schemaName tableName jobIds
        dc <- Ops.countDLQChildrenBatch schemaName tableName jobIds
        pure (j, c, cc, dc)

  let childCounts = fmap fst combined
      pausedParents = Map.keys $ Map.filter (\(t, p) -> p == t) combined

  apiJobs <- toApiJobs jobs
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
                pure (Left err409 {errBody = "Job is suspended — use resume endpoint"})
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
      Just job -> do
        -- Snapshot into parent_state before DLQ move (survives CASCADE delete).
        when (isRollup job) $ do
          (results, failures, mSnapshot, _dlqFailures) <-
            Ops.readChildResultsRaw schemaName tableName (primaryKey job)
          let merged = Ops.mergeRawChildResults results failures mSnapshot
          when (not (Map.null merged)) $
            void $
              Ops.persistParentState schemaName tableName (primaryKey job) (toJSON merged)
        Just <$> Ops.moveToDLQ schemaName tableName "Manually moved to DLQ via admin API" job

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
                pure (Left err409 {errBody = "Job is in-flight — cannot suspend"})

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
  -> Maybe Text
  -> Handler (DLQResponse payload)
listDLQHandler tableName config mLimit mOffset mParentId mGroupKey = do
  let (limit, offset) = validatePagination mLimit mOffset
      env = serverEnv config
      schemaName = schema env
      filters =
        catMaybes
          [ FilterParentId <$> mParentId
          , FilterGroupKey <$> mGroupKey
          ]

  (dlqJobs, total) <- liftIO $ runSimpleDb env $ withDbTransaction $ do
    j <- Ops.listDLQFiltered schemaName tableName filters limit offset
    c <- Ops.countDLQFiltered schemaName tableName filters
    pure (j, c)

  apiDlqJobs <- toApiDLQJobs dlqJobs
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
    , stats = statsServer @registry table config
    }

-- | Queues API handler
queuesServer
  :: forall registry
   . (RegistryTables registry)
  => Proxy registry
  -> QueuesAPI (AsServerT Handler)
queuesServer registryProxy =
  QueuesAPI
    { listQueues = pure $ QueuesResponse {queues = registryTableNames registryProxy}
    }

-- | Events server — raw WAI application for SSE streaming.
--
-- Uses WAI 'responseStream' with explicit flush after every event so the
-- browser receives data immediately.  Sends a @: keepalive@ comment every
-- 15 seconds to keep the connection alive through reverse proxies.
--
-- When 'enableSSE' is 'False', sends a single @disabled@ event and closes
-- immediately. The admin UI detects this and skips reconnection.
eventsServer
  :: forall registry
   . ArbiterServerConfig registry
  -> Tagged Handler Application
eventsServer config = Tagged $ \_req sendResponse ->
  if not (enableSSE config)
    then sendResponse $ responseStream status200 sseHeaders $ \write flush -> do
      write "data: {\"event\":\"disabled\"}\n\n"
      flush
    else do
      let env = serverEnv config
          connPool = fromJust (connectionPool $ simplePool env)
      bracket
        (Pool.takeResource connPool)
        ( \(conn, localPool) -> do
            -- Clean up LISTEN state before returning to pool so the connection
            -- doesn't accumulate buffered notifications while idle.
            -- If UNLISTEN fails, the connection is likely broken — destroy it
            -- instead of returning a dead connection to the pool.
            result <- (PG.execute_ conn "UNLISTEN *" >> pure True) `catch` (\(_ :: SomeException) -> pure False)
            if result
              then Pool.putResource localPool conn
              else Pool.destroyResource connPool localPool conn
        )
        $ \(conn, _) -> do
          _ <- PG.execute_ conn $ "LISTEN " <> fromString (T.unpack Schema.eventStreamingChannel)
          sendResponse $ responseStream status200 sseHeaders $ \write flush -> do
            -- Send immediate "connected" event
            write "data: {\"event\":\"connected\",\"message\":\"Stream connected\"}\n\n"
            flush
            -- Loop: wait for notifications with a 15s keepalive heartbeat.
            -- The heartbeat keeps Warp and any reverse proxies from timing out.
            let go = do
                  mNotification <- timeout 15_000_000 (getNotification conn)
                  case mNotification of
                    Just notification -> do
                      let payload = notificationData notification
                      write ("data: " <> Builder.byteString payload <> "\n\n")
                      flush
                      go
                    Nothing -> do
                      -- Timeout: send SSE comment to keep connection alive
                      write ": keepalive\n\n"
                      flush
                      go
            -- IOException from getNotification = PG connection lost
            -- IOException from write/flush = client disconnected
            go `catch` (\(_ :: SomeException) -> pure ())
  where
    sseHeaders =
      [ ("Content-Type", "text/event-stream")
      , ("Cache-Control", "no-cache")
      , ("Connection", "keep-alive")
      , ("X-Accel-Buffering", "no")
      ]

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

-- | List all cron schedules
listCronSchedulesHandler
  :: forall registry
   . ArbiterServerConfig registry
  -> Handler CronSchedulesResponse
listCronSchedulesHandler config = do
  let env = serverEnv config
      schemaName = schema env
      connPool = fromJust (connectionPool $ simplePool env)
  rows <- liftIO $ Pool.withResource connPool $ \conn ->
    CS.listCronSchedules conn schemaName
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
      connPool = fromJust (connectionPool $ simplePool env)

  -- Validate cron expression if provided
  case update.overrideExpression of
    Just (Just expr) ->
      case parseCronSchedule expr of
        Left err -> throwError err400 {errBody = "Invalid cron expression: " <> fromString err}
        Right _ -> pure ()
    _ -> pure ()

  -- Validate overlap policy if provided
  case update.overrideOverlap of
    Just (Just ov) ->
      case overlapPolicyFromText ov of
        Nothing ->
          throwError
            err400
              { errBody = "Invalid overlap policy: must be SkipOverlap or AllowOverlap"
              }
        Just _ -> pure ()
    _ -> pure ()

  -- Apply update and read back in a single connection
  result <- liftIO $ Pool.withResource connPool $ \conn -> do
    rowsAffected <- CS.updateCronSchedule conn schemaName name update
    if rowsAffected == 0
      then pure Nothing
      else CS.getCronScheduleByName conn schemaName name

  case result of
    Nothing -> throwError err404 {errBody = "Cron schedule not found"}
    Just row -> pure row

-- | Type class to build server implementations for registry entries
class BuildServer registry (reg :: [(Symbol, Type)]) where
  buildServer :: ArbiterServerConfig registry -> ServerT (RegistryToAPI reg) Handler

-- Base case: empty registry, just queues, events, and cron endpoints
instance
  (RegistryTables registry)
  => BuildServer registry '[]
  where
  buildServer config =
    queuesServer @registry (Proxy @registry)
      :<|> eventsServer config
      :<|> cronServer config

-- Single table case: table endpoints :<|> queues :<|> events :<|> cron endpoints
instance
  ( JobPayload payload
  , KnownSymbol tableName
  , RegistryTables registry
  )
  => BuildServer registry ('(tableName, payload) ': '[])
  where
  buildServer config =
    let tableName = T.pack $ symbolVal (Proxy @tableName)
     in tableServer @registry @payload tableName config
          :<|> queuesServer @registry (Proxy @registry)
          :<|> eventsServer config
          :<|> cronServer config

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
     in tableServer @registry @payload tableName config
          :<|> buildServer @registry @(nextTable ': moreRest) config

-- | Complete Arbiter server at @\/api\/v1\/...@
arbiterServer
  :: forall registry
   . (BuildServer registry registry)
  => ArbiterServerConfig registry
  -> ServerT (ArbiterAPI registry) Handler
arbiterServer = buildServer @registry @registry

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

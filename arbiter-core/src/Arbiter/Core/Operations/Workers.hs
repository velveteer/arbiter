-- | Worker-registry database operations.
module Arbiter.Core.Operations.Workers
  ( registerWorker
  , heartbeatWorker
  , setWorkerPaused
  , markWorkerShuttingDown
  , deregisterWorker
  , workerRegistered
  , listWorkers
  , sweepStaleWorkers
  ) where

import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.Maybe (listToMaybe)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Data.UUID.Types (UUID)

import Arbiter.Core.Exceptions (throwParsing)
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.MonadArbiter qualified as MA
import Arbiter.Core.Sql.Query (Query)
import Arbiter.Core.Sql.Workers qualified as Sql
import Arbiter.Core.Worker (WorkerRow (..), workerHealthFromText)

-- | Register or refresh a worker and return its effective paused state.
registerWorker
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -> Text
  -> Maybe Text
  -> Maybe Int32
  -> NominalDiffTime
  -> Maybe Value
  -> m (Maybe Bool)
registerWorker schema workerId queue host threads staleThreshold metadata =
  listToMaybe
    <$> MA.executeQuery
      (Sql.upsertWorkerSQL schema workerId queue host threads (realToFrac staleThreshold) metadata)

-- | Record a heartbeat and return the worker's effective paused state.
heartbeatWorker :: (MonadArbiter m) => SchemaName -> UUID -> m (Maybe Bool)
heartbeatWorker schema workerId =
  listToMaybe <$> MA.executeQuery (Sql.heartbeatWorkerSQL schema workerId)

-- | Set a worker's pause flag.
setWorkerPaused :: (MonadArbiter m) => SchemaName -> UUID -> Bool -> m Int64
setWorkerPaused schema workerId paused =
  countOrZero (Sql.setWorkerPausedSQL schema paused workerId)

-- | Mark a worker as gracefully draining.
markWorkerShuttingDown :: (MonadArbiter m) => SchemaName -> UUID -> m Int64
markWorkerShuttingDown schema workerId =
  MA.executeStatement (Sql.markWorkerShuttingDownSQL schema workerId)

-- | Remove a worker registry row.
deregisterWorker :: (MonadArbiter m) => SchemaName -> UUID -> m Int64
deregisterWorker schema workerId =
  MA.executeStatement (Sql.deleteWorkerSQL schema workerId)

-- | Whether the worker registry holds this identity.
workerRegistered :: (MonadArbiter m) => SchemaName -> UUID -> m Bool
workerRegistered schema workerId =
  or <$> MA.executeQuery (Sql.workerRegisteredSQL schema workerId)

-- | List workers, optionally filtered by queue and heartbeat age.
listWorkers
  :: (MonadArbiter m)
  => SchemaName
  -> Maybe Text
  -> Maybe NominalDiffTime
  -> m [WorkerRow]
listWorkers schema queue liveSecs = do
  rows <- MA.executeQuery (Sql.listWorkersSQL schema queue (realToFrac <$> liveSecs))
  traverse decodeHealth rows
  where
    decodeHealth (worker, rawHealth) = do
      health <- either throwParsing pure (workerHealthFromText rawHealth)
      pure worker {health}

-- | Delete workers older than their recorded stale threshold.
sweepStaleWorkers :: (MonadArbiter m) => SchemaName -> m Int64
sweepStaleWorkers schema = MA.executeStatement (Sql.deleteStaleWorkersSQL schema)

countOrZero :: (MonadArbiter m) => Query Int64 -> m Int64
countOrZero query = do
  rows <- MA.executeQuery query
  pure $ case rows of
    [n] -> n
    _ -> 0

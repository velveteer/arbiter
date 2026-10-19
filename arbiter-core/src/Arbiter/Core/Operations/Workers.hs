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
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Data.UUID.Types (UUID)

import Arbiter.Core.Exceptions (orThrowParsing)
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter, countOr0)
import Arbiter.Core.MonadArbiter qualified as MA
import Arbiter.Core.Sql.Workers qualified as Sql
import Arbiter.Core.Worker (WorkerRow (..), workerHealthFromText)

-- | Register or refresh a worker and return its effective pause state (worker or
-- queue). 'Nothing' when the upsert returns no row.
registerWorker
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -- ^ Worker id
  -> Text
  -- ^ Queue name
  -> Maybe Text
  -- ^ Host name
  -> Maybe Int32
  -- ^ Worker thread count
  -> NominalDiffTime
  -- ^ Heartbeat age after which the worker counts as stale
  -> Maybe Value
  -- ^ Free-form metadata
  -> m (Maybe Bool)
registerWorker schemaName workerId queue host threads staleThreshold metadata =
  MA.queryMaybe
    (Sql.upsertWorkerSQL schemaName workerId queue host threads (realToFrac staleThreshold) metadata)

-- | Record a heartbeat and return the worker's effective pause state (worker or queue).
-- 'Nothing' when the worker has no registry row.
heartbeatWorker :: (MonadArbiter m) => SchemaName -> UUID -> m (Maybe Bool)
heartbeatWorker schemaName workerId =
  MA.queryMaybe (Sql.heartbeatWorkerSQL schemaName workerId)

-- | Set a worker's pause flag and notify the worker.
setWorkerPaused :: (MonadArbiter m) => SchemaName -> UUID -> Bool -> m Int64
setWorkerPaused schemaName workerId paused =
  countOr0 (Sql.setWorkerPausedSQL schemaName paused workerId)

-- | Mark a worker as gracefully draining.
markWorkerShuttingDown :: (MonadArbiter m) => SchemaName -> UUID -> m Int64
markWorkerShuttingDown schemaName workerId =
  MA.executeStatement (Sql.markWorkerShuttingDownSQL schemaName workerId)

-- | Remove a worker registry row.
deregisterWorker :: (MonadArbiter m) => SchemaName -> UUID -> m Int64
deregisterWorker schemaName workerId =
  MA.executeStatement (Sql.deleteWorkerSQL schemaName workerId)

-- | Whether the worker registry holds this identity.
workerRegistered :: (MonadArbiter m) => SchemaName -> UUID -> m Bool
workerRegistered schemaName workerId =
  MA.queryAny (Sql.workerRegisteredSQL schemaName workerId)

-- | List workers, optionally filtered by queue and heartbeat age.
listWorkers
  :: (MonadArbiter m)
  => SchemaName
  -> Maybe Text
  -> Maybe NominalDiffTime
  -> m [WorkerRow]
listWorkers schemaName queue liveSecs = do
  rows <- MA.executeQuery (Sql.listWorkersSQL schemaName queue (realToFrac <$> liveSecs))
  traverse decodeHealth rows
  where
    decodeHealth (worker, rawHealth) = do
      health <- orThrowParsing (workerHealthFromText rawHealth)
      pure worker {health}

-- | Delete workers whose last heartbeat is older than their stale threshold.
sweepStaleWorkers :: (MonadArbiter m) => SchemaName -> m Int64
sweepStaleWorkers schemaName = MA.executeStatement (Sql.deleteStaleWorkersSQL schemaName)

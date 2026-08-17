-- | High-level worker and queue registry operations.
module Arbiter.Core.HighLevel.Runtime
  ( registerWorker
  , heartbeatWorker
  , setWorkerPaused
  , markWorkerShuttingDown
  , deregisterWorker
  , listWorkers
  , sweepStaleWorkers
  , ensureQueue
  , setQueuePaused
  , getQueue
  , listQueues
  ) where

import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import Data.UUID.Types (UUID)

import Arbiter.Core.MonadArbiter (MonadArbiter, getSchema)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.Queues (QueueRow)
import Arbiter.Core.Worker (WorkerRow)

-- | Register or refresh a worker and return its effective pause state.
registerWorker
  :: (MonadArbiter m)
  => UUID
  -> Text
  -> Maybe Text
  -> Maybe Int32
  -> NominalDiffTime
  -> Maybe Value
  -> m (Maybe Bool)
registerWorker workerId queue host threads staleThreshold metadata =
  getSchema >>= \schema -> Ops.registerWorker schema workerId queue host threads staleThreshold metadata

-- | Record a heartbeat and return the worker's effective pause state.
heartbeatWorker :: (MonadArbiter m) => UUID -> m (Maybe Bool)
heartbeatWorker workerId = getSchema >>= \schema -> Ops.heartbeatWorker schema workerId

-- | Set a worker's pause flag.
setWorkerPaused :: (MonadArbiter m) => UUID -> Bool -> m Int64
setWorkerPaused workerId paused = getSchema >>= \schema -> Ops.setWorkerPaused schema workerId paused

-- | Mark a worker as gracefully draining.
markWorkerShuttingDown :: (MonadArbiter m) => UUID -> m Int64
markWorkerShuttingDown workerId = getSchema >>= \schema -> Ops.markWorkerShuttingDown schema workerId

-- | Remove a worker registry row.
deregisterWorker :: (MonadArbiter m) => UUID -> m Int64
deregisterWorker workerId = getSchema >>= \schema -> Ops.deregisterWorker schema workerId

-- | List workers, optionally filtered by queue and heartbeat age.
listWorkers :: (MonadArbiter m) => Maybe Text -> Maybe NominalDiffTime -> m [WorkerRow]
listWorkers queue liveSecs = getSchema >>= \schema -> Ops.listWorkers schema queue liveSecs

-- | Delete workers older than their recorded stale threshold.
sweepStaleWorkers :: (MonadArbiter m) => m Int64
sweepStaleWorkers = getSchema >>= Ops.sweepStaleWorkers

-- | Ensure a queue registry row exists.
ensureQueue :: (MonadArbiter m) => Text -> m Int64
ensureQueue queue = getSchema >>= \schema -> Ops.ensureQueue schema queue

-- | Set a queue's pause flag and notify its workers.
setQueuePaused :: (MonadArbiter m) => Text -> Bool -> m Int64
setQueuePaused queue paused = getSchema >>= \schema -> Ops.setQueuePaused schema queue paused

-- | Get one queue registry row.
getQueue :: (MonadArbiter m) => Text -> m (Maybe QueueRow)
getQueue queue = getSchema >>= \schema -> Ops.getQueue schema queue

-- | List all queues registered in the schema.
listQueues :: (MonadArbiter m) => m [QueueRow]
listQueues = getSchema >>= Ops.listQueues

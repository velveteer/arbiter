{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Dispatcher
  ( runDispatcher
  ) where

import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.QueueRegistry (TableForPayload)
import Data.ByteString.Char8 qualified as BSC
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Proxy (Proxy (..))
import Data.Text qualified as T
import GHC.TypeLits (symbolVal)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Exception qualified as Ex
import UnliftIO.MVar qualified as MVar
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config (HandlerMode (..), WorkerConfig (..))
import Arbiter.Worker.Logger (LogLevel (..))
import Arbiter.Worker.Logger.Internal (tryLog)
import Arbiter.Worker.NotificationListener (withNotificationLoop)

-- | Run the dispatcher loop
--
-- The dispatcher wakes on PostgreSQL NOTIFY (when jobs are inserted) or poll timer
-- expiration, then claims jobs up to available worker capacity. If no workers are
-- free, the wakeup is a no-op.
--
-- __Responsiveness__: Workers that finish between notifications may sit idle until the
-- next notification or poll. For maximum responsiveness with existing jobs, set a short
-- poll interval (e.g., @pollInterval = Just 1@ for 1 second). For workloads with
-- continuous job insertion, NOTIFY provides sub-second latency.
--
-- The dispatcher runs in a dedicated thread with its own database connection.
runDispatcher
  :: forall m registry payload result
   . ( MonadUnliftIO m
     , QueueOperation m registry payload
     )
  => WorkerConfig m payload result
  -> Int
  -- ^ Worker capacity
  -> STM.TBQueue (NonEmpty (JobRead payload))
  -- ^ Work queue (batches of jobs)
  -> STM.TVar Int
  -- ^ Busy worker count
  -> Maybe (MVar.MVar ())
  -- ^ Liveness signal (pulsed after each successful claim cycle)
  -> STM.TVar Bool
  -- ^ Worker finished signal
  -> m ()
runDispatcher config workerCapacity workQueue busyWorkerCount mLivenessMVar workerFinishedVar = do
  let
    tableNameVal = T.pack $ symbolVal (Proxy @(TableForPayload payload registry))

    calcFreeWorkers :: STM.STM Int
    calcFreeWorkers = do
      busyCount <- STM.readTVar busyWorkerCount
      qLen <- fromIntegral <$> STM.lengthTBQueue workQueue
      pure $ workerCapacity - (busyCount + qLen)

    -- Get free workers if any are available (non-blocking)
    getFreeWorkers :: STM.STM (Maybe Int)
    getFreeWorkers = do
      free <- calcFreeWorkers
      pure $ if free > 0 then Just free else Nothing

    claimAndEnqueue :: Int -> m ()
    claimAndEnqueue freeWorkers = do
      eJobs <- Ex.tryAny $ case handlerMode config of
        SingleJobMode _ ->
          fmap (map (:| [])) (Arb.claimNextVisibleJobs freeWorkers (visibilityTimeout config))
        BatchedJobsMode batchSize _ ->
          Arb.claimNextVisibleJobsBatched batchSize freeWorkers (visibilityTimeout config)
      case eJobs of
        Left e -> do
          tryLog (logConfig config) Error $ "Dispatcher exception: " <> T.pack (show e)
        Right batches -> do
          STM.atomically $ traverse_ (STM.writeTBQueue workQueue) batches
          traverse_ (flip MVar.tryPutMVar ()) mLivenessMVar

    -- Claim jobs on wakeup if workers are available
    claimOnWakeup :: m ()
    claimOnWakeup = do
      mFree <- STM.atomically getFreeWorkers
      traverse_ claimAndEnqueue mFree

  -- Claim on startup
  claimOnWakeup

  -- The notification loop wakes on DB notifications, poll timer, or worker completion
  let notificationChannel = T.unpack $ Schema.notificationChannelForTable tableNameVal
      workerFinishedTrigger = Just $ do
        d <- STM.readTVar workerFinishedVar
        STM.checkSTM d
        STM.writeTVar workerFinishedVar False
  withNotificationLoop
    (BSC.unpack . connStr $ config)
    notificationChannel
    (workerStateVar config)
    (pollInterval config)
    (Just $ logConfig config)
    workerFinishedTrigger
    (const claimOnWakeup)

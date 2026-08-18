{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Dispatcher
  ( runDispatcher
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.Listen (Notification)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (void)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty (..))
import UnliftIO.Exception qualified as Ex
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config (HandlerMode (..), WorkerConfig (..), handlerBatchSize, readEffectiveState)
import Arbiter.Worker.Logger (LogLevel (..), tryLog)
import Arbiter.Worker.NotificationListener (runNotificationConsumer)

-- | Wake on NOTIFY, poll timer, or worker-finished, then claim up to capacity.
-- @notifVar@ is filled from the shared hub in "Arbiter.Core.Listen".
runDispatcher
  :: forall payload m
   . (QueueOperation m payload)
  => WorkerConfig m payload
  -> Int
  -> STM.TBQueue (NonEmpty (JobRead payload))
  -> STM.TVar Int
  -> STM.TVar Bool
  -> STM.TVar (Maybe Notification)
  -> m ()
runDispatcher config workerCapacity workQueue busyWorkerCount workerFinishedVar notifVar = do
  -- The claim statement only varies with free capacity, so render every variant once.
  claimSql <-
    Arb.mkClaimSql @payload (handlerBatchSize config) workerCapacity (visibilityTimeout config) (Just (workerId config))
  let
    calcFreeWorkers :: STM.STM Int
    calcFreeWorkers = do
      busyCount <- STM.readTVar busyWorkerCount
      qLen <- fromIntegral <$> STM.lengthTBQueue workQueue
      pure $ workerCapacity - (busyCount + qLen)

    getFreeWorkers :: STM.STM (Maybe Int)
    getFreeWorkers = do
      free <- calcFreeWorkers
      pure $ if free > 0 then Just free else Nothing

    claimAndEnqueue :: Int -> m ()
    claimAndEnqueue freeWorkers = do
      eJobs <- Ex.tryAny $ case handlerMode config of
        SingleJobMode _ ->
          map (:| []) <$> Ops.claimJobsCached claimSql freeWorkers
        BatchedJobsMode _ _ ->
          Ops.claimJobsBatchedCached claimSql freeWorkers
      case eJobs of
        Left e ->
          tryLog (logConfig config) Error $ "Dispatcher exception: " <> displayEx e
        Right batches ->
          STM.atomically $ traverse_ (STM.writeTBQueue workQueue) batches
      -- Pulse on every attempt so a failing claim path still proves liveness.
      STM.atomically $ void $ STM.tryPutTMVar (heartbeatSignal config) ()

    claimOnWakeup :: m ()
    claimOnWakeup = do
      mFree <- STM.atomically getFreeWorkers
      traverse_ claimAndEnqueue mFree

    workerFinishedTrigger = Just $ do
      d <- STM.readTVar workerFinishedVar
      STM.checkSTM d
      STM.writeTVar workerFinishedVar False

  runNotificationConsumer
    (readEffectiveState config)
    (pollInterval config)
    notifVar
    workerFinishedTrigger
    (const claimOnWakeup)

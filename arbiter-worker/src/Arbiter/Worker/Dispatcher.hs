{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Dispatcher
  ( runDispatcher
  ) where

import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (void)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Text qualified as T
import Database.PostgreSQL.Simple.Notification qualified as PS
import UnliftIO (MonadUnliftIO)
import UnliftIO.Exception qualified as Ex
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config (HandlerMode (..), WorkerConfig (..), readEffectiveState)
import Arbiter.Worker.Logger (LogLevel (..))
import Arbiter.Worker.Logger.Internal (tryLog)
import Arbiter.Worker.NotificationListener (runNotificationConsumer)

-- | Wake on NOTIFY, poll timer, or worker-finished, then claim up to capacity.
-- @notifVar@ is filled by the shared "Arbiter.Worker.MultiChannelListener".
runDispatcher
  :: forall m registry payload result
   . ( MonadUnliftIO m
     , QueueOperation m registry payload
     )
  => WorkerConfig m payload result
  -> Int
  -> STM.TBQueue (NonEmpty (JobRead payload))
  -> STM.TVar Int
  -> STM.TVar Bool
  -> STM.TVar (Maybe PS.Notification)
  -> m ()
runDispatcher config workerCapacity workQueue busyWorkerCount workerFinishedVar notifVar = do
  -- The claim statement only varies with free capacity, so render every variant once.
  claimSql <-
    let batchSize = case handlerMode config of
          SingleJobMode _ -> 1
          BatchedJobsMode n _ -> n
     in Arb.mkClaimSql @m @registry @payload batchSize workerCapacity (visibilityTimeout config) (Just (workerId config))
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
          tryLog (logConfig config) Error $ "Dispatcher exception: " <> T.pack (show e)
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

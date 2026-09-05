{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Worker.Dispatcher
  ( runDispatcher
  ) where

import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (JobRead)
import Arbiter.Core.Listen (Notification)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (void)
import Data.Foldable (traverse_)
import Data.List.NonEmpty (NonEmpty (..))
import UnliftIO.Chan (Chan, writeChan)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config
  ( HandlerMode (..)
  , WorkerConfig (..)
  , handlerBatchSize
  , heartbeatSignal
  , readEffectiveState
  )
import Arbiter.Worker.Logger (LogLevel (..), newFailureGate, tryReported)
import Arbiter.Worker.NotificationListener (runNotificationConsumer)

-- | Wake on NOTIFY, poll timer, or worker-finished, then claim up to capacity.
-- @notifVar@ is filled from the shared hub in "Arbiter.Core.Listen".
runDispatcher
  :: forall payload m
   . (QueueOperation m payload)
  => WorkerConfig m payload
  -> Int
  -> Ops.JobStatements
  -> Chan (NonEmpty (JobRead payload))
  -> STM.TVar Int
  -> STM.TVar Int
  -> STM.TVar Bool
  -> STM.TVar (Maybe Notification)
  -> m ()
runDispatcher config workerCapacity statements workQueue queuedCount busyWorkerCount workerFinishedVar notifVar = do
  claimGate <- newFailureGate
  let
    calcFreeWorkers :: STM.STM Int
    calcFreeWorkers = do
      busyCount <- STM.readTVar busyWorkerCount
      queued <- STM.readTVar queuedCount
      pure $ workerCapacity - (busyCount + queued)

    getFreeWorkers :: STM.STM (Maybe Int)
    getFreeWorkers = do
      free <- calcFreeWorkers
      pure $ if free > 0 then Just free else Nothing

    claimAndEnqueue :: Int -> m ()
    claimAndEnqueue freeWorkers = do
      eJobs <- tryReported (logConfig config) Error claimGate "Dispatcher claim" $
        case handlerMode config of
          SingleJobMode _ ->
            map (:| []) <$> Ops.claimJobsCached statements freeWorkers
          BatchedJobsMode _ _ ->
            Ops.claimJobsBatchedCached statements freeWorkers
      -- The count goes up before the write, so a free-worker reading never overshoots.
      traverse_ (traverse_ (\batch -> STM.atomically (STM.modifyTVar' queuedCount (+ 1)) *> writeChan workQueue batch)) eJobs
      -- Pulse on every attempt, including a failed claim.
      STM.atomically $ void $ STM.tryPutTMVar (heartbeatSignal config) ()

    claimOnWakeup :: m ()
    claimOnWakeup = do
      mFree <- STM.atomically getFreeWorkers
      traverse_ claimAndEnqueue mFree

    workerFinishedTrigger = Just $ do
      finished <- STM.readTVar workerFinishedVar
      STM.checkSTM finished
      STM.writeTVar workerFinishedVar False

  runNotificationConsumer
    (readEffectiveState config)
    (pollInterval config)
    notifVar
    workerFinishedTrigger
    (const claimOnWakeup)

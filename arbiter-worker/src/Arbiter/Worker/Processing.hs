{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | The worker thread: takes batches off the queue and runs each through
-- "Arbiter.Worker.Batch".
module Arbiter.Worker.Processing
  ( workerLoop
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Job.Types qualified as Job
import Control.Exception (fromException)
import Control.Exception qualified as E
import Control.Monad (forever)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (toList)
import Data.List.NonEmpty (NonEmpty)
import UnliftIO
  ( MonadUnliftIO
  , atomically
  , finally
  , mask_
  )
import UnliftIO.Async qualified as Async
import UnliftIO.Concurrent (threadDelay)

import Arbiter.Worker.Batch (afterBatch, newHandoff, runBatch)
import Arbiter.Worker.ChannelHandlers (RunningJobs, withRegisteredJobs)
import Arbiter.Worker.Config
import Arbiter.Worker.Heartbeat (HeartbeatGuard)
import Arbiter.Worker.Logger
import Arbiter.Worker.Settlement (PoolEffects, PoolMode, batchLog)
import Arbiter.Worker.WorkQueue (WorkQueue, finishWork, popWork)

-- | The pause after a batch escapes with an unexpected exception.
exceptionPauseMicros :: Int
exceptionPauseMicros = 2_000_000

-- | Main loop for a single worker thread.
workerLoop
  :: forall payload m
   . (MonadUnliftIO m)
  => WorkerConfig m payload
  -> RunningJobs
  -- ^ Pool-shared map from job id to running handler async.
  -> HeartbeatGuard payload
  -> PoolMode m payload
  -> PoolEffects m payload
  -> WorkQueue (NonEmpty (Job.JobRead payload))
  -> m ()
workerLoop config runningJobs guard mode effectsFor workQueue =
  forever $ mask_ $ do
    -- Mask covers the window between taking a batch (which moves it from queued
    -- to busy) and entering the finally block that frees the busy slot.
    jobBatch <- popWork workQueue

    let jobIds = map Job.primaryKey (toList jobBatch)

    flip
      finally
      (atomically (finishWork workQueue))
      $ do
        handoff <- liftIO (newHandoff guard)
        let effects = effectsFor jobBatch
        result <-
          withRegisteredJobs runningJobs jobIds $
            liftIO (runBatch effects guard mode handoff jobBatch)
        case result of
          Right () -> pure ()
          Left exception
            | Just cancel <- fromException exception -> liftIO (afterBatch effects handoff jobBatch cancel)
            | Just Async.AsyncCancelled <- fromException exception -> liftIO (E.throwIO exception)
            | otherwise -> do
                tryLog (batchLog config jobBatch) Error $ "Worker exception: " <> displayEx exception
                threadDelay exceptionPauseMicros

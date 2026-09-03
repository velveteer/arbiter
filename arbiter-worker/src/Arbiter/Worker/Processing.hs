{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Handler execution and job settlement for a worker pool.
module Arbiter.Worker.Processing
  ( workerLoop
  ) where

import Arbiter.Core.Exceptions (JobForceCancelled (..), displayEx)
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobResult
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Trace (ConsumeSpan, resolveTracer, withConsumeSpan)
import Control.Exception (fromException)
import Control.Exception qualified as E
import Control.Monad (forever, unless)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (toList, traverse_)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Time (getCurrentTime)
import UnliftIO
  ( atomically
  , catchSyncOrAsync
  , finally
  , mask_
  , modifyTVar'
  , readTBQueue
  , tryAny
  , writeTVar
  )
import UnliftIO.Async qualified as Async
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.STM (TBQueue, TVar)

import Arbiter.Worker.ChannelHandlers (RunningJobs, withRegisteredJobs)
import Arbiter.Worker.Config
import Arbiter.Worker.Heartbeat (withJobsHeartbeat)
import Arbiter.Worker.Logger
import Arbiter.Worker.Logger.Internal (runHook)
import Arbiter.Worker.Results (storeJobResult)
import Arbiter.Worker.Settle
  ( CancelHandoff
  , cancelFinalized
  , finalized
  , markCancelFinalized
  , newCancelHandoff
  , pendingJobs
  , settleInterruptibly
  )
import Arbiter.Worker.Settlement
  ( ackOrGone
  , batchCallbacks
  , batchLog
  , finalizeForceCancelled
  , jobLog
  , reportBatchOutcome
  , reportSuccess
  )

-- | Main loop for a single worker thread.
workerLoop
  :: forall payload m
   . ( EncodeJobResult (ResultOf m payload)
     , JobOperation m payload
     )
  => WorkerConfig m payload
  -> ConsumeSpan
  -- ^ The pool's consumer-span shape, built once for its queue.
  -> RunningJobs
  -- ^ Pool-shared map from job id to running handler async.
  -> TBQueue (NonEmpty (Job.JobRead payload))
  -> TVar Int
  -- ^ Busy worker count
  -> TVar Bool
  -- ^ Worker finished signal
  -> m ()
workerLoop config consumeSpan runningJobs workQueue busyCount workerFinishedVar = forever $ mask_ $ do
  -- Mask covers the window between the atomic claim (which increments
  -- busyCount) and entering the finally block that decrements it.
  jobBatch <- atomically $ do
    batch <- readTBQueue workQueue
    modifyTVar' busyCount (+ 1)
    pure batch

  let jobIds = map Job.primaryKey (toList jobBatch)

  flip
    finally
    ( atomically $ do
        modifyTVar' busyCount (subtract 1)
        writeTVar workerFinishedVar True
    )
    $ do
      handoff <- newCancelHandoff
      result <-
        withRegisteredJobs runningJobs jobIds $
          processJobsWithRetry config consumeSpan handoff jobBatch
      case result of
        Right () -> pure ()
        Left e
          -- Finalized inside the job span, so the trace carries the cancel. One
          -- delivered before that catch, or interrupting it, arrives here undone.
          | Just (JobForceCancelled cancelledIds reclaimedIds) <- fromException e -> do
              alreadyFinalized <- cancelFinalized handoff
              unless alreadyFinalized $
                finalizeForceCancelled config jobBatch cancelledIds reclaimedIds handoff
          | Just Async.AsyncCancelled <- fromException e -> liftIO (E.throwIO e)
          | otherwise -> do
              tryLog (batchLog config jobBatch) Error $ "Worker exception: " <> displayEx e
              threadDelay 2_000_000

processJobsWithRetry
  :: forall payload m
   . ( EncodeJobResult (ResultOf m payload)
     , JobOperation m payload
     )
  => WorkerConfig m payload
  -> ConsumeSpan
  -- ^ The pool's consumer-span shape, built once for its queue.
  -> CancelHandoff
  -> NonEmpty (Job.JobRead payload)
  -> m ()
processJobsWithRetry config consumeSpan handoff jobs = do
  startTime <- liftIO getCurrentTime
  schemaName <- Arb.getSchema
  tracer <- resolveTracer
  let (firstJob :| _) = jobs
      -- Rethrown with base throwIO, UnliftIO's wrapping it as synchronous. The flag
      -- is set last, so an interrupted finalizer leaves the rest to 'workerLoop'.
      onForceCancel exc@(JobForceCancelled cancelledIds goneIds) = do
        finalizeForceCancelled config jobs cancelledIds goneIds handoff
        markCancelFinalized handoff
        liftIO $ E.throwIO exc
      claimHook job =
        runHook (jobLog config job) "onJobClaimed" $
          Job.onJobClaimed (observabilityHooks config) job startTime
  -- The span covers the claim hooks, the outcome report and the force-cancel
  -- finalizer, so every terminal hook fires while it is open.
  withConsumeSpan tracer consumeSpan jobs $ flip catchSyncOrAsync onForceCancel $ do
    traverse_ claimHook jobs
    result <-
      tryAny
        $ withJobsHeartbeat
          (observabilityHooks config)
          (jobHeartbeatInterval config)
          (visibilityTimeout config)
          (maxJobDuration config)
          startTime
          jobs
          (pendingJobs handoff jobs)
          (logConfig config)
          (heartbeatSignal config)
        $ case handlerMode config of
          SingleJobMode handler ->
            settleInterruptibly
              handoff
              (finalized [firstJob])
              ( withDbTransaction $ do
                  handlerResult <- runHandlerWithConnection handler firstJob
                  ackOrGone firstJob
                  storeJobResult schemaName firstJob handlerResult
              )
              (const (reportSuccess config startTime firstJob))
          BatchedJobsMode _ handler -> handler jobs (batchCallbacks config handoff jobs startTime schemaName)
    endTime <- liftIO getCurrentTime
    reportBatchOutcome config startTime endTime jobs handoff result

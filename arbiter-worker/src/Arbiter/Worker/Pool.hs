{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Execution of one worker pool and its job lifecycle. Multi-queue
-- orchestration lives in "Arbiter.Worker.MultiQueue".
module Arbiter.Worker.Pool
  ( runWorkerPool
  , runReaperOp
  ) where

import Arbiter.Core.Exceptions
  ( BranchCancelException (..)
  , JobException (..)
  , JobForceCancelled (..)
  , JobGoneException (..)
  , JobNackException (..)
  , JobPermanentException (..)
  , JobRetryableException (..)
  , ParsingException (..)
  , TreeCancelException (..)
  , displayEx
  , namedJobIds
  , throwJobGoneIds
  )
import Arbiter.Core.HighLevel (JobOperation, QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobResult
import Arbiter.Core.Listen qualified as Listen
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Arbiter.Core.Trace
  ( ConsumeShape (..)
  , ConsumeSpan
  , consumeSpanFor
  , markSpanError
  , recordJobCancelled
  , recordJobFailure
  , resolveTracer
  , withConsumeSpan
  )
import Control.Exception (SomeException, fromException, toException)
import Control.Exception qualified as E
import Control.Monad (forever, replicateM, unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Aeson (Value)
import Data.Bifunctor (second)
import Data.Bool (bool)
import Data.Either (fromRight, partitionEithers)
import Data.Foldable (toList, traverse_)
import Data.Int (Int32, Int64)
import Data.List (partition)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (NominalDiffTime, UTCTime, getCurrentTime)
import Data.UUID (UUID)
import System.Directory (removeFile)
import UnliftIO
  ( MonadUnliftIO
  , atomically
  , bracket
  , catchSyncOrAsync
  , checkSTM
  , finally
  , isEmptyTBQueue
  , lengthTBQueue
  , mask_
  , modifyTVar'
  , newTBQueueIO
  , newTVarIO
  , readTBQueue
  , readTVar
  , tryAny
  , waitAnyCatch
  , writeTVar
  )
import UnliftIO.Async qualified as Async
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.STM (STM, TBQueue, TVar)
import UnliftIO.STM qualified as STM

import Arbiter.Worker.BackoffStrategy
import Arbiter.Worker.ChannelHandlers
  ( RunningJobs
  , handleCancelNotif
  , handleCronRunNotif
  , handlePauseNotif
  , withRegisteredJobs
  )
import Arbiter.Worker.Config
import Arbiter.Worker.Cron (CronJob (..), runCronScheduler)
import Arbiter.Worker.Dispatcher
import Arbiter.Worker.Heartbeat (withJobsHeartbeat)
import Arbiter.Worker.Logger
import Arbiter.Worker.Logger.Internal
  ( runHook
  , withJobContext
  , withJobContextList
  , withJobContextOne
  )
import Arbiter.Worker.Reaper (reaperLoop, runReaperOp)
import Arbiter.Worker.Retry (spawnRetried)
import Arbiter.Worker.Settle
  ( CancelHandoff
  , byIdDesc
  , cancelFinalized
  , disowned
  , finalized
  , hasIdIn
  , markCancelFinalized
  , newCancelHandoff
  , pendingJobs
  , record
  , recordCancelled
  , settle
  , settleBy
  , settleInterruptibly
  , unownedJobs
  )

-- ---------------------------------------------------------------------------
-- Worker Pool
-- ---------------------------------------------------------------------------

-- | Run a worker pool: a dispatcher and its worker threads.
runWorkerPool
  :: forall payload m
   . ( Arb.RegistryAdmissionPolicies (RegistryOf m)
     , EncodeJobResult (ResultOf m payload)
     , QueueOperation m payload
     , RegistryTables (RegistryOf m)
     )
  => WorkerConfig m payload
  -> m ()
runWorkerPool config = do
  case validateWorkerConfig config of
    Left err -> liftIO $ E.throwIO (WorkerConfigException err)
    Right () -> pure ()
  let workerCap = workerCount config
      queueName = Arb.queueTable @payload @m
      -- Constant for the pool, so a claim never rebuilds the queue-wide attributes.
      consumeSpan = consumeSpanFor queueName (poolSpanShape config)

  schemaName <- getSchema
  workQueue <- newTBQueueIO (fromIntegral workerCap)
  busyWorkerCount <- newTVarIO 0
  workerFinishedVar <- newTVarIO False
  runningJobs <- STM.newTVarIO Map.empty

  registerResult <- tryAny (registerSelf config schemaName queueName)
  case registerResult of
    Left e -> warnEx (logConfig config) "Worker registry insert failed" e
    Right mPaused ->
      traverse_ (atomically . writePause config) mPaused

  dispatcherNotifVar <- STM.newTVarIO Nothing
  cronRunVar <- STM.newTVarIO False
  let createChannel = TE.encodeUtf8 (Schema.notificationChannelForTable queueName)
      pauseChannel = TE.encodeUtf8 (Schema.pauseNotifyChannel schemaName queueName)
      cancelChannel = TE.encodeUtf8 (Schema.cancelNotifyChannel schemaName queueName)
      cronRunChannel = TE.encodeUtf8 (Schema.cronRunNotifyChannel schemaName)
      cronNames = Set.fromList (map name (cronJobs config))
      cronHandlers =
        if null (cronJobs config)
          then []
          else [(cronRunChannel, handleCronRunNotif cronNames cronRunVar)]
      handlers =
        [ (createChannel, atomically . STM.writeTVar dispatcherNotifVar . Just)
        , (pauseChannel, handlePauseNotif config)
        , (cancelChannel, handleCancelNotif config runningJobs)
        ]
          <> cronHandlers

  evalContT $ do
    withLivenessFile config
    mListener <- lift getListener
    listenerReady <- case mListener of
      Nothing -> do
        lift $ tryLog (logConfig config) Info "No listen connection, running poll-only"
        pure (pure True)
      Just listener ->
        ContT $
          Listen.withChannels
            listener
            (Listen.HubLog (tryLog (logConfig config) Warning) (tryLog (logConfig config) Error))
            handlers
    void . ContT $ Async.withAsync (publishListenerReady config listenerReady)
    let spawn = spawnRetried (workerStateVar config) (logConfig config) queueName
    heartbeat <-
      spawn "Worker heartbeat" $
        heartbeatLoop config schemaName queueName
    dispatcher <-
      spawn "Dispatcher" $
        runDispatcher config workerCap workQueue busyWorkerCount workerFinishedVar dispatcherNotifVar
    workers <-
      replicateM workerCap
        $ spawn "Worker thread"
        $ workerLoop config consumeSpan runningJobs workQueue busyWorkerCount workerFinishedVar
    crons <-
      unlessNull (cronJobs config)
        $ spawn "Cron scheduler"
        $ runCronScheduler (workerStateVar config) cronRunVar (logConfig config) schemaName queueName (cronJobs config)
    reaper <-
      spawn "Reaper" $
        reaperLoop (logConfig config) (onMaintenance config) (reaperInterval config) (reaperTimeout config)

    (_, res) <- waitAnyCatch (dispatcher : reaper : heartbeat : crons <> workers)
    case res of
      Left e ->
        lift $ tryLog (logConfig config) Error $ "Thread pool exception: " <> displayEx e
      Right _ -> pure ()

    lift $ shutdownPool config schemaName workQueue busyWorkerCount

-- | Flip 'listenerReadyVar' once the pool's channels are subscribed. Runs
-- alongside the pool and never gates startup.
publishListenerReady :: (MonadUnliftIO m) => WorkerConfig n payload -> STM Bool -> m ()
publishListenerReady config ready =
  atomically $ do
    ready >>= checkSTM
    writeTVar (listenerReadyVar config) True

-- | Remove the liveness file when the pool exits, after the drain.
withLivenessFile :: (MonadUnliftIO m) => WorkerConfig n payload -> ContT r m ()
withLivenessFile config = case livenessFile config of
  Nothing -> pure ()
  Just path -> ContT $ \k ->
    bracket
      (pure ())
      (\_ -> void . tryAny . liftIO . removeFile $ path)
      (\_ -> k ())

-- | Re-insert the worker's registry row, returning the effective paused state for the
-- caller to seed 'pauseVar' with.
registerSelf :: (MonadArbiter m) => WorkerConfig n payload -> SchemaName -> Text -> m (Maybe Bool)
registerSelf config schemaName queueName =
  Ops.registerWorker
    schemaName
    (workerId config)
    queueName
    (workerHost config)
    (Just (fromIntegral (workerCount config)))
    (workerStaleThreshold config)
    (workerMetadata config)

-- | Mark shutting-down, drain, then deregister. Every write is best-effort and logged
-- when it fails.
shutdownPool
  :: (MonadArbiter m)
  => WorkerConfig n payload
  -> SchemaName
  -> TBQueue a
  -> TVar Int
  -> m ()
shutdownPool config schemaName workQueue busyCount = do
  shutdownWorker config
  let wid = workerId config
      logCfg = logConfig config
  tryWarn logCfg "Failed to mark worker shutting down" (Ops.markWorkerShuttingDown schemaName wid)
  drainPool logCfg (gracefulShutdownTimeout config) workQueue busyCount
  tryWarn logCfg "Failed to deregister worker" (Ops.deregisterWorker schemaName wid)

-- | Wait for the work queue to drain and all worker threads to go idle,
-- optionally bounded by a timeout. Logs the entry, periodic progress (every
-- 10s) when no timeout is set, and the result.
drainPool
  :: (MonadUnliftIO m)
  => LogConfig
  -> Maybe NominalDiffTime
  -> TBQueue a
  -> TVar Int
  -> m ()
drainPool logCfg mTimeout workQueue busyCount = do
  tryLog logCfg Info "Starting graceful shutdown. Draining in-flight jobs..."
  result <- case mTimeout of
    Nothing -> Right () <$ drainLoop
    Just timeoutSecs ->
      Async.race (threadDelay $ ceiling (timeoutSecs * 1_000_000)) waitForDrain
  case result of
    Right () -> tryLog logCfg Info "All workers are now idle. Graceful shutdown complete."
    Left () -> tryLog logCfg Warning "Graceful shutdown timed out. Some jobs may still be in-flight."
  where
    waitForDrain = atomically $ do
      qEmpty <- isEmptyTBQueue workQueue
      checkSTM qEmpty
      busy <- readTVar busyCount
      checkSTM (busy == 0)
    drainLoop = do
      drainOrTick <- Async.race (threadDelay 10_000_000) waitForDrain
      case drainOrTick of
        Right () -> pure ()
        Left () -> do
          (busy, qLen) <-
            atomically $
              (,)
                <$> readTVar busyCount
                <*> (fromIntegral <$> lengthTBQueue workQueue)
          tryLog logCfg Info $
            "Graceful shutdown: waiting for "
              <> T.pack (show (busy :: Int))
              <> " busy worker(s), "
              <> T.pack (show (qLen :: Int))
              <> " job(s) in queue..."
          drainLoop

-- | Ticks at @pollInterval@, gated by a proof-of-work signal unless paused.
-- Each tick bumps @arbiter_workers.last_heartbeat@, reconciles the registry
-- @paused@ flag into local state, and re-registers if the sweeper deleted
-- the row.
heartbeatLoop
  :: (MonadArbiter m)
  => WorkerConfig n payload
  -> SchemaName
  -> Text
  -- ^ Queue name (used when the row needs re-registering).
  -> m ()
heartbeatLoop config schemaName queueName = do
  tick
  forever $ throttledWait *> tick
  where
    logCfg = logConfig config
    sig = heartbeatSignal config
    readShuttingDown = (== ShuttingDown) <$> STM.readTVar (workerStateVar config)
    cadenceMicros = ceiling (workerHeartbeatInterval config * 1_000_000)
    throttledWait = do
      delayVar <- STM.registerDelay cadenceMicros
      atomically $ do
        STM.readTVar delayVar >>= checkSTM
        paused <- STM.readTVar (pauseVar config)
        unless paused $ STM.takeTMVar sig
    tick = do
      traverse_
        (\path -> tryWarn logCfg "Liveness probe write failed" (liftIO $ writeFile path ""))
        (livenessFile config)
      epoch <- STM.atomically $ STM.readTVar (pauseEpoch config)
      result <- tryAny $ Ops.heartbeatWorker schemaName (workerId config)
      case result of
        Left e -> warnEx logCfg "Worker registry heartbeat failed" e
        Right Nothing -> reregister
        Right (Just rp) -> reconcile epoch rp
    reregister = do
      shutting <- STM.atomically readShuttingDown
      unless shutting $ do
        tryLog logCfg Warning "Worker registry row missing, re-registering"
        tryWarn logCfg "Worker re-registration failed" (registerSelf config schemaName queueName)
    reconcile epoch rp =
      STM.atomically $ do
        shutting <- readShuttingDown
        unless shutting $ writePauseIfCurrent config epoch rp

-- | @[]@ when the list is empty, else a singleton holding @act@'s result.
unlessNull :: (Applicative f) => [a] -> f b -> f [b]
unlessNull xs act = if null xs then pure [] else (: []) <$> act

-- | Run @act@. On exception, log a warning under @label@ and continue.
tryWarn :: (MonadUnliftIO m) => LogConfig -> Text -> m a -> m ()
tryWarn logCfg label act = tryWarnWith logCfg label () (void act)

-- | 'tryWarn' keeping @act@'s result, falling back to @fallback@ when it threw.
tryWarnWith :: (MonadUnliftIO m) => LogConfig -> Text -> a -> m a -> m a
tryWarnWith logCfg label fallback act =
  tryAny act >>= either (\e -> fallback <$ warnEx logCfg label e) pure

-- | The pool's log context narrowed to one job.
jobLog :: WorkerConfig m payload -> Job.JobRead payload -> LogConfig
jobLog config = withJobContextOne (logConfig config)

-- | The pool's log context narrowed to a set of jobs.
jobsLog :: WorkerConfig m payload -> [Job.JobRead payload] -> LogConfig
jobsLog config = withJobContextList (logConfig config)

-- | The pool's log context narrowed to a claimed batch.
batchLog :: WorkerConfig m payload -> NonEmpty (Job.JobRead payload) -> LogConfig
batchLog config = withJobContext (logConfig config)

-- | Why a job a worker no longer holds is reported unavailable.
unownedReason :: Text
unownedReason = "no longer claimed by this worker"

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

-- | Ack a job, throwing if another worker reclaimed it mid-flight.
ackOrGone :: (JobOperation m payload) => Job.JobRead payload -> m ()
ackOrGone job = do
  rowsAffected <- Arb.ackJob job
  when (rowsAffected == 0) $
    throwJobGoneIds "reclaimed by another worker during processing" [Job.primaryKey job]

-- | Report a completed job, once the handoff already records it.
reportSuccess
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> UTCTime
  -> Job.JobRead payload
  -> m ()
reportSuccess config startTime j = do
  endT <- liftIO getCurrentTime
  runHook (jobLog config j) "onJobSuccess" $ Job.onJobSuccess (observabilityHooks config) j startTime endT

-- | The settle operations a batch handler drives its jobs through.
batchCallbacks
  :: forall payload m
   . ( EncodeJobResult (ResultOf m payload)
     , JobOperation m payload
     )
  => WorkerConfig m payload
  -> CancelHandoff
  -> NonEmpty (Job.JobRead payload)
  -> UTCTime
  -> Text
  -> BatchCallbacks m payload (ResultOf m payload)
batchCallbacks config handoff jobs startTime schemaName =
  BatchCallbacks
    { ack = (`ackOneStoring` Nothing)
    , ackWith = \job r -> ackOneStoring job (encodeJobResult r)
    , ackAll = \js -> ackBatchStoring (map (\j -> (j, Nothing)) js)
    , ackAllWith = \prs -> ackBatchStoring (map (second encodeJobResult) prs)
    , failRetry = failAs (Retryable . JobRetryableException)
    , failPermanent = failAs (Permanent . JobPermanentException)
    , cancelBranch = failAs (BranchCancel . BranchCancelException)
    , cancelTree = failAs (TreeCancel . TreeCancelException)
    , nack = nackOne
    }
  where
    shape = batchSpanShape jobs
    nackOne j = releaseJobs config handoff [j]
    failAs mkExc j msg = failWith j (toException (mkExc msg))
    failWith j exc = do
      endT <- liftIO getCurrentTime
      settle
        handoff
        (finalized [j])
        ( withDbTransaction $
            handleJobFailure config Ops.TakeLocks shape (classifyException exc) startTime endT j
        )
        $ \outcome -> do
          reportWritten outcome
          settleUnwritten config handoff [(j, outcome)]
    ackOneStoring j mVal =
      settle
        handoff
        (finalized [j])
        ( withDbTransaction $ do
            ackOrGone j
            storeEncodedResult schemaName j mVal
        )
        (const (reportSuccess config startTime j))
    ackBatchStoring pairs =
      let js = map fst pairs
       in settleBy
            handoff
            ( withDbTransaction $ do
                acked <- Set.fromList <$> Arb.ackJobsBatch js
                storeEncodedResults schemaName (filter (hasIdIn acked . fst) pairs)
                pure (partition (hasIdIn acked) js)
            )
            (\(done, reclaimed) -> finalized done <> disowned reclaimed)
            $ \(done, reclaimed) -> do
              traverse_ (reportSuccess config startTime) done
              let reclaimedLog = jobsLog config reclaimed
              unless (null reclaimed) $ do
                tryLog reclaimedLog Info "Jobs no longer claimed by this worker during bulk completion, skipped"
                void $ settleGoneJobs config handoff unownedReason reclaimed

-- | Delete the jobs a force-cancel flagged, report them, and hand back the attempt
-- the claim consumed for the batch siblings it interrupted.
finalizeForceCancelled
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> NonEmpty (Job.JobRead payload)
  -> [Int64]
  -- ^ The jobs the cancel named.
  -> [Int64]
  -- ^ Jobs the same signal found gone, which report rather than take a nack.
  -> CancelHandoff
  -> m ()
finalizeForceCancelled config jobs cancelledIds goneIds handoff = do
  tryLog (batchLog config jobs) Info "Job(s) force-cancelled"
  schemaName <- getSchema
  pending <- pendingJobs handoff jobs
  -- A nack keeps the claim, so the cancel can name a job the handler already finalized.
  let settling = byIdDesc (hasIdIn (Set.fromList (cancelledIds <> map Job.primaryKey pending))) jobs
      goneSet = Set.fromList goneIds
      deletable = [Job.primaryKey j | j <- settling, not (hasIdIn goneSet j)]
  deleted <-
    deleteCancelledOrWarn (batchLog config jobs) (workerId config) schemaName (Job.queueName firstJob) deletable
  (fresh, cancelled) <- recordCancelled handoff (deleted <> Set.fromList cancelledIds)
  let (gone, interrupted) = partition (hasIdIn cancelled) settling
      (unreported, alreadyReported) = partition (hasIdIn fresh) gone
      (unavailable, siblings) = partition (hasIdIn goneSet) interrupted
      unavailableLog = jobsLog config unavailable
  reportGoneJobs config handoff cancelled "force-cancelled" unreported
  record handoff (finalized alreadyReported)
  unless (null unavailable) $ do
    tryLog unavailableLog Info "Job(s) no longer claimed by this worker, skipping retry"
    reportGoneJobs config handoff mempty unownedReason unavailable
  releaseOrWarn config handoff "Releasing a force-cancel batch sibling failed" siblings
  where
    (firstJob :| _) = jobs

-- | Hand back the attempt the claim consumed for the jobs left unfinalized, in one
-- statement, and report whichever of them the nack found under another claim.
releaseJobs
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> [Job.JobRead payload]
  -> m ()
releaseJobs _ _ [] = pure ()
releaseJobs config handoff js =
  settle
    handoff
    (finalized js)
    (Set.fromList <$> Arb.nackJobsBatch js)
    $ \released ->
      settleUnwritten config handoff [(j, Left unownedReason) | j <- js, not (hasIdIn released j)]

-- | 'releaseJobs' on an unwinding path, which has nothing left to throw to.
releaseOrWarn
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> Text
  -> [Job.JobRead payload]
  -> m ()
releaseOrWarn config handoff warning js =
  tryWarn (jobsLog config js) warning (releaseJobs config handoff js)

-- | Settle the jobs a failure could not be written for, each under its own reason.
settleUnwritten
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> [(Job.JobRead payload, FailureOutcome m)]
  -> m ()
settleUnwritten config handoff pairs =
  traverse_ settleOne (Map.toList (Map.fromListWith (<>) [(reason, [j]) | (j, Left reason) <- pairs]))
  where
    settleOne (reason, js) = do
      cancelled <- settleGoneJobs config handoff reason js
      let (forced, unavailable) = partition (hasIdIn cancelled) js
      unless (null forced) $ tryLog (jobsLog config forced) Info "Job(s) force-cancelled"
      unless (null unavailable) $ tryLog (jobsLog config unavailable) Warning ("Job(s) " <> reason)

-- | Delete whichever of @jobs@ a force-cancel flagged, report them all, return those ids.
-- The delete is recorded, so a cancel signal arriving later does not report them again.
settleGoneJobs
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> Text
  -> [Job.JobRead payload]
  -> m (Set.Set Int64)
settleGoneJobs config handoff reason = \case
  [] -> pure mempty
  jobs@(j : _) -> do
    schemaName <- getSchema
    let logCfg = jobsLog config jobs
    cancelled <- deleteCancelledOrWarn logCfg (workerId config) schemaName (Job.queueName j) (map Job.primaryKey jobs)
    void $ recordCancelled handoff cancelled
    cancelled <$ reportGoneJobs config handoff cancelled reason jobs

-- | Delete whichever of @jobIds@ a force-cancel flagged against this worker's lease,
-- or against none, returning the ids it deleted. One held elsewhere is that worker's.
deleteCancelledOrWarn
  :: (MonadArbiter m)
  => LogConfig
  -> UUID
  -> SchemaName
  -> Text
  -- ^ Queue the jobs belong to.
  -> [Int64]
  -> m (Set.Set Int64)
deleteCancelledOrWarn logCfg owner schemaName queue jobIds =
  tryWarnWith logCfg "Deleting force-cancelled jobs failed" mempty $
    Set.fromList <$> Ops.deleteCancelledJobs schemaName queue (Just owner) jobIds

-- | Interpret a finished batch: warn when the handler left jobs unfinalized,
-- skip retry for gone or nacked jobs, otherwise fail whatever was not finalized.
reportBatchOutcome
  :: forall payload m
   . (JobOperation m payload)
  => WorkerConfig m payload
  -> UTCTime
  -> UTCTime
  -> NonEmpty (Job.JobRead payload)
  -> CancelHandoff
  -> Either SomeException ()
  -> m ()
reportBatchOutcome config startTime endTime jobs handoff outcome = do
  unhandled <- pendingJobs handoff jobs
  let splitNamed ids = partition (hasIdIn (Set.fromList ids)) unhandled
      reportUnavailable ids reason = do
        -- An exception naming no job speaks for none of them, so the remainder keeps
        -- its attempt and reports when it runs.
        let (jobsGone, siblings) = splitNamed ids
        void $ settleGoneJobs config handoff reason jobsGone
        unless (null ids) $
          releaseOrWarn config handoff "Releasing an interrupted batch sibling failed" siblings
      unownedOf = unownedJobs handoff jobs
  case outcome of
    Right () ->
      unless (null unhandled) $
        tryLog (jobsLog config unhandled) Warning "Handler left jobs unfinalized, will reprocess"
    Left exc
      | Just (JobGoneException reason gone) <- fromException exc -> do
          tryLog (batchLog config jobs) Info $ "Job(s) " <> reason <> ", skipping retry" <> namedJobIds gone
          reportUnavailable gone reason
      | Just JobNackException <- fromException exc -> do
          -- Hand back the attempt the claim consumed for every job the handler
          -- left unfinalized, so the nacked reprocess does not record a failure.
          releaseOrWarn config handoff "Handing back a nacked batch's attempt failed" unhandled
          tryLog (batchLog config jobs) Info "Job(s) nacked, will be reprocessed"
      | otherwise -> do
          -- Fail the jobs the handler did not finalize, in a separate transaction.
          let failure@(_, kind) = classifyException exc
              queue = Job.queueName firstJob
          schemaName <- getSchema
          -- A tree or branch cancel acts on the tree, not on this worker's claim.
          unowned <- if cancelsTree kind then unownedOf else pure []
          -- Every row the settles below touch, in one pass. A cancel reaches the whole
          -- tree, so its pass has to as well.
          let lockTrees
                | cancelsTree kind = Ops.lockJobTreesFromRoot
                | otherwise = Ops.lockJobTrees
          settle
            handoff
            (finalized unhandled)
            ( withDbTransaction $ do
                Ops.lockJobParents schemaName queue (map Job.parentId unhandled)
                lockTrees schemaName queue (map Job.primaryKey (unhandled <> unowned))
                outcomes <- traverse (\j -> (j,) <$> failJob failure j) unhandled
                traverse_ (void . cancelJobFor kind) unowned
                pure outcomes
            )
            $ \outcomes -> do
              traverse_ report outcomes
              settleUnwritten config handoff outcomes
  where
    (firstJob :| _) = jobs
    shape = batchSpanShape jobs
    report (job, written) =
      tryWarn (jobLog config job) "Reporting a job's failure failed" (reportWritten written)
    failJob failure job =
      handleJobFailure config Ops.LocksHeld shape failure startTime endTime job

-- | Store a job's result for its parent rollup, if it has one.
storeJobResult
  :: (EncodeJobResult result, MonadArbiter m)
  => Text
  -> Job.JobRead payload
  -> result
  -> m ()
storeJobResult schemaName job = storeEncodedResult schemaName job . encodeJobResult

-- | 'storeJobResult' on an already-encoded result. 'Nothing' stores nothing.
storeEncodedResult
  :: (MonadArbiter m)
  => Text
  -> Job.JobRead payload
  -> Maybe Value
  -> m ()
storeEncodedResult schemaName job mVal =
  case (Job.parentId job, mVal) of
    (Just pid, Just val) ->
      void $ Ops.insertResult schemaName (Job.queueName job) pid (Job.primaryKey job) val
    (Nothing, Just val)
      | Ops.archivesOnAck job ->
          void $ Ops.updateArchiveResult schemaName (Job.queueName job) (Job.primaryKey job) val
    _ -> pure ()

-- | 'storeEncodedResult' over a batch from one queue: one statement for the
-- child results, one for the archived roots.
storeEncodedResults
  :: (MonadArbiter m)
  => Text
  -> [(Job.JobRead payload, Maybe Value)]
  -> m ()
storeEncodedResults _ [] = pure ()
storeEncodedResults schemaName pairs@((firstJob, _) : _) = do
  let (childRows, rootRows) = partitionEithers (mapMaybe resultRow pairs)
      queue = Job.queueName firstJob
  void $ Ops.insertResultsBatch schemaName queue childRows
  void $ Ops.updateArchiveResultsBatch schemaName queue rootRows
  where
    resultRow (job, mVal) = do
      val <- mVal
      case Job.parentId job of
        Just pid -> Just (Left (pid, Job.primaryKey job, val))
        Nothing
          | Ops.archivesOnAck job -> Just (Right (Job.primaryKey job, val))
          | otherwise -> Nothing

data FailureKind = RetryFailure | PermanentFailure | TreeCancelFailure | BranchCancelFailure
  deriving stock (Eq)

-- | The job's own attempt budget, or the default.
jobMaxAtts :: Job.JobRead payload -> Int32
jobMaxAtts job = fromMaybe Job.defaultMaxAttempts (Job.maxAttempts job)

-- | Classify a handler exception into an error message and failure disposition.
-- 'reportBatchOutcome' intercepts 'JobGoneException' first, so it never arrives here.
classifyException :: SomeException -> (T.Text, FailureKind)
classifyException e
  | Just (Retryable (JobRetryableException msg)) <- fromException e = (msg, RetryFailure)
  | Just (Permanent (JobPermanentException msg)) <- fromException e = (msg, PermanentFailure)
  | Just (TreeCancel (TreeCancelException msg)) <- fromException e = (msg, TreeCancelFailure)
  | Just (BranchCancel (BranchCancelException msg)) <- fromException e = (msg, BranchCancelFailure)
  | Just (ParsingException msg) <- fromException e = (msg, PermanentFailure)
  | otherwise = (T.pack $ show e, RetryFailure) -- Unknown exception, treat as retryable

-- | Whether a failure deletes a job tree rather than acting on the job's own claim.
cancelsTree :: FailureKind -> Bool
cancelsTree kind = kind `elem` [TreeCancelFailure, BranchCancelFailure]

-- | Report jobs this worker can no longer act on, as cancelled where a force-cancel
-- deleted them, recording each against the handoff.
reportGoneJobs
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> Set.Set Int64
  -- ^ Ids a force-cancel accounted for.
  -> Text
  -> [Job.JobRead payload]
  -> m ()
reportGoneJobs config handoff cancelled reason js =
  settleInterruptibly handoff (finalized js) (pure ()) (const (traverse_ report js))
  where
    report job
      | hasIdIn cancelled job = fireCancelled config job "force-cancelled"
      | otherwise =
          runHook (jobLog config job) "onJobUnavailable" $
            Job.onJobUnavailable (observabilityHooks config) job reason

-- | What this pool's consumer spans cover.
poolSpanShape :: WorkerConfig m payload -> ConsumeShape
poolSpanShape = bool PerJob PerBatch . (> 1) . handlerBatchSize

-- | What the consumer span over this batch covers, which a pool claiming fewer jobs
-- than it asked for narrows to the one job in hand.
batchSpanShape :: NonEmpty (Job.JobRead payload) -> ConsumeShape
batchSpanShape = bool PerJob PerBatch . (> 1) . length

-- | Report a failed job to its hooks and to the consumer span it ran under. A span
-- covering more than this job also covers the ones that succeeded, so only a
-- single-job span takes the error status.
fireFailure
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> ConsumeShape
  -- ^ What the span this job ran under covers.
  -> Job.JobRead payload
  -> Text
  -> UTCTime
  -> UTCTime
  -> m ()
fireFailure config shape job errorMsg startTime endTime = do
  recordJobFailure job errorMsg
  when (shape == PerJob) (markSpanError errorMsg)
  runHook (jobLog config job) "onJobFailure" $
    Job.onJobFailure (observabilityHooks config) job errorMsg startTime endTime

-- | Report a cancelled job.
fireCancelled
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> Job.JobRead payload
  -> Text
  -> m ()
fireCancelled config job errorMsg = do
  recordJobCancelled job errorMsg
  runHook (jobLog config job) "onJobCancelled" $
    Job.onJobCancelled (observabilityHooks config) job errorMsg

-- | Delete what a tree or branch cancel names, returning the rows deleted.
cancelJobFor
  :: (JobOperation m payload)
  => FailureKind
  -> Job.JobRead payload
  -> m Int64
cancelJobFor kind job = do
  schemaName <- getSchema
  case kind of
    BranchCancelFailure ->
      Ops.cancelJobCascade schemaName (Job.queueName job) (fromMaybe (Job.primaryKey job) (Job.parentId job))
    _ -> Ops.cancelJobTree schemaName (Job.queueName job) (Job.primaryKey job)

-- | 'Left' why a failure write found no row, 'Right' how to report the one it wrote.
type FailureOutcome m = Either Text (m ())

-- | Report a failure the write landed.
reportWritten :: (Monad m) => FailureOutcome m -> m ()
reportWritten = fromRight (pure ())

-- | Handle failure for a single job (retry or move to DLQ), for the caller to report
-- once it commits.
handleJobFailure
  :: forall payload m
   . (JobOperation m payload)
  => WorkerConfig m payload
  -> Ops.TreeLocks
  -- ^ Whether the caller already holds the parent and tree locks.
  -> ConsumeShape
  -- ^ What the span this job ran under covers.
  -> (Text, FailureKind)
  -- ^ The handler exception, classified once for the whole batch.
  -> UTCTime
  -> UTCTime
  -> Job.JobRead payload
  -> m (FailureOutcome m)
handleJobFailure config locks shape (errorMsg, failureKind) startTime endTime job
  -- A batch sibling's cancel takes out the whole tree, so no rows still means gone.
  | cancelsTree failureKind =
      Right (fireCancelled config job errorMsg) <$ cancelJobFor failureKind job
  | failureKind == PermanentFailure || Job.attempts job >= jobMaxAtts job = do
      schemaName <- getSchema
      wrote
        "no longer available for the dead-letter queue"
        (runHook cfg "onJobFailedAndMovedToDLQ" $ Job.onJobFailedAndMovedToDLQ hooks errorMsg job)
        <$> Ops.moveToDLQ locks schemaName (Job.queueName job) errorMsg job
  | otherwise = do
      let baseDelay = calculateBackoff (backoffStrategy config) (Job.attempts job)
      backoffSecs <- liftIO $ applyJitter (jitter config) baseDelay
      wrote
        "no longer available for retry"
        (runHook cfg "onJobRetry" $ Job.onJobRetry hooks job backoffSecs)
        <$> Arb.updateJobForRetry backoffSecs errorMsg job
  where
    cfg = jobLog config job
    hooks = observabilityHooks config
    -- Nothing written means the job went elsewhere, so the failure is not ours to report.
    wrote reason after rowsAffected
      | rowsAffected == 0 = Left reason
      | otherwise = Right (fireFailure config shape job errorMsg startTime endTime >> after)

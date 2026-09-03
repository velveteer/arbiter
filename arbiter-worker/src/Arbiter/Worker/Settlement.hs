{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Job completion, failure, cancellation, and retry settlement.
module Arbiter.Worker.Settlement
  ( ackOrGone
  , reportSuccess
  , batchCallbacks
  , finalizeForceCancelled
  , reportBatchOutcome
  , jobLog
  , batchLog
  ) where

import Arbiter.Core.Exceptions
  ( BranchCancelException (..)
  , JobDeadlineExceeded (..)
  , JobException (..)
  , JobGoneException (..)
  , JobNackException (..)
  , JobPermanentException (..)
  , JobRetryableException (..)
  , ParsingException (..)
  , TreeCancelException (..)
  , namedJobIds
  , throwJobGoneIds
  )
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobResult
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.Trace
  ( ConsumeShape (..)
  , markSpanError
  , recordJobCancelled
  , recordJobFailure
  )
import Control.Exception (SomeException, fromException, toException)
import Control.Monad (unless, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Bifunctor (second)
import Data.Bool (bool)
import Data.Either (fromRight)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.List (partition)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime, getCurrentTime)
import Data.UUID (UUID)

import Arbiter.Worker.BackoffStrategy
import Arbiter.Worker.Config
import Arbiter.Worker.Logger
import Arbiter.Worker.Logger.Internal
  ( runHook
  , tryWarn
  , tryWarnWith
  , withJobContext
  , withJobContextList
  , withJobContextOne
  )
import Arbiter.Worker.Results (storeEncodedResult, storeEncodedResults)
import Arbiter.Worker.Settle
  ( CancelHandoff
  , byIdDesc
  , disowned
  , finalized
  , hasIdIn
  , pendingJobs
  , record
  , recordCancelled
  , settle
  , settleBy
  , settleInterruptibly
  , unownedJobs
  )

-- | Add one job to the pool log context.
jobLog :: WorkerConfig m payload -> Job.JobRead payload -> LogConfig
jobLog config = withJobContextOne (logConfig config)

-- | Add jobs to the pool log context.
jobsLog :: WorkerConfig m payload -> [Job.JobRead payload] -> LogConfig
jobsLog config = withJobContextList (logConfig config)

-- | Add a claimed batch to the pool log context.
batchLog :: WorkerConfig m payload -> NonEmpty (Job.JobRead payload) -> LogConfig
batchLog config = withJobContext (logConfig config)

unownedReason :: Text
unownedReason = "no longer claimed by this worker"

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
reportSuccess config startTime job = do
  endT <- liftIO getCurrentTime
  runHook (jobLog config job) "onJobSuccess" $ Job.onJobSuccess (observabilityHooks config) job startTime endT

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
    , ackWith = \job result -> ackOneStoring job (encodeJobResult result)
    , ackAll = \toAck -> ackBatchStoring (map (\job -> (job, Nothing)) toAck)
    , ackAllWith = \resultPairs -> ackBatchStoring (map (second encodeJobResult) resultPairs)
    , failRetry = failAs (Retryable . JobRetryableException)
    , failPermanent = failAs (Permanent . JobPermanentException)
    , cancelBranch = failAs (BranchCancel . BranchCancelException)
    , cancelTree = failAs (TreeCancel . TreeCancelException)
    , nack = nackOne
    }
  where
    shape = batchSpanShape jobs
    nackOne job = releaseJobs config handoff [job]
    failAs mkExc job msg = failWith job (toException (mkExc msg))
    failWith job exc = do
      endT <- liftIO getCurrentTime
      settle
        handoff
        (finalized [job])
        ( withDbTransaction $
            handleJobFailure config Ops.TakeLocks shape (classifyException exc) startTime endT job
        )
        $ \outcome -> do
          reportWritten outcome
          settleUnwritten config handoff [(job, outcome)]
    ackOneStoring job mVal =
      settle
        handoff
        (finalized [job])
        ( withDbTransaction $ do
            ackOrGone job
            storeEncodedResult schemaName job mVal
        )
        (const (reportSuccess config startTime job))
    ackBatchStoring pairs =
      let jobsToAck = map fst pairs
       in settleBy
            handoff
            ( withDbTransaction $ do
                acked <- Set.fromList <$> Arb.ackJobsBatch jobsToAck
                storeEncodedResults schemaName (filter (hasIdIn acked . fst) pairs)
                pure (partition (hasIdIn acked) jobsToAck)
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
  -- ^ Jobs that the same signal found unavailable. Report these jobs without a nack.
  -> CancelHandoff
  -> m ()
finalizeForceCancelled config jobs cancelledIds goneIds handoff = do
  tryLog (batchLog config jobs) Info "Job(s) force-cancelled"
  schemaName <- getSchema
  pending <- pendingJobs handoff jobs
  -- The cancel can name a job the handler finalized after the cancel took effect.
  let settling = byIdDesc (hasIdIn (Set.fromList (cancelledIds <> map Job.primaryKey pending))) jobs
      goneSet = Set.fromList goneIds
      deletable = [Job.primaryKey job | job <- settling, not (hasIdIn goneSet job)]
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
releaseJobs config handoff jobs =
  settle
    handoff
    (finalized jobs)
    (Set.fromList <$> Arb.nackJobsBatch jobs)
    $ \released ->
      settleUnwritten config handoff [(job, Left unownedReason) | job <- jobs, not (hasIdIn released job)]

-- | 'releaseJobs' on an unwinding path. A failure is logged as a warning.
releaseOrWarn
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> Text
  -> [Job.JobRead payload]
  -> m ()
releaseOrWarn config handoff warning jobs =
  tryWarn (jobsLog config jobs) warning (releaseJobs config handoff jobs)

-- | Settle the jobs a failure could not be written for, each under its own reason.
settleUnwritten
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> [(Job.JobRead payload, FailureOutcome m)]
  -> m ()
settleUnwritten config handoff pairs =
  traverse_ settleOne (Map.toList (Map.fromListWith (<>) [(reason, [job]) | (job, Left reason) <- pairs]))
  where
    settleOne (reason, jobs) = do
      cancelled <- settleGoneJobs config handoff reason jobs
      let (forced, unavailable) = partition (hasIdIn cancelled) jobs
      unless (null forced) $ tryLog (jobsLog config forced) Info "Job(s) force-cancelled"
      unless (null unavailable) $ tryLog (jobsLog config unavailable) Warning ("Job(s) " <> reason)

-- | Delete whichever of @jobs@ a force-cancel flagged, report them all, return those ids.
-- The delete is recorded in the handoff.
settleGoneJobs
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> Text
  -> [Job.JobRead payload]
  -> m (Set.Set Int64)
settleGoneJobs config handoff reason = \case
  [] -> pure mempty
  jobs@(firstJob : _) -> do
    schemaName <- getSchema
    let logCfg = jobsLog config jobs
    cancelled <-
      deleteCancelledOrWarn logCfg (workerId config) schemaName (Job.queueName firstJob) (map Job.primaryKey jobs)
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

-- | Interpret a finished batch. Warn when the handler left jobs unfinalized. Skip
-- retry for gone or nacked jobs. Fail the rest.
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
        -- An exception naming no job speaks for none of them. The remainder keeps
        -- its attempt.
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
          -- left unfinalized.
          releaseOrWarn config handoff "Handing back a nacked batch's attempt failed" unhandled
          tryLog (batchLog config jobs) Info "Job(s) nacked, will be reprocessed"
      | otherwise -> do
          -- Fail the jobs the handler did not finalize, in a separate transaction.
          let failure@(reason, kind) = classifyException exc
              queue = Job.queueName firstJob
          when (null unhandled) $
            tryLog (batchLog config jobs) Warning ("Handler stopped with its jobs already finalized: " <> reason)
          schemaName <- getSchema
          -- A tree or branch cancel acts on the whole tree.
          unowned <- if cancelsTree kind then unownedOf else pure []
          -- Lock every row the settles below touch, in one pass. A cancel's pass
          -- covers the whole tree.
          let lockTrees
                | cancelsTree kind = Ops.lockJobTreesFromRoot
                | otherwise = Ops.lockJobTrees
          settle
            handoff
            (finalized unhandled)
            ( withDbTransaction $ do
                Ops.lockJobParents schemaName queue (map Job.parentId unhandled)
                lockTrees schemaName queue (map Job.primaryKey (unhandled <> unowned))
                outcomes <- traverse (\job -> (job,) <$> failJob failure job) unhandled
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

data FailureKind = RetryFailure | PermanentFailure | TreeCancelFailure | BranchCancelFailure
  deriving stock (Eq)

-- | The job's own attempt budget, or the default.
jobMaxAtts :: Job.JobRead payload -> Int32
jobMaxAtts job = fromMaybe Job.defaultMaxAttempts (Job.maxAttempts job)

-- | Classify a handler exception into an error message and failure disposition.
-- 'reportBatchOutcome' intercepts 'JobGoneException' before this.
classifyException :: SomeException -> (T.Text, FailureKind)
classifyException exception
  | Just (Retryable (JobRetryableException msg)) <- fromException exception = (msg, RetryFailure)
  | Just (Permanent (JobPermanentException msg)) <- fromException exception = (msg, PermanentFailure)
  | Just (TreeCancel (TreeCancelException msg)) <- fromException exception = (msg, TreeCancelFailure)
  | Just (BranchCancel (BranchCancelException msg)) <- fromException exception = (msg, BranchCancelFailure)
  | Just (ParsingException msg) <- fromException exception = (msg, PermanentFailure)
  | Just (JobDeadlineExceeded msg) <- fromException exception = (msg, RetryFailure)
  | otherwise = (T.pack $ show exception, RetryFailure) -- Unknown exception, treat as retryable

-- | Whether a failure deletes a job tree or updates the job claim.
cancelsTree :: FailureKind -> Bool
cancelsTree kind = kind `elem` [TreeCancelFailure, BranchCancelFailure]

-- | Report jobs this worker can no longer act on. A job a force-cancel deleted is
-- reported as cancelled. Each is recorded against the handoff.
reportGoneJobs
  :: (JobOperation m payload)
  => WorkerConfig m payload
  -> CancelHandoff
  -> Set.Set Int64
  -- ^ Ids a force-cancel accounted for.
  -> Text
  -> [Job.JobRead payload]
  -> m ()
reportGoneJobs config handoff cancelled reason jobs =
  settleInterruptibly handoff (finalized jobs) (pure ()) (const (traverse_ report jobs))
  where
    report job
      | hasIdIn cancelled job = fireCancelled config job "force-cancelled"
      | otherwise =
          runHook (jobLog config job) "onJobUnavailable" $
            Job.onJobUnavailable (observabilityHooks config) job reason

-- | What the consumer span over this batch covers. A batch of one narrows it to
-- that job.
batchSpanShape :: NonEmpty (Job.JobRead payload) -> ConsumeShape
batchSpanShape = bool PerJob PerBatch . (> 1) . length

-- | Report a failed job to its hooks and to the consumer span it ran under. Only a
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
  -- A batch sibling's cancel takes out the whole tree. Zero rows still means gone.
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
    -- Nothing written means the job went elsewhere.
    wrote reason after rowsAffected
      | rowsAffected == 0 = Left reason
      | otherwise = Right (fireFailure config shape job errorMsg startTime endTime >> after)

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | The pool's side of a batch's lifecycle: the statements and hooks
-- "Arbiter.Worker.Batch" drives, unlifted to IO.
module Arbiter.Worker.Settlement
  ( PoolEffects
  , PoolMode
  , poolEffects
  , poolMode
  , batchLog
  ) where

import Arbiter.Core.Exceptions (throwJobGoneIds)
import Arbiter.Core.HighLevel (JobOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types qualified as Job
import Arbiter.Core.JobResult
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.Trace
  ( ConsumeShape (..)
  , ConsumeSpan
  , capturingContextIO
  , markSpanError
  , recordJobCancelled
  , recordJobFailure
  , resolveTracer
  , toConsumeShape
  , withConsumeSpan
  )
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (Value)
import Data.Bifunctor (second)
import Data.Foldable (traverse_)
import Data.Int (Int32, Int64)
import Data.List (partition)
import Data.List.NonEmpty (NonEmpty (..))
import Data.Maybe (fromMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Time (UTCTime)
import UnliftIO (MonadUnliftIO, UnliftIO (..), askUnliftIO)

import Arbiter.Worker.BackoffStrategy
import Arbiter.Worker.Batch
import Arbiter.Worker.Config
import Arbiter.Worker.Heartbeat.Guard (reclaimedReason)
import Arbiter.Worker.Logger
import Arbiter.Worker.Logger.Internal (jobHook, poolLog, withJobContext)
import Arbiter.Worker.Results (storeEncodedResult, storeEncodedResults, storeJobResult)

-- | Add a claimed batch to the pool log context.
batchLog :: WorkerConfig m payload -> NonEmpty (Job.JobRead payload) -> LogConfig
batchLog config = withJobContext (logConfig config)

-- | The effects of one batch, each run in its own transaction or hook. A
-- statement runs in the handler's own context at a callback, so it joins a
-- transaction the handler holds, and in the pool's otherwise. Built once for the
-- pool. A batch binds its jobs.
type PoolEffects m payload = NonEmpty (Job.JobRead payload) -> Effects IO (UnliftIO m) (Job.JobRead payload) Value

poolEffects
  :: forall payload m
   . (JobOperation m payload)
  => WorkerConfig m payload
  -> Ops.JobStatements
  -> ConsumeSpan
  -> m (PoolEffects m payload)
poolEffects config statements consumeSpan = do
  schemaName <- getSchema
  unlift@(UnliftIO run) <- askUnliftIO
  pure $ \jobs@(firstJob :| _) ->
    let queue = Job.queueName firstJob
     in Effects
          { effectAmbient = unlift
          , effectSpan = \body -> run $ do
              tracer <- resolveTracer
              withConsumeSpan tracer consumeSpan jobs (capturingContextIO >>= liftIO . body)
          , effectAck = \(UnliftIO runIn) job stored ->
              runIn $ withDbTransaction $ do
                ackOrGone statements job
                storeEncodedResult schemaName job stored
          , effectAckAll = \(UnliftIO runIn) pairs ->
              runIn $ withDbTransaction $ do
                let jobsToAck = map fst pairs
                acked <- Set.fromList <$> Ops.ackJobsBatchWith (Ops.statementsAck statements) jobsToAck
                let ackedJob = (`Set.member` acked) . Job.primaryKey
                storeEncodedResults schemaName (filter (ackedJob . fst) pairs)
                pure (partition ackedJob jobsToAck)
          , effectFail = \(UnliftIO runIn) failure job ->
              runIn $ withDbTransaction $ handleJobFailure config Ops.TakeLocks failure job
          , effectFailAll = \(UnliftIO runIn) failure@(_, kind) unhandled unowned ->
              runIn $ withDbTransaction $ do
                -- Lock every row the failures below touch, in one pass. A cancel's
                -- pass covers the whole tree.
                let lockTrees
                      | cancelsTree kind = Ops.lockJobTreesFromRoot
                      | otherwise = Ops.lockJobTrees
                Ops.lockJobParents schemaName queue (map Job.parentId unhandled)
                lockTrees schemaName queue (map Job.primaryKey (unhandled <> unowned))
                outcomes <-
                  traverse (\job -> (job,) <$> handleJobFailure config Ops.LocksHeld failure job) unhandled
                traverse_ (void . cancelJobFor kind) unowned
                pure outcomes
          , effectDeleteCancelled = \(UnliftIO runIn) gone ->
              runIn (Set.fromList <$> Ops.deleteCancelledJobs schemaName queue (Just (workerId config)) (map Job.primaryKey gone))
          , effectRelease = \(UnliftIO runIn) released -> runIn (Set.fromList <$> Arb.nackJobsBatch released)
          , effectReport = \(UnliftIO runIn) -> runIn . report (batchSpanShape jobs)
          , effectLog = poolLog (logConfig config)
          }
  where
    hooks = observabilityHooks config
    hook = jobHook (logConfig config)
    report shape = \case
      Claimed job startTime -> hook job "onJobClaimed" $ Job.onJobClaimed hooks job startTime
      Succeeded job startTime endTime -> hook job "onJobSuccess" $ Job.onJobSuccess hooks job startTime endTime
      Failed job errorMsg startTime endTime outcome -> do
        fireFailure config shape job errorMsg startTime endTime
        case outcome of
          Retrying delay -> hook job "onJobRetry" $ Job.onJobRetry hooks job delay
          _ -> hook job "onJobFailedAndMovedToDLQ" $ Job.onJobFailedAndMovedToDLQ hooks errorMsg job
      Cancelled job reason -> fireCancelled config job reason
      Unavailable job reason -> hook job "onJobUnavailable" $ Job.onJobUnavailable hooks job reason

-- | How a pool runs a batch.
type PoolMode m payload = Mode IO (UnliftIO m) (Job.JobRead payload) Value

-- | How the pool runs a batch's handler, unlifted to IO.
poolMode
  :: forall payload m
   . ( EncodeJobResult (ResultOf m payload)
     , JobOperation m payload
     )
  => WorkerConfig m payload
  -> Ops.JobStatements
  -> m (PoolMode m payload)
poolMode config statements = do
  schemaName <- getSchema
  UnliftIO run <- askUnliftIO
  pure $ case handlerMode config of
    SingleJobMode handler ->
      SingleMode $ \job ->
        run $ withDbTransaction $ do
          handlerResult <- runHandlerWithConnection handler job
          ackOrGone statements job
          storeJobResult schemaName job handlerResult
    BatchedJobsMode _ handler ->
      BatchedMode $ \jobs callbacks -> run (handler jobs (batchCallbacks callbacks))

-- | The handler's callbacks over the batch's settle operations. Each captures the
-- handler's context at the call.
batchCallbacks
  :: (EncodeJobResult result, MonadUnliftIO m)
  => Callbacks IO (UnliftIO m) (Job.JobRead payload) Value -> BatchCallbacks m payload result
batchCallbacks callbacks =
  BatchCallbacks
    { ack = ackAs Nothing
    , ackWith = \job result -> ackAs (encodeJobResult result) job
    , ackAll = \jobs -> here (\ctx -> callbackAckAll callbacks ctx (map (,Nothing) jobs))
    , ackAllWith = \pairs -> here (\ctx -> callbackAckAll callbacks ctx (map (second encodeJobResult) pairs))
    , failRetry = failAs RetryFailure
    , failPermanent = failAs PermanentFailure
    , cancelBranch = failAs BranchCancelFailure
    , cancelTree = failAs TreeCancelFailure
    , nack = \job -> here (\ctx -> callbackNack callbacks ctx job)
    }
  where
    here act = askUnliftIO >>= liftIO . act
    ackAs stored job = here (\ctx -> callbackAck callbacks ctx job stored)
    failAs kind job msg = here (\ctx -> callbackFail callbacks ctx (msg, kind) job)

-- | Ack a job inside the caller's transaction, throwing if another worker reclaimed it mid-flight.
ackOrGone :: (JobOperation m payload) => Ops.JobStatements -> Job.JobRead payload -> m ()
ackOrGone statements job = do
  rowsAffected <- Ops.ackJobWith (Ops.statementsAck statements) job
  when (rowsAffected == 0) $
    throwJobGoneIds reclaimedReason [Job.primaryKey job]

-- | The job's own attempt budget, or the default.
jobMaxAtts :: Job.JobRead payload -> Int32
jobMaxAtts job = fromMaybe Job.defaultMaxAttempts (Job.maxAttempts job)

-- | What the consumer span over this batch covers. A batch of one narrows it to
-- that job.
batchSpanShape :: NonEmpty (Job.JobRead payload) -> ConsumeShape
batchSpanShape = toConsumeShape . length

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
  jobHook (logConfig config) job "onJobFailure" $
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
  jobHook (logConfig config) job "onJobCancelled" $
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

-- | Write a single job's failure (retry or move to DLQ), for the caller to report
-- once it commits. 'Left' why the write found no row.
handleJobFailure
  :: forall payload m
   . (JobOperation m payload)
  => WorkerConfig m payload
  -> Ops.TreeLocks
  -- ^ Whether the caller already holds the parent and tree locks.
  -> Failure
  -- ^ The handler exception, classified once for the whole batch.
  -> Job.JobRead payload
  -> m (Either Text Outcome)
handleJobFailure config locks (errorMsg, failureKind) job
  -- A batch sibling's cancel takes out the whole tree. Zero rows still means gone.
  | cancelsTree failureKind = Right TreeCancelled <$ cancelJobFor failureKind job
  | failureKind == PermanentFailure || Job.attempts job >= jobMaxAtts job = do
      schemaName <- getSchema
      wrote "no longer available for the dead-letter queue" DeadLettered
        <$> Ops.moveToDLQ locks schemaName (Job.queueName job) errorMsg job
  | otherwise = do
      let baseDelay = calculateBackoff (backoffStrategy config) (Job.attempts job)
      backoffSecs <- liftIO $ applyJitter (jitter config) baseDelay
      wrote "no longer available for retry" (Retrying backoffSecs)
        <$> Arb.updateJobForRetry backoffSecs errorMsg job
  where
    -- Nothing written means the job went elsewhere.
    wrote reason outcome rowsAffected
      | rowsAffected == 0 = Left reason
      | otherwise = Right outcome

{-# LANGUAGE OverloadedStrings #-}

-- | Cron schedule reconciliation and execution.
module Arbiter.Worker.Cron.Scheduler
  ( CronLog
  , newCronLog
  , runCronScheduler
  , initCronSchedules
  , processCronCatchUp
  , processRunRequests
  , enumerateCatchUpTicks
  , makeDedupKey
  , computeDelayMicros
  ) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Types (DedupKey (IgnoreDuplicate), setDedupKey)
import Arbiter.Core.MonadArbiter (MonadArbiter, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Control.Concurrent.STM (retry)
import Control.Monad (unless, void, when)
import Control.Monad.IO.Class (MonadIO)
import Data.Foldable (for_, traverse_)
import Data.Maybe (fromMaybe, isNothing)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime, addUTCTime, diffUTCTime, getCurrentTime)
import System.Cron (CronSchedule, parseCronSchedule)
import UnliftIO
  ( MonadUnliftIO
  , SomeException
  , TVar
  , atomically
  , liftIO
  , readTVar
  , readTVarIO
  , registerDelay
  , tryAny
  , writeTVar
  )

import Arbiter.Worker.Cron.Types
import Arbiter.Worker.Logger (FailureGates, LogConfig, LogLevel (..), newFailureGates, tryLog, tryReportedOn)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | Upsert default expression and overlap for each 'CronJob' into the
-- @cron_schedules@ table. Preserves any user overrides and enabled state.
initCronSchedules
  :: (MonadArbiter m)
  => SchemaName -> Text -> [CronJob payload] -> LogConfig -> m ()
initCronSchedules schemaName queueName jobs logCfg = do
  for_ jobs $ \cj ->
    Ops.upsertCronDefault
      schemaName
      (name cj)
      queueName
      (cronExpression cj)
      (overlapPolicyToText (overlap cj))
      (timezone cj)
  liftIO . tryLog logCfg Info $
    "Cron schedules initialized: " <> T.pack (show (length jobs)) <> " schedule(s) upserted"

-- | Scheduler entry point. Exits cleanly when the worker state becomes
-- 'ShuttingDown' so graceful shutdown stops creating new jobs.
runCronScheduler
  :: (QueueOperation m payload)
  => TVar WorkerState
  -> TVar Bool
  -- ^ Set by the run-now listener when a schedule this pool owns is requested.
  -> LogConfig
  -> SchemaName
  -> Text
  -- ^ Queue name (recorded on each schedule row).
  -> [CronJob payload]
  -> m ()
runCronScheduler stateVar runNowVar logCfg schemaName queueName jobs = do
  initCronSchedules schemaName queueName jobs logCfg
  cronLog <- newCronLog logCfg
  startupNow <- liftIO getCurrentTime
  shuttingDown <- isShuttingDown stateVar
  unless shuttingDown $ do
    processRunRequests cronLog schemaName jobs startupNow
    processCronCatchUp cronLog schemaName queueName jobs startupNow
  logCron cronLog Info $ "Cron scheduler started with " <> T.pack (show (length jobs)) <> " schedule(s)"
  loop cronLog
  where
    loop cronLog = do
      now <- liftIO getCurrentTime
      timerVar <- liftIO $ registerDelay (computeDelayMicros now)
      serve cronLog timerVar
    serve cronLog timerVar = do
      wake <- waitForWake stateVar runNowVar timerVar
      case wake of
        WakeShutdown -> pure ()
        WakeMinute -> do
          now <- liftIO getCurrentTime
          processRunRequests cronLog schemaName jobs now
          processCronCatchUp cronLog schemaName queueName jobs now
          loop cronLog
        WakeRunNow -> do
          now <- liftIO getCurrentTime
          processRunRequests cronLog schemaName jobs now
          serve cronLog timerVar

-- | Scheduler catch-up step. Each cron runs in its own transaction.
-- Backfill schedules hold a per-(schema, queue, name) advisory lock.
processCronCatchUp
  :: (QueueOperation m payload)
  => CronLog
  -> Text
  -> Text
  -- ^ Queue name
  -> [CronJob payload]
  -> UTCTime
  -- ^ Current wall-clock time
  -> m ()
processCronCatchUp cronLog schemaName queueName jobs now = do
  let currentTick = truncateToMinute now
  traverse_ (processOneCron currentTick) jobs
  where
    processOneCron currentTick cj = do
      outcome <- tryCron cronLog ("Cron '" <> name cj <> "' tick") . withDbTransaction $ do
        haveLeader <- case backfill cj of
          NoBackfill -> pure True
          Backfill _ -> Ops.tryAcquireCronLeader schemaName queueName (name cj)
        if not haveLeader
          then pure NotLeader
          else do
            mRow <- Ops.getCronScheduleByName schemaName (name cj)
            processOne mRow currentTick cj
            void $ Ops.touchCronChecked schemaName currentTick [name cj]
            pure Ran
      case outcome of
        Left _ -> pure ()
        Right NotLeader ->
          logCron cronLog Debug $ "Cron '" <> name cj <> "' skipped, another pool holds the lock"
        Right Ran -> pure ()
    processOne mRow currentTick cj = case resolveAndParse cj mRow of
      Disabled -> pure ()
      ParseError expr err ->
        logCron cronLog Error $
          "Cron schedule '"
            <> name cj
            <> "' has invalid effective expression '"
            <> expr
            <> "': "
            <> T.pack err
      InvalidTimezone tzName ->
        logCron cronLog Error $
          "Cron schedule '"
            <> name cj
            <> "' has unknown timezone '"
            <> tzName
            <> "'"
      Effective effectiveOv sched effectiveTz -> do
        let ticksInWindow = enumerateCatchUpTicks (backfill cj) (mRow >>= CS.lastCheckedAt) currentTick
            ticksToFire = pickTicksToFire sched effectiveTz effectiveOv ticksInWindow
            replayCount = length (filter (/= currentTick) ticksToFire)
        when (replayCount > 0)
          $ logCron cronLog Info
          $ "Replaying " <> T.pack (show replayCount) <> " missed tick(s) for '" <> name cj <> "'"
        for_ ticksToFire $ \t ->
          tryInsertCronJob cronLog schemaName cj effectiveOv effectiveTz (tickKindFor currentTick t) t

data TickOutcome = NotLeader | Ran

-- | 'Live' for @currentTick@, 'Replay' otherwise.
tickKindFor :: UTCTime -> UTCTime -> TickKind
tickKindFor currentTick t = if t == currentTick then Live else Replay

-- | 'SkipOverlap' keeps the oldest match so a catch-up handler starts from
-- the earliest unprocessed checkpoint. 'AllowOverlap' keeps all.
-- Match evaluation uses the supplied timezone ('Nothing' = UTC).
pickTicksToFire :: CronSchedule -> Maybe Text -> OverlapPolicy -> [UTCTime] -> [UTCTime]
pickTicksToFire sched tz ov ticks =
  let matching = filter (matchesInTimezone tz sched) ticks
   in case ov of
        SkipOverlap -> take 1 matching
        AllowOverlap -> matching

-- | Minutes to evaluate for a 'processCronCatchUp' call. Returns @[]@ when
-- the watermark is already at or past @currentTick@ (preventing re-fires
-- after job processing).
enumerateCatchUpTicks :: BackfillPolicy -> Maybe UTCTime -> UTCTime -> [UTCTime]
enumerateCatchUpTicks _ (Just lastChecked) currentTick
  | truncateToMinute lastChecked >= currentTick = []
enumerateCatchUpTicks NoBackfill _ currentTick = [currentTick]
enumerateCatchUpTicks _ Nothing currentTick = [currentTick]
enumerateCatchUpTicks (Backfill window) (Just lastChecked) currentTick =
  let truncatedLast = truncateToMinute lastChecked
      -- Truncate so a non-minute-multiple window doesn't stride past currentTick.
      windowFloor = truncateToMinute (addUTCTime (negate window) currentTick)
      startMinute = max (addUTCTime 60 truncatedLast) windowFloor
   in enumMinutes startMinute currentTick

-- | Result of resolving a schedule's effective config (code defaults +
-- runtime DB overrides).
data Resolved
  = Disabled
  | ParseError Text String
  | InvalidTimezone Text
  | Effective OverlapPolicy CronSchedule (Maybe Text)

-- | The row's overlap override, falling back to the schedule's code default.
effectiveOverlapFor :: CronJob payload -> CS.CronScheduleRow -> OverlapPolicy
effectiveOverlapFor cj row = fromMaybe (overlap cj) (overlapPolicyFromText (CS.effectiveOverlap row))

resolveAndParse :: CronJob payload -> Maybe CS.CronScheduleRow -> Resolved
resolveAndParse cj mRow =
  let (expr, ov, tz, isEnabled) = case mRow of
        Nothing -> (cronExpression cj, overlap cj, timezone cj, True)
        Just row@CS.CronScheduleRow {CS.enabled = rowEnabled} ->
          ( CS.effectiveExpression row
          , effectiveOverlapFor cj row
          , CS.effectiveTimezone row
          , rowEnabled
          )
   in if isEnabled
        then case parseCronSchedule expr of
          Left err -> ParseError expr err
          Right sched -> case tz of
            Just tzName | isNothing (resolveTZ tzName) -> InvalidTimezone tzName
            _ -> Effective ov sched tz
        else Disabled

-- | Attempt to insert a single cron-tick job. Any failure is logged.
--
-- The insert and the per-tick @last_checked_at@ advance are atomic. If
-- either fails the other rolls back, so a successful fire is always paired
-- with a watermark advance to that tick.
tryInsertCronJob
  :: (QueueOperation m payload)
  => CronLog -> Text -> CronJob payload -> OverlapPolicy -> Maybe Text -> TickKind -> UTCTime -> m ()
tryInsertCronJob cronLog schemaName cj effectiveOv effectiveTz kind tick = do
  result <- tryCron cronLog ("Cron schedule '" <> name cj <> "' insert") . withDbTransaction $ do
    -- Gate first: another pool may have already fired this minute.
    fired <- Ops.tryFireCronGate schemaName (name cj) tick
    when fired $ do
      let key = makeDedupKeyFromParts (name cj) effectiveOv effectiveTz tick
          jobWrite = setDedupKey (Just (IgnoreDuplicate key)) $ builder cj kind tick
      void $ HL.insertJob jobWrite
    void $ Ops.touchCronChecked schemaName tick [name cj]
  traverse_
    (const . logCron cronLog Debug $ "Cron schedule '" <> name cj <> "' processed at " <> formatMinute tick)
    result

data RunNowOutcome = Fired | Skipped | NotRequested

-- | Claim and fire every schedule with a pending run request. A 'SkipOverlap'
-- schedule reuses its constant dedup key, so a manual run is skipped while one
-- of its jobs is already active.
--
-- The claim and the insert are atomic. If either fails the other rolls back.
processRunRequests
  :: forall payload m
   . (QueueOperation m payload)
  => CronLog -> Text -> [CronJob payload] -> UTCTime -> m ()
processRunRequests cronLog schemaName jobs now = do
  scan <- tryCron cronLog "Cron run-request scan" $ Ops.pendingCronRuns schemaName (map name jobs)
  traverse_ (fireRequested . Set.fromList) scan
  where
    fireRequested requested =
      traverse_ (claimAndFire (truncateToMinute now)) (filter (\cj -> Set.member (name cj) requested) jobs)
    -- A run-now happens only when somebody asks for one, so every failure is news.
    claimAndFire tick cj = do
      outcome <- tryAny . withDbTransaction $ do
        claimed <- Ops.claimCronRun schemaName (name cj)
        maybe (pure NotRequested) (fireClaimed tick cj) claimed
      case outcome of
        Left e ->
          logCron cronLog Error $ "Cron '" <> name cj <> "' run-now aborted: " <> displayEx e
        Right NotRequested -> pure ()
        Right Fired ->
          logCron cronLog Info $ "Cron '" <> name cj <> "' run-now fired at " <> formatMinute tick
        Right Skipped ->
          logCron cronLog Warning $ "Cron '" <> name cj <> "' run-now skipped, a job is already active"
    fireClaimed tick cj row = do
      let key = case effectiveOverlapFor cj row of
            SkipOverlap -> Just (IgnoreDuplicate (skipOverlapKey (name cj)))
            AllowOverlap -> Nothing
          jobWrite = setDedupKey key $ builder cj Live tick
      inserted <- HL.insertJob jobWrite
      case inserted of
        Just _ -> do
          void $ Ops.touchCronManualRun schemaName tick (name cj)
          pure Fired
        Nothing -> pure Skipped

-- | The scheduler's logger with the gates its repeating failures report through.
data CronLog = CronLog
  { cronLogConfig :: LogConfig
  , cronLogGates :: FailureGates
  }

-- | A 'CronLog' with no gate tripped, for one scheduler run.
newCronLog :: (MonadIO m) => LogConfig -> m CronLog
newCronLog logCfg = CronLog logCfg <$> newFailureGates

-- | Log a cron message, swallowing logger failures.
logCron :: (MonadIO m) => CronLog -> LogLevel -> Text -> m ()
logCron cronLog level msg = liftIO $ tryLog (cronLogConfig cronLog) level msg

-- | Run a scheduler step, reporting only when its outcome changes. A schedule that
-- keeps failing the same way says so one time, then again when it recovers.
tryCron :: (MonadUnliftIO m) => CronLog -> Text -> m a -> m (Either SomeException a)
tryCron cronLog = tryReportedOn (cronLogConfig cronLog) Error (cronLogGates cronLog)

-- | Dedup key for a cron job, from its code-defined overlap and timezone.
makeDedupKey :: CronJob payload -> UTCTime -> Text
makeDedupKey cj tick = makeDedupKeyFromParts (name cj) (overlap cj) (timezone cj) tick

-- | For 'AllowOverlap', the key includes the tick formatted in the schedule's
-- timezone. A DST fall-back minute fires one time.
makeDedupKeyFromParts :: Text -> OverlapPolicy -> Maybe Text -> UTCTime -> Text
makeDedupKeyFromParts jobName ov tz tick = case ov of
  SkipOverlap -> skipOverlapKey jobName
  AllowOverlap -> "arbiter_cron:" <> jobName <> ":" <> formatMinuteInTimezone tz tick

-- | The tick-independent key a 'SkipOverlap' schedule reuses, so at most one of
-- its jobs is ever active.
skipOverlapKey :: Text -> Text
skipOverlapKey jobName = "arbiter_cron:" <> jobName

-- | The longest a scheduler tick waits for the next minute boundary.
maxTickDelayMicros :: Int
maxTickDelayMicros = 120_000_000

-- | Delay in microseconds until the next minute boundary, clamped to
-- @[0, 'maxTickDelayMicros']@.
computeDelayMicros :: UTCTime -> Int
computeDelayMicros now =
  let nextMinute = truncateToMinute (addUTCTime 60 now)
   in max 0 (min maxTickDelayMicros (Ops.micros (diffUTCTime nextMinute now)))

-- | Why the scheduler woke.
data WakeReason = WakeShutdown | WakeMinute | WakeRunNow

-- | Block until @timerVar@ elapses, a run-now signal arrives, or shutdown.
-- Shutdown wins, then an elapsed minute boundary, then a pending run-now.
waitForWake :: (MonadIO m) => TVar WorkerState -> TVar Bool -> TVar Bool -> m WakeReason
waitForWake stateVar runNowVar timerVar = liftIO . atomically $ do
  st <- readTVar stateVar
  case st of
    ShuttingDown -> pure WakeShutdown
    _ -> do
      timedOut <- readTVar timerVar
      reason <-
        if timedOut
          then pure WakeMinute
          else do
            requested <- readTVar runNowVar
            unless requested retry
            pure WakeRunNow
      writeTVar runNowVar False
      pure reason

-- | Snapshot of the current 'WorkerState' for use outside STM.
isShuttingDown :: (MonadIO m) => TVar WorkerState -> m Bool
isShuttingDown stateVar = (== ShuttingDown) <$> readTVarIO stateVar

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
  for_ jobs $ \cron ->
    Ops.upsertCronDefault
      schemaName
      (name cron)
      queueName
      (cronExpression cron)
      (overlapPolicyToText (overlap cron))
      (timezone cron)
  liftIO . tryLog logCfg Info $
    "Cron schedules initialized: " <> T.pack (show (length jobs)) <> " schedule(s) upserted"

-- | Scheduler entry point. Exits when the worker state becomes 'ShuttingDown'.
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
    processOneCron currentTick cron = do
      outcome <- tryCron cronLog ("Cron '" <> name cron <> "' tick") . withDbTransaction $ do
        haveLeader <- case backfill cron of
          NoBackfill -> pure True
          Backfill _ -> Ops.tryAcquireCronLeader schemaName queueName (name cron)
        if not haveLeader
          then pure NotLeader
          else do
            mRow <- Ops.getCronScheduleByName schemaName (name cron)
            processOne mRow currentTick cron
            void $ Ops.touchCronChecked schemaName currentTick [name cron]
            pure Ran
      case outcome of
        Left _ -> pure ()
        Right NotLeader ->
          logCron cronLog Debug $ "Cron '" <> name cron <> "' skipped, another pool holds the lock"
        Right Ran -> pure ()
    processOne mRow currentTick cron = case resolveAndParse cron mRow of
      Disabled -> pure ()
      ParseError expr err ->
        logCron cronLog Error $
          "Cron schedule '"
            <> name cron
            <> "' has invalid effective expression '"
            <> expr
            <> "': "
            <> T.pack err
      InvalidTimezone tzName ->
        logCron cronLog Error $
          "Cron schedule '"
            <> name cron
            <> "' has unknown timezone '"
            <> tzName
            <> "'"
      Effective effectiveOv sched effectiveTz -> do
        let ticksInWindow = enumerateCatchUpTicks (backfill cron) (mRow >>= CS.lastCheckedAt) currentTick
            ticksToFire = pickTicksToFire sched effectiveTz effectiveOv ticksInWindow
            replayCount = length (filter (/= currentTick) ticksToFire)
        when (replayCount > 0)
          $ logCron cronLog Info
          $ "Replaying " <> T.pack (show replayCount) <> " missed tick(s) for '" <> name cron <> "'"
        for_ ticksToFire $ \tick ->
          tryInsertCronJob cronLog schemaName cron effectiveOv effectiveTz (tickKindFor currentTick tick) tick

data TickOutcome = NotLeader | Ran

-- | 'Live' for @currentTick@ and 'Replay' for any other tick.
tickKindFor :: UTCTime -> UTCTime -> TickKind
tickKindFor currentTick tick = if tick == currentTick then Live else Replay

-- | 'SkipOverlap' keeps the oldest match. 'AllowOverlap' keeps all.
-- Match evaluation uses the supplied timezone ('Nothing' = UTC).
pickTicksToFire :: CronSchedule -> Maybe Text -> OverlapPolicy -> [UTCTime] -> [UTCTime]
pickTicksToFire sched zone overlapPolicy ticks =
  let matching = filter (matchesInTimezone zone sched) ticks
   in case overlapPolicy of
        SkipOverlap -> take 1 matching
        AllowOverlap -> matching

-- | Minutes to evaluate for a 'processCronCatchUp' call. Returns @[]@ when
-- the watermark is at or past @currentTick@.
enumerateCatchUpTicks :: BackfillPolicy -> Maybe UTCTime -> UTCTime -> [UTCTime]
enumerateCatchUpTicks _ (Just lastChecked) currentTick
  | truncateToMinute lastChecked >= currentTick = []
enumerateCatchUpTicks NoBackfill _ currentTick = [currentTick]
enumerateCatchUpTicks _ Nothing currentTick = [currentTick]
enumerateCatchUpTicks (Backfill window) (Just lastChecked) currentTick =
  let truncatedLast = truncateToMinute lastChecked
      -- Truncate the window floor to a minute boundary.
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
effectiveOverlapFor cron row = fromMaybe (overlap cron) (overlapPolicyFromText (CS.effectiveOverlap row))

resolveAndParse :: CronJob payload -> Maybe CS.CronScheduleRow -> Resolved
resolveAndParse cron mRow =
  let (expr, overlapPolicy, zone, isEnabled) = case mRow of
        Nothing -> (cronExpression cron, overlap cron, timezone cron, True)
        Just row@CS.CronScheduleRow {CS.enabled = rowEnabled} ->
          ( CS.effectiveExpression row
          , effectiveOverlapFor cron row
          , CS.effectiveTimezone row
          , rowEnabled
          )
   in if isEnabled
        then case parseCronSchedule expr of
          Left err -> ParseError expr err
          Right sched -> case zone of
            Just tzName | isNothing (resolveTZ tzName) -> InvalidTimezone tzName
            _ -> Effective overlapPolicy sched zone
        else Disabled

-- | Attempt to insert a single cron-tick job. Any failure is logged.
--
-- The insert and the per-tick @last_checked_at@ advance are atomic. If
-- either fails the other rolls back.
tryInsertCronJob
  :: (QueueOperation m payload)
  => CronLog -> Text -> CronJob payload -> OverlapPolicy -> Maybe Text -> TickKind -> UTCTime -> m ()
tryInsertCronJob cronLog schemaName cron effectiveOv effectiveTz kind tick = do
  result <- tryCron cronLog ("Cron schedule '" <> name cron <> "' insert") . withDbTransaction $ do
    -- Gate first. Another pool may have fired this minute.
    fired <- Ops.tryFireCronGate schemaName (name cron) tick
    when fired $ do
      let key = makeDedupKeyFromParts (name cron) effectiveOv effectiveTz tick
          jobWrite = setDedupKey (Just (IgnoreDuplicate key)) $ builder cron kind tick
      void $ HL.insertJob jobWrite
    void $ Ops.touchCronChecked schemaName tick [name cron]
  traverse_
    (const . logCron cronLog Debug $ "Cron schedule '" <> name cron <> "' processed at " <> formatMinute tick)
    result

data RunNowOutcome = Fired | Skipped | NotRequested

-- | Claim and fire every schedule with a pending run request. A 'SkipOverlap'
-- schedule reuses its constant dedup key. A manual run is skipped while one of
-- its jobs is active.
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
      traverse_ (claimAndFire (truncateToMinute now)) (filter (\cron -> Set.member (name cron) requested) jobs)
    -- Every run-now failure is reported.
    claimAndFire tick cron = do
      outcome <- tryAny . withDbTransaction $ do
        claimed <- Ops.claimCronRun schemaName (name cron)
        maybe (pure NotRequested) (fireClaimed tick cron) claimed
      case outcome of
        Left exception ->
          logCron cronLog Error $ "Cron '" <> name cron <> "' run-now aborted: " <> displayEx exception
        Right NotRequested -> pure ()
        Right Fired ->
          logCron cronLog Info $ "Cron '" <> name cron <> "' run-now fired at " <> formatMinute tick
        Right Skipped ->
          logCron cronLog Warning $ "Cron '" <> name cron <> "' run-now skipped, a job is already active"
    fireClaimed tick cron row = do
      let key = case effectiveOverlapFor cron row of
            SkipOverlap -> Just (IgnoreDuplicate (skipOverlapKey (name cron)))
            AllowOverlap -> Nothing
          jobWrite = setDedupKey key $ builder cron Live tick
      inserted <- HL.insertJob jobWrite
      case inserted of
        Just _ -> do
          void $ Ops.touchCronManualRun schemaName tick (name cron)
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
makeDedupKey cron tick = makeDedupKeyFromParts (name cron) (overlap cron) (timezone cron) tick

-- | For 'AllowOverlap', the key includes the tick formatted in the schedule's
-- timezone. A DST fall-back minute fires one time.
makeDedupKeyFromParts :: Text -> OverlapPolicy -> Maybe Text -> UTCTime -> Text
makeDedupKeyFromParts jobName overlapPolicy zone tick = case overlapPolicy of
  SkipOverlap -> skipOverlapKey jobName
  AllowOverlap -> "arbiter_cron:" <> jobName <> ":" <> formatMinuteInTimezone zone tick

-- | The tick-independent key a 'SkipOverlap' schedule reuses. At most one of its
-- jobs is active.
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
  state <- readTVar stateVar
  case state of
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

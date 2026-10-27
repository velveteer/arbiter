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
  , mkDedupKeyFromParts
  , computeDelayMicros
  ) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Enum (showText)
import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Types (DedupKey (IgnoreDuplicate), setDedupKey)
import Arbiter.Core.MonadArbiter (MonadArbiter, getSchema, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (unless, void, when)
import Control.Monad.IO.Class (MonadIO)
import Data.Either (fromRight)
import Data.Foldable (for_, traverse_)
import Data.Maybe (fromMaybe, isNothing)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, addUTCTime, diffUTCTime, getCurrentTime)
import System.Cron (CronSchedule, parseCronSchedule)
import UnliftIO
  ( MonadUnliftIO
  , SomeException
  , TVar
  , atomically
  , checkSTM
  , liftIO
  , orElse
  , readTVar
  , registerDelay
  , tryAny
  , writeTVar
  )

import Arbiter.Worker.Cron.Types
import Arbiter.Worker.Logger (FailureGates, LogConfig, LogLevel (..), newFailureGates, tryLog, tryReportedOn)
import Arbiter.Worker.WorkerState (WorkerState (..), isShuttingDown)

-- | Upsert the default expression, overlap, timezone and initial enabled flag of
-- each 'CronJob' into the @cron_schedules@ table. Preserves user overrides. An
-- existing row keeps its enabled state.
initCronSchedules
  :: (MonadArbiter m)
  => LogConfig
  -> Text
  -- ^ Queue name
  -> [CronJob payload]
  -> m ()
initCronSchedules logCfg queueName jobs = do
  schemaName <- getSchema
  for_ jobs $ \cron ->
    Ops.upsertCronDefault
      schemaName
      (name cron)
      queueName
      (cronExpression cron)
      (overlapPolicyToText (overlap cron))
      (timezone cron)
      (initiallyEnabled cron)
  tryLog logCfg Info $
    "Cron schedules initialized: " <> showText (length jobs) <> " schedule(s) upserted"

-- | Scheduler entry point. Exits when the worker state becomes 'ShuttingDown'.
runCronScheduler
  :: (QueueOperation m payload)
  => LogConfig
  -> TVar WorkerState
  -> TVar Bool
  -- ^ Set by the run-now listener when a schedule this pool owns is requested.
  -> SchemaName
  -> Text
  -- ^ Queue name (recorded on each schedule row).
  -> [CronJob payload]
  -> m ()
runCronScheduler logCfg stateVar runNowVar schemaName queueName jobs = do
  initCronSchedules logCfg queueName jobs
  cronLog <- newCronLog logCfg
  startupNow <- liftIO getCurrentTime
  shuttingDown <- atomically (isShuttingDown stateVar)
  unless shuttingDown $ catchUp cronLog startupNow
  logCron cronLog Info $ "Cron scheduler started with " <> showText (length jobs) <> " schedule(s)"
  loop cronLog
  where
    catchUp cronLog now = do
      processRunRequests cronLog schemaName jobs now
      processCronCatchUp cronLog schemaName queueName jobs now
    loop cronLog = do
      now <- liftIO getCurrentTime
      timerVar <- registerDelay (computeDelayMicros now)
      serve cronLog timerVar
    serve cronLog timerVar = do
      wake <- waitForWake stateVar runNowVar timerVar
      case wake of
        WakeShutdown -> pure ()
        WakeMinute -> do
          liftIO getCurrentTime >>= catchUp cronLog
          loop cronLog
        WakeRunNow -> do
          now <- liftIO getCurrentTime
          processRunRequests cronLog schemaName jobs now
          serve cronLog timerVar

-- | Scheduler catch-up step. Each tick commits independently. Backfill ticks
-- acquire a per-schedule advisory lock for their transaction.
--
-- A failed tick is logged and skipped once a later tick fires. The watermark
-- of a Backfill schedule stays behind a failed newest tick, and the next pass
-- retries it.
processCronCatchUp
  :: (QueueOperation m payload)
  => CronLog
  -> SchemaName
  -> Text
  -- ^ Queue name
  -> [CronJob payload]
  -> UTCTime
  -- ^ Current wall-clock time
  -> m ()
processCronCatchUp cronLog schemaName queueName jobs now = traverse_ processOneCron jobs
  where
    currentTick = truncateToMinute now
    processOneCron cron = do
      void $ tryCron cronLog (cronLabel cron <> " tick") $ do
        mRow <- Ops.getCronScheduleByName schemaName (name cron)
        when (isNothing mRow)
          $ logCron cronLog Warning
          $ cronLabel cron <> " has no schedule row and does not fire until the pool restarts"
        completed <- processOne mRow cron
        when completed $ void $ Ops.touchCronChecked schemaName [name cron] currentTick
    processOne mRow cron = case resolveAndParse cron mRow of
      Disabled -> pure True
      ParseError expr err ->
        True <$ logCron cronLog Error (cronLabel cron <> " has invalid effective expression '" <> expr <> "': " <> T.pack err)
      InvalidTimezone tzName ->
        True <$ logCron cronLog Error (cronLabel cron <> " has unknown timezone '" <> tzName <> "'")
      Effective effectiveOv sched effectiveTz -> do
        let ticksToFire =
              filter (matchesInTimezone effectiveTz sched) $
                enumerateCatchUpTicks (backfill cron) (mRow >>= CS.lastCheckedAt) currentTick
            fire = fireCronTick cronLog schemaName queueName cron effectiveOv currentTick
        (replayed, lastOutcome) <- fireTicks (stopsAt effectiveOv) fire currentTick ticksToFire
        when (replayed > 0)
          $ logCron cronLog Info
          $ cronLabel cron <> " replayed " <> showText replayed <> " missed tick(s)"
        pure (advancesWatermark (backfill cron) lastOutcome)

-- | Fire one catch-up tick in its own transaction and log how it ended.
fireCronTick
  :: (QueueOperation m payload)
  => CronLog -> SchemaName -> Text -> CronJob payload -> OverlapPolicy -> UTCTime -> UTCTime -> m TickOutcome
fireCronTick cronLog schemaName queueName cron effectiveOv currentTick tick = do
  result <- tryCron cronLog (cronLabel cron <> " insert") . withDbTransaction $ do
    haveLeader <- case backfill cron of
      NoBackfill -> pure True
      Backfill _ -> Ops.tryAcquireCronLeader schemaName queueName (name cron)
    if haveLeader
      then TickHandled <$> insertCronJob schemaName cron effectiveOv (tickKindFor currentTick tick) tick
      else pure TickNoLeader
  let outcome = fromRight TickFailed result
  case outcome of
    TickHandled _ -> logCron cronLog Debug $ cronLabel cron <> " processed at " <> formatMinute tick
    TickNoLeader -> logCron cronLog Debug $ cronLabel cron <> " skipped, another pool holds the lock"
    TickFailed -> pure ()
  pure outcome

-- | Fire @ticks@ in order until @stop@ holds. Returns the replayed count and the last outcome.
fireTicks
  :: (Monad m) => (TickOutcome -> Bool) -> (UTCTime -> m TickOutcome) -> UTCTime -> [UTCTime] -> m (Int, TickOutcome)
fireTicks stop fire currentTick = go (0, TickHandled False)
  where
    go acc [] = pure acc
    go (replayed, _) (tick : ticks) = do
      outcome <- fire tick
      let next = (replayed + replayedBy tick outcome, outcome)
      if stop outcome then pure next else go next ticks
    replayedBy tick outcome = if tick /= currentTick && outcome == TickHandled True then 1 else 0

-- | Whether a tick outcome ends the catch-up pass under an overlap policy.
stopsAt :: OverlapPolicy -> TickOutcome -> Bool
stopsAt AllowOverlap = (== TickNoLeader)
stopsAt SkipOverlap = (/= TickFailed)

-- | Whether the last tick outcome lets the pass advance the watermark to the current tick.
advancesWatermark :: BackfillPolicy -> TickOutcome -> Bool
advancesWatermark _ (TickHandled _) = True
advancesWatermark NoBackfill TickFailed = True
advancesWatermark _ _ = False

-- | How one tick's transaction ended. 'TickHandled' carries whether this pool
-- fired the gate.
data TickOutcome = TickHandled Bool | TickNoLeader | TickFailed
  deriving stock (Eq)

-- | 'Live' for @currentTick@ and 'Replay' for any other tick.
tickKindFor :: UTCTime -> UTCTime -> TickKind
tickKindFor currentTick tick = if tick == currentTick then Live else Replay

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
      startMinute = max (addUTCTime oneMinute truncatedLast) windowFloor
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
resolveAndParse cron mRow
  | not isEnabled = Disabled
  | otherwise = either (ParseError expr) withSchedule (parseCronSchedule expr)
  where
    (expr, overlapPolicy, zone, isEnabled) = case mRow of
      Nothing -> (cronExpression cron, overlap cron, timezone cron, initiallyEnabled cron)
      Just row@CS.CronScheduleRow {CS.enabled = rowEnabled} ->
        ( CS.effectiveExpression row
        , effectiveOverlapFor cron row
        , CS.effectiveTimezone row
        , rowEnabled
        )
    withSchedule sched
      | Just tzName <- zone, isNothing (resolveTZ tzName) = InvalidTimezone tzName
      | otherwise = Effective overlapPolicy sched zone

-- | Fire the gate for @tick@, insert its job if the gate opened, and advance
-- the watermark to @tick@. Returns whether this call fired the gate.
insertCronJob
  :: (QueueOperation m payload)
  => SchemaName -> CronJob payload -> OverlapPolicy -> TickKind -> UTCTime -> m Bool
insertCronJob schemaName cron effectiveOv kind tick = do
  -- Gate first. Another pool may have fired this minute.
  fired <- Ops.tryFireCronGate schemaName (name cron) tick
  when fired $ do
    let key = mkDedupKeyFromParts (name cron) effectiveOv tick
        jobWrite = setDedupKey (Just (IgnoreDuplicate key)) $ builder cron kind tick
    void $ HL.insertJob jobWrite
  void $ Ops.touchCronChecked schemaName [name cron] tick
  pure fired

data RunNowOutcome = Fired | Skipped | NotRequested

-- | Claim and fire every schedule with a pending run request. A 'SkipOverlap'
-- schedule reuses its constant dedup key, so its manual run is skipped while one
-- of its jobs is active.
--
-- The claim and the insert are atomic. If either fails the other rolls back.
processRunRequests
  :: forall payload m
   . (QueueOperation m payload)
  => CronLog -> SchemaName -> [CronJob payload] -> UTCTime -> m ()
processRunRequests cronLog schemaName jobs now = do
  scan <- tryCron cronLog "Cron run-request scan" $ Ops.pendingCronRuns schemaName (map name jobs)
  traverse_ (fireRequested . Set.fromList) scan
  where
    fireRequested requested =
      traverse_ claimAndFire (filter (\cron -> Set.member (name cron) requested) jobs)
    tick = truncateToMinute now
    -- Every run-now failure is reported.
    claimAndFire cron = do
      outcome <- tryAny . withDbTransaction $ do
        claimed <- Ops.claimCronRun schemaName (name cron)
        maybe (pure NotRequested) (fireClaimed cron) claimed
      case outcome of
        Left exception ->
          logCron cronLog Error $ cronLabel cron <> " run-now aborted: " <> displayEx exception
        Right NotRequested -> pure ()
        Right Fired ->
          logCron cronLog Info $ cronLabel cron <> " run-now fired at " <> formatMinute tick
        Right Skipped ->
          logCron cronLog Warning $ cronLabel cron <> " run-now skipped, a job is already active"
    fireClaimed cron row = do
      let key = case effectiveOverlapFor cron row of
            SkipOverlap -> Just (IgnoreDuplicate (skipOverlapKey (name cron)))
            AllowOverlap -> Nothing
          jobWrite = setDedupKey key $ builder cron Live tick
      inserted <- HL.insertJob jobWrite
      case inserted of
        Just _ -> do
          void $ Ops.touchCronManualRun schemaName (name cron) tick
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

-- | A schedule's name as a log prefix.
cronLabel :: CronJob payload -> Text
cronLabel cron = "Cron '" <> name cron <> "'"

-- | Log a cron message, swallowing logger failures.
logCron :: (MonadIO m) => CronLog -> LogLevel -> Text -> m ()
logCron cronLog level msg = liftIO $ tryLog (cronLogConfig cronLog) level msg

-- | Run a scheduler step. Report new failures and recovery. Suppress repeated
-- identical failures.
tryCron :: (MonadUnliftIO m) => CronLog -> Text -> m a -> m (Either SomeException a)
tryCron cronLog = tryReportedOn (cronLogConfig cronLog) Error (cronLogGates cronLog)

-- | The dedup key for a schedule's tick. An 'AllowOverlap' key includes the UTC tick minute.
mkDedupKeyFromParts :: Text -> OverlapPolicy -> UTCTime -> Text
mkDedupKeyFromParts jobName overlapPolicy tick = case overlapPolicy of
  SkipOverlap -> skipOverlapKey jobName
  AllowOverlap -> skipOverlapKey jobName <> ":" <> formatMinute tick

-- | The tick-independent key a 'SkipOverlap' schedule reuses. At most one of its
-- jobs is active.
skipOverlapKey :: Text -> Text
skipOverlapKey jobName = "arbiter_cron:" <> jobName

-- | The longest a scheduler tick waits for the next minute boundary.
maxTickDelay :: NominalDiffTime
maxTickDelay = 120

-- | Delay in microseconds until the next minute boundary, clamped to
-- zero through 'maxTickDelay'.
computeDelayMicros :: UTCTime -> Int
computeDelayMicros now =
  let nextMinute = truncateToMinute (addUTCTime oneMinute now)
   in max 0 (min (Ops.micros maxTickDelay) (Ops.micros (diffUTCTime nextMinute now)))

-- | Why the scheduler woke.
data WakeReason = WakeShutdown | WakeMinute | WakeRunNow

-- | Block until @timerVar@ elapses, a run-now signal arrives, or shutdown.
-- Shutdown wins, then an elapsed minute boundary, then a pending run-now.
waitForWake :: (MonadIO m) => TVar WorkerState -> TVar Bool -> TVar Bool -> m WakeReason
waitForWake stateVar runNowVar timerVar = atomically $ do
  state <- readTVar stateVar
  case state of
    ShuttingDown -> pure WakeShutdown
    _ -> do
      reason <- (WakeMinute <$ awaitSet timerVar) `orElse` (WakeRunNow <$ awaitSet runNowVar)
      writeTVar runNowVar False
      pure reason
  where
    awaitSet var = readTVar var >>= checkSTM

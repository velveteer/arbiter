{-# LANGUAGE OverloadedStrings #-}

-- | Cron scheduler for the Arbiter worker pool.
--
-- When 'cronJobs' is non-empty in 'WorkerConfig', 'runWorkerPool' spawns a
-- scheduler thread that inserts jobs on 5-field cron expressions.
-- Dedup keys prevent duplicate insertion across multiple worker instances.
--
-- Expressions default to UTC. Pass an IANA tz name (e.g. @\"America\/New_York\"@)
-- to 'cronJobInTimezone' to evaluate in local time instead.
--
-- The scheduler consults the @cron_schedules@ table each tick for runtime
-- overrides (expression, overlap, timezone, enabled).
module Arbiter.Worker.Cron
  ( -- * Types
    CronJob (..)
  , OverlapPolicy (..)
  , BackfillPolicy (..)
  , TickKind (..)

    -- * Smart Constructors
  , cronJob
  , cronJobInTimezone

    -- * Helpers
  , overlapPolicyToText
  , overlapPolicyFromText
  , validateCronScheduleUpdate
  , updateCronScheduleChecked
  , resolveTZ
  , matchesInTimezone
  , formatMinuteInTimezone

    -- * Internal
  , runCronScheduler
  , initCronSchedules
  , processCronCatchUp
  , processRunRequests
  , enumerateCatchUpTicks
  , truncateToMinute
  , formatMinute
  , makeDedupKey
  , computeDelayMicros
  , enumMinutes
  ) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.Job.Types (DedupKey (IgnoreDuplicate), JobWrite, dedupKey, parentId)
import Arbiter.Core.MonadArbiter (MonadArbiter, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Control.Concurrent.STM (retry)
import Control.Exception (displayException)
import Control.Monad (forM_, unless, void, when)
import Control.Monad.IO.Class (MonadIO)
import Data.Either (isRight)
import Data.Int (Int64)
import Data.List (unfoldr)
import Data.Maybe (fromMaybe, isJust, isNothing)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Data.Time
  ( NominalDiffTime
  , UTCTime (..)
  , addUTCTime
  , defaultTimeLocale
  , diffUTCTime
  , formatTime
  , getCurrentTime
  , localTimeToUTC
  , secondsToDiffTime
  , utc
  )
import Data.Time.Zones (TZ, utcToLocalTimeTZ)
import Data.Time.Zones.All (fromTZName, tzByLabel)
import GHC.Generics (Generic)
import System.Cron (CronSchedule, parseCronSchedule, scheduleMatches)
import UnliftIO (TVar, atomically, liftIO, readTVar, readTVarIO, registerDelay, tryAny, writeTVar)

import Arbiter.Worker.Logger (LogConfig, LogLevel (..), tryLog)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | How overlapping cron ticks are deduplicated.
data OverlapPolicy
  = -- | At most one pending or running job per schedule.
    SkipOverlap
  | -- | One job per tick. Concurrent execution of prior ticks is allowed.
    AllowOverlap
  deriving stock (Eq, Generic, Show)

-- | Bound on replay of missed ticks. Applies at startup and mid-flight.
data BackfillPolicy
  = -- | Drop missed minutes silently. Default.
    NoBackfill
  | -- | Replay missed minutes up to the given duration.
    Backfill NominalDiffTime
  deriving stock (Eq, Generic, Show)

-- | Whether a tick is for the current minute or a replay of a past minute.
data TickKind = Live | Replay
  deriving stock (Eq, Generic, Show)

-- | Convert an 'OverlapPolicy' to its text representation.
overlapPolicyToText :: OverlapPolicy -> Text
overlapPolicyToText SkipOverlap = "SkipOverlap"
overlapPolicyToText AllowOverlap = "AllowOverlap"

-- | Parse an 'OverlapPolicy' from text.
overlapPolicyFromText :: Text -> Maybe OverlapPolicy
overlapPolicyFromText "SkipOverlap" = Just SkipOverlap
overlapPolicyFromText "AllowOverlap" = Just AllowOverlap
overlapPolicyFromText _ = Nothing

-- | Check a cron patch before it is written. The scheduler does not reject a
-- bad override at tick time: it stops firing, or falls back to the default.
validateCronScheduleUpdate :: CS.CronScheduleUpdate -> Either Text ()
validateCronScheduleUpdate (CS.CronScheduleUpdate mExpr mOverlap mTz _) = do
  check mExpr (isRight . parseCronSchedule) "Invalid cron expression"
  check mOverlap (isJust . overlapPolicyFromText) "Invalid overlap policy: must be SkipOverlap or AllowOverlap"
  check mTz (isJust . resolveTZ) "Invalid timezone: must be an IANA tz name (e.g. America/New_York)"
  where
    check field ok message = case field of
      Just (Just v) | not (ok v) -> Left message
      _ -> Right ()

-- | 'Arbiter.Core.HighLevel.updateCronScheduleUnchecked' behind
-- 'validateCronScheduleUpdate'. Returns rows affected (0 = not found).
updateCronScheduleChecked
  :: (MonadArbiter m)
  => Text
  -> CS.CronScheduleUpdate
  -> m (Either Text Int64)
updateCronScheduleChecked scheduleName upd =
  case validateCronScheduleUpdate upd of
    Left err -> pure (Left err)
    Right () -> Right <$> HL.updateCronScheduleUnchecked scheduleName upd

-- | A cron schedule definition.
--
-- Use 'cronJob' to construct (eagerly parses the cron expression). For a
-- non-UTC timezone, use 'cronJobInTimezone' (also validates the tz name).
data CronJob payload = CronJob
  { name :: Text
  -- ^ Human-readable name for logging and dedup keys
  , cronExpression :: Text
  -- ^ Original cron expression text (for DB storage)
  , overlap :: OverlapPolicy
  -- ^ How to handle overlapping ticks
  , backfill :: BackfillPolicy
  -- ^ How to replay missed ticks, at startup and mid-flight. Default: 'NoBackfill'.
  , timezone :: Maybe Text
  -- ^ IANA tz name (e.g. @\"America\/New_York\"@). 'Nothing' means UTC.
  -- The @override_timezone@ DB column wins if set.
  , builder :: TickKind -> UTCTime -> JobWrite payload
  -- ^ Build a job for the given tick time. 'Replay' is passed for any tick
  -- whose minute is not the current scheduler minute (startup or mid-flight
  -- catch-up). 'Live' is passed for the current minute boundary.
  }
  deriving stock (Generic)

-- | Smart constructor for 'CronJob'. Parses the cron expression eagerly.
--
-- Returns @Left@ with an error message if the cron expression is invalid.
--
-- @
-- cronJob "nightly-report" "0 3 * * *" SkipOverlap
--   (\\_kind _tick -> defaultJob (GenerateReport "nightly"))
-- @
--
-- The builder receives a 'TickKind' ('Live' for the current minute, 'Replay'
-- for any catch-up tick) and the tick time.
--
-- To enable backfill, set it via record update:
--
-- @
-- let Right cj = cronJob "nightly-report" "0 3 * * *" AllowOverlap
--       (\\kind tick -> (defaultJob (GenerateReport tick))
--          { priority = case kind of
--              Replay -> 10
--              Live -> 0 })
-- in cj { backfill = Backfill 86400 }  -- replay up to 24 hours
-- @
--
-- Note: cron expressions are evaluated in __UTC__. @\"0 3 * * *\"@ fires at
-- 03:00 UTC regardless of the server's local timezone.
cronJob
  :: Text
  -- ^ Schedule name (used in dedup keys and logging)
  -> Text
  -- ^ Cron expression (5-field: minute hour day-of-month month day-of-week)
  -> OverlapPolicy
  -> (TickKind -> UTCTime -> JobWrite payload)
  -- ^ Job builder. Receives the tick kind and the tick time.
  -> Either String (CronJob payload)
cronJob cronName expr ov mk =
  case parseCronSchedule expr of
    Left err -> Left err
    Right _ ->
      Right
        CronJob
          { name = cronName
          , cronExpression = expr
          , overlap = ov
          , backfill = NoBackfill
          , timezone = Nothing
          , builder = mk
          }

-- | Like 'cronJob' but evaluated in a specific timezone. The tz name is
-- validated eagerly against the bundled @tzdata@ database.
cronJobInTimezone
  :: Text
  -- ^ Schedule name
  -> Text
  -- ^ IANA tz name (e.g. @\"America\/New_York\"@)
  -> Text
  -- ^ Cron expression (5-field)
  -> OverlapPolicy
  -> (TickKind -> UTCTime -> JobWrite payload)
  -> Either String (CronJob payload)
cronJobInTimezone cronName tzName expr ov mk =
  case resolveTZ tzName of
    Nothing -> Left $ "Unknown timezone: " <> T.unpack tzName
    Just _ -> fmap (\cj -> cj {timezone = Just tzName}) (cronJob cronName expr ov mk)

-- | Look up an IANA tz name in the bundled @tzdata@ database.
resolveTZ :: Text -> Maybe TZ
resolveTZ name = fmap tzByLabel (fromTZName (encodeUtf8 name))

-- | Match a cron schedule against a UTC tick, evaluated in @tz@.
-- 'Nothing' means UTC. An unknown tz name returns 'False'.
matchesInTimezone :: Maybe Text -> CronSchedule -> UTCTime -> Bool
matchesInTimezone Nothing sched t = scheduleMatches sched t
matchesInTimezone (Just tzName) sched t =
  case resolveTZ tzName of
    Nothing -> False
    Just tz ->
      let local = utcToLocalTimeTZ tz t
          asUtc = localTimeToUTC utc local
       in scheduleMatches sched asUtc

-- | Format a UTC tick as @YYYY-MM-DDTHH:MM@ in the given timezone.
-- DST fall-back maps two UTC instants to the same local minute, so dedup
-- keys built from this collapse to a single fire.
formatMinuteInTimezone :: Maybe Text -> UTCTime -> Text
formatMinuteInTimezone Nothing t = formatMinute t
formatMinuteInTimezone (Just tzName) t =
  case resolveTZ tzName of
    Nothing -> formatMinute t
    Just tz ->
      let local = utcToLocalTimeTZ tz t
       in T.pack (formatTime defaultTimeLocale "%Y-%m-%dT%H:%M" local)

-- | Upsert default expression and overlap for each 'CronJob' into the
-- @cron_schedules@ table. Preserves any user overrides and enabled state.
initCronSchedules
  :: (MonadArbiter m)
  => SchemaName -> Text -> [CronJob payload] -> LogConfig -> m ()
initCronSchedules schemaName queueName jobs logCfg = do
  forM_ jobs $ \cj ->
    Ops.upsertCronDefault
      schemaName
      (name cj)
      queueName
      (cronExpression cj)
      (overlapPolicyToText (overlap cj))
      (timezone cj)
  logCron logCfg Info $
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
  startupNow <- liftIO getCurrentTime
  shuttingDown <- isShuttingDown stateVar
  unless shuttingDown $ do
    processRunRequests logCfg schemaName jobs startupNow
    processCronCatchUp logCfg schemaName queueName jobs startupNow
  logCron logCfg Info $ "Cron scheduler started with " <> T.pack (show (length jobs)) <> " schedule(s)"
  loop
  where
    loop = do
      now <- liftIO getCurrentTime
      timerVar <- liftIO $ registerDelay (computeDelayMicros now)
      serve timerVar
    serve timerVar = do
      wake <- waitForWake stateVar runNowVar timerVar
      case wake of
        WakeShutdown -> pure ()
        WakeMinute -> do
          now <- liftIO getCurrentTime
          processRunRequests logCfg schemaName jobs now
          processCronCatchUp logCfg schemaName queueName jobs now
          loop
        WakeRunNow -> do
          now <- liftIO getCurrentTime
          processRunRequests logCfg schemaName jobs now
          serve timerVar

-- | Scheduler catch-up step. Each cron runs in its own transaction.
-- Backfill schedules hold a per-(schema, queue, name) advisory lock.
processCronCatchUp
  :: (QueueOperation m payload)
  => LogConfig
  -> Text
  -> Text
  -- ^ Queue name
  -> [CronJob payload]
  -> UTCTime
  -- ^ Current wall-clock time
  -> m ()
processCronCatchUp logCfg schemaName queueName jobs now = do
  let currentTick = truncateToMinute now
  forM_ jobs (processOneCron currentTick)
  where
    processOneCron currentTick cj = do
      outcome <- tryAny . withDbTransaction $ do
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
        Left e ->
          logCron logCfg Error $ "Cron '" <> name cj <> "' tick aborted: " <> T.pack (displayException e)
        Right NotLeader ->
          logCron logCfg Debug $ "Cron '" <> name cj <> "' skipped, another pool holds the lock"
        Right Ran -> pure ()
    processOne mRow currentTick cj = case resolveAndParse cj mRow of
      Disabled -> pure ()
      ParseError expr err ->
        logCron logCfg Error $
          "Cron schedule '"
            <> name cj
            <> "' has invalid effective expression '"
            <> expr
            <> "': "
            <> T.pack err
      InvalidTimezone tzName ->
        logCron logCfg Error $
          "Cron schedule '"
            <> name cj
            <> "' has unknown timezone '"
            <> tzName
            <> "'"
      Effective effectiveOv sched effectiveTz -> do
        let ticksInWindow = enumerateCatchUpTicks (backfill cj) (mRow >>= CS.lastCheckedAt) currentTick
            ticksToFire = pickTicksToFire sched effectiveTz effectiveOv ticksInWindow
            replayCount = length (filter (/= currentTick) ticksToFire)
        when (replayCount > 0) $
          logCron logCfg Info $
            "Replaying " <> T.pack (show replayCount) <> " missed tick(s) for '" <> name cj <> "'"
        forM_ ticksToFire $ \t ->
          void $ tryInsertCronJob logCfg schemaName cj effectiveOv effectiveTz (tickKindFor currentTick t) t

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

-- | Attempt to insert a single cron-tick job. Returns 'False' on any failure
-- (logged), 'True' on successful insert or a dedup-blocked no-op.
--
-- The insert and the per-tick @last_checked_at@ advance are atomic. If
-- either fails the other rolls back, so a successful fire is always paired
-- with a watermark advance to that tick.
tryInsertCronJob
  :: (QueueOperation m payload)
  => LogConfig -> Text -> CronJob payload -> OverlapPolicy -> Maybe Text -> TickKind -> UTCTime -> m Bool
tryInsertCronJob logCfg schemaName cj effectiveOv effectiveTz kind tick = do
  result <- tryAny . withDbTransaction $ do
    -- Gate first: another pool may have already fired this minute.
    fired <- Ops.tryFireCronGate schemaName (name cj) tick
    when fired $ do
      let key = makeDedupKeyFromParts (name cj) effectiveOv effectiveTz tick
          jobWrite = (builder cj kind tick) {dedupKey = Just (IgnoreDuplicate key)}
      void $ HL.insertJob jobWrite
    void $ Ops.touchCronChecked schemaName tick [name cj]
  case result of
    Left e -> do
      logCron logCfg Error $ "Cron schedule '" <> name cj <> "' failed to insert: " <> T.pack (displayException e)
      pure False
    Right () -> do
      logCron logCfg Debug $ "Cron schedule '" <> name cj <> "' processed at " <> formatMinute tick
      pure True

data RunNowOutcome = Fired | Skipped | Dropped | NotRequested

-- | Claim and fire every schedule with a pending run request. A 'SkipOverlap'
-- schedule reuses its constant dedup key, so a manual run is skipped while one
-- of its jobs is already active.
--
-- The claim and the insert are atomic. If either fails the other rolls back.
processRunRequests
  :: forall payload m
   . (QueueOperation m payload)
  => LogConfig -> Text -> [CronJob payload] -> UTCTime -> m ()
processRunRequests logCfg schemaName jobs now = do
  scan <- tryAny $ Ops.pendingCronRuns schemaName (map name jobs)
  case scan of
    Left e -> logCron logCfg Error $ "Cron run-request scan failed: " <> T.pack (displayException e)
    Right pending -> do
      let requested = Set.fromList pending
      forM_ (filter (\cj -> Set.member (name cj) requested) jobs) (claimAndFire (truncateToMinute now))
  where
    claimAndFire tick cj = do
      outcome <- tryAny . withDbTransaction $ do
        claimed <- Ops.claimCronRun schemaName (name cj)
        maybe (pure NotRequested) (fireClaimed tick cj) claimed
      case outcome of
        Left e ->
          logCron logCfg Error $ "Cron '" <> name cj <> "' run-now aborted: " <> T.pack (displayException e)
        Right NotRequested -> pure ()
        Right Fired ->
          logCron logCfg Info $ "Cron '" <> name cj <> "' run-now fired at " <> formatMinute tick
        Right Skipped ->
          logCron logCfg Warning $ "Cron '" <> name cj <> "' run-now skipped, a job is already active"
        Right Dropped ->
          logCron logCfg Error $ "Cron '" <> name cj <> "' run-now inserted no job, the parent job it references is gone"
    fireClaimed tick cj row = do
      let key = case effectiveOverlapFor cj row of
            SkipOverlap -> Just (IgnoreDuplicate (skipOverlapKey (name cj)))
            AllowOverlap -> Nothing
          jobWrite = (builder cj Live tick) {dedupKey = key}
      inserted <- HL.insertJob jobWrite
      case inserted of
        Just _ -> do
          void $ Ops.touchCronManualRun schemaName tick (name cj)
          pure Fired
        -- An absent job is either the dedup key or a missing parent, and only
        -- the parent it names can tell the two apart.
        Nothing -> do
          parentGone <- case parentId jobWrite of
            Nothing -> pure False
            Just pid -> not <$> HL.jobExists @payload pid
          pure $ if parentGone then Dropped else Skipped

-- | Log a cron message, swallowing logger failures.
logCron :: (MonadIO m) => LogConfig -> LogLevel -> Text -> m ()
logCron logCfg level msg = liftIO $ tryLog logCfg level msg

-- | Dedup key for a cron job, from its code-defined overlap and timezone.
makeDedupKey :: CronJob payload -> UTCTime -> Text
makeDedupKey cj tick = makeDedupKeyFromParts (name cj) (overlap cj) (timezone cj) tick

-- | For 'AllowOverlap', the key includes the tick formatted in the schedule's
-- timezone, so DST fall-back fires once instead of twice.
makeDedupKeyFromParts :: Text -> OverlapPolicy -> Maybe Text -> UTCTime -> Text
makeDedupKeyFromParts jobName ov tz tick = case ov of
  SkipOverlap -> skipOverlapKey jobName
  AllowOverlap -> "arbiter_cron:" <> jobName <> ":" <> formatMinuteInTimezone tz tick

-- | The tick-independent key a 'SkipOverlap' schedule reuses, so at most one of
-- its jobs is ever active.
skipOverlapKey :: Text -> Text
skipOverlapKey jobName = "arbiter_cron:" <> jobName

-- | Compute the delay in microseconds until the next minute boundary,
-- clamped to @[0, 120_000_000]@.
computeDelayMicros :: UTCTime -> Int
computeDelayMicros now =
  let nextMinute = truncateToMinute (addUTCTime 60 now)
      delaySeconds = diffUTCTime nextMinute now
      rawMicros = ceiling (delaySeconds * 1_000_000) :: Int
   in max 0 (min 120_000_000 rawMicros)

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
isShuttingDown stateVar = do
  st <- readTVarIO stateVar
  pure $ case st of
    ShuttingDown -> True
    _ -> False

-- | Truncate a 'UTCTime' to the current minute (zero out seconds).
truncateToMinute :: UTCTime -> UTCTime
truncateToMinute t =
  let secs = utctDayTime t
      truncated = secondsToDiffTime (floor secs `div` 60 * 60)
   in t {utctDayTime = truncated}

-- | Format a 'UTCTime' as @YYYY-MM-DDTHH:MM@ for dedup key buckets.
formatMinute :: UTCTime -> Text
formatMinute = T.pack . formatTime defaultTimeLocale "%Y-%m-%dT%H:%M"

-- | Enumerate all minute-boundary times from @start@ through @end@ (inclusive).
-- Both @start@ and @end@ should be truncated to minute boundaries.
enumMinutes :: UTCTime -> UTCTime -> [UTCTime]
enumMinutes start end =
  unfoldr
    (\t -> if t > end then Nothing else Just (t, addUTCTime 60 t))
    start

{-# LANGUAGE OverloadedStrings #-}

-- | Cron scheduler for the Arbiter worker pool.
--
-- When 'cronJobs' is non-empty in 'WorkerConfig', 'runWorkerPool' spawns a
-- scheduler thread that inserts jobs on 5-field cron expressions.
-- Dedup keys prevent duplicate insertion across multiple worker instances.
--
-- __All cron expressions are evaluated in UTC.__ There is no local-timezone
-- support - @\"0 3 * * *\"@ means 03:00 UTC, not 03:00 in the server's
-- local time. Account for your timezone offset when writing expressions.
--
-- The scheduler consults the @cron_schedules@ table each tick for runtime
-- overrides (expression, overlap, enabled).
module Arbiter.Worker.Cron
  ( -- * Types
    CronJob (..)
  , OverlapPolicy (..)
  , BackfillPolicy (..)
  , TickKind (..)

    -- * Smart Constructor
  , cronJob

    -- * Helpers
  , overlapPolicyToText
  , overlapPolicyFromText

    -- * Internal
  , runCronScheduler
  , initCronSchedules
  , processCronCatchUp
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
import Arbiter.Core.Job.Types (DedupKey (IgnoreDuplicate), JobWrite, dedupKey)
import Arbiter.Core.MonadArbiter (MonadArbiter, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (forM_, forever, void, when)
import Control.Monad.IO.Class (MonadIO)
import Data.List (unfoldr)
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time
  ( NominalDiffTime
  , UTCTime (..)
  , addUTCTime
  , defaultTimeLocale
  , diffUTCTime
  , formatTime
  , getCurrentTime
  , secondsToDiffTime
  )
import GHC.Generics (Generic)
import System.Cron (CronSchedule, parseCronSchedule, scheduleMatches)
import UnliftIO (MonadUnliftIO, liftIO, tryAny)
import UnliftIO.Concurrent (threadDelay)

import Arbiter.Worker.Logger (LogConfig, LogLevel (..))
import Arbiter.Worker.Logger.Internal (logMessage)

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

-- | A cron schedule definition.
--
-- Use 'cronJob' to construct - it parses the cron expression eagerly,
-- so invalid expressions are caught at construction time.
data CronJob payload = CronJob
  { name :: Text
  -- ^ Human-readable name for logging and dedup keys
  , cronExpression :: Text
  -- ^ Original cron expression text (for DB storage)
  , schedule :: CronSchedule
  -- ^ Parsed cron schedule (internal)
  , overlap :: OverlapPolicy
  -- ^ How to handle overlapping ticks
  , backfill :: BackfillPolicy
  -- ^ How to replay missed ticks, at startup and mid-flight. Default: 'NoBackfill'.
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
--          { priority = case kind of Replay -> 10; Live -> 0 })
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
    Right sched ->
      Right
        CronJob
          { name = cronName
          , cronExpression = expr
          , schedule = sched
          , overlap = ov
          , backfill = NoBackfill
          , builder = mk
          }

-- | Upsert default expression and overlap for each 'CronJob' into the
-- @cron_schedules@ table. Preserves any user overrides and enabled state.
initCronSchedules
  :: (MonadArbiter m)
  => Text -> [CronJob payload] -> LogConfig -> m ()
initCronSchedules schemaName jobs logCfg = do
  forM_ jobs $ \cj ->
    Ops.upsertCronDefault
      schemaName
      (name cj)
      (cronExpression cj)
      (overlapPolicyToText (overlap cj))
  logCron logCfg Info $
    "Cron schedules initialized: " <> T.pack (show (length jobs)) <> " schedule(s) upserted"

-- | Scheduler entry point: upsert defaults, run one catch-up pass at startup,
-- then loop on minute boundaries.
runCronScheduler
  :: (MonadUnliftIO m, QueueOperation m registry payload)
  => LogConfig
  -> Text
  -> [CronJob payload]
  -> m ()
runCronScheduler logCfg schemaName jobs = do
  initCronSchedules schemaName jobs logCfg
  -- Startup catch-up is the same code path as mid-flight. 'BackfillPolicy'
  -- bounds the window. 'NoBackfill' processes only the current minute.
  startupNow <- liftIO getCurrentTime
  processCronCatchUp logCfg schemaName jobs startupNow
  logCron logCfg Info $ "Cron scheduler started with " <> T.pack (show (length jobs)) <> " schedule(s)"
  forever $ do
    waitUntilNextMinute
    now <- liftIO getCurrentTime
    processCronCatchUp logCfg schemaName jobs now

-- | Scheduler catch-up step. Fires every matching minute in the
-- 'BackfillPolicy' window. The watermark advances to @currentTick@ after
-- processing regardless of which inserts succeeded. Failed inserts are
-- logged but not retried (retrying would risk re-firing sibling ticks
-- whose jobs have already been processed).
processCronCatchUp
  :: (MonadUnliftIO m, QueueOperation m registry payload)
  => LogConfig
  -> Text
  -> [CronJob payload]
  -> UTCTime
  -- ^ Current wall-clock time
  -> m ()
processCronCatchUp logCfg schemaName jobs now = do
  (rowMap, dbFetchOk) <- fetchScheduleRows logCfg schemaName
  let currentTick = truncateToMinute now
  forM_ jobs (processOne rowMap currentTick)
  when (dbFetchOk && not (null jobs)) $
    tryOrLog logCfg "Failed to update last_checked_at" $
      Ops.touchCronChecked schemaName currentTick (map name jobs)
  where
    processOne rowMap currentTick cj = do
      let mRow = lookup (name cj) rowMap
      case resolveAndParse cj mRow of
        Disabled -> pure ()
        ParseError expr err ->
          logCron logCfg Error $
            "Cron schedule '"
              <> name cj
              <> "' has invalid effective expression '"
              <> expr
              <> "': "
              <> T.pack err
        Effective effectiveOv sched -> do
          let ticksInWindow = enumerateCatchUpTicks (backfill cj) (mRow >>= CS.lastCheckedAt) currentTick
              ticksToFire = pickTicksToFire sched effectiveOv ticksInWindow
              replayCount = length (filter (/= currentTick) ticksToFire)
          when (replayCount > 0) $
            logCron logCfg Info $
              "Replaying " <> T.pack (show replayCount) <> " missed tick(s) for '" <> name cj <> "'"
          forM_ ticksToFire $ \t ->
            void $ tryInsertCronJob logCfg schemaName cj effectiveOv (tickKindFor currentTick t) t

-- | 'Live' for @currentTick@, 'Replay' otherwise.
tickKindFor :: UTCTime -> UTCTime -> TickKind
tickKindFor currentTick t = if t == currentTick then Live else Replay

-- | 'SkipOverlap' keeps the most recent match. 'AllowOverlap' keeps all.
pickTicksToFire :: CronSchedule -> OverlapPolicy -> [UTCTime] -> [UTCTime]
pickTicksToFire sched ov ticks =
  let matching = filter (scheduleMatches sched) ticks
   in case ov of
        SkipOverlap -> take 1 (reverse matching)
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

-- | Fetch schedule rows from DB, falling back to empty on error.
fetchScheduleRows
  :: (MonadArbiter m, MonadUnliftIO m)
  => LogConfig -> Text -> m ([(Text, CS.CronScheduleRow)], Bool)
fetchScheduleRows logCfg schemaName = do
  result <- tryAny $ Ops.listCronSchedules schemaName
  case result of
    Right rows -> pure ([(CS.name r, r) | r <- rows], True)
    Left e -> do
      logCron logCfg Error $ "Failed to fetch cron schedules from DB, using code defaults: " <> T.pack (show e)
      pure ([], False)

-- | Result of resolving a schedule's effective config (code defaults +
-- runtime DB overrides).
data Resolved
  = Disabled
  | ParseError Text String
  | Effective OverlapPolicy CronSchedule

resolveAndParse :: CronJob payload -> Maybe CS.CronScheduleRow -> Resolved
resolveAndParse cj mRow =
  let (expr, ov, isEnabled) = case mRow of
        Nothing -> (cronExpression cj, overlap cj, True)
        Just row@CS.CronScheduleRow {CS.enabled = rowEnabled} ->
          ( CS.effectiveExpression row
          , fromMaybe (overlap cj) (overlapPolicyFromText (CS.effectiveOverlap row))
          , rowEnabled
          )
   in if isEnabled
        then case parseCronSchedule expr of
          Right sched -> Effective ov sched
          Left err -> ParseError expr err
        else Disabled

-- | Attempt to insert a single cron-tick job. Returns 'False' on any failure
-- (logged), 'True' on successful insert or a dedup-blocked no-op.
--
-- The insert and the per-tick @last_checked_at@ advance are atomic. If
-- either fails the other rolls back, so a successful fire is always paired
-- with a watermark advance to that tick.
tryInsertCronJob
  :: (MonadUnliftIO m, QueueOperation m registry payload)
  => LogConfig -> Text -> CronJob payload -> OverlapPolicy -> TickKind -> UTCTime -> m Bool
tryInsertCronJob logCfg schemaName cj effectiveOv kind tick = do
  result <- tryAny . withDbTransaction $ do
    let key = makeDedupKeyFromParts (name cj) effectiveOv tick
        jobWrite = (builder cj kind tick) {dedupKey = Just (IgnoreDuplicate key)}
    mJob <- HL.insertJob jobWrite
    case mJob of
      Just _ -> void $ Ops.touchCronLastFired schemaName (name cj)
      Nothing -> pure ()
    void $ Ops.touchCronChecked schemaName tick [name cj]
  case result of
    Left e -> do
      logCron logCfg Error $ "Cron schedule '" <> name cj <> "' failed to insert: " <> T.pack (show e)
      pure False
    Right () -> do
      logCron logCfg Debug $ "Cron schedule '" <> name cj <> "' processed at " <> formatMinute tick
      pure True

-- | Log a cron message.
logCron :: (MonadIO m) => LogConfig -> LogLevel -> Text -> m ()
logCron logCfg level msg = liftIO $ logMessage logCfg level msg

-- | Try an action, logging errors without re-throwing.
tryOrLog :: (MonadUnliftIO m) => LogConfig -> Text -> m a -> m ()
tryOrLog logCfg prefix action = do
  result <- tryAny action
  case result of
    Right _ -> pure ()
    Left e -> logCron logCfg Error $ prefix <> ": " <> T.pack (show e)

-- | Compute the dedup key for a cron job using the code-defined overlap policy.
makeDedupKey :: CronJob payload -> UTCTime -> Text
makeDedupKey cj tick = makeDedupKeyFromParts (name cj) (overlap cj) tick

-- | Compute dedup key from overlap policy, schedule name, and tick time.
makeDedupKeyFromParts :: Text -> OverlapPolicy -> UTCTime -> Text
makeDedupKeyFromParts jobName ov tick = case ov of
  SkipOverlap -> "arbiter_cron:" <> jobName
  AllowOverlap -> "arbiter_cron:" <> jobName <> ":" <> formatMinute tick

-- | Compute the delay in microseconds until the next minute boundary,
-- clamped to @[0, 120_000_000]@.
computeDelayMicros :: UTCTime -> Int
computeDelayMicros now =
  let nextMinute = truncateToMinute (addUTCTime 60 now)
      delaySeconds = diffUTCTime nextMinute now
      rawMicros = ceiling (delaySeconds * 1_000_000) :: Int
   in max 0 (min 120_000_000 rawMicros)

-- | Sleep until the next minute boundary (:00 seconds).
waitUntilNextMinute :: (MonadIO m) => m ()
waitUntilNextMinute = liftIO $ do
  now <- getCurrentTime
  threadDelay (computeDelayMicros now)

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

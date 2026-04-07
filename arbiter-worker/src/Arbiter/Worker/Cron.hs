{-# LANGUAGE OverloadedStrings #-}

-- | Cron scheduler for the Arbiter worker pool.
--
-- When 'cronJobs' is non-empty in 'WorkerConfig', 'runWorkerPool' spawns a
-- scheduler thread that inserts jobs on 5-field cron expressions.
-- Dedup keys prevent duplicate insertion across multiple worker instances.
--
-- __All cron expressions are evaluated in UTC.__ There is no local-timezone
-- support — @\"0 3 * * *\"@ means 03:00 UTC, not 03:00 in the server's
-- local time. Account for your timezone offset when writing expressions.
--
-- The scheduler consults the @cron_schedules@ table each tick for runtime
-- overrides (expression, overlap, enabled).
module Arbiter.Worker.Cron
  ( -- * Types
    CronJob (..)
  , OverlapPolicy (..)
  , BackfillPolicy (..)

    -- * Smart Constructor
  , cronJob

    -- * Helpers
  , overlapPolicyToText
  , overlapPolicyFromText

    -- * DB Init
  , initCronSchedules

    -- * Internal
  , runCronScheduler
  , processCronTick
  , backfillMissedTicks
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
import Control.Monad (forM_, forever, unless, void, when)
import Control.Monad.IO.Class (MonadIO)
import Data.Foldable (fold)
import Data.List (unfoldr)
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
  = -- | At most one pending or running job per schedule name.
    SkipOverlap
  | -- | One job per tick. Allows concurrent execution of prior ticks.
    AllowOverlap
  deriving stock (Eq, Generic, Show)

-- | Controls whether missed ticks are backfilled on startup.
--
-- On start (or restart), the scheduler reads @last_checked_at@ from the
-- @cron_schedules@ table to find missed ticks within the backfill window,
-- then inserts them in chronological order. Dedup keys prevent duplicates.
--
-- With 'SkipOverlap', only the most recent missed tick is inserted
-- (the dedup key is time-independent). With 'AllowOverlap', each missed
-- tick produces its own job.
data BackfillPolicy
  = -- | Do not backfill missed ticks. Default.
    NoBackfill
  | -- | Backfill missed ticks up to the given duration.
    --
    -- Example: @Backfill 86400@ backfills up to 24 hours of missed ticks.
    Backfill NominalDiffTime
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
-- Use 'cronJob' to construct — it parses the cron expression eagerly,
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
  -- ^ Whether to backfill missed ticks on startup. Default: 'NoBackfill'.
  , builder :: Bool -> UTCTime -> JobWrite payload
  -- ^ Build a job for the given tick time. The 'Bool' is 'True' when the
  -- job is being backfilled (missed tick), 'False' for live ticks.
  }
  deriving stock (Generic)

-- | Smart constructor for 'CronJob'. Parses the cron expression eagerly.
--
-- Returns @Left@ with an error message if the cron expression is invalid.
--
-- @
-- cronJob "nightly-report" "0 3 * * *" SkipOverlap
--   (\\_backfill _tick -> defaultJob (GenerateReport "nightly"))
-- @
--
-- The builder receives a backfill flag ('True' for missed ticks being
-- replayed on startup, 'False' for live ticks) and the tick time.
--
-- To enable backfill, set it via record update:
--
-- @
-- let Right cj = cronJob "nightly-report" "0 3 * * *" AllowOverlap
--       (\\isBackfill tick -> (defaultJob (GenerateReport tick))
--          { priority = if isBackfill then 10 else 0 })
-- in cj { backfill = Backfill 86400 }  -- backfill up to 24 hours
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
  -> (Bool -> UTCTime -> JobWrite payload)
  -- ^ Job builder. Receives backfill flag and tick time.
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

-- | Scheduler entry point: upsert defaults, backfill, then loop on minute boundaries.
runCronScheduler
  :: (MonadUnliftIO m, QueueOperation m registry payload)
  => LogConfig
  -> Text
  -> [CronJob payload]
  -> m ()
runCronScheduler logCfg schemaName jobs = do
  initCronSchedules schemaName jobs logCfg
  backfillMissedTicks logCfg schemaName jobs
  logCron logCfg Info $ "Cron scheduler started with " <> T.pack (show (length jobs)) <> " schedule(s)"
  forever $ do
    waitUntilNextMinute
    now <- liftIO getCurrentTime
    processCronTick False logCfg schemaName jobs (truncateToMinute now)

-- | Backfill missed ticks for schedules with a 'Backfill' policy.
--
-- Reads @last_checked_at@ from the @cron_schedules@ table, enumerates
-- missed ticks within the backfill window, and inserts them atomically
-- alongside a @last_checked_at@ checkpoint.
backfillMissedTicks
  :: (MonadUnliftIO m, QueueOperation m registry payload)
  => LogConfig
  -> Text
  -> [CronJob payload]
  -> m ()
backfillMissedTicks logCfg schemaName jobs = do
  let backfillJobs = [(cj, w) | cj <- jobs, Backfill w <- [backfill cj]]
  unless (null backfillJobs) $ do
    result <- tryAny $ Ops.listCronSchedules schemaName
    case result of
      Left e -> logCron logCfg Error $ "Backfill: failed to read schedules: " <> T.pack (show e)
      Right rows -> do
        now <- liftIO getCurrentTime
        let currentTick = truncateToMinute now
            rowMap = [(CS.name r, r) | r <- rows]
            plan = concatMap (planBackfill rowMap now currentTick) backfillJobs
        unless (null plan) $ do
          forM_ plan $ \(cj, _, ticks) ->
            logCron logCfg Info $
              "Backfilling "
                <> T.pack (show (length ticks))
                <> " missed tick(s) for '"
                <> name cj
                <> "'"
          let scheduleNames = [name cj | (cj, _, _) <- plan]
          tryLog logCfg "Backfill failed" $ withDbTransaction $ do
            void $ Ops.touchCronChecked schemaName scheduleNames
            forM_ plan $ \(cj, effectiveOv, ticks) ->
              forM_ ticks $ \tick ->
                insertCronJob True schemaName cj effectiveOv tick

-- | Compute which ticks need backfilling for a single schedule.
planBackfill
  :: [(Text, CS.CronScheduleRow)]
  -> UTCTime
  -> UTCTime
  -> (CronJob payload, NominalDiffTime)
  -> [(CronJob payload, OverlapPolicy, [UTCTime])]
planBackfill rowMap now currentTick (cj, window) = fold $ do
  row <- lookup (name cj) rowMap
  lastChecked <- CS.lastCheckedAt row
  case resolveAndParse cj (Just row) of
    Resolved effectiveOv sched ->
      let earliest = truncateToMinute (addUTCTime (negate window) now)
          start = max (truncateToMinute lastChecked) earliest
          allTicks = enumMinutes (addUTCTime 60 start) currentTick
          matching = filter (scheduleMatches sched) allTicks
          ticks = case effectiveOv of
            SkipOverlap -> take 1 (reverse matching)
            AllowOverlap -> matching
       in pure [(cj, effectiveOv, ticks) | not (null ticks)]
    _ -> pure []

-- | Process a single cron tick at the given time.
--
-- For each 'CronJob', consults the DB for effective expression, overlap, and enabled.
-- If the schedule is disabled, it is skipped. If the effective expression fails to parse,
-- it is logged and skipped.
processCronTick
  :: (MonadUnliftIO m, QueueOperation m registry payload)
  => Bool
  -- ^ Whether this is a backfill tick
  -> LogConfig
  -> Text
  -> [CronJob payload]
  -> UTCTime
  -> m ()
processCronTick isBackfill logCfg schemaName jobs tick = do
  (rowMap, dbFetchOk) <- fetchScheduleRows logCfg schemaName

  forM_ jobs $ \cj ->
    case resolveAndParse cj (lookup (name cj) rowMap) of
      Disabled -> pure ()
      ParseError expr err ->
        logCron logCfg Error $
          "Cron schedule '"
            <> name cj
            <> "' has invalid effective expression '"
            <> expr
            <> "': "
            <> T.pack err
      Resolved effectiveOv effectiveSched ->
        when (scheduleMatches effectiveSched tick) $
          tryInsertCronJob isBackfill logCfg schemaName cj effectiveOv tick

  when dbFetchOk $
    tryLog logCfg "Failed to update last_checked_at" $
      Ops.touchCronChecked schemaName (map name jobs)

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

-- | Result of resolving a schedule's effective config.
data Resolved
  = Disabled
  | ParseError Text String
  | Resolved OverlapPolicy CronSchedule

-- | Resolve effective config from code defaults + DB override and parse the expression.
resolveAndParse :: CronJob payload -> Maybe CS.CronScheduleRow -> Resolved
resolveAndParse cj mRow =
  let (expr, ov, isEnabled) = case mRow of
        Nothing -> (cronExpression cj, overlap cj, True)
        Just row@CS.CronScheduleRow {CS.enabled = rowEnabled} ->
          ( CS.effectiveExpression row
          , maybe (overlap cj) id (overlapPolicyFromText (CS.effectiveOverlap row))
          , rowEnabled
          )
   in if not isEnabled
        then Disabled
        else case parseCronSchedule expr of
          Right sched -> Resolved ov sched
          Left err -> ParseError expr err

insertCronJob
  :: (QueueOperation m registry payload)
  => Bool -> Text -> CronJob payload -> OverlapPolicy -> UTCTime -> m ()
insertCronJob isBackfill schemaName cj effectiveOv tick = withDbTransaction $ do
  let key = makeDedupKeyFromParts (name cj) effectiveOv tick
      jobWrite = (builder cj isBackfill tick) {dedupKey = Just (IgnoreDuplicate key)}
  mJob <- HL.insertJob jobWrite
  case mJob of
    Just _ -> void $ Ops.touchCronLastFired schemaName (name cj)
    Nothing -> pure ()

tryInsertCronJob
  :: (MonadUnliftIO m, QueueOperation m registry payload)
  => Bool -> LogConfig -> Text -> CronJob payload -> OverlapPolicy -> UTCTime -> m ()
tryInsertCronJob isBackfill logCfg schemaName cj effectiveOv tick = do
  result <- tryAny $ insertCronJob isBackfill schemaName cj effectiveOv tick
  case result of
    Left e ->
      logCron logCfg Error $ "Cron schedule '" <> name cj <> "' failed to insert: " <> T.pack (show e)
    Right () ->
      logCron logCfg Debug $ "Cron schedule '" <> name cj <> "' processed at " <> formatMinute tick

-- | Log a cron message.
logCron :: (MonadIO m) => LogConfig -> LogLevel -> Text -> m ()
logCron logCfg level msg = liftIO $ logMessage logCfg level msg

-- | Try an action, logging errors without re-throwing.
tryLog :: (MonadUnliftIO m) => LogConfig -> Text -> m a -> m ()
tryLog logCfg prefix action = do
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

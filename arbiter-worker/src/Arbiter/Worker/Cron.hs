{-# LANGUAGE OverloadedStrings #-}

-- | Cron scheduler for the Arbiter worker pool.
--
-- When 'cronJobs' is non-empty in 'WorkerConfig', 'runWorkerPool' spawns an
-- additional thread that inserts jobs based on 5-field cron expressions.
-- Uses 'IgnoreDuplicate' dedup keys for multi-instance idempotency.
--
-- __All cron expressions are evaluated in UTC.__ There is no local-timezone
-- support — @\"0 3 * * *\"@ means 03:00 UTC, not 03:00 in the server's
-- local time. Account for your timezone offset when writing expressions.
--
-- When a connection pool and schema are provided, the scheduler consults the
-- @cron_schedules@ table for runtime overrides (expression, overlap, enabled).
module Arbiter.Worker.Cron
  ( -- * Types
    CronJob (..)
  , OverlapPolicy (..)

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
  , truncateToMinute
  , formatMinute
  , makeDedupKey
  , computeDelayMicros
  ) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (DedupKey (IgnoreDuplicate), JobWrite, dedupKey)
import Control.Monad (forM_, forever, when)
import Control.Monad.IO.Class (MonadIO)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time
  ( UTCTime (..)
  , defaultTimeLocale
  , diffUTCTime
  , formatTime
  , getCurrentTime
  , secondsToDiffTime
  )
import Database.PostgreSQL.Simple (Connection)
import GHC.Generics (Generic)
import System.Cron (CronSchedule, parseCronSchedule, scheduleMatches)
import UnliftIO (MonadUnliftIO, liftIO, tryAny)
import UnliftIO.Concurrent (threadDelay)

import Arbiter.Worker.Logger (LogConfig, LogLevel (..))
import Arbiter.Worker.Logger.Internal (logMessage)

-- | Determines how overlapping cron ticks are deduplicated.
data OverlapPolicy
  = -- | At most one pending or running job per schedule name.
    SkipOverlap
  | -- | One job per tick. Allows concurrent execution of prior ticks.
    AllowOverlap
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
  , builder :: UTCTime -> JobWrite payload
  -- ^ Build a job for the given tick time (truncated to minute)
  }
  deriving stock (Generic)

-- | Smart constructor for 'CronJob'. Parses the cron expression eagerly.
--
-- Returns @Left@ with an error message if the cron expression is invalid.
--
-- Note: cron expressions are evaluated in __UTC__. @\"0 3 * * *\"@ fires at
-- 03:00 UTC regardless of the server's local timezone.
--
-- Example:
--
-- @
-- cronJob "nightly-report" "0 3 * * *" SkipOverlap
--   (\\_ -> defaultJob (GenerateReport "nightly"))
-- @
cronJob
  :: Text
  -- ^ Schedule name (used in dedup keys and logging)
  -> Text
  -- ^ Cron expression (5-field: minute hour day-of-month month day-of-week)
  -> OverlapPolicy
  -> (UTCTime -> JobWrite payload)
  -> Either String (CronJob payload)
cronJob name expr overlap mk =
  case parseCronSchedule expr of
    Left err -> Left err
    Right schedule ->
      Right
        CronJob
          { name = name
          , cronExpression = expr
          , schedule = schedule
          , overlap = overlap
          , builder = mk
          }

-- | Initialize the @cron_schedules@ table and upsert defaults for all cron jobs.
--
-- Called once at scheduler startup. Upserts default_expression and
-- default_overlap for each 'CronJob', preserving any user overrides and
-- enabled state.
initCronSchedules :: Connection -> Text -> [CronJob payload] -> LogConfig -> IO ()
initCronSchedules conn schemaName jobs logCfg = do
  forM_ jobs $ \cj ->
    CS.upsertCronScheduleDefault
      conn
      schemaName
      (name cj)
      (cronExpression cj)
      (overlapPolicyToText (overlap cj))
  logMessage logCfg Info $ "Cron schedules initialized: " <> T.pack (show (length jobs)) <> " schedule(s) upserted"

-- | Run the cron scheduler loop. Called by 'runWorkerPool' when 'cronJobs'
-- is non-empty.
--
-- On each minute boundary, checks all schedules and inserts matching jobs
-- with appropriate dedup keys. Exceptions per-schedule are caught and logged;
-- the loop continues.
--
-- The provided 'Connection' is used to consult the @cron_schedules@ table
-- for effective expression, overlap, and enabled state on each tick.
runCronScheduler
  :: (MonadUnliftIO m, QueueOperation m registry payload)
  => Connection
  -> LogConfig
  -> Text
  -> [CronJob payload]
  -> m ()
runCronScheduler conn logCfg schemaName jobs = do
  liftIO $ initCronSchedules conn schemaName jobs logCfg
  liftIO $ logMessage logCfg Info $ "Cron scheduler started with " <> T.pack (show (length jobs)) <> " schedule(s)"
  forever $ do
    waitUntilNextMinute
    now <- liftIO getCurrentTime
    processCronTick conn logCfg schemaName jobs (truncateToMinute now)

-- | Process a single cron tick at the given time.
--
-- For each 'CronJob', consults the DB for effective expression, overlap, and enabled.
-- If the schedule is disabled, it is skipped. If the effective expression fails to parse,
-- it is logged and skipped.
processCronTick
  :: (MonadUnliftIO m, QueueOperation m registry payload)
  => Connection
  -> LogConfig
  -> Text
  -> [CronJob payload]
  -> UTCTime
  -> m ()
processCronTick conn logCfg schemaName jobs tick = do
  -- Batch-fetch all schedule rows once; fall back to code defaults on DB error
  rowMap <- do
    result <- tryAny . liftIO $ CS.listCronSchedules conn schemaName
    case result of
      Right rows -> pure [(CS.name r, r) | r <- rows]
      Left e -> do
        liftIO $
          logMessage logCfg Error $
            "Failed to fetch cron schedules from DB, using code defaults: "
              <> T.pack (show e)
        pure []

  forM_ jobs $ \cj -> do
    let mRow = lookup (name cj) rowMap

    let (effectiveExpr, effectiveOv, isEnabled) = case mRow of
          Nothing ->
            -- No DB row yet (shouldn't happen after init, but be safe)
            (cronExpression cj, overlap cj, True)
          Just row@CS.CronScheduleRow {CS.enabled = rowEnabled} ->
            let expr = CS.effectiveExpression row
                ovText = CS.effectiveOverlap row
                ov = case overlapPolicyFromText ovText of
                  Just p -> p
                  Nothing -> overlap cj -- fallback to code default
             in (expr, ov, rowEnabled)

    when isEnabled $ do
      -- Parse the effective expression
      case parseCronSchedule effectiveExpr of
        Left err ->
          liftIO $
            logMessage logCfg Error $
              "Cron schedule '"
                <> name cj
                <> "' has invalid effective expression '"
                <> effectiveExpr
                <> "': "
                <> T.pack err
        Right effectiveSched ->
          when (scheduleMatches effectiveSched tick) $ do
            let key = makeDedupKeyFromParts (name cj) effectiveOv tick
                jobWrite = (builder cj tick) {dedupKey = Just (IgnoreDuplicate key)}
            result <- tryAny $ HL.insertJob jobWrite
            liftIO $ case result of
              Left e ->
                logMessage logCfg Error $
                  "Cron schedule '"
                    <> name cj
                    <> "' failed to insert: "
                    <> T.pack (show e)
              Right Nothing ->
                logMessage logCfg Debug $
                  "Cron schedule '"
                    <> name cj
                    <> "' skipped (dedup key exists): "
                    <> key
              Right (Just _) -> do
                logMessage logCfg Info $
                  "Cron schedule '"
                    <> name cj
                    <> "' fired at "
                    <> formatMinute tick
                -- Update last_fired_at
                fireResult <- tryAny . liftIO $ CS.touchCronScheduleLastFired conn schemaName (name cj)
                case fireResult of
                  Right () -> pure ()
                  Left e ->
                    liftIO $
                      logMessage logCfg Error $
                        "Cron schedule '"
                          <> name cj
                          <> "' failed to update last_fired_at: "
                          <> T.pack (show e)

  -- Mark all schedules as checked
  result <- tryAny . liftIO $ CS.touchCronSchedulesChecked conn schemaName (map name jobs)
  case result of
    Right () -> pure ()
    Left e ->
      liftIO $
        logMessage logCfg Error $
          "Failed to update last_checked_at: " <> T.pack (show e)

-- | Compute the dedup key for a cron job at the given tick time.
--
-- Uses the code-defined overlap policy. If the schedule has a DB override,
-- use 'makeDedupKeyFromParts' with the effective policy instead.
makeDedupKey :: CronJob payload -> UTCTime -> Text
makeDedupKey cj tick = makeDedupKeyFromParts (name cj) (overlap cj) tick

-- | Internal helper: compute dedup key from components.
makeDedupKeyFromParts :: Text -> OverlapPolicy -> UTCTime -> Text
makeDedupKeyFromParts jobName ov tick = case ov of
  SkipOverlap -> "arbiter_cron:" <> jobName
  AllowOverlap -> "arbiter_cron:" <> jobName <> ":" <> formatMinute tick

-- | Compute the delay in microseconds until the next minute boundary,
-- clamped to @[0, 120_000_000]@.
computeDelayMicros :: UTCTime -> Int
computeDelayMicros now =
  let nextMinute = truncateToMinute now {utctDayTime = utctDayTime now + 60}
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

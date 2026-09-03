{-# LANGUAGE OverloadedStrings #-}

-- | Cron declarations, validation, and timezone calculations.
module Arbiter.Worker.Cron.Types
  ( CronJob (..)
  , OverlapPolicy (..)
  , BackfillPolicy (..)
  , TickKind (..)
  , cronJob
  , cronJobInTimezone
  , overlapPolicyToText
  , overlapPolicyFromText
  , validateCronScheduleUpdate
  , updateCronScheduleChecked
  , resolveTZ
  , matchesInTimezone
  , nextRunInTimezone
  , nextRunFromExpression
  , formatMinuteInTimezone
  , truncateToMinute
  , formatMinute
  , enumMinutes
  ) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (JobWrite)
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Control.Applicative ((<|>))
import Control.Monad (join, unless)
import Data.Either (isRight)
import Data.Int (Int64)
import Data.List (find, unfoldr)
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding (encodeUtf8)
import Data.Time
  ( LocalTime
  , NominalDiffTime
  , UTCTime (..)
  , addUTCTime
  , defaultTimeLocale
  , formatTime
  , localTimeToUTC
  , secondsToDiffTime
  , utc
  , utcToLocalTime
  )
import Data.Time.Zones (LocalToUTCResult (..), TZ, localTimeToUTCFull, utcToLocalTimeTZ)
import Data.Time.Zones.All (fromTZName, tzByLabel)
import GHC.Generics (Generic)
import System.Cron (CronSchedule, nextMatch, parseCronSchedule, scheduleMatches)

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
    check field ok message = unless (all ok (join field)) (Left message)

-- | 'Arbiter.Core.HighLevel.updateCronScheduleUnchecked' behind
-- 'validateCronScheduleUpdate'. Returns rows affected (0 = not found).
updateCronScheduleChecked
  :: (MonadArbiter m)
  => Text
  -> CS.CronScheduleUpdate
  -> m (Either Text Int64)
updateCronScheduleChecked scheduleName upd =
  traverse (const (HL.updateCronScheduleUnchecked scheduleName upd)) (validateCronScheduleUpdate upd)

-- | A cron schedule. Built with 'cronJob', or 'cronJobInTimezone' for a non-UTC one.
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

-- | Build a 'CronJob', parsing the expression eagerly and returning @Left@ on a bad one.
-- The expression is evaluated in UTC, whatever the server's own timezone.
-- 'cronJobInTimezone' is the local-time form, and 'backfill' is set by record update.
--
-- @
-- cronJob "nightly-report" "0 3 * * *" SkipOverlap
--   (\\_kind _tick -> defaultJob (GenerateReport "nightly"))
-- @
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
  CronJob
    { name = cronName
    , cronExpression = expr
    , overlap = ov
    , backfill = NoBackfill
    , timezone = Nothing
    , builder = mk
    }
    <$ parseCronSchedule expr

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

-- | The first tick after @now@ that @sched@ matches, evaluated in @tz@.
-- 'Nothing' means UTC. An unknown tz name returns 'Nothing'.
--
-- A replayed local minute is reported, though its insert is deduped while the earlier run
-- is still live.
nextRunInTimezone :: Maybe Text -> CronSchedule -> UTCTime -> Maybe UTCTime
nextRunInTimezone Nothing sched now = nextMatch sched now
nextRunInTimezone (Just tzName) sched now = do
  tz <- resolveTZ tzName
  replayed tz <|> seek tz (localTimeToUTC utc (utcToLocalTimeTZ tz now))
  where
    -- A replayed minute runs ahead of @now@ in UTC while reading behind it locally.
    replayed tz = do
      endsAt <- replayEnd tz now
      find (matchesInTimezone (Just tzName) sched) (enumMinutes (addUTCTime 60 (truncateToMinute now)) endsAt)
    seek tz from = do
      localMinute <- nextMatch sched from
      find (> now) (ticksWearing tz (utcToLocalTime utc localMinute)) <|> seek tz localMinute

-- | When @t@ is in the first pass of a repeated local hour, when that hour reads again.
replayEnd :: TZ -> UTCTime -> Maybe UTCTime
replayEnd tz t = case localTimeToUTCFull tz (utcToLocalTimeTZ tz t) of
  LTUAmbiguous _ second _ _ | t < second -> Just second
  _ -> Nothing

-- | Every UTC tick whose local clock in @tz@ reads @local@, earliest first.
ticksWearing :: TZ -> LocalTime -> [UTCTime]
ticksWearing tz local = case localTimeToUTCFull tz local of
  LTUUnique tick _ -> [tick]
  LTUAmbiguous first second _ _ -> [first, second]
  LTUNone _ _ -> []

-- | \'nextRunInTimezone\' over an unparsed expression. A bad one returns \'Nothing\'.
nextRunFromExpression :: Maybe Text -> Text -> UTCTime -> Maybe UTCTime
nextRunFromExpression tzName expr now =
  either (const Nothing) (\sched -> nextRunInTimezone tzName sched now) (parseCronSchedule expr)

-- | Format a UTC tick as @YYYY-MM-DDTHH:MM@ in the given timezone.
-- DST fall-back maps two UTC instants to the same local minute, so dedup
-- keys built from this collapse to a single fire.
formatMinuteInTimezone :: Maybe Text -> UTCTime -> Text
formatMinuteInTimezone tzName t =
  maybe (formatMinute t) localMinute (tzName >>= resolveTZ)
  where
    localMinute tz = T.pack (formatTime defaultTimeLocale "%Y-%m-%dT%H:%M" (utcToLocalTimeTZ tz t))

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

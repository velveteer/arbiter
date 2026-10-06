{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module Test.Arbiter.Worker.Cron (spec) where

import Arbiter.Core.Job.Types (defaultJob)
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Setup (mkTime)
import Data.List (find)
import Data.Time
  ( UTCTime (..)
  , addUTCTime
  , fromGregorian
  )
import System.Cron (parseCronSchedule)
import Test.Hspec
  ( Spec
  , describe
  , expectationFailure
  , it
  , shouldBe
  , shouldNotBe
  , shouldSatisfy
  )

import Arbiter.Worker.Cron
  ( BackfillPolicy (..)
  , CronJob (..)
  , OverlapPolicy (..)
  , computeDelayMicros
  , cronJob
  , cronJobInTimezone
  , enumMinutes
  , enumerateCatchUpTicks
  , formatMinute
  , matchesInTimezone
  , mkDedupKeyFromParts
  , nextRunInTimezone
  , resolveTZ
  , truncateToMinute
  )

spec :: Spec
spec = do
  -- Pure unit tests (no DB needed)
  describe "cronJob smart constructor" $ do
    it "accepts a valid 5-field cron expression" $ do
      let result = cronJob "test" "0 3 * * *" SkipOverlap (\_ _ -> defaultJob (SimpleTask "x"))
      case result of
        Right _ -> pure ()
        Left err -> expectationFailure $ "Expected Right, got Left: " <> show err

    it "rejects an invalid cron expression" $ do
      let result = cronJob "test" "bad cron" SkipOverlap (\_ _ -> defaultJob (SimpleTask "x"))
      case result of
        Left _ -> pure ()
        Right _ -> expectationFailure "Expected Left (parse error), got Right"

    it "accepts every-minute expression" $ do
      let result = cronJob "test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "x"))
      case result of
        Right _ -> pure ()
        Left err -> expectationFailure $ "Expected Right, got Left: " <> show err

  describe "truncateToMinute" $ do
    it "zeroes out seconds" $ do
      let input = mkTime 2025 6 15 12 34 56
          expected = mkTime 2025 6 15 12 34 0
      truncateToMinute input `shouldBe` expected

    it "preserves an already-truncated time" $ do
      let input = mkTime 2025 6 15 12 34 0
      truncateToMinute input `shouldBe` input

    it "handles sub-second precision (fractional seconds become 0)" $ do
      let day = fromGregorian 2025 6 15
          -- 12:34:56.789
          secs = 12 * 3600 + 34 * 60 + 56.789
          input = UTCTime day secs
          expected = mkTime 2025 6 15 12 34 0
      truncateToMinute input `shouldBe` expected

  describe "formatMinute" $ do
    it "produces YYYY-MM-DDTHH:MM format" $ do
      let tick = mkTime 2025 1 9 8 5 0
      formatMinute tick `shouldBe` "2025-01-09T08:05"

  describe "mkDedupKeyFromParts" $ do
    it "SkipOverlap produces arbiter_cron:<name> (no time)" $ do
      let Right cron = cronJob "nightly" "0 3 * * *" SkipOverlap (\_ _ -> defaultJob (SimpleTask "x"))
          tick = mkTime 2025 6 15 3 0 0
      mkDedupKeyFromParts (name cron) (overlap cron) tick `shouldBe` "arbiter_cron:nightly"

    it "AllowOverlap produces arbiter_cron:<name>:<utc minute>" $ do
      let Right cron = cronJob "nightly" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "x"))
          tick = mkTime 2025 6 15 3 0 0
      mkDedupKeyFromParts (name cron) (overlap cron) tick `shouldBe` "arbiter_cron:nightly:2025-06-15T03:00"

  describe "timezone handling" $ do
    it "cronJobInTimezone rejects an unknown Olson name" $ do
      let result =
            cronJobInTimezone
              "test"
              "Made/Up_Zone"
              "0 3 * * *"
              SkipOverlap
              (\_ _ -> defaultJob (SimpleTask "x"))
      case result of
        Left _ -> pure ()
        Right _ -> expectationFailure "Expected Left for invalid timezone"

    it "cronJobInTimezone accepts a real Olson name and sets the field" $ do
      let result =
            cronJobInTimezone
              "ny"
              "America/New_York"
              "0 3 * * *"
              SkipOverlap
              (\_ _ -> defaultJob (SimpleTask "x"))
      case result of
        Right cron -> timezone cron `shouldBe` Just "America/New_York"
        Left err -> expectationFailure $ "Expected Right, got: " <> err

    it "resolveTZ knows UTC and Etc/UTC" $ do
      case resolveTZ "UTC" of
        Just _ -> pure ()
        Nothing -> expectationFailure "Expected UTC to resolve"
      case resolveTZ "Etc/UTC" of
        Just _ -> pure ()
        Nothing -> expectationFailure "Expected Etc/UTC to resolve"

    it "matchesInTimezone with Nothing == scheduleMatches in UTC" $ do
      let Right sched = parseCronSchedule "0 3 * * *"
          tick = mkTime 2025 6 15 3 0 0
      matchesInTimezone Nothing sched tick `shouldBe` True

    it "DST spring-forward: '30 2 * * *' in America/New_York does not fire on the gap day" $ do
      -- On 2025-03-09 in NY, clocks jump 02:00 EST -> 03:00 EDT. Local 02:30
      -- does not exist that day. Cron does not fire.
      let Right sched = parseCronSchedule "30 2 * * *"
          zone = Just "America/New_York"
          -- Walk every UTC minute on 2025-03-09 (and a buffer on either
          -- side) checking that none match locally to 02:30 NY.
          ticks =
            [ mkTime 2025 3 9 hour minute 0
            | hour <- [0 .. 23]
            , minute <- [0 .. 59]
            ]
          matches = filter (matchesInTimezone zone sched) ticks
      matches `shouldBe` []

    it "DST spring-forward: same expression fires normally on a non-DST day" $ do
      -- Sanity check that the matcher fires on a normal day.
      let Right sched = parseCronSchedule "30 2 * * *"
          zone = Just "America/New_York"
          ticks =
            [ mkTime 2025 3 10 hour minute 0
            | hour <- [0 .. 23]
            , minute <- [0 .. 59]
            ]
          matches = filter (matchesInTimezone zone sched) ticks
      length matches `shouldBe` 1

    it "nextRunInTimezone reports the tick the scheduler fires across a fall-back" $ do
      -- 01:30 NY runs at 05:30 and 06:30 UTC. The scheduler fires the first.
      let Right sched = parseCronSchedule "30 1 * * *"
          zone = Just "America/New_York"
          now = mkTime 2025 11 2 5 20 0
      nextRunInTimezone zone sched now `shouldBe` Just (mkTime 2025 11 2 5 30 0)

    it "nextRunInTimezone skips a spring-forward gap" $ do
      -- Local 02:30 does not exist on 2025-03-09 in NY.
      let Right sched = parseCronSchedule "30 2 * * *"
          zone = Just "America/New_York"
          now = mkTime 2025 3 9 6 0 0
      nextRunInTimezone zone sched now `shouldBe` Just (mkTime 2025 3 10 6 30 0)

    it "nextRunInTimezone agrees with matchesInTimezone on an ordinary day" $ do
      let Right sched = parseCronSchedule "30 2 * * *"
          zone = Just "America/New_York"
          now = mkTime 2025 6 15 0 0 0
          next = nextRunInTimezone zone sched now
      next `shouldBe` Just (mkTime 2025 6 15 6 30 0)
      fmap (matchesInTimezone zone sched) next `shouldBe` Just True

    it "nextRunInTimezone names no tick an earlier one beats" $ do
      -- Nothing before the reported tick may match.
      let Right sched = parseCronSchedule "30 1 * * *"
          zone = Just "America/New_York"
          starts = [mkTime 2025 11 2 0 0 0, mkTime 2025 3 9 0 0 0, mkTime 2025 6 15 0 0 0]
          earlierMatch now =
            case nextRunInTimezone zone sched now of
              Nothing -> Just now
              Just next ->
                find
                  (matchesInTimezone zone sched)
                  (takeWhile (< next) (iterate (addUTCTime 60) (addUTCTime 60 now)))
      map earlierMatch starts `shouldBe` [Nothing, Nothing, Nothing]

    it "nextRunInTimezone skips a fixed-time schedule's second pass" $ do
      -- 01:30 EDT ran at 05:30. 01:30 EST is the same local minute and does not fire.
      let Right sched = parseCronSchedule "30 1 * * *"
          zone = Just "America/New_York"
          now = mkTime 2025 11 2 5 35 0
      nextRunInTimezone zone sched now `shouldBe` Just (mkTime 2025 11 3 6 30 0)

    it "nextRunInTimezone reports a wildcard schedule's replayed minute" $ do
      -- 01:15 EST is a later tick than 01:15 EDT. A local-minute walk alone misses it.
      let Right sched = parseCronSchedule "*/15 1 * * *"
          zone = Just "America/New_York"
          now = mkTime 2025 11 2 6 0 0
      nextRunInTimezone zone sched now `shouldBe` Just (mkTime 2025 11 2 6 15 0)

    it "DST fall-back: '30 1 * * *' in America/New_York matches once" $ do
      -- On 2025-11-02 in NY, clocks fall back 02:00 EDT -> 01:00 EST. Local
      -- 01:30 reads twice, at 05:30 UTC and 06:30 UTC. A fixed-time schedule
      -- fires on the first reading only.
      let Right sched = parseCronSchedule "30 1 * * *"
          zone = Just "America/New_York"
          ticks =
            [ mkTime 2025 11 2 hour minute 0
            | hour <- [0 .. 23]
            , minute <- [0 .. 59]
            ]
          matches = filter (matchesInTimezone zone sched) ticks
      matches `shouldBe` [mkTime 2025 11 2 5 30 0]

    it "DST fall-back: a wildcard minute or hour field runs through both passes" $ do
      -- The local day of 2025-11-02 runs from 04:00Z to 04:59Z the next day, 25
      -- real hours. A schedule whose minute or hour field starts with * runs in
      -- both 01:00 hours, as Vixie cron does.
      let Right everyFive = parseCronSchedule "*/5 * * * *"
          Right hourly = parseCronSchedule "0 * * * *"
          Right withinOne = parseCronSchedule "*/30 1 * * *"
          zone = Just "America/New_York"
          ticks = enumMinutes (mkTime 2025 11 2 4 0 0) (mkTime 2025 11 3 4 59 0)
          countMatches sched = length (filter (matchesInTimezone zone sched) ticks)
      countMatches everyFive `shouldBe` 25 * 12
      countMatches hourly `shouldBe` 25
      countMatches withinOne `shouldBe` 4

    it "two schedules in different zones produce different UTC fire times" $ do
      let Right sched = parseCronSchedule "0 9 * * *"
          tzNy = Just "America/New_York"
          tzBerlin = Just "Europe/Berlin"
          day = [mkTime 2025 6 15 hour minute 0 | hour <- [0 .. 23], minute <- [0 .. 59]]
          fireNy = filter (matchesInTimezone tzNy sched) day
          fireBerlin = filter (matchesInTimezone tzBerlin sched) day
      length fireNy `shouldBe` 1
      length fireBerlin `shouldBe` 1
      -- NY 09:00 EDT = 13:00 UTC. Berlin 09:00 CEST = 07:00 UTC. Different.
      fireNy `shouldNotBe` fireBerlin

    it "UTC default behavior unchanged when timezone is Nothing" $ do
      let Right sched = parseCronSchedule "0 3 * * *"
          tick = mkTime 2025 6 15 3 0 0
          nonMatch = mkTime 2025 6 15 8 0 0
      matchesInTimezone Nothing sched tick `shouldBe` True
      matchesInTimezone Nothing sched nonMatch `shouldBe` False

  describe "computeDelayMicros" $ do
    it "normal case: 15s before next minute" $ do
      -- 12:34:45 → next minute is 12:35:00 → 15s = 15_000_000 µs
      let tick = mkTime 2025 6 15 12 34 45
      computeDelayMicros tick `shouldBe` 15_000_000

    it "on a minute boundary: returns 60s" $ do
      -- 12:34:00 → next minute is 12:35:00 → 60s = 60_000_000 µs
      let tick = mkTime 2025 6 15 12 34 0
      computeDelayMicros tick `shouldBe` 60_000_000

    it "just past a minute: returns ~60s" $ do
      -- 12:34:01 → next minute is 12:35:00 → 59s = 59_000_000 µs
      let tick = mkTime 2025 6 15 12 34 1
      computeDelayMicros tick `shouldBe` 59_000_000

    it "near next minute: returns ~0s" $ do
      -- 12:34:59 → next minute is 12:35:00 → 1s = 1_000_000 µs
      let tick = mkTime 2025 6 15 12 34 59
      computeDelayMicros tick `shouldBe` 1_000_000

    it "half-minute mark: returns 30s" $ do
      let tick = mkTime 2025 6 15 12 34 30
      computeDelayMicros tick `shouldBe` 30_000_000

    it "midnight boundary: 23:59:59 returns 1s" $ do
      -- 23:59:59 → next minute is 00:00:00 next day → 1s = 1_000_000 µs
      let tick = mkTime 2025 6 15 23 59 59
      computeDelayMicros tick `shouldBe` 1_000_000

  describe "enumMinutes" $ do
    it "returns empty list when start > end" $ do
      let start = mkTime 2025 6 15 12 5 0
          end = mkTime 2025 6 15 12 0 0
      enumMinutes start end `shouldBe` []

    it "returns single element when start == end" $ do
      let tick = mkTime 2025 6 15 12 0 0
      enumMinutes tick tick `shouldBe` [tick]

    it "enumerates consecutive minutes" $ do
      let start = mkTime 2025 6 15 12 0 0
          end = mkTime 2025 6 15 12 3 0
      enumMinutes start end
        `shouldBe` [ mkTime 2025 6 15 12 0 0
                   , mkTime 2025 6 15 12 1 0
                   , mkTime 2025 6 15 12 2 0
                   , mkTime 2025 6 15 12 3 0
                   ]

    it "crosses midnight boundary" $ do
      let start = mkTime 2025 6 15 23 58 0
          end = mkTime 2025 6 16 0 1 0
      length (enumMinutes start end) `shouldBe` 4

  describe "enumerateCatchUpTicks" $ do
    it "NoBackfill returns only the current tick" $ do
      let lastChecked = mkTime 2025 6 15 9 0 0
          currentTick = mkTime 2025 6 15 12 0 0
      enumerateCatchUpTicks NoBackfill (Just lastChecked) currentTick
        `shouldBe` [currentTick]

    it "Backfill with no last_checked_at returns only the current tick" $ do
      let currentTick = mkTime 2025 6 15 12 0 0
      enumerateCatchUpTicks (Backfill 3600) Nothing currentTick
        `shouldBe` [currentTick]

    it "Backfill includes the window cutoff minute when lastChecked predates the window" $ do
      -- currentTick = 12:00, window = 60s. Window cutoff = 11:59.
      -- lastChecked is 3 hours ago (way before the window). The 11:59
      -- minute has never been processed and is included.
      let currentTick = mkTime 2025 6 15 12 0 0
          lastChecked = mkTime 2025 6 15 9 0 0
      enumerateCatchUpTicks (Backfill 60) (Just lastChecked) currentTick
        `shouldSatisfy` elem (mkTime 2025 6 15 11 59 0)

    it "Backfill skips lastChecked itself when it is inside the window" $ do
      -- lastChecked is inside the window and already processed. The catch-up
      -- starts at lastChecked + 1.
      let currentTick = mkTime 2025 6 15 12 0 0
          lastChecked = mkTime 2025 6 15 11 59 0
          ticks = enumerateCatchUpTicks (Backfill 120) (Just lastChecked) currentTick
      ticks `shouldSatisfy` notElem lastChecked
      ticks `shouldSatisfy` elem currentTick

    it "Backfill with a non-minute-multiple window still includes the live tick" $ do
      -- Regression: 90s window puts windowFloor mid-minute, dropping currentTick.
      let currentTick = mkTime 2025 6 15 12 0 0
          lastChecked = mkTime 2025 6 15 9 0 0
          ticks = enumerateCatchUpTicks (Backfill 90) (Just lastChecked) currentTick
      ticks `shouldSatisfy` elem currentTick
      ticks `shouldSatisfy` all ((== (0 :: Int)) . (`mod` 60) . floor . utctDayTime)

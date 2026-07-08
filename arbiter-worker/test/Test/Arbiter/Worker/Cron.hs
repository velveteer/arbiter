{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns -Wno-x-partial #-}

module Test.Arbiter.Worker.Cron (spec) where

import Arbiter.Core.CronSchedule (CronScheduleUpdate (..))
import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (DedupKey (IgnoreDuplicate), Job (..), JobRead, defaultJob)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Simple (SimpleEnv (..), createSimpleEnvWithPool, inTransaction, runSimpleDb)
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Setup (cleanupData, createSharedPool, setupOnce)
import Control.Exception (bracket, catch)
import Control.Monad (forM_, void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Maybe (isJust)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Time
  ( UTCTime (..)
  , fromGregorian
  , getCurrentTime
  , secondsToDiffTime
  )
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import System.Cron (parseCronSchedule)
import Test.Hspec
  ( Spec
  , around
  , beforeAll
  , describe
  , expectationFailure
  , it
  , runIO
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
  , formatMinuteInTimezone
  , initCronSchedules
  , makeDedupKey
  , matchesInTimezone
  , processCronCatchUp
  , resolveTZ
  , truncateToMinute
  )
import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), LogLevel (..))

type WorkerTestRegistry = '[ '("arbiter_cron_test", WorkerTestPayload)]

testSchema :: Text
testSchema = "arbiter_cron_test"

testTable :: Text
testTable = "arbiter_cron_test"

-- | A silent logger for tests (filters out everything below Error).
testLogConfig :: LogConfig
testLogConfig =
  LogConfig
    { minLogLevel = Error -- Only show errors
    , logDestination = LogStdout
    , additionalContext = pure []
    }

-- | Helper to build a UTCTime from components.
mkTime :: Integer -> Int -> Int -> Int -> Int -> Int -> UTCTime
mkTime year month day hour minute second =
  let d = fromGregorian year month day
      s = secondsToDiffTime (fromIntegral $ hour * 3600 + minute * 60 + second)
   in UTCTime d s

spec :: ByteString -> Spec
spec connStr = do
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
      let d = fromGregorian 2025 6 15
          -- 12:34:56.789
          s = 12 * 3600 + 34 * 60 + 56.789
          input = UTCTime d s
          expected = mkTime 2025 6 15 12 34 0
      truncateToMinute input `shouldBe` expected

  describe "formatMinute" $ do
    it "produces YYYY-MM-DDTHH:MM format" $ do
      let t = mkTime 2025 1 9 8 5 0
      formatMinute t `shouldBe` "2025-01-09T08:05"

  describe "makeDedupKey" $ do
    it "SkipOverlap produces arbiter_cron:<name> (no time)" $ do
      let Right cj = cronJob "nightly" "0 3 * * *" SkipOverlap (\_ _ -> defaultJob (SimpleTask "x"))
          tick = mkTime 2025 6 15 3 0 0
      makeDedupKey cj tick `shouldBe` "arbiter_cron:nightly"

    it "AllowOverlap produces arbiter_cron:<name>:<time>" $ do
      let Right cj = cronJob "nightly" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "x"))
          tick = mkTime 2025 6 15 3 0 0
      makeDedupKey cj tick `shouldBe` "arbiter_cron:nightly:2025-06-15T03:00"

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
        Right cj -> timezone cj `shouldBe` Just "America/New_York"
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
      -- does not exist that day. The UTC minute that would correspond to
      -- 02:30 local maps to 03:30 EDT instead. Cron must not fire.
      let Right sched = parseCronSchedule "30 2 * * *"
          tz = Just "America/New_York"
          -- Walk every UTC minute on 2025-03-09 (and a buffer on either
          -- side) checking that none match locally to 02:30 NY.
          ticks =
            [ mkTime 2025 3 9 h m 0
            | h <- [0 .. 23]
            , m <- [0 .. 59]
            ]
          matches = filter (matchesInTimezone tz sched) ticks
      matches `shouldBe` []

    it "DST spring-forward: same expression fires normally on a non-DST day" $ do
      -- Sanity check that the matcher fires on a normal day.
      let Right sched = parseCronSchedule "30 2 * * *"
          tz = Just "America/New_York"
          ticks =
            [ mkTime 2025 3 10 h m 0
            | h <- [0 .. 23]
            , m <- [0 .. 59]
            ]
          matches = filter (matchesInTimezone tz sched) ticks
      length matches `shouldBe` 1

    it "DST fall-back: '30 1 * * *' in America/New_York matches twice in UTC" $ do
      -- On 2025-11-02 in NY, clocks fall back 02:00 EDT -> 01:00 EST. Local
      -- 01:30 happens twice: once as EDT (05:30 UTC) and once as EST
      -- (06:30 UTC). Both UTC ticks should match locally, and the local-minute
      -- dedup key collapses them to a single fire.
      let Right sched = parseCronSchedule "30 1 * * *"
          tz = Just "America/New_York"
          ticks =
            [ mkTime 2025 11 2 h m 0
            | h <- [0 .. 23]
            , m <- [0 .. 59]
            ]
          matches = filter (matchesInTimezone tz sched) ticks
      length matches `shouldBe` 2
      -- Both matches format to the same local minute, so AllowOverlap dedup
      -- blocks the second insert.
      map (formatMinuteInTimezone tz) matches
        `shouldBe` ["2025-11-02T01:30", "2025-11-02T01:30"]

    it "two schedules in different zones produce different UTC fire times" $ do
      let Right sched = parseCronSchedule "0 9 * * *"
          tzNy = Just "America/New_York"
          tzBerlin = Just "Europe/Berlin"
          day = [mkTime 2025 6 15 h m 0 | h <- [0 .. 23], m <- [0 .. 59]]
          fireNy = filter (matchesInTimezone tzNy sched) day
          fireBerlin = filter (matchesInTimezone tzBerlin sched) day
      length fireNy `shouldBe` 1
      length fireBerlin `shouldBe` 1
      -- NY 09:00 EDT = 13:00 UTC; Berlin 09:00 CEST = 07:00 UTC. Different.
      fireNy `shouldNotBe` fireBerlin

    it "UTC default behavior unchanged when timezone is Nothing" $ do
      let Right sched = parseCronSchedule "0 3 * * *"
          tick = mkTime 2025 6 15 3 0 0
          nonMatch = mkTime 2025 6 15 8 0 0
      matchesInTimezone Nothing sched tick `shouldBe` True
      matchesInTimezone Nothing sched nonMatch `shouldBe` False

    it "formatMinuteInTimezone with Nothing == formatMinute" $ do
      let t = mkTime 2025 6 15 12 30 0
      formatMinuteInTimezone Nothing t `shouldBe` formatMinute t

    it "formatMinuteInTimezone formats the local minute" $ do
      -- 2025-06-15T12:30 UTC = 08:30 in America/New_York (EDT, UTC-4)
      let t = mkTime 2025 6 15 12 30 0
      formatMinuteInTimezone (Just "America/New_York") t
        `shouldBe` "2025-06-15T08:30"

  describe "computeDelayMicros" $ do
    it "normal case: 15s before next minute" $ do
      -- 12:34:45 → next minute is 12:35:00 → 15s = 15_000_000 µs
      let t = mkTime 2025 6 15 12 34 45
      computeDelayMicros t `shouldBe` 15_000_000

    it "on a minute boundary: returns 60s" $ do
      -- 12:34:00 → next minute is 12:35:00 → 60s = 60_000_000 µs
      let t = mkTime 2025 6 15 12 34 0
      computeDelayMicros t `shouldBe` 60_000_000

    it "just past a minute: returns ~60s" $ do
      -- 12:34:01 → next minute is 12:35:00 → 59s = 59_000_000 µs
      let t = mkTime 2025 6 15 12 34 1
      computeDelayMicros t `shouldBe` 59_000_000

    it "near next minute: returns ~0s" $ do
      -- 12:34:59 → next minute is 12:35:00 → 1s = 1_000_000 µs
      let t = mkTime 2025 6 15 12 34 59
      computeDelayMicros t `shouldBe` 1_000_000

    it "half-minute mark: returns 30s" $ do
      let t = mkTime 2025 6 15 12 34 30
      computeDelayMicros t `shouldBe` 30_000_000

    it "midnight boundary: 23:59:59 returns 1s" $ do
      -- 23:59:59 → next minute is 00:00:00 next day → 1s = 1_000_000 µs
      let t = mkTime 2025 6 15 23 59 59
      computeDelayMicros t `shouldBe` 1_000_000

  describe "enumMinutes" $ do
    it "returns empty list when start > end" $ do
      let start = mkTime 2025 6 15 12 5 0
          end = mkTime 2025 6 15 12 0 0
      enumMinutes start end `shouldBe` []

    it "returns single element when start == end" $ do
      let t = mkTime 2025 6 15 12 0 0
      enumMinutes t t `shouldBe` [t]

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

  -- Integration tests (require PostgreSQL)
  describe "processCronTick" $ beforeAll (setupOnce connStr testSchema testTable True) $ do
    sharedPool <- runIO (createSharedPool connStr)
    around (withPool sharedPool) $ do
      it "inserts a job when the schedule matches the tick time" $ \env -> do
        let Right cj =
              cronJob
                "every-min"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (SimpleTask "cron-fired"))
            tick = mkTime 2025 6 15 12 0 0
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cj] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` SimpleTask "cron-fired"
        dedupKey (head jobs) `shouldBe` Just (IgnoreDuplicate "arbiter_cron:every-min:2025-06-15T12:00")

      it "does not insert a job when the schedule does not match" $ \env -> do
        -- "0 3 * * *" matches only at 03:00
        let Right cj =
              cronJob
                "nightly"
                "0 3 * * *"
                SkipOverlap
                (\_ _ -> defaultJob (SimpleTask "should-not-fire"))
            tick = mkTime 2025 6 15 12 0 0 -- 12:00, not 03:00
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cj] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 0

      it "SkipOverlap: two ticks at different times produce only 1 job" $ \env -> do
        let Right cj =
              cronJob
                "every-min"
                "* * * * *"
                SkipOverlap
                (\_ _ -> defaultJob (SimpleTask "skip-test"))
            tick1 = mkTime 2025 6 15 12 0 0
            tick2 = mkTime 2025 6 15 12 1 0
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cj] tick1
          processCronCatchUp testLogConfig testSchema testTable [cj] tick2

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        -- Both ticks produce the same dedup key "arbiter_cron:every-min", so only 1 job
        length jobs `shouldBe` 1

      it "AllowOverlap: two ticks at different times produce 2 jobs" $ \env -> do
        let Right cj =
              cronJob
                "every-min"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (SimpleTask "overlap-test"))
            tick1 = mkTime 2025 6 15 12 0 0
            tick2 = mkTime 2025 6 15 12 1 0
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cj] tick1
          processCronCatchUp testLogConfig testSchema testTable [cj] tick2

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 2

      it "only matching schedules fire when multiple are provided" $ \env -> do
        let Right cjAlways =
              cronJob
                "always"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (SimpleTask "always-fires"))
            Right cjNever =
              cronJob
                "nightly"
                "0 3 * * *"
                AllowOverlap
                (\_ _ -> defaultJob (SimpleTask "should-not-fire"))
            tick = mkTime 2025 6 15 12 0 0 -- 12:00, not 03:00
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cjAlways, cjNever] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cjAlways, cjNever] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` SimpleTask "always-fires"

      it "cronJobMake receives the tick time" $ \env -> do
        let Right cj =
              cronJob
                "time-check"
                "* * * * *"
                AllowOverlap
                (\_ t -> defaultJob (SimpleTask (formatMinute t)))
            tick = mkTime 2025 6 15 14 30 0
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cj] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` SimpleTask "2025-06-15T14:30"

      it "advances last_checked_at for all schedules, including failed inserts" $ \env -> do
        -- Failed inserts are logged but not retried. Watermark moves forward
        -- so the next iteration doesn't re-fire siblings that succeeded.
        let Right good =
              cronJob
                "good-1"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (SimpleTask "ok"))
            Right bad =
              cronJob
                "bad-1"
                "* * * * *"
                AllowOverlap
                (\_ _ -> error "intentional builder failure")
            tick = mkTime 2025 6 15 12 0 0
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [good, bad] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [good, bad] tick

        rows <- runSimpleDb env $ Ops.listCronSchedules testSchema Nothing
        let getRow n = lookup n [(CS.name r, r) | r <- rows]
        case getRow "good-1" of
          Just row -> CS.lastCheckedAt row `shouldBe` Just tick
          Nothing -> expectationFailure "good-1 schedule missing"
        case getRow "bad-1" of
          Just row -> CS.lastCheckedAt row `shouldBe` Just tick
          Nothing -> expectationFailure "bad-1 schedule missing"

        -- The good cron's job was actually inserted, not just watermarked.
        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        map payload jobs `shouldBe` [SimpleTask "ok"]

  -- DB integration tests for cron schedule management
  describe "initCronSchedules" $ beforeAll (setupOnce connStr testSchema testTable True) $ do
    sharedPool <- runIO (createSharedPool connStr)
    around (withPool sharedPool) $ do
      it "upserts rows" $ \env -> do
        let Right cj1 = cronJob "test-a" "0 3 * * *" SkipOverlap (\_ _ -> defaultJob (SimpleTask "a"))
            Right cj2 = cronJob "test-b" "*/5 * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "b"))
        -- Phase 1: Insert
        runSimpleDb env $ initCronSchedules testSchema testTable [cj1, cj2] testLogConfig
        rows <- runSimpleDb env $ Ops.listCronSchedules testSchema Nothing
        length rows `shouldBe` 2
        map CS.name rows `shouldBe` ["test-a", "test-b"]
        map CS.defaultExpression rows `shouldBe` ["0 3 * * *", "*/5 * * * *"]

        -- Phase 2: Re-upsert with modified expression - should update, not duplicate
        let Right cj1' = cronJob "test-a" "*/10 * * * *" SkipOverlap (\_ _ -> defaultJob (SimpleTask "a"))
        runSimpleDb env $ initCronSchedules testSchema testTable [cj1', cj2] testLogConfig
        rows2 <- runSimpleDb env $ Ops.listCronSchedules testSchema Nothing
        length rows2 `shouldBe` 2
        map CS.defaultExpression rows2 `shouldBe` ["*/10 * * * *", "*/5 * * * *"]

      it "skips disabled schedules" $ \env -> do
        let Right cj = cronJob "disabled-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "should-skip"))
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          _ <-
            Ops.updateCronSchedule
              testSchema
              "disabled-test"
              CronScheduleUpdate
                { overrideExpression = Nothing
                , overrideOverlap = Nothing
                , overrideTimezone = Nothing
                , enabled = Just False
                }
          processCronCatchUp testLogConfig testSchema testTable [cj] (mkTime 2025 6 15 12 0 0)

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 0

      it "uses DB expression override over default" $ \env -> do
        -- Create a schedule that fires every minute
        let Right cj = cronJob "override-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "override"))
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          _ <-
            Ops.updateCronSchedule
              testSchema
              "override-test"
              CronScheduleUpdate
                { overrideExpression = Just (Just "0 3 * * *")
                , overrideOverlap = Nothing
                , overrideTimezone = Nothing
                , enabled = Nothing
                }
          -- Tick at 12:00 should NOT fire (overridden to 3am only)
          processCronCatchUp testLogConfig testSchema testTable [cj] (mkTime 2025 6 15 12 0 0)

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 0

      it "updates last_fired_at on successful fire" $ \env -> do
        let Right cj = cronJob "fire-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "fire"))
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cj] (mkTime 2025 6 15 12 0 0)

        mRow <- runSimpleDb env $ Ops.getCronScheduleByName testSchema "fire-test"
        case mRow of
          Nothing -> expectationFailure "Expected cron schedule row to exist"
          Just row -> CS.lastFiredAt row `shouldSatisfy` isJust

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
      -- minute has never been processed, so it must be included.
      let currentTick = mkTime 2025 6 15 12 0 0
          lastChecked = mkTime 2025 6 15 9 0 0
      enumerateCatchUpTicks (Backfill 60) (Just lastChecked) currentTick
        `shouldSatisfy` elem (mkTime 2025 6 15 11 59 0)

    it "Backfill skips lastChecked itself when it is inside the window" $ do
      -- lastChecked is inside the window. We already processed lastChecked,
      -- so the catch-up starts at lastChecked + 1.
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

  describe "processCronCatchUp" $ beforeAll (setupOnce connStr testSchema testTable True) $ do
    sharedPool <- runIO (createSharedPool connStr)
    around (withPool sharedPool) $ do
      it "fires every missed minute for schedules with Backfill policy" $ \env -> do
        -- Simulate a scheduler wake-up after a 5-minute gap. last_checked_at
        -- is 5 minutes in the past. With Backfill 600 (10 min window) the
        -- catch-up must fire a job for each missed minute so we do not
        -- silently drop ticks during GC pauses or scheduler delays.
        let Right base =
              cronJob
                "catchup-backfill"
                "* * * * *"
                AllowOverlap
                (\_ t -> defaultJob (SimpleTask (formatMinute t)))
            cj = base {backfill = Backfill 600}
        runSimpleDb env $ initCronSchedules testSchema testTable [cj] testLogConfig

        withResource sharedPool $ \conn ->
          void $
            PG.execute
              conn
              "UPDATE arbiter_cron_test.cron_schedules SET last_checked_at = NOW() - interval '5 minutes' WHERE name = ?"
              (PG.Only ("catchup-backfill" :: Text))

        now <- runSimpleDb env $ liftIO getCurrentTime
        runSimpleDb env $ processCronCatchUp testLogConfig testSchema testTable [cj] now

        jobs <- runSimpleDb env $ HL.listJobs 100 0 :: IO [JobRead WorkerTestPayload]
        -- Expect at least 5 missed minutes plus the current one.
        length jobs `shouldSatisfy` (>= 5)

      it "does not replay missed minutes for NoBackfill schedules" $ \env -> do
        -- NoBackfill is the user's declaration that stale ticks should not
        -- be replayed. Even with a stale last_checked_at, only the current
        -- minute fires.
        let Right cj =
              cronJob
                "catchup-nobackfill"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (SimpleTask "no-replay"))
        runSimpleDb env $ initCronSchedules testSchema testTable [cj] testLogConfig

        withResource sharedPool $ \conn ->
          void $
            PG.execute
              conn
              "UPDATE arbiter_cron_test.cron_schedules SET last_checked_at = NOW() - interval '5 minutes' WHERE name = ?"
              (PG.Only ("catchup-nobackfill" :: Text))

        now <- runSimpleDb env $ liftIO getCurrentTime
        runSimpleDb env $ processCronCatchUp testLogConfig testSchema testTable [cj] now

        jobs <- runSimpleDb env $ HL.listJobs 100 0 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1

      it "fires only the current minute when last_checked_at is null" $ \env -> do
        -- Fresh schedule with no last_checked_at must not retroactively
        -- replay ticks from before it existed, regardless of policy.
        let Right base =
              cronJob
                "fresh"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (SimpleTask "fresh"))
            cj = base {backfill = Backfill 3600}
        runSimpleDb env $ initCronSchedules testSchema testTable [cj] testLogConfig

        now <- runSimpleDb env $ liftIO getCurrentTime
        runSimpleDb env $ processCronCatchUp testLogConfig testSchema testTable [cj] now

        jobs <- runSimpleDb env $ HL.listJobs 100 0 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1

      it "sets last_checked_at to currentTick, not wall-clock NOW()" $ \env -> do
        -- Slow-processing regression. Watermark must equal the 'now' passed in.
        let Right cj =
              cronJob "watermark" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "x"))
            currentTickPast = mkTime 2025 6 15 12 0 0
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cj] currentTickPast

        rows <- runSimpleDb env $ Ops.listCronSchedules testSchema Nothing
        case lookup "watermark" [(CS.name r, r) | r <- rows] of
          Just row -> CS.lastCheckedAt row `shouldBe` Just currentTickPast
          Nothing -> expectationFailure "watermark schedule missing"

      it "does not advance last_checked_at backwards" $ \env -> do
        -- GREATEST guard for concurrent pools with skewed clocks.
        let Right cj =
              cronJob "monotonic" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "x"))
            later = mkTime 2025 6 15 12 5 0
            earlier = mkTime 2025 6 15 12 0 0
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cj] later
          processCronCatchUp testLogConfig testSchema testTable [cj] earlier

        rows <- runSimpleDb env $ Ops.listCronSchedules testSchema Nothing
        case lookup "monotonic" [(CS.name r, r) | r <- rows] of
          Just row -> CS.lastCheckedAt row `shouldBe` Just later
          Nothing -> expectationFailure "monotonic schedule missing"

      it "does not re-fire ticks whose jobs were already processed" $ \env -> do
        -- After firing and processing the live tick, a second call at the
        -- same minute must not produce a duplicate (the dedup row is gone
        -- once the job is acked, so the watermark is what protects us).
        let Right cj =
              cronJob
                "no-duplicate"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (SimpleTask "once"))
            tick = mkTime 2025 6 15 12 0 0
        runSimpleDb env $ do
          initCronSchedules testSchema testTable [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema testTable [cj] tick

        claimed <- runSimpleDb env $ HL.claimNextVisibleJobs 100 60 :: IO [JobRead WorkerTestPayload]
        length claimed `shouldBe` 1
        runSimpleDb env $ forM_ claimed $ \j -> void $ HL.ackJob j

        runSimpleDb env $ processCronCatchUp testLogConfig testSchema testTable [cj] tick
        afterRetry <- runSimpleDb env $ HL.listJobs 100 0 :: IO [JobRead WorkerTestPayload]
        afterRetry `shouldBe` []

      it "gate prevents double-fire when last_fired_at already covers the minute" $ \env -> do
        -- Simulates a fast pool that already fired and acked 12:00.
        -- A slow pool retrying the same minute would double-fire without the gate.
        let Right cj = cronJob "skew-race" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "skew"))
            tick = mkTime 2025 6 15 12 0 0
        runSimpleDb env $ initCronSchedules testSchema testTable [cj] testLogConfig
        withResource sharedPool $ \conn ->
          void $
            PG.execute
              conn
              "UPDATE arbiter_cron_test.cron_schedules SET last_fired_at = ? WHERE name = ?"
              (tick, "skew-race" :: Text)
        runSimpleDb env $ processCronCatchUp testLogConfig testSchema testTable [cj] tick
        jobs <- runSimpleDb env $ HL.listJobs 100 0 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 0

      it "gate lets the next minute through after firing the previous one" $ \env -> do
        let Right cj = cronJob "skew-advance" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "advance"))
            tickPrev = mkTime 2025 6 15 12 0 0
            tickNext = mkTime 2025 6 15 12 1 0
        runSimpleDb env $ initCronSchedules testSchema testTable [cj] testLogConfig
        withResource sharedPool $ \conn ->
          void $
            PG.execute
              conn
              "UPDATE arbiter_cron_test.cron_schedules SET last_fired_at = ? WHERE name = ?"
              (tickPrev, "skew-advance" :: Text)
        runSimpleDb env $ processCronCatchUp testLogConfig testSchema testTable [cj] tickNext
        jobs <- runSimpleDb env $ HL.listJobs 100 0 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1

  describe "cron concurrency primitives" $ beforeAll (setupOnce connStr testSchema testTable True) $ do
    sharedPool <- runIO (createSharedPool connStr)
    around (withPool sharedPool) $ do
      it "touchCronChecked advances last_checked_at monotonically and matches by name" $ \env -> do
        let Right cj = cronJob "touch-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "x"))
            tEarly = mkTime 2025 6 15 12 0 0
            tLate = mkTime 2025 6 15 12 5 0
        runSimpleDb env $ initCronSchedules testSchema testTable [cj] testLogConfig

        nLate <- runSimpleDb env $ Ops.touchCronChecked testSchema tLate ["touch-test"]
        nLate `shouldBe` 1
        Just rowLate <- runSimpleDb env $ Ops.getCronScheduleByName testSchema "touch-test"
        CS.lastCheckedAt rowLate `shouldBe` Just tLate

        -- An earlier watermark still matches the row but never moves it backwards.
        nEarly <- runSimpleDb env $ Ops.touchCronChecked testSchema tEarly ["touch-test"]
        nEarly `shouldBe` 1
        Just rowEarly <- runSimpleDb env $ Ops.getCronScheduleByName testSchema "touch-test"
        CS.lastCheckedAt rowEarly `shouldBe` Just tLate

        -- An unknown name matches nothing.
        nMiss <- runSimpleDb env $ Ops.touchCronChecked testSchema tLate ["no-such-schedule"]
        nMiss `shouldBe` 0

      it "tryFireCronGate fires once per minute floor" $ \env -> do
        let Right cj = cronJob "gate-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "x"))
            m0 = mkTime 2025 6 15 12 0 0
            m1 = mkTime 2025 6 15 12 1 0
        runSimpleDb env $ initCronSchedules testSchema testTable [cj] testLogConfig

        firstFire <- runSimpleDb env $ Ops.tryFireCronGate testSchema "gate-test" m0
        firstFire `shouldBe` True
        -- The same minute floor cannot fire twice.
        secondFire <- runSimpleDb env $ Ops.tryFireCronGate testSchema "gate-test" m0
        secondFire `shouldBe` False
        -- A later minute floor fires again.
        nextFire <- runSimpleDb env $ Ops.tryFireCronGate testSchema "gate-test" m1
        nextFire `shouldBe` True

      it "tryAcquireCronLeader is mutually exclusive per (schema, queue, name) and releases on commit" $ \_env ->
        bracket (connectPostgreSQL connStr) close $ \conn1 ->
          bracket (connectPostgreSQL connStr) close $ \conn2 -> do
            let acquire conn name =
                  inTransaction @WorkerTestRegistry conn testSchema (Ops.tryAcquireCronLeader testSchema testTable name)

            -- The advisory lock is transaction-scoped, so conn1 must stay open to hold it.
            PG.begin conn1
            got1 <- acquire conn1 "leader"
            got1 `shouldBe` True

            PG.begin conn2
            -- conn1 still holds "leader", so conn2 loses on the same key.
            got2 <- acquire conn2 "leader"
            got2 `shouldBe` False
            -- A different schedule name is independent.
            got3 <- acquire conn2 "other"
            got3 `shouldBe` True
            PG.rollback conn2

            -- Once conn1 commits, "leader" is free for a fresh transaction.
            PG.commit conn1
            PG.begin conn2
            got4 <- acquire conn2 "leader"
            got4 `shouldBe` True
            PG.rollback conn2

withPool :: Pool PG.Connection -> (SimpleEnv WorkerTestRegistry -> IO a) -> IO a
withPool sharedPool action = do
  let env = createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema
  withResource sharedPool $ \conn -> do
    cleanupData testSchema testTable conn
    _ <- PG.execute_ conn "DELETE FROM arbiter_cron_test.cron_schedules" `catch` (\(_ :: PG.SqlError) -> pure 0)
    pure ()
  action env

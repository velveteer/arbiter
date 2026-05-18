{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns -Wno-x-partial #-}

module Test.Arbiter.Worker.Cron (spec) where

import Arbiter.Core.CronSchedule (CronScheduleUpdate (..))
import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (DedupKey (IgnoreDuplicate), Job (..), JobRead, defaultJob)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Simple (SimpleEnv (..), createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Setup (cleanupData, setupOnce)
import Control.Exception (catch)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Maybe (isJust)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes, withResource)
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
import Test.Hspec
  ( Spec
  , around
  , beforeAll
  , describe
  , expectationFailure
  , it
  , runIO
  , shouldBe
  , shouldSatisfy
  )

import Arbiter.Worker.Cron
  ( BackfillPolicy (..)
  , CronJob (..)
  , OverlapPolicy (..)
  , computeDelayMicros
  , cronJob
  , enumMinutes
  , enumerateCatchUpTicks
  , formatMinute
  , initCronSchedules
  , makeDedupKey
  , processCronCatchUp
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
          initCronSchedules testSchema [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema [cj] tick

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
          initCronSchedules testSchema [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema [cj] tick

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
          initCronSchedules testSchema [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema [cj] tick1
          processCronCatchUp testLogConfig testSchema [cj] tick2

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
          initCronSchedules testSchema [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema [cj] tick1
          processCronCatchUp testLogConfig testSchema [cj] tick2

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
          initCronSchedules testSchema [cjAlways, cjNever] testLogConfig
          processCronCatchUp testLogConfig testSchema [cjAlways, cjNever] tick

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
          initCronSchedules testSchema [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema [cj] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` SimpleTask "2025-06-15T14:30"

      it "advances last_checked_at only for schedules whose insert succeeded" $ \env -> do
        -- Two schedules. The 'good' builder yields a normal job. The 'bad'
        -- builder produces a bottom payload that throws when the job is
        -- forced, simulating an insert-time failure. Only 'good' should
        -- have last_checked_at advanced.
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
          initCronSchedules testSchema [good, bad] testLogConfig
          processCronCatchUp testLogConfig testSchema [good, bad] tick

        rows <- runSimpleDb env $ Ops.listCronSchedules testSchema
        let getRow n = lookup n [(CS.name r, r) | r <- rows]
        case getRow "good-1" of
          Just row -> CS.lastCheckedAt row `shouldSatisfy` isJust
          Nothing -> expectationFailure "good-1 schedule missing"
        case getRow "bad-1" of
          Just row -> CS.lastCheckedAt row `shouldBe` Nothing
          Nothing -> expectationFailure "bad-1 schedule missing"

  -- DB integration tests for cron schedule management
  describe "initCronSchedules" $ beforeAll (setupOnce connStr testSchema testTable True) $ do
    sharedPool <- runIO (createSharedPool connStr)
    around (withPool sharedPool) $ do
      it "upserts rows" $ \env -> do
        let Right cj1 = cronJob "test-a" "0 3 * * *" SkipOverlap (\_ _ -> defaultJob (SimpleTask "a"))
            Right cj2 = cronJob "test-b" "*/5 * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "b"))
        -- Phase 1: Insert
        runSimpleDb env $ initCronSchedules testSchema [cj1, cj2] testLogConfig
        rows <- runSimpleDb env $ Ops.listCronSchedules testSchema
        length rows `shouldBe` 2
        map CS.name rows `shouldBe` ["test-a", "test-b"]
        map CS.defaultExpression rows `shouldBe` ["0 3 * * *", "*/5 * * * *"]

        -- Phase 2: Re-upsert with modified expression - should update, not duplicate
        let Right cj1' = cronJob "test-a" "*/10 * * * *" SkipOverlap (\_ _ -> defaultJob (SimpleTask "a"))
        runSimpleDb env $ initCronSchedules testSchema [cj1', cj2] testLogConfig
        rows2 <- runSimpleDb env $ Ops.listCronSchedules testSchema
        length rows2 `shouldBe` 2
        map CS.defaultExpression rows2 `shouldBe` ["*/10 * * * *", "*/5 * * * *"]

      it "skips disabled schedules" $ \env -> do
        let Right cj = cronJob "disabled-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "should-skip"))
        runSimpleDb env $ do
          initCronSchedules testSchema [cj] testLogConfig
          _ <-
            Ops.updateCronSchedule
              testSchema
              "disabled-test"
              CronScheduleUpdate
                { overrideExpression = Nothing
                , overrideOverlap = Nothing
                , enabled = Just False
                }
          processCronCatchUp testLogConfig testSchema [cj] (mkTime 2025 6 15 12 0 0)

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 0

      it "uses DB expression override over default" $ \env -> do
        -- Create a schedule that fires every minute
        let Right cj = cronJob "override-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "override"))
        runSimpleDb env $ do
          initCronSchedules testSchema [cj] testLogConfig
          _ <-
            Ops.updateCronSchedule
              testSchema
              "override-test"
              CronScheduleUpdate
                { overrideExpression = Just (Just "0 3 * * *")
                , overrideOverlap = Nothing
                , enabled = Nothing
                }
          -- Tick at 12:00 should NOT fire (overridden to 3am only)
          processCronCatchUp testLogConfig testSchema [cj] (mkTime 2025 6 15 12 0 0)

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 0

      it "updates last_fired_at on successful fire" $ \env -> do
        let Right cj = cronJob "fire-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "fire"))
        runSimpleDb env $ do
          initCronSchedules testSchema [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema [cj] (mkTime 2025 6 15 12 0 0)

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
      ticks `shouldSatisfy` all ((== 0) . (`mod` 60) . floor . utctDayTime)

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
        runSimpleDb env $ initCronSchedules testSchema [cj] testLogConfig

        withResource sharedPool $ \conn ->
          void $
            PG.execute
              conn
              "UPDATE arbiter_cron_test.cron_schedules SET last_checked_at = NOW() - interval '5 minutes' WHERE name = ?"
              (PG.Only ("catchup-backfill" :: Text))

        now <- runSimpleDb env $ liftIO getCurrentTime
        runSimpleDb env $ processCronCatchUp testLogConfig testSchema [cj] now

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
        runSimpleDb env $ initCronSchedules testSchema [cj] testLogConfig

        withResource sharedPool $ \conn ->
          void $
            PG.execute
              conn
              "UPDATE arbiter_cron_test.cron_schedules SET last_checked_at = NOW() - interval '5 minutes' WHERE name = ?"
              (PG.Only ("catchup-nobackfill" :: Text))

        now <- runSimpleDb env $ liftIO getCurrentTime
        runSimpleDb env $ processCronCatchUp testLogConfig testSchema [cj] now

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
        runSimpleDb env $ initCronSchedules testSchema [cj] testLogConfig

        now <- runSimpleDb env $ liftIO getCurrentTime
        runSimpleDb env $ processCronCatchUp testLogConfig testSchema [cj] now

        jobs <- runSimpleDb env $ HL.listJobs 100 0 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1

      it "sets last_checked_at to currentTick, not wall-clock NOW()" $ \env -> do
        -- Slow-processing regression. Watermark must equal the 'now' passed in.
        let Right cj =
              cronJob "watermark" "* * * * *" AllowOverlap (\_ _ -> defaultJob (SimpleTask "x"))
            currentTickPast = mkTime 2025 6 15 12 0 0
        runSimpleDb env $ do
          initCronSchedules testSchema [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema [cj] currentTickPast

        rows <- runSimpleDb env $ Ops.listCronSchedules testSchema
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
          initCronSchedules testSchema [cj] testLogConfig
          processCronCatchUp testLogConfig testSchema [cj] later
          processCronCatchUp testLogConfig testSchema [cj] earlier

        rows <- runSimpleDb env $ Ops.listCronSchedules testSchema
        case lookup "monotonic" [(CS.name r, r) | r <- rows] of
          Just row -> CS.lastCheckedAt row `shouldBe` Just later
          Nothing -> expectationFailure "monotonic schedule missing"

createSharedPool :: ByteString -> IO (Pool PG.Connection)
createSharedPool connStr =
  newPool $
    setNumStripes (Just 1) $
      defaultPoolConfig
        (connectPostgreSQL connStr)
        close
        60
        5

withPool :: Pool PG.Connection -> (SimpleEnv WorkerTestRegistry -> IO a) -> IO a
withPool sharedPool action = do
  let env = createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema
  withResource sharedPool $ \conn -> do
    cleanupData testSchema testTable conn
    _ <- PG.execute_ conn "DELETE FROM arbiter_cron_test.cron_schedules" `catch` (\(_ :: PG.SqlError) -> pure 0)
    pure ()
  action env

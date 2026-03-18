{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns -Wno-x-partial #-}

module Test.Arbiter.Worker.Cron (spec) where

import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (DedupKey (IgnoreDuplicate), Job (..), JobRead, defaultJob)
import Arbiter.Simple (SimpleEnv (..), createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Setup (cleanupData, setupOnce)
import Control.Exception (catch)
import Data.ByteString (ByteString)
import Data.Maybe (isJust)
import Data.Pool (Pool, defaultPoolConfig, newPool, setNumStripes, withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Time
  ( UTCTime (..)
  , fromGregorian
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
  ( OverlapPolicy (..)
  , computeDelayMicros
  , cronJob
  , formatMinute
  , initCronSchedules
  , makeDedupKey
  , processCronTick
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
      let result = cronJob "test" "0 3 * * *" SkipOverlap (\_ -> defaultJob (SimpleTask "x"))
      case result of
        Right _ -> pure ()
        Left err -> expectationFailure $ "Expected Right, got Left: " <> show err

    it "rejects an invalid cron expression" $ do
      let result = cronJob "test" "bad cron" SkipOverlap (\_ -> defaultJob (SimpleTask "x"))
      case result of
        Left _ -> pure ()
        Right _ -> expectationFailure "Expected Left (parse error), got Right"

    it "accepts every-minute expression" $ do
      let result = cronJob "test" "* * * * *" AllowOverlap (\_ -> defaultJob (SimpleTask "x"))
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
      let Right cj = cronJob "nightly" "0 3 * * *" SkipOverlap (\_ -> defaultJob (SimpleTask "x"))
          tick = mkTime 2025 6 15 3 0 0
      makeDedupKey cj tick `shouldBe` "arbiter_cron:nightly"

    it "AllowOverlap produces arbiter_cron:<name>:<time>" $ do
      let Right cj = cronJob "nightly" "0 3 * * *" AllowOverlap (\_ -> defaultJob (SimpleTask "x"))
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

  -- Integration tests (require PostgreSQL)
  describe "processCronTick" $ beforeAll (setupOnce connStr testSchema testTable True) $ do
    sharedPool <- runIO (createSharedPool connStr)
    around (withPool sharedPool) $ do
      it "inserts a job when the schedule matches the tick time" $ \(env, pool) -> do
        let Right cj =
              cronJob
                "every-min"
                "* * * * *"
                AllowOverlap
                (\_ -> defaultJob (SimpleTask "cron-fired"))
            tick = mkTime 2025 6 15 12 0 0
        withResource pool $ \conn -> do
          initCronSchedules conn testSchema [cj] testLogConfig
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` SimpleTask "cron-fired"
        dedupKey (head jobs) `shouldBe` Just (IgnoreDuplicate "arbiter_cron:every-min:2025-06-15T12:00")

      it "does not insert a job when the schedule does not match" $ \(env, pool) -> do
        -- "0 3 * * *" matches only at 03:00
        let Right cj =
              cronJob
                "nightly"
                "0 3 * * *"
                SkipOverlap
                (\_ -> defaultJob (SimpleTask "should-not-fire"))
            tick = mkTime 2025 6 15 12 0 0 -- 12:00, not 03:00
        withResource pool $ \conn -> do
          initCronSchedules conn testSchema [cj] testLogConfig
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 0

      it "SkipOverlap: two ticks at different times produce only 1 job" $ \(env, pool) -> do
        let Right cj =
              cronJob
                "every-min"
                "* * * * *"
                SkipOverlap
                (\_ -> defaultJob (SimpleTask "skip-test"))
            tick1 = mkTime 2025 6 15 12 0 0
            tick2 = mkTime 2025 6 15 12 1 0
        withResource pool $ \conn -> do
          initCronSchedules conn testSchema [cj] testLogConfig
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick1
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick2

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        -- Both ticks produce the same dedup key "arbiter_cron:every-min", so only 1 job
        length jobs `shouldBe` 1

      it "AllowOverlap: two ticks at different times produce 2 jobs" $ \(env, pool) -> do
        let Right cj =
              cronJob
                "every-min"
                "* * * * *"
                AllowOverlap
                (\_ -> defaultJob (SimpleTask "overlap-test"))
            tick1 = mkTime 2025 6 15 12 0 0
            tick2 = mkTime 2025 6 15 12 1 0
        withResource pool $ \conn -> do
          initCronSchedules conn testSchema [cj] testLogConfig
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick1
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick2

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 2

      it "only matching schedules fire when multiple are provided" $ \(env, pool) -> do
        let Right cjAlways =
              cronJob
                "always"
                "* * * * *"
                AllowOverlap
                (\_ -> defaultJob (SimpleTask "always-fires"))
            Right cjNever =
              cronJob
                "nightly"
                "0 3 * * *"
                AllowOverlap
                (\_ -> defaultJob (SimpleTask "should-not-fire"))
            tick = mkTime 2025 6 15 12 0 0 -- 12:00, not 03:00
        withResource pool $ \conn -> do
          initCronSchedules conn testSchema [cjAlways, cjNever] testLogConfig
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cjAlways, cjNever] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` SimpleTask "always-fires"

      it "cronJobMake receives the tick time" $ \(env, pool) -> do
        let Right cj =
              cronJob
                "time-check"
                "* * * * *"
                AllowOverlap
                (\t -> defaultJob (SimpleTask (formatMinute t)))
            tick = mkTime 2025 6 15 14 30 0
        withResource pool $ \conn -> do
          initCronSchedules conn testSchema [cj] testLogConfig
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` SimpleTask "2025-06-15T14:30"

  -- DB integration tests for cron schedule management
  describe "initCronSchedules" $ beforeAll (setupOnce connStr testSchema testTable True) $ do
    sharedPool <- runIO (createSharedPool connStr)
    around (withPool sharedPool) $ do
      it "upserts rows" $ \(_env, pool) -> do
        let Right cj1 = cronJob "test-a" "0 3 * * *" SkipOverlap (\_ -> defaultJob (SimpleTask "a"))
            Right cj2 = cronJob "test-b" "*/5 * * * *" AllowOverlap (\_ -> defaultJob (SimpleTask "b"))
        withResource pool $ \conn -> do
          -- Phase 1: Insert
          initCronSchedules conn testSchema [cj1, cj2] testLogConfig
          rows <- CS.listCronSchedules conn testSchema
          length rows `shouldBe` 2
          map CS.name rows `shouldBe` ["test-a", "test-b"]
          map CS.defaultExpression rows `shouldBe` ["0 3 * * *", "*/5 * * * *"]

          -- Phase 2: Re-upsert with modified expression — should update, not duplicate
          let Right cj1' = cronJob "test-a" "*/10 * * * *" SkipOverlap (\_ -> defaultJob (SimpleTask "a"))
          initCronSchedules conn testSchema [cj1', cj2] testLogConfig
          rows2 <- CS.listCronSchedules conn testSchema
          length rows2 `shouldBe` 2
          map CS.defaultExpression rows2 `shouldBe` ["*/10 * * * *", "*/5 * * * *"]

      it "skips disabled schedules" $ \(env, pool) -> do
        let Right cj = cronJob "disabled-test" "* * * * *" AllowOverlap (\_ -> defaultJob (SimpleTask "should-skip"))
        withResource pool $ \conn -> do
          initCronSchedules conn testSchema [cj] testLogConfig

          -- Disable the schedule
          _ <-
            CS.updateCronSchedule
              conn
              testSchema
              "disabled-test"
              CS.CronScheduleUpdate
                { CS.overrideExpression = Nothing
                , CS.overrideOverlap = Nothing
                , CS.enabled = Just False
                }

          let tick = mkTime 2025 6 15 12 0 0
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 0

      it "uses DB expression override over default" $ \(env, pool) -> do
        -- Create a schedule that fires every minute
        let Right cj = cronJob "override-test" "* * * * *" AllowOverlap (\_ -> defaultJob (SimpleTask "override"))
        withResource pool $ \conn -> do
          initCronSchedules conn testSchema [cj] testLogConfig

          -- Override expression to "0 3 * * *" (3am only)
          _ <-
            CS.updateCronSchedule
              conn
              testSchema
              "override-test"
              CS.CronScheduleUpdate
                { CS.overrideExpression = Just (Just "0 3 * * *")
                , CS.overrideOverlap = Nothing
                , CS.enabled = Nothing
                }

          -- Tick at 12:00 should NOT fire (overridden to 3am only)
          let tick = mkTime 2025 6 15 12 0 0
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick

        jobs <- runSimpleDb env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead WorkerTestPayload]
        length jobs `shouldBe` 0

      it "updates last_fired_at on successful fire" $ \(env, pool) -> do
        let Right cj = cronJob "fire-test" "* * * * *" AllowOverlap (\_ -> defaultJob (SimpleTask "fire"))
        withResource pool $ \conn -> do
          initCronSchedules conn testSchema [cj] testLogConfig

          let tick = mkTime 2025 6 15 12 0 0
          runSimpleDb env $ processCronTick conn testLogConfig testSchema [cj] tick

          mRow <- CS.getCronScheduleByName conn testSchema "fire-test"
          case mRow of
            Nothing -> expectationFailure "Expected cron schedule row to exist"
            Just row -> CS.lastFiredAt row `shouldSatisfy` isJust

createSharedPool :: ByteString -> IO (Pool PG.Connection)
createSharedPool connStr =
  newPool $
    setNumStripes (Just 1) $
      defaultPoolConfig
        (connectPostgreSQL connStr)
        close
        60
        5

withPool :: Pool PG.Connection -> ((SimpleEnv WorkerTestRegistry, Pool PG.Connection) -> IO a) -> IO a
withPool sharedPool action = do
  let env = createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema
  withResource sharedPool $ \conn -> do
    cleanupData testSchema testTable conn
    -- Also clean up cron_schedules table if it exists
    _ <- PG.execute_ conn "DELETE FROM arbiter_cron_test.cron_schedules" `catch` (\(_ :: PG.SqlError) -> pure 0)
    pure ()
  action (env, sharedPool)

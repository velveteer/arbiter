{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns -Wno-x-partial #-}

-- | Cron scheduler test suite, instantiated for each 'Arbiter.Core.MonadArbiter.MonadArbiter' backend.
module Arbiter.Worker.TestKit.Cron (cronSpec) where

import Arbiter.Core.CronSchedule (CronScheduleUpdate (..))
import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (DedupKey (IgnoreDuplicate), JobRead, dedupKey, defaultJob, payload)
import Arbiter.Core.MonadArbiter (withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Test.Setup (mkTime, withConn)
import Arbiter.Worker.Cron
  ( BackfillPolicy (..)
  , CronJob (..)
  , OverlapPolicy (..)
  , cronJob
  , cronJobInTimezone
  , formatMinute
  , initCronSchedules
  , newCronLog
  , processCronCatchUp
  , processRunRequests
  )
import Arbiter.Worker.Logger (silentLogConfig)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Foldable (traverse_)
import Data.Maybe (isJust)
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime, getCurrentTime)
import Database.PostgreSQL.Simple (Only (..))
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec (Spec, before, describe, expectationFailure, it, shouldBe, shouldNotBe, shouldSatisfy)
import UnliftIO (newEmptyMVar, putMVar, takeMVar)
import UnliftIO.Async (wait, withAsync)

import Arbiter.Worker.TestKit.Backend (TestBackend (..))

-- | 'processCronCatchUp' under a fresh gate store.
catchUpAt
  :: (QueueOperation m payload)
  => Text
  -> Text
  -> [CronJob payload]
  -> UTCTime
  -> m ()
catchUpAt schema table jobs tick = do
  cronLog <- newCronLog silentLogConfig
  processCronCatchUp cronLog schema table jobs tick

-- | 'processRunRequests' under a fresh gate store.
runRequestsAt
  :: (QueueOperation m payload)
  => Text
  -> [CronJob payload]
  -> UTCTime
  -> m ()
runRequestsAt schema jobs now = do
  cronLog <- newCronLog silentLogConfig
  processRunRequests cronLog schema jobs now

-- | An UPDATE on the schema's cron table by schedule name, with @?@ holes for the SET values.
cronUpdate :: Text -> Text -> PG.Query
cronUpdate schema setClause = fromString . T.unpack $ "UPDATE " <> CS.cronSchedulesTable schema <> " SET " <> setClause <> " WHERE name = ?"

-- | Cron scheduler suite.
cronSpec
  :: forall payload m env
   . ( Eq payload
     , QueueOperation m payload
     , Show payload
     )
  => TestBackend payload m env
  -> Spec
cronSpec TestBackend {schema, table, connStr, mkSimple, mkEnv, runM} =
  before mkEnv $ do
    describe "processCronTick" $ do
      it "inserts a job when the schedule matches the tick time" $ \env -> do
        let Right cron =
              cronJob
                "every-min"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "cron-fired"))
            tick = mkTime 2025 6 15 12 0 0
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] tick

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` mkSimple "cron-fired"
        dedupKey (head jobs) `shouldBe` Just (IgnoreDuplicate "arbiter_cron:every-min:2025-06-15T12:00")

      it "does not insert a job when the schedule does not match" $ \env -> do
        -- "0 3 * * *" matches only at 03:00
        let Right cron =
              cronJob
                "nightly"
                "0 3 * * *"
                SkipOverlap
                (\_ _ -> defaultJob (mkSimple "should-not-fire"))
            tick = mkTime 2025 6 15 12 0 0 -- 12:00
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] tick

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 0

      it "SkipOverlap: two ticks at different times produce only 1 job" $ \env -> do
        let Right cron =
              cronJob
                "every-min"
                "* * * * *"
                SkipOverlap
                (\_ _ -> defaultJob (mkSimple "skip-test"))
            tick1 = mkTime 2025 6 15 12 0 0
            tick2 = mkTime 2025 6 15 12 1 0
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] tick1
          catchUpAt schema table [cron] tick2

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        -- Both ticks produce the same dedup key "arbiter_cron:every-min". One job results.
        length jobs `shouldBe` 1

      it "AllowOverlap: two ticks at different times produce 2 jobs" $ \env -> do
        let Right cron =
              cronJob
                "every-min"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "overlap-test"))
            tick1 = mkTime 2025 6 15 12 0 0
            tick2 = mkTime 2025 6 15 12 1 0
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] tick1
          catchUpAt schema table [cron] tick2

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 2

      it "only matching schedules fire when multiple are provided" $ \env -> do
        let Right cjAlways =
              cronJob
                "always"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "always-fires"))
            Right cjNever =
              cronJob
                "nightly"
                "0 3 * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "should-not-fire"))
            tick = mkTime 2025 6 15 12 0 0 -- 12:00
        runM env $ do
          initCronSchedules schema table [cjAlways, cjNever] silentLogConfig
          catchUpAt schema table [cjAlways, cjNever] tick

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` mkSimple "always-fires"

      it "cronJobMake receives the tick time" $ \env -> do
        let Right cron =
              cronJob
                "time-check"
                "* * * * *"
                AllowOverlap
                (\_ tickAt -> defaultJob (mkSimple (formatMinute tickAt)))
            tick = mkTime 2025 6 15 14 30 0
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] tick

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 1
        payload (head jobs) `shouldBe` mkSimple "2025-06-15T14:30"

      it "advances a failed schedule and continues processing other schedules" $ \env -> do
        let Right good =
              cronJob
                "good-1"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "ok"))
            Right bad =
              cronJob
                "bad-1"
                "* * * * *"
                AllowOverlap
                (\_ _ -> errorWithoutStackTrace "intentional builder failure")
            recovered = bad {builder = \_ _ -> defaultJob (mkSimple "recovered")}
            tick = mkTime 2025 6 15 12 0 0
        runM env $ do
          initCronSchedules schema table [good, bad] silentLogConfig
          catchUpAt schema table [good, bad] tick

        rows <- runM env $ Ops.listCronSchedules schema Nothing
        let getRow scheduleName = lookup scheduleName [(CS.name row, row) | row <- rows]
        case getRow "good-1" of
          Just row -> CS.lastCheckedAt row `shouldBe` Just tick
          Nothing -> expectationFailure "good-1 schedule missing"
        case getRow "bad-1" of
          Just row -> CS.lastCheckedAt row `shouldBe` Just tick
          Nothing -> expectationFailure "bad-1 schedule missing"

        -- The good cron's job was inserted.
        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload jobs `shouldBe` [mkSimple "ok"]

        runM env $ catchUpAt schema table [recovered] (mkTime 2025 6 15 12 1 0)
        recoveredJobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload recoveredJobs `shouldBe` [mkSimple "recovered"]

      it "fires later backfill and live ticks after a tick keeps failing" $ \env -> do
        let failedTick = mkTime 2025 6 15 12 0 0
            currentTick = mkTime 2025 6 15 12 2 0
            Right base =
              cronJob "partial-backfill" "* * * * *" AllowOverlap $ \_ tick ->
                if tick == failedTick
                  then errorWithoutStackTrace "persistent builder failure"
                  else defaultJob (mkSimple (formatMinute tick))
            cron = base {backfill = Backfill 600}
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          void $ Ops.touchCronChecked schema (mkTime 2025 6 15 11 59 0) [name cron]
          catchUpAt schema table [cron] currentTick

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload jobs `shouldBe` map (mkSimple . formatMinute) [mkTime 2025 6 15 12 1 0, currentTick]
        Just row <- runM env $ Ops.getCronScheduleByName schema (name cron)
        CS.lastCheckedAt row `shouldBe` Just currentTick

        runM env $ catchUpAt schema table [cron] (mkTime 2025 6 15 12 3 0)
        next <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload next `shouldBe` [mkSimple "2025-06-15T12:03"]

      it "retries a failed newest backfill tick on the next pass" $ \env -> do
        let lastChecked = mkTime 2025 6 15 11 59 0
            failedTick = mkTime 2025 6 15 12 1 0
            nextTick = mkTime 2025 6 15 12 2 0
            Right base =
              cronJob "retry-newest" "* * * * *" AllowOverlap $ \_ tick ->
                if tick == failedTick
                  then errorWithoutStackTrace "transient builder failure"
                  else defaultJob (mkSimple (formatMinute tick))
            cron = base {backfill = Backfill 600}
            recovered = cron {builder = \_ tick -> defaultJob (mkSimple (formatMinute tick))}
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          void $ Ops.touchCronChecked schema lastChecked [name cron]
          catchUpAt schema table [cron] failedTick

        Just held <- runM env $ Ops.getCronScheduleByName schema (name cron)
        CS.lastCheckedAt held `shouldBe` Just (mkTime 2025 6 15 12 0 0)

        runM env $ catchUpAt schema table [recovered] nextTick
        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload jobs `shouldBe` map (mkSimple . formatMinute) [mkTime 2025 6 15 12 0 0, failedTick, nextTick]
        Just row <- runM env $ Ops.getCronScheduleByName schema (name cron)
        CS.lastCheckedAt row `shouldBe` Just nextTick

      it "fires a later SkipOverlap tick after the oldest backfill tick fails" $ \env -> do
        let failedTick = mkTime 2025 6 15 12 0 0
            currentTick = mkTime 2025 6 15 12 2 0
            Right base =
              cronJob "skip-failed" "* * * * *" SkipOverlap $ \_ tick ->
                if tick == failedTick
                  then errorWithoutStackTrace "persistent builder failure"
                  else defaultJob (mkSimple (formatMinute tick))
            cron = base {backfill = Backfill 600}
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          void $ Ops.touchCronChecked schema (mkTime 2025 6 15 11 59 0) [name cron]
          catchUpAt schema table [cron] currentTick

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload jobs `shouldBe` [mkSimple "2025-06-15T12:01"]
        Just row <- runM env $ Ops.getCronScheduleByName schema (name cron)
        CS.lastCheckedAt row `shouldBe` Just currentTick

      it "retains the backfill watermark while another pool holds the leader lock" $ \env -> do
        let lastChecked = mkTime 2025 6 15 11 59 0
            currentTick = mkTime 2025 6 15 12 1 0
            Right base = cronJob "contended-backfill" "* * * * *" AllowOverlap (\_ tick -> defaultJob (mkSimple (formatMinute tick)))
            cron = base {backfill = Backfill 600}
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          void $ Ops.touchCronChecked schema lastChecked [name cron]
        held <- newEmptyMVar
        release <- newEmptyMVar
        let holdLeader = runM env . withDbTransaction $ do
              got <- Ops.tryAcquireCronLeader schema table (name cron)
              liftIO (putMVar held got)
              liftIO (takeMVar release)
        withAsync holdLeader $ \holder -> do
          got <- takeMVar held
          got `shouldBe` True
          runM env $ catchUpAt schema table [cron] currentTick
          Just blocked <- runM env $ Ops.getCronScheduleByName schema (name cron)
          CS.lastCheckedAt blocked `shouldBe` Just lastChecked
          putMVar release ()
          wait holder

        runM env $ catchUpAt schema table [cron] currentTick
        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload jobs `shouldBe` map (mkSimple . formatMinute) [mkTime 2025 6 15 12 0 0, currentTick]

    describe "processRunRequests" $ do
      it "fires a requested schedule the tick would not match" $ \env -> do
        let Right cron =
              cronJob
                "run-nightly"
                "0 3 * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "manual"))
            now = mkTime 2025 6 15 12 30 0
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          _ <- Ops.requestCronRun schema "run-nightly"
          runRequestsAt schema [cron] now

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload jobs `shouldBe` [mkSimple "manual"]

        Just row <- runM env $ Ops.getCronScheduleByName schema "run-nightly"
        CS.runRequestedAt row `shouldBe` Nothing

      it "keeps the request pending when the insert fails" $ \env -> do
        -- The claim rolls back with the insert. A later pass fires it.
        let failing :: CronJob payload
            Right failing =
              cronJob
                "run-atomic"
                "0 3 * * *"
                AllowOverlap
                (\_ _ -> errorWithoutStackTrace "intentional builder failure")
            Right working = cronJob "run-atomic" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "manual"))
            now = mkTime 2025 6 15 12 30 0
        runM env $ do
          initCronSchedules schema table [failing] silentLogConfig
          _ <- Ops.requestCronRun schema "run-atomic"
          runRequestsAt schema [failing] now

        Just pendingRow <- runM env $ Ops.getCronScheduleByName schema "run-atomic"
        CS.runRequestedAt pendingRow `shouldNotBe` Nothing

        runM env $ runRequestsAt schema [working] now
        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload jobs `shouldBe` [mkSimple "manual"]

        Just firedRow <- runM env $ Ops.getCronScheduleByName schema "run-atomic"
        CS.runRequestedAt firedRow `shouldBe` Nothing

      it "records the manual run at the tick without advancing the gate" $ \env -> do
        let Right cron = cronJob "run-gate" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "gate"))
            now = mkTime 2025 6 15 12 30 45
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          _ <- Ops.requestCronRun schema "run-gate"
          runRequestsAt schema [cron] now

        Just row <- runM env $ Ops.getCronScheduleByName schema "run-gate"
        CS.lastManualRunAt row `shouldBe` Just (mkTime 2025 6 15 12 30 0)
        CS.lastFiredAt row `shouldBe` Nothing

      it "leaves last_manual_run_at alone when the run is skipped" $ \env -> do
        let Right cron = cronJob "run-skipmark" "0 3 * * *" SkipOverlap (\_ _ -> defaultJob (mkSimple "skip"))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          _ <- Ops.requestCronRun schema "run-skipmark"
          runRequestsAt schema [cron] (mkTime 2025 6 15 12 30 0)
          _ <- Ops.requestCronRun schema "run-skipmark"
          runRequestsAt schema [cron] (mkTime 2025 6 15 12 31 0)

        Just row <- runM env $ Ops.getCronScheduleByName schema "run-skipmark"
        CS.lastManualRunAt row `shouldBe` Just (mkTime 2025 6 15 12 30 0)

      it "expires a request no pool claimed in time" $ \env -> do
        let Right cron = cronJob "run-expire" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "expire"))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          void $ Ops.requestCronRun schema "run-expire"

        withConn connStr $ \conn ->
          void $
            PG.execute
              conn
              (cronUpdate schema "run_requested_at = NOW() - interval '10 minutes'")
              (Only ("run-expire" :: Text))

        now <- getCurrentTime
        runM env $ runRequestsAt schema [cron] now
        jobs <- runM env $ HL.listJobs 100 0 :: IO [JobRead payload]
        map payload jobs `shouldBe` []

        Just row <- runM env $ Ops.getCronScheduleByName schema "run-expire"
        CS.runRequestedAt row `shouldBe` Nothing

        outcome <- runM env $ Ops.requestCronRun schema "run-expire"
        outcome `shouldBe` Ops.RunReqStamped

      it "AllowOverlap: a manual run does not suppress a Backfill replay" $ \env -> do
        let Right base =
              cronJob
                "run-backfill"
                "0 * * * *"
                AllowOverlap
                (\_ tickAt -> defaultJob (mkSimple (formatMinute tickAt)))
            cron = base {backfill = Backfill 86400}
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          void $ Ops.touchCronChecked schema (mkTime 2025 6 15 9 0 0) ["run-backfill"]
          _ <- Ops.requestCronRun schema "run-backfill"
          runRequestsAt schema [cron] (mkTime 2025 6 15 12 30 45)
          catchUpAt schema table [cron] (mkTime 2025 6 15 12 30 45)

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        let expected = map mkSimple ["2025-06-15T10:00", "2025-06-15T11:00", "2025-06-15T12:00", "2025-06-15T12:30"]
        length jobs `shouldBe` length expected
        map payload jobs `shouldSatisfy` all (`elem` expected)
        expected `shouldSatisfy` all (`elem` map payload jobs)

      it "SkipOverlap: a manual run's active job dedups a Backfill replay" $ \env -> do
        let Right base =
              cronJob
                "run-skipbf"
                "0 * * * *"
                SkipOverlap
                (\_ tickAt -> defaultJob (mkSimple (formatMinute tickAt)))
            cron = base {backfill = Backfill 86400}
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          void $ Ops.touchCronChecked schema (mkTime 2025 6 15 9 0 0) ["run-skipbf"]
          _ <- Ops.requestCronRun schema "run-skipbf"
          runRequestsAt schema [cron] (mkTime 2025 6 15 12 30 45)
          catchUpAt schema table [cron] (mkTime 2025 6 15 12 30 45)

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload jobs `shouldBe` [mkSimple "2025-06-15T12:30"]

      it "does nothing without a pending request" $ \env -> do
        let Right cron = cronJob "run-none" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "nope"))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          runRequestsAt schema [cron] (mkTime 2025 6 15 12 30 0)

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 0

      it "passes the builder a minute-truncated Live tick" $ \env -> do
        let Right cron =
              cronJob
                "run-tick"
                "0 3 * * *"
                AllowOverlap
                (\_ tickAt -> defaultJob (mkSimple (T.pack (show tickAt))))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          _ <- Ops.requestCronRun schema "run-tick"
          runRequestsAt schema [cron] (mkTime 2025 6 15 12 30 45)

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        map payload jobs `shouldBe` [mkSimple (T.pack (show (mkTime 2025 6 15 12 30 0)))]

      it "refuses a second request while one is still pending" $ \env -> do
        let Right cron = cronJob "run-coalesce" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "once"))
        outcomes <- runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          first <- Ops.requestCronRun schema "run-coalesce"
          second <- Ops.requestCronRun schema "run-coalesce"
          pure (first, second)
        outcomes `shouldBe` (Ops.RunReqStamped, Ops.RunReqPending)

      it "accepts a fresh request once the pending one is claimed" $ \env -> do
        let Right cron = cronJob "run-again" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "again"))
        outcome <- runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          _ <- Ops.requestCronRun schema "run-again"
          _ <- Ops.claimCronRun schema "run-again"
          Ops.requestCronRun schema "run-again"
        outcome `shouldBe` Ops.RunReqStamped

      it "claims a request exactly once across pools" $ \env -> do
        let Right cron = cronJob "run-once" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "once"))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          void $ Ops.requestCronRun schema "run-once"

        won <- runM env $ Ops.claimCronRun schema "run-once"
        lost <- runM env $ Ops.claimCronRun schema "run-once"
        fmap CS.name won `shouldBe` Just "run-once"
        fmap CS.name lost `shouldBe` Nothing

      it "SkipOverlap: a request is skipped while a job is already active" $ \env -> do
        let Right cron = cronJob "run-skip" "0 3 * * *" SkipOverlap (\_ _ -> defaultJob (mkSimple "skip"))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          _ <- Ops.requestCronRun schema "run-skip"
          runRequestsAt schema [cron] (mkTime 2025 6 15 12 30 0)
          _ <- Ops.requestCronRun schema "run-skip"
          runRequestsAt schema [cron] (mkTime 2025 6 15 12 31 0)

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 1

      it "disabling a schedule drops its pending request" $ \env -> do
        let Right cron = cronJob "run-off" "0 3 * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "nope"))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          _ <- Ops.requestCronRun schema "run-off"
          _ <-
            Ops.updateCronSchedule
              schema
              "run-off"
              CronScheduleUpdate
                { overrideExpression = Nothing
                , overrideOverlap = Nothing
                , overrideTimezone = Nothing
                , enabled = Just False
                }
          runRequestsAt schema [cron] (mkTime 2025 6 15 12 30 0)

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 0
        Just row <- runM env $ Ops.getCronScheduleByName schema "run-off"
        CS.runRequestedAt row `shouldBe` Nothing

    describe "initCronSchedules" $ do
      it "upserts rows" $ \env -> do
        let Right cj1 = cronJob "test-a" "0 3 * * *" SkipOverlap (\_ _ -> defaultJob (mkSimple "a"))
            Right cj2 = cronJob "test-b" "*/5 * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "b"))
        -- Phase 1: Insert
        runM env $ initCronSchedules schema table [cj1, cj2] silentLogConfig
        rows <- runM env $ Ops.listCronSchedules schema Nothing
        length rows `shouldBe` 2
        map CS.name rows `shouldBe` ["test-a", "test-b"]
        map CS.defaultExpression rows `shouldBe` ["0 3 * * *", "*/5 * * * *"]

        -- Phase 2: Re-upsert with a modified expression. The row is updated in place.
        let Right cj1' = cronJob "test-a" "*/10 * * * *" SkipOverlap (\_ _ -> defaultJob (mkSimple "a"))
        runM env $ initCronSchedules schema table [cj1', cj2] silentLogConfig
        rows2 <- runM env $ Ops.listCronSchedules schema Nothing
        length rows2 `shouldBe` 2
        map CS.defaultExpression rows2 `shouldBe` ["*/10 * * * *", "*/5 * * * *"]

      it "skips disabled schedules" $ \env -> do
        let Right cron = cronJob "disabled-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "should-skip"))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          _ <-
            Ops.updateCronSchedule
              schema
              "disabled-test"
              CronScheduleUpdate
                { overrideExpression = Nothing
                , overrideOverlap = Nothing
                , overrideTimezone = Nothing
                , enabled = Just False
                }
          catchUpAt schema table [cron] (mkTime 2025 6 15 12 0 0)

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 0

      it "uses DB expression override over default" $ \env -> do
        -- Create a schedule that fires every minute
        let Right cron = cronJob "override-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "override"))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          _ <-
            Ops.updateCronSchedule
              schema
              "override-test"
              CronScheduleUpdate
                { overrideExpression = Just (Just "0 3 * * *")
                , overrideOverlap = Nothing
                , overrideTimezone = Nothing
                , enabled = Nothing
                }
          -- A tick at 12:00 does not fire under the 3am override.
          catchUpAt schema table [cron] (mkTime 2025 6 15 12 0 0)

        jobs <- runM env $ HL.claimNextVisibleJobs 10 60 :: IO [JobRead payload]
        length jobs `shouldBe` 0

      it "updates last_fired_at on successful fire" $ \env -> do
        let Right cron = cronJob "fire-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "fire"))
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] (mkTime 2025 6 15 12 0 0)

        mRow <- runM env $ Ops.getCronScheduleByName schema "fire-test"
        case mRow of
          Nothing -> expectationFailure "Expected cron schedule row to exist"
          Just row -> CS.lastFiredAt row `shouldSatisfy` isJust

    describe "processCronCatchUp" $ do
      it "fires every missed minute for schedules with Backfill policy" $ \env -> do
        -- Simulate a scheduler wake-up after a 5-minute gap. last_checked_at
        -- is 5 minutes in the past. With Backfill 600 (10 min window) the
        -- catch-up fires a job for each missed minute.
        let Right base =
              cronJob
                "catchup-backfill"
                "* * * * *"
                AllowOverlap
                (\_ tickAt -> defaultJob (mkSimple (formatMinute tickAt)))
            cron = base {backfill = Backfill 600}
        runM env $ initCronSchedules schema table [cron] silentLogConfig

        withConn connStr $ \conn ->
          void $
            PG.execute
              conn
              (cronUpdate schema "last_checked_at = NOW() - interval '5 minutes'")
              (Only ("catchup-backfill" :: Text))

        now <- getCurrentTime
        runM env $ catchUpAt schema table [cron] now

        jobs <- runM env $ HL.listJobs 100 0 :: IO [JobRead payload]
        -- Expect at least 5 missed minutes plus the current one.
        length jobs `shouldSatisfy` (>= 5)

      it "does not replay missed minutes for NoBackfill schedules" $ \env -> do
        -- NoBackfill replays no stale ticks. With a stale last_checked_at, only
        -- the current minute fires.
        let Right cron =
              cronJob
                "catchup-nobackfill"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "no-replay"))
        runM env $ initCronSchedules schema table [cron] silentLogConfig

        withConn connStr $ \conn ->
          void $
            PG.execute
              conn
              (cronUpdate schema "last_checked_at = NOW() - interval '5 minutes'")
              (Only ("catchup-nobackfill" :: Text))

        now <- getCurrentTime
        runM env $ catchUpAt schema table [cron] now

        jobs <- runM env $ HL.listJobs 100 0 :: IO [JobRead payload]
        length jobs `shouldBe` 1

      it "fires only the current minute when last_checked_at is null" $ \env -> do
        -- A fresh schedule with no last_checked_at replays no earlier ticks,
        -- whatever the policy.
        let Right base =
              cronJob
                "fresh"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "fresh"))
            cron = base {backfill = Backfill 3600}
        runM env $ initCronSchedules schema table [cron] silentLogConfig

        now <- getCurrentTime
        runM env $ catchUpAt schema table [cron] now

        jobs <- runM env $ HL.listJobs 100 0 :: IO [JobRead payload]
        length jobs `shouldBe` 1

      it "sets last_checked_at to currentTick" $ \env -> do
        -- Slow-processing regression. Watermark must equal the 'now' passed in.
        let Right cron =
              cronJob "watermark" "* * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "x"))
            currentTickPast = mkTime 2025 6 15 12 0 0
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] currentTickPast

        rows <- runM env $ Ops.listCronSchedules schema Nothing
        case lookup "watermark" [(CS.name row, row) | row <- rows] of
          Just row -> CS.lastCheckedAt row `shouldBe` Just currentTickPast
          Nothing -> expectationFailure "watermark schedule missing"

      it "does not advance last_checked_at backwards" $ \env -> do
        -- GREATEST guard for concurrent pools with skewed clocks.
        let Right cron =
              cronJob "monotonic" "* * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "x"))
            later = mkTime 2025 6 15 12 5 0
            earlier = mkTime 2025 6 15 12 0 0
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] later
          catchUpAt schema table [cron] earlier

        rows <- runM env $ Ops.listCronSchedules schema Nothing
        case lookup "monotonic" [(CS.name row, row) | row <- rows] of
          Just row -> CS.lastCheckedAt row `shouldBe` Just later
          Nothing -> expectationFailure "monotonic schedule missing"

      it "does not re-fire ticks whose jobs were already processed" $ \env -> do
        -- After firing and processing the live tick, a second call at the
        -- same minute produces no duplicate. The dedup row is gone once the
        -- job is acked. The watermark blocks the re-fire.
        let Right cron =
              cronJob
                "no-duplicate"
                "* * * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "once"))
            tick = mkTime 2025 6 15 12 0 0
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] tick

        claimed <- runM env $ HL.claimNextVisibleJobs 100 60 :: IO [JobRead payload]
        length claimed `shouldBe` 1
        runM env $ traverse_ (void . HL.ackJob) claimed

        runM env $ catchUpAt schema table [cron] tick
        afterRetry <- runM env $ HL.listJobs 100 0 :: IO [JobRead payload]
        afterRetry `shouldBe` []

      it "DST fall-back: a fixed-time schedule fires once after its first job is acked" $ \env -> do
        -- 01:30 America/New_York on 2025-11-02 reads at 05:30Z and 06:30Z.
        let Right cron =
              cronJobInTimezone
                "fall-back"
                "America/New_York"
                "30 1 * * *"
                AllowOverlap
                (\_ _ -> defaultJob (mkSimple "once"))
            firstPass = mkTime 2025 11 2 5 30 0
            secondPass = mkTime 2025 11 2 6 30 0
        runM env $ do
          initCronSchedules schema table [cron] silentLogConfig
          catchUpAt schema table [cron] firstPass

        claimed <- runM env $ HL.claimNextVisibleJobs 100 60 :: IO [JobRead payload]
        length claimed `shouldBe` 1
        runM env $ traverse_ (void . HL.ackJob) claimed

        runM env $ catchUpAt schema table [cron] secondPass
        afterSecond <- runM env $ HL.listJobs 100 0 :: IO [JobRead payload]
        afterSecond `shouldBe` []

      it "gate prevents double-fire when last_fired_at already covers the minute" $ \env -> do
        -- Simulates a fast pool that already fired and acked 12:00. The gate
        -- blocks a slow pool retrying the same minute.
        let Right cron = cronJob "skew-race" "* * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "skew"))
            tick = mkTime 2025 6 15 12 0 0
        runM env $ initCronSchedules schema table [cron] silentLogConfig
        withConn connStr $ \conn ->
          void $
            PG.execute
              conn
              (cronUpdate schema "last_fired_at = ?")
              (tick, "skew-race" :: Text)
        runM env $ catchUpAt schema table [cron] tick
        jobs <- runM env $ HL.listJobs 100 0 :: IO [JobRead payload]
        length jobs `shouldBe` 0

      it "gate lets the next minute through after firing the previous one" $ \env -> do
        let Right cron = cronJob "skew-advance" "* * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "advance"))
            tickPrev = mkTime 2025 6 15 12 0 0
            tickNext = mkTime 2025 6 15 12 1 0
        runM env $ initCronSchedules schema table [cron] silentLogConfig
        withConn connStr $ \conn ->
          void $
            PG.execute
              conn
              (cronUpdate schema "last_fired_at = ?")
              (tickPrev, "skew-advance" :: Text)
        runM env $ catchUpAt schema table [cron] tickNext
        jobs <- runM env $ HL.listJobs 100 0 :: IO [JobRead payload]
        length jobs `shouldBe` 1

    describe "cron concurrency primitives" $ do
      it "touchCronChecked advances last_checked_at monotonically and matches by name" $ \env -> do
        let Right cron = cronJob "touch-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "x"))
            tEarly = mkTime 2025 6 15 12 0 0
            tLate = mkTime 2025 6 15 12 5 0
        runM env $ initCronSchedules schema table [cron] silentLogConfig

        nLate <- runM env $ Ops.touchCronChecked schema tLate ["touch-test"]
        nLate `shouldBe` 1
        Just rowLate <- runM env $ Ops.getCronScheduleByName schema "touch-test"
        CS.lastCheckedAt rowLate `shouldBe` Just tLate

        -- An earlier watermark matches the row and leaves it in place.
        nEarly <- runM env $ Ops.touchCronChecked schema tEarly ["touch-test"]
        nEarly `shouldBe` 1
        Just rowEarly <- runM env $ Ops.getCronScheduleByName schema "touch-test"
        CS.lastCheckedAt rowEarly `shouldBe` Just tLate

        -- An unknown name matches nothing.
        nMiss <- runM env $ Ops.touchCronChecked schema tLate ["no-such-schedule"]
        nMiss `shouldBe` 0

      it "tryFireCronGate fires once per minute floor" $ \env -> do
        let Right cron = cronJob "gate-test" "* * * * *" AllowOverlap (\_ _ -> defaultJob (mkSimple "x"))
            minuteZero = mkTime 2025 6 15 12 0 0
            minuteOne = mkTime 2025 6 15 12 1 0
        runM env $ initCronSchedules schema table [cron] silentLogConfig

        firstFire <- runM env $ Ops.tryFireCronGate schema "gate-test" minuteZero
        firstFire `shouldBe` True
        -- The same minute floor cannot fire twice.
        secondFire <- runM env $ Ops.tryFireCronGate schema "gate-test" minuteZero
        secondFire `shouldBe` False
        -- A later minute floor fires again.
        nextFire <- runM env $ Ops.tryFireCronGate schema "gate-test" minuteOne
        nextFire `shouldBe` True

      it "tryAcquireCronLeader is mutually exclusive per (schema, queue, name) and releases on commit" $ \env -> do
        let acquire = Ops.tryAcquireCronLeader schema table
        held <- newEmptyMVar
        release <- newEmptyMVar
        -- The advisory lock is transaction-scoped. The holder's transaction stays open to hold it.
        let holdLeader = runM env . withDbTransaction $ do
              got <- acquire "leader"
              liftIO (putMVar held got)
              liftIO (takeMVar release)
        withAsync holdLeader $ \holder -> do
          got1 <- takeMVar held
          got1 `shouldBe` True

          -- The holder still has "leader". A second transaction loses on the same key.
          -- A different schedule name is independent.
          (got2, got3) <- runM env . withDbTransaction $ (,) <$> acquire "leader" <*> acquire "other"
          got2 `shouldBe` False
          got3 `shouldBe` True

          -- Once the holder commits, "leader" is free for a fresh transaction.
          putMVar release ()
          wait holder
          got4 <- runM env . withDbTransaction $ acquire "leader"
          got4 `shouldBe` True

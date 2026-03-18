{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Simple.Concurrency (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Test.Concurrency
  ( concurrencySpec
  , countHolViolations
  , groupsConsistencyStressSpec
  , inFlightConcurrencySpec
  , installHolDetector
  , raceConditionSpec
  , removeHolDetector
  )
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Setup (setupOnce)
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, replicateM_, void, when)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Maybe (fromJust)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec
import UnliftIO.Async (mapConcurrently)

import Arbiter.Simple.MonadArbiter (SimpleConnectionPool (..))
import Arbiter.Simple.SimpleDb (SimpleEnv (..), createSimpleEnvWithPool, inTransaction, runSimpleDb)
import Test.Arbiter.Simple.TestHelpers (cleanupSimpleTest, createSimplePool)

testSchema :: Text
testSchema = "arbiter_simple_concurrency_test"

type SimpleConcurrencyTestRegistry = '[ '("arbiter_simple_concurrency_test", TestPayload)]

testTable :: Text
testTable = "arbiter_simple_concurrency_test"

withCleanup
  :: Pool PG.Connection -> (SimpleEnv SimpleConcurrencyTestRegistry -> IO a) -> IO a
withCleanup sharedPool action = do
  let env = createSimpleEnvWithPool (Proxy @SimpleConcurrencyTestRegistry) sharedPool testSchema
  cleanupSimpleTest env testSchema testTable
  action env

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  sharedPool <- runIO (createSimplePool 10 connStr)
  let withConn :: SimpleEnv SimpleConcurrencyTestRegistry -> (PG.Connection -> IO ()) -> IO ()
      withConn env f = withResource (fromJust (connectionPool (simplePool env))) f
  around (withCleanup sharedPool) $ do
    concurrencySpec @TestPayload TestMessage runSimpleDb
    raceConditionSpec @TestPayload TestMessage runSimpleDb
    groupsConsistencyStressSpec @TestPayload
      testSchema
      testTable
      TestMessage
      runSimpleDb
      withConn
    inFlightConcurrencySpec @TestPayload
      testSchema
      testTable
      TestMessage
      runSimpleDb
      withConn

    describe "Group Serialization Race (localConnection)" $ do
      it "groups trigger serializes concurrent inserts within a group" $ \env -> do
        violationsRef <- newIORef (0 :: Int)

        replicateM_ 500 $ do
          connSlow <- PG.connectPostgreSQL connStr
          _ <- PG.execute_ connSlow "BEGIN"

          -- Pod 1: insert within slow transaction (groups trigger holds row lock)
          Just jobA <-
            inTransaction @SimpleConcurrencyTestRegistry connSlow testSchema $
              HL.insertJob ((defaultJob (TestMessage "SlowPod")) {groupKey = Just "serialize"})

          -- Pod 2 insert + concurrent claims + Pod 1 commit — all concurrent.
          -- Without groups trigger serialization, Pod 2 could commit first and
          -- be claimed before Pod 1's transaction commits.
          results <-
            mapConcurrently
              id
              $ replicate 10 (runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload])
                <> [ do
                       void $
                         runSimpleDb env $
                           HL.insertJob ((defaultJob (TestMessage "FastPod")) {groupKey = Just "serialize"})
                       pure []
                   , do
                       _ <- PG.execute_ connSlow "COMMIT"
                       PG.close connSlow
                       pure []
                   ]

          let allClaimed = concat results
          -- Any claimed job from this group must be Pod 1's (lower id).
          -- Pod 2's job being claimed first would mean it committed before
          -- Pod 1 — a serialization failure.
          forM_ allClaimed $ \j ->
            when (groupKey j == Just "serialize" && primaryKey j /= primaryKey jobA) $
              atomicModifyIORef' violationsRef (\n -> (n + 1, ()))

          forM_ allClaimed $ \j -> void $ runSimpleDb env (HL.ackJob j)
          let drain = do
                c <- runSimpleDb env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead TestPayload]
                if null c
                  then pure ()
                  else do
                    forM_ c $ \j -> void $ runSimpleDb env (HL.ackJob j)
                    drain
          drain

        violations <- readIORef violationsRef
        violations `shouldBe` 0

      it "out-of-order inserts do not cause HOL violations" $ \env -> do
        withConn env $ \conn -> installHolDetector conn testSchema testTable

        doneRef <- newIORef False
        let inserter = do
              replicateM_ 200 $
                void $
                  runSimpleDb env $
                    HL.insertJob (defaultJob (TestMessage "ooo")) {groupKey = Just "ooo-race"}
              atomicModifyIORef' doneRef (const (True, ()))

            claimer = do
              let go = do
                    done <- readIORef doneRef
                    claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
                    forM_ claimed $ \job -> void $ runSimpleDb env (HL.ackJob job)
                    if done && null claimed
                      then pure ()
                      else do when (null claimed) $ threadDelay 1_000; go
              go

        _ <-
          mapConcurrently id $
            replicate 5 inserter <> replicate 10 claimer

        -- Drain stragglers
        let drain = do
              c <- runSimpleDb env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead TestPayload]
              if null c
                then pure ()
                else do forM_ c $ \j -> void $ runSimpleDb env (HL.ackJob j); drain
        drain

        withConn env $ \conn -> do
          countHolViolations conn testSchema testTable >>= (`shouldBe` 0)
          removeHolDetector conn testSchema testTable

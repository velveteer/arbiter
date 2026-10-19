{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Simple.Concurrency (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Concurrency
  ( concurrencySpec
  , drainAll
  , holViolations
  , installHolDetector
  , raceConditionSpec
  , removeHolDetector
  )
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Poll (bumpRef)
import Arbiter.Test.Setup (cleanupData, concurrencyPoolSize, createPoolOf, setupOnce, withConn)
import Control.Concurrent (threadDelay)
import Control.Monad (replicateM_, void, when)
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec
import UnliftIO.Async (mapConcurrently)

import Arbiter.Simple.SimpleDb (createSimpleEnvWithPool, inTransaction, runSimpleDb)

testSchema :: Text
testSchema = "arbiter_simple_concurrency_test"

type SimpleConcurrencyTestRegistry = '[Queue "arbiter_simple_concurrency_test" TestPayload]

testTable :: Text
testTable = "arbiter_simple_concurrency_test"

-- | Rounds of the group serialization race.
groupSerializationRounds :: Int
groupSerializationRounds = 500

-- | Wait before a claimer polls again after an empty claim.
claimerIdleDelayMicros :: Int
claimerIdleDelayMicros = 1_000

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  sharedPool <- runIO (createPoolOf concurrencyPoolSize connStr)
  sharedEnv <- runIO (createSimpleEnvWithPool (Proxy @SimpleConcurrencyTestRegistry) sharedPool testSchema)
  around (\action -> withResource sharedPool (cleanupData testSchema testTable) >> action sharedEnv) $ do
    concurrencySpec @TestPayload TestMessage runSimpleDb
    raceConditionSpec @TestPayload TestMessage runSimpleDb

    describe "Group Serialization Race (inTransaction)" $ do
      it "groups trigger serializes concurrent inserts within a group" $ \env -> do
        violationsRef <- newIORef (0 :: Int)

        replicateM_ groupSerializationRounds $ withConn connStr $ \connSlow -> do
          _ <- PG.execute_ connSlow "BEGIN"

          -- Pod 1 inserts inside the slow transaction. The groups trigger holds the row lock.
          Just jobA <-
            inTransaction @SimpleConcurrencyTestRegistry connSlow testSchema $
              HL.insertJob (setGroupKey (Just "serialize") $ defaultJob (TestMessage "SlowPod"))

          -- Pod 2 insert, concurrent claims, and Pod 1 commit run at the same time.
          results <-
            mapConcurrently
              id
              $ replicate 10 (runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload])
                <> [ do
                       void
                         $ runSimpleDb env
                         $ HL.insertJob (setGroupKey (Just "serialize") $ defaultJob (TestMessage "FastPod"))
                       pure []
                   , do
                       _ <- PG.execute_ connSlow "COMMIT"
                       pure []
                   ]

          let allClaimed = concat results
          -- Any claimed job from this group must be Pod 1's job, which has the lower id.
          for_ allClaimed $ \job ->
            when (groupKey job == Just "serialize" && primaryKey job /= primaryKey jobA) $
              bumpRef violationsRef

          for_ allClaimed $ \job -> void $ runSimpleDb env (HL.ackJob job)
          drainAll
            (runSimpleDb env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead TestPayload])
            (void . runSimpleDb env . HL.ackJob)

        violations <- readIORef violationsRef
        violations `shouldBe` 0

      it "out-of-order inserts do not cause HOL violations" $ \env -> do
        withResource sharedPool $ \conn -> installHolDetector conn testSchema testTable

        doneRef <- newIORef False
        let inserter = do
              replicateM_ 200
                $ void
                $ runSimpleDb env
                $ HL.insertJob
                $ setGroupKey (Just "ooo-race")
                $ defaultJob (TestMessage "ooo")
              atomicModifyIORef' doneRef (const (True, ()))

            claimer = do
              let go = do
                    done <- readIORef doneRef
                    claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
                    for_ claimed $ \job -> void $ runSimpleDb env (HL.ackJob job)
                    if done && null claimed
                      then pure ()
                      else do when (null claimed) $ threadDelay claimerIdleDelayMicros; go
              go

        _ <-
          mapConcurrently id $
            replicate 5 inserter <> replicate 10 claimer

        -- Drain stragglers
        drainAll
          (runSimpleDb env (HL.claimNextVisibleJobs 100 60) :: IO [JobRead TestPayload])
          (void . runSimpleDb env . HL.ackJob)

        withResource sharedPool $ \conn -> do
          holViolations conn testSchema testTable >>= (`shouldBe` [])
          removeHolDetector conn testSchema testTable

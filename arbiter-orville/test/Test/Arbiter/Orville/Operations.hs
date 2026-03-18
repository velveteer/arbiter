{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Orville.Operations (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Operations (operationsSpec)
import Control.Exception (SomeException, catch, throwIO)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Orville.PostgreSQL qualified as O
import Test.Hspec

import Test.Arbiter.Orville.TestHelpers (cleanupOrvilleTest, runOrvilleTest, setupOrvilleTest)

testSchema :: Text
testSchema = "arbiter_orville_ops_test"

type OrvilleOpsTestRegistry = '[ '("arbiter_orville_ops_test", TestPayload)]

testTable :: Text
testTable = "arbiter_orville_ops_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOrvilleTest connStr testSchema testTable 5) $ beforeWith (\env -> cleanupOrvilleTest env >> pure env) $ do
  operationsSpec @TestPayload @OrvilleOpsTestRegistry TestMessage (runOrvilleTest @OrvilleOpsTestRegistry)

  describe "Transaction Participation" $ do
    it "commits job insertion within user transaction" $ \env -> do
      let job = defaultJob (TestMessage "InTx")

      runOrvilleTest env $ do
        O.withTransaction $ do
          _ <- HL.insertJob job
          pure ()

      -- Job should be in the queue (transaction committed)
      claimed <- runOrvilleTest env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
      length claimed `shouldBe` 1
      payload (head claimed) `shouldBe` TestMessage "InTx"

    it "rolls back job insertion when user transaction fails" $ \env -> do
      let job = defaultJob (TestMessage "RollbackTest")

      -- Start a user transaction and intentionally fail it
      result <-
        ( runOrvilleTest env $ do
            O.withTransaction $ do
              _ <- HL.insertJob job
              -- Force a rollback by throwing an error
              liftIO $ throwIO (userError "Intentional rollback")
        )
          `catch` \(e :: SomeException) -> pure (show e)

      result `shouldContain` "Intentional rollback"

      -- Job should NOT be in the queue (transaction rolled back)
      claimed <- runOrvilleTest env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
      length claimed `shouldBe` 0

    it "shares transaction with user's database operations" $ \env -> do
      let job = (defaultJob (TestMessage "SharedTx")) {groupKey = Just "g1"}
      let testDLQJob =
            (defaultJob (TestMessage "TestDLQ"))
              { groupKey = Just "g2"
              }

      -- Start transaction with both user operation and job insertion
      runOrvilleTest env $ do
        O.withTransaction $ do
          _ <- HL.insertJob testDLQJob
          _ <- HL.insertJob job
          pure ()

      -- Both jobs should be committed
      claimed <- runOrvilleTest env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead TestPayload]
      length claimed `shouldBe` 2

      -- Verify both were inserted (user's DLQ marker and the main job)
      map payload claimed `shouldMatchList` [TestMessage "SharedTx", TestMessage "TestDLQ"]

    it "rolls back both user operations and job when transaction fails" $ \env -> do
      let job1 = (defaultJob (TestMessage "FirstJob")) {groupKey = Just "g1"}
      let job2 = (defaultJob (TestMessage "SecondJob")) {groupKey = Just "g2"}

      -- Start transaction, insert two jobs, then fail
      result <-
        ( runOrvilleTest env $ do
            O.withTransaction $ do
              _ <- HL.insertJob job1
              _ <- HL.insertJob job2

              -- Force rollback
              liftIO $ throwIO (userError "Force rollback")
        )
          `catch` \(e :: SomeException) -> pure (show e)

      result `shouldContain` "Force rollback"

      -- Both jobs should NOT be in queue (transaction rolled back)
      claimed <- runOrvilleTest env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead TestPayload]
      length claimed `shouldBe` 0

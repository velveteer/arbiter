{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Orville.Operations (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Operations (operationsSpec)
import Arbiter.Test.Setup (sharedPoolSize)
import Control.Exception (SomeException, catch, throwIO)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Text (Text)
import Orville.PostgreSQL qualified as O
import Test.Hspec

import Test.Arbiter.Orville.TestHelpers (cleanupOrvilleTest, runOrvilleTest, setupOrvilleTest)

testSchema :: Text
testSchema = "arbiter_orville_ops_test"

type OrvilleOpsTestRegistry = '[QueueWithResult "arbiter_orville_ops_test" TestPayload [Text]]

testTable :: Text
testTable = "arbiter_orville_ops_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOrvilleTest connStr testSchema testTable sharedPoolSize) $ beforeWith (\env -> cleanupOrvilleTest env >> pure env) $ do
  operationsSpec @TestPayload TestMessage pure (runOrvilleTest @OrvilleOpsTestRegistry)

  describe "Transaction Participation" $ do
    it "commits job insertion within user transaction" $ \env -> do
      let job = defaultJob (TestMessage "InTx")

      runOrvilleTest env (O.withTransaction (void (HL.insertJob job)))

      claimed <- runOrvilleTest env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
      map payload claimed `shouldBe` [TestMessage "InTx"]

    it "rolls back job insertion when user transaction fails" $ \env -> do
      let job = defaultJob (TestMessage "RollbackTest")

      result <-
        ( runOrvilleTest env $ do
            O.withTransaction $ do
              _ <- HL.insertJob job
              liftIO $ throwIO (userError "Intentional rollback")
        )
          `catch` \(exception :: SomeException) -> pure (show exception)

      result `shouldContain` "Intentional rollback"

      claimed <- runOrvilleTest env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
      claimed `shouldSatisfy` null

    it "commits two job inserts in one transaction" $ \env -> do
      let job1 = setGroupKey (Just "g1") $ defaultJob (TestMessage "SharedTx")
      let job2 = setGroupKey (Just "g2") $ defaultJob (TestMessage "SharedTx2")

      runOrvilleTest env (O.withTransaction (traverse_ HL.insertJob [job2, job1]))

      claimed <- runOrvilleTest env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead TestPayload]
      map payload claimed `shouldMatchList` [TestMessage "SharedTx", TestMessage "SharedTx2"]

    it "rolls back both job inserts when the transaction fails" $ \env -> do
      let job1 = setGroupKey (Just "g1") $ defaultJob (TestMessage "FirstJob")
      let job2 = setGroupKey (Just "g2") $ defaultJob (TestMessage "SecondJob")

      result <-
        ( runOrvilleTest env $ do
            O.withTransaction $ do
              traverse_ HL.insertJob [job1, job2]
              liftIO $ throwIO (userError "Force rollback")
        )
          `catch` \(exception :: SomeException) -> pure (show exception)

      result `shouldContain` "Force rollback"

      claimed <- runOrvilleTest env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead TestPayload]
      claimed `shouldSatisfy` null

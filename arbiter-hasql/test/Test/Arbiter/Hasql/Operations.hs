{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Hasql.Operations (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Core.MonadArbiter (withDbTransaction)
import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Operations (operationsSpec)
import Arbiter.Test.Setup (setupOnce)
import Control.Exception (SomeException, catch, throwIO)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Maybe (fromJust)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Test.Hspec

import Arbiter.Hasql.Compat qualified as Compat
import Arbiter.Hasql.HasqlDb (HasqlEnv (..), createHasqlEnvWithPool, inTransaction, runHasqlDb)
import Arbiter.Hasql.MonadArbiter (HasqlConnectionPool (..))
import Test.Arbiter.Hasql.TestHelpers (cleanupHasqlTest, createHasqlPool)

testSchema :: Text
testSchema = "arbiter_hasql_ops_test"

type HasqlOpsTestRegistry = '[QueueWithResult "arbiter_hasql_ops_test" TestPayload [Text]]

testTable :: Text
testTable = "arbiter_hasql_ops_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  sharedPool <- runIO (createHasqlPool 5 connStr)
  mkEnv <- runIO (createHasqlEnvWithPool (Proxy @HasqlOpsTestRegistry) sharedPool testSchema)
  around (\action -> cleanupHasqlTest connStr testSchema testTable >> action mkEnv) $ do
    operationsSpec @TestPayload TestMessage pure runHasqlDb

    describe "Transaction Participation (inTransaction)" $ do
      it "commits job insertion within user transaction" $ \env -> do
        let job = defaultJob (TestMessage "InTx")

        withResource (fromJust $ connectionPool (hasqlPool env)) $ \conn -> do
          Compat.runSQL conn "BEGIN"
          inTransaction @HasqlOpsTestRegistry conn testSchema $ do
            _ <- HL.insertJob job
            pure ()
          Compat.runSQL conn "COMMIT"
          pure ()

        claimed <- runHasqlDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 1
        payload (head claimed) `shouldBe` TestMessage "InTx"

      it "rolls back job insertion when user transaction fails" $ \env -> do
        let job = defaultJob (TestMessage "RollbackTest")

        result <-
          ( withResource (fromJust $ connectionPool (hasqlPool env)) $ \conn -> do
              Compat.runSQL conn "BEGIN"
              inTransaction @HasqlOpsTestRegistry conn testSchema $ do
                _ <- HL.insertJob job
                pure ()
              Compat.runSQL conn "ROLLBACK"
              pure ("rolled back" :: String)
          )
            `catch` \(exception :: SomeException) -> pure (show exception)

        result `shouldContain` "rolled back"

        claimed <- runHasqlDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 0

      it "shares transaction with user's database operations" $ \env -> do
        let job1 = setGroupKey (Just "g1") $ defaultJob (TestMessage "SharedTx")
        let job2 = setGroupKey (Just "g2") $ defaultJob (TestMessage "SharedTx2")

        runHasqlDb env $ do
          withDbTransaction $ do
            _ <- HL.insertJob job1
            _ <- HL.insertJob job2
            pure ()

        claimed <- runHasqlDb env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 2
        map payload claimed `shouldMatchList` [TestMessage "SharedTx", TestMessage "SharedTx2"]

      it "rolls back both user operations and job when transaction fails" $ \env -> do
        let job1 = setGroupKey (Just "g1") $ defaultJob (TestMessage "FirstJob")
        let job2 = setGroupKey (Just "g2") $ defaultJob (TestMessage "SecondJob")

        result <-
          ( runHasqlDb env $ do
              withDbTransaction $ do
                _ <- HL.insertJob job1
                _ <- HL.insertJob job2
                liftIO $ throwIO (userError "Force rollback")
          )
            `catch` \(exception :: SomeException) -> pure (show exception)

        result `shouldContain` "Force rollback"

        claimed <- runHasqlDb env (HL.claimNextVisibleJobs 2 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 0

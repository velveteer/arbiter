{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Simple.Operations (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Operations (operationsSpec)
import Arbiter.Test.Setup (cleanupData, createSharedPool, execute_, setupOnce, withConn)
import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, catch, throwIO)
import Control.Monad (void)
import Data.ByteString (ByteString)
import Data.Pool (withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec
import UnliftIO.Async (async, wait)

import Arbiter.Simple.SimpleDb (createSimpleEnvWithPool, inTransaction, runSimpleDb)

-- | A pause for the second transaction to start and block.
blockMicros :: Int
blockMicros = 100_000

testSchema :: Text
testSchema = "arbiter_simple_test"

type SimpleOpsTestRegistry = '[QueueWithResult "arbiter_simple_test" TestPayload [Text]]

testTable :: Text
testTable = "arbiter_simple_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  sharedPool <- runIO (createSharedPool connStr)
  sharedEnv <- runIO (createSimpleEnvWithPool (Proxy @SimpleOpsTestRegistry) sharedPool testSchema)
  around (\action -> withResource sharedPool (cleanupData testSchema testTable) >> action sharedEnv) $ do
    operationsSpec @TestPayload TestMessage pure runSimpleDb

    describe "Transaction Participation (inTransaction)" $ do
      it "commits job insertion within user transaction" $ \env -> do
        let job = setGroupKey (Just "g1") $ defaultJob (TestMessage "InTx")

        withResource sharedPool $ \conn ->
          PG.withTransaction conn $ do
            Just _inserted <- inTransaction @SimpleOpsTestRegistry conn testSchema $ HL.insertJob job
            pure ()

        claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        map payload claimed `shouldBe` [TestMessage "InTx"]

      it "rolls back job insertion when user transaction fails" $ \env -> do
        let job = setGroupKey (Just "g1") $ defaultJob (TestMessage "RollbackTest")

        result <-
          ( withResource sharedPool $ \conn ->
              PG.withTransaction conn $ do
                Just _inserted <- inTransaction @SimpleOpsTestRegistry conn testSchema $ HL.insertJob job
                throwIO (userError "Intentional rollback")
          )
            `catch` \(exception :: SomeException) -> pure (show exception)

        result `shouldContain` "Intentional rollback"

        claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        claimed `shouldSatisfy` null

      it "shares transaction with user's database operations" $ \env -> do
        let job = setGroupKey (Just "g1") $ defaultJob (TestMessage "SharedTx")

        withResource sharedPool $ \conn -> do
          execute_ conn "CREATE TEMP TABLE test_tx_table (value TEXT)"

          PG.withTransaction conn $ do
            execute_ conn "INSERT INTO test_tx_table VALUES ('test-value')"
            Just _inserted <- inTransaction @SimpleOpsTestRegistry conn testSchema $ HL.insertJob job
            pure ()

          result <- PG.query_ conn "SELECT value FROM test_tx_table" :: IO [[Text]]
          result `shouldBe` [["test-value"]]

        claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        map payload claimed `shouldBe` [TestMessage "SharedTx"]

      it "rolls back both user operations and job when transaction fails" $ \env -> do
        let job = setGroupKey (Just "g1") $ defaultJob (TestMessage "BothRollback")

        withResource sharedPool $ \conn -> do
          execute_ conn "CREATE TEMP TABLE test_rollback_table (value TEXT)"

          result <-
            ( PG.withTransaction conn $ do
                execute_ conn "INSERT INTO test_rollback_table VALUES ('should-rollback')"
                Just _inserted <- inTransaction @SimpleOpsTestRegistry conn testSchema $ HL.insertJob job
                throwIO (userError "Force rollback")
            )
              `catch` \(exception :: SomeException) -> pure (show exception)

          result `shouldContain` "Force rollback"

          rows <- PG.query_ conn "SELECT value FROM test_rollback_table" :: IO [[Text]]
          rows `shouldBe` []

        claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        claimed `shouldSatisfy` null

    describe "Insertion Ordering" $ do
      it "insertJob: grouped inserts serialize via groups table upsert" $ \env -> do
        withConn connStr $ \connA -> withConn connStr $ \connB -> do
          _ <- PG.execute_ connA "BEGIN"
          Just _ <-
            inTransaction @SimpleOpsTestRegistry connA testSchema
              $ HL.insertJob
              $ setGroupKey (Just "g1")
              $ defaultJob (TestMessage "JobA")

          -- B blocks on the same-group upsert until A commits.
          asyncB <- async $ do
            _ <- PG.execute_ connB "BEGIN"
            Just job <-
              inTransaction @SimpleOpsTestRegistry connB testSchema
                $ HL.insertJob
                $ setGroupKey (Just "g1")
                $ defaultJob (TestMessage "JobB")
            _ <- PG.execute_ connB "COMMIT"
            pure job

          threadDelay blockMicros
          _ <- PG.execute_ connA "COMMIT"
          void (wait asyncB)

        [first] <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        payload first `shouldBe` TestMessage "JobA"
        _ <- runSimpleDb env (HL.ackJob first)

        [second] <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        payload second `shouldBe` TestMessage "JobB"

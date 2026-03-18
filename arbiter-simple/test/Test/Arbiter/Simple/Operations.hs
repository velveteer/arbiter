{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Simple.Operations (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
import Arbiter.Test.Fixtures (TestPayload (..))
import Arbiter.Test.Operations (operationsSpec)
import Arbiter.Test.Setup (execute_, setupOnce)
import Control.Concurrent (threadDelay)
import Control.Exception (SomeException, catch, throwIO)
import Data.ByteString (ByteString)
import Data.Maybe (fromJust)
import Data.Pool (withResource)
import Data.Pool qualified as Pool
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec
import UnliftIO.Async (async, wait)

import Arbiter.Simple.MonadArbiter (SimpleConnectionPool (..))
import Arbiter.Simple.SimpleDb (SimpleEnv (..), createSimpleEnvWithPool, inTransaction, runSimpleDb)
import Test.Arbiter.Simple.TestHelpers (cleanupSimpleTest, createSimplePool)

testSchema :: Text
testSchema = "arbiter_simple_test"

type SimpleOpsTestRegistry = '[ '("arbiter_simple_test", TestPayload)]

testTable :: Text
testTable = "arbiter_simple_test"

withCleanup
  :: Pool.Pool PG.Connection -> (SimpleEnv SimpleOpsTestRegistry -> IO a) -> IO a
withCleanup sharedPool action = do
  let env = createSimpleEnvWithPool (Proxy @SimpleOpsTestRegistry) sharedPool testSchema
  cleanupSimpleTest env testSchema testTable
  action env

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable False) $ do
  sharedPool <- runIO (createSimplePool 5 connStr)
  around (withCleanup sharedPool) $ do
    operationsSpec @TestPayload TestMessage runSimpleDb

    describe "Transaction Participation (localConnection)" $ do
      it "commits job insertion within user transaction" $ \env -> do
        let job = (defaultJob (TestMessage "InTx")) {groupKey = Just "g1"}

        -- Start a user transaction and insert a job with localConnection
        let connPool = fromJust (connectionPool (simplePool env))
        withResource connPool $ \conn -> do
          PG.withTransaction conn $ do
            -- Insert job using localConnection to share the transaction
            Just _inserted <- inTransaction @SimpleOpsTestRegistry conn testSchema $ HL.insertJob job
            -- Transaction commits here
            pure ()

        -- Job should be in the queue (transaction committed)
        claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 1
        payload (head claimed) `shouldBe` TestMessage "InTx"

      it "rolls back job insertion when user transaction fails" $ \env -> do
        let job = (defaultJob (TestMessage "RollbackTest")) {groupKey = Just "g1"}

        -- Start a user transaction and intentionally fail it
        let connPool = fromJust (connectionPool (simplePool env))
        result <-
          ( withResource connPool $ \conn -> do
              PG.withTransaction conn $ do
                -- Insert job using localConnection to share the transaction
                Just _inserted <- inTransaction @SimpleOpsTestRegistry conn testSchema $ HL.insertJob job
                -- Force a rollback by throwing an error
                throwIO (userError "Intentional rollback")
          )
            `catch` \(e :: SomeException) -> pure (show e)

        result `shouldContain` "Intentional rollback"

        -- Job should NOT be in the queue (transaction rolled back)
        claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 0

      it "shares transaction with user's database operations" $ \env -> do
        let job = (defaultJob (TestMessage "SharedTx")) {groupKey = Just "g1"}

        -- Use the same connection for creating table, transaction, and verification
        let connPool = fromJust (connectionPool (simplePool env))
        withResource connPool $ \conn -> do
          -- Create a test table
          execute_ conn "CREATE TEMP TABLE test_tx_table (value TEXT)"

          -- Start transaction with both user operation and job insertion
          PG.withTransaction conn $ do
            -- User's database operation
            execute_ conn "INSERT INTO test_tx_table VALUES ('test-value')"
            -- Insert job in same transaction
            Just _inserted <- inTransaction @SimpleOpsTestRegistry conn testSchema $ HL.insertJob job
            pure ()

          -- Verify user's data was committed (on same connection)
          result <- PG.query_ conn "SELECT value FROM test_tx_table" :: IO [[Text]]
          length result `shouldBe` 1
          head (head result) `shouldBe` "test-value"

        -- Job should be committed
        claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 1

      it "rolls back both user operations and job when transaction fails" $ \env -> do
        let job = (defaultJob (TestMessage "BothRollback")) {groupKey = Just "g1"}

        -- Use the same connection for creating table, transaction, and verification
        let connPool = fromJust (connectionPool (simplePool env))
        withResource connPool $ \conn -> do
          -- Create a temp table
          execute_ conn "CREATE TEMP TABLE test_rollback_table (value TEXT)"

          -- Start transaction and fail it
          result <-
            ( PG.withTransaction conn $ do
                -- User's database operation
                execute_ conn "INSERT INTO test_rollback_table VALUES ('should-rollback')"
                -- Insert job in same transaction
                Just _inserted <- inTransaction @SimpleOpsTestRegistry conn testSchema $ HL.insertJob job
                -- Force rollback
                throwIO (userError "Force rollback")
            )
              `catch` \(e :: SomeException) -> pure (show e)

          result `shouldContain` "Force rollback"

          -- User's data should also be rolled back
          rows <- PG.query_ conn "SELECT value FROM test_rollback_table" :: IO [[Text]]
          length rows `shouldBe` 0

        -- Job should NOT be in queue
        claimed <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        length claimed `shouldBe` 0

    describe "Insertion Ordering" $ do
      it "insertJob: grouped inserts serialize via groups table upsert" $ \env -> do
        -- The groups table upsert (INSERT...ON CONFLICT) serializes concurrent
        -- inserts within the same group. B blocks until A commits, preventing
        -- the race where B's job becomes visible before A's.

        connA <- PG.connectPostgreSQL connStr
        connB <- PG.connectPostgreSQL connStr

        -- A starts transaction and inserts (holds lock on groups table row)
        _ <- PG.execute_ connA "BEGIN"
        Just _ <-
          inTransaction @SimpleOpsTestRegistry connA testSchema $
            HL.insertJob (defaultJob (TestMessage "JobA")) {groupKey = Just "g1"}

        -- B tries to insert same group — BLOCKS on groups table upsert until A commits
        asyncB <- async $ do
          _ <- PG.execute_ connB "BEGIN"
          Just job <-
            inTransaction @SimpleOpsTestRegistry connB testSchema $
              HL.insertJob (defaultJob (TestMessage "JobB")) {groupKey = Just "g1"}
          _ <- PG.execute_ connB "COMMIT"
          pure job

        -- Give B time to start and block
        threadDelay 100_000

        -- A commits (releases groups table lock, B unblocks)
        _ <- PG.execute_ connA "COMMIT"

        -- B can now proceed
        _ <- wait asyncB

        PG.close connA
        PG.close connB

        -- Both jobs now visible. A has lower ID, claimed first.
        [first] <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        payload first `shouldBe` TestMessage "JobA"
        _ <- runSimpleDb env (HL.ackJob first)

        [second] <- runSimpleDb env (HL.claimNextVisibleJobs 1 60) :: IO [JobRead TestPayload]
        payload second `shouldBe` TestMessage "JobB"

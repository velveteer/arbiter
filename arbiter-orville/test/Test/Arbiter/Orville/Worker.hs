{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Orville.Worker (spec) where

import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types (Job (..), JobRead, defaultJob)
import Arbiter.Core.JobResult (HasJobResult (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)
import Arbiter.Worker.TestKit (workerSpec)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Generics (Generic)
import Orville.PostgreSQL qualified as O
import Orville.PostgreSQL.Execution.ExecutionResult qualified as ExecResult
import Orville.PostgreSQL.Raw.RawSql qualified as RawSql
import Orville.PostgreSQL.Raw.SqlValue qualified as SqlValue
import Test.Hspec
  ( Spec
  , beforeAll
  , beforeWith
  , describe
  , expectationFailure
  , it
  , shouldBe
  )
import UnliftIO.Async (withAsync)

import Test.Arbiter.Orville.TestHelpers
  ( OrvilleTestEnv (..)
  , TestOrville (..)
  , cleanupOrvilleTest
  , executeSql
  , runOrvilleTest
  , setupOrvilleTest
  )

workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_orville_worker_test"

data OrvilleWorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

instance HasJobResult OrvilleWorkerTestPayload where
  type ResultOf OrvilleWorkerTestPayload = Maybe [Text]

type OrvilleWorkerTestRegistry = '[ '("arbiter_orville_worker_test", OrvilleWorkerTestPayload)]

testTable :: Text
testTable = "arbiter_orville_worker_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOrvilleTest connStr workerTestSchemaName testTable 10) $ beforeWith (\env -> cleanupOrvilleTest env >> pure env) $ do
  workerSpec @OrvilleWorkerTestPayload @OrvilleWorkerTestRegistry SimpleTask FailingTask id runOrvilleTest

  describe "Transactional Atomicity" $ do
    it "rolls back user operations when handler fails" $ \env -> do
      -- Create a test table to track user operations
      runOrvilleTest env $ do
        executeSql $ "CREATE TABLE IF NOT EXISTS " <> workerTestSchemaName <> ".test_operations (job_id INT, operation TEXT)"
        executeSql $ "TRUNCATE " <> workerTestSchemaName <> ".test_operations"

      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry (Maybe [Text])
          handler job = do
            -- User performs their own database operation using the connection
            let insertSql =
                  RawSql.fromText $
                    "INSERT INTO "
                      <> workerTestSchemaName
                      <> ".test_operations (job_id, operation) VALUES ("
                      <> T.pack (show (primaryKey job))
                      <> ", 'processed')"
            -- Then the handler fails
            O.executeVoid O.InsertQuery insertSql
            throwRetryable "Simulated failure"

      -- Insert a job
      let job =
            (defaultJob (SimpleTask "WillFail"))
              { groupKey = Just "g1"
              , maxAttempts = Just 1
              }
      void $ runOrvilleTest env $ HL.insertJob job

      -- Start worker pool with max 1 attempt so it goes to DLQ
      config <- transactionalWorkerConfig 10 handler
      runOrvilleTest env
        $ withAsync
          ( runWorkerPool
              ( config
                  { workerCount = 1
                  , pollInterval = 0.1
                  }
              )
          )
        $ \_ ->
          do
            -- Wait for job to be processed and moved to DLQ
            liftIO $ waitUntil 10_000 $ do
              dlqJobs <- runOrvilleTest env $ HL.listDLQJobs @_ @_ @OrvilleWorkerTestPayload 10 0
              pure (length dlqJobs == 1)

            -- Verify the job is in the DLQ with the correct payload
            dlqJobs <- HL.listDLQJobs @_ @_ @OrvilleWorkerTestPayload 10 0
            liftIO $ length dlqJobs `shouldBe` 1
            liftIO $ (payload $ DLQ.jobSnapshot (head dlqJobs)) `shouldBe` SimpleTask "WillFail"

            -- Verify the user's database operation was rolled back
            O.withConnection $ \conn -> do
              let countSql = RawSql.fromText $ "SELECT COUNT(*) FROM " <> workerTestSchemaName <> ".test_operations WHERE operation = 'processed'"
              result <- liftIO $ RawSql.execute conn countSql
              rows <- liftIO $ ExecResult.readRows result
              case rows of
                [[(_name, val)]] -> do
                  case SqlValue.toInt64 val of
                    Right count | count == 0 -> pure ()
                    Right count -> liftIO $ expectationFailure $ "Expected 0 rows but got " <> show count
                    Left err -> liftIO $ expectationFailure $ "Failed to decode count: " <> err
                _ -> liftIO $ expectationFailure "Expected one row from COUNT query"

      O.runOrvilleWithState (testOrvilleState env) $
        O.executeVoid O.OtherQuery (RawSql.fromText $ "DROP TABLE IF EXISTS " <> workerTestSchemaName <> ".test_operations")

    it "commits user operations when handler succeeds" $ \env -> do
      -- Create a test table to track user operations
      O.runOrvilleWithState (testOrvilleState env) $ do
        O.executeVoid
          O.OtherQuery
          ( RawSql.fromText $
              "CREATE TABLE IF NOT EXISTS " <> workerTestSchemaName <> ".test_operations (job_id INT, operation TEXT)"
          )
        O.executeVoid O.OtherQuery (RawSql.fromText $ "TRUNCATE " <> workerTestSchemaName <> ".test_operations")

      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry (Maybe [Text])
          handler job = do
            -- User performs their own database operation using the connection
            let insertSql =
                  RawSql.fromText $
                    "INSERT INTO "
                      <> workerTestSchemaName
                      <> ".test_operations (job_id, operation) VALUES ("
                      <> T.pack (show (primaryKey job))
                      <> ", 'processed')"
            O.executeVoid O.InsertQuery insertSql
            pure mempty

      -- Insert a job
      let job =
            (defaultJob (SimpleTask "WillSucceed"))
              { groupKey = Just "g1"
              }
      void $ runOrvilleTest env $ HL.insertJob job

      -- Start worker pool
      config <- transactionalWorkerConfig 10 handler
      runOrvilleTest env
        $ withAsync
          ( runWorkerPool
              ( config
                  { workerCount = 1
                  , pollInterval = 0.1
                  }
              )
          )
        $ \_ -> do
          -- Wait for job to be processed
          liftIO $ waitUntil 10_000 $ do
            jobs <- runOrvilleTest env $ HL.listJobs @_ @OrvilleWorkerTestRegistry @OrvilleWorkerTestPayload 10 0
            pure (null jobs)

          -- Verify the job is NOT in the queue anymore
          O.withConnection $ \conn -> do
            let countSql = RawSql.fromText $ "SELECT COUNT(*) FROM " <> Schema.jobQueueTable workerTestSchemaName testTable
            result <- liftIO $ RawSql.execute conn countSql
            rows <- liftIO $ ExecResult.readRows result
            case rows of
              [[(_name, val)]] -> do
                case SqlValue.toInt64 val of
                  Right count | count == 0 -> pure ()
                  Right count -> liftIO $ expectationFailure $ "Expected 0 jobs in queue but got " <> show count
                  Left err -> liftIO $ expectationFailure $ "Failed to decode count: " <> err
              _ -> liftIO $ expectationFailure "Expected one row from COUNT query"

            -- Verify the user's database operation was committed
            let countOpsSQL = RawSql.fromText $ "SELECT COUNT(*) FROM " <> workerTestSchemaName <> ".test_operations WHERE operation = 'processed'"
            result2 <- liftIO $ RawSql.execute conn countOpsSQL
            rows2 <- liftIO $ ExecResult.readRows result2
            case rows2 of
              [[(_name, val)]] -> do
                case SqlValue.toInt64 val of
                  Right count | count == 1 -> pure ()
                  Right count -> liftIO $ expectationFailure $ "Expected 1 row but got " <> show count
                  Left err -> liftIO $ expectationFailure $ "Failed to decode count: " <> err
              _ -> liftIO $ expectationFailure "Expected one row from COUNT query"

      -- Cleanup test table
      runOrvilleTest env $
        O.executeVoid O.OtherQuery (RawSql.fromText $ "DROP TABLE IF EXISTS " <> workerTestSchemaName <> ".test_operations")

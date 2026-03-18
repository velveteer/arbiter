{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Orville.Worker (spec) where

import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema qualified as Schema
import Arbiter.Core.Job.Types (Job (..), JobRead, defaultJob)
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config (WorkerConfig (..), defaultWorkerConfig)
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
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
  , shouldMatchList
  , shouldSatisfy
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

-- Test schema name constant
workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_orville_worker_test"

-- | Local test payload type with correct schema name
data OrvilleWorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Local test registry
type OrvilleWorkerTestRegistry = '[ '("arbiter_orville_worker_test", OrvilleWorkerTestPayload)]

-- Table name for tests
testTable :: Text
testTable = "arbiter_orville_worker_test"

-- Helper to run worker pool
runWorker
  :: OrvilleTestEnv OrvilleWorkerTestRegistry
  -> WorkerConfig (TestOrville OrvilleWorkerTestRegistry) OrvilleWorkerTestPayload ()
  -> IO ()
runWorker env config =
  runOrvilleTest env $ runWorkerPool config

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOrvilleTest connStr workerTestSchemaName testTable 10) $ beforeWith (\env -> cleanupOrvilleTest env >> pure env) $ do
  describe "Worker Pool" $ do
    it "processes jobs successfully" $ \env -> do
      -- Track completed jobs
      completedRef <- newIORef []

      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry ()
          handler job = do
            liftIO $ atomicModifyIORef' completedRef $ \jobs -> (payload job : jobs, ())
            pure ()

      -- Insert some jobs
      let jobs =
            [ (defaultJob (SimpleTask "Job 1"))
                { groupKey = Just "g1"
                }
            , (defaultJob (SimpleTask "Job 2"))
                { groupKey = Just "g2"
                }
            , (defaultJob (SimpleTask "Job 3"))
                { groupKey = Just "g3"
                }
            ]

      runOrvilleTest env $ forM_ jobs $ \job -> HL.insertJob job

      -- Start worker pool
      config <- defaultWorkerConfig connStr 10 handler

      -- Run worker pool with automatic cleanup
      withAsync
        ( runWorker env $
            config
              { workerCount = 3
              , pollInterval = 0.1 -- Poll every 100ms for faster tests
              }
        )
        $ \_ -> liftIO $ do
          waitUntil 10_000 $ (== 3) . length <$> readIORef completedRef

          -- Check that all jobs were completed
          completed <- readIORef completedRef
          length completed `shouldBe` 3
          completed `shouldMatchList` [SimpleTask "Job 1", SimpleTask "Job 2", SimpleTask "Job 3"]

    it "respects worker count concurrency limit" $ \env -> do
      -- Track concurrent workers
      activeRef <- newIORef (0 :: Int)
      maxActiveRef <- newIORef (0 :: Int)
      completedRef <- newIORef (0 :: Int)

      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry ()
          handler _job = do
            -- Atomically increment and read active count
            active <- liftIO $ atomicModifyIORef' activeRef $ \n -> let n' = n + 1 in (n', n')
            liftIO $ atomicModifyIORef' maxActiveRef $ \maxN -> (max maxN active, ())

            -- Simulate work
            liftIO $ threadDelay 500_000 -- 500ms

            -- Decrement active count
            liftIO $ atomicModifyIORef' activeRef $ \n -> (n - 1, ())
            liftIO $ atomicModifyIORef' completedRef $ \n -> (n + 1, ())

            pure ()

      -- Insert jobs (more than worker count)
      let jobs =
            map
              ( \i ->
                  (defaultJob (SimpleTask (T.pack $ "Job " <> show @Int i)))
                    { groupKey = Just (T.pack $ "g" <> show i)
                    }
              )
              [1 .. 10]

      forM_ jobs $ \job -> runOrvilleTest env $ HL.insertJob job

      -- Start worker pool with limited workers
      config <- defaultWorkerConfig connStr 10 handler

      withAsync
        ( runWorker env $
            config
              { workerCount = 3
              , pollInterval = 0.05
              }
        )
        $ \_ -> do
          waitUntil 10_000 $ (== 10) <$> readIORef completedRef

          -- Check that all jobs were processed and we never exceeded the worker count
          completed <- readIORef completedRef
          completed `shouldBe` 10
          maxActive <- readIORef maxActiveRef
          maxActive `shouldSatisfy` (> 0)
          maxActive `shouldSatisfy` (<= 3)

    it "retries failed jobs up to max attempts" $ \env -> do
      -- Track attempts per job
      attemptsRef <- newIORef (0 :: Int)

      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry ()
          handler job = do
            attempts <- liftIO $ atomicModifyIORef' attemptsRef $ \n -> (n + 1, n + 1)
            case payload job of
              FailingTask maxFails ->
                when (attempts < maxFails) $
                  throwRetryable "Not yet!"
              _ -> pure ()

      -- Insert a job that fails twice before succeeding
      let job =
            (defaultJob (FailingTask 3))
              { groupKey = Just "g1"
              }
      void $ runOrvilleTest env $ HL.insertJob job

      -- Start worker pool
      config <- defaultWorkerConfig connStr 10 handler

      withAsync
        ( runWorker env $
            config
              { workerCount = 1
              , pollInterval = 0.1
              , maxAttempts = 5 -- Allow enough retries
              , jitter = NoJitter -- Predictable timing for test
              }
        )
        $ \_ -> do
          waitUntil 15_000 $ (== 3) <$> readIORef attemptsRef

          -- Check that the job was attempted 3 times
          attempts <- readIORef attemptsRef
          attempts `shouldBe` 3

    it "moves jobs to DLQ after max attempts" $ \env -> do
      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry ()
          handler _job = throwRetryable "Always fails"

      -- Insert a failing job
      let job =
            (defaultJob (SimpleTask "Doomed"))
              { groupKey = Just "g1"
              }
      void $ runOrvilleTest env $ HL.insertJob job

      -- Start worker pool with low max attempts
      config <- defaultWorkerConfig connStr 10 handler

      withAsync
        ( runWorker env $
            config
              { workerCount = 1
              , pollInterval = 0.1
              , maxAttempts = 1
              }
        )
        $ \_ -> do
          waitUntil 10_000 $ do
            dlqJobs <- runOrvilleTest env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob OrvilleWorkerTestPayload]
            pure (length dlqJobs == 1)

          -- Check DLQ
          dlqJobs <- runOrvilleTest env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob OrvilleWorkerTestPayload]
          length dlqJobs `shouldBe` 1

          -- Verify the job in DLQ
          let dlqJob = head dlqJobs
          (payload $ DLQ.jobSnapshot dlqJob) `shouldBe` SimpleTask "Doomed"
          (attempts $ DLQ.jobSnapshot dlqJob) `shouldBe` 1

    it "processes all jobs from different groups" $ \env -> do
      -- Track which groups are being processed
      processingRef <- newIORef []

      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry ()
          handler job = do
            liftIO $ atomicModifyIORef' processingRef $ \groups -> (groupKey job : groups, ())
            liftIO $ threadDelay 1_000_000 -- 1 second
            pure ()

      -- Insert jobs from different groups
      let jobs =
            [ (defaultJob (SimpleTask "G1-1"))
                { groupKey = Just "g1"
                }
            , (defaultJob (SimpleTask "G2-1"))
                { groupKey = Just "g2"
                }
            , (defaultJob (SimpleTask "G3-1"))
                { groupKey = Just "g3"
                }
            ]

      forM_ jobs $ \job -> runOrvilleTest env $ HL.insertJob job

      -- Start worker pool with multiple workers
      config <- defaultWorkerConfig connStr 10 handler

      withAsync (runWorker env $ config {workerCount = 3, pollInterval = 0.05}) $ \_ -> do
        waitUntil 10_000 $ (== 3) . length <$> readIORef processingRef

        -- Check that all groups were processed
        processed <- readIORef processingRef
        length processed `shouldBe` 3
        processed `shouldMatchList` [Just "g1", Just "g2", Just "g3"]

  describe "Head-of-Line Blocking" $ do
    it "processes jobs in the same group serially" $ \env -> do
      -- Track job completion order and concurrent execution
      orderRef <- newIORef []
      activeRef <- newIORef (0 :: Int)
      maxActiveRef <- newIORef (0 :: Int)

      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry ()
          handler job = do
            -- Atomically increment and read active count
            active <- liftIO $ atomicModifyIORef' activeRef $ \n -> let n' = n + 1 in (n', n')
            liftIO $ atomicModifyIORef' maxActiveRef $ \maxN -> (max maxN active, ())
            liftIO $ threadDelay 200_000 -- Small delay
            liftIO $ atomicModifyIORef' orderRef $ \order -> (payload job : order, ())
            liftIO $ atomicModifyIORef' activeRef $ \n -> (n - 1, ())

      -- Insert multiple jobs in the same group
      let jobs =
            [ (defaultJob (SimpleTask "First"))
                { groupKey = Just "g1"
                }
            , (defaultJob (SimpleTask "Second"))
                { groupKey = Just "g1"
                }
            , (defaultJob (SimpleTask "Third"))
                { groupKey = Just "g1"
                }
            ]

      forM_ jobs $ \job -> runOrvilleTest env $ HL.insertJob job

      -- Start worker pool
      config <- defaultWorkerConfig connStr 10 handler

      withAsync
        ( runWorker env $
            config
              { workerCount = 3 -- Multiple workers, but should still be serial per group
              , pollInterval = 0.05
              }
        )
        $ \_ -> do
          waitUntil 10_000 $ (== 3) . length <$> readIORef orderRef

          -- Check all jobs completed and were processed in order
          order <- readIORef orderRef
          length order `shouldBe` 3
          reverse order `shouldBe` [SimpleTask "First", SimpleTask "Second", SimpleTask "Third"]
          -- Verify at most 1 job from the group ran concurrently
          maxActive <- readIORef maxActiveRef
          maxActive `shouldBe` 1

  describe "Transactional Atomicity" $ do
    it "rolls back user operations when handler fails" $ \env -> do
      -- Create a test table to track user operations
      runOrvilleTest env $ do
        executeSql $ "CREATE TABLE IF NOT EXISTS " <> workerTestSchemaName <> ".test_operations (job_id INT, operation TEXT)"
        executeSql $ "TRUNCATE " <> workerTestSchemaName <> ".test_operations"

      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry ()
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
              }
      void $ runOrvilleTest env $ HL.insertJob job

      -- Start worker pool with max 1 attempt so it goes to DLQ
      config <- defaultWorkerConfig connStr 10 handler
      runOrvilleTest env
        $ withAsync
          ( runWorkerPool
              ( config
                  { workerCount = 1
                  , pollInterval = 0.1
                  , maxAttempts = 1
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

      let handler :: JobRead OrvilleWorkerTestPayload -> TestOrville OrvilleWorkerTestRegistry ()
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

      -- Insert a job
      let job =
            (defaultJob (SimpleTask "WillSucceed"))
              { groupKey = Just "g1"
              }
      void $ runOrvilleTest env $ HL.insertJob job

      -- Start worker pool
      config <- defaultWorkerConfig connStr 10 handler
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

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_GHC -Wno-x-partial #-}

module Test.Arbiter.Hasql.Worker (spec) where

import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Types (Job (..), JobRead, defaultJob)
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (setupOnce)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config (WorkerConfig (..), defaultWorkerConfig)
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Generics (Generic)
import Hasql.Connection qualified as Hasql
import Test.Hspec
import UnliftIO.Async (withAsync)

import Arbiter.Hasql.HasqlDb (HasqlDb, createHasqlEnvWithPool, runHasqlDb)
import Test.Arbiter.Hasql.TestHelpers (cleanupHasqlTest, createHasqlPool)

workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_hasql_worker_test"

data HasqlWorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type HasqlWorkerTestRegistry = '[ '("arbiter_hasql_worker_test", HasqlWorkerTestPayload)]

testTable :: Text
testTable = "arbiter_hasql_worker_test"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr workerTestSchemaName testTable False >> createHasqlPool 10 connStr) $ beforeWith (\pool -> cleanupHasqlTest connStr workerTestSchemaName testTable >> pure pool) $ do
  let mkEnv pool = createHasqlEnvWithPool (Proxy @HasqlWorkerTestRegistry) pool workerTestSchemaName

  describe "Worker Pool" $ do
    it "processes jobs successfully" $ \pool -> do
      let env = mkEnv pool
      completedRef <- newIORef []

      let handler :: Hasql.Connection -> JobRead HasqlWorkerTestPayload -> HasqlDb HasqlWorkerTestRegistry IO ()
          handler _conn job = do
            liftIO $ atomicModifyIORef' completedRef $ \jobs -> (payload job : jobs, ())

      let jobs =
            [ (defaultJob (SimpleTask "Job 1")) {groupKey = Just "g1"}
            , (defaultJob (SimpleTask "Job 2")) {groupKey = Just "g2"}
            , (defaultJob (SimpleTask "Job 3")) {groupKey = Just "g3"}
            ]

      runHasqlDb env $ forM_ jobs $ \job -> HL.insertJob job

      config <- defaultWorkerConfig connStr 10 handler

      withAsync
        ( runHasqlDb env $
            runWorkerPool config {workerCount = 3, pollInterval = 0.1}
        )
        $ \_ -> liftIO $ do
          waitUntil 10_000 $ (== 3) . length <$> readIORef completedRef
          completed <- readIORef completedRef
          length completed `shouldBe` 3
          completed `shouldMatchList` [SimpleTask "Job 1", SimpleTask "Job 2", SimpleTask "Job 3"]

    it "respects worker count concurrency limit" $ \pool -> do
      let env = mkEnv pool
      activeRef <- newIORef (0 :: Int)
      maxActiveRef <- newIORef (0 :: Int)
      completedRef <- newIORef (0 :: Int)

      let handler :: Hasql.Connection -> JobRead HasqlWorkerTestPayload -> HasqlDb HasqlWorkerTestRegistry IO ()
          handler _conn _job = do
            active <- liftIO $ atomicModifyIORef' activeRef $ \n -> let n' = n + 1 in (n', n')
            liftIO $ atomicModifyIORef' maxActiveRef $ \maxN -> (max maxN active, ())
            liftIO $ threadDelay 500_000
            liftIO $ atomicModifyIORef' activeRef $ \n -> (n - 1, ())
            liftIO $ atomicModifyIORef' completedRef $ \n -> (n + 1, ())

      let jobs =
            map
              ( \i ->
                  (defaultJob (SimpleTask (T.pack $ "Job " <> show @Int i)))
                    { groupKey = Just (T.pack $ "g" <> show i)
                    }
              )
              [1 .. 10]

      forM_ jobs $ \job -> runHasqlDb env $ HL.insertJob job

      config <- defaultWorkerConfig connStr 10 handler

      withAsync
        (runHasqlDb env $ runWorkerPool config {workerCount = 3, pollInterval = 0.05})
        $ \_ -> do
          waitUntil 10_000 $ (== 10) <$> readIORef completedRef
          completed <- readIORef completedRef
          completed `shouldBe` 10
          maxActive <- readIORef maxActiveRef
          maxActive `shouldSatisfy` (> 0)
          maxActive `shouldSatisfy` (<= 3)

    it "retries failed jobs up to max attempts" $ \pool -> do
      let env = mkEnv pool
      attemptsRef <- newIORef (0 :: Int)

      let handler :: Hasql.Connection -> JobRead HasqlWorkerTestPayload -> HasqlDb HasqlWorkerTestRegistry IO ()
          handler _conn job = do
            attempts <- liftIO $ atomicModifyIORef' attemptsRef $ \n -> (n + 1, n + 1)
            case payload job of
              FailingTask maxFails ->
                when (attempts < maxFails) $
                  throwRetryable "Not yet!"
              _ -> pure ()

      let job = (defaultJob (FailingTask 3)) {groupKey = Just "g1"}
      void $ runHasqlDb env $ HL.insertJob job

      config <- defaultWorkerConfig connStr 10 handler

      withAsync
        ( runHasqlDb env $
            runWorkerPool config {workerCount = 1, pollInterval = 0.1, maxAttempts = 5, jitter = NoJitter}
        )
        $ \_ -> do
          waitUntil 15_000 $ (== 3) <$> readIORef attemptsRef
          attempts <- readIORef attemptsRef
          attempts `shouldBe` 3

    it "moves jobs to DLQ after max attempts" $ \pool -> do
      let env = mkEnv pool
      let handler :: Hasql.Connection -> JobRead HasqlWorkerTestPayload -> HasqlDb HasqlWorkerTestRegistry IO ()
          handler _conn _job = throwRetryable "Always fails"

      let job = (defaultJob (SimpleTask "Doomed")) {groupKey = Just "g1"}
      void $ runHasqlDb env $ HL.insertJob job

      config <- defaultWorkerConfig connStr 10 handler

      withAsync
        (runHasqlDb env $ runWorkerPool config {workerCount = 1, pollInterval = 0.1, maxAttempts = 1})
        $ \_ -> do
          waitUntil 10_000 $ do
            dlqJobs <- runHasqlDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob HasqlWorkerTestPayload]
            pure (length dlqJobs == 1)

          dlqJobs <- runHasqlDb env $ HL.listDLQJobs 10 0 :: IO [DLQ.DLQJob HasqlWorkerTestPayload]
          length dlqJobs `shouldBe` 1
          let dlqJob = head dlqJobs
          (payload $ DLQ.jobSnapshot dlqJob) `shouldBe` SimpleTask "Doomed"
          (attempts $ DLQ.jobSnapshot dlqJob) `shouldBe` 1

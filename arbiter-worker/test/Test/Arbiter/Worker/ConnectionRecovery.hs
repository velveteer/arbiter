{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Connection failure recovery tests.
--
-- Verifies that the worker pool survives PostgreSQL connection termination
-- and continues processing jobs after reconnecting.
module Test.Arbiter.Worker.ConnectionRecovery (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
  ( defaultJob
  , payload
  , setGroupKey
  , setMaxAttempts
  )
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Simple (SimpleDb, SimpleEnv, createSimpleEnvWithPool, inTransaction, runSimpleDb)
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (cleanupData, createSharedPool, setupOnce)
import Control.Concurrent (threadDelay)
import Control.Monad (forM_, void, when)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple (Only (..), close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec (Spec, around, beforeAll, describe, it, runIO, shouldBe)
import UnliftIO.Async (withAsync)

import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)

type WorkerTestRegistry = '[Queue "arbiter_worker_recovery_test" WorkerTestPayload]

testSchema :: Text
testSchema = "arbiter_worker_recovery_test"

testTable :: Text
testTable = "arbiter_worker_recovery_test"

withPool :: Pool PG.Connection -> (SimpleEnv WorkerTestRegistry -> IO a) -> IO a
withPool sharedPool action = do
  env <- createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema
  withResource sharedPool $ \conn -> cleanupData testSchema testTable conn
  action env

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable True) $ do
  sharedPool <- runIO (createSharedPool connStr)
  around (withPool sharedPool) $ do
    describe "Connection Recovery" $ do
      it "processes jobs inserted before and after a connection kill" $ \env -> do
        completedRef <- newIORef (0 :: Int)

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = do
              liftIO $ atomicModifyIORef' completedRef $ \count -> (count + 1, ())

        -- Insert 3 jobs before the kill
        forM_ [1 :: Int .. 3] $ \jobIndex ->
          runSimpleDb env
            $ void
            $ HL.insertJob
            $ setGroupKey (Just "g1")
            $ defaultJob (SimpleTask (T.pack $ "Pre-kill " <> show jobIndex))

        config <- transactionalWorkerConfig 10 handler
        let workerConfig =
              config
                { workerCount = 1
                , pollInterval = 0.2
                , jitter = NoJitter
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          -- Wait for initial jobs to be processed
          waitUntil 10_000 $ (== 3) <$> readIORef completedRef

          preKillCompleted <- readIORef completedRef
          preKillCompleted `shouldBe` 3

          -- Kill all connections from our test schema (dispatcher, listener)
          killSchemaConnections connStr testSchema

          -- Wait for retryOnException to reconnect (5s delay + reconnect)
          threadDelay 7_000_000

          -- Insert more jobs on a fresh connection. The pool may still hold
          -- dead connections.
          insertJobsDirect
            env
            connStr
            [ SimpleTask "Post-kill 1"
            , SimpleTask "Post-kill 2"
            , SimpleTask "Post-kill 3"
            ]

          -- Wait for post-kill processing
          waitUntil 10_000 $ (== 6) <$> readIORef completedRef

          completed <- readIORef completedRef
          completed `shouldBe` 6

      it "retries job after worker ack fails due to terminated connection" $ \env -> do
        completedRef <- newIORef (0 :: Int)
        killOnceRef <- newIORef True

        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler conn job = do
              case payload job of
                SlowTask _ -> do
                  shouldKill <- liftIO $ atomicModifyIORef' killOnceRef $ \armed -> (False, armed)
                  when shouldKill $ liftIO $ do
                    -- Terminate this handler's own connection from another connection.
                    myPid <- PG.query_ @(Only Int) conn "SELECT pg_backend_pid()"
                    case myPid of
                      [Only pid] -> terminatePid connStr pid
                      _ -> pure ()
                -- The next DB operation on conn will fail
                _ -> pure ()
              liftIO $ atomicModifyIORef' completedRef $ \count -> (count + 1, ())

        -- Insert the slow job that will get its connection killed
        runSimpleDb env
          $ void
          $ HL.insertJob
          $ setMaxAttempts (Just 3)
          $ setGroupKey (Just "g1")
          $ defaultJob (SlowTask 1)

        config <- transactionalWorkerConfig 10 handler
        let workerConfig =
              config
                { workerCount = 1
                , pollInterval = 0.2
                , jitter = NoJitter
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 15_000 $ (== 2) <$> readIORef completedRef

          completed <- readIORef completedRef
          -- The handler runs twice. The first attempt loses its connection after
          -- the increment. The second attempt succeeds.
          completed `shouldBe` 2

          -- Verify the job was acked and removed from the queue. The handler
          -- increments before its transaction commits. Poll for the removal.
          let remainingCount =
                length <$> runSimpleDb env (HL.listJobs @WorkerTestPayload 10 0)
          waitUntil 10_000 $ (== 0) <$> remainingCount

          remaining <- remainingCount
          remaining `shouldBe` 0

-- | Kill active connections that reference our test schema.
killSchemaConnections :: ByteString -> Text -> IO ()
killSchemaConnections connStr schemaName = do
  conn <- connectPostgreSQL connStr
  _ <-
    PG.query @_ @(Only Bool)
      conn
      "SELECT pg_terminate_backend(pid) \
      \FROM pg_stat_activity \
      \WHERE pid <> pg_backend_pid() \
      \  AND datname = current_database() \
      \  AND query LIKE ?"
      (Only ("%" <> schemaName <> "%" :: Text))
  close conn

-- | Terminate a specific backend PID.
terminatePid :: ByteString -> Int -> IO ()
terminatePid connStr pid = do
  conn <- connectPostgreSQL connStr
  _ <- PG.query @_ @(Only Bool) conn "SELECT pg_terminate_backend(?)" (Only pid)
  close conn

-- | Insert jobs on a fresh connection. The pool may still hold dead
-- connections after a kill.
insertJobsDirect :: SimpleEnv WorkerTestRegistry -> ByteString -> [WorkerTestPayload] -> IO ()
insertJobsDirect _env connStr payloads = do
  conn <- connectPostgreSQL connStr
  PG.withTransaction conn $
    forM_ payloads $ \task ->
      inTransaction @WorkerTestRegistry conn testSchema
        $ void
        $ HL.insertJob
        $ setGroupKey (Just "g1")
        $ defaultJob task
  close conn

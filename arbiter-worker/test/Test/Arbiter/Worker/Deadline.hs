{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Fence tests: the deadlines the worker holds a handler to without asking the database.
module Test.Arbiter.Worker.Deadline (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.DLQ (DLQJob (..))
import Arbiter.Core.Job.Types
  ( JobRead
  , ObservabilityHooks (..)
  , defaultJob
  , defaultObservabilityHooks
  , lastError
  , setMaxAttempts
  )
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Simple (SimpleDb, SimpleEnv, createSimpleEnvWithPool, runSimpleDb)
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (cleanupData, createSharedPool, setupOnce)
import Control.Concurrent (threadDelay)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.Foldable (toList)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.List.NonEmpty (NonEmpty)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import Test.Hspec (Spec, around, beforeAll, describe, it, runIO, shouldBe, shouldSatisfy)
import UnliftIO (bracket, bracket_, finally)
import UnliftIO.Async (withAsync)

import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config
  ( BatchCallbacks (..)
  , WorkerConfig (..)
  , ackAll
  , defaultBatchedWorkerConfig
  , transactionalWorkerConfig
  )
import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), defaultLogConfig, silentLogConfig)

type WorkerTestRegistry = '[Queue "arbiter_worker_deadline_test" WorkerTestPayload]

testSchema :: Text
testSchema = "arbiter_worker_deadline_test"

testTable :: Text
testTable = "arbiter_worker_deadline_test"

-- | Longer than any deadline under test, so only a fence ends the handler.
handlerSleepMicros :: Int
handlerSleepMicros = 30_000_000

-- | Cleanup slow enough that the heartbeat outlives the fence throw.
unwindMicros :: Int
unwindMicros = 4_000_000

withPool :: Pool PG.Connection -> (SimpleEnv WorkerTestRegistry -> IO a) -> IO a
withPool sharedPool action = do
  env <- createSimpleEnvWithPool (Proxy @WorkerTestRegistry) sharedPool testSchema
  withResource sharedPool $ \conn -> cleanupData testSchema testTable conn
  action env

spec :: ByteString -> Spec
spec connStr = beforeAll (setupOnce connStr testSchema testTable True) $ do
  sharedPool <- runIO (createSharedPool connStr)
  around (withPool sharedPool) $ do
    describe "Heartbeat scheduling" $ do
      it "stops retrying once the lease is spent instead of spinning" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        loggedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job =
              liftIO (atomicModifyIORef' startedRef (\n -> (n + 1, ())) >> threadDelay handlerSleepMicros)
                `finally` liftIO (threadDelay unwindMicros)
            capture _level msg _ctx =
              when ("Heartbeat error" `T.isInfixOf` msg) $
                atomicModifyIORef' loggedRef (\n -> (n + 1, ()))

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 30))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 4
                , jobHeartbeatInterval = 1
                , logConfig = defaultLogConfig {logDestination = LogCallback capture}
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          attempts <-
            bracket_ (blockRowUpdates connStr) (unblockRowUpdates connStr) $ do
              threadDelay (unwindMicros + 6_000_000)
              readIORef loggedRef
          attempts `shouldSatisfy` (< 25)

      it "keeps beating at its interval after a failed extend" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        beatsRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\n -> (n + 1, ()))
              threadDelay 9_000_000
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \_ _ _ -> liftIO (atomicModifyIORef' beatsRef (\n -> (n + 1, ())))
                }

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 9))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 20
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          bracket_ (blockRowUpdates connStr) (unblockRowUpdates connStr) (threadDelay 1_500_000)
          threadDelay 5_000_000
          beats <- readIORef beatsRef
          beats `shouldSatisfy` (>= 3)

    describe "Job deadline" $ do
      it "interrupts a handler that outruns the maximum job duration" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\n -> (n + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\n -> (n + 1, ()))

        runSimpleDb env
          $ void
          $ HL.insertJob (setMaxAttempts (Just 1) (defaultJob (SlowTask 30)))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          waitUntil 20_000 $ not . null <$> listDLQ env
          dlq <- listDLQ env
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          map (lastError . jobSnapshot) dlq
            `shouldBe` [Just "handler ran past the maximum job duration of 1s"]

      it "reports stopping a handler that had already finalized its batch" $ \env -> do
        loggedRef <- newIORef ([] :: [Text])
        finishedRef <- newIORef (0 :: Int)
        let handler
              :: NonEmpty (JobRead WorkerTestPayload)
              -> BatchCallbacks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
              -> SimpleDb WorkerTestRegistry IO ()
            handler jobs callbacks = do
              ackAll callbacks (toList jobs)
              liftIO $ do
                threadDelay handlerSleepMicros
                atomicModifyIORef' finishedRef (\n -> (n + 1, ()))
            capture _level msg _ctx = atomicModifyIORef' loggedRef (\ms -> (msg : ms, ()))

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 30))

        config <- defaultBatchedWorkerConfig 1 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , logConfig = defaultLogConfig {logDestination = LogCallback capture}
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 15_000 $ any ("finalized" `T.isInfixOf`) <$> readIORef loggedRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          logged <- readIORef loggedRef
          filter ("maximum job duration" `T.isInfixOf`) logged `shouldSatisfy` (not . null)

    describe "Lease fence" $ do
      it "stops a handler once its heartbeat can no longer reach the database" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        reasonsRef <- newIORef ([] :: [Text])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\n -> (n + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\n -> (n + 1, ()))
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks =
              defaultObservabilityHooks
                { onJobUnavailable = \_ reason ->
                    liftIO $ atomicModifyIORef' reasonsRef (\rs -> (reason : rs, ()))
                }

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 30))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 3
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          (reasons, finished) <-
            bracket_ (blockRowUpdates connStr) (unblockRowUpdates connStr) $ do
              waitUntil 20_000 $ not . null <$> readIORef reasonsRef
              (,) <$> readIORef reasonsRef <*> readIORef finishedRef
          finished `shouldBe` 0
          reasons `shouldBe` ["lease expired without renewal"]

      it "does not carry the fence past a row its extend did not move" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        reasonsRef <- newIORef ([] :: [Text])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\n -> (n + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\n -> (n + 1, ()))
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks =
              defaultObservabilityHooks
                { onJobUnavailable = \_ reason ->
                    liftIO $ atomicModifyIORef' reasonsRef (\rs -> (reason : rs, ()))
                }

        runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 30))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , visibilityTimeout = 5
                , jobHeartbeatInterval = 2
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ (== 1) <$> readIORef startedRef
          takeClaimHolder connStr
          waitUntil 15_000 $ not . null <$> readIORef reasonsRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          reasons <- readIORef reasonsRef
          reasons `shouldBe` ["lease expired without renewal"]

      it "survives a failed heartbeat when the retry pause fits the lease" $ \env ->
        survivesOneFailedHeartbeat connStr env 8 4 11_000_000
      it "survives a failed heartbeat under a short visibility timeout" $ \env ->
        survivesOneFailedHeartbeat connStr env 5 2 8_000_000

-- | One extend fails. A fenced handler re-runs, so the assertion is the run count.
survivesOneFailedHeartbeat
  :: ByteString
  -> SimpleEnv WorkerTestRegistry
  -> NominalDiffTime
  -- ^ Visibility timeout.
  -> NominalDiffTime
  -- ^ Heartbeat interval. The block spans it, so the extend due one interval in fails.
  -> Int
  -- ^ Handler run time in microseconds, which must outlast the lease.
  -> IO ()
survivesOneFailedHeartbeat connStr env timeout interval handlerMicros = do
  startedRef <- newIORef (0 :: Int)
  finishedRef <- newIORef (0 :: Int)
  let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
      handler _conn _job = liftIO $ do
        atomicModifyIORef' startedRef (\n -> (n + 1, ()))
        threadDelay handlerMicros
        atomicModifyIORef' finishedRef (\n -> (n + 1, ()))

  runSimpleDb env $ void $ HL.insertJob (defaultJob (SlowTask 1))

  config <- transactionalWorkerConfig 1 handler
  let workerConfig =
        config
          { pollInterval = 0.2
          , jitter = NoJitter
          , visibilityTimeout = timeout
          , jobHeartbeatInterval = interval
          , logConfig = silentLogConfig
          }
      blockMicros = ceiling (interval * 1_000_000) + 500_000

  withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
    waitUntil 10_000 $ (== 1) <$> readIORef startedRef
    bracket_ (blockRowUpdates connStr) (unblockRowUpdates connStr) (threadDelay blockMicros)
    waitUntil (handlerMicros `div` 1000 + 4_000) $ (== 1) <$> readIORef finishedRef
    started <- readIORef startedRef
    started `shouldBe` 1

listDLQ :: SimpleEnv WorkerTestRegistry -> IO [DLQJob WorkerTestPayload]
listDLQ env = runSimpleDb env (HL.listDLQJobs 10 0)

-- | Fail every row update, which is the heartbeat's only statement once a job is claimed.
blockRowUpdates :: ByteString -> IO ()
blockRowUpdates connStr = withFreshConn connStr $ \conn -> do
  void $
    PG.execute_
      conn
      ( fromString
          ( T.unpack
              ( "CREATE OR REPLACE FUNCTION "
                  <> blockFunction
                  <> "() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'update blocked'; END; $$"
              )
          )
      )
  void $
    PG.execute_
      conn
      ( fromString
          ( T.unpack
              ( "CREATE TRIGGER block_update BEFORE UPDATE ON "
                  <> qualifiedTable
                  <> " FOR EACH ROW EXECUTE FUNCTION "
                  <> blockFunction
                  <> "()"
              )
          )
      )

unblockRowUpdates :: ByteString -> IO ()
unblockRowUpdates connStr = withFreshConn connStr $ \conn -> do
  void $ PG.execute_ conn (fromString (T.unpack ("DROP TRIGGER IF EXISTS block_update ON " <> qualifiedTable)))
  void $ PG.execute_ conn (fromString (T.unpack ("DROP FUNCTION IF EXISTS " <> blockFunction <> "()")))

-- | Take the claim without bumping its token, so the extend reports 'VisibilityUnchanged'.
takeClaimHolder :: ByteString -> IO ()
takeClaimHolder connStr = withFreshConn connStr $ \conn ->
  void $
    PG.execute_
      conn
      ( fromString
          ( T.unpack
              ( "UPDATE "
                  <> qualifiedTable
                  <> " SET claimed_by = '00000000-0000-0000-0000-000000000009'::uuid WHERE claimed_by IS NOT NULL"
              )
          )
      )

qualifiedTable :: Text
qualifiedTable = testSchema <> "." <> testTable

blockFunction :: Text
blockFunction = testSchema <> ".block_update"

withFreshConn :: ByteString -> (PG.Connection -> IO a) -> IO a
withFreshConn connStr = bracket (connectPostgreSQL connStr) close

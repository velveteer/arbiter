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
import Control.Exception (uninterruptibleMask_)
import Control.Monad (void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.ByteString (ByteString)
import Data.Foldable (toList)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.List.NonEmpty (NonEmpty)
import Data.Maybe (isJust)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Clock (getMonotonicTime)
import Test.Hspec (Spec, around, beforeAll, describe, it, runIO, shouldBe, shouldSatisfy)
import UnliftIO (bracket, bracket_, finally, tryAny)
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

-- | Longer than any deadline under test.
handlerSleepMicros :: Int
handlerSleepMicros = 30_000_000

-- | Cleanup slow enough that the heartbeat outlives the fence throw.
unwindMicros :: Int
unwindMicros = 4_000_000

-- | A masked stretch longer than the deadline, so delivery has to wait it out.
maskedMicros :: Int
maskedMicros = 4_000_000

-- | How long a hung extend sleeps on the server. Longer than the lease under test.
hangSeconds :: Int
hangSeconds = 8

-- | The fence must give up on a hung extend well before the extend returns.
hungFenceMillis :: Int
hungFenceMillis = 5_000

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
      it "stops retrying once the lease is spent" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        loggedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job =
              liftIO (atomicModifyIORef' startedRef (\count -> (count + 1, ())) >> threadDelay handlerSleepMicros)
                `finally` liftIO (threadDelay unwindMicros)
            capture _level msg _ctx =
              when ("Heartbeat error" `T.isInfixOf` msg) $
                atomicModifyIORef' loggedRef (\count -> (count + 1, ()))

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
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay 9_000_000
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \_ _ _ -> liftIO (atomicModifyIORef' beatsRef (\count -> (count + 1, ())))
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
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))

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
                atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            capture _level msg _ctx = atomicModifyIORef' loggedRef (\messages -> (msg : messages, ()))

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

      it "keeps beating while a masked handler holds off the deadline" $ \env -> do
        startedRef <- newIORef (Nothing :: Maybe Double)
        finishedRef <- newIORef (0 :: Int)
        beatsRef <- newIORef ([] :: [Double])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              getMonotonicTime >>= writeIORef startedRef . Just
              uninterruptibleMask_ (threadDelay maskedMicros)
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks = defaultObservabilityHooks {onJobHeartbeat = \_ _ _ -> recordBeat beatsRef}

        runSimpleDb env
          $ void
          $ HL.insertJob (setMaxAttempts (Just 1) (defaultJob (SlowTask 30)))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , visibilityTimeout = 6
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ isJust <$> readIORef startedRef
          waitUntil 20_000 $ not . null <$> listDLQ env
          started <- maybe 0 id <$> readIORef startedRef
          beats <- readIORef beatsRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          length (filter (< started + maskedSeconds) beats) `shouldSatisfy` (>= 2)

      it "interrupts a handler that catches sync exceptions" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              void (tryAny (threadDelay handlerSleepMicros))
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))

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
          finished <- readIORef finishedRef
          finished `shouldBe` 0

      it "keeps the lease while a fenced handler unwinds" $ \env -> do
        startedRef <- newIORef (Nothing :: Maybe Double)
        finishedRef <- newIORef (0 :: Int)
        beatsRef <- newIORef ([] :: [Double])
        reasonsRef <- newIORef ([] :: [Text])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job =
              liftIO
                ( do
                    getMonotonicTime >>= writeIORef startedRef . Just
                    threadDelay handlerSleepMicros
                    atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
                )
                `finally` liftIO (threadDelay unwindMicros)
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks =
              defaultObservabilityHooks
                { onJobHeartbeat = \_ _ _ -> recordBeat beatsRef
                , onJobUnavailable = \_ reason ->
                    liftIO $ atomicModifyIORef' reasonsRef (\seen -> (reason : seen, ()))
                }

        runSimpleDb env
          $ void
          $ HL.insertJob (setMaxAttempts (Just 1) (defaultJob (SlowTask 30)))

        config <- transactionalWorkerConfig 1 handler
        let workerConfig =
              config
                { pollInterval = 0.2
                , jitter = NoJitter
                , maxJobDuration = Just 1
                , visibilityTimeout = 3
                , jobHeartbeatInterval = 1
                , observabilityHooks = hooks
                , logConfig = silentLogConfig
                }

        withAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
          waitUntil 10_000 $ isJust <$> readIORef startedRef
          waitUntil 20_000 $ not . null <$> listDLQ env
          started <- maybe 0 id <$> readIORef startedRef
          beats <- readIORef beatsRef
          reasons <- readIORef reasonsRef
          finished <- readIORef finishedRef
          finished `shouldBe` 0
          reasons `shouldBe` []
          dlq <- listDLQ env
          map (lastError . jobSnapshot) dlq
            `shouldBe` [Just "handler ran past the maximum job duration of 1s"]
          length (filter (> started + 1) beats) `shouldSatisfy` (>= 2)

    describe "Lease fence" $ do
      it "stops a handler once its heartbeat can no longer reach the database" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        reasonsRef <- newIORef ([] :: [Text])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks =
              defaultObservabilityHooks
                { onJobUnavailable = \_ reason ->
                    liftIO $ atomicModifyIORef' reasonsRef (\seen -> (reason : seen, ()))
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
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks =
              defaultObservabilityHooks
                { onJobUnavailable = \_ reason ->
                    liftIO $ atomicModifyIORef' reasonsRef (\seen -> (reason : seen, ()))
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

      it "stops a handler whose extend hangs past the lease" $ \env -> do
        startedRef <- newIORef (0 :: Int)
        finishedRef <- newIORef (0 :: Int)
        reasonsRef <- newIORef ([] :: [Text])
        let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ do
              atomicModifyIORef' startedRef (\count -> (count + 1, ()))
              threadDelay handlerSleepMicros
              atomicModifyIORef' finishedRef (\count -> (count + 1, ()))
            hooks :: ObservabilityHooks (SimpleDb WorkerTestRegistry IO) WorkerTestPayload
            hooks =
              defaultObservabilityHooks
                { onJobUnavailable = \_ reason ->
                    liftIO $ atomicModifyIORef' reasonsRef (\seen -> (reason : seen, ()))
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
            bracket_ (hangRowUpdates connStr) (unblockRowUpdates connStr) $ do
              waitUntil hungFenceMillis $ not . null <$> readIORef reasonsRef
              (,) <$> readIORef reasonsRef <*> readIORef finishedRef
          finished `shouldBe` 0
          reasons `shouldBe` ["lease expired without renewal"]
        awaitHungUpdates connStr

      it "survives a failed heartbeat when the retry pause fits the lease" $ \env ->
        survivesOneFailedHeartbeat connStr env 8 4 11_000_000
      it "survives a failed heartbeat under a short visibility timeout" $ \env ->
        survivesOneFailedHeartbeat connStr env 5 2 8_000_000

-- | One extend fails. The assertion is the run count.
survivesOneFailedHeartbeat
  :: ByteString
  -> SimpleEnv WorkerTestRegistry
  -> NominalDiffTime
  -- ^ Visibility timeout.
  -> NominalDiffTime
  -- ^ Heartbeat interval. The block spans it and fails the extend due one interval in.
  -> Int
  -- ^ Handler run time in microseconds. It must outlast the lease.
  -> IO ()
survivesOneFailedHeartbeat connStr env timeout interval handlerMicros = do
  startedRef <- newIORef (0 :: Int)
  finishedRef <- newIORef (0 :: Int)
  let handler :: JobHandler (SimpleDb WorkerTestRegistry IO) WorkerTestPayload ()
      handler _conn _job = liftIO $ do
        atomicModifyIORef' startedRef (\count -> (count + 1, ()))
        threadDelay handlerMicros
        atomicModifyIORef' finishedRef (\count -> (count + 1, ()))

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

maskedSeconds :: Double
maskedSeconds = fromIntegral maskedMicros / 1_000_000

recordBeat :: (MonadIO m) => IORef [Double] -> m ()
recordBeat beatsRef = liftIO $ getMonotonicTime >>= \now -> atomicModifyIORef' beatsRef (\beats -> (now : beats, ()))

-- | Fail every row update. Once a job is claimed, the heartbeat's only statement is an update.
blockRowUpdates :: ByteString -> IO ()
blockRowUpdates = installUpdateTrigger "BEGIN RAISE EXCEPTION 'update blocked'; END;"

-- | Stall every row update on the server for 'hangSeconds', then let it through.
hangRowUpdates :: ByteString -> IO ()
hangRowUpdates = installUpdateTrigger ("BEGIN PERFORM pg_sleep(" <> T.pack (show hangSeconds) <> "); RETURN NEW; END;")

-- | Wait for the stalled extends to finish on the server, so the next test starts clean.
awaitHungUpdates :: ByteString -> IO ()
awaitHungUpdates connStr =
  waitUntil ((hangSeconds + 4) * 1_000) $ withFreshConn connStr $ \conn -> do
    [PG.Only sleeping] <-
      PG.query_
        conn
        "SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND query LIKE '%pg_sleep%' AND pid <> pg_backend_pid()"
    pure (sleeping == (0 :: Int))

installUpdateTrigger :: Text -> ByteString -> IO ()
installUpdateTrigger body connStr = withFreshConn connStr $ \conn -> do
  void $
    PG.execute_
      conn
      ( fromString
          ( T.unpack
              ( "CREATE OR REPLACE FUNCTION "
                  <> blockFunction
                  <> "() RETURNS trigger LANGUAGE plpgsql AS $$ "
                  <> body
                  <> " $$"
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

-- | Take the claim without bumping its token. The extend then reports 'VisibilityUnchanged'.
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

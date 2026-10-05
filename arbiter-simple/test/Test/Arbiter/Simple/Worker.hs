{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Simple.Worker
  ( spec
  , listenerSpec
  , multiQueueSpec
  , deadlineSpec
  , cronSpec
  , reclaimSpec
  , connectionRecoverySpec
  , lifecycleSpec
  ) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (defaultJob, setGroupKey)
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Test.Setup (addQueueTable, cleanupData, cleanupOnce, createPoolOf, createSharedPool, setupOnce)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)
import Arbiter.Worker.TestKit qualified as TestKit
import Control.Concurrent (threadDelay)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Pool (Pool, withResource)
import Data.Proxy (Proxy (..))
import Data.Text (Text, pack)
import Database.PostgreSQL.Simple (Connection)
import GHC.Generics (Generic)
import Test.Hspec (Spec, afterAll_, beforeAll, describe, it, runIO, shouldBe)

import Arbiter.Simple
  ( SimpleDb
  , SimpleEnv
  , createSimpleEnv
  , createSimpleEnvWithPool
  , destroySimpleEnv
  , disableListener
  , runSimpleDb
  , useDedicatedListener
  )

-- | Build the schema and one env over a shared pool, then run a suite over the backend.
withSimpleBackend
  :: Proxy registry
  -> ByteString
  -> Text
  -> (TestKit.TestBackend WorkerTestPayload (SimpleDb registry IO) (SimpleEnv registry) -> Spec)
  -> Spec
withSimpleBackend proxy connStr schema suite =
  beforeAll (setupOnce connStr schema schema True) $ do
    pool <- runIO (createSharedPool connStr)
    env <- runIO (createSimpleEnvWithPool proxy pool schema)
    afterAll_ (destroySimpleEnv env) $ suite (simpleBackend proxy connStr schema pool env)

simpleBackend
  :: Proxy registry
  -> ByteString
  -> Text
  -> Pool Connection
  -> SimpleEnv registry
  -> TestKit.TestBackend WorkerTestPayload (SimpleDb registry IO) (SimpleEnv registry)
simpleBackend proxy connStr schema pool env =
  TestKit.TestBackend
    { schema
    , table = schema
    , connStr
    , mkSimple = SimpleTask
    , mkFailing = FailingTask
    , mkEnv = emptied >> pure env
    , pollOnly = disableListener
    , mkFreshEnv = emptied >> createSimpleEnv proxy connStr schema
    , destroyEnv = destroySimpleEnv
    , mkHandler = TestKit.plainHandler
    , runCommand = TestKit.statementCommand
    , runM = runSimpleDb
    }
  where
    emptied = withResource pool (cleanupData schema schema)

testSchema :: Text
testSchema = "arbiter_worker_test"

type WorkerTestRegistry = '[QueueWithResult "arbiter_worker_test" WorkerTestPayload (Maybe [Text])]

spec :: ByteString -> Spec
spec connStr = withSimpleBackend (Proxy @WorkerTestRegistry) connStr testSchema TestKit.workerSpec

listenSchema :: Text
listenSchema = "arbiter_worker_listen_test"

type ListenTestRegistry = '[Queue "arbiter_worker_listen_test" WorkerTestPayload]

listenerSpec :: ByteString -> Spec
listenerSpec connStr =
  withSimpleBackend (Proxy @ListenTestRegistry) connStr listenSchema $ \backend -> do
    TestKit.listenerSpec backend
    dedicatedListenerSpec connStr

dedicatedListenerSpec :: ByteString -> Spec
dedicatedListenerSpec connStr =
  describe "dedicated listener" $
    it "wakes the dispatcher on a size-1 pool the pool listener would starve" $ do
      cleanupOnce connStr listenSchema listenSchema
      pool <- createPoolOf 1 connStr
      env <- useDedicatedListener connStr =<< createSimpleEnvWithPool (Proxy @ListenTestRegistry) pool listenSchema
      ref <- newIORef (0 :: Int)
      let handler :: JobHandler (SimpleDb ListenTestRegistry IO) WorkerTestPayload ()
          handler _conn _job = liftIO $ atomicModifyIORef' ref $ \count -> (count + 1, ())
      config <- transactionalWorkerConfig 1 handler
      let workerConfig = config {workerCount = 1, pollInterval = 300, jitter = NoJitter}
      withLinkedAsync (runSimpleDb env $ runWorkerPool workerConfig) $ \_ -> do
        threadDelay 1_000_000
        runSimpleDb env
          $ void
          $ HL.insertJob
          $ setGroupKey (Just "g1")
          $ defaultJob (SimpleTask "dedicated")
        waitUntil 5_000 $ (== 1) <$> readIORef ref
        readIORef ref >>= (`shouldBe` 1)
      destroySimpleEnv env

newtype QueueAPayload = QueueAPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype QueueBPayload = QueueBPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type MultiQRegistry =
  '[ Queue "mq_listen_a" QueueAPayload
   , Queue "mq_listen_b" QueueBPayload
   ]

mqSchema :: Text
mqSchema = "mq_listen_test"

mqTableA :: Text
mqTableA = "mq_listen_a"

mqTableB :: Text
mqTableB = "mq_listen_b"

multiQueueSpec :: ByteString -> Spec
multiQueueSpec connStr =
  beforeAll (setupOnce connStr mqSchema mqTableA True >> addQueueTable connStr mqSchema mqTableB True) $
    TestKit.multiQueueListenerSpec backend mqTableB QueueBPayload TestKit.plainHandler
  where
    backend =
      TestKit.TestBackend
        { schema = mqSchema
        , table = mqTableA
        , connStr
        , mkSimple = QueueAPayload
        , mkFailing = QueueAPayload . pack . show
        , mkEnv = freshEnv
        , pollOnly = disableListener
        , mkFreshEnv = freshEnv
        , destroyEnv = destroySimpleEnv
        , mkHandler = TestKit.plainHandler
        , runCommand = TestKit.statementCommand
        , runM = runSimpleDb
        }
    freshEnv = do
      cleanupOnce connStr mqSchema mqTableA
      cleanupOnce connStr mqSchema mqTableB
      createSimpleEnv (Proxy @MultiQRegistry) connStr mqSchema

deadlineSchema :: Text
deadlineSchema = "arbiter_worker_deadline_test"

type DeadlineRegistry = '[Queue "arbiter_worker_deadline_test" WorkerTestPayload]

deadlineSpec :: ByteString -> Spec
deadlineSpec connStr = withSimpleBackend (Proxy @DeadlineRegistry) connStr deadlineSchema TestKit.deadlineSpec

cronSchema :: Text
cronSchema = "arbiter_cron_test"

type CronRegistry = '[Queue "arbiter_cron_test" WorkerTestPayload]

cronSpec :: ByteString -> Spec
cronSpec connStr = withSimpleBackend (Proxy @CronRegistry) connStr cronSchema TestKit.cronSpec

reclaimSchema :: Text
reclaimSchema = "arbiter_worker_concurrency_test"

type ReclaimRegistry = '[Queue "arbiter_worker_concurrency_test" WorkerTestPayload]

reclaimSpec :: ByteString -> Spec
reclaimSpec connStr = withSimpleBackend (Proxy @ReclaimRegistry) connStr reclaimSchema TestKit.reclaimSpec

recoverySchema :: Text
recoverySchema = "arbiter_worker_recovery_test"

type RecoveryRegistry = '[Queue "arbiter_worker_recovery_test" WorkerTestPayload]

connectionRecoverySpec :: ByteString -> Spec
connectionRecoverySpec connStr =
  withSimpleBackend (Proxy @RecoveryRegistry) connStr recoverySchema TestKit.connectionRecoverySpec

lifecycleSchema :: Text
lifecycleSchema = "arbiter_worker_lifecycle_test"

type LifecycleRegistry = '[QueueWithResult "arbiter_worker_lifecycle_test" WorkerTestPayload (Maybe [Text])]

lifecycleSpec :: ByteString -> Spec
lifecycleSpec connStr = withSimpleBackend (Proxy @LifecycleRegistry) connStr lifecycleSchema TestKit.lifecycleSpec

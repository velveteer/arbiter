{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Hasql.Worker
  ( spec
  , listenerSpec
  , multiQueueSpec
  , deadlineSpec
  , cronSpec
  , reclaimSpec
  , connectionRecoverySpec
  , lifecycleSpec
  ) where

import Arbiter.Core.Listen (withChannels)
import Arbiter.Core.MonadArbiter (JobHandler, getListener)
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Test.Fixtures (QueueAPayload (..), QueueBPayload (..), WorkerTestPayload (..))
import Arbiter.Test.Poll (bumpRef, waitUntil)
import Arbiter.Test.Setup (addQueueTable, cleanupOnce, execute_, quietHubLog, setupOnce, withConn)
import Arbiter.Test.Worker qualified as TestKit
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (transactionalWorkerConfig)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically)
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Data.IORef (newIORef, readIORef)
import Data.Maybe (isJust)
import Data.Proxy (Proxy (..))
import Data.Text (Text, pack)
import System.Timeout (timeout)
import Test.Hspec
import UnliftIO.Async (async, cancel)

import Arbiter.Hasql.HasqlDb
  ( HasqlDb
  , HasqlEnv
  , createHasqlEnv
  , createHasqlEnvWithPool
  , destroyHasqlEnv
  , disableListener
  , runHasqlDb
  , useDedicatedListener
  )
import Test.Arbiter.Hasql.TestHelpers
  ( createHasqlPool
  , createHasqlPoolWith
  , nativeConnect
  , refusingCancelConnect
  , runHasqlCommand
  , testConnect
  )

-- | A pause for the worker to start.
workerStartMicros :: Int
workerStartMicros = 1_000_000

-- | The most time a stop may take.
stopTimeoutMicros :: Int
stopTimeoutMicros = 5_000_000

workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_hasql_worker_test"

type HasqlWorkerTestRegistry = '[QueueWithResult "arbiter_hasql_worker_test" WorkerTestPayload (Maybe [Text])]

spec :: ByteString -> Spec
spec connStr = withHasqlBackend (Proxy @HasqlWorkerTestRegistry) connStr workerTestSchemaName TestKit.workerSpec

listenSchema :: Text
listenSchema = "arbiter_hasql_listen_test"

type HasqlListenRegistry = '[Queue "arbiter_hasql_listen_test" WorkerTestPayload]

listenerSpec :: ByteString -> Spec
listenerSpec connStr =
  withHasqlBackend (Proxy @HasqlListenRegistry) connStr listenSchema $ \backend -> do
    TestKit.listenerSpec backend
    dedicatedListenerSpec connStr
    transportListenerSpec connStr

-- | An address that never completes the TCP handshake.
blackHoleConnStr :: ByteString
blackHoleConnStr = "host=10.255.255.1 port=5432 dbname=arbiter user=arbiter"

dedicatedListenerSpec :: ByteString -> Spec
dedicatedListenerSpec connStr =
  describe "dedicated listener" $
    it "lets pool shutdown interrupt a connect in progress" $ do
      cleanupOnce connStr listenSchema listenSchema
      pool <- createHasqlPool 1 connStr
      env <-
        useDedicatedListener (testConnect blackHoleConnStr)
          =<< createHasqlEnvWithPool (Proxy @HasqlListenRegistry) pool listenSchema
      let handler :: JobHandler (HasqlDb HasqlListenRegistry IO) WorkerTestPayload ()
          handler _conn _job = pure ()
      config <- transactionalWorkerConfig 1 handler
      worker <- async (runHasqlDb env (runWorkerPool config))
      threadDelay workerStartMicros
      stopped <- timeout stopTimeoutMicros (cancel worker)
      stopped `shouldSatisfy` isJust
      destroyHasqlEnv env

transportListenerSpec :: ByteString -> Spec
transportListenerSpec connStr =
  describe "pool listener" $ do
    for_ nativeConnect $ \connect ->
      it "delivers a NOTIFY over the native transport" $ do
        env <- createHasqlEnv (Proxy @HasqlListenRegistry) (connect connStr) listenSchema
        Just listener <- runHasqlDb env getListener
        received <- newIORef (0 :: Int)
        let chan = "arbiter_hasql_native_listen"
        withChannels listener quietHubLog [(chan, \_ -> bumpRef received)] $ \ready -> do
          waitUntil 5_000 (atomically ready)
          withConn connStr $ \conn -> execute_ conn "NOTIFY arbiter_hasql_native_listen"
          waitUntil 5_000 $ (== 1) <$> readIORef received
        destroyHasqlEnv env
    for_ refusingCancelConnect $ \connect ->
      it "stops on deregistration when the session cleanup fails" $ do
        pool <- createHasqlPoolWith connect 2 connStr
        env <- createHasqlEnvWithPool (Proxy @HasqlListenRegistry) pool listenSchema
        Just listener <- runHasqlDb env getListener
        stopped <-
          timeout stopTimeoutMicros $
            withChannels listener quietHubLog [("arbiter_hasql_refused_cancel", \_ -> pure ())] $ \ready ->
              waitUntil 5_000 (atomically ready)
        stopped `shouldSatisfy` isJust
        destroyHasqlEnv env

mqSchema :: Text
mqSchema = "arbiter_hasql_mq_test"

mqTableA :: Text
mqTableA = "mqh_listen_a"

mqTableB :: Text
mqTableB = "mqh_listen_b"

type HasqlMultiQRegistry =
  '[ Queue "mqh_listen_a" QueueAPayload
   , Queue "mqh_listen_b" QueueBPayload
   ]

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
        , destroyEnv = destroyHasqlEnv
        , mkHandler = TestKit.plainHandler
        , runCommand = runHasqlCommand
        , runM = runHasqlDb
        }
    freshEnv = do
      cleanupOnce connStr mqSchema mqTableA
      cleanupOnce connStr mqSchema mqTableB
      createHasqlEnv (Proxy @HasqlMultiQRegistry) (testConnect connStr) mqSchema

workerPoolSize :: Int
workerPoolSize = 10

-- | Build the schema and one env over a shared pool, then run a suite over the backend.
withHasqlBackend
  :: Proxy registry
  -> ByteString
  -> Text
  -> (TestKit.TestBackend WorkerTestPayload (HasqlDb registry IO) (HasqlEnv registry) -> Spec)
  -> Spec
withHasqlBackend proxy connStr schema suite =
  beforeAll (setupOnce connStr schema schema True) $ do
    pool <- runIO (createHasqlPool workerPoolSize connStr)
    env <- runIO (createHasqlEnvWithPool proxy pool schema)
    afterAll_ (destroyHasqlEnv env) $ suite (hasqlBackend proxy connStr schema env)

hasqlBackend
  :: Proxy registry
  -> ByteString
  -> Text
  -> HasqlEnv registry
  -> TestKit.TestBackend WorkerTestPayload (HasqlDb registry IO) (HasqlEnv registry)
hasqlBackend proxy connStr schema env =
  TestKit.TestBackend
    { schema
    , table = schema
    , connStr
    , mkSimple = SimpleTask
    , mkFailing = FailingTask
    , mkEnv = cleanupOnce connStr schema schema >> pure env
    , pollOnly = disableListener
    , mkFreshEnv = cleanupOnce connStr schema schema >> createHasqlEnv proxy (testConnect connStr) schema
    , destroyEnv = destroyHasqlEnv
    , mkHandler = TestKit.plainHandler
    , runCommand = runHasqlCommand
    , runM = runHasqlDb
    }

deadlineSchema :: Text
deadlineSchema = "arbiter_hasql_deadline_test"

type HasqlDeadlineRegistry = '[Queue "arbiter_hasql_deadline_test" WorkerTestPayload]

deadlineSpec :: ByteString -> Spec
deadlineSpec connStr = withHasqlBackend (Proxy @HasqlDeadlineRegistry) connStr deadlineSchema TestKit.deadlineSpec

cronSchema :: Text
cronSchema = "arbiter_hasql_cron_test"

type HasqlCronRegistry = '[Queue "arbiter_hasql_cron_test" WorkerTestPayload]

cronSpec :: ByteString -> Spec
cronSpec connStr = withHasqlBackend (Proxy @HasqlCronRegistry) connStr cronSchema TestKit.cronSpec

reclaimSchema :: Text
reclaimSchema = "arbiter_hasql_reclaim_test"

type HasqlReclaimRegistry = '[Queue "arbiter_hasql_reclaim_test" WorkerTestPayload]

reclaimSpec :: ByteString -> Spec
reclaimSpec connStr = withHasqlBackend (Proxy @HasqlReclaimRegistry) connStr reclaimSchema TestKit.reclaimSpec

recoverySchema :: Text
recoverySchema = "arbiter_hasql_recovery_test"

type HasqlRecoveryRegistry = '[Queue "arbiter_hasql_recovery_test" WorkerTestPayload]

connectionRecoverySpec :: ByteString -> Spec
connectionRecoverySpec connStr =
  withHasqlBackend (Proxy @HasqlRecoveryRegistry) connStr recoverySchema TestKit.connectionRecoverySpec

lifecycleSchema :: Text
lifecycleSchema = "arbiter_hasql_lifecycle_test"

type HasqlLifecycleRegistry = '[QueueWithResult "arbiter_hasql_lifecycle_test" WorkerTestPayload (Maybe [Text])]

lifecycleSpec :: ByteString -> Spec
lifecycleSpec connStr = withHasqlBackend (Proxy @HasqlLifecycleRegistry) connStr lifecycleSchema TestKit.lifecycleSpec

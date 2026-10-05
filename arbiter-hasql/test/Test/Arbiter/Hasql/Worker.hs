{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
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

import Arbiter.Core.Listen (HubLog (..), withChannels)
import Arbiter.Core.MonadArbiter (JobHandler, getListener)
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (addQueueTable, cleanupOnce, execute_, setupOnce, withConn)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (transactionalWorkerConfig)
import Arbiter.Worker.TestKit qualified as TestKit
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Foldable (for_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Maybe (isJust)
import Data.Proxy (Proxy (..))
import Data.Text (Text, pack)
import GHC.Generics (Generic)
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

workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_hasql_worker_test"

data HasqlWorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type HasqlWorkerTestRegistry = '[QueueWithResult "arbiter_hasql_worker_test" HasqlWorkerTestPayload (Maybe [Text])]

spec :: ByteString -> Spec
spec connStr = withHasqlBackend (Proxy @HasqlWorkerTestRegistry) connStr workerTestSchemaName TestKit.workerSpec

listenSchema :: Text
listenSchema = "arbiter_hasql_listen_test"

type HasqlListenRegistry = '[Queue "arbiter_hasql_listen_test" HasqlWorkerTestPayload]

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
      let handler :: JobHandler (HasqlDb HasqlListenRegistry IO) HasqlWorkerTestPayload ()
          handler _conn _job = pure ()
      config <- transactionalWorkerConfig 1 handler
      worker <- async (runHasqlDb env (runWorkerPool config))
      threadDelay 1_000_000
      stopped <- timeout 5_000_000 (cancel worker)
      stopped `shouldSatisfy` isJust
      destroyHasqlEnv env

-- | A hub log that reports nothing.
quietHubLog :: HubLog
quietHubLog = HubLog {hubRecovered = \_ -> pure (), hubWarn = \_ -> pure (), hubError = \_ -> pure (), hubRepeatInterval = 1}

transportListenerSpec :: ByteString -> Spec
transportListenerSpec connStr =
  describe "pool listener" $ do
    for_ nativeConnect $ \connect ->
      it "delivers a NOTIFY over the native transport" $ do
        env <- createHasqlEnv (Proxy @HasqlListenRegistry) (connect connStr) listenSchema
        Just listener <- runHasqlDb env getListener
        received <- newIORef (0 :: Int)
        let chan = "arbiter_hasql_native_listen"
        withChannels listener quietHubLog [(chan, \_ -> atomicModifyIORef' received (\n -> (n + 1, ())))] $ \ready -> do
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
          timeout 5_000_000 $
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

newtype MqAPayload = MqAPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype MqBPayload = MqBPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type HasqlMultiQRegistry =
  '[ Queue "mqh_listen_a" MqAPayload
   , Queue "mqh_listen_b" MqBPayload
   ]

multiQueueSpec :: ByteString -> Spec
multiQueueSpec connStr =
  beforeAll (setupOnce connStr mqSchema mqTableA True >> addQueueTable connStr mqSchema mqTableB True) $
    TestKit.multiQueueListenerSpec backend mqTableB MqBPayload TestKit.plainHandler
  where
    backend =
      TestKit.TestBackend
        { schema = mqSchema
        , table = mqTableA
        , connStr
        , mkSimple = MqAPayload
        , mkFailing = MqAPayload . pack . show
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
  -> (TestKit.TestBackend HasqlWorkerTestPayload (HasqlDb registry IO) (HasqlEnv registry) -> Spec)
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
  -> TestKit.TestBackend HasqlWorkerTestPayload (HasqlDb registry IO) (HasqlEnv registry)
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

type HasqlDeadlineRegistry = '[Queue "arbiter_hasql_deadline_test" HasqlWorkerTestPayload]

deadlineSpec :: ByteString -> Spec
deadlineSpec connStr = withHasqlBackend (Proxy @HasqlDeadlineRegistry) connStr deadlineSchema TestKit.deadlineSpec

cronSchema :: Text
cronSchema = "arbiter_hasql_cron_test"

type HasqlCronRegistry = '[Queue "arbiter_hasql_cron_test" HasqlWorkerTestPayload]

cronSpec :: ByteString -> Spec
cronSpec connStr = withHasqlBackend (Proxy @HasqlCronRegistry) connStr cronSchema TestKit.cronSpec

reclaimSchema :: Text
reclaimSchema = "arbiter_hasql_reclaim_test"

type HasqlReclaimRegistry = '[Queue "arbiter_hasql_reclaim_test" HasqlWorkerTestPayload]

reclaimSpec :: ByteString -> Spec
reclaimSpec connStr = withHasqlBackend (Proxy @HasqlReclaimRegistry) connStr reclaimSchema TestKit.reclaimSpec

recoverySchema :: Text
recoverySchema = "arbiter_hasql_recovery_test"

type HasqlRecoveryRegistry = '[Queue "arbiter_hasql_recovery_test" HasqlWorkerTestPayload]

connectionRecoverySpec :: ByteString -> Spec
connectionRecoverySpec connStr =
  withHasqlBackend (Proxy @HasqlRecoveryRegistry) connStr recoverySchema TestKit.connectionRecoverySpec

lifecycleSchema :: Text
lifecycleSchema = "arbiter_hasql_lifecycle_test"

type HasqlLifecycleRegistry = '[QueueWithResult "arbiter_hasql_lifecycle_test" HasqlWorkerTestPayload (Maybe [Text])]

lifecycleSpec :: ByteString -> Spec
lifecycleSpec connStr = withHasqlBackend (Proxy @HasqlLifecycleRegistry) connStr lifecycleSchema TestKit.lifecycleSpec

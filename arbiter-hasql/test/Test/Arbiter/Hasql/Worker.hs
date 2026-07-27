{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Hasql.Worker (spec, listenerSpec, multiQueueSpec) where

import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))
import Arbiter.Test.Setup (addQueueTable, setupOnce)
import Arbiter.Worker.TestKit (workerSpec)
import Arbiter.Worker.TestKit qualified as TestKit
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import GHC.Generics (Generic)
import Test.Hspec

import Arbiter.Hasql.HasqlDb
  ( createHasqlEnv
  , createHasqlEnvWithPool
  , destroyHasqlEnv
  , disableListener
  , runHasqlDb
  )
import Test.Arbiter.Hasql.TestHelpers (cleanupHasqlTest, createHasqlPool)

workerTestSchemaName :: Text
workerTestSchemaName = "arbiter_hasql_worker_test"

data HasqlWorkerTestPayload
  = SimpleTask Text
  | FailingTask Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type HasqlWorkerTestRegistry = '[QueueWithResult "arbiter_hasql_worker_test" HasqlWorkerTestPayload (Maybe [Text])]

testTable :: Text
testTable = "arbiter_hasql_worker_test"

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr workerTestSchemaName testTable False >> createHasqlPool 10 connStr) $
    beforeWith (\pool -> cleanupHasqlTest connStr workerTestSchemaName testTable >> pure pool) $ do
      let runM pool act = do
            env <- createHasqlEnvWithPool (Proxy @HasqlWorkerTestRegistry) pool workerTestSchemaName
            runHasqlDb env act
      workerSpec @HasqlWorkerTestPayload
        SimpleTask
        FailingTask
        (\f _conn job -> f job)
        runM

listenSchema :: Text
listenSchema = "arbiter_hasql_listen_test"

type HasqlListenRegistry = '[Queue "arbiter_hasql_listen_test" HasqlWorkerTestPayload]

listenerSpec :: ByteString -> Spec
listenerSpec connStr =
  beforeAll (setupOnce connStr listenSchema listenSchema True) $
    TestKit.listenerSpec @HasqlWorkerTestPayload
      listenSchema
      connStr
      SimpleTask
      (cleanupHasqlTest connStr listenSchema listenSchema >> createHasqlEnv (Proxy @HasqlListenRegistry) connStr listenSchema)
      ( cleanupHasqlTest connStr listenSchema listenSchema
          >> (disableListener <$> createHasqlEnv (Proxy @HasqlListenRegistry) connStr listenSchema)
      )
      destroyHasqlEnv
      (\f _conn job -> f job)
      runHasqlDb

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
    TestKit.multiQueueListenerSpec @MqAPayload @MqBPayload
      mqTableA
      mqTableB
      connStr
      MqAPayload
      MqBPayload
      mkEnv
      destroyHasqlEnv
      (\f _conn job -> f job)
      runHasqlDb
  where
    mkEnv = do
      cleanupHasqlTest connStr mqSchema mqTableA
      cleanupHasqlTest connStr mqSchema mqTableB
      createHasqlEnv (Proxy @HasqlMultiQRegistry) connStr mqSchema

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Shared-listener tests on the orville backend, covering both a single queue
-- and multiple queues sharing one env's dedicated LISTEN connection.
module Test.Arbiter.Orville.Listener (listenerSpec, multiQueueSpec) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Setup (addQueueTable, cleanupOnce, setupOnce)
import Arbiter.Worker.TestKit qualified as TestKit
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Text (Text)
import GHC.Generics (Generic)
import Test.Hspec (Spec, beforeAll)

import Test.Arbiter.Orville.TestHelpers
  ( TestOrville
  , createOrvilleTestEnv
  , destroyOrvilleTestEnv
  , orvilleTestHandler
  , runOrvilleTest
  )
import Test.Arbiter.Orville.Worker (OrvilleWorkerTestPayload, withOrvilleBackend)

listenSchema :: Text
listenSchema = "arbiter_orville_listen_test"

type OrvilleListenRegistry = '[Queue "arbiter_orville_listen_test" OrvilleWorkerTestPayload]

listenerSpec :: ByteString -> Spec
listenerSpec connStr = withOrvilleBackend @OrvilleListenRegistry connStr listenSchema TestKit.listenerSpec

mqSchema :: Text
mqSchema = "arbiter_orville_mq_test"

mqTableA :: Text
mqTableA = "mqo_listen_a"

mqTableB :: Text
mqTableB = "mqo_listen_b"

newtype MqAPayload = MqAPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype MqBPayload = MqBPayload Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type OrvilleMultiQRegistry =
  '[ Queue "mqo_listen_a" MqAPayload
   , Queue "mqo_listen_b" MqBPayload
   ]

multiQueueSpec :: ByteString -> Spec
multiQueueSpec connStr =
  beforeAll (setupOnce connStr mqSchema mqTableA True >> addQueueTable connStr mqSchema mqTableB True) $
    TestKit.multiQueueListenerSpec @MqAPayload @MqBPayload @(TestOrville OrvilleMultiQRegistry)
      mqTableA
      mqTableB
      connStr
      MqAPayload
      MqBPayload
      mkEnv
      destroyOrvilleTestEnv
      (orvilleTestHandler mqSchema)
      runOrvilleTest
  where
    mkEnv = do
      cleanupOnce connStr mqSchema mqTableA
      cleanupOnce connStr mqSchema mqTableB
      createOrvilleTestEnv connStr mqSchema mqTableA 10

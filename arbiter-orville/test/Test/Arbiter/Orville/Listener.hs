{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Shared-listener tests on the orville backend, covering both a single queue
-- and multiple queues sharing one env's dedicated LISTEN connection.
module Test.Arbiter.Orville.Listener (listenerSpec, multiQueueSpec) where

import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Fixtures (QueueAPayload (..), QueueBPayload (..), WorkerTestPayload)
import Arbiter.Test.Setup (addQueueTable, cleanupOnce, setupOnce)
import Arbiter.Worker.TestKit qualified as TestKit
import Data.ByteString (ByteString)
import Data.Text (Text, pack)
import Test.Hspec (Spec, beforeAll)

import Test.Arbiter.Orville.TestHelpers
  ( OrvilleTestEnv
  , TestOrville
  , createOrvilleTestEnv
  , destroyOrvilleTestEnv
  , disableOrvilleListener
  , orvillePoolSize
  , orvilleTestHandler
  , runOrvilleTest
  )
import Test.Arbiter.Orville.Worker (withOrvilleBackend)

listenSchema :: Text
listenSchema = "arbiter_orville_listen_test"

type OrvilleListenRegistry = '[Queue "arbiter_orville_listen_test" WorkerTestPayload]

listenerSpec :: ByteString -> Spec
listenerSpec connStr = withOrvilleBackend @OrvilleListenRegistry connStr listenSchema TestKit.listenerSpec

mqSchema :: Text
mqSchema = "arbiter_orville_mq_test"

mqTableA :: Text
mqTableA = "mqo_listen_a"

mqTableB :: Text
mqTableB = "mqo_listen_b"

type OrvilleMultiQRegistry =
  '[ Queue "mqo_listen_a" QueueAPayload
   , Queue "mqo_listen_b" QueueBPayload
   ]

multiQueueSpec :: ByteString -> Spec
multiQueueSpec connStr =
  beforeAll (setupOnce connStr mqSchema mqTableA True >> addQueueTable connStr mqSchema mqTableB True) $
    TestKit.multiQueueListenerSpec backend mqTableB QueueBPayload (orvilleTestHandler mqSchema)
  where
    backend :: TestKit.TestBackend QueueAPayload (TestOrville OrvilleMultiQRegistry) (OrvilleTestEnv OrvilleMultiQRegistry)
    backend =
      TestKit.TestBackend
        { schema = mqSchema
        , table = mqTableA
        , connStr
        , mkSimple = QueueAPayload
        , mkFailing = QueueAPayload . pack . show
        , mkEnv = freshEnv
        , pollOnly = disableOrvilleListener
        , mkFreshEnv = freshEnv
        , destroyEnv = destroyOrvilleTestEnv
        , mkHandler = orvilleTestHandler mqSchema
        , runCommand = TestKit.statementCommand
        , runM = runOrvilleTest
        }
    freshEnv = do
      cleanupOnce connStr mqSchema mqTableA
      cleanupOnce connStr mqSchema mqTableB
      createOrvilleTestEnv connStr mqSchema mqTableA orvillePoolSize

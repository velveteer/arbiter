{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | Shared listener tests for the env-owned LISTEN hub.
module Test.Arbiter.Worker.SharedListener (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (defaultJob, setGroupKey)
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Simple
  ( SimpleDb
  , createSimpleEnv
  , createSimpleEnvWithPool
  , destroySimpleEnv
  , disableListener
  , runSimpleDb
  , useDedicatedListener
  )
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Test.Setup (cleanupData, setupOnce)
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)
import Control.Concurrent (threadDelay)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Pool (defaultPoolConfig, newPool, setNumStripes)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Test.Hspec (Spec, beforeAll, describe, it, shouldBe)

import Arbiter.Worker.TestKit (listenerSpec)

type ListenTestRegistry = '[Queue "arbiter_worker_listen_test" WorkerTestPayload]

testSchema :: Text
testSchema = "arbiter_worker_listen_test"

cleanup :: ByteString -> IO ()
cleanup connStr = do
  conn <- connectPostgreSQL connStr
  cleanupData testSchema testSchema conn
  close conn

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr testSchema testSchema True) $ do
    listenerSpec @WorkerTestPayload
      testSchema
      connStr
      SimpleTask
      (cleanup connStr >> createSimpleEnv (Proxy @ListenTestRegistry) connStr testSchema)
      (cleanup connStr >> (disableListener <$> createSimpleEnv (Proxy @ListenTestRegistry) connStr testSchema))
      destroySimpleEnv
      (\f _conn job -> f job)
      runSimpleDb
    dedicatedListenerSpec connStr

dedicatedListenerSpec :: ByteString -> Spec
dedicatedListenerSpec connStr =
  describe "dedicated listener" $
    it "wakes the dispatcher on a size-1 pool the pool listener would starve" $ do
      cleanup connStr
      pool <- newPool $ setNumStripes (Just 1) $ defaultPoolConfig (connectPostgreSQL connStr) close 60 1
      env <- useDedicatedListener connStr =<< createSimpleEnvWithPool (Proxy @ListenTestRegistry) pool testSchema
      ref <- newIORef (0 :: Int)
      let handler :: JobHandler (SimpleDb ListenTestRegistry IO) WorkerTestPayload ()
          handler _conn _job = liftIO $ atomicModifyIORef' ref $ \n -> (n + 1, ())
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

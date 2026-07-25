{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | poolConfigForWorkers sizes the pool to the enabled workers.
module Test.Arbiter.Worker.PoolSizing (spec) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (defaultJob)
import Arbiter.Core.MonadArbiter (JobHandler)
import Arbiter.Core.PoolConfig (poolSize)
import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Simple
  ( SimpleDb
  , createSimpleEnv
  , createSimpleEnvWithConfig
  , destroySimpleEnv
  , runSimpleDb
  )
import Arbiter.Test.Fixtures (WorkerTestPayload (..))
import Arbiter.Test.Poll (waitUntil)
import Arbiter.Test.Setup (cleanupData, setupOnce)
import Control.Monad (forM_, void)
import Control.Monad.IO.Class (liftIO)
import Data.ByteString (ByteString)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple (close, connectPostgreSQL)
import Test.Hspec (Spec, beforeAll, describe, it, shouldBe)
import UnliftIO.Async (withAsync)

import Arbiter.Worker (namedWorkerPool, poolConfigForWorkers, runWorkerPools)
import Arbiter.Worker.BackoffStrategy (Jitter (NoJitter))
import Arbiter.Worker.Config (WorkerConfig (..), transactionalWorkerConfig)

type SizingTestRegistry = '[Queue "arbiter_worker_sizing_test" WorkerTestPayload]

testSchema :: Text
testSchema = "arbiter_worker_sizing_test"

cleanup :: ByteString -> IO ()
cleanup connStr = do
  conn <- connectPostgreSQL connStr
  cleanupData testSchema testSchema conn
  close conn

spec :: ByteString -> Spec
spec connStr =
  beforeAll (setupOnce connStr testSchema testSchema True) $
    describe "pool sizing" $
      it "sizes the pool for the workers and processes jobs" $ do
        cleanup connStr
        ref <- newIORef (0 :: Int)
        let handler :: JobHandler (SimpleDb SizingTestRegistry IO) WorkerTestPayload ()
            handler _conn _job = liftIO $ atomicModifyIORef' ref $ \n -> (n + 1, ())
        config <- transactionalWorkerConfig 3 handler
        let pools = [namedWorkerPool config {pollInterval = 0.2, jitter = NoJitter}]
        poolCfg <- poolConfigForWorkers pools
        poolSize poolCfg `shouldBe` 7 -- 2 * 3 + 1
        env <- createSimpleEnvWithConfig (Proxy @SizingTestRegistry) connStr testSchema poolCfg
        withAsync (runSimpleDb env $ runWorkerPools (Proxy @SizingTestRegistry) pools (\_ -> pure ())) $ \_ -> do
          producer <- createSimpleEnv (Proxy @SizingTestRegistry) connStr testSchema
          forM_ [1 :: Int .. 6] $ \i ->
            runSimpleDb producer $
              void $
                HL.insertJob (defaultJob (SimpleTask (T.pack ("sizing " <> show i))))
          waitUntil 10_000 $ (== 6) <$> readIORef ref
          readIORef ref >>= (`shouldBe` 6)
          destroySimpleEnv producer
        destroySimpleEnv env

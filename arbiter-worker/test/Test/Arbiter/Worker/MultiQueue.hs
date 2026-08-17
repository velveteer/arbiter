{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Arbiter.Worker.MultiQueue (spec) where

import Arbiter.Core.MonadArbiter (JobHandler, MonadArbiter (..))
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Fixtures (WorkerTestPayload)
import Control.Exception (Exception)
import Control.Monad.IO.Class (MonadIO)
import Test.Hspec (Spec, describe, it, shouldBe, shouldThrow)
import UnliftIO (MonadUnliftIO, throwIO)

import Arbiter.Worker
  ( WorkerPoolSelectionException (..)
  , WorkerState (ShuttingDown)
  , getWorkerState
  , namedWorkerPool
  , runSelectedWorkerPools
  , transactionalWorkerConfig
  )

newtype FailingDb a = FailingDb {runFailingDb :: IO a}
  deriving newtype (Applicative, Functor, Monad, MonadFail, MonadIO, MonadUnliftIO)

data PoolFailure = PoolFailure
  deriving stock (Eq, Show)
  deriving anyclass (Exception)

type FailingRegistry = '[Queue "failing_queue" WorkerTestPayload]

instance MonadArbiter FailingDb where
  type RegistryOf FailingDb = FailingRegistry
  type Handler FailingDb job result = job -> FailingDb result

  getSchema = throwIO PoolFailure
  executeQuery _ = throwIO PoolFailure
  executeQueryPrepared _ = throwIO PoolFailure
  executeStatement _ = throwIO PoolFailure
  withDbTransaction = id
  runHandlerWithConnection handler job = handler job
  getListener = pure Nothing

spec :: Spec
spec =
  describe "multi-pool lifecycle" $ do
    it "rejects selected queues without a configured pool" $ do
      let handler :: JobHandler FailingDb WorkerTestPayload ()
          handler _ = pure ()
      config <- transactionalWorkerConfig 1 handler
      runFailingDb (runSelectedWorkerPools ["missing_queue"] [namedWorkerPool config])
        `shouldThrow` (\(WorkerPoolSelectionException message) -> message == "No worker pool configured for: missing_queue")

    it "rethrows the first pool failure after winding down its peers" $ do
      let handler :: JobHandler FailingDb WorkerTestPayload ()
          handler _ = pure ()
      first <- transactionalWorkerConfig 1 handler
      peer <- transactionalWorkerConfig 1 handler
      let pools = [namedWorkerPool first, namedWorkerPool peer]
      runFailingDb (runSelectedWorkerPools ["failing_queue"] pools)
        `shouldThrow` (== PoolFailure)
      getWorkerState peer >>= (`shouldBe` ShuttingDown)

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

module Test.Arbiter.Orville.WorkerAdapters
  ( spec
  ) where

import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
  ( JobRead
  , ObservabilityHooks (..)
  , defaultJob
  , defaultObservabilityHooks
  , payload
  , primaryKey
  )
import Arbiter.Core.QueueRegistry (Queue)
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Test.Setup qualified as TestSetup
import Arbiter.Worker (runWorkerPool)
import Arbiter.Worker.Config (BatchCallbacks, WorkerConfig (..), ack, ackAll, defaultBatchedWorkerConfig)
import Control.Concurrent.MVar (newEmptyMVar, putMVar, takeMVar)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Reader (ReaderT)
import Data.ByteString (ByteString)
import Data.Foldable (toList, traverse_)
import Data.IORef (atomicModifyIORef', newIORef, readIORef)
import Data.List.NonEmpty (NonEmpty)
import Data.Text (Text)
import Orville.PostgreSQL qualified as O
import Test.Hspec (Spec, afterAll, beforeAll, it, shouldBe, shouldMatchList)
import UnliftIO.Exception (Exception, throwIO, try)

import Arbiter.Orville.Worker (orvilleBatchedHandler, orvilleHooks)
import Test.Arbiter.Orville.TestHelpers
  ( OrvilleTestEnv
  , TestOrville
  , cleanupOrvilleTest
  , createOrvilleTestEnv
  , destroyOrvilleTestEnv
  , runOrvilleTest
  )
import Test.Arbiter.Orville.Worker (OrvilleWorkerTestPayload (..))

type AdapterRegistry = '[Queue "arbiter_orville_adapter_test" OrvilleWorkerTestPayload]

type Base = ReaderT O.OrvilleState IO

data RollbackAck = RollbackAck
  deriving stock (Show)
  deriving anyclass (Exception)

adapterSchema :: Text
adapterSchema = "arbiter_orville_adapter_test"

spec :: ByteString -> Spec
spec connStr =
  beforeAll
    ( TestSetup.setupOnce connStr adapterSchema adapterSchema True
        >> createOrvilleTestEnv connStr adapterSchema adapterSchema 10
    ) $
    afterAll destroyOrvilleTestEnv $ do
      it "runs a handler and hooks written in the base monad" $ \env -> do
        cleanupOrvilleTest env
        successRef <- newIORef []

        let
          handler :: NonEmpty (JobRead OrvilleWorkerTestPayload) -> BatchCallbacks Base OrvilleWorkerTestPayload () -> Base ()
          handler jobs callbacks = ackAll callbacks (toList jobs)

          hooks :: ObservabilityHooks Base OrvilleWorkerTestPayload
          hooks =
            defaultObservabilityHooks
              { onJobSuccess = \job _ _ -> liftIO $ atomicModifyIORef' successRef $ \seen -> (payload job : seen, ())
              }

        void . run env $ HL.insertJobsBatch [defaultJob (SimpleTask "a"), defaultJob (SimpleTask "b")]
        config <- defaultBatchedWorkerConfig 1 10 (orvilleBatchedHandler handler)

        withLinkedAsync (run env $ runWorkerPool config {pollInterval = 0.05, observabilityHooks = orvilleHooks hooks}) $ \_ -> do
          waitUntil 10_000 $ (== 2) . length <$> readIORef successRef
          readIORef successRef >>= (`shouldMatchList` [SimpleTask "a", SimpleTask "b"])
          remaining <- run env (HL.countJobs @OrvilleWorkerTestPayload)
          remaining `shouldBe` 0

      it "runs a callback inside the handler's transaction" $ \env -> do
        cleanupOrvilleTest env
        settled <- newEmptyMVar

        let
          handler :: NonEmpty (JobRead OrvilleWorkerTestPayload) -> BatchCallbacks Base OrvilleWorkerTestPayload () -> Base ()
          handler jobs callbacks =
            flip traverse_ jobs $ \job -> do
              outcome <- try (O.withTransaction (ack callbacks job *> throwIO RollbackAck))
              liftIO $ putMVar settled (primaryKey job, either (\RollbackAck -> True) (const False) outcome)

        void . run env $ HL.insertJobsBatch [defaultJob (SimpleTask "rolled-back")]
        config <- defaultBatchedWorkerConfig 1 10 (orvilleBatchedHandler handler)

        withLinkedAsync (run env $ runWorkerPool config {pollInterval = 0.05}) $ \_ -> do
          (jobId, rolledBack) <- takeMVar settled
          rolledBack `shouldBe` True
          stillQueued <- run env (HL.jobExists @OrvilleWorkerTestPayload jobId)
          stillQueued `shouldBe` True
  where
    run :: OrvilleTestEnv AdapterRegistry -> TestOrville AdapterRegistry a -> IO a
    run = runOrvilleTest

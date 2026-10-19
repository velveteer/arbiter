{-# LANGUAGE OverloadedStrings #-}

-- | Spawn pool threads that survive transient failures.
module Arbiter.Worker.Retry
  ( spawnRetried
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Threads (labelArbiterThread)
import Control.Monad (unless)
import Control.Monad.Trans.Cont (ContT (..))
import Data.Text (Text)
import UnliftIO (MonadUnliftIO, liftIO, tryAny)
import UnliftIO.Async (Async, race, withAsync)
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.STM (TVar, atomically, checkSTM)

import Arbiter.Worker.Logger (LogConfig, LogLevel (..), tryLog)
import Arbiter.Worker.WorkerState (WorkerState, isShuttingDown)

-- | Wait between attempts of a retried infrastructure thread.
retryBackoffMicros :: Int
retryBackoffMicros = 5_000_000

-- | Spawn a thread that logs and retries transient failures until the pool shuts down.
spawnRetried
  :: (MonadUnliftIO m)
  => TVar WorkerState
  -> LogConfig
  -> Text
  -- ^ The queue this thread serves, for its RTS label.
  -> Text
  -- ^ Label for log messages, and the role in its RTS label.
  -> m ()
  -- ^ Action to run.
  -> ContT r m (Async ())
spawnRetried stateVar logCfg queue label action =
  ContT . withAsync $ do
    labelArbiterThread label (Just queue)
    loop
  where
    loop = tryAny action >>= either onFailure pure
    onFailure exception = do
      stopping <- atomically (isShuttingDown stateVar)
      unless stopping $ do
        tryLog logCfg Error $ label <> " error (retrying): " <> displayEx exception
        -- Shutdown wins the race.
        race awaitShutdown (liftIO (threadDelay retryBackoffMicros))
          >>= either pure (const loop)
    awaitShutdown = atomically (isShuttingDown stateVar >>= checkSTM)

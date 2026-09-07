{-# LANGUAGE OverloadedStrings #-}

-- | Retry combinators for worker infrastructure threads (notification listener,
-- cron scheduler, etc.) that should survive transient database failures.
module Arbiter.Worker.Retry
  ( retryOnException
  , spawnRetried
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Threads (labelArbiterThread)
import Control.Monad (unless)
import Control.Monad.Trans.Cont (ContT (..))
import Data.Text qualified as T
import UnliftIO (MonadUnliftIO, liftIO)
import UnliftIO.Async (Async, race, withAsync)
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.Exception (tryAny)
import UnliftIO.STM (TVar, atomically, readTVar, readTVarIO, retrySTM)

import Arbiter.Worker.Logger (LogConfig, LogLevel (..), tryLog)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | Run an action in a retry loop that survives transient failures, logging each one
-- and waiting five seconds before the next attempt. Exits cleanly once the pool is
-- shutting down.
retryOnException
  :: (MonadUnliftIO m)
  => TVar WorkerState
  -> LogConfig
  -> T.Text
  -- ^ Label for log messages (e.g. "Notification listener")
  -> m ()
  -- ^ Action to run
  -> m ()
retryOnException stateVar logCfg label action = loop
  where
    loop = tryAny action >>= either onFailure pure
    onFailure exception = do
      stopping <- (== ShuttingDown) <$> readTVarIO stateVar
      unless stopping $ do
        tryLog logCfg Error $ label <> " error (retrying): " <> displayEx exception
        -- Shutdown wins the race.
        race awaitShutdown (liftIO (threadDelay retryBackoffMicros))
          >>= either pure (const loop)
    awaitShutdown = liftIO . atomically $ do
      state <- readTVar stateVar
      unless (state == ShuttingDown) retrySTM

-- | Wait between attempts of a retried infrastructure thread.
retryBackoffMicros :: Int
retryBackoffMicros = 5_000_000

-- | Spawn a thread under 'withAsync' and 'retryOnException'. A transient failure
-- restarts the action.
spawnRetried
  :: (MonadUnliftIO m)
  => TVar WorkerState
  -> LogConfig
  -> T.Text
  -- ^ The queue this thread serves, for its RTS label.
  -> T.Text
  -- ^ Label for log messages, and the role in its RTS label.
  -> m ()
  -- ^ Action to run.
  -> ContT r m (Async ())
spawnRetried stateVar logCfg queue label action =
  ContT . withAsync $ do
    labelArbiterThread label (Just queue)
    retryOnException stateVar logCfg label action

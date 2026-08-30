{-# LANGUAGE OverloadedStrings #-}

-- | Retry combinators for worker infrastructure threads (notification listener,
-- cron scheduler, etc.) that should survive transient database failures.
module Arbiter.Worker.Retry
  ( isJobSignal
  , retryOnException
  , spawnRetried
  ) where

import Arbiter.Core.Exceptions (JobException, JobGoneException, displayEx)
import Arbiter.Core.Threads (labelArbiterThread)
import Control.Monad.Trans.Cont (ContT (..))
import Data.Maybe (isJust)
import Data.Text qualified as T
import UnliftIO (MonadUnliftIO, SomeException, fromException, liftIO)
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
    loop = do
      result <- tryAny action
      case result of
        Right () -> pure ()
        Left e -> do
          status <- readTVarIO stateVar
          case status of
            ShuttingDown -> pure ()
            _ -> do
              tryLog logCfg Error $
                label <> " error (retrying): " <> displayEx e
              sleepResult <-
                race
                  ( liftIO . atomically $
                      readTVar stateVar >>= \st ->
                        case st of
                          ShuttingDown -> pure ()
                          _ -> retrySTM
                  )
                  (liftIO $ threadDelay 5_000_000)
              case sleepResult of
                Left () -> pure ()
                Right () -> loop

-- | A signal only the worker layer can act on, so a retry loop rethrows it.
isJobSignal :: SomeException -> Bool
isJobSignal e =
  isJust (fromException e :: Maybe JobException)
    || isJust (fromException e :: Maybe JobGoneException)

-- | Spawn a thread under 'withAsync' and 'retryOnException', so a transient failure
-- restarts the action rather than killing the thread.
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

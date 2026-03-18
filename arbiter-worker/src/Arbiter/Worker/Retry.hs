{-# LANGUAGE OverloadedStrings #-}

-- | Retry combinator for worker infrastructure threads (notification listener,
-- cron scheduler, etc.) that should survive transient database failures.
module Arbiter.Worker.Retry
  ( retryOnException
  ) where

import Control.Monad (void)
import Data.Text qualified as T
import UnliftIO (MonadUnliftIO, liftIO)
import UnliftIO.Async (race)
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.Exception (tryAny)
import UnliftIO.STM (TVar, atomically, readTVar, readTVarIO, retrySTM)

import Arbiter.Worker.Logger (LogConfig, LogLevel (..))
import Arbiter.Worker.Logger.Internal (logMessage)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | Run an action in a retry loop, surviving transient failures.
--
-- On synchronous exceptions, checks the worker state — if 'ShuttingDown',
-- exits cleanly; otherwise logs the error and retries after a 5-second delay.
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
              void . tryAny . liftIO $
                logMessage logCfg Error $
                  label <> " error (retrying): " <> T.pack (show e)
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

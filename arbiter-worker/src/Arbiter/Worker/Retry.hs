{-# LANGUAGE OverloadedStrings #-}

-- | Retry combinators for worker infrastructure threads (notification listener,
-- cron scheduler, etc.) that should survive transient database failures.
module Arbiter.Worker.Retry
  ( retryOnException
  , retryOnExceptionForever
  , spawnRetried
  ) where

import Arbiter.Core.Exceptions
  ( JobException
  , JobNotFoundException
  , JobStolenException
  )
import Control.Monad (forever)
import Control.Monad.Trans.Cont (ContT (..))
import Data.Maybe (isJust)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import UnliftIO (MonadUnliftIO, SomeException, fromException, liftIO, throwIO)
import UnliftIO.Async (Async, race, withAsync)
import UnliftIO.Concurrent (threadDelay)
import UnliftIO.Exception (tryAny)
import UnliftIO.STM (TVar, atomically, readTVar, readTVarIO, retrySTM)

import Arbiter.Worker.Logger (LogConfig, LogLevel (..))
import Arbiter.Worker.Logger.Internal (tryLog)
import Arbiter.Worker.WorkerState (WorkerState (..))

-- | Run an action in a retry loop, surviving transient failures.
--
-- On synchronous exceptions, checks the worker state - if 'ShuttingDown',
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
              tryLog logCfg Error $
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

-- | Like 'retryOnException' but never returns on its own, even if the worker
-- is shutting down. Job signals propagate so they reach the worker layer
-- where they have semantic meaning ('JobException' user decisions,
-- 'JobStolenException' and 'JobNotFoundException' reclaim signals).
-- Everything else (including transient DB errors) is retried.
retryOnExceptionForever
  :: (MonadUnliftIO m)
  => LogConfig
  -> T.Text
  -- ^ Label for log messages
  -> NominalDiffTime
  -- ^ Delay between retries on transient failure
  -> m a
  -- ^ Action to run (typically itself a forever loop)
  -> m b
retryOnExceptionForever logCfg label delay action = forever $ do
  result <- tryAny action
  case result of
    Right _ -> pure ()
    Left e
      | isJobSignal e -> throwIO e
      | otherwise -> do
          tryLog logCfg Error $
            label <> " error (retrying): " <> T.pack (show e)
          liftIO $ threadDelay (ceiling (delay * 1_000_000))

isJobSignal :: SomeException -> Bool
isJobSignal e =
  isJust (fromException e :: Maybe JobException)
    || isJust (fromException e :: Maybe JobStolenException)
    || isJust (fromException e :: Maybe JobNotFoundException)

-- | Spawn a thread under 'withAsync', wrapped in 'retryOnException' so
-- transient failures restart the action instead of killing the thread.
spawnRetried
  :: (MonadUnliftIO m)
  => TVar WorkerState
  -> LogConfig
  -> T.Text
  -- ^ Label for log messages.
  -> m ()
  -- ^ Action to run.
  -> ContT r m (Async ())
spawnRetried stateVar logCfg label action =
  ContT . withAsync $ retryOnException stateVar logCfg label action

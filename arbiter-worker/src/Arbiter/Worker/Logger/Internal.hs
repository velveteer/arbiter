{-# LANGUAGE OverloadedStrings #-}

-- | Internal logging implementation for Arbiter.
--
-- This module is not part of the public API.
module Arbiter.Worker.Logger.Internal
  ( withJobContext
  , runHook
  , jobHook
  , poolLog
  , tryWarn
  , tryWarnWith
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Job.Types qualified as Job
import Control.Monad (void)
import Data.Aeson (KeyValue (..), object)
import Data.Aeson.Types (Pair)
import Data.List.NonEmpty (NonEmpty (..), nonEmpty, toList)
import Data.Text (Text)
import UnliftIO (MonadUnliftIO, tryAny)

import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), LogLevel (..), tryLog, warnEx)

-- | Add the jobs' fields to every message a 'LogConfig' emits.
withJobContext :: LogConfig -> NonEmpty (Job.JobRead payload) -> LogConfig
withJobContext config jobs
  | loggingActive config = config {additionalContext = (buildJobContext jobs <>) <$> additionalContext config}
  | otherwise = config

-- | 'withJobContext' scoped to a single job.
withJobContextOne :: LogConfig -> Job.JobRead payload -> LogConfig
withJobContextOne config job = withJobContext config (job :| [])

-- | 'withJobContext' scoped to a job list. No context when the list is empty.
withJobContextList :: LogConfig -> [Job.JobRead payload] -> LogConfig
withJobContextList config = maybe config (withJobContext config) . nonEmpty

-- | True when some destination emits messages.
loggingActive :: LogConfig -> Bool
loggingActive = destinationActive . logDestination

destinationActive :: LogDestination -> Bool
destinationActive = \case
  LogDiscard -> False
  LogTee first second -> destinationActive first || destinationActive second
  _ -> True

-- | Build structured context for a batch of jobs.
buildJobContext :: NonEmpty (Job.JobRead payload) -> [Pair]
buildJobContext jobs = ["jobs" .= map (object . mkContext) (toList jobs)]
  where
    mkContext :: Job.JobRead payload -> [Pair]
    mkContext job =
      [ "job_id" .= Job.primaryKey job
      , "job_attempts" .= Job.attempts job
      , "job_group_key" .= Job.groupKey job
      , "job_queue" .= Job.queueName job
      ]

-- | Run an observability hook, catching and logging any exceptions.
runHook
  :: (MonadUnliftIO m)
  => LogConfig
  -> Text
  -- ^ Hook name (for logging)
  -> m ()
  -- ^ Hook action
  -> m ()
runHook cfg hookName action =
  tryAny action
    >>= either
      (\exception -> tryLog cfg Warning $ "Observability hook '" <> hookName <> "' failed: " <> displayEx exception)
      pure

-- | Run an observability hook with the job in the log context.
jobHook :: (MonadUnliftIO m) => LogConfig -> Job.JobRead payload -> Text -> m () -> m ()
jobHook cfg = runHook . withJobContextOne cfg

-- | Log with the jobs in context.
poolLog :: LogConfig -> LogLevel -> [Job.JobRead payload] -> Text -> IO ()
poolLog cfg level jobs = tryLog (withJobContextList cfg jobs) level

-- | Run an action and log a warning if it fails.
tryWarn :: (MonadUnliftIO m) => LogConfig -> Text -> m a -> m ()
tryWarn logCfg label act = tryWarnWith logCfg label () (void act)

-- | Run an action and return a fallback value if it fails.
tryWarnWith :: (MonadUnliftIO m) => LogConfig -> Text -> a -> m a -> m a
tryWarnWith logCfg label fallback act =
  tryAny act >>= either (\exception -> fallback <$ warnEx logCfg label exception) pure

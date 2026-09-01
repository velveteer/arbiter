{-# LANGUAGE OverloadedStrings #-}

-- | Internal logging implementation for Arbiter.
--
-- This module is not part of the public API.
module Arbiter.Worker.Logger.Internal
  ( withJobContext
  , withJobContextOne
  , withJobContextList
  , runHook
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Job.Types qualified as Job
import Data.Aeson (KeyValue (..), object)
import Data.Aeson.Types (Pair)
import Data.List.NonEmpty (NonEmpty (..), nonEmpty, toList)
import Data.Text (Text)
import UnliftIO (MonadUnliftIO, tryAny)

import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), LogLevel (..), tryLog)

-- | Augment a 'LogConfig' so every message it emits carries the jobs' fields.
-- The context is folded into 'additionalContext', which is evaluated only when a
-- message is actually emitted, so the happy path pays nothing.
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

-- | Building job context is wasted work when logs are discarded.
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
    >>= either (\e -> tryLog cfg Warning $ "Observability hook '" <> hookName <> "' failed: " <> displayEx e) pure

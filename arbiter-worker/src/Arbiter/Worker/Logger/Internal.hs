{-# LANGUAGE OverloadedStrings #-}

-- | Internal logging implementation for Arbiter.
--
-- This module is not part of the public API.
module Arbiter.Worker.Logger.Internal
  ( logMessage
  , tryLog
  , warnEx
  , withJobContext
  , withJobContextOne
  , withJobContextList
  , runHook
  ) where

import Arbiter.Core.Job.Types qualified as Job
import Control.Exception (displayException, finally)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger qualified as ML
import Control.Monad.Logger.Aeson qualified as MLA
import Data.Aeson (KeyValue (..), object)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Pair)
import Data.List.NonEmpty (NonEmpty (..), nonEmpty, toList)
import Data.Text (Text)
import Data.Text qualified as T
import UnliftIO (MonadUnliftIO, SomeException, tryAny)

import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), LogLevel (..))

-- | Log a message using the given config.
logMessage :: LogConfig -> LogLevel -> Text -> IO ()
logMessage config level msg = when (level >= minLogLevel config) $ do
  extraCtx <- additionalContext config
  runWithDestination (logDestination config) extraCtx level msg

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
loggingActive config = case logDestination config of
  LogDiscard -> False
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

-- | Run the logger with the appropriate destination and context.
runWithDestination :: LogDestination -> [Pair] -> LogLevel -> Text -> IO ()
runWithDestination dest ctx level msg = case dest of
  LogStdout -> MLA.runStdoutLoggingT $ MLA.withThreadContext ctx $ logAt level msg
  LogStderr -> MLA.runStderrLoggingT $ MLA.withThreadContext ctx $ logAt level msg
  LogFastLogger loggerSet -> MLA.runFastLoggingT loggerSet $ MLA.withThreadContext ctx $ logAt level msg
  LogCallback cb -> do
    threadCtx <- KM.toList <$> MLA.myThreadContext
    cb level msg (threadCtx <> ctx)
  LogTee base extra ->
    runWithDestination base ctx level msg `finally` runWithDestination extra ctx level msg
  LogDiscard -> pure ()
  where
    logAt :: (ML.MonadLogger m) => LogLevel -> Text -> m ()
    logAt Debug = ML.logDebugN
    logAt Info = ML.logInfoN
    logAt Warning = ML.logWarnN
    logAt Error = ML.logErrorN

-- | Log a message, swallowing any exceptions from the logging infrastructure.
tryLog :: (MonadUnliftIO m) => LogConfig -> LogLevel -> Text -> m ()
tryLog cfg level msg = void . tryAny . liftIO $ logMessage cfg level msg

-- | 'tryLog' an exception at 'Warning' under @label@.
warnEx :: (MonadUnliftIO m) => LogConfig -> Text -> SomeException -> m ()
warnEx logCfg label e = tryLog logCfg Warning $ label <> ": " <> T.pack (displayException e)

-- | Run an observability hook, catching and logging any exceptions.
runHook
  :: (MonadUnliftIO m)
  => LogConfig
  -> Text
  -- ^ Hook name (for logging)
  -> m ()
  -- ^ Hook action
  -> m ()
runHook cfg hookName action = do
  result <- tryAny action
  case result of
    Left e -> tryLog cfg Warning $ "Observability hook '" <> hookName <> "' failed: " <> T.pack (displayException e)
    Right _ -> pure ()

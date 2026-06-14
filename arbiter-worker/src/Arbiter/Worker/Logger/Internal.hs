{-# LANGUAGE OverloadedStrings #-}

-- | Internal logging implementation for Arbiter.
--
-- This module is not part of the public API.
module Arbiter.Worker.Logger.Internal
  ( logMessage
  , tryLog
  , withJobContext
  , withJobContextOne
  , showJobIds
  , runHook
  ) where

import Arbiter.Core.Job.Types qualified as Job
import Control.Monad (void, when)
import Control.Monad.Catch (MonadMask)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Logger qualified as ML
import Control.Monad.Logger.Aeson qualified as MLA
import Data.Aeson (KeyValue (..), object)
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Pair)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..), toList)
import Data.Text (Text)
import Data.Text qualified as T
import UnliftIO (MonadUnliftIO, tryAny)

import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), LogLevel (..))

-- | Log a message using the given config.
logMessage :: LogConfig -> LogLevel -> Text -> IO ()
logMessage config level msg = when (level >= minLogLevel config) $ do
  extraCtx <- additionalContext config
  runWithDestination (logDestination config) extraCtx level msg

-- | Run an action with job context attached to the thread.
--
-- This sets up thread-local context that will be included in all log messages
-- within the action.
withJobContext
  :: (MonadIO m, MonadMask m)
  => NonEmpty (Job.JobRead payload)
  -> m a
  -> m a
withJobContext jobs = MLA.withThreadContext (buildJobContext jobs)

-- | 'withJobContext' for a single job (per-job callbacks and hooks).
withJobContextOne :: (MonadIO m, MonadMask m) => Job.JobRead payload -> m a -> m a
withJobContextOne job = withJobContext (job :| [])

-- | Comma-separated job ids for log messages.
showJobIds :: [Int64] -> Text
showJobIds = T.intercalate ", " . map (T.pack . show)

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
    Left e -> tryLog cfg Warning $ "Observability hook '" <> hookName <> "' failed: " <> T.pack (show e)
    Right _ -> pure ()

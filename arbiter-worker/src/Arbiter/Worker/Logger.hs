{-# LANGUAGE OverloadedStrings #-}

-- | The worker's own structured JSON logging: its level, its destination, and the
-- context every message carries. Application-level job logging belongs on
-- 'Arbiter.Core.Job.Types.ObservabilityHooks' instead.
module Arbiter.Worker.Logger
  ( -- * Log Configuration
    LogConfig (..)
  , LogDestination (..)
  , defaultLogConfig
  , silentLogConfig

    -- * Log Levels
  , LogLevel (..)

    -- * Emitting
  , tryLog
  , warnEx

    -- * Re-exports for structured context
  , Pair
  , (.=)
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Control.Exception (finally)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger qualified as ML
import Control.Monad.Logger.Aeson ((.=))
import Control.Monad.Logger.Aeson qualified as MLA
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Pair)
import Data.Text (Text)
import System.Log.FastLogger (LoggerSet)
import UnliftIO (MonadUnliftIO, SomeException, tryAny)

-- | Log severity levels.
data LogLevel
  = Debug
  | Info
  | Warning
  | Error
  deriving stock (Bounded, Enum, Eq, Ord, Read, Show)

-- | Where Arbiter writes log output.
data LogDestination
  = -- | Log to stdout (default)
    LogStdout
  | -- | Log to stderr
    LogStderr
  | -- | Log to a custom fast-logger 'LoggerSet'
    LogFastLogger LoggerSet
  | -- | Log to a user-provided callback. The callback receives the 'LogLevel',
    -- the plain message 'Text', and all structured context as @['Pair']@
    -- such as job information and additional context. Use this callback to
    -- send Arbiter logs to an application logging system.
    --
    -- @
    -- let cb level msg ctx = myLogger level msg ctx
    -- in defaultLogConfig { logDestination = LogCallback cb }
    -- @
    LogCallback (LogLevel -> Text -> [Pair] -> IO ())
  | -- | Emit every message to both destinations, the first one before the second.
    LogTee LogDestination LogDestination
  | -- | Discard all logs (silent mode)
    LogDiscard

-- | How the worker emits its own logs.
data LogConfig = LogConfig
  { minLogLevel :: LogLevel
  -- ^ Minimum severity to emit. Messages below this level are dropped.
  -- Default: 'Info'.
  , logDestination :: LogDestination
  -- ^ Where to write logs. Default: 'LogStdout'.
  , additionalContext :: IO [Pair]
  -- ^ Context merged into every message, read at log time so it can pick up
  -- thread-local state such as an ambient trace id. Default: @pure []@.
  }

-- | Default log configuration: Info level to stdout, no additional context.
defaultLogConfig :: LogConfig
defaultLogConfig =
  LogConfig
    { minLogLevel = Info
    , logDestination = LogStdout
    , additionalContext = pure []
    }

-- | Silent log configuration: discards all logs.
silentLogConfig :: LogConfig
silentLogConfig = defaultLogConfig {logDestination = LogDiscard}

-- | Log a message, swallowing any exceptions from the logging infrastructure.
tryLog :: (MonadUnliftIO m) => LogConfig -> LogLevel -> Text -> m ()
tryLog cfg level msg = void . tryAny . liftIO $ logMessage cfg level msg

-- | 'tryLog' an exception at 'Warning' under @label@.
warnEx :: (MonadUnliftIO m) => LogConfig -> Text -> SomeException -> m ()
warnEx logCfg label e = tryLog logCfg Warning $ label <> ": " <> displayEx e

-- | Log a message using the given config.
logMessage :: LogConfig -> LogLevel -> Text -> IO ()
logMessage config level msg = when (level >= minLogLevel config) $ do
  extraCtx <- additionalContext config
  runWithDestination (logDestination config) extraCtx level msg

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

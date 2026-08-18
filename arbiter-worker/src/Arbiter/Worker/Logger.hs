{-# LANGUAGE OverloadedStrings #-}

-- | Logging for the Arbiter worker.
--
-- Arbiter handles its own structured JSON logging internally. Users can:
--
-- * Control the minimum log level
-- * Choose the output destination (stdout, stderr, custom LoggerSet, or callback)
-- * Provide a 'LogCallback' to receive pre-rendered JSON log lines
-- * Inject additional context (e.g., trace IDs) into every log message
--
-- For application-level job logging, use 'Arbiter.Core.Job.Types.ObservabilityHooks' instead.
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

import Control.Exception (displayException, finally)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Logger qualified as ML
import Control.Monad.Logger.Aeson ((.=))
import Control.Monad.Logger.Aeson qualified as MLA
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Pair)
import Data.Text (Text)
import Data.Text qualified as T
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
    -- (job info, additional context, etc.). This lets you integrate Arbiter's
    -- logs into your own structured logging stack.
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

-- | Configuration for Arbiter's internal logging.
--
-- Arbiter always outputs structured JSON logs. This config controls filtering,
-- destination, and allows injecting additional context.
data LogConfig = LogConfig
  { minLogLevel :: LogLevel
  -- ^ Minimum severity to emit. Messages below this level are dropped.
  -- Default: 'Info'.
  , logDestination :: LogDestination
  -- ^ Where to write logs. Default: 'LogStdout'.
  , additionalContext :: IO [Pair]
  -- ^ Additional context merged into every log message. This IO action is
  -- called at log time, allowing you to read thread-local state (e.g., trace
  -- IDs from an OpenTelemetry context). Default: @pure []@.
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
warnEx logCfg label e = tryLog logCfg Warning $ label <> ": " <> T.pack (displayException e)

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

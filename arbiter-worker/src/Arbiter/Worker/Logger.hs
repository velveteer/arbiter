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
  , recoveryLevel
  , hubLogFor
  , sharedHubLogFor

    -- * Repeat suppression
  , FailureGate
  , newFailureGate
  , reportOutcome
  , tryReported
  , FailureGates
  , newFailureGates
  , tryReportedOn

    -- * Re-exports for structured context
  , Pair
  , (.=)
  ) where

import Arbiter.Core.Exceptions (displayEx)
import Arbiter.Core.Listen (HubLog (..))
import Control.Exception (finally)
import Control.Monad (void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Logger qualified as ML
import Control.Monad.Logger.Aeson ((.=))
import Control.Monad.Logger.Aeson qualified as MLA
import Data.Aeson.KeyMap qualified as KM
import Data.Aeson.Types (Pair)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import GHC.Clock (getMonotonicTime)
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
  , identityContext :: [Pair]
  -- ^ The library's pool and worker pairs. 'additionalContext' wins on a
  -- collision. Default: @[]@.
  , failureRepeatInterval :: NominalDiffTime
  -- ^ How often a standing failure says so again, so a monitor watching the error
  -- rate does not see an ongoing outage clear itself. Default: 60s.
  }

-- | Gap between repeats of a standing failure.
defaultFailureRepeatInterval :: NominalDiffTime
defaultFailureRepeatInterval = 60

-- | Default log configuration: Info level to stdout, no additional context.
defaultLogConfig :: LogConfig
defaultLogConfig =
  LogConfig
    { minLogLevel = Info
    , logDestination = LogStdout
    , additionalContext = pure []
    , identityContext = []
    , failureRepeatInterval = defaultFailureRepeatInterval
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

-- | The failure a repeating action reported last and when, absent while healthy.
newtype FailureGate = FailureGate (IORef (Maybe (Text, Double)))

-- | A gate for one repeating action, starting healthy.
newFailureGate :: (MonadIO m) => m FailureGate
newFailureGate = FailureGate <$> liftIO (newIORef Nothing)

-- | Log an attempt only when it changes the gate, so an outage logs one line
-- instead of one per attempt, then says so again every 'failureRepeatInterval'.
-- A failure that reads differently from the one the gate holds is a new outage
-- and logs again. @subject@ reads with both failed and recovered, and a log that
-- shows the failure always shows the recovery.
reportOutcome
  :: (MonadUnliftIO m)
  => LogConfig
  -> LogLevel
  -> FailureGate
  -> Text
  -> Either SomeException a
  -> m ()
reportOutcome cfg level (FailureGate ref) subject result = do
  reported <- liftIO (readIORef ref)
  now <- liftIO getMonotonicTime
  case result of
    Left e
      | let failure = displayEx e
      , worthLogging failure now reported ->
          liftIO (writeIORef ref (Just (failure, now)))
            *> tryLog cfg level (subject <> " failed: " <> failure)
    Right _
      | Just _ <- reported ->
          liftIO (writeIORef ref Nothing)
            *> tryLog cfg (recoveryLevel cfg level) (subject <> " recovered")
    _ -> pure ()
  where
    worthLogging failure now =
      maybe True (\(held, at) -> held /= failure || now - at >= repeatAfter)
    repeatAfter = realToFrac (failureRepeatInterval cfg)

-- | Hub loggers over @cfg@, for events that belong to the registrant.
hubLogFor :: LogConfig -> HubLog
hubLogFor cfg =
  HubLog
    { hubInfo = tryLog cfg (recoveryLevel cfg Error)
    , hubWarn = tryLog cfg Warning
    , hubError = tryLog cfg Error
    }

-- | 'hubLogFor' without the 'identityContext', for events that belong to the hub.
sharedHubLogFor :: LogConfig -> HubLog
sharedHubLogFor cfg = hubLogFor cfg {identityContext = []}

-- | The gentlest level a recovery can take and still reach a log that showed the
-- failure. Never louder than the failure itself.
recoveryLevel :: LogConfig -> LogLevel -> LogLevel
recoveryLevel cfg level = min level (max Info (minLogLevel cfg))

-- | Gates addressed by subject, for a caller whose failure sites are known only
-- at runtime. Every subject holds its gate for the life of the store, so draw
-- them from a bounded set.
newtype FailureGates = FailureGates (IORef (Map Text FailureGate))

-- | An empty gate store.
newFailureGates :: (MonadIO m) => m FailureGates
newFailureGates = FailureGates <$> liftIO (newIORef Map.empty)

-- | 'tryReported' against the gate @subject@ names, created on first use.
tryReportedOn
  :: (MonadUnliftIO m)
  => LogConfig
  -> LogLevel
  -> FailureGates
  -> Text
  -> m a
  -> m (Either SomeException a)
tryReportedOn cfg level gates subject action = do
  gate <- gateFor gates subject
  tryReported cfg level gate subject action

-- | The gate @subject@ names, adding one when it is new.
gateFor :: (MonadIO m) => FailureGates -> Text -> m FailureGate
gateFor (FailureGates ref) subject =
  liftIO (Map.lookup subject <$> readIORef ref) >>= maybe add pure
  where
    add = do
      gate <- newFailureGate
      liftIO . atomicModifyIORef' ref $ \gates ->
        maybe (Map.insert subject gate gates, gate) ((,) gates) (Map.lookup subject gates)

-- | 'reportOutcome' over a run of @action@.
tryReported
  :: (MonadUnliftIO m)
  => LogConfig
  -> LogLevel
  -> FailureGate
  -> Text
  -> m a
  -> m (Either SomeException a)
tryReported cfg level gate subject action = do
  result <- tryAny action
  result <$ reportOutcome cfg level gate subject result

-- | Log a message using the given config.
logMessage :: LogConfig -> LogLevel -> Text -> IO ()
logMessage config level msg = when (level >= minLogLevel config) $ do
  extraCtx <- additionalContext config
  runWithDestination (logDestination config) (identityContext config <> extraCtx) level msg

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

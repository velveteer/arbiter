-- | Logging for the Arbiter worker.
--
-- Arbiter handles its own structured JSON logging internally. Users can:
--
-- * Control the minimum log level
-- * Choose the output destination (stdout, stderr, custom LoggerSet, or callback)
-- * Provide a 'LogCallback' to receive pre-rendered JSON log lines
-- * Inject additional context (e.g., trace IDs) into every log message
--
-- For application-level job logging, use 'ObservabilityHooks' instead.
module Arbiter.Worker.Logger
  ( -- * Log Configuration
    LogConfig (..)
  , LogDestination (..)
  , defaultLogConfig
  , silentLogConfig

    -- * Log Levels
  , LogLevel (..)

    -- * Re-exports for structured context
  , Pair
  , (.=)
  ) where

import Control.Monad.Logger.Aeson ((.=))
import Data.Aeson.Types (Pair)
import Data.Text (Text)
import System.Log.FastLogger (LoggerSet)

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

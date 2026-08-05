{-# LANGUAGE LambdaCase #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Arbiter's logs as OTel log records.
module Arbiter.Otel.Logs
  ( loggerDestination
  , otelLogs
  ) where

import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), LogLevel (..), Pair)
import Control.Monad (void)
import Data.Aeson (Value (..))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KM
import Data.Foldable (toList)
import Data.HashMap.Strict qualified as HM
import Data.Scientific (toBoundedInteger, toRealFloat)
import Data.Text (Text)
import OpenTelemetry.Log.Core qualified as Log

-- | Send a config's logs to @dest@ as well as its own. 'Nothing' leaves the
-- destination alone. A config that discards its own logs still exports them.
otelLogs :: Maybe LogDestination -> LogConfig -> LogConfig
otelLogs dest cfg = maybe cfg (\d -> cfg {logDestination = teed d}) dest
  where
    teed d = case logDestination cfg of
      LogDiscard -> d
      existing -> LogTee existing d

-- | Route Arbiter's logs to @provider@ as OTel log records.
loggerDestination :: Log.LoggerProvider -> LogDestination
loggerDestination provider = LogCallback (otelLogCallback (Log.makeLogger provider "arbiter"))

-- | One Arbiter log line as an OTel log record, with its structured context as attributes.
otelLogCallback :: Log.Logger -> LogLevel -> Text -> [Pair] -> IO ()
otelLogCallback logger level msg context =
  void . Log.emitLogRecord logger $
    Log.LogRecordArguments
      { Log.timestamp = Nothing
      , Log.observedTimestamp = Nothing
      , Log.context = Nothing
      , Log.severityText = Just (severityLabel level)
      , Log.severityNumber = Just (severityOf level)
      , Log.body = Log.TextValue msg
      , Log.attributes = HM.fromList (map logAttribute context)
      , Log.eventName = Nothing
      }

severityOf :: LogLevel -> Log.SeverityNumber
severityOf = \case
  Debug -> Log.Debug
  Info -> Log.Info
  Warning -> Log.Warn
  Error -> Log.Error

severityLabel :: LogLevel -> Text
severityLabel = \case
  Debug -> "Debug"
  Info -> "Info"
  Warning -> "Warning"
  Error -> "Error"

logAttribute :: Pair -> (Text, Log.AnyValue)
logAttribute (key, value) = (Key.toText key, anyValue value)

anyValue :: Value -> Log.AnyValue
anyValue = \case
  String t -> Log.TextValue t
  Bool b -> Log.BoolValue b
  -- Integral only when it fits the record's Int64, so a huge exponent stays a double.
  Number n -> maybe (Log.DoubleValue (toRealFloat n)) Log.IntValue (toBoundedInteger n)
  Array a -> Log.ArrayValue (map anyValue (toList a))
  Object o -> Log.HashMapValue (HM.fromList (map logAttribute (KM.toList o)))
  Null -> Log.NullValue

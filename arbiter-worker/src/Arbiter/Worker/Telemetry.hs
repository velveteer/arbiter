{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Worker-lifecycle OpenTelemetry instruments, bound through 'ObservabilityHooks',
-- and Arbiter's logs as OTel log records.
module Arbiter.Worker.Telemetry
  ( ArbiterMeters
  , arbiterMeter
  , newArbiterMeters
  , globalArbiterMeters
  , withOtelMetrics
  , otelHooks
  , withOtelHooks
  , attrs

    -- * Logs
  , otelLogDestination
  , withOtelLogs
  , otelLogs
  ) where

import Arbiter.Core.Job.Types (ObservabilityHooks (..), defaultObservabilityHooks)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (Value (..))
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KM
import Data.Foldable (toList)
import Data.HashMap.Strict qualified as HM
import Data.Int (Int64)
import Data.Scientific (floatingOrInteger)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock (diffUTCTime)
import OpenTelemetry.Attributes (Attributes, toAttribute, unsafeAttributesFromListIgnoringLimits)
import OpenTelemetry.Log.Core qualified as Log
import OpenTelemetry.Metric.Core
  ( AdvisoryParameters (..)
  , Counter
  , Histogram
  , Meter
  , MeterProvider
  , counterAdd
  , defaultAdvisoryParameters
  , getGlobalMeterProvider
  , getMeter
  , histogramRecord
  , meterCreateCounterInt64
  , meterCreateHistogram
  )

import Arbiter.Worker.Config (WorkerConfig (..))
import Arbiter.Worker.Logger (LogConfig (..), LogDestination (..), LogLevel (..), Pair)
import Arbiter.Worker.Logger.Internal (runWithDestination)

data ArbiterMeters = ArbiterMeters
  { claimed :: Counter Int64
  , processed :: Counter Int64
  , duration :: Histogram
  }

-- | Arbiter's instrumentation scope, shared by every meter the project registers.
arbiterMeter :: MeterProvider -> IO Meter
arbiterMeter mp = getMeter mp "arbiter"

newArbiterMeters :: MeterProvider -> IO ArbiterMeters
newArbiterMeters mp = do
  meter <- arbiterMeter mp
  ArbiterMeters
    <$> meterCreateCounterInt64 meter "arbiter.jobs.claimed" Nothing (Just "Jobs claimed by workers") defaultAdvisoryParameters
    <*> meterCreateCounterInt64
      meter
      "arbiter.jobs.processed"
      Nothing
      (Just "Jobs processed, by terminal outcome")
      defaultAdvisoryParameters
    <*> meterCreateHistogram
      meter
      "arbiter.job.handler.duration"
      (Just "s")
      (Just "Handler execution time in seconds")
      durationBuckets

-- | Second-scale bounds, the SDK default ladder being millisecond-shaped.
durationBuckets :: AdvisoryParameters
durationBuckets =
  defaultAdvisoryParameters
    { advisoryExplicitBucketBoundaries = Just [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300]
    }

-- | Arbiter's instruments on the caller's own global meter provider.
globalArbiterMeters :: IO ArbiterMeters
globalArbiterMeters = newArbiterMeters =<< getGlobalMeterProvider

-- | Record a pool's jobs into @ms@, keeping whatever hooks the pool already has.
withOtelMetrics
  :: (MonadIO m)
  => Maybe ArbiterMeters
  -> Text
  -- ^ The queue this pool serves, as the metric's @queue@ attribute.
  -> WorkerConfig m payload result
  -> WorkerConfig m payload result
withOtelMetrics ms queue cfg =
  cfg {observabilityHooks = withOtelHooks ms queue (observabilityHooks cfg)}

-- | Add @ms@'s metric hooks to @hooks@. 'Nothing' returns @hooks@ unchanged.
withOtelHooks
  :: (MonadIO m)
  => Maybe ArbiterMeters
  -> Text
  -> ObservabilityHooks m payload
  -> ObservabilityHooks m payload
withOtelHooks Nothing _ hooks = hooks
withOtelHooks (Just ms) queue hooks = otelHooks ms queue <> hooks

otelHooks :: (MonadIO m) => ArbiterMeters -> Text -> ObservabilityHooks m payload
otelHooks ms queue =
  defaultObservabilityHooks
    { onJobClaimed = \_ _ -> liftIO (counterAdd (claimed ms) 1 queueAttr)
    , onJobSuccess = \_ start end -> liftIO $ do
        counterAdd (processed ms) 1 successAttr
        histogramRecord (duration ms) (secs start end) successAttr
    , onJobFailure = \_ _ start end -> liftIO (histogramRecord (duration ms) (secs start end) failureAttr)
    , onJobRetry = \_ _ -> liftIO (counterAdd (processed ms) 1 retryAttr)
    , onJobFailedAndMovedToDLQ = \_ _ -> liftIO (counterAdd (processed ms) 1 dlqAttr)
    , onJobCancelled = \_ _ -> liftIO (counterAdd (processed ms) 1 cancelledAttr)
    }
  where
    secs start end = realToFrac (diffUTCTime end start)
    outcome o = attrs [("queue", queue), ("outcome", o)]
    queueAttr = attrs [("queue", queue)]
    successAttr = outcome "success"
    failureAttr = outcome "failure"
    retryAttr = outcome "retry"
    dlqAttr = outcome "dlq"
    cancelledAttr = outcome "cancelled"

-- | Build an OTel attribute set from text key/value pairs.
attrs :: [(Text, Text)] -> Attributes
attrs kvs = unsafeAttributesFromListIgnoringLimits [(k, toAttribute v) | (k, v) <- kvs]

-- | Send a pool's logs to @dest@, keeping the level and context its config already carries.
withOtelLogs :: Maybe LogDestination -> WorkerConfig m payload result -> WorkerConfig m payload result
withOtelLogs dest cfg = cfg {logConfig = otelLogs dest (logConfig cfg)}

-- | 'withOtelLogs' on a bare 'LogConfig'. 'Nothing' leaves the destination alone. Emitted
-- alongside the configured destination, so a collector never takes the logs off stdout.
otelLogs :: Maybe LogDestination -> LogConfig -> LogConfig
otelLogs dest cfg = maybe cfg (\d -> cfg {logDestination = alsoTo (logDestination cfg) d}) dest

alsoTo :: LogDestination -> LogDestination -> LogDestination
alsoTo base extra = LogCallback $ \level msg ctx -> do
  runWithDestination base ctx level msg
  runWithDestination extra ctx level msg

-- | Route Arbiter's logs to the global logger provider as OTel log records.
otelLogDestination :: (MonadIO m) => m LogDestination
otelLogDestination = do
  provider <- Log.getGlobalLoggerProvider
  pure (LogCallback (otelLogCallback (Log.makeLogger provider "arbiter")))

-- | One Arbiter log line as an OTel log record, with its structured context as attributes.
otelLogCallback :: Log.Logger -> LogLevel -> Text -> [Pair] -> IO ()
otelLogCallback logger level msg context =
  void . Log.emitLogRecord logger $
    Log.LogRecordArguments
      { Log.timestamp = Nothing
      , Log.observedTimestamp = Nothing
      , Log.context = Nothing
      , Log.severityText = Just (T.pack (show level))
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

logAttribute :: Pair -> (Text, Log.AnyValue)
logAttribute (key, value) = (Key.toText key, anyValue value)

anyValue :: Value -> Log.AnyValue
anyValue = \case
  String t -> Log.TextValue t
  Bool b -> Log.BoolValue b
  Number n -> either Log.DoubleValue Log.IntValue (floatingOrInteger n)
  Array a -> Log.ArrayValue (map anyValue (toList a))
  Object o -> Log.HashMapValue (HM.fromList (map logAttribute (KM.toList o)))
  Null -> Log.NullValue

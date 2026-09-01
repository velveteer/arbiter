{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Worker-lifecycle OpenTelemetry instruments, bound through 'ObservabilityHooks'.
module Arbiter.Otel.Metrics
  ( ArbiterMeters
  , arbiterMeter
  , newArbiterMeters
  , otelHooks
  , otelMaintenance
  , attrs
  , rateLimitKind
  , concurrencyKind
  ) where

import Arbiter.Core.Concurrency.Spec (ConcurrencyKey (..))
import Arbiter.Core.Job.Types (AdmissionKeys (..), ObservabilityHooks (..), admission, defaultObservabilityHooks)
import Arbiter.Core.RateLimit.Spec (RateLimitKey (..))
import Arbiter.Worker.Config (MaintenanceOp, maintenanceOpName)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Fixed (Fixed (MkFixed))
import Data.Foldable (traverse_)
import Data.Int (Int64)
import Data.Text (Text)
import Data.Time.Clock (diffUTCTime, nominalDiffTimeToSeconds)
import OpenTelemetry.Attributes (Attributes, toAttribute, unsafeAttributesFromListIgnoringLimits)
import OpenTelemetry.Metric.Core
  ( AdvisoryParameters (..)
  , Counter
  , Histogram
  , Meter
  , MeterProvider
  , counterAdd
  , defaultAdvisoryParameters
  , getMeter
  , histogramRecord
  , meterCreateCounterInt64
  , meterCreateHistogram
  )

import Arbiter.Otel.MetricNames qualified as Name

-- | The job-lifecycle instruments.
data ArbiterMeters = ArbiterMeters
  { claimed :: Counter Int64
  , processed :: Counter Int64
  , retried :: Counter Int64
  , admitted :: Counter Int64
  , maintained :: Counter Int64
  , duration :: Histogram
  }

-- | Arbiter's instrumentation scope, shared by every meter the project registers.
arbiterMeter :: MeterProvider -> IO Meter
arbiterMeter mp = getMeter mp "arbiter"

-- | Create the instruments on a meter provider.
newArbiterMeters :: MeterProvider -> IO ArbiterMeters
newArbiterMeters mp = do
  meter <- arbiterMeter mp
  let counter name desc =
        meterCreateCounterInt64 meter (Name.metricName name) Nothing (Just desc) defaultAdvisoryParameters
  ArbiterMeters
    <$> counter Name.JobsClaimed "Jobs claimed by workers"
    <*> counter Name.JobsProcessed "Jobs processed, by terminal outcome"
    <*> counter Name.JobsRetries "Failed jobs scheduled for another attempt"
    <*> counter Name.AdmissionAdmitted "Claimed jobs that passed an admission policy, by kind and policy"
    <*> counter Name.MaintenanceRows "Rows a reaper op touched, by op"
    <*> meterCreateHistogram
      meter
      (Name.metricName Name.HandlerDuration)
      (Just "s")
      (Just "Seconds a job spent in the handler (a batched pool records its batch's span for each job)")
      durationBuckets

-- | Second-scale histogram bounds, the SDK default ladder being millisecond-shaped.
durationBuckets :: AdvisoryParameters
durationBuckets =
  defaultAdvisoryParameters
    { advisoryExplicitBucketBoundaries = Just [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300]
    }

-- | Observability hooks recording one queue's jobs to the instruments.
otelHooks :: (MonadIO m) => ArbiterMeters -> Text -> ObservabilityHooks m payload
otelHooks ms queue =
  defaultObservabilityHooks
    { onJobClaimed = \job _ -> liftIO $ do
        counterAdd (claimed ms) 1 queueAttr
        traverse_ (admissionThrough rateLimitKind . rlkPrefix) (jobRateLimitKey (admission job))
        traverse_ (admissionThrough concurrencyKind . ckPrefix) (jobConcurrencyKey (admission job))
    , onJobSuccess = \_ start end -> liftIO $ do
        counterAdd (processed ms) 1 successAttr
        histogramRecord (duration ms) (secs start end) successAttr
    , onJobFailure = \_ _ start end -> liftIO (histogramRecord (duration ms) (secs start end) failureAttr)
    , onJobRetry = \_ _ -> liftIO (counterAdd (retried ms) 1 queueAttr)
    , onJobFailedAndMovedToDLQ = \_ _ -> liftIO (counterAdd (processed ms) 1 dlqAttr)
    , onJobCancelled = \_ _ -> liftIO (counterAdd (processed ms) 1 cancelledAttr)
    , onJobUnavailable = \_ _ -> liftIO (counterAdd (processed ms) 1 unavailableAttr)
    }
  where
    -- A clock stepped backwards must not subtract from the histogram's sum.
    secs start end = case nominalDiffTimeToSeconds (diffUTCTime end start) of
      MkFixed ps -> max 0 (fromInteger ps / 1e12)
    outcome o = attrs [("queue", queue), ("outcome", o)]
    queueAttr = attrs [("queue", queue)]
    -- The policy prefix, never the key: a per-tenant suffix would be unbounded.
    admissionThrough kind prefix =
      counterAdd (admitted ms) 1 (attrs [("queue", queue), ("kind", kind), ("policy", prefix)])
    successAttr = outcome "success"
    failureAttr = outcome "failure"
    dlqAttr = outcome "dlq"
    cancelledAttr = outcome "cancelled"
    unavailableAttr = outcome "unavailable"

-- | Record rows affected by a reaper operation. Schema-wide reaper work has no
-- queue attribute.
otelMaintenance :: (MonadIO m) => ArbiterMeters -> MaintenanceOp -> Int64 -> m ()
otelMaintenance ms op n = liftIO (counterAdd (maintained ms) n (attrs [("op", maintenanceOpName op)]))

-- | Build an OTel attribute set from text key/value pairs.
attrs :: [(Text, Text)] -> Attributes
attrs kvs = unsafeAttributesFromListIgnoringLimits [(k, toAttribute v) | (k, v) <- kvs]

-- | The @kind@ attribute for an admission metric. Counters and the
-- gauges so a dashboard can join them.
rateLimitKind, concurrencyKind :: Text
rateLimitKind = "rate_limit"
concurrencyKind = "concurrency"

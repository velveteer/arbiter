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
import Arbiter.Core.Job.Types
  ( HasKind (..)
  , ObservabilityHooks (..)
  , PayloadKeys (..)
  , defaultObservabilityHooks
  , payloadKeys
  )
import Arbiter.Core.RateLimit.Spec (RateLimitKey (..))
import Arbiter.Worker.Config (MaintenanceOp, maintenanceOpName)
import Control.Monad (mfilter)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Fixed (Fixed (MkFixed))
import Data.Foldable (traverse_)
import Data.Int (Int64)
import Data.Map.Strict qualified as Map
import Data.Set qualified as Set
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
otelHooks :: forall m payload. (HasKind payload, MonadIO m) => ArbiterMeters -> Text -> ObservabilityHooks m payload
otelHooks ms queue =
  defaultObservabilityHooks
    { onJobClaimed = \job _ -> liftIO $ do
        counterAdd (claimed ms) 1 (queueAttr (labelOf job))
        traverse_ (admissionThrough rateLimitKind . rlkPrefix) (jobRateLimitKey (payloadKeys job))
        traverse_ (admissionThrough concurrencyKind . ckPrefix) (jobConcurrencyKey (payloadKeys job))
    , onJobSuccess = \job start end -> liftIO $ do
        let attributes = successAttr (labelOf job)
        counterAdd (processed ms) 1 attributes
        histogramRecord (duration ms) (secs start end) attributes
    , onJobFailure = \job _ start end -> liftIO (histogramRecord (duration ms) (secs start end) (failureAttr (labelOf job)))
    , onJobRetry = \job _ -> liftIO (counterAdd (retried ms) 1 (queueAttr (labelOf job)))
    , onJobFailedAndMovedToDLQ = \_ job -> liftIO (counterAdd (processed ms) 1 (dlqAttr (labelOf job)))
    , onJobCancelled = \job _ -> liftIO (counterAdd (processed ms) 1 (cancelledAttr (labelOf job)))
    , onJobUnavailable = \job _ -> liftIO (counterAdd (processed ms) 1 (unavailableAttr (labelOf job)))
    }
  where
    -- A clock stepped backwards must not subtract from the histogram's sum.
    secs start end = case nominalDiffTimeToSeconds (diffUTCTime end start) of
      MkFixed ps -> max 0 (fromInteger ps / 1e12)
    -- The declared labels, never the stored one on its own: a payload labelling by
    -- tenant would be unbounded.
    declared = Set.fromList (kindsFor @payload)
    labelOf job = mfilter (`Set.member` declared) (jobKind (payloadKeys job))
    -- One attribute set per label, built once per hook set rather than per job.
    byLabel :: (Maybe Text -> Attributes) -> Maybe Text -> Attributes
    byLabel build = \label -> Map.findWithDefault (build Nothing) label table
      where
        table = Map.fromList [(label, build label) | label <- Nothing : map Just (Set.toList declared)]
    kindPair = foldMap (\k -> [("kind", k)])
    queueAttr = byLabel (\label -> attrs (("queue", queue) : kindPair label))
    outcomeAttr o = byLabel (\label -> attrs ([("queue", queue), ("outcome", o)] <> kindPair label))
    successAttr = outcomeAttr "success"
    failureAttr = outcomeAttr "failure"
    dlqAttr = outcomeAttr "dlq"
    cancelledAttr = outcomeAttr "cancelled"
    unavailableAttr = outcomeAttr "unavailable"
    -- The policy prefix, never the key: a per-tenant suffix would be unbounded.
    admissionThrough kind prefix =
      counterAdd (admitted ms) 1 (attrs [("queue", queue), ("kind", kind), ("policy", prefix)])

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

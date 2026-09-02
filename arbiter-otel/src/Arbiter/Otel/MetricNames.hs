{-# LANGUAGE OverloadedStrings #-}

-- | The names of the instruments arbiter registers.
module Arbiter.Otel.MetricNames
  ( MetricName (..)
  , metricName
  , arbiterMetricNames
  ) where

import Data.Text (Text)

-- | Every instrument the library registers.
data MetricName
  = -- Worker lifecycle
    JobsClaimed
  | JobsProcessed
  | JobsRetries
  | AdmissionAdmitted
  | MaintenanceRows
  | HandlerDuration
  | -- Queue depth
    QueueDepth
  | QueueDepthByKind
  | QueueOldestReadyAge
  | QueueOldestInFlightAge
  | Workers
  | -- Admission
    AdmissionKeys
  | AdmissionLimit
  | AdmissionInFlight
  | AdmissionBusiestKey
  | AdmissionTokens
  | -- Postgres health
    PgTableDeadTuples
  | PgTableLiveTuples
  | PgTableAutovacuumAge
  | PgTableSizeBytes
  | PgTableScans
  | PgTableBlocks
  | PgTableXidAge
  | PgDbConnections
  | PgDbBackends
  | PgDbOldestTransactionAge
  | PgDbOldestQueryAge
  | GaugesAge
  deriving stock (Bounded, Enum, Eq, Show)

-- | The exported name of a metric.
metricName :: MetricName -> Text
metricName = \case
  JobsClaimed -> "arbiter.jobs.claimed"
  JobsProcessed -> "arbiter.jobs.processed"
  JobsRetries -> "arbiter.jobs.retries"
  AdmissionAdmitted -> "arbiter.admission.admitted"
  MaintenanceRows -> "arbiter.maintenance.rows"
  HandlerDuration -> "arbiter.job.handler.duration"
  QueueDepth -> "arbiter.queue.depth"
  QueueDepthByKind -> "arbiter.queue.depth_by_kind"
  QueueOldestReadyAge -> "arbiter.queue.oldest_ready_age"
  QueueOldestInFlightAge -> "arbiter.queue.oldest_in_flight_age"
  Workers -> "arbiter.workers"
  AdmissionKeys -> "arbiter.admission.keys"
  AdmissionLimit -> "arbiter.admission.limit"
  AdmissionInFlight -> "arbiter.admission.in_flight"
  AdmissionBusiestKey -> "arbiter.admission.busiest_key"
  AdmissionTokens -> "arbiter.admission.tokens"
  PgTableDeadTuples -> "arbiter.pg.table.dead_tuples"
  PgTableLiveTuples -> "arbiter.pg.table.live_tuples"
  PgTableAutovacuumAge -> "arbiter.pg.table.autovacuum_age"
  PgTableSizeBytes -> "arbiter.pg.table.size_bytes"
  PgTableScans -> "arbiter.pg.table.scans"
  PgTableBlocks -> "arbiter.pg.table.blocks"
  PgTableXidAge -> "arbiter.pg.table.xid_age"
  PgDbConnections -> "arbiter.pg.database.connections"
  PgDbBackends -> "arbiter.pg.database.backends"
  PgDbOldestTransactionAge -> "arbiter.pg.database.oldest_transaction_age"
  PgDbOldestQueryAge -> "arbiter.pg.database.oldest_query_age"
  GaugesAge -> "arbiter.gauges.age"

-- | Every metric name arbiter exports.
arbiterMetricNames :: [Text]
arbiterMetricNames = map metricName [minBound .. maxBound]

{-# LANGUAGE OverloadedStrings #-}

-- | The names of the instruments arbiter registers.
module Arbiter.Otel.MetricNames
  ( MetricName (..)
  , metricName
  , arbiterMetricNames
  ) where

import Data.Text (Text)

-- | Every instrument the library registers. A job's @kind@ attribute is absent when its
-- label is not declared.
data MetricName
  = -- Worker lifecycle

    -- | Counter, no unit. Attributes: @queue@, @kind@.
    JobsClaimed
  | -- | Counter, no unit. Attributes: @queue@, @outcome@ (success, dlq, cancelled, unavailable), @kind@.
    JobsProcessed
  | -- | Counter, no unit. Attributes: @queue@, @kind@.
    JobsRetries
  | -- | Counter, no unit. Attributes: @queue@, @policy_kind@, @policy@.
    AdmissionAdmitted
  | -- | Counter, no unit. Attributes: @op@.
    MaintenanceRows
  | -- | Histogram, @s@. Attributes: @queue@, @outcome@ (success, failure), @kind@.
    HandlerDuration
  | -- Queue depth

    -- | Gauge, @{job}@. Attributes: @queue@, @status@.
    QueueDepth
  | -- | Gauge, @{job}@. Attributes: @queue@, @kind@.
    QueueDepthByKind
  | -- | Gauge, @s@. Attributes: @queue@.
    QueueOldestReadyAge
  | -- | Gauge, @s@. Attributes: @queue@.
    QueueOldestInFlightAge
  | -- | Gauge, @{worker}@. Attributes: @queue@, @state@.
    Workers
  | -- Admission

    -- | Gauge, @{key}@. Attributes: @policy_kind@, @policy@.
    AdmissionKeys
  | -- | Gauge, @{slot}@. Attributes: @policy_kind@, @policy@.
    AdmissionLimit
  | -- | Gauge, @{job}@. Attributes: @policy@.
    AdmissionInFlight
  | -- | Gauge, @{job}@. Attributes: @policy@.
    AdmissionBusiestKey
  | -- | Gauge, @{token}@. Attributes: @policy@, @stat@.
    AdmissionTokens
  | -- Postgres health

    -- | Gauge, @{tuple}@. Attributes: @table@.
    PgTableDeadTuples
  | -- | Gauge, @{tuple}@. Attributes: @table@.
    PgTableLiveTuples
  | -- | Gauge, @s@. Attributes: @table@.
    PgTableAutovacuumAge
  | -- | Gauge, @By@. Attributes: @table@.
    PgTableSize
  | -- | Counter, @{scan}@. Attributes: @table@, @path@.
    PgTableScans
  | -- | Counter, @{block}@. Attributes: @table@, @source@.
    PgTableBlocks
  | -- | Gauge, @{transaction}@. Attributes: @table@.
    PgTableXidAge
  | -- | Gauge, @{connection}@. Attributes: @state@.
    PgDbConnections
  | -- | Gauge, @{backend}@. No attributes.
    PgDbBackends
  | -- | Gauge, @s@. No attributes.
    PgDbOldestTransactionAge
  | -- | Gauge, @s@. No attributes.
    PgDbOldestQueryAge
  | -- | Gauge, @{status}@, 1 when reachable and 0 when not. No attributes.
    DbReachable
  | -- | Gauge, @s@. No attributes.
    GaugesAge
  deriving stock (Bounded, Enum, Eq, Show)

-- | The exported name of a metric.
metricName :: MetricName -> Text
metricName = \case
  JobsClaimed -> "arbiter.jobs.claimed"
  JobsProcessed -> "arbiter.jobs.processed"
  JobsRetries -> "arbiter.jobs.retries"
  AdmissionAdmitted -> "arbiter.admission.admitted"
  MaintenanceRows -> "arbiter.maintenance.rows"
  HandlerDuration -> "arbiter.jobs.handler.duration"
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
  PgTableSize -> "arbiter.pg.table.size"
  PgTableScans -> "arbiter.pg.table.scans"
  PgTableBlocks -> "arbiter.pg.table.blocks"
  PgTableXidAge -> "arbiter.pg.table.xid_age"
  PgDbConnections -> "arbiter.pg.database.connections"
  PgDbBackends -> "arbiter.pg.database.backends"
  PgDbOldestTransactionAge -> "arbiter.pg.database.oldest_transaction_age"
  PgDbOldestQueryAge -> "arbiter.pg.database.oldest_query_age"
  DbReachable -> "arbiter.db.reachable"
  GaugesAge -> "arbiter.gauges.age"

-- | Every metric name arbiter exports.
arbiterMetricNames :: [Text]
arbiterMetricNames = map metricName [minBound .. maxBound]

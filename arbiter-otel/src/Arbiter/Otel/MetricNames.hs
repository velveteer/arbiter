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
    -- ^ Counter, no unit. Attributes: @queue@, @kind@.
  | JobsProcessed
    -- ^ Counter, no unit. Attributes: @queue@, @outcome@, @kind@.
  | JobsRetries
    -- ^ Counter, no unit. Attributes: @queue@, @kind@.
  | AdmissionAdmitted
    -- ^ Counter, no unit. Attributes: @queue@, @policy_kind@, @policy@.
  | MaintenanceRows
    -- ^ Counter, no unit. Attributes: @op@.
  | HandlerDuration
    -- ^ Histogram, @s@. Attributes: @queue@, @outcome@, @kind@.
  | -- Queue depth
    QueueDepth
    -- ^ Gauge, @{job}@. Attributes: @queue@, @status@.
  | QueueDepthByKind
    -- ^ Gauge, @{job}@. Attributes: @queue@, @kind@.
  | QueueOldestReadyAge
    -- ^ Gauge, @s@. Attributes: @queue@.
  | QueueOldestInFlightAge
    -- ^ Gauge, @s@. Attributes: @queue@.
  | Workers
    -- ^ Gauge, @{worker}@. Attributes: @queue@, @state@.
  | -- Admission
    AdmissionKeys
    -- ^ Gauge, @{key}@. Attributes: @policy_kind@, @policy@.
  | AdmissionLimit
    -- ^ Gauge, @{slot}@. Attributes: @policy_kind@, @policy@.
  | AdmissionInFlight
    -- ^ Gauge, @{job}@. Attributes: @policy@.
  | AdmissionBusiestKey
    -- ^ Gauge, @{job}@. Attributes: @policy@.
  | AdmissionTokens
    -- ^ Gauge, @{token}@. Attributes: @policy@, @stat@.
  | -- Postgres health
    PgTableDeadTuples
    -- ^ Gauge, @{tuple}@. Attributes: @table@.
  | PgTableLiveTuples
    -- ^ Gauge, @{tuple}@. Attributes: @table@.
  | PgTableAutovacuumAge
    -- ^ Gauge, @s@. Attributes: @table@.
  | PgTableSize
    -- ^ Gauge, @By@. Attributes: @table@.
  | PgTableScans
    -- ^ Counter, @{scan}@. Attributes: @table@, @path@.
  | PgTableBlocks
    -- ^ Counter, @{block}@. Attributes: @table@, @source@.
  | PgTableXidAge
    -- ^ Gauge, @{transaction}@. Attributes: @table@.
  | PgDbConnections
    -- ^ Gauge, @{connection}@. Attributes: @state@.
  | PgDbBackends
    -- ^ Gauge, @{backend}@. No attributes.
  | PgDbOldestTransactionAge
    -- ^ Gauge, @s@. No attributes.
  | PgDbOldestQueryAge
    -- ^ Gauge, @s@. No attributes.
  | DbReachable
    -- ^ Gauge, @{status}@, 1 when reachable and 0 when not. No attributes.
  | GaugesAge
    -- ^ Gauge, @s@. No attributes.
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

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
  | QueueOldestReadyAge
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
  | PgConnections
  | PgBackends
  | PgOldestTransactionAge
  | PgOldestQueryAge
  | PgXidAge
  | PgBlocks
  | PgTransactions
  | PgDeadlocks
  | GaugesAge
  deriving stock (Bounded, Enum, Eq, Show)

metricName :: MetricName -> Text
metricName = \case
  JobsClaimed -> "arbiter.jobs.claimed"
  JobsProcessed -> "arbiter.jobs.processed"
  JobsRetries -> "arbiter.jobs.retries"
  AdmissionAdmitted -> "arbiter.admission.admitted"
  MaintenanceRows -> "arbiter.maintenance.rows"
  HandlerDuration -> "arbiter.job.handler.duration"
  QueueDepth -> "arbiter.queue.depth"
  QueueOldestReadyAge -> "arbiter.queue.oldest_ready_age"
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
  PgConnections -> "arbiter.pg.connections"
  PgBackends -> "arbiter.pg.backends"
  PgOldestTransactionAge -> "arbiter.pg.oldest_transaction_age"
  PgOldestQueryAge -> "arbiter.pg.oldest_query_age"
  PgXidAge -> "arbiter.pg.xid_age"
  PgBlocks -> "arbiter.pg.blocks"
  PgTransactions -> "arbiter.pg.transactions"
  PgDeadlocks -> "arbiter.pg.deadlocks"
  GaugesAge -> "arbiter.gauges.age"

arbiterMetricNames :: [Text]
arbiterMetricNames = map metricName [minBound .. maxBound]

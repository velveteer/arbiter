{-# LANGUAGE OverloadedStrings #-}

-- | The names of the instruments arbiter registers.
module Arbiter.Otel.MetricNames
  ( arbiterMetricNames

    -- * Worker lifecycle
  , jobsClaimed
  , jobsProcessed
  , jobsRetries
  , admissionAdmitted
  , maintenanceRows
  , handlerDuration

    -- * Queue depth
  , queueDepth
  , queueOldestReadyAge
  , workers

    -- * Admission
  , admissionKeys
  , admissionLimit
  , admissionInFlight
  , admissionBusiestKey
  , admissionTokens

    -- * Postgres health
  , pgTableDeadTuples
  , pgTableLiveTuples
  , pgTableAutovacuumAge
  , pgTableSizeBytes
  , pgTableScans
  , pgConnections
  , pgBackends
  , pgOldestTransactionAge
  , pgOldestQueryAge
  , pgXidAge
  , pgBlocks
  , pgTransactions
  , pgDeadlocks
  , gaugesAge
  ) where

import Data.Text (Text)

-- | Every instrument the library registers.
arbiterMetricNames :: [Text]
arbiterMetricNames =
  [ jobsClaimed
  , jobsProcessed
  , jobsRetries
  , admissionAdmitted
  , maintenanceRows
  , handlerDuration
  , queueDepth
  , queueOldestReadyAge
  , workers
  , admissionKeys
  , admissionLimit
  , admissionInFlight
  , admissionBusiestKey
  , admissionTokens
  , pgTableDeadTuples
  , pgTableLiveTuples
  , pgTableAutovacuumAge
  , pgTableSizeBytes
  , pgTableScans
  , pgConnections
  , pgBackends
  , pgOldestTransactionAge
  , pgOldestQueryAge
  , pgXidAge
  , pgBlocks
  , pgTransactions
  , pgDeadlocks
  , gaugesAge
  ]

jobsClaimed, jobsProcessed, jobsRetries, admissionAdmitted, maintenanceRows, handlerDuration :: Text
jobsClaimed = "arbiter.jobs.claimed"
jobsProcessed = "arbiter.jobs.processed"
jobsRetries = "arbiter.jobs.retries"
admissionAdmitted = "arbiter.admission.admitted"
maintenanceRows = "arbiter.maintenance.rows"
handlerDuration = "arbiter.job.handler.duration"

queueDepth, queueOldestReadyAge, workers :: Text
queueDepth = "arbiter.queue.depth"
queueOldestReadyAge = "arbiter.queue.oldest_ready_age"
workers = "arbiter.workers"

admissionKeys, admissionLimit, admissionInFlight, admissionBusiestKey, admissionTokens :: Text
admissionKeys = "arbiter.admission.keys"
admissionLimit = "arbiter.admission.limit"
admissionInFlight = "arbiter.admission.in_flight"
admissionBusiestKey = "arbiter.admission.busiest_key"
admissionTokens = "arbiter.admission.tokens"

pgTableDeadTuples, pgTableLiveTuples, pgTableAutovacuumAge, pgTableSizeBytes, pgTableScans :: Text
pgTableDeadTuples = "arbiter.pg.table.dead_tuples"
pgTableLiveTuples = "arbiter.pg.table.live_tuples"
pgTableAutovacuumAge = "arbiter.pg.table.autovacuum_age"
pgTableSizeBytes = "arbiter.pg.table.size_bytes"
pgTableScans = "arbiter.pg.table.scans"

pgConnections, pgBackends, pgOldestTransactionAge, pgOldestQueryAge, pgXidAge :: Text
pgConnections = "arbiter.pg.connections"
pgBackends = "arbiter.pg.backends"
pgOldestTransactionAge = "arbiter.pg.oldest_transaction_age"
pgOldestQueryAge = "arbiter.pg.oldest_query_age"
pgXidAge = "arbiter.pg.xid_age"

pgBlocks, pgTransactions, pgDeadlocks, gaugesAge :: Text
pgBlocks = "arbiter.pg.blocks"
pgTransactions = "arbiter.pg.transactions"
pgDeadlocks = "arbiter.pg.deadlocks"
gaugesAge = "arbiter.gauges.age"

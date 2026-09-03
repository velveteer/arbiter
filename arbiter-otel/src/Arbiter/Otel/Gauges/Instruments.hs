{-# LANGUAGE OverloadedStrings #-}

-- | OpenTelemetry instruments backed by the gauge snapshot cache.
module Arbiter.Otel.Gauges.Instruments
  ( registerInstruments
  ) where

import Arbiter.Core.Concurrency.Stats qualified as Conc (ConcurrencyPolicyView (..))
import Arbiter.Core.Health qualified as Health
import Arbiter.Core.Job.Types (jobStatusToText)
import Arbiter.Core.Operations
  ( QueueOverview (..)
  , QueueStats (..)
  , queueStatusCounts
  )
import Arbiter.Core.RateLimit.Stats qualified as RL (RateLimitPolicyView (..))
import Control.Concurrent.STM (readTVarIO)
import Control.Monad (void)
import Data.Bifunctor (first)
import Data.Foldable (toList, traverse_)
import Data.HashMap.Strict (HashMap)
import Data.IORef (IORef, atomicModifyIORef')
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Text (Text)
import GHC.Clock (getMonotonicTime)
import OpenTelemetry.Metric.Core
  ( Counter
  , Meter
  , counterAdd
  , defaultAdvisoryParameters
  , meterCreateCounterDouble
  , meterCreateObservableGaugeDouble
  , observe
  )

import Arbiter.Otel.Gauges.Cache
  ( Baseline
  , Cached (..)
  , GaugeCache (..)
  , SeriesKey
  , Snapshot (..)
  , lastScan
  , live
  , riseSince
  )
import Arbiter.Otel.MetricNames qualified as Name
import Arbiter.Otel.Metrics (attrs, concurrencyKind, rateLimitKind)

-- | Register instruments that read the last shared snapshot, and return the action
-- that carries a freshly published reading onto the counters.
registerInstruments :: Meter -> GaugeCache -> IO (Cached -> IO ())
registerInstruments meter cache = do
  let withCached emit = [\res -> readTVarIO (export cache) >>= traverse_ (emit res) . live]
      callback emit = withCached (\res -> emit res . reading)
      -- Every replica exports the winner's reading, so aggregate with max, never sum.
      shared desc = desc <> " (shared reading, do not sum across replicas)"
      regGauge name unit desc cbs =
        void $
          meterCreateObservableGaugeDouble
            meter
            (Name.metricName name)
            (Just unit)
            (Just desc)
            defaultAdvisoryParameters
            cbs
      reg name unit desc emit = regGauge name unit (shared desc) (callback emit)
      regCounter name unit desc series = do
        counter <-
          meterCreateCounterDouble
            meter
            (Name.metricName name)
            (Just unit)
            (Just (shared desc))
            defaultAdvisoryParameters
        pure $ \c ->
          traverse_
            (addRise (counterBaselines cache) (Name.metricName name) counter (takenAt c))
            (series (reading c))

  reg Name.QueueDepth "{job}" "Jobs in a queue by status" $
    observed $
      over queues $ \o ->
        [ ([("queue", overviewQueue o), ("status", st)], fromIntegral n)
        | (st, n) <- statusCounts (overviewStats o)
        ]
  reg Name.QueueDepthByKind "{job}" "Jobs in a queue by payload variant" $
    observed $
      over queues $ \o ->
        [ ([("queue", overviewQueue o), ("kind", k)], fromIntegral n)
        | (k, n) <- Map.toList (kindCounts (overviewStats o))
        ]
  reg Name.QueueOldestReadyAge "s" "Age of the oldest claimable job (0 = none ready)" $
    perQueue oldestReadyAgeSeconds
  reg Name.QueueOldestInFlightAge "s" "Time the longest-running job has been leased (0 = none in flight)" $
    perQueue oldestInFlightAgeSeconds
  -- Active and paused partition the pools with a fresh heartbeat, so a queue's fleet is their sum.
  reg Name.Workers "{worker}" "Registered workers by state" $
    observed $
      over queues $ \o ->
        let paused = overviewWorkersPaused o
         in [ ([("queue", overviewQueue o), ("state", "active")], fromIntegral (max 0 (overviewWorkersLive o - paused)))
            , ([("queue", overviewQueue o), ("state", "paused")], fromIntegral paused)
            ]

  -- Keyed by policy prefix, never by admission key: a per-tenant suffix would be unbounded.
  reg Name.AdmissionKeys "{key}" "Live admission keys, by policy" $
    bothKinds (fromIntegral . Conc.keyCount) (fromIntegral . RL.bucketCount)
  reg Name.AdmissionLimit "{slot}" "Effective cap per key (concurrency slots, rate-limit tokens)" $
    bothKinds (fromIntegral . effectiveLimit) effectiveMaxTokens
  reg Name.AdmissionInFlight "{job}" "Jobs holding a concurrency slot, by policy" $
    perConcurrency (fromIntegral . Conc.totalInFlight)
  reg Name.AdmissionBusiestKey "{job}" "In-flight count of the fullest key, by policy" $
    perConcurrency (fromIntegral . fromMaybe 0 . Conc.maxInFlight)
  reg Name.AdmissionTokens "{token}" "Rate-limit tokens left across a policy's buckets" $
    observed $
      over rateLimits $ \p ->
        [ ([("policy", RL.prefix p), ("stat", stat)], fromMaybe 0 mt)
        | (stat, mt) <- [("min", RL.minTokens p), ("avg", RL.avgTokens p)]
        ]

  reg Name.PgTableDeadTuples "{tuple}" "Dead tuples pending vacuum" $ perTable (fromIntegral . Health.deadTup)
  reg Name.PgTableLiveTuples "{tuple}" "Estimated live tuples" $ perTable (fromIntegral . Health.liveTup)
  reg Name.PgTableAutovacuumAge "s" "Seconds since last (auto)vacuum, absent until one runs" $
    perTableMaybe Health.autovacuumAge
  reg Name.PgTableSizeBytes "By" "Total relation size" $ perTable (fromIntegral . Health.totalBytes)
  reg Name.PgTableXidAge "{transaction}" "Transaction-id age of the table (wraparound headroom)" $
    perTableMaybe (fmap fromIntegral . Health.xidAge)
  reg Name.PgDbConnections "{connection}" "Backends by state, across the whole database" $
    perDbBy "state" (map (fmap fromIntegral) . connCounts)
  reg Name.PgDbOldestTransactionAge "s" "Age of the oldest open transaction, across the whole database" $
    perDb Health.oldestTxnAge
  reg Name.PgDbOldestQueryAge "s" "Age of the oldest running query, across the whole database" $
    perDb Health.oldestQueryAge
  reg Name.PgDbBackends "{backend}" "Backends connected to the database" $
    perDb (fromIntegral . Health.numBackends)

  advances <-
    sequence
      [ regCounter Name.PgTableScans "{scan}" "Table scans, by the access path they took" $
          perTableTotals "path" (\t -> [("seq", Health.seqScan t), ("index", Health.idxScan t)])
      , regCounter Name.PgTableBlocks "{block}" "Block reads for the table, by whether they hit the cache" $
          perTableTotals "source" (\t -> [("hit", Health.blksHit t), ("disk", Health.blksRead t)])
      ]

  -- Absent until the first scan, so a reading never claims to know before it does.
  regGauge
    Name.DbReachable
    "{status}"
    "1 when the last health scan reached the database, 0 when it failed"
    [\res -> readTVarIO (databaseReachable cache) >>= traverse_ (\ok -> observe res (if ok then 1 else 0) (attrs []))]

  -- How far behind the exported readings have fallen, from registration until the
  -- first. A stopped loop leaves the other gauges holding their last reading, which
  -- only this tells apart from a fresh one.
  regGauge
    Name.GaugesAge
    "s"
    "Seconds since the exported readings were scanned"
    [ \res -> do
        now <- getMonotonicTime
        scanned <- lastScan <$> readTVarIO (export cache)
        observe res (now - fromMaybe (registeredAt cache) scanned) (attrs [])
    ]

  pure (\c -> traverse_ ($ c) advances)
  where
    effectiveLimit p = fromMaybe (Conc.defaultLimit p) (Conc.overrideLimit p)
    effectiveMaxTokens p = fromMaybe (RL.defaultMaxTokens p) (RL.overrideMaxTokens p)
    statusCounts = map (first jobStatusToText) . queueStatusCounts
    connCounts h =
      [ ("active", Health.connActive h)
      , ("idle", Health.connIdle h)
      , ("idle_in_transaction", Health.connIdleInTxn h)
      , ("idle_in_transaction_aborted", Health.connIdleInTxnAborted h)
      , ("blocked", Health.connBlocked h)
      , ("other", Health.connOther h)
      ]
    -- Every series is a list of rows picked out of the snapshot, each row labelled
    -- and valued. Counters hand that list over, gauges observe it.
    over pick label snap = concatMap label (pick snap)
    observed rows res = traverse_ (\(kvs, v) -> observe res v (attrs kvs)) . rows
    perConcurrency field = observed (over concurrency (\p -> [([("policy", Conc.prefix p)], field p)]))
    bothKinds concField rateField = observed $ \snap ->
      [([("policy_kind", concurrencyKind), ("policy", Conc.prefix p)], concField p) | p <- concurrency snap]
        <> [([("policy_kind", rateLimitKind), ("policy", RL.prefix p)], rateField p) | p <- rateLimits snap]
    dbOf = toList . db
    perTableTotals label pairs =
      over tables (\t -> [([("table", Health.table t), (label, k)], v) | (k, v) <- pairs t])
    perDbTotals label pairs = over dbOf (\h -> [([(label, k)], v) | (k, v) <- pairs h])
    dbTotal field = over dbOf (\h -> [([], field h)])
    perTable field = observed (over tables (\t -> [([("table", Health.table t)], field t)]))
    perTableMaybe field =
      observed (over tables (\t -> [([("table", Health.table t)], v) | v <- toList (field t)]))
    perQueue field =
      observed (over queues (\o -> [([("queue", overviewQueue o)], fromMaybe 0 (field (overviewStats o)))]))
    perDb = observed . dbTotal
    perDbBy label = observed . perDbTotals label

-- | Count an absolute total's rise since the scan it was last counted from.
addRise
  :: IORef (HashMap SeriesKey Baseline)
  -> Text
  -> Counter Double
  -> Double
  -- ^ Monotonic time the reading was scanned at.
  -> ([(Text, Text)], Double)
  -> IO ()
addRise baselines name counter scannedAt (kvs, total) = do
  rise <- atomicModifyIORef' baselines (riseSince (name, kvs) scannedAt total)
  counterAdd counter rise (attrs kvs)

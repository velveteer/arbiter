{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Queue-depth and Postgres-health gauges, backed by a slow-refresh cache.
module Arbiter.Otel.Gauges
  ( startGauges
  , withGaugeLoop
  ) where

import Arbiter.Core.Concurrency.Stats qualified as Conc (ConcurrencyPolicyView (..))
import Arbiter.Core.Health qualified as Health
import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.Job.Types (jobStatusToText)
import Arbiter.Core.MonadArbiter (MonadArbiter, withDbTransaction)
import Arbiter.Core.Operations
  ( QueueOverview (..)
  , QueueStats (..)
  , Shared (..)
  , gateNameFor
  , getAllQueueStats
  , listConcurrencyPolicies
  , listRateLimitPolicies
  , micros
  , queueStatusCounts
  , runGatedShared
  , setLocalStatementTimeout
  )
import Arbiter.Core.RateLimit.Stats qualified as RL (RateLimitPolicyView (..))
import Arbiter.Worker (LogConfig, LogLevel (..), tryLog, warnEx)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically, modifyTVar', readTVarIO, writeTVar)
import Control.Exception (SomeException)
import Control.Monad (forever, void)
import Data.Bifunctor (first)
import Data.Foldable (toList, traverse_)
import Data.HashMap.Strict (HashMap)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef, writeIORef)
import Data.Maybe (fromMaybe, isNothing)
import Data.Text (Text)
import Data.Time (NominalDiffTime)
import GHC.Clock (getMonotonicTime)
import OpenTelemetry.Metric.Core
  ( Meter
  , ObservableResult
  , defaultAdvisoryParameters
  , meterCreateObservableCounterDouble
  , meterCreateObservableGaugeDouble
  , observe
  )
import System.Timeout (timeout)
import UnliftIO.Exception (bracket, finally, tryAny)

import Arbiter.Otel.Gauges.Cells
  ( Baseline
  , Cached (..)
  , Export (..)
  , GaugeCells (..)
  , SeriesKey
  , Snapshot (..)
  , lastScan
  , live
  , newGaugeCells
  , retire
  , riseSince
  )
import Arbiter.Otel.MetricNames qualified as Name
import Arbiter.Otel.Metrics (arbiterMeter, attrs, concurrencyKind, rateLimitKind)
import Arbiter.Otel.Telemetry qualified as Tel

-- | The gate a queue set publishes its snapshot under. One per set, so a reader never
-- exports another set's queues.
gaugeGate :: (MonadArbiter m) => [TableName] -> m Text
gaugeGate = gateNameFor "refresh-gauges"

-- | Register the gauges and return the refresh loop for the caller to run, over the
-- given queues and the caller's own database env. A handle with its metrics off
-- registers nothing and its loop does nothing.
startGauges
  :: (MonadArbiter m)
  => Tel.Telemetry
  -> LogConfig
  -> (forall a. m a -> IO a)
  -> SchemaName
  -> [TableName]
  -> NominalDiffTime
  -> IO (IO ())
startGauges tel baseLog runDb schema queueTables refreshInterval = do
  (loop, stop) <- prepareGauges tel baseLog runDb schema queueTables refreshInterval
  pure (loop `finally` stop)

-- | 'startGauges' with the reading bracketed, so the instruments stop observing however
-- @use@ ends. Each series' last point stands until the meter provider shuts down.
withGaugeLoop
  :: (MonadArbiter m)
  => Tel.Telemetry
  -> LogConfig
  -> (forall a. m a -> IO a)
  -> SchemaName
  -> [TableName]
  -> NominalDiffTime
  -> (IO () -> IO b)
  -- ^ Runs the refresh loop, typically on a thread of its own.
  -> IO b
withGaugeLoop tel baseLog runDb schema queueTables refreshInterval use =
  bracket (prepareGauges tel baseLog runDb schema queueTables refreshInterval) snd (use . fst)

-- | The refresh loop and the action retiring the reading its instruments observe.
prepareGauges
  :: (MonadArbiter m)
  => Tel.Telemetry
  -> LogConfig
  -> (forall a. m a -> IO a)
  -> SchemaName
  -> [TableName]
  -> NominalDiffTime
  -> IO (IO (), IO ())
prepareGauges tel baseLog runDb schema queueTables refreshInterval
  | isNothing (Tel.meters tel) = pure (pure (), pure ())
  | otherwise = do
      cells <- newGaugeCells =<< getMonotonicTime
      arbiterMeter (Tel.provider tel) >>= flip registerInstruments cells
      loop <-
        gaugeRefreshLoop (Tel.telemetryLogConfig tel baseLog) runDb schema queueTables (max 1 refreshInterval) cells
      pure (loop, atomically (modifyTVar' (cache cells) retire))

-- | Register the observable instruments, each reading whatever the loop last cached.
registerInstruments :: Meter -> GaugeCells -> IO ()
registerInstruments meter cells = do
  let withCached emit = [\res -> readTVarIO (cache cells) >>= traverse_ (emit res) . live]
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
      regCounter name unit desc series =
        let emit res c =
              traverse_ (observeRise (counterBaselines cells) (Name.metricName name) res (takenAt c)) (series (reading c))
         in void $
              meterCreateObservableCounterDouble
                meter
                (Name.metricName name)
                (Just unit)
                (Just (shared desc))
                defaultAdvisoryParameters
                (withCached emit)

  reg Name.QueueDepth "{job}" "Jobs in a queue by status" $
    observed $
      over queues $ \o ->
        [ ([("queue", overviewQueue o), ("status", st)], fromIntegral n)
        | (st, n) <- statusCounts (overviewStats o)
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
  reg Name.PgTableAutovacuumAge "s" "Seconds since last (auto)vacuum (-1 = never)" $
    perTable Health.autovacuumAge
  reg Name.PgTableSizeBytes "By" "Total relation size" $ perTable (fromIntegral . Health.totalBytes)
  reg Name.PgConnections "{connection}" "Backends by state" $
    perDbBy "state" (map (fmap fromIntegral) . connCounts)
  reg Name.PgOldestTransactionAge "s" "Age of the oldest open transaction" $
    perDb Health.oldestTxnAge
  reg Name.PgOldestQueryAge "s" "Age of the oldest running query" $ perDb Health.oldestQueryAge
  reg Name.PgXidAge "{transaction}" "Transaction-id age (wraparound headroom)" $
    perDb (fromIntegral . Health.xidAge)
  reg Name.PgBackends "{backend}" "Backends currently connected" $
    perDb (fromIntegral . Health.numBackends)

  regCounter Name.PgTableScans "{scan}" "Table scans, by the access path they took" $
    perTableTotals "path" (\t -> [("seq", Health.seqScan t), ("index", Health.idxScan t)])
  regCounter Name.PgBlocks "{block}" "Shared-buffer reads, by whether they hit the cache" $
    perDbTotals "source" (\h -> [("hit", Health.blksHit h), ("disk", Health.blksRead h)])
  regCounter Name.PgTransactions "{transaction}" "Transactions by outcome" $
    perDbTotals "outcome" (\h -> [("commit", Health.xactCommit h), ("rollback", Health.xactRollback h)])
  regCounter Name.PgDeadlocks "{deadlock}" "Deadlocks detected" $ dbTotal Health.deadlocks

  -- How far behind the exported readings have fallen, from registration until the
  -- first. A stopped loop leaves the other gauges holding their last reading, which
  -- only this tells apart from a fresh one.
  regGauge
    Name.GaugesAge
    "s"
    "Seconds since the exported readings were scanned"
    [ \res -> do
        now <- getMonotonicTime
        scanned <- lastScan <$> readTVarIO (cache cells)
        observe res (now - fromMaybe (registeredAt cells) scanned) (attrs [])
    ]
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
      [([("kind", concurrencyKind), ("policy", Conc.prefix p)], concField p) | p <- concurrency snap]
        <> [([("kind", rateLimitKind), ("policy", RL.prefix p)], rateField p) | p <- rateLimits snap]
    dbOf = toList . db
    perTableTotals label pairs =
      over tables (\t -> [([("table", Health.table t), (label, k)], v) | (k, v) <- pairs t])
    perDbTotals label pairs = over dbOf (\h -> [([(label, k)], v) | (k, v) <- pairs h])
    dbTotal field = over dbOf (\h -> [([], field h)])
    perTable field = observed (over tables (\t -> [([("table", Health.table t)], field t)]))
    perQueue field =
      observed (over queues (\o -> [([("queue", overviewQueue o)], fromMaybe 0 (field (overviewStats o)))]))
    perDb = observed . dbTotal
    perDbBy label = observed . perDbTotals label

-- | The loop that scans and publishes the reading every instrument reads from.
gaugeRefreshLoop
  :: forall m
   . (MonadArbiter m)
  => LogConfig
  -> (forall a. m a -> IO a)
  -> SchemaName
  -> [TableName]
  -> NominalDiffTime
  -> GaugeCells
  -> IO (IO ())
gaugeRefreshLoop logCfg runDb schema queueTables refreshInterval cells = do
  gateRef <- newIORef Nothing
  pure $ forever $ do
    started <- getMonotonicTime
    refreshed <- refresh gateRef
    now <- getMonotonicTime
    either
      (warn "Gauge refresh failed, keeping the last reading")
      (traverse_ (atomically . writeTVar (cache cells) . Live) . (>>= stamp started now))
      refreshed
    let elapsed = now - started
    -- The gate reopens gateInterval after the publish, so a scan slower than the
    -- slack would otherwise lose the gate on the very next tick.
    threadDelay (max (micros gateInterval) (micros refreshInterval - round (elapsed * 1_000_000)))
  where
    refresh gateRef = tryAny (resolveGate gateRef) >>= either (pure . Left) sharedScan
    sharedScan gate =
      bounded (gatedScan gate) >>= \case
        Right (Just (Unreadable why)) -> localScan why
        r -> pure r
    gatedScan gate = runDb (runGatedShared schema gate gateInterval staleAfter scan)
    -- Fixed for the registration, and a query of its own when it is long.
    resolveGate gateRef =
      readIORef gateRef
        >>= maybe (runDb (gaugeGate queueTables) >>= \gate -> gate <$ writeIORef gateRef (Just gate)) pure
    -- A scan that outlives the freshness window is abandoned, so long as a fresh
    -- reading stands in for it. Only unblocks a driver that yields to the runtime.
    bounded :: IO (Maybe (Shared Snapshot)) -> IO (Either SomeException (Maybe (Shared Snapshot)))
    bounded act = do
      stale <- readingStale
      tryAny (if stale then Just <$> act else timeout (micros staleAfter) act)
        >>= traverse (maybe (Nothing <$ tryLog logCfg Warning abandoned) pure)
    abandoned = "Gauge scan outlived the freshness window, abandoned"
    -- Only an unreadable payload falls back, and only with nothing fresh of its own.
    localScan why = do
      tryLog logCfg Warning ("Shared gauge payload unreadable, scanning locally: " <> why)
      stale <- readingStale
      if stale then tryAny (Just . Ran <$> runDb scan) else pure (Right Nothing)
    readingStale = do
      now <- getMonotonicTime
      cached <- live <$> readTVarIO (cache cells)
      pure (maybe True (\c -> now - takenAt c > realToFrac staleAfter) cached)
    stamp started now = \case
      Ran snap -> Just (Cached started snap)
      Published age snap -> Just (Cached (now - age) snap)
      Unreadable _ -> Nothing
    -- Under the loop's period, so a replica can win the gate on consecutive ticks.
    gateInterval = 0.9 * refreshInterval
    -- Covers a slow scan plus one missed refresh.
    staleAfter = 3 * refreshInterval
    -- The bound that holds whether or not the driver is interruptible. Per read, so
    -- none of them pins a snapshot for the whole scan.
    boundedRead :: forall a. m a -> m a
    boundedRead q = withDbTransaction (setLocalStatementTimeout staleAfter >> q)
    scan = do
      overviews <- boundedRead (getAllQueueStats schema queueTables)
      (dbHealth, tableHealth) <- boundedRead (Health.getPgHealth schema queueTables)
      concPolicies <- boundedRead (listConcurrencyPolicies schema)
      -- No queue tables, so the policy read stays off the job tables: the
      -- queue-side throttled count is already a depth gauge.
      rlPolicies <- boundedRead (listRateLimitPolicies schema [])
      pure
        Snapshot
          { queues = overviews
          , db = dbHealth
          , tables = tableHealth
          , concurrency = concPolicies
          , rateLimits = rlPolicies
          }
    warn = warnEx logCfg

-- | Export an absolute total as its rise since the scan it was last counted from.
observeRise
  :: IORef (HashMap SeriesKey Baseline)
  -> Text
  -> ObservableResult Double
  -> Double
  -- ^ Monotonic time the reading was scanned at.
  -> ([(Text, Text)], Double)
  -> IO ()
observeRise baselines name res scannedAt (kvs, total) = do
  rise <- atomicModifyIORef' baselines (riseSince (name, kvs) scannedAt total)
  observe res rise (attrs kvs)

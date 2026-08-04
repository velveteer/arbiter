{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Queue-depth and Postgres-health gauges, backed by a slow-refresh cache.
module Arbiter.Otel.Gauges
  ( startGauges
  , withGaugeLoop
  ) where

import Arbiter.Core.Concurrency.Stats qualified as Conc (ConcurrencyPolicyView (..))
import Arbiter.Core.Exceptions (ParsingException (..))
import Arbiter.Core.Health qualified as Health
import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.Job.Types (JobStatus (..), jobStatusToText)
import Arbiter.Core.MonadArbiter (MonadArbiter)
import Arbiter.Core.Operations
  ( QueueOverview (..)
  , QueueStats (..)
  , Shared (..)
  , gateNameFor
  , getAllQueueStats
  , listConcurrencyPolicies
  , listRateLimitPolicies
  , runGatedShared
  )
import Arbiter.Core.RateLimit.Stats qualified as RL (RateLimitPolicyView (..))
import Arbiter.Worker (LogConfig, LogLevel (..), tryLog, warnEx)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically, readTVarIO, writeTVar)
import Control.Exception (SomeException, fromException)
import Control.Monad (forever, void)
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
import UnliftIO (MonadUnliftIO)
import UnliftIO.Exception (bracket, finally, tryAny)

import Arbiter.Otel.Gauges.Cells
  ( Baseline
  , Cached (..)
  , GaugeCells (..)
  , SeriesKey
  , Snapshot (..)
  , newGaugeCells
  , riseSince
  )
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
  :: (MonadArbiter m, MonadUnliftIO m)
  => Tel.Telemetry
  -> LogConfig
  -> (forall a. m a -> IO a)
  -> SchemaName
  -> [TableName]
  -> NominalDiffTime
  -> IO (IO ())
startGauges tel baseLog runDb schema queueTables refreshInterval = do
  (loop, retire) <- prepareGauges tel baseLog runDb schema queueTables refreshInterval
  pure (loop `finally` retire)

-- | 'startGauges' with the reading bracketed, so the instruments stop exporting however
-- @use@ ends.
withGaugeLoop
  :: (MonadArbiter m, MonadUnliftIO m)
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

-- | The refresh loop and the action blanking the reading its instruments export.
prepareGauges
  :: (MonadArbiter m, MonadUnliftIO m)
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
      pure (loop, atomically (writeTVar (cache cells) Nothing))

-- | Register the observable instruments, each reading whatever the loop last cached.
registerInstruments :: Meter -> GaugeCells -> IO ()
registerInstruments meter cells = do
  let withCached emit = [\res -> readTVarIO (cache cells) >>= traverse_ (emit res)]
      callback emit = withCached (\res -> emit res . reading)
      -- Every replica exports the winner's reading, so aggregate with max, never sum.
      shared desc = desc <> " (shared reading, do not sum across replicas)"
      regGauge name unit desc cbs =
        void $ meterCreateObservableGaugeDouble meter name (Just unit) (Just desc) defaultAdvisoryParameters cbs
      reg name unit desc emit = regGauge name unit (shared desc) (callback emit)
      regCounter name unit desc series =
        let emit res c = traverse_ (observeRise (counterBaselines cells) name res (takenAt c)) (series (reading c))
         in void $
              meterCreateObservableCounterDouble
                meter
                name
                (Just unit)
                (Just (shared desc))
                defaultAdvisoryParameters
                (withCached emit)

  reg "arbiter.queue.depth" "{job}" "Jobs in a queue by status" $
    observed $
      over queues $ \o ->
        [ ([("queue", overviewQueue o), ("status", st)], fromIntegral n)
        | (st, n) <- statusCounts (overviewStats o)
        ]
  reg "arbiter.queue.oldest_ready_age" "s" "Age of the oldest claimable job (0 = none ready)" $
    observed $
      over queues $ \o ->
        [([("queue", overviewQueue o)], fromMaybe 0 (oldestReadyAgeSeconds (overviewStats o)))]
  -- The states partition the live workers, so a queue's fleet is their sum.
  reg "arbiter.workers" "{worker}" "Registered workers by state" $
    observed $
      over queues $ \o ->
        let paused = overviewWorkersPaused o
         in [ ([("queue", overviewQueue o), ("state", "running")], fromIntegral (max 0 (overviewWorkersLive o - paused)))
            , ([("queue", overviewQueue o), ("state", "paused")], fromIntegral paused)
            ]

  -- Keyed by policy prefix, never by admission key: a per-tenant suffix would be unbounded.
  reg "arbiter.admission.keys" "{key}" "Live admission keys, by policy" $ \res snap -> do
    traverse_
      (\p -> observe res (fromIntegral (Conc.keyCount p)) (admissionAttrs concurrencyKind (Conc.prefix p)))
      (concurrency snap)
    traverse_
      (\p -> observe res (fromIntegral (RL.bucketCount p)) (admissionAttrs rateLimitKind (RL.prefix p)))
      (rateLimits snap)
  reg "arbiter.admission.limit" "{slot}" "Effective cap per key (concurrency slots, rate-limit tokens)" $ \res snap -> do
    traverse_
      (\p -> observe res (fromIntegral (effectiveLimit p)) (admissionAttrs concurrencyKind (Conc.prefix p)))
      (concurrency snap)
    traverse_ (\p -> observe res (effectiveMaxTokens p) (admissionAttrs rateLimitKind (RL.prefix p))) (rateLimits snap)
  reg "arbiter.admission.in_flight" "{job}" "Jobs holding a concurrency slot, by policy" $ \res snap ->
    traverse_ (\p -> observe res (fromIntegral (Conc.totalInFlight p)) (policyAttrs (Conc.prefix p))) (concurrency snap)
  reg "arbiter.admission.busiest_key" "{job}" "In-flight count of the fullest key, by policy" $ \res snap ->
    traverse_
      (\p -> observe res (fromIntegral (fromMaybe 0 (Conc.maxInFlight p))) (policyAttrs (Conc.prefix p)))
      (concurrency snap)
  reg "arbiter.admission.tokens" "{token}" "Rate-limit tokens left across a policy's buckets" $ \res snap ->
    traverse_
      ( \p -> do
          traverse_ (\t -> observe res t (tokenAttrs (RL.prefix p) "min")) (RL.minTokens p)
          traverse_ (\t -> observe res t (tokenAttrs (RL.prefix p) "avg")) (RL.avgTokens p)
      )
      (rateLimits snap)

  reg "arbiter.pg.table.dead_tuples" "{tuple}" "Dead tuples pending vacuum" $ perTable (fromIntegral . Health.deadTup)
  reg "arbiter.pg.table.live_tuples" "{tuple}" "Estimated live tuples" $ perTable (fromIntegral . Health.liveTup)
  reg "arbiter.pg.table.autovacuum_age" "s" "Seconds since last (auto)vacuum (-1 = never)" $
    perTable Health.autovacuumAge
  reg "arbiter.pg.table.size_bytes" "By" "Total relation size" $ perTable (fromIntegral . Health.totalBytes)
  reg "arbiter.pg.connections" "{connection}" "Backends by state" $
    perDbBy "state" (map (fmap fromIntegral) . connCounts)
  reg "arbiter.pg.oldest_transaction_age" "s" "Age of the oldest open transaction" $
    perDb Health.oldestTxnAge
  reg "arbiter.pg.oldest_query_age" "s" "Age of the oldest running query" $ perDb Health.oldestQueryAge
  reg "arbiter.pg.xid_age" "{transaction}" "Transaction-id age (wraparound headroom)" $
    perDb (fromIntegral . Health.xidAge)
  reg "arbiter.pg.backends" "{backend}" "Backends currently connected" $
    perDb (fromIntegral . Health.numBackends)

  regCounter "arbiter.pg.table.scans" "{scan}" "Table scans, by the access path they took" $
    perTableTotals "path" (\t -> [("seq", Health.seqScan t), ("index", Health.idxScan t)])
  regCounter "arbiter.pg.blocks" "{block}" "Shared-buffer reads, by whether they hit the cache" $
    perDbTotals "source" (\h -> [("hit", Health.blksHit h), ("disk", Health.blksRead h)])
  regCounter "arbiter.pg.transactions" "{transaction}" "Transactions by outcome" $
    perDbTotals "outcome" (\h -> [("commit", Health.xactCommit h), ("rollback", Health.xactRollback h)])
  regCounter "arbiter.pg.deadlocks" "{deadlock}" "Deadlocks detected" $ dbTotal Health.deadlocks

  -- How far behind the exported readings have fallen, from registration until the
  -- first. A stopped loop leaves the other gauges holding their last reading, which
  -- only this tells apart from a fresh one.
  regGauge
    "arbiter.gauges.age"
    "s"
    "Seconds since the exported readings were scanned"
    [ \res -> do
        now <- getMonotonicTime
        cached <- readTVarIO (cache cells)
        observe res (now - maybe (registeredAt cells) takenAt cached) (attrs [])
    ]
  where
    admissionAttrs kind prefix = attrs [("kind", kind), ("policy", prefix)]
    policyAttrs prefix = attrs [("policy", prefix)]
    tokenAttrs prefix stat = attrs [("policy", prefix), ("stat", stat)]
    effectiveLimit p = fromMaybe (Conc.defaultLimit p) (Conc.overrideLimit p)
    effectiveMaxTokens p = fromMaybe (RL.defaultMaxTokens p) (RL.overrideMaxTokens p)
    statusCounts s =
      [ (jobStatusToText Ready, readyJobs s)
      , (jobStatusToText InFlight, inFlightJobs s)
      , (jobStatusToText Scheduled, scheduledJobs s)
      , (jobStatusToText Backoff, backoffJobs s)
      , (jobStatusToText Throttled, throttledJobs s)
      , (jobStatusToText Suspended, suspendedJobs s)
      , (jobStatusToText Cancelled, cancelledJobs s)
      ]
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
    dbOf = toList . db
    perTableTotals label pairs =
      over tables (\t -> [([("table", Health.table t), (label, k)], v) | (k, v) <- pairs t])
    perDbTotals label pairs = over dbOf (\h -> [([(label, k)], v) | (k, v) <- pairs h])
    dbTotal field = over dbOf (\h -> [([], field h)])
    perTable field = observed (over tables (\t -> [([("table", Health.table t)], field t)]))
    perDb = observed . dbTotal
    perDbBy label = observed . perDbTotals label

-- | The loop that scans and publishes the reading every instrument reads from.
gaugeRefreshLoop
  :: (MonadArbiter m, MonadUnliftIO m)
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
      (traverse_ (atomically . writeTVar (cache cells) . Just . stamp started now))
      refreshed
    let elapsed = now - started
    -- The gate reopens gateInterval after the publish, so a scan slower than the
    -- slack would otherwise lose the gate on the very next tick.
    threadDelay (max (micros gateInterval) (micros refreshInterval - round (elapsed * 1_000_000)))
  where
    micros :: NominalDiffTime -> Int
    micros t = round (realToFrac t * 1_000_000 :: Double)
    refresh gateRef =
      tryAny (resolveGate gateRef)
        >>= either (pure . Left) (\gate -> bounded (gatedScan gate) >>= either localScan (pure . Right))
    gatedScan gate = runDb (runGatedShared schema gate gateInterval staleAfter scan)
    -- Fixed for the registration, and a query of its own when it is long.
    resolveGate gateRef =
      readIORef gateRef
        >>= maybe (runDb (gaugeGate queueTables) >>= \gate -> gate <$ writeIORef gateRef (Just gate)) pure
    -- A scan that outlives the freshness window is abandoned, so long as a fresh
    -- reading stands in for it.
    bounded :: IO (Maybe (Shared Snapshot)) -> IO (Either SomeException (Maybe (Shared Snapshot)))
    bounded act = do
      stale <- readingStale
      tryAny (if stale then Just <$> act else timeout (micros staleAfter) act)
        >>= traverse (maybe (Nothing <$ tryLog logCfg Warning abandoned) pure)
    abandoned = "Gauge scan outlived the freshness window, abandoned"
    -- Only an unreadable payload falls back, and only with nothing fresh of its own.
    localScan e
      | Just (ParsingException _) <- fromException e = do
          warn "Shared gauge payload unreadable, scanning locally" e
          stale <- readingStale
          if stale then bounded (runDb (Just . Ran <$> scan)) else pure (Right Nothing)
      | otherwise = pure (Left e)
    readingStale = do
      now <- getMonotonicTime
      cached <- readTVarIO (cache cells)
      pure (maybe True (\c -> now - takenAt c > realToFrac staleAfter) cached)
    stamp started now = \case
      Ran snap -> Cached started snap
      Published age snap -> Cached (now - age) snap
    -- Under the loop's period, so a replica can win the gate on consecutive ticks.
    gateInterval = 0.9 * refreshInterval
    -- Covers a slow scan plus one missed refresh.
    staleAfter = 3 * refreshInterval
    scan = do
      overviews <- getAllQueueStats schema queueTables
      (dbHealth, tableHealth) <- Health.getPgHealth schema queueTables
      concPolicies <- listConcurrencyPolicies schema
      -- No queue tables, so the policy read stays off the job tables: the
      -- queue-side throttled count is already a depth gauge.
      rlPolicies <- listRateLimitPolicies schema []
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

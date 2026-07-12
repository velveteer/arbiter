{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ScopedTypeVariables #-}

-- | Queue-depth and Postgres-health gauges, backed by a slow-refresh cache.
module Arbiter.Serve.Gauges
  ( startGauges
  ) where

import Arbiter.Core.Health qualified as Health
import Arbiter.Core.Operations (QueueOverview (..), QueueStats (..), discoverQueues, getAllQueueStats, runGatedShared)
import Arbiter.Core.PoolConfig (PoolConfig (..), defaultPoolConfig)
import Arbiter.Hasql (runHasqlDb, setPreparedStatements, withHasqlEnv)
import Arbiter.Worker (LogLevel (..), defaultLogConfig, tryLog)
import Arbiter.Worker.Telemetry (arbiterMeter, attrs, otelLogs)
import Control.Concurrent (threadDelay)
import Control.Concurrent.STM (atomically, newTVarIO, readTVarIO, writeTVar)
import Control.Monad (forever, void)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Foldable (traverse_)
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import GHC.Clock (getMonotonicTime)
import GHC.Generics (Generic)
import OpenTelemetry.Metric.Core
  ( defaultAdvisoryParameters
  , meterCreateObservableCounterDouble
  , meterCreateObservableGaugeDouble
  , observe
  )
import UnliftIO.Exception (tryAny)

import Arbiter.Serve.Telemetry qualified as Tel

data Snapshot = Snapshot
  { queues :: [QueueOverview]
  , db :: Maybe Health.PgDbHealth
  , tables :: [Health.PgTableHealth]
  }
  deriving stock (Generic)
  deriving anyclass (FromJSON, ToJSON)

-- | Depth and health describe the database, so one replica per interval scans it and publishes
-- what it read. The rest export that, rather than repeating the scan or holding a stale one.
gaugeGate :: Text
gaugeGate = "refresh-gauges"

startGauges :: Tel.Telemetry -> ByteString -> Text -> Bool -> NominalDiffTime -> IO (IO ())
startGauges tel connStr schema prepared refreshInterval = do
  cache <- newTVarIO blank
  meter <- arbiterMeter (Tel.provider tel)
  let callback emit = [\res -> readTVarIO cache >>= emit res]
      reg name unit desc emit =
        void $ meterCreateObservableGaugeDouble meter name (Just unit) (Just desc) defaultAdvisoryParameters (callback emit)
      regCounter name unit desc emit =
        void $ meterCreateObservableCounterDouble meter name (Just unit) (Just desc) defaultAdvisoryParameters (callback emit)

  reg "arbiter.queue.depth" "{job}" "Jobs in a queue by status" $ \res snap ->
    traverse_
      ( \o ->
          traverse_
            (\(st, n) -> observe res (fromIntegral n) (attrs [("queue", overviewQueue o), ("status", st)]))
            (statusCounts (overviewStats o))
      )
      (queues snap)
  reg "arbiter.queue.oldest_ready_age" "s" "Age of the oldest claimable job (0 = none ready)" $ \res snap ->
    traverse_
      (\o -> observe res (fromMaybe 0 (oldestReadyAgeSeconds (overviewStats o))) (attrs [("queue", overviewQueue o)]))
      (queues snap)
  reg "arbiter.workers" "{job}" "Registered workers by state" $ \res snap ->
    traverse_
      ( \o -> do
          observe res (fromIntegral (overviewWorkersLive o)) (attrs [("queue", overviewQueue o), ("state", "live")])
          observe res (fromIntegral (overviewWorkersPaused o)) (attrs [("queue", overviewQueue o), ("state", "paused")])
      )
      (queues snap)

  reg "arbiter.pg.table.dead_tuples" "{job}" "Dead tuples pending vacuum" $ perTable Health.deadTup fromIntegral
  reg "arbiter.pg.table.live_tuples" "{job}" "Estimated live tuples" $ perTable Health.liveTup fromIntegral
  reg "arbiter.pg.table.autovacuum_age" "s" "Seconds since last (auto)vacuum (-1 = never)" $
    perTable Health.autovacuumAge id
  reg "arbiter.pg.table.size_bytes" "By" "Total relation size" $ perTable Health.totalBytes fromIntegral
  reg "arbiter.pg.connections" "{job}" "Backends by state" $
    perDbBy "state" (map (fmap fromIntegral) . connCounts)
  reg "arbiter.pg.oldest_transaction_age" "s" "Age of the oldest open transaction" $ perDb Health.oldestTxnAge
  reg "arbiter.pg.oldest_query_age" "s" "Age of the oldest running query" $ perDb Health.oldestQueryAge
  reg "arbiter.pg.xid_age" "{job}" "Transaction-id age (wraparound headroom)" $ perDb (fromIntegral . Health.xidAge)
  reg "arbiter.pg.backends" "{backend}" "Backends currently connected" $ perDb (fromIntegral . Health.numBackends)

  regCounter "arbiter.pg.table.scans" "{scan}" "Table scans, by the access path they took" $
    perTableBy "path" (\t -> [("seq", Health.seqScan t), ("index", Health.idxScan t)])
  regCounter "arbiter.pg.blocks" "{block}" "Shared-buffer reads, by whether they hit the cache" $
    perDbBy "source" (\h -> [("hit", Health.blksHit h), ("disk", Health.blksRead h)])
  regCounter "arbiter.pg.transactions" "{transaction}" "Transactions by outcome" $
    perDbBy "outcome" (\h -> [("commit", Health.xactCommit h), ("rollback", Health.xactRollback h)])
  regCounter "arbiter.pg.deadlocks" "{deadlock}" "Deadlocks detected" $ perDb Health.deadlocks

  pure $
    withHasqlEnv (Proxy @'[]) connStr schema defaultPoolConfig {poolSize = 2} $ \env0 -> do
      let env = setPreparedStatements prepared env0
      forever $ do
        started <- getMonotonicTime
        refreshed <- tryAny (runHasqlDb env (runGatedShared schema gaugeGate refreshInterval staleAfter scan))
        -- A refresh that failed, or a snapshot nobody has published lately, exports nothing.
        -- A depth alert should fire on absent samples, never on a reading that stopped moving.
        snap <- either (\e -> Nothing <$ warn "Gauge refresh failed, dropping its samples" e) pure refreshed
        atomically $ writeTVar cache (fromMaybe blank snap)
        elapsed <- subtract started <$> getMonotonicTime
        threadDelay (max 0 (round ((realToFrac refreshInterval - elapsed) * 1_000_000)))
  where
    logCfg = otelLogs (Tel.logDestination tel) defaultLogConfig
    blank = Snapshot [] Nothing []
    -- Two intervals, so a single missed refresh does not blank every replica's series.
    staleAfter = 2 * refreshInterval
    scan = do
      overviews <- discoverQueues schema >>= getAllQueueStats schema
      (dbHealth, tableHealth) <- Health.getPgHealth schema
      pure Snapshot {queues = overviews, db = dbHealth, tables = tableHealth}
    warn label e = tryLog logCfg Warning (label <> ": " <> T.pack (show e))
    statusCounts s =
      [ ("ready", readyJobs s)
      , ("in_flight", inFlightJobs s)
      , ("scheduled", scheduledJobs s)
      , ("backoff", backoffJobs s)
      , ("throttled", throttledJobs s)
      , ("suspended", suspendedJobs s)
      ]
    connCounts h =
      [ ("active", Health.connActive h)
      , ("idle", Health.connIdle h)
      , ("idle_in_transaction", Health.connIdleInTxn h)
      , ("blocked", Health.connBlocked h)
      ]
    perTable field conv res snap = traverse_ (\t -> observe res (conv (field t)) (attrs [("table", Health.table t)])) (tables snap)
    perTableBy label pairs res snap =
      traverse_
        (\t -> traverse_ (\(k, v) -> observe res v (attrs [("table", Health.table t), (label, k)])) (pairs t))
        (tables snap)
    perDb field res snap = traverse_ (\h -> observe res (field h) (attrs [])) (db snap)
    perDbBy label pairs res snap =
      traverse_ (\h -> traverse_ (\(k, v) -> observe res v (attrs [(label, k)])) (pairs h)) (db snap)

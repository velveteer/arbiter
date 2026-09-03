{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Main (main) where

import Arbiter.Core.Concurrency.Spec (HasConcurrency (..), concurrencyBy, concurrencyPool)
import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.HighLevel (QueueOperation, RegistryAdmissionPolicies)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
  ( JobRead
  , JobWrite
  , attempts
  , defaultGroupedJob
  , defaultJob
  , payload
  , setNotVisibleUntil
  )
import Arbiter.Core.MonadArbiter (HasRegistry, MonadArbiter (..), ResultOf)
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.QueueRegistry (Queue, RegistryTables)
import Arbiter.Core.RateLimit.Spec (HasRateLimit (..), limitBy, tokenBucket)
import Arbiter.Hasql (HasqlDb, createHasqlEnvWithConfig, runHasqlDb, setPreparedStatements)
import Arbiter.Migrations (MigrationResult (..), defaultMigrationConfig, runMigrationsForRegistry)
import Arbiter.Orville
  ( createOrvilleConnectionOptions
  , orvilleExecuteQuery
  , orvilleExecuteStatement
  , orvilleRunHandlerWithConnection
  , orvilleWithDbTransaction
  )
import Arbiter.Otel qualified as Otel
import Arbiter.Simple (SimpleDb, SimpleEnv, createSimpleEnv, createSimpleEnvWithConfig, runSimpleDb)
import Arbiter.Worker
  ( BatchCallbacks (..)
  , EncodeJobResult
  , WorkerConfig (..)
  , defaultBatchedWorkerConfig
  , runWorkerPool
  , silentLogConfig
  , transactionalWorkerConfig
  )
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently_, race_)
import Control.Concurrent.MVar (MVar, modifyMVar, newMVar)
import Control.Exception (finally)
import Control.Monad (replicateM, void, when)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Reader (ReaderT (..), asks)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.Foldable (toList, traverse_)
import Data.IORef (IORef, atomicModifyIORef', modifyIORef', newIORef, readIORef, writeIORef)
import Data.Int (Int64)
import Data.List (find, partition)
import Data.List.NonEmpty (NonEmpty)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Tagged (Tagged (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime, addUTCTime, diffUTCTime, getCurrentTime)
import Database.PostgreSQL.Simple (Connection, In (..), Only (..), Query, close, connectPostgreSQL, execute)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Hasql.Connection qualified as Hasql
import Numeric (showFFloat)
import OpenTelemetry.Exporter.Span (ExportResult (..), SpanExporter (..))
import OpenTelemetry.Metric (createMeterProvider, defaultSdkMeterProviderOptions)
import OpenTelemetry.Metric.Core (setGlobalMeterProvider)
import OpenTelemetry.Processor.Batch.Span (batchProcessor, batchTimeoutConfig)
import OpenTelemetry.Resource (materializeResources, mkResource)
import OpenTelemetry.Trace.Core
  ( FlushResult (..)
  , ShutdownResult (..)
  , TracerProvider
  , createTracerProvider
  , emptyTracerProviderOptions
  , getGlobalTracerProvider
  , setGlobalTracerProvider
  )
import Orville.PostgreSQL qualified as O
import Orville.PostgreSQL.UnliftIO qualified as O
import System.Exit (die)
import System.IO.Unsafe (unsafePerformIO)
import Test.Tasty (localOption, mkTimeout)
import Test.Tasty.Bench
import Test.Tasty.Providers (IsTest (..), singleTest, testPassed)
import UnliftIO (MonadUnliftIO)

benchSchema :: Text
benchSchema = "arbiter"

benchConnStr :: ByteString
benchConnStr = "host=localhost port=5432 user=postgres password=master dbname=postgres options='-c track_functions=all'"

trialCount :: Int
trialCount = 10

trialDurationUs :: Int
trialDurationUs = 10_000_000

-- | Per-test timeout. Each test runs trialCount trials plus per-trial preloads.
benchTimeout :: Integer
benchTimeout = fromIntegral trialCount * fromIntegral trialDurationUs * 40

steadyStateWarmupUs :: Int
steadyStateWarmupUs = 2_000_000

data BenchPayload
  = BenchMessage Int
  | BenchBatch Int
  | -- | Fails on its first attempt, then succeeds. Populates the backoff backlog.
    BenchFlaky Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Gated payloads for the overhead benches. The huge limits keep the gates open.
newtype BenchRl = BenchRl Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype BenchCc = BenchCc Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype BenchBoth = BenchBoth Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Gate-key cardinality knob. Sweep to vary the per-key sharing ratio.
gateKeyCount :: Int
gateKeyCount = 256

gateKey :: Int -> Text
gateKey index = T.pack (show (index `mod` gateKeyCount))

-- | A 'BenchBoth' concurrency key decorrelated from 'gateKey'.
gateKey2 :: Int -> Text
gateKey2 index = T.pack (show (index `mod` max 1 (gateKeyCount - 1)))

instance HasRateLimit BenchRl where
  rateLimitFor = limitBy (tokenBucket "blr" 1.0e9 1) (\(BenchRl index) -> gateKey index)

instance HasConcurrency BenchCc where
  concurrencyFor = concurrencyBy (concurrencyPool "blc" 1000000) (\(BenchCc index) -> gateKey index)

instance HasRateLimit BenchBoth where
  rateLimitFor = limitBy (tokenBucket "bbr" 1.0e9 1) (\(BenchBoth index) -> gateKey index)

instance HasConcurrency BenchBoth where
  concurrencyFor = concurrencyBy (concurrencyPool "bbc" 1000000) (\(BenchBoth index) -> gateKey2 index)

type BenchRegistry =
  '[ Queue "bench_queue" BenchPayload
   , Queue "bench_rl_queue" BenchRl
   , Queue "bench_cc_queue" BenchCc
   , Queue "bench_both_queue" BenchBoth
   ]

data QueueFlavor
  = Ungrouped
  | Grouped Int
  | -- | Grouped with 80 percent of jobs assigned to a small hot set.
    GroupedSkewed Int Int
  | Mixed Int
  | -- | Grouped. A fraction of jobs is scheduled into the near future. A fraction
    -- fails once into backoff. Exercises the next_due and in_flight_until claim
    -- sources alongside the ready window.
    GroupedBacklog Int
  | -- | Grouped. Half the backlog is parked far into the future. The rest is ready.
    -- Measures whether a standing scheduled backlog slows the claim for the ready
    -- work in those groups.
    GroupedDormant Int
  | -- | Ungrouped 'GroupedDormant'. Half the backlog is parked 30 days out. Half is
    -- ready. Exercises the ungrouped ready/due index split.
    UngroupedDormant
  deriving stock (Eq)

-- | Deterministic 80/20 group distribution. Eight jobs in each ten go to the
-- hot set. The rest span all other groups. The group comes from a separate
-- counter.
skewedGroupKey :: Int -> Int -> Int -> Text
skewedGroupKey numGroups hotGroups index =
  T.pack $ "g" <> show groupNumber
  where
    total = max 1 numGroups
    hot = max 1 (min hotGroups total)
    cold = max 1 (total - hot)
    spread = index `div` 10
    groupNumber
      | index `mod` 10 < 8 = (spread `mod` hot) + 1
      | hot == total = (spread `mod` hot) + 1
      | otherwise = hot + (spread `mod` cold) + 1

data BenchMode
  = BenchSingleJobMode
  | BenchBatchedJobsMode Int

-- | Whether a trial's pools carry OpenTelemetry, measuring what instrumenting costs.
data Instrumentation = Plain | Instrumented
  deriving stock (Eq)

-- | The SDK the instrumented trials record into. Real span and metric machinery that
-- exports nowhere. Built once and shared by every trial.
benchTelemetry :: IO (TracerProvider, Otel.Telemetry)
benchTelemetry = modifyMVar benchTelemetryVar $ \case
  Just built -> pure (Just built, built)
  Nothing -> do
    processor <- batchProcessor batchTimeoutConfig discardSpans
    tracerProvider <- createTracerProvider [processor] emptyTracerProviderOptions
    (meterProvider, _env) <- createMeterProvider (materializeResources (mkResource [])) defaultSdkMeterProviderOptions
    setGlobalMeterProvider meterProvider
    -- The handle holds nothing bracketed and outlives the call.
    tel <- Otel.withExternalTelemetry (Just meterProvider) Nothing pure
    pure (Just (tracerProvider, tel), (tracerProvider, tel))
  where
    discardSpans =
      SpanExporter
        { spanExporterExport = const (pure Success)
        , spanExporterShutdown = pure ShutdownSuccess
        , spanExporterForceFlush = pure FlushSuccess
        }

benchTelemetryVar :: MVar (Maybe (TracerProvider, Otel.Telemetry))
benchTelemetryVar = unsafePerformIO (newMVar Nothing)
{-# NOINLINE benchTelemetryVar #-}

-- | Run @use@ over a trial's pools under the global tracer provider that trial
-- measures, restoring the previous provider afterwards.
withInstrumentedPools
  :: (QueueOperation m BenchPayload)
  => Instrumentation
  -> [WorkerConfig m BenchPayload]
  -> ([WorkerConfig m BenchPayload] -> IO a)
  -> IO a
withInstrumentedPools otel configs use = do
  previous <- getGlobalTracerProvider
  instrumented `finally` setGlobalTracerProvider previous
  where
    instrumented = case otel of
      Plain -> do
        setGlobalTracerProvider =<< createTracerProvider [] emptyTracerProviderOptions
        use configs
      Instrumented -> do
        (tracerProvider, tel) <- benchTelemetry
        setGlobalTracerProvider tracerProvider
        use (map (Otel.instrumentConfig tel) configs)

-- | One job in a 'GroupedBacklog' queue, selected by index. A fifth are flaky and
-- fail once into backoff. A fifth are scheduled into the near future. The rest are
-- ready now.
backlogJob :: UTCTime -> Int -> Int -> JobWrite BenchPayload
backlogJob now numGroups index =
  let groupKey = T.pack $ "g" <> show ((index `mod` numGroups) + 1)
   in case index `mod` 5 of
        0 -> defaultGroupedJob groupKey (BenchFlaky index)
        1 -> setNotVisibleUntil (Just (addUTCTime (scheduledDelay index) now)) $ defaultGroupedJob groupKey (BenchBatch index)
        _ -> defaultGroupedJob groupKey (BenchBatch index)

-- | Spread scheduled jobs across the first few seconds of the trial.
scheduledDelay :: Int -> NominalDiffTime
scheduledDelay index = realToFrac (0.5 + fromIntegral (index `mod` 7) * 0.4 :: Double)

-- | One job in a 'GroupedDormant' queue. Each group alternates ready jobs with
-- jobs parked 30 days out.
dormantJob :: UTCTime -> Int -> Int -> JobWrite BenchPayload
dormantJob now numGroups index =
  let groupKey = T.pack $ "g" <> show ((index `mod` numGroups) + 1)
   in if odd (index `div` numGroups)
        then setNotVisibleUntil (Just (addUTCTime (30 * 86400) now)) $ defaultGroupedJob groupKey (BenchBatch index)
        else defaultGroupedJob groupKey (BenchBatch index)

-- | Ungrouped 'dormantJob'. Alternates ready jobs with jobs parked 30 days out.
dormantUngroupedJob :: UTCTime -> Int -> JobWrite BenchPayload
dormantUngroupedJob now index =
  if odd index
    then setNotVisibleUntil (Just (addUTCTime (30 * 86400) now)) $ defaultJob (BenchBatch index)
    else defaultJob (BenchBatch index)

-- | A flaky job on its first attempt. The dispatcher increments attempts at
-- claim. The retry lands on attempts >= 2 and succeeds.
isFlakyFirst :: JobRead BenchPayload -> Bool
isFlakyFirst job = case payload job of
  BenchFlaky _ -> attempts job <= 1
  _ -> False

-- | Single-job handler body. Fails flaky-first jobs into backoff and acks the rest.
flakyGate :: (MonadIO m) => m () -> JobRead BenchPayload -> m ()
flakyGate onAck job
  | isFlakyFirst job = throwRetryable "bench-induced backoff"
  | otherwise = onAck

-- | Batched handler body. Retries the flaky-first jobs, acks the rest, and returns
-- the acked count.
flakyBatch
  :: (Monad m) => BatchCallbacks m BenchPayload () -> NonEmpty (JobRead BenchPayload) -> m Int
flakyBatch callbacks jobs = do
  let (toFail, toAck) = partition isFlakyFirst (toList jobs)
  traverse_ (\job -> failRetry callbacks job "bench-induced backoff") toFail
  ackAll callbacks toAck
  pure (length toAck)

data BenchStats = BenchStats
  { statsMean :: !Double
  , statsStdev :: !Double
  , statsSamples :: !Int
  }

newtype ThroughputBench = ThroughputBench (IO String)

instance IsTest ThroughputBench where
  testOptions = Tagged []
  run _opts (ThroughputBench action) _progress = testPassed <$> action

computeStats :: [Double] -> BenchStats
computeStats samples = BenchStats {statsMean = mean, statsStdev = stdev, statsSamples = count}
  where
    count = length samples
    mean = sum samples / fromIntegral count
    stdev
      | count > 1 = sqrt (sum [(sample - mean) * (sample - mean) | sample <- samples] / fromIntegral (count - 1))
      | otherwise = 0

formatStats :: String -> BenchStats -> String
formatStats unit stats =
  show (round (statsMean stats) :: Int)
    <> " +/- "
    <> show (round (statsStdev stats) :: Int)
    <> " "
    <> unit
    <> " ("
    <> showFFloat (Just 1) (if statsMean stats > 0 then statsStdev stats / statsMean stats * 100 else 0) ""
    <> "%, n="
    <> show (statsSamples stats)
    <> ")"

data TriggerStats = TriggerStats
  { tsFuncName :: Text
  , tsCalls :: Int64
  , tsTotalTimeMs :: Double
  , tsMeanTimeMs :: Double
  }

resetTriggerStats :: Connection -> IO ()
resetTriggerStats conn =
  void $
    PG.execute_
      conn
      "DO $$ BEGIN PERFORM pg_stat_reset_single_function_counters(oid) FROM pg_proc WHERE proname LIKE 'maintain_bench_queue_groups%'; END $$"

readTriggerStats :: Connection -> IO [TriggerStats]
readTriggerStats conn =
  map (\(name, calls, total) -> TriggerStats name calls total (if calls > 0 then total / fromIntegral calls else 0))
    <$> PG.query_
      conn
      "SELECT funcname::text, calls, total_time \
      \FROM pg_stat_user_functions \
      \WHERE funcname LIKE 'maintain_bench_queue_groups%' \
      \ORDER BY funcname"

formatTriggerStats :: [TriggerStats] -> String
formatTriggerStats [] = "  (no trigger stats)"
formatTriggerStats stats =
  unlines $
    map
      ( \stat ->
          "  "
            <> T.unpack (tsFuncName stat)
            <> ": "
            <> show (tsCalls stat)
            <> " calls, "
            <> showFFloat (Just 1) (tsTotalTimeMs stat) ""
            <> "ms total, "
            <> showFFloat (Just 3) (tsMeanTimeMs stat) ""
            <> "ms/call"
      )
      stats

multiTrial :: Int -> IO () -> IO Double -> String -> IO String
multiTrial trials setup measure unit = do
  samples <- replicateM trials (setup >> measure)
  pure $ formatStats unit (computeStats samples)

-- ---------------------------------------------------------------------------
-- Write-amplification / autovacuum sampling
-- ---------------------------------------------------------------------------

-- | Per-table readings from pg_stat_user_tables. The dead-tuple gauge is instantaneous. The rest are cumulative.
data TableSnap = TableSnap
  { tnUpd :: Int64
  , tnHot :: Int64
  , tnDead :: Int64
  , tnAutovac :: Int64
  }

-- | WAL position (bytes from 0\/0) plus per-table churn counters at one instant.
data DbSnapshot = DbSnapshot
  { dsWal :: Int64
  , dsQueue :: TableSnap
  , dsGroups :: TableSnap
  }

zeroSnapshot :: DbSnapshot
zeroSnapshot = DbSnapshot 0 (TableSnap 0 0 0 0) (TableSnap 0 0 0 0)

-- | HOT-update percentage of total updates between two snapshots of a table.
hotPct :: TableSnap -> TableSnap -> Double
hotPct before after =
  let updates = fromIntegral (tnUpd after - tnUpd before) :: Double
      hotUpdates = fromIntegral (tnHot after - tnHot before) :: Double
   in if updates > 0 then hotUpdates / updates * 100 else 0

-- | Dead tuples a table holds at one instant. Autovacuum lowers this.
deadAt :: TableSnap -> Int64
deadAt = tnDead

-- | 'deadAt' per 1000 jobs.
deadPerK :: TableSnap -> Double -> Double
deadPerK snap jobs = fromIntegral (deadAt snap) / jobs * 1000

-- | A table's snapshot row by relname from a pg_stat_user_tables result.
snapRow :: [(Text, Int64, Int64, Int64, Int64)] -> Text -> TableSnap
snapRow rows name =
  case find (\(relName, _, _, _, _) -> relName == name) rows of
    Just (_, updates, hotUpdates, dead, autovac) -> TableSnap updates hotUpdates dead autovac
    Nothing -> TableSnap 0 0 0 0

-- | WAL position plus a per-table snapshot lookup over the named tables.
captureTableSnaps :: Connection -> [Text] -> IO (Int64, Text -> TableSnap)
captureTableSnaps conn names = do
  [Only wal] <- PG.query_ conn "SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), '0/0')::bigint"
  rows <-
    PG.query
      conn
      "SELECT relname::text, n_tup_upd, n_tup_hot_upd, n_dead_tup, autovacuum_count \
      \FROM pg_stat_user_tables \
      \WHERE schemaname = 'arbiter' AND relname IN ?"
      (Only (In names))
  pure (wal, snapRow rows)

captureDbSnapshot :: Connection -> IO DbSnapshot
captureDbSnapshot conn = do
  (wal, snapFor) <- captureTableSnaps conn ["bench_queue", "bench_queue_groups"]
  pure $ DbSnapshot wal (snapFor "bench_queue") (snapFor "bench_queue_groups")

-- | One trial's throughput plus write-amplification\/autovacuum metrics, all
-- measured over the post-warmup window.
data SteadyResult = SteadyResult
  { srThroughput :: Double
  , srGroupTriggerUsPerJob :: Double
  , srWalPerJob :: Double
  , srQueueHotPct :: Double
  , srQueueUpdPerJob :: Double
  , srQueueDead :: Int64
  , srQueueDeadPerK :: Double
  , srQueueAutovac :: Int64
  , srGroupsHotPct :: Double
  , srGroupsDead :: Int64
  , srGroupsDeadPerK :: Double
  , srGroupsAutovac :: Int64
  , srTriggers :: [TriggerStats]
  }

mkSteadyResult :: Double -> Int -> DbSnapshot -> DbSnapshot -> [TriggerStats] -> SteadyResult
mkSteadyResult throughput processed before after triggers =
  SteadyResult
    { srThroughput = throughput
    , srGroupTriggerUsPerJob = sum (map tsTotalTimeMs triggers) * 1000 / jobs
    , srWalPerJob = fromIntegral (dsWal after - dsWal before) / jobs
    , srQueueHotPct = hotPct (dsQueue before) (dsQueue after)
    , srQueueUpdPerJob = fromIntegral (tnUpd (dsQueue after) - tnUpd (dsQueue before)) / jobs
    , srQueueDead = deadAt (dsQueue after)
    , srQueueDeadPerK = deadPerK (dsQueue after) jobs
    , srQueueAutovac = tnAutovac (dsQueue after) - tnAutovac (dsQueue before)
    , srGroupsHotPct = hotPct (dsGroups before) (dsGroups after)
    , srGroupsDead = deadAt (dsGroups after)
    , srGroupsDeadPerK = deadPerK (dsGroups after) jobs
    , srGroupsAutovac = tnAutovac (dsGroups after) - tnAutovac (dsGroups before)
    , srTriggers = triggers
    }
  where
    jobs = fromIntegral (max 1 processed) :: Double

formatSteady :: [SteadyResult] -> String
formatSteady [] = "(no samples)"
formatSteady results =
  formatStats "jobs/sec" (computeStats (map srThroughput results))
    <> "\n  group triggers: "
    <> showFFloat (Just 2) (meanOf srGroupTriggerUsPerJob) ""
    <> " us/job"
    <> "\n  write-amp: "
    <> showFFloat (Just 0) (meanOf srWalPerJob) ""
    <> " B WAL/job | queue HOT "
    <> showFFloat (Just 0) (meanOf srQueueHotPct) ""
    <> "% ("
    <> showFFloat (Just 2) (meanOf srQueueUpdPerJob) ""
    <> " upd/job) | groups HOT "
    <> showFFloat (Just 0) (meanOf srGroupsHotPct) ""
    <> "%"
    <> "\n  autovacuum: queue "
    <> showFFloat (Just 0) (meanOf (fromIntegral . srQueueDead)) ""
    <> " dead ("
    <> showFFloat (Just 0) (meanOf srQueueDeadPerK) ""
    <> "/1k jobs), "
    <> showFFloat (Just 1) (meanOf (fromIntegral . srQueueAutovac)) ""
    <> " autovac/trial | groups "
    <> showFFloat (Just 0) (meanOf (fromIntegral . srGroupsDead)) ""
    <> " dead ("
    <> showFFloat (Just 0) (meanOf srGroupsDeadPerK) ""
    <> "/1k jobs), "
    <> showFFloat (Just 1) (meanOf (fromIntegral . srGroupsAutovac)) ""
    <> " autovac/trial\n"
    <> formatTriggerStats (srTriggers (last results))
  where
    meanOf :: (SteadyResult -> Double) -> Double
    meanOf field = sum (map field results) / fromIntegral (length results)

multiTrialSteady :: Int -> IO () -> IO SteadyResult -> IO String
multiTrialSteady trials setup measure = do
  results <- replicateM trials (setup >> measure)
  pure $ formatSteady results

-- | Bracket an action returning (throughput, jobs processed) with trigger-stat
-- reset and WAL/churn snapshots, yielding a 'SteadyResult'. For trials whose
-- whole duration is the measurement window, such as the worker benches.
captureWindow :: Connection -> IO (Double, Int) -> IO SteadyResult
captureWindow statsConn body = do
  resetTriggerStats statsConn
  snap0 <- captureDbSnapshot statsConn
  (throughput, processed) <- body
  snap1 <- captureDbSnapshot statsConn
  trg <- readTriggerStats statsConn
  pure (mkSteadyResult throughput processed snap0 snap1 trg)

type RunM m = forall a. m a -> IO a

type SimpleM = SimpleDb BenchRegistry IO
type HasqlM = HasqlDb BenchRegistry IO

newtype BenchOrville a = BenchOrville {unBenchOrville :: ReaderT (Text, O.OrvilleState) IO a}
  deriving newtype
    (Applicative, Functor, Monad, MonadCatch, MonadIO, MonadMask, MonadThrow, MonadUnliftIO, O.MonadOrville)

instance O.HasOrvilleState BenchOrville where
  askOrvilleState = BenchOrville $ asks snd
  localOrvilleState adjust (BenchOrville action) = BenchOrville $ ReaderT $ \(schema, state) ->
    runReaderT action (schema, adjust state)

instance O.MonadOrvilleControl BenchOrville where
  liftWithConnection = O.liftWithConnectionViaUnliftIO
  liftCatch = O.liftCatchViaUnliftIO
  liftMask = O.liftMaskViaUnliftIO

instance MonadArbiter BenchOrville where
  type RegistryOf BenchOrville = BenchRegistry
  type Handler BenchOrville job result = job -> BenchOrville result
  getSchema = BenchOrville $ asks fst
  executeQuery = orvilleExecuteQuery
  executeStatement = orvilleExecuteStatement
  withDbTransaction = orvilleWithDbTransaction
  runHandlerWithConnection = orvilleRunHandlerWithConnection
  getListener = pure Nothing

type OrvilleM = BenchOrville

-- | @numPools@ copies of a backend's config, tuned for benching.
benchConfigs
  :: RunM m
  -> Int
  -> m (WorkerConfig m BenchPayload)
  -> IO [WorkerConfig m BenchPayload]
benchConfigs runM numPools mkConfig = replicateM numPools (benchTune <$> runM mkConfig)

-- | The poll interval and logging every benched pool runs with.
benchTune :: WorkerConfig m payload -> WorkerConfig m payload
benchTune config = config {pollInterval = 0.1, logConfig = silentLogConfig}

-- | A worker trial for one backend. @mkSingle@ builds that backend's single-job config.
workerTrial
  :: (HasRegistry m BenchRegistry)
  => RunM m
  -> Connection
  -> (Int -> m (WorkerConfig m BenchPayload))
  -> Int
  -> Int
  -> Int
  -> Int
  -> BenchMode
  -> IO SteadyResult
workerTrial runM statsConn mkSingle totalJobs durationUs numPools workersPerPool modeConfig = do
  configs <- benchConfigs runM numPools $ case modeConfig of
    BenchSingleJobMode -> mkSingle workersPerPool
    BenchBatchedJobsMode batchSize ->
      defaultBatchedWorkerConfig workersPerPool batchSize (\jobs callbacks -> void $ flakyBatch callbacks jobs)
  runWorkerTrial runM statsConn configs totalJobs durationUs

simpleWorkerTrial :: RunM SimpleM -> Connection -> Int -> Int -> Int -> Int -> BenchMode -> IO SteadyResult
simpleWorkerTrial runM statsConn =
  workerTrial runM statsConn $ \workersPerPool ->
    transactionalWorkerConfig workersPerPool (\(_conn :: Connection) job -> flakyGate (pure ()) job)

hasqlWorkerTrial :: RunM HasqlM -> Connection -> Int -> Int -> Int -> Int -> BenchMode -> IO SteadyResult
hasqlWorkerTrial runM statsConn =
  workerTrial runM statsConn $ \workersPerPool ->
    transactionalWorkerConfig workersPerPool (\(_conn :: Hasql.Connection) job -> flakyGate (pure ()) job)

orvilleWorkerTrial :: RunM OrvilleM -> Connection -> Int -> Int -> Int -> Int -> BenchMode -> IO SteadyResult
orvilleWorkerTrial runM statsConn =
  workerTrial runM statsConn $ \workersPerPool ->
    transactionalWorkerConfig workersPerPool (\job -> flakyGate (pure ()) job)

runWorkerTrial
  :: (HasRegistry m BenchRegistry)
  => RunM m -> Connection -> [WorkerConfig m BenchPayload] -> Int -> Int -> IO SteadyResult
runWorkerTrial runM statsConn configs totalJobs durationUs =
  captureWindow statsConn $ do
    start <- getCurrentTime
    race_
      (mapConcurrently_ (\config -> runM $ runWorkerPool config) configs)
      (threadDelay durationUs)
    end <- getCurrentTime
    remaining <- runM (HL.countJobs @BenchPayload)
    let processed = totalJobs - fromIntegral remaining :: Int
        elapsed = realToFrac (diffUTCTime end start) :: Double
    pure (fromIntegral processed / elapsed, processed)

-- | Shared measurement window: run workers and producers, warm up, then
-- measure one snapshot-bounded interval and report throughput.
runMeasuredWindow
  :: snap
  -> (Connection -> IO snap)
  -> (Connection -> IO ())
  -> (Connection -> IO [TriggerStats])
  -> [Text]
  -> Connection
  -> IORef Int
  -> [IO ()]
  -> Int
  -> IO (Double, Int, snap, snap, [TriggerStats])
runMeasuredWindow zeroSnap captureSnap resetTrg readTrg analyzeTables statsConn processedCounter threads durationUs = do
  startTime <- getCurrentTime
  startRef <- newIORef startTime
  endRef <- newIORef startTime
  snap0Ref <- newIORef zeroSnap
  snap1Ref <- newIORef zeroSnap
  trgRef <- newIORef []
  race_
    (mapConcurrently_ id threads)
    ( do
        threadDelay steadyStateWarmupUs
        writeIORef processedCounter 0
        -- Window start. Reset trigger stats and ANALYZE the tables. The WAL/churn
        -- snapshot follows the ANALYZE.
        resetTrg statsConn
        traverse_ (\table -> execute_ statsConn ("ANALYZE " <> benchSchema <> "." <> table)) analyzeTables
        captureSnap statsConn >>= writeIORef snap0Ref
        getCurrentTime >>= writeIORef startRef
        threadDelay durationUs
        getCurrentTime >>= writeIORef endRef
        captureSnap statsConn >>= writeIORef snap1Ref
        readTrg statsConn >>= writeIORef trgRef
    )
  processed <- readIORef processedCounter
  start <- readIORef startRef
  end <- readIORef endRef
  snap0 <- readIORef snap0Ref
  snap1 <- readIORef snap1Ref
  trg <- readIORef trgRef
  let elapsed = realToFrac (diffUTCTime end start) :: Double
  pure (fromIntegral processed / elapsed, processed, snap0, snap1, trg)

-- | Steady-state trial. Producers insert continuously while workers consume.
-- Workers increment a counter per job.
runSteadyStateTrial
  :: (HasRegistry m BenchRegistry)
  => RunM m
  -- ^ Runner for workers
  -> RunM SimpleM
  -- ^ Runner for producers (separate pool)
  -> Connection
  -- ^ Stats connection for WAL/churn snapshots, used on the timer thread
  -> [WorkerConfig m BenchPayload]
  -> IORef Int
  -- ^ Counter that handlers increment per job processed
  -> Int
  -- ^ Batch size for producer inserts
  -> Int
  -- ^ Number of concurrent producer threads
  -> Int
  -- ^ Delay between producer batches (microseconds), 0 for no delay
  -> QueueFlavor
  -> Int
  -- ^ Trial duration (microseconds)
  -> IO SteadyResult
runSteadyStateTrial runM producerRunM statsConn configs processedCounter producerBatchSize numProducers producerDelayUs flavor durationUs = do
  batchCounter <- newIORef (0 :: Int)
  let mkProducer producerId = do
        when (producerDelayUs > 0) $
          threadDelay (producerId * (producerDelayUs `div` numProducers))
        let go = do
              offset <- atomicModifyIORef' batchCounter (\count -> (count + producerBatchSize, count))
              now <- getCurrentTime
              let jobs = case flavor of
                    Ungrouped ->
                      [defaultJob (BenchBatch index) | index <- [1 .. producerBatchSize]]
                    Grouped numGroups ->
                      [ defaultGroupedJob (T.pack $ "g" <> show (((offset + index) `mod` numGroups) + 1)) (BenchBatch index)
                      | index <- [1 .. producerBatchSize]
                      ]
                    GroupedSkewed numGroups hotGroups ->
                      [ defaultGroupedJob (skewedGroupKey numGroups hotGroups (offset + index)) (BenchBatch index)
                      | index <- [1 .. producerBatchSize]
                      ]
                    Mixed numGroups ->
                      [ if even index
                          then defaultJob (BenchBatch index)
                          else defaultGroupedJob (T.pack $ "g" <> show (((offset + index) `mod` numGroups) + 1)) (BenchBatch index)
                      | index <- [1 .. producerBatchSize]
                      ]
                    GroupedBacklog numGroups ->
                      [backlogJob now numGroups (offset + index) | index <- [1 .. producerBatchSize]]
                    GroupedDormant numGroups ->
                      [dormantJob now numGroups (offset + index) | index <- [1 .. producerBatchSize]]
                    UngroupedDormant ->
                      [dormantUngroupedJob now (offset + index) | index <- [1 .. producerBatchSize]]
              producerRunM $ void $ HL.insertJobsBatch_ jobs
              when (producerDelayUs > 0) $ threadDelay producerDelayUs
              go
         in go
  (throughput, processed, snap0, snap1, trg) <-
    runMeasuredWindow
      zeroSnapshot
      captureDbSnapshot
      resetTriggerStats
      readTriggerStats
      ["bench_queue", "bench_queue_groups"]
      statsConn
      processedCounter
      ( map (\config -> runM $ runWorkerPool config) configs
          <> [mkProducer producerIndex | producerIndex <- [0 .. numProducers - 1]]
      )
      durationUs
  pure (mkSteadyResult throughput processed snap0 snap1 trg)

-- | A steady-state trial for one backend. @mkSingle@ builds that backend's single-job
-- config, counting each job it processes.
steadyStateTrial
  :: (HasRegistry m BenchRegistry, QueueOperation m BenchPayload)
  => RunM m
  -> RunM SimpleM
  -> Connection
  -> (Int -> IORef Int -> m (WorkerConfig m BenchPayload))
  -> Int
  -- ^ Trial duration (microseconds)
  -> Int
  -- ^ Pools
  -> Int
  -- ^ Workers per pool
  -> Int
  -- ^ Producer batch size
  -> BenchMode
  -> QueueFlavor
  -> Instrumentation
  -> IO SteadyResult
steadyStateTrial runM producerRunM statsConn mkSingle durationUs numPools workersPerPool producerBatchSize modeConfig flavor otel = do
  processedCounter <- newIORef (0 :: Int)
  configs <- benchConfigs runM numPools $ case modeConfig of
    BenchSingleJobMode -> mkSingle workersPerPool processedCounter
    BenchBatchedJobsMode batchSize -> defaultBatchedWorkerConfig workersPerPool batchSize $ \jobs callbacks -> do
      acked <- flakyBatch callbacks jobs
      countProcessedN processedCounter acked
  withInstrumentedPools otel configs $ \instrumented ->
    runSteadyStateTrial
      runM
      producerRunM
      statsConn
      instrumented
      processedCounter
      producerBatchSize
      10
      50_000
      flavor
      durationUs

-- | Count one processed job against a steady-state trial's counter.
countProcessed :: (MonadIO n) => IORef Int -> n ()
countProcessed = flip countProcessedN 1

-- | 'countProcessed' over a batch.
countProcessedN :: (MonadIO n) => IORef Int -> Int -> n ()
countProcessedN counter delta = liftIO $ atomicModifyIORef' counter (\count -> (count + delta, ()))

-- Setup

execute_ :: Connection -> Text -> IO ()
execute_ conn sql = void $ execute conn (fromString (T.unpack sql) :: Query) ()

setupSchema :: IO ()
setupSchema = do
  conn <- connectPostgreSQL benchConnStr
  execute_ conn $ "DROP SCHEMA IF EXISTS " <> benchSchema <> " CASCADE"
  res <- runMigrationsForRegistry (Proxy @BenchRegistry) benchConnStr benchSchema defaultMigrationConfig
  case res of
    MigrationSuccess -> pure ()
    MigrationError err -> die $ "Migration failed: " <> err
  close conn

cleanupData :: Connection -> IO ()
cleanupData conn = do
  execute_ conn "SET client_min_messages = WARNING"
  execute_
    conn
    ("TRUNCATE " <> benchSchema <> ".bench_queue, " <> benchSchema <> ".bench_queue_groups CASCADE")

-- | Truncate on a fresh connection that is closed afterwards.
cleanupFresh :: IO ()
cleanupFresh = do
  conn <- connectPostgreSQL benchConnStr
  cleanupData conn
  close conn

-- | Settle the database before a measurement group. Clears the dead-tuple debt
-- earlier suites left behind and flushes checkpoint work.
quietBenchDb :: IO ()
quietBenchDb = do
  conn <- connectPostgreSQL benchConnStr
  execute_ conn "SET client_min_messages = WARNING"
  tables <- PG.query conn "SELECT tablename FROM pg_tables WHERE schemaname = ?" (Only benchSchema)
  traverse_ (\(Only table) -> execute_ conn ("VACUUM ANALYZE " <> benchSchema <> "." <> table)) (tables :: [Only Text])
  execute_ conn "CHECKPOINT"
  close conn

-- | Defer an action to the first time the returned trigger runs.
once :: IO () -> IO (IO ())
once action = do
  done <- newIORef False
  pure $ do
    already <- atomicModifyIORef' done (\old -> (True, old))
    when (not already) action

-- | Truncate a gated queue's tables and the shared bucket/count tables.
cleanupGatedFresh :: Text -> IO ()
cleanupGatedFresh table = do
  conn <- connectPostgreSQL benchConnStr
  execute_ conn "SET client_min_messages = WARNING"
  execute_ conn $
    "TRUNCATE "
      <> benchSchema
      <> "."
      <> table
      <> ", "
      <> benchSchema
      <> "."
      <> table
      <> "_groups, "
      <> benchSchema
      <> ".arbiter_rate_limits, "
      <> benchSchema
      <> ".arbiter_concurrency CASCADE"
  close conn

-- | WAL plus churn on the shared count and bucket tables over one window.
data GatedResult = GatedResult
  { grThroughput :: Double
  , grWalPerJob :: Double
  , grCcHotPct :: Double
  , grCcUpdPerJob :: Double
  , grRlHotPct :: Double
  , grRlUpdPerJob :: Double
  , grTriggers :: [TriggerStats]
  }

data GatedSnap = GatedSnap Int64 TableSnap TableSnap

captureGatedSnap :: Connection -> IO GatedSnap
captureGatedSnap conn = do
  (wal, snapFor) <- captureTableSnaps conn ["arbiter_concurrency", "arbiter_rate_limits"]
  pure $ GatedSnap wal (snapFor "arbiter_concurrency") (snapFor "arbiter_rate_limits")

zeroGatedSnap :: GatedSnap
zeroGatedSnap = GatedSnap 0 (TableSnap 0 0 0 0) (TableSnap 0 0 0 0)

resetGatedTriggers :: Connection -> IO ()
resetGatedTriggers conn =
  void $
    PG.execute_
      conn
      "DO $$ BEGIN PERFORM pg_stat_reset_single_function_counters(oid) FROM pg_proc \
      \WHERE proname LIKE 'maintain_bench_%concurrency%' OR proname LIKE 'ensure_bench_%rate_limit%'; END $$"

readGatedTriggers :: Connection -> IO [TriggerStats]
readGatedTriggers conn =
  map (\(name, calls, total) -> TriggerStats name calls total (if calls > 0 then total / fromIntegral calls else 0))
    <$> PG.query_
      conn
      "SELECT funcname::text, calls, total_time FROM pg_stat_user_functions \
      \WHERE funcname LIKE 'maintain_bench_%concurrency%' OR funcname LIKE 'ensure_bench_%rate_limit%' \
      \ORDER BY funcname"

mkGatedResult :: Double -> Int -> GatedSnap -> GatedSnap -> [TriggerStats] -> GatedResult
mkGatedResult throughput processed (GatedSnap walBefore concBefore rateBefore) (GatedSnap walAfter concAfter rateAfter) triggers =
  GatedResult
    { grThroughput = throughput
    , grWalPerJob = fromIntegral (walAfter - walBefore) / jobs
    , grCcHotPct = hotPct concBefore concAfter
    , grCcUpdPerJob = fromIntegral (tnUpd concAfter - tnUpd concBefore) / jobs
    , grRlHotPct = hotPct rateBefore rateAfter
    , grRlUpdPerJob = fromIntegral (tnUpd rateAfter - tnUpd rateBefore) / jobs
    , grTriggers = triggers
    }
  where
    jobs = fromIntegral (max 1 processed) :: Double

formatGated :: [GatedResult] -> String
formatGated [] = "(no samples)"
formatGated results =
  formatStats "jobs/sec" (computeStats (map grThroughput results))
    <> "\n  "
    <> showFFloat (Just 0) (meanOf grWalPerJob) ""
    <> " B WAL/job | count HOT "
    <> showFFloat (Just 0) (meanOf grCcHotPct) ""
    <> "% ("
    <> showFFloat (Just 2) (meanOf grCcUpdPerJob) ""
    <> " upd/job) | bucket HOT "
    <> showFFloat (Just 0) (meanOf grRlHotPct) ""
    <> "% ("
    <> showFFloat (Just 2) (meanOf grRlUpdPerJob) ""
    <> " upd/job)\n"
    <> formatTriggerStats (grTriggers (last results))
  where
    meanOf :: (GatedResult -> Double) -> Double
    meanOf field = sum (map field results) / fromIntegral (length results)

multiTrialGated :: Int -> IO () -> IO GatedResult -> IO String
multiTrialGated trials setup measure = formatGated <$> replicateM trials (setup >> measure)

-- | One steady-state window over a gated queue: 10 producers insert, a 10-worker pool acks.
runGatedSteadyTrial
  :: ( EncodeJobResult (ResultOf m payload)
     , QueueOperation SimpleM payload
     , QueueOperation m payload
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     )
  => RunM m
  -> RunM SimpleM
  -> Connection
  -> WorkerConfig m payload
  -> IORef Int
  -> Text
  -> (Int -> JobWrite payload)
  -> Int
  -> IO GatedResult
runGatedSteadyTrial runM producerRunM statsConn cfg processedCounter table mkJob durationUs = do
  batchCounter <- newIORef (0 :: Int)
  let producer = do
        offset <- atomicModifyIORef' batchCounter (\count -> (count + 100, count))
        producerRunM $ void $ HL.insertJobsBatch_ [mkJob (offset + index) | index <- [1 .. 100]]
        producer
  (throughput, processed, snap0, snap1, trg) <-
    runMeasuredWindow
      zeroGatedSnap
      captureGatedSnap
      resetGatedTriggers
      readGatedTriggers
      [table, table <> "_groups"]
      statsConn
      processedCounter
      (runM (runWorkerPool cfg) : replicate 10 producer)
      durationUs
  pure (mkGatedResult throughput processed snap0 snap1 trg)

-- | What a payload must satisfy to drive the gated benches.
type GatedPayload payload =
  ( QueueOperation HasqlM payload
  , QueueOperation SimpleM payload
  , ResultOf HasqlM payload ~ ()
  )

hasqlGatedSteadyTrial
  :: forall payload
   . (GatedPayload payload)
  => RunM HasqlM -> RunM SimpleM -> Connection -> BenchMode -> Text -> (Int -> JobWrite payload) -> Int -> IO GatedResult
hasqlGatedSteadyTrial runM producerRunM statsConn mode table mkJob durationUs = do
  processedCounter <- newIORef (0 :: Int)
  cfg0 <- runM $ case mode of
    BenchSingleJobMode ->
      transactionalWorkerConfig 10 $ \(_conn :: Hasql.Connection) (_job :: JobRead payload) ->
        countProcessed processedCounter
    BenchBatchedJobsMode batchSize ->
      defaultBatchedWorkerConfig 10 batchSize $ \(jobs :: NonEmpty (JobRead payload)) callbacks -> do
        ackAll callbacks (toList jobs)
        countProcessedN processedCounter (length jobs)
  runGatedSteadyTrial runM producerRunM statsConn (benchTune cfg0) processedCounter table mkJob durationUs

-- | No gate / rate limit / concurrency / both, each ungrouped and grouped, in single
-- and batched mode.
gatingBenches
  :: IO ()
  -> ( forall payload
        . (GatedPayload payload)
       => BenchMode -> Text -> (Int -> JobWrite payload) -> Int -> IO GatedResult
     )
  -> [Benchmark]
gatingBenches settle trial =
  [ bgroup "ungrouped" (profiles (\ctor index -> defaultJob (ctor index)))
  , bgroup
      "grouped (5000 groups)"
      (profiles (\ctor index -> defaultGroupedJob (T.pack ("g" <> show ((index `mod` 5000) + 1))) (ctor index)))
  ]
  where
    profiles :: (forall payload. (Int -> payload) -> Int -> JobWrite payload) -> [Benchmark]
    profiles wrap =
      [ profile "no gate (baseline)" "bench_queue" (wrap BenchMessage)
      , profile "rate limit" "bench_rl_queue" (wrap BenchRl)
      , profile "concurrency" "bench_cc_queue" (wrap BenchCc)
      , profile "both" "bench_both_queue" (wrap BenchBoth)
      ]
    profile
      :: (GatedPayload payload)
      => String -> Text -> (Int -> JobWrite payload) -> Benchmark
    profile name table mkJob =
      bgroup
        name
        [ mode "single" BenchSingleJobMode
        , mode "batched (size 10)" (BenchBatchedJobsMode 10)
        ]
      where
        mode label benchMode =
          singleTest label
            $ ThroughputBench
            $ multiTrialGated trialCount (settle >> cleanupGatedFresh table) (trial benchMode table mkJob trialDurationUs)

setupQueue :: SimpleEnv BenchRegistry -> Int -> QueueFlavor -> IO ()
setupQueue simpleEnv totalJobs flavor = do
  conn <- connectPostgreSQL benchConnStr
  cleanupData conn
  now <- getCurrentTime

  let chunkSize = 50000
      mkJobs offset = case flavor of
        Ungrouped ->
          [defaultJob (BenchBatch index) | index <- [offset + 1 .. min (offset + chunkSize) totalJobs]]
        Grouped numGroups ->
          [ defaultGroupedJob (T.pack $ "g" <> show ((index `mod` numGroups) + 1)) (BenchBatch index)
          | index <- [offset + 1 .. min (offset + chunkSize) totalJobs]
          ]
        GroupedSkewed numGroups hotGroups ->
          [ defaultGroupedJob (skewedGroupKey numGroups hotGroups index) (BenchBatch index)
          | index <- [offset + 1 .. min (offset + chunkSize) totalJobs]
          ]
        Mixed numGroups ->
          [ if even index
              then defaultJob (BenchBatch index)
              else defaultGroupedJob (T.pack $ "g" <> show ((index `mod` numGroups) + 1)) (BenchBatch index)
          | index <- [offset + 1 .. min (offset + chunkSize) totalJobs]
          ]
        GroupedBacklog numGroups ->
          [backlogJob now numGroups index | index <- [offset + 1 .. min (offset + chunkSize) totalJobs]]
        GroupedDormant numGroups ->
          [dormantJob now numGroups index | index <- [offset + 1 .. min (offset + chunkSize) totalJobs]]
        UngroupedDormant ->
          [dormantUngroupedJob now index | index <- [offset + 1 .. min (offset + chunkSize) totalJobs]]
      go offset
        | offset >= totalJobs = pure ()
        | otherwise = do
            runSimpleDb simpleEnv $ void $ HL.insertJobsBatch_ (mkJobs offset)
            go (offset + chunkSize)
  -- Disable triggers for the bulk load, then rebuild the summary in one pass.
  execute_ conn ("ALTER TABLE " <> benchSchema <> ".bench_queue DISABLE TRIGGER USER")
  go 0
  execute_ conn ("ALTER TABLE " <> benchSchema <> ".bench_queue ENABLE TRIGGER USER")
  runSimpleDb simpleEnv $ void HL.refreshAllGroupsFully

  execute_ conn ("ANALYZE " <> benchSchema <> ".bench_queue")
  execute_ conn ("ANALYZE " <> benchSchema <> ".bench_queue_groups")
  close conn

-- Benchmark suites

main :: IO ()
main = do
  putStrLn "Setting up benchmark database schema..."
  setupSchema
  putStrLn "Schema ready. Creating environments..."

  let benchPoolConfig = PoolConfig {poolSize = 25, poolIdleTimeout = 60, poolStripes = Just 4}
  simpleEnv <- createSimpleEnvWithConfig (Proxy @BenchRegistry) benchConnStr benchSchema benchPoolConfig

  hasqlEnv <- createHasqlEnvWithConfig (Proxy @BenchRegistry) benchConnStr benchSchema benchPoolConfig

  let orvilleOptions = createOrvilleConnectionOptions benchConnStr benchPoolConfig
  orvillePool <- O.createConnectionPool orvilleOptions
  let orvilleState = O.newOrvilleState O.defaultErrorDetailLevel orvillePool

  statsConn <- connectPostgreSQL benchConnStr
  -- Suppress "index does not exist, skipping" NOTICEs.
  execute_ statsConn "SET client_min_messages = WARNING"
  -- Keep autovacuum off the shared gated tables.
  execute_ statsConn "ALTER TABLE arbiter.arbiter_concurrency SET (autovacuum_enabled = false)"
  execute_ statsConn "ALTER TABLE arbiter.arbiter_rate_limits SET (autovacuum_enabled = false)"
  [Only trackSetting] <- PG.query_ statsConn "SHOW track_functions"
  when (trackSetting /= ("all" :: Text))
    $ putStrLn
    $ "WARNING: track_functions = " <> T.unpack trackSetting <> " (set to 'all' in postgresql.conf for trigger stats)"

  putStrLn $
    "Running benchmarks ("
      <> show trialCount
      <> " trials x "
      <> show (trialDurationUs `div` 1_000_000)
      <> "s per trial)..."

  producerEnv <- createSimpleEnv (Proxy @BenchRegistry) benchConnStr benchSchema
  settleGated <- once quietBenchDb

  let simpleRun :: RunM SimpleM
      simpleRun = runSimpleDb simpleEnv

      producerRun :: RunM SimpleM
      producerRun = runSimpleDb producerEnv

      -- Prepared claims on, the recommended direct-connection setting.
      hasqlPreparedRun :: RunM HasqlM
      hasqlPreparedRun = runHasqlDb (setPreparedStatements True hasqlEnv)

      orvilleRun :: RunM OrvilleM
      orvilleRun action = runReaderT (unBenchOrville action) (benchSchema, orvilleState)

  defaultMain $
    map
      (localOption (mkTimeout benchTimeout))
      [ bgroup "Worker Throughput (simple)" $
          simpleWorkerBenches statsConn simpleEnv simpleRun
      , bgroup "Worker Throughput (hasql)" $
          hasqlWorkerBenches statsConn simpleEnv hasqlPreparedRun
      , bgroup "Group Cardinality and Skew (hasql)" $
          groupShapeBenches statsConn simpleEnv hasqlPreparedRun
      , bgroup "Worker Throughput (orville)" $
          orvilleWorkerBenches statsConn simpleEnv orvilleRun
      , bgroup "Steady-State Throughput (simple)" $
          steadyStateBenches $
            steadyStateTrial simpleRun producerRun statsConn $ \workers counter ->
              transactionalWorkerConfig workers $ \(_conn :: Connection) job ->
                flakyGate (countProcessed counter) job
      , bgroup "Steady-State Throughput (hasql)" $
          steadyStateBenches $
            steadyStateTrial hasqlPreparedRun producerRun statsConn $ \workers counter ->
              transactionalWorkerConfig workers $ \(_conn :: Hasql.Connection) job ->
                flakyGate (countProcessed counter) job
      , bgroup "Steady-State Throughput (orville)" $
          steadyStateBenches $
            steadyStateTrial orvilleRun producerRun statsConn $ \workers counter ->
              transactionalWorkerConfig workers $ \job -> flakyGate (countProcessed counter) job
      , bgroup "Gating Overhead (hasql)" $
          gatingBenches settleGated (hasqlGatedSteadyTrial hasqlPreparedRun producerRun statsConn)
      ]

_claimBenches :: SimpleEnv BenchRegistry -> Int -> [(String, QueueFlavor)] -> [Benchmark]
_claimBenches simpleEnv queueSize flavors =
  flip map flavors $ \(label, flavor) ->
    let mkBench name action =
          singleTest name
            $ ThroughputBench
            $ multiTrial
              trialCount
              (setupQueue simpleEnv queueSize flavor)
              (claimTrial (runSimpleDb simpleEnv) trialDurationUs action)
              "claims/sec"
     in bgroup
          label
          [ mkBench "claim 1" $
              not . null <$> (HL.claimNextVisibleJobs 1 60 :: SimpleDb BenchRegistry IO [JobRead BenchPayload])
          , mkBench "claim 20x10" $
              not . null <$> (HL.claimNextVisibleJobsBatched 10 20 60 :: SimpleDb BenchRegistry IO [NonEmpty (JobRead BenchPayload)])
          ]

claimTrial :: RunM m -> Int -> m Bool -> IO Double
claimTrial runM durationUs claimAction = do
  counter <- newIORef (0 :: Int)
  start <- getCurrentTime
  race_
    ( let go = do
            gotWork <- runM claimAction
            modifyIORef' counter (+ 1)
            when gotWork go
       in go
    )
    (threadDelay durationUs)
  end <- getCurrentTime
  count <- readIORef counter
  let elapsed = realToFrac (diffUTCTime end start) :: Double
  pure (fromIntegral count / elapsed)

simpleWorkerBenches :: Connection -> SimpleEnv BenchRegistry -> RunM SimpleM -> [Benchmark]
simpleWorkerBenches statsConn simpleEnv runM =
  mkWorkerBenches
    simpleEnv
    (\jobs duration pools workers mode -> simpleWorkerTrial runM statsConn jobs duration pools workers mode)

hasqlWorkerBenches :: Connection -> SimpleEnv BenchRegistry -> RunM HasqlM -> [Benchmark]
hasqlWorkerBenches statsConn simpleEnv runM =
  mkWorkerBenches
    simpleEnv
    (\jobs duration pools workers mode -> hasqlWorkerTrial runM statsConn jobs duration pools workers mode)

orvilleWorkerBenches :: Connection -> SimpleEnv BenchRegistry -> RunM OrvilleM -> [Benchmark]
orvilleWorkerBenches statsConn simpleEnv runM =
  mkWorkerBenches
    simpleEnv
    (\jobs duration pools workers mode -> orvilleWorkerTrial runM statsConn jobs duration pools workers mode)

-- | Measure the group-maintenance cost as group size and key skew increase.
-- A smaller trial set keeps this diagnostic suite practical.
groupShapeBenches :: Connection -> SimpleEnv BenchRegistry -> RunM HasqlM -> [Benchmark]
groupShapeBenches statsConn simpleEnv runM = map profile shapes
  where
    totalJobs = 300000
    shapeTrials = 3
    shapeDurationUs = 5_000_000
    trial = hasqlWorkerTrial runM statsConn
    cardinality jobsPerGroup =
      ( show jobsPerGroup <> " jobs/group"
      , Grouped (max 1 (totalJobs `div` jobsPerGroup))
      )
    shapes =
      map cardinality [1, 10, 100, 1000, 10000]
        <> [("80/20 skew, 1000 groups, 10 hot", GroupedSkewed 1000 10)]
    profile (label, flavor) =
      bgroup
        label
        [ measured "single job mode" BenchSingleJobMode
        , measured "batched mode (size 10)" (BenchBatchedJobsMode 10)
        ]
      where
        measured name mode =
          singleTest name
            $ ThroughputBench
            $ multiTrialSteady
              shapeTrials
              (setupQueue simpleEnv totalJobs flavor)
              (trial totalJobs shapeDurationUs 4 10 mode)

mkWorkerBenches
  :: SimpleEnv BenchRegistry
  -> (Int -> Int -> Int -> Int -> BenchMode -> IO SteadyResult)
  -> [Benchmark]
mkWorkerBenches simpleEnv trial =
  [ bgroup (poolLabel pools <> " x " <> show workers <> " workers") $
      mkWorkerFlavorBenches simpleEnv trial pools workers defaultFlavors
  | (pools, workers) <-
      [ -- (1, 5)
        (4, 10)
        -- , (1, 20)
        -- , (4, 20)
      ]
  ]
  where
    poolLabel :: Int -> String
    poolLabel 1 = "1 pool"
    poolLabel count = show count <> " pools"
    defaultFlavors :: [(String, QueueFlavor)]
    defaultFlavors =
      [ ("ungrouped", Ungrouped)
      , ("ungrouped (dormant)", UngroupedDormant)
      , -- , ("10 groups", Grouped 10)
        ("50000 groups", Grouped 50000)
      , ("50000 groups (scheduled + backoff)", GroupedBacklog 50000)
      , ("50000 groups (dormant)", GroupedDormant 50000)
      -- , ("200000 groups", Grouped 200000)
      -- , ("mixed (50000 groups + ungrouped)", Mixed 50000)
      ]

mkWorkerFlavorBenches
  :: SimpleEnv BenchRegistry
  -> (Int -> Int -> Int -> Int -> BenchMode -> IO SteadyResult)
  -> Int
  -> Int
  -> [(String, QueueFlavor)]
  -> [Benchmark]
mkWorkerFlavorBenches simpleEnv trial pools workers flavors =
  let numJobs = 1000000
   in flip map flavors $ \(label, flavor) ->
        let mkBench name mode =
              singleTest name
                $ ThroughputBench
                $ multiTrialSteady
                  trialCount
                  (setupQueue simpleEnv numJobs flavor)
                  (trial numJobs trialDurationUs pools workers mode)
         in bgroup
              label
              [ mkBench "single job mode" BenchSingleJobMode
              , mkBench "batched mode (size 10)" (BenchBatchedJobsMode 10)
              ]

steadyStateBenches
  :: (Int -> Int -> Int -> Int -> BenchMode -> QueueFlavor -> Instrumentation -> IO SteadyResult)
  -> [Benchmark]
steadyStateBenches trial =
  [ bgroup "4 pools x 10 workers" $
      flip map steadyStateFlavors $ \(label, flavor) ->
        let producerBatch = 100
            mkBench name mode otel =
              singleTest name
                $ ThroughputBench
                $ multiTrialSteady
                  trialCount
                  cleanupFresh
                  (trial trialDurationUs 4 10 producerBatch mode flavor otel)
         in bgroup label $
              [ mkBench "single job mode" BenchSingleJobMode Plain
              , mkBench "batched mode (size 10)" (BenchBatchedJobsMode 10) Plain
              ]
                -- One flavor carries the OTel comparison. The cost is per job.
                <> [mkBench "single job mode (instrumented)" BenchSingleJobMode Instrumented | flavor == Ungrouped]
  ]
  where
    steadyStateFlavors :: [(String, QueueFlavor)]
    steadyStateFlavors =
      [ ("ungrouped", Ungrouped)
      , ("5000 groups", Grouped 5000)
      , ("5000 groups (scheduled + backoff)", GroupedBacklog 5000)
      ]

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Main (main) where

import Arbiter.Core.Concurrency.Spec (HasConcurrency (..), concurrencyBy, concurrencyPool)
import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.HasArbiterSchema (ArbiterSchema, HasArbiterSchema (..), ResultOf)
import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types
  ( JobRead
  , JobWrite
  , attempts
  , defaultGroupedJob
  , defaultJob
  , notVisibleUntil
  , payload
  )
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Core.QueueRegistry (QueueSpec (..))
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
import Arbiter.Simple (SimpleDb, SimpleEnv, createSimpleEnv, createSimpleEnvWithConfig, runSimpleDb)
import Arbiter.Worker
  ( BatchCallbacks (..)
  , WorkerConfig (..)
  , defaultBatchedWorkerConfig
  , runWorkerPool
  , silentLogConfig
  , transactionalWorkerConfig
  )
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently_, race_)
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
import Orville.PostgreSQL qualified as O
import Orville.PostgreSQL.UnliftIO qualified as O
import System.Exit (die)
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

-- | Per-test timeout. tasty-bench 0.5 defaults to 100s, too tight for these
-- multi-trial tests (trialCount trials plus per-trial preloads).
benchTimeout :: Integer
benchTimeout = fromIntegral trialCount * fromIntegral trialDurationUs * 40

steadyStateWarmupUs :: Int
steadyStateWarmupUs = 2_000_000

data BenchPayload
  = BenchMessage Int
  | BenchBatch Int
  | -- | Fails on its first attempt, then succeeds, to populate the backoff backlog.
    BenchFlaky Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Gated payloads for the overhead benches. Limits are huge, so gates never throttle.
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
gateKey i = T.pack (show (i `mod` gateKeyCount))

-- | A 'BenchBoth' concurrency key decorrelated from 'gateKey'.
gateKey2 :: Int -> Text
gateKey2 i = T.pack (show (i `mod` max 1 (gateKeyCount - 1)))

instance HasRateLimit BenchRl where
  rateLimitFor = limitBy (tokenBucket "blr" 1.0e9 1) (\(BenchRl i) -> gateKey i)

instance HasConcurrency BenchCc where
  concurrencyFor = concurrencyBy (concurrencyPool "blc" 1000000) (\(BenchCc i) -> gateKey i)

instance HasRateLimit BenchBoth where
  rateLimitFor = limitBy (tokenBucket "bbr" 1.0e9 1) (\(BenchBoth i) -> gateKey i)

instance HasConcurrency BenchBoth where
  concurrencyFor = concurrencyBy (concurrencyPool "bbc" 1000000) (\(BenchBoth i) -> gateKey2 i)

type BenchRegistry =
  '[ Queue "bench_queue" BenchPayload
   , Queue "bench_rl_queue" BenchRl
   , Queue "bench_cc_queue" BenchCc
   , Queue "bench_both_queue" BenchBoth
   ]

data QueueFlavor
  = Ungrouped
  | Grouped Int
  | Mixed Int
  | -- | Grouped, with a fraction of jobs scheduled into the near future and a
    -- fraction that fail once into backoff, exercising the next_due and
    -- in_flight_until claim sources alongside the ready window.
    GroupedBacklog Int
  | -- | Grouped, half the backlog scheduled far into the future (parked, never due
    -- in-trial) while the rest is ready. Measures whether a standing scheduled
    -- backlog slows the claim for the ready work in those same groups.
    GroupedDormant Int
  | -- | Ungrouped 'GroupedDormant': half the backlog parked 30 days out, half
    -- ready, exercising the ungrouped ready/due index split.
    UngroupedDormant

data BenchMode
  = BenchSingleJobMode
  | BenchBatchedJobsMode Int

-- | One job in a 'GroupedBacklog' queue, selected by index: roughly a fifth are
-- flaky (fail once into backoff), a fifth are scheduled into the near future,
-- the rest are ready now.
backlogJob :: UTCTime -> Int -> Int -> JobWrite BenchPayload
backlogJob now numGroups i =
  let gk = T.pack $ "g" <> show ((i `mod` numGroups) + 1)
   in case i `mod` 5 of
        0 -> defaultGroupedJob gk (BenchFlaky i)
        1 -> (defaultGroupedJob gk (BenchBatch i)) {notVisibleUntil = Just (addUTCTime (scheduledDelay i) now)}
        _ -> defaultGroupedJob gk (BenchBatch i)

-- | Spread scheduled jobs across the first few seconds so they come due during
-- the trial rather than all at once.
scheduledDelay :: Int -> NominalDiffTime
scheduledDelay i = realToFrac (0.5 + fromIntegral (i `mod` 7) * 0.4 :: Double)

-- | One job in a 'GroupedDormant' queue. Each group alternates ready jobs with
-- jobs parked 30 days out, so the claim must skip a standing scheduled backlog
-- to reach the ready work.
dormantJob :: UTCTime -> Int -> Int -> JobWrite BenchPayload
dormantJob now numGroups i =
  let gk = T.pack $ "g" <> show ((i `mod` numGroups) + 1)
   in if odd (i `div` numGroups)
        then (defaultGroupedJob gk (BenchBatch i)) {notVisibleUntil = Just (addUTCTime (30 * 86400) now)}
        else defaultGroupedJob gk (BenchBatch i)

-- | Ungrouped 'dormantJob': alternate ready jobs with jobs parked 30 days out.
dormantUngroupedJob :: UTCTime -> Int -> JobWrite BenchPayload
dormantUngroupedJob now i =
  if odd i
    then (defaultJob (BenchBatch i)) {notVisibleUntil = Just (addUTCTime (30 * 86400) now)}
    else defaultJob (BenchBatch i)

-- | A flaky job on its first attempt. The dispatcher increments attempts at
-- claim, so the retry lands on attempts >= 2 and succeeds.
isFlakyFirst :: JobRead BenchPayload -> Bool
isFlakyFirst job = case payload job of
  BenchFlaky _ -> attempts job <= 1
  _ -> False

-- | Single-job handler body: fail flaky-first jobs into backoff, else ack.
flakyGate :: (MonadIO m) => m () -> JobRead BenchPayload -> m ()
flakyGate onAck job
  | isFlakyFirst job = throwRetryable "bench-induced backoff"
  | otherwise = onAck

-- | Batched handler body: failRetry the flaky-first jobs, ack the rest, and
-- return the acked count for throughput accounting.
flakyBatch
  :: (Monad m, ResultOf m BenchPayload ~ ()) => BatchCallbacks m BenchPayload -> NonEmpty (JobRead BenchPayload) -> m Int
flakyBatch cb jobs = do
  let (toFail, toAck) = partition isFlakyFirst (toList jobs)
  traverse_ (\job -> failRetry cb job "bench-induced backoff") toFail
  ackAll cb toAck
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
computeStats xs = BenchStats {statsMean = mean, statsStdev = sd, statsSamples = n}
  where
    n = length xs
    mean = sum xs / fromIntegral n
    sd
      | n > 1 = sqrt (sum [(x - mean) * (x - mean) | x <- xs] / fromIntegral (n - 1))
      | otherwise = 0

formatStats :: String -> BenchStats -> String
formatStats unit s =
  show (round (statsMean s) :: Int)
    <> " +/- "
    <> show (round (statsStdev s) :: Int)
    <> " "
    <> unit
    <> " ("
    <> showFFloat (Just 1) (if statsMean s > 0 then statsStdev s / statsMean s * 100 else 0) ""
    <> "%, n="
    <> show (statsSamples s)
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
      ( \s ->
          "  "
            <> T.unpack (tsFuncName s)
            <> ": "
            <> show (tsCalls s)
            <> " calls, "
            <> showFFloat (Just 1) (tsTotalTimeMs s) ""
            <> "ms total, "
            <> showFFloat (Just 3) (tsMeanTimeMs s) ""
            <> "ms/call"
      )
      stats

multiTrial :: Int -> IO () -> IO Double -> String -> IO String
multiTrial n setup measure unit = do
  samples <- replicateM n (setup >> measure)
  pure $ formatStats unit (computeStats samples)

-- ---------------------------------------------------------------------------
-- Write-amplification / autovacuum sampling
-- ---------------------------------------------------------------------------

-- | Per-table cumulative counters from pg_stat_user_tables.
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
hotPct a b =
  let du = fromIntegral (tnUpd b - tnUpd a) :: Double
      dh = fromIntegral (tnHot b - tnHot a) :: Double
   in if du > 0 then dh / du * 100 else 0

-- | A table's snapshot row by relname from a pg_stat_user_tables result.
snapRow :: [(Text, Int64, Int64, Int64, Int64)] -> Text -> TableSnap
snapRow rows name =
  case find (\(rn, _, _, _, _) -> rn == name) rows of
    Just (_, u, h, d, av) -> TableSnap u h d av
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
mkSteadyResult throughput processed s0 s1 trg =
  SteadyResult
    { srThroughput = throughput
    , srWalPerJob = fromIntegral (dsWal s1 - dsWal s0) / jobs
    , srQueueHotPct = hotPct (dsQueue s0) (dsQueue s1)
    , srQueueUpdPerJob = fromIntegral (tnUpd (dsQueue s1) - tnUpd (dsQueue s0)) / jobs
    , srQueueDead = tnDead (dsQueue s1)
    , srQueueDeadPerK = fromIntegral (tnDead (dsQueue s1)) / jobs * 1000
    , srQueueAutovac = tnAutovac (dsQueue s1) - tnAutovac (dsQueue s0)
    , srGroupsHotPct = hotPct (dsGroups s0) (dsGroups s1)
    , srGroupsDead = tnDead (dsGroups s1)
    , srGroupsDeadPerK = fromIntegral (tnDead (dsGroups s1)) / jobs * 1000
    , srGroupsAutovac = tnAutovac (dsGroups s1) - tnAutovac (dsGroups s0)
    , srTriggers = trg
    }
  where
    jobs = fromIntegral (max 1 processed) :: Double

formatSteady :: [SteadyResult] -> String
formatSteady [] = "(no samples)"
formatSteady rs =
  formatStats "jobs/sec" (computeStats (map srThroughput rs))
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
    <> formatTriggerStats (srTriggers (last rs))
  where
    meanOf :: (SteadyResult -> Double) -> Double
    meanOf f = sum (map f rs) / fromIntegral (length rs)

multiTrialSteady :: Int -> IO () -> IO SteadyResult -> IO String
multiTrialSteady n setup measure = do
  results <- replicateM n (setup >> measure)
  pure $ formatSteady results

-- | Bracket an action returning (throughput, jobs processed) with trigger-stat
-- reset and WAL/churn snapshots, yielding a 'SteadyResult'. For trials whose
-- whole duration is the measurement window (no warmup) - i.e. the worker benches.
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
  localOrvilleState f (BenchOrville action) = BenchOrville $ ReaderT $ \(s, os) ->
    runReaderT action (s, f os)

instance O.MonadOrvilleControl BenchOrville where
  liftWithConnection = O.liftWithConnectionViaUnliftIO
  liftCatch = O.liftCatchViaUnliftIO
  liftMask = O.liftMaskViaUnliftIO

instance HasArbiterSchema BenchOrville where
  type RegistryOf BenchOrville = BenchRegistry

  getSchema = BenchOrville $ asks fst

instance MonadArbiter BenchOrville where
  type Handler BenchOrville jobs result = jobs -> BenchOrville result
  executeQuery = orvilleExecuteQuery
  executeStatement = orvilleExecuteStatement
  withDbTransaction = orvilleWithDbTransaction
  runHandlerWithConnection = orvilleRunHandlerWithConnection
  getListener = pure Nothing

type OrvilleM = BenchOrville

simpleWorkerTrial :: RunM SimpleM -> Connection -> Int -> Int -> Int -> Int -> BenchMode -> IO SteadyResult
simpleWorkerTrial runM statsConn totalJobs durationUs numPools workersPerPool modeConfig = do
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <-
        runM $ transactionalWorkerConfig workersPerPool (\(_conn :: Connection) job -> flakyGate (pure ()) job)
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <- runM $ defaultBatchedWorkerConfig workersPerPool batchSize (\jobs cb -> void $ flakyBatch cb jobs)
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
  runWorkerTrial runM statsConn configs totalJobs durationUs

hasqlWorkerTrial :: RunM HasqlM -> Connection -> Int -> Int -> Int -> Int -> BenchMode -> IO SteadyResult
hasqlWorkerTrial runM statsConn totalJobs durationUs numPools workersPerPool modeConfig = do
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <-
        runM $
          transactionalWorkerConfig workersPerPool (\(_conn :: Hasql.Connection) job -> flakyGate (pure ()) job)
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <-
        runM $ defaultBatchedWorkerConfig workersPerPool batchSize (\jobs cb -> void $ flakyBatch cb jobs)
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
  runWorkerTrial runM statsConn configs totalJobs durationUs

orvilleWorkerTrial :: RunM OrvilleM -> Connection -> Int -> Int -> Int -> Int -> BenchMode -> IO SteadyResult
orvilleWorkerTrial runM statsConn totalJobs durationUs numPools workersPerPool modeConfig = do
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ transactionalWorkerConfig workersPerPool (\job -> flakyGate (pure ()) job)
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <- runM $ defaultBatchedWorkerConfig workersPerPool batchSize (\jobs cb -> void $ flakyBatch cb jobs)
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
  runWorkerTrial runM statsConn configs totalJobs durationUs

runWorkerTrial
  :: (ArbiterSchema m BenchRegistry, MonadArbiter m, MonadUnliftIO m)
  => RunM m -> Connection -> [WorkerConfig m BenchPayload] -> Int -> Int -> IO SteadyResult
runWorkerTrial runM statsConn configs totalJobs durationUs =
  captureWindow statsConn $ do
    start <- getCurrentTime
    race_
      (mapConcurrently_ (\c -> runM $ runWorkerPool c) configs)
      (threadDelay durationUs)
    end <- getCurrentTime
    remaining <- runM (HL.countJobs @_ @BenchRegistry @BenchPayload)
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
  t0 <- getCurrentTime
  startRef <- newIORef t0
  endRef <- newIORef t0
  snap0Ref <- newIORef zeroSnap
  snap1Ref <- newIORef zeroSnap
  trgRef <- newIORef []
  race_
    (mapConcurrently_ id threads)
    ( do
        threadDelay steadyStateWarmupUs
        writeIORef processedCounter 0
        -- Window start: reset trigger stats and ANALYZE so the planner sees the
        -- steady-state depth, then snapshot WAL/churn. ANALYZE WAL is excluded
        -- because the snapshot is taken after it.
        resetTrg statsConn
        traverse_ (\t -> execute_ statsConn ("ANALYZE " <> benchSchema <> "." <> t)) analyzeTables
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
-- Workers increment a counter per job, decoupling throughput from queue
-- depth at trial boundaries.
runSteadyStateTrial
  :: (ArbiterSchema m BenchRegistry, MonadArbiter m, MonadUnliftIO m)
  => RunM m
  -- ^ Runner for workers
  -> RunM SimpleM
  -- ^ Runner for producers (separate pool)
  -> Connection
  -- ^ Stats connection for WAL/churn snapshots (used only on the timer thread)
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
              offset <- atomicModifyIORef' batchCounter (\n -> (n + producerBatchSize, n))
              now <- getCurrentTime
              let jobs = case flavor of
                    Ungrouped ->
                      [defaultJob (BenchBatch i) | i <- [1 .. producerBatchSize]]
                    Grouped numGroups ->
                      [ defaultGroupedJob (T.pack $ "g" <> show (((offset + i) `mod` numGroups) + 1)) (BenchBatch i)
                      | i <- [1 .. producerBatchSize]
                      ]
                    Mixed numGroups ->
                      [ if even i
                          then defaultJob (BenchBatch i)
                          else defaultGroupedJob (T.pack $ "g" <> show (((offset + i) `mod` numGroups) + 1)) (BenchBatch i)
                      | i <- [1 .. producerBatchSize]
                      ]
                    GroupedBacklog numGroups ->
                      [backlogJob now numGroups (offset + i) | i <- [1 .. producerBatchSize]]
                    GroupedDormant numGroups ->
                      [dormantJob now numGroups (offset + i) | i <- [1 .. producerBatchSize]]
                    UngroupedDormant ->
                      [dormantUngroupedJob now (offset + i) | i <- [1 .. producerBatchSize]]
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
      (map (\c -> runM $ runWorkerPool c) configs <> [mkProducer i | i <- [0 .. numProducers - 1]])
      durationUs
  pure (mkSteadyResult throughput processed snap0 snap1 trg)

simpleSteadyStateTrial
  :: RunM SimpleM -> RunM SimpleM -> Connection -> Int -> Int -> Int -> Int -> BenchMode -> QueueFlavor -> IO SteadyResult
simpleSteadyStateTrial runM producerRunM statsConn durationUs numPools workersPerPool producerBatchSize modeConfig flavor = do
  processedCounter <- newIORef (0 :: Int)
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ transactionalWorkerConfig workersPerPool $ \(_conn :: Connection) job ->
        flakyGate (liftIO $ atomicModifyIORef' processedCounter (\n -> (n + 1, ()))) job
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <- runM $ defaultBatchedWorkerConfig workersPerPool batchSize $ \jobs cb -> do
        acked <- flakyBatch cb jobs
        liftIO $ atomicModifyIORef' processedCounter (\n -> (n + acked, ()))
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
  runSteadyStateTrial runM producerRunM statsConn configs processedCounter producerBatchSize 10 50_000 flavor durationUs

hasqlSteadyStateTrial
  :: RunM HasqlM -> RunM SimpleM -> Connection -> Int -> Int -> Int -> Int -> BenchMode -> QueueFlavor -> IO SteadyResult
hasqlSteadyStateTrial runM producerRunM statsConn durationUs numPools workersPerPool producerBatchSize modeConfig flavor = do
  processedCounter <- newIORef (0 :: Int)
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ transactionalWorkerConfig workersPerPool $ \(_conn :: Hasql.Connection) job ->
        flakyGate (liftIO $ atomicModifyIORef' processedCounter (\n -> (n + 1, ()))) job
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <- runM $ defaultBatchedWorkerConfig workersPerPool batchSize $ \jobs cb -> do
        acked <- flakyBatch cb jobs
        liftIO $ atomicModifyIORef' processedCounter (\n -> (n + acked, ()))
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
  runSteadyStateTrial runM producerRunM statsConn configs processedCounter producerBatchSize 10 50_000 flavor durationUs

orvilleSteadyStateTrial
  :: RunM OrvilleM -> RunM SimpleM -> Connection -> Int -> Int -> Int -> Int -> BenchMode -> QueueFlavor -> IO SteadyResult
orvilleSteadyStateTrial runM producerRunM statsConn durationUs numPools workersPerPool producerBatchSize modeConfig flavor = do
  processedCounter <- newIORef (0 :: Int)
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ transactionalWorkerConfig workersPerPool $ \job ->
        flakyGate (liftIO $ atomicModifyIORef' processedCounter (\n -> (n + 1, ()))) job
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <- runM $ defaultBatchedWorkerConfig workersPerPool batchSize $ \jobs cb -> do
        acked <- flakyBatch cb jobs
        liftIO $ atomicModifyIORef' processedCounter (\n -> (n + acked, ()))
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
  runSteadyStateTrial runM producerRunM statsConn configs processedCounter producerBatchSize 10 50_000 flavor durationUs

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

-- | Truncate on a fresh connection that is closed afterwards, so the per-trial
-- steady-state setup does not leak a connection per trial.
cleanupFresh :: IO ()
cleanupFresh = do
  conn <- connectPostgreSQL benchConnStr
  cleanupData conn
  close conn

-- | Settle the database before a measurement group: clear the dead-tuple debt
-- earlier suites left behind and flush checkpoint work, so neither runs mid-window.
quietBenchDb :: IO ()
quietBenchDb = do
  conn <- connectPostgreSQL benchConnStr
  execute_ conn "SET client_min_messages = WARNING"
  tables <- PG.query conn "SELECT tablename FROM pg_tables WHERE schemaname = ?" (Only benchSchema)
  traverse_ (\(Only t) -> execute_ conn ("VACUUM ANALYZE " <> benchSchema <> "." <> t)) (tables :: [Only Text])
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
mkGatedResult throughput processed (GatedSnap w0 cc0 rl0) (GatedSnap w1 cc1 rl1) trg =
  GatedResult
    { grThroughput = throughput
    , grWalPerJob = fromIntegral (w1 - w0) / jobs
    , grCcHotPct = hotPct cc0 cc1
    , grCcUpdPerJob = fromIntegral (tnUpd cc1 - tnUpd cc0) / jobs
    , grRlHotPct = hotPct rl0 rl1
    , grRlUpdPerJob = fromIntegral (tnUpd rl1 - tnUpd rl0) / jobs
    , grTriggers = trg
    }
  where
    jobs = fromIntegral (max 1 processed) :: Double

formatGated :: [GatedResult] -> String
formatGated [] = "(no samples)"
formatGated rs =
  formatStats "jobs/sec" (computeStats (map grThroughput rs))
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
    <> formatTriggerStats (grTriggers (last rs))
  where
    meanOf :: (GatedResult -> Double) -> Double
    meanOf f = sum (map f rs) / fromIntegral (length rs)

multiTrialGated :: Int -> IO () -> IO GatedResult -> IO String
multiTrialGated n setup measure = formatGated <$> replicateM n (setup >> measure)

-- | One steady-state window over a gated queue: 10 producers insert, a 10-worker pool acks.
runGatedSteadyTrial
  :: ( MonadUnliftIO m
     , QueueOperation SimpleM BenchRegistry payload
     , QueueOperation m BenchRegistry payload
     , ResultOf m payload ~ ()
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
        offset <- atomicModifyIORef' batchCounter (\n -> (n + 100, n))
        producerRunM $ void $ HL.insertJobsBatch_ [mkJob (offset + i) | i <- [1 .. 100]]
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

hasqlGatedSteadyTrial
  :: forall payload
   . ( QueueOperation HasqlM BenchRegistry payload
     , QueueOperation SimpleM BenchRegistry payload
     , ResultOf HasqlM payload ~ ()
     )
  => RunM HasqlM -> RunM SimpleM -> Connection -> BenchMode -> Text -> (Int -> JobWrite payload) -> Int -> IO GatedResult
hasqlGatedSteadyTrial runM producerRunM statsConn mode table mkJob durationUs = do
  processedCounter <- newIORef (0 :: Int)
  cfg0 <- runM $ case mode of
    BenchSingleJobMode ->
      transactionalWorkerConfig 10 $ \(_conn :: Hasql.Connection) (_job :: JobRead payload) ->
        liftIO (atomicModifyIORef' processedCounter (\n -> (n + 1, ())))
    BenchBatchedJobsMode batchSize ->
      defaultBatchedWorkerConfig 10 batchSize $ \(jobs :: NonEmpty (JobRead payload)) cb -> do
        ackAll cb (toList jobs)
        liftIO (atomicModifyIORef' processedCounter (\n -> (n + length jobs, ())))
  let cfg = cfg0 {pollInterval = 0.1, logConfig = silentLogConfig}
  runGatedSteadyTrial runM producerRunM statsConn cfg processedCounter table mkJob durationUs

-- | No gate / rate limit / concurrency / both, each ungrouped and grouped, in single
-- and batched mode.
gatingBenches
  :: IO ()
  -> ( forall payload
        . ( QueueOperation HasqlM BenchRegistry payload
          , QueueOperation SimpleM BenchRegistry payload
          , ResultOf HasqlM payload ~ ()
          )
       => BenchMode -> Text -> (Int -> JobWrite payload) -> Int -> IO GatedResult
     )
  -> [Benchmark]
gatingBenches settle trial =
  [ bgroup "ungrouped" (profiles (\ctor i -> defaultJob (ctor i)))
  , bgroup
      "grouped (5000 groups)"
      (profiles (\ctor i -> defaultGroupedJob (T.pack ("g" <> show ((i `mod` 5000) + 1))) (ctor i)))
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
      :: ( QueueOperation HasqlM BenchRegistry payload
         , QueueOperation SimpleM BenchRegistry payload
         , ResultOf HasqlM payload ~ ()
         )
      => String -> Text -> (Int -> JobWrite payload) -> Benchmark
    profile name table mkJob =
      bgroup
        name
        [ mode "single" BenchSingleJobMode
        , mode "batched (size 10)" (BenchBatchedJobsMode 10)
        ]
      where
        mode label m =
          singleTest label $
            ThroughputBench $
              multiTrialGated trialCount (settle >> cleanupGatedFresh table) (trial m table mkJob trialDurationUs)

setupQueue :: SimpleEnv BenchRegistry -> Int -> QueueFlavor -> IO ()
setupQueue simpleEnv totalJobs flavor = do
  conn <- connectPostgreSQL benchConnStr
  cleanupData conn
  now <- getCurrentTime

  let chunkSize = 50000
      mkJobs offset = case flavor of
        Ungrouped ->
          [defaultJob (BenchBatch i) | i <- [offset + 1 .. min (offset + chunkSize) totalJobs]]
        Grouped numGroups ->
          [ defaultGroupedJob (T.pack $ "g" <> show ((i `mod` numGroups) + 1)) (BenchBatch i)
          | i <- [offset + 1 .. min (offset + chunkSize) totalJobs]
          ]
        Mixed numGroups ->
          [ if even i
              then defaultJob (BenchBatch i)
              else defaultGroupedJob (T.pack $ "g" <> show ((i `mod` numGroups) + 1)) (BenchBatch i)
          | i <- [offset + 1 .. min (offset + chunkSize) totalJobs]
          ]
        GroupedBacklog numGroups ->
          [backlogJob now numGroups i | i <- [offset + 1 .. min (offset + chunkSize) totalJobs]]
        GroupedDormant numGroups ->
          [dormantJob now numGroups i | i <- [offset + 1 .. min (offset + chunkSize) totalJobs]]
        UngroupedDormant ->
          [dormantUngroupedJob now i | i <- [offset + 1 .. min (offset + chunkSize) totalJobs]]
      go offset
        | offset >= totalJobs = pure ()
        | otherwise = do
            runSimpleDb simpleEnv $ void $ HL.insertJobsBatch_ (mkJobs offset)
            go (offset + chunkSize)
  -- Disable triggers for the bulk load, then rebuild the summary in one pass.
  execute_ conn ("ALTER TABLE " <> benchSchema <> ".bench_queue DISABLE TRIGGER USER")
  go 0
  execute_ conn ("ALTER TABLE " <> benchSchema <> ".bench_queue ENABLE TRIGGER USER")
  runSimpleDb simpleEnv $ void $ HL.refreshAllGroups @_ @BenchRegistry

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
  -- Suppress "index does not exist, skipping" NOTICEs from applyIndexProfile.
  execute_ statsConn "SET client_min_messages = WARNING"
  -- Keep autovacuum off the shared gated tables so a mid-window pass is not measured.
  execute_ statsConn "ALTER TABLE arbiter.arbiter_concurrency SET (autovacuum_enabled = false)"
  execute_ statsConn "ALTER TABLE arbiter.arbiter_rate_limits SET (autovacuum_enabled = false)"
  [Only trackSetting] <- PG.query_ statsConn "SHOW track_functions"
  when (trackSetting /= ("all" :: Text)) $
    putStrLn $
      "WARNING: track_functions = " <> T.unpack trackSetting <> " (set to 'all' in postgresql.conf for trigger stats)"

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
      , bgroup "Worker Throughput (orville)" $
          orvilleWorkerBenches statsConn simpleEnv orvilleRun
      , bgroup "Steady-State Throughput (simple)" $
          steadyStateBenches (\d p w b m f -> simpleSteadyStateTrial simpleRun producerRun statsConn d p w b m f)
      , bgroup "Steady-State Throughput (hasql)" $
          steadyStateBenches (\d p w b m f -> hasqlSteadyStateTrial hasqlPreparedRun producerRun statsConn d p w b m f)
      , bgroup "Steady-State Throughput (orville)" $
          steadyStateBenches (\d p w b m f -> orvilleSteadyStateTrial orvilleRun producerRun statsConn d p w b m f)
      , bgroup "Gating Overhead (hasql)" $
          gatingBenches settleGated (\m t j d -> hasqlGatedSteadyTrial hasqlPreparedRun producerRun statsConn m t j d)
      ]

_claimBenches :: SimpleEnv BenchRegistry -> Int -> [(String, QueueFlavor)] -> [Benchmark]
_claimBenches simpleEnv queueSize flavors =
  flip map flavors $ \(label, flavor) ->
    let mkBench name action =
          singleTest name $
            ThroughputBench $
              multiTrial
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
  mkWorkerBenches simpleEnv (\n d p w m -> simpleWorkerTrial runM statsConn n d p w m)

hasqlWorkerBenches :: Connection -> SimpleEnv BenchRegistry -> RunM HasqlM -> [Benchmark]
hasqlWorkerBenches statsConn simpleEnv runM =
  mkWorkerBenches simpleEnv (\n d p w m -> hasqlWorkerTrial runM statsConn n d p w m)

orvilleWorkerBenches :: Connection -> SimpleEnv BenchRegistry -> RunM OrvilleM -> [Benchmark]
orvilleWorkerBenches statsConn simpleEnv runM =
  mkWorkerBenches simpleEnv (\n d p w m -> orvilleWorkerTrial runM statsConn n d p w m)

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
    poolLabel n = show n <> " pools"
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
              singleTest name $
                ThroughputBench $
                  multiTrialSteady
                    trialCount
                    (setupQueue simpleEnv numJobs flavor)
                    (trial numJobs trialDurationUs pools workers mode)
         in bgroup
              label
              [ mkBench "single job mode" BenchSingleJobMode
              , mkBench "batched mode (size 10)" (BenchBatchedJobsMode 10)
              ]

steadyStateBenches
  :: (Int -> Int -> Int -> Int -> BenchMode -> QueueFlavor -> IO SteadyResult)
  -> [Benchmark]
steadyStateBenches trial =
  [ bgroup "4 pools x 10 workers" $
      flip map steadyStateFlavors $ \(label, flavor) ->
        let producerBatch = 100
            mkBench name mode =
              singleTest name $
                ThroughputBench $
                  multiTrialSteady
                    trialCount
                    cleanupFresh
                    (trial trialDurationUs 4 10 producerBatch mode flavor)
         in bgroup
              label
              [ mkBench "single job mode" BenchSingleJobMode
              , mkBench "batched mode (size 10)" (BenchBatchedJobsMode 10)
              ]
  ]
  where
    steadyStateFlavors :: [(String, QueueFlavor)]
    steadyStateFlavors =
      [ ("ungrouped", Ungrouped)
      , ("5000 groups", Grouped 5000)
      , ("5000 groups (scheduled + backoff)", GroupedBacklog 5000)
      ]

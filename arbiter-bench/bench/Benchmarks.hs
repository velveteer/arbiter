{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

module Main (main) where

import Arbiter.Core.HasArbiterSchema (HasArbiterSchema)
import Arbiter.Core.HighLevel qualified as HL
import Arbiter.Core.Job.Types (JobRead, defaultGroupedJob, defaultJob)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.PoolConfig (PoolConfig (..))
import Arbiter.Hasql (HasqlDb, createHasqlEnvWithConfig, runHasqlDb)
import Arbiter.Migrations (MigrationResult (..), defaultMigrationConfig, runMigrationsForRegistry)
import Arbiter.Orville
  ( createOrvilleConnectionOptions
  , orvilleExecuteQuery
  , orvilleExecuteStatement
  , orvilleRunHandlerWithConnection
  , orvilleWithDbTransaction
  )
import Arbiter.Simple (SimpleDb, SimpleEnv, createSimpleEnv, runSimpleDb)
import Arbiter.Worker
  ( WorkerConfig (..)
  , defaultBatchedWorkerConfig
  , defaultWorkerConfig
  , runWorkerPool
  , silentLogConfig
  )
import Control.Concurrent (threadDelay)
import Control.Concurrent.Async (mapConcurrently_, race_)
import Control.Monad (replicateM, void, when)
import Control.Monad.Catch (MonadCatch, MonadMask, MonadThrow)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Reader (ReaderT (..), asks)
import Data.Aeson (FromJSON, ToJSON)
import Data.ByteString (ByteString)
import Data.IORef (modifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty)
import Data.Proxy (Proxy (..))
import Data.String (fromString)
import Data.Tagged (Tagged (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (diffUTCTime, getCurrentTime)
import Data.Typeable (Typeable)
import Database.PostgreSQL.Simple (Connection, Only (..), Query, close, connectPostgreSQL, execute)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Hasql.Connection qualified as Hasql
import Numeric (showFFloat)
import Orville.PostgreSQL qualified as O
import Orville.PostgreSQL.UnliftIO qualified as O
import System.Exit (die)
import Test.Tasty.Bench
import Test.Tasty.Providers (IsTest (..), singleTest, testPassed)
import UnliftIO (MonadUnliftIO)

benchSchema :: Text
benchSchema = "arbiter"

benchConnStr :: ByteString
benchConnStr = "host=localhost port=54324 user=postgres password=master dbname=postgres options='-c track_functions=all'"

trialCount :: Int
trialCount = 10

trialDurationUs :: Int
trialDurationUs = 10_000_000

data BenchPayload
  = BenchMessage Int
  | BenchBatch Int
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type BenchRegistry = '[ '("bench_queue", BenchPayload)]

data QueueFlavor
  = Ungrouped
  | Grouped Int
  | Mixed Int

data BenchMode
  = BenchSingleJobMode
  | BenchBatchedJobsMode Int

data BenchStats = BenchStats
  { statsMean :: !Double
  , statsStdev :: !Double
  , statsSamples :: !Int
  }

newtype ThroughputBench = ThroughputBench (IO String)
  deriving stock (Typeable)

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

multiTrialWithStats :: Int -> IO () -> IO (Double, [TriggerStats]) -> String -> IO String
multiTrialWithStats n setup measure unit = do
  results <- replicateM n (setup >> measure)
  let throughputs = map fst results
      lastStats = snd (last results)
  pure $ formatStats unit (computeStats throughputs) <> "\n" <> formatTriggerStats lastStats

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

instance HasArbiterSchema BenchOrville BenchRegistry where
  getSchema = BenchOrville $ asks fst

instance MonadArbiter BenchOrville where
  type Handler BenchOrville jobs result = jobs -> BenchOrville result
  executeQuery = orvilleExecuteQuery
  executeStatement = orvilleExecuteStatement
  withDbTransaction = orvilleWithDbTransaction
  runHandlerWithConnection = orvilleRunHandlerWithConnection

type OrvilleM = BenchOrville

simpleWorkerTrial :: RunM SimpleM -> Int -> Int -> Int -> Int -> BenchMode -> IO Double
simpleWorkerTrial runM totalJobs durationUs numPools workersPerPool modeConfig = do
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ defaultWorkerConfig benchConnStr workersPerPool (\(_conn :: Connection) _job -> pure ())
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <- runM $ defaultBatchedWorkerConfig benchConnStr workersPerPool batchSize (\(_conn :: Connection) _jobs -> pure ())
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
  runWorkerTrial runM configs totalJobs durationUs

hasqlWorkerTrial :: RunM HasqlM -> Int -> Int -> Int -> Int -> BenchMode -> IO Double
hasqlWorkerTrial runM totalJobs durationUs numPools workersPerPool modeConfig = do
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ defaultWorkerConfig benchConnStr workersPerPool (\(_conn :: Hasql.Connection) _job -> pure ())
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <-
        runM $ defaultBatchedWorkerConfig benchConnStr workersPerPool batchSize (\(_conn :: Hasql.Connection) _jobs -> pure ())
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
  runWorkerTrial runM configs totalJobs durationUs

orvilleWorkerTrial :: RunM OrvilleM -> Int -> Int -> Int -> Int -> BenchMode -> IO Double
orvilleWorkerTrial runM totalJobs durationUs numPools workersPerPool modeConfig = do
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ defaultWorkerConfig benchConnStr workersPerPool (\_job -> pure ())
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <- runM $ defaultBatchedWorkerConfig benchConnStr workersPerPool batchSize (\_jobs -> pure ())
      pure c {pollInterval = 0.1, logConfig = silentLogConfig}
  runWorkerTrial runM configs totalJobs durationUs

runWorkerTrial
  :: (HasArbiterSchema m BenchRegistry, MonadArbiter m, MonadMask m, MonadUnliftIO m)
  => RunM m -> [WorkerConfig m BenchPayload ()] -> Int -> Int -> IO Double
runWorkerTrial runM configs totalJobs durationUs = do
  start <- getCurrentTime
  race_
    (mapConcurrently_ (\c -> runM $ runWorkerPool c) configs)
    (threadDelay durationUs)
  end <- getCurrentTime
  remaining <- runM (HL.countJobs @_ @BenchRegistry @BenchPayload)
  let processed = fromIntegral totalJobs - remaining
      elapsed = realToFrac (diffUTCTime end start) :: Double
  pure (fromIntegral processed / elapsed)

-- | Steady-state trial: a producer inserts jobs continuously while workers consume.
--
-- The producer inserts in batches to keep the queue fed. Measures how many jobs
-- are fully processed (inserted + claimed + acked) during the trial.
runSteadyStateTrial
  :: (HasArbiterSchema m BenchRegistry, MonadArbiter m, MonadMask m, MonadUnliftIO m)
  => RunM m
  -- ^ Runner for workers
  -> RunM SimpleM
  -- ^ Runner for producers (separate pool)
  -> [WorkerConfig m BenchPayload ()]
  -> Int
  -- ^ Batch size for producer inserts
  -> Int
  -- ^ Number of concurrent producer threads
  -> Int
  -- ^ Delay between producer batches (microseconds), 0 for no delay
  -> QueueFlavor
  -> Int
  -- ^ Trial duration (microseconds)
  -> IO Double
runSteadyStateTrial runM producerRunM configs producerBatchSize numProducers producerDelayUs flavor durationUs = do
  insertCounter <- newIORef (0 :: Int)
  let mkProducer producerId = do
        -- Stagger startup so producers don't all insert simultaneously
        when (producerDelayUs > 0) $
          threadDelay (producerId * (producerDelayUs `div` numProducers))
        let jobs = case flavor of
              Ungrouped ->
                [defaultJob (BenchBatch i) | i <- [1 .. producerBatchSize]]
              Grouped numGroups ->
                [ defaultGroupedJob (T.pack $ "g" <> show ((i `mod` numGroups) + 1)) (BenchBatch i)
                | i <- [1 .. producerBatchSize]
                ]
              Mixed numGroups ->
                [ if even i
                    then defaultJob (BenchBatch i)
                    else defaultGroupedJob (T.pack $ "g" <> show ((i `mod` numGroups) + 1)) (BenchBatch i)
                | i <- [1 .. producerBatchSize]
                ]
            go = do
              producerRunM $ void $ HL.insertJobsBatch_ jobs
              modifyIORef' insertCounter (+ producerBatchSize)
              when (producerDelayUs > 0) $ threadDelay producerDelayUs
              go
         in go
  start <- getCurrentTime
  race_
    ( mapConcurrently_
        id
        ( map (\c -> runM $ runWorkerPool c) configs
            <> [mkProducer i | i <- [0 .. numProducers - 1]]
        )
    )
    (threadDelay durationUs)
  end <- getCurrentTime
  remaining <- runM (HL.countJobs @_ @BenchRegistry @BenchPayload)
  inserted <- readIORef insertCounter
  let processed = fromIntegral inserted - remaining
      elapsed = realToFrac (diffUTCTime end start) :: Double
  pure (fromIntegral processed / elapsed)

simpleSteadyStateTrial
  :: RunM SimpleM -> RunM SimpleM -> Int -> Int -> Int -> Int -> BenchMode -> QueueFlavor -> IO Double
simpleSteadyStateTrial runM producerRunM durationUs numPools workersPerPool producerBatchSize modeConfig flavor = do
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ defaultWorkerConfig benchConnStr workersPerPool (\(_conn :: Connection) _job -> pure ())
      pure c {pollInterval = 1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <- runM $ defaultBatchedWorkerConfig benchConnStr workersPerPool batchSize (\(_conn :: Connection) _jobs -> pure ())
      pure c {pollInterval = 1, logConfig = silentLogConfig}
  runSteadyStateTrial runM producerRunM configs producerBatchSize 10 50_000 flavor durationUs

hasqlSteadyStateTrial
  :: RunM HasqlM -> RunM SimpleM -> Int -> Int -> Int -> Int -> BenchMode -> QueueFlavor -> IO Double
hasqlSteadyStateTrial runM producerRunM durationUs numPools workersPerPool producerBatchSize modeConfig flavor = do
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ defaultWorkerConfig benchConnStr workersPerPool (\(_conn :: Hasql.Connection) _job -> pure ())
      pure c {pollInterval = 1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <-
        runM $ defaultBatchedWorkerConfig benchConnStr workersPerPool batchSize (\(_conn :: Hasql.Connection) _jobs -> pure ())
      pure c {pollInterval = 1, logConfig = silentLogConfig}
  runSteadyStateTrial runM producerRunM configs producerBatchSize 10 50_000 flavor durationUs

orvilleSteadyStateTrial
  :: RunM OrvilleM -> RunM SimpleM -> Int -> Int -> Int -> Int -> BenchMode -> QueueFlavor -> IO Double
orvilleSteadyStateTrial runM producerRunM durationUs numPools workersPerPool producerBatchSize modeConfig flavor = do
  configs <- replicateM numPools $ case modeConfig of
    BenchSingleJobMode -> do
      c <- runM $ defaultWorkerConfig benchConnStr workersPerPool (\_job -> pure ())
      pure c {pollInterval = 1, logConfig = silentLogConfig}
    BenchBatchedJobsMode batchSize -> do
      c <- runM $ defaultBatchedWorkerConfig benchConnStr workersPerPool batchSize (\_jobs -> pure ())
      pure c {pollInterval = 1, logConfig = silentLogConfig}
  runSteadyStateTrial runM producerRunM configs producerBatchSize 10 50_000 flavor durationUs

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

setupQueue :: SimpleEnv BenchRegistry -> Int -> QueueFlavor -> IO ()
setupQueue simpleEnv totalJobs flavor = do
  conn <- connectPostgreSQL benchConnStr
  cleanupData conn

  runSimpleDb simpleEnv $ do
    let jobs = case flavor of
          Ungrouped ->
            [defaultJob (BenchBatch i) | i <- [1 .. totalJobs]]
          Grouped numGroups ->
            [ defaultGroupedJob (T.pack $ "g" <> show ((i `mod` numGroups) + 1)) (BenchBatch i)
            | i <- [1 .. totalJobs]
            ]
          Mixed numGroups ->
            [ if even i
                then defaultJob (BenchBatch i)
                else defaultGroupedJob (T.pack $ "g" <> show ((i `mod` numGroups) + 1)) (BenchBatch i)
            | i <- [1 .. totalJobs]
            ]
    void $ HL.insertJobsBatch_ jobs

  execute_ conn ("VACUUM ANALYZE " <> benchSchema <> ".bench_queue")
  execute_ conn ("VACUUM ANALYZE " <> benchSchema <> ".bench_queue_groups")
  execute_ conn "CHECKPOINT"
  close conn

-- Benchmark suites

main :: IO ()
main = do
  putStrLn "Setting up benchmark database schema..."
  setupSchema
  putStrLn "Schema ready. Creating environments..."

  simpleEnv <- createSimpleEnv (Proxy @BenchRegistry) benchConnStr benchSchema

  let hasqlPoolConfig = PoolConfig {poolSize = 25, poolIdleTimeout = 60, poolStripes = Just 1}
  hasqlEnv <- createHasqlEnvWithConfig (Proxy @BenchRegistry) benchConnStr benchSchema hasqlPoolConfig

  let orvilleOptions = createOrvilleConnectionOptions benchConnStr hasqlPoolConfig
  orvillePool <- O.createConnectionPool orvilleOptions
  let orvilleState = O.newOrvilleState O.defaultErrorDetailLevel orvillePool

  statsConn <- connectPostgreSQL benchConnStr
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

  let simpleRun :: RunM SimpleM
      simpleRun = runSimpleDb simpleEnv

      producerRun :: RunM SimpleM
      producerRun = runSimpleDb producerEnv

      hasqlRun :: RunM HasqlM
      hasqlRun = runHasqlDb hasqlEnv

      orvilleRun :: RunM OrvilleM
      orvilleRun action = runReaderT (unBenchOrville action) (benchSchema, orvilleState)

  defaultMain
    -- [ bgroup "Claim Throughput" $
    --     claimBenches
    --       simpleEnv
    --       200000
    --       [ ("ungrouped", Ungrouped)
    --       , ("10 groups", Grouped 10)
    --       , ("1000 groups", Grouped 1000)
    --       , ("200k groups", Grouped 200000)
    --       , ("mixed (10000 groups + ungrouped)", Mixed 10000)
    --       ]
    [ bgroup "Worker Throughput (simple)" $
        simpleWorkerBenches statsConn simpleEnv simpleRun
    , bgroup "Worker Throughput (hasql)" $
        hasqlWorkerBenches statsConn simpleEnv hasqlRun
    , bgroup "Worker Throughput (orville)" $
        orvilleWorkerBenches statsConn simpleEnv orvilleRun
    , bgroup "Steady-State Throughput (simple)" $
        steadyStateBenches statsConn (\d p w b m f -> simpleSteadyStateTrial simpleRun producerRun d p w b m f)
    , bgroup "Steady-State Throughput (hasql)" $
        steadyStateBenches statsConn (\d p w b m f -> hasqlSteadyStateTrial hasqlRun producerRun d p w b m f)
    , bgroup "Steady-State Throughput (orville)" $
        steadyStateBenches statsConn (\d p w b m f -> orvilleSteadyStateTrial orvilleRun producerRun d p w b m f)
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
  mkWorkerBenches statsConn simpleEnv (\n d p w m -> simpleWorkerTrial runM n d p w m)

hasqlWorkerBenches :: Connection -> SimpleEnv BenchRegistry -> RunM HasqlM -> [Benchmark]
hasqlWorkerBenches statsConn simpleEnv runM =
  mkWorkerBenches statsConn simpleEnv (\n d p w m -> hasqlWorkerTrial runM n d p w m)

orvilleWorkerBenches :: Connection -> SimpleEnv BenchRegistry -> RunM OrvilleM -> [Benchmark]
orvilleWorkerBenches statsConn simpleEnv runM =
  mkWorkerBenches statsConn simpleEnv (\n d p w m -> orvilleWorkerTrial runM n d p w m)

mkWorkerBenches
  :: Connection
  -> SimpleEnv BenchRegistry
  -> (Int -> Int -> Int -> Int -> BenchMode -> IO Double)
  -> [Benchmark]
mkWorkerBenches statsConn simpleEnv trial =
  [ bgroup (poolLabel pools <> " x " <> show workers <> " workers") $
      mkWorkerFlavorBenches statsConn simpleEnv trial pools workers defaultFlavors
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
      , -- , ("10 groups", Grouped 10)
        ("50000 groups", Grouped 50000)
        -- , ("200000 groups", Grouped 200000)
        -- , ("mixed (50000 groups + ungrouped)", Mixed 50000)
      ]

mkWorkerFlavorBenches
  :: Connection
  -> SimpleEnv BenchRegistry
  -> (Int -> Int -> Int -> Int -> BenchMode -> IO Double)
  -> Int
  -> Int
  -> [(String, QueueFlavor)]
  -> [Benchmark]
mkWorkerFlavorBenches statsConn simpleEnv trial pools workers flavors =
  let numJobs = 200000
   in flip map flavors $ \(label, flavor) ->
        let mkBench name mode =
              singleTest name $
                ThroughputBench $
                  multiTrialWithStats
                    trialCount
                    (setupQueue simpleEnv numJobs flavor)
                    ( do
                        resetTriggerStats statsConn
                        throughput <- trial numJobs trialDurationUs pools workers mode
                        stats <- readTriggerStats statsConn
                        pure (throughput, stats)
                    )
                    "jobs/sec"
         in bgroup
              label
              [ mkBench "single job mode" BenchSingleJobMode
              , mkBench "batched mode (size 10)" (BenchBatchedJobsMode 10)
              ]

steadyStateBenches
  :: Connection
  -> (Int -> Int -> Int -> Int -> BenchMode -> QueueFlavor -> IO Double)
  -> [Benchmark]
steadyStateBenches statsConn trial =
  [ bgroup "4 pools x 10 workers" $
      flip map steadyStateFlavors $ \(label, flavor) ->
        let producerBatch = 100
            mkBench name mode =
              singleTest name $
                ThroughputBench $
                  multiTrialWithStats
                    trialCount
                    (cleanupData =<< connectPostgreSQL benchConnStr)
                    ( do
                        resetTriggerStats statsConn
                        throughput <- trial trialDurationUs 4 10 producerBatch mode flavor
                        stats <- readTriggerStats statsConn
                        pure (throughput, stats)
                    )
                    "jobs/sec"
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
      ]

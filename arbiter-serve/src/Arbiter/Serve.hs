{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes #-}
{-# LANGUAGE TypeApplications #-}

-- | The reusable @serve@ harness: health, telemetry, metrics, graceful drain.
module Arbiter.Serve
  ( -- * Serving a typed deployment
    ServeConfig (..)
  , defaultServeConfig
  , TelemetryMode (..)
  , runArbiterServe

    -- * Serving a custom app
  , ServeEnv (..)
  , runArbiterServeWith

    -- * Serving pools whose queues are named at runtime
  , ServePool (..)
  , instrumentPool
  , runServeHarness
  ) where

import Arbiter.Core.Job.Types (Job (..), JobPayload, JobRead, ObservabilityHooks (..))
import Arbiter.Core.Operations (RegistryPayloadAdmission)
import Arbiter.Hasql (hasqlSettings)
import Arbiter.Hasql.Compat qualified as Compat
import Arbiter.Servant.API (ArbiterAPI)
import Arbiter.Servant.Server
  ( ArbiterServerConfig (..)
  , BuildServer
  , QueueSpec (..)
  , cachedFor
  , initArbiterServer
  , newCacheCell
  )
import Arbiter.Servant.UI (arbiterAppWithAdmin)
import Arbiter.Worker
  ( NamedWorkerPool (..)
  , RetryPolicy
  , WorkerConfig (logConfig, observabilityHooks, workerStateVar)
  , WorkerState (..)
  , awaitShutdown
  , getEnabledQueuesFrom
  , queueOf
  , retryPolicyOf
  , runWorkerPool
  , signalShutdown
  , tryLog
  , withPoolContext
  )
import Arbiter.Worker.Logger (LogConfig, LogDestination, LogLevel (..))
import Arbiter.Worker.Telemetry (ArbiterMeters, withOtelHooks, withOtelLogs, withOtelMetrics)
import Arbiter.Worker.Trace (resolveTracer)
import Control.Concurrent (forkIO)
import Control.Concurrent.Async (concurrently_, mapConcurrently_, race_)
import Control.Concurrent.MVar (MVar, modifyMVar, newMVar)
import Control.Concurrent.STM (TVar, newTVarIO, readTVarIO)
import Control.Exception (SomeException, try)
import Control.Monad (void)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.IO.Unlift (MonadUnliftIO, liftIO, withRunInIO)
import Data.Aeson (Value, parseJSON)
import Data.Aeson.Types (parseMaybe)
import Data.ByteString (ByteString)
import Data.ByteString.Lazy qualified as BL
import Data.Either (isRight)
import Data.Foldable (traverse_)
import Data.Map.Strict qualified as Map
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.String (fromString)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time.Clock (NominalDiffTime)
import Hasql.Connection qualified as HC
import Network.HTTP.Types (methodGet, status200, status503)
import Network.Wai (Application, Middleware, pathInfo, requestMethod, responseLBS)
import Network.Wai.Handler.Warp
  ( defaultSettings
  , runSettings
  , setGracefulShutdownTimeout
  , setHost
  , setInstallShutdownHandler
  , setPort
  , setTimeout
  )
import Servant.Server (HasServer)
import System.Posix.Signals (Handler (Catch), installHandler, sigINT, sigTERM)

import Arbiter.Serve.Gauges (startGauges)
import Arbiter.Serve.Telemetry qualified as Tel

data ServeConfig = ServeConfig
  { host :: Text
  , port :: Int
  , connStr :: ByteString
  , schema :: Text
  , telemetry :: TelemetryMode
  , metricsScrape :: Bool
  -- ^ Serve the Prometheus scrape endpoint on 'metricsPort'. 'TelemetryManaged' only.
  , metricsPort :: Int
  , preparedStatements :: Bool
  -- ^ Prepare the harness's own statements once per connection. Off behind a pooler that lacks them.
  , gaugeInterval :: NominalDiffTime
  -- ^ How often a replica rescans every queue's depth and the database's health.
  , shutdownGraceSecs :: NominalDiffTime
  -- ^ How long Warp waits for open connections to close on shutdown. Keep it under the
  -- orchestrator's termination grace period. A held-open scrape or SSE stream waits this long.
  }

-- | A 'ServeConfig' with sensible defaults, overridable per field. Binds loopback.
defaultServeConfig :: ByteString -> Text -> ServeConfig
defaultServeConfig connectionString schemaName =
  ServeConfig
    { host = "127.0.0.1"
    , port = 8080
    , connStr = connectionString
    , schema = schemaName
    , telemetry = TelemetryManaged
    , metricsScrape = True
    , metricsPort = 9464
    , preparedStatements = True
    , gaugeInterval = 60
    , shutdownGraceSecs = 5
    }

-- | How the harness sets up OpenTelemetry.
data TelemetryMode
  = -- | No telemetry.
    TelemetryOff
  | -- | Harness initializes its own providers (OTLP push + Prometheus scrape).
    TelemetryManaged
  | -- | Use the caller's global providers, routing metrics through the global meter provider.
    TelemetryExternal

managedTelemetry :: TelemetryMode -> Bool
managedTelemetry = \case
  TelemetryManaged -> True
  _ -> False

data ServeEnv = ServeEnv
  { stateVar :: TVar WorkerState
  , meters :: Maybe ArbiterMeters
  , logs :: Maybe LogDestination
  -- ^ Overrides each pool's log destination. 'Nothing' leaves the pool's own in place.
  }

-- | Serve a registry's REST API, admin UI, and worker pools, in the monad the pools run in.
-- Run the migrations first.
--
-- @
-- runSimpleDb env $ runArbiterServe \@AppRegistry serveCfg [namedWorkerPool emailCfg]
-- @
runArbiterServe
  :: forall registry m
   . ( BuildServer registry registry
     , HasServer (ArbiterAPI registry) '[]
     , MonadUnliftIO m
     , RegistryPayloadAdmission registry
     )
  => ServeConfig
  -> [NamedWorkerPool m]
  -> m ()
runArbiterServe sc pools = do
  apiCfg <- liftIO $ initArbiterServer (Proxy @registry) (connStr sc) (schema sc)
  runArbiterServeWith sc apiCfg pools (arbiterAppWithAdmin @registry)

-- | 'runArbiterServe' over an app you build yourself, with the config wired to @pools@.
runArbiterServeWith
  :: (MonadUnliftIO m)
  => ServeConfig
  -> ArbiterServerConfig registry
  -> [NamedWorkerPool m]
  -> (ArbiterServerConfig registry -> Application)
  -> m ()
runArbiterServeWith sc apiCfg pools mkApp =
  withRunInIO $ \runner ->
    runServeHarness sc apiCfg mkApp (map (servePool runner) pools)

-- | One pool the harness serves, with the monad it runs in erased.
data ServePool = ServePool
  { poolQueue :: Text
  -- ^ The queue it claims from. What @ARBITER_ENABLED_QUEUES@ selects on.
  , poolRetry :: RetryPolicy
  -- ^ What the queue's consumer routes retry with.
  , poolHooks :: ObservabilityHooks IO Value
  -- ^ The pool's lifecycle hooks, so a job consumed over HTTP fires what a claimed one does.
  , poolRun :: ServeEnv -> IO ()
  -- ^ Run until the pool drains. Instrument it with 'instrumentPool'.
  }

servePool :: (MonadUnliftIO m) => (forall a. m a -> IO a) -> NamedWorkerPool m -> ServePool
servePool runner pool@(NamedWorkerPool name cfg) =
  ServePool
    { poolQueue = queue
    , poolRetry = retryPolicyOf cfg
    , poolHooks = runtimeHooks runner (logConfig cfg) (observabilityHooks cfg)
    , poolRun = \env -> runner (runWorkerPool (named (instrumentPool env queue cfg)))
    }
  where
    queue = queueOf pool
    named c = c {logConfig = withPoolContext name (logConfig c)}

-- | A pool's typed hooks over the runtime-typed jobs the consumer routes carry, so a job
-- consumed over HTTP fires what a claimed one does.
runtimeHooks
  :: forall m payload
   . (JobPayload payload)
  => (forall a. m a -> IO a)
  -> LogConfig
  -> ObservabilityHooks m payload
  -> ObservabilityHooks IO Value
runtimeHooks runner logCfg hooks =
  ObservabilityHooks
    { onJobClaimed = \j t -> at j $ \tj -> onJobClaimed hooks tj t
    , onJobSuccess = \j s e -> at j $ \tj -> onJobSuccess hooks tj s e
    , onJobFailure = \j msg s e -> at j $ \tj -> onJobFailure hooks tj msg s e
    , onJobRetry = \j d -> at j $ \tj -> onJobRetry hooks tj d
    , onJobFailedAndMovedToDLQ = \msg j -> at j $ \tj -> onJobFailedAndMovedToDLQ hooks msg tj
    , onJobCancelled = \j msg -> at j $ \tj -> onJobCancelled hooks tj msg
    , onJobHeartbeat = \j c s -> at j $ \tj -> onJobHeartbeat hooks tj c s
    }
  where
    at :: JobRead Value -> (JobRead payload -> m ()) -> IO ()
    at j k = case parseMaybe parseJSON (payload j) of
      Just p -> runner (k j {payload = p})
      Nothing -> tryLog logCfg Warning (skipped (primaryKey j))
    skipped jobId =
      "Skipped the queue's hooks for job "
        <> T.pack (show jobId)
        <> ": its stored payload does not decode at the type this queue's pool runs"

-- | Point a pool's jobs at the harness's meters and logs, and its drain at the harness's
-- shutdown signal.
instrumentPool :: (MonadIO m) => ServeEnv -> Text -> WorkerConfig m payload result -> WorkerConfig m payload result
instrumentPool env name cfg =
  (withOtelLogs (logs env) (withOtelMetrics (meters env) name cfg))
    { workerStateVar = stateVar env
    }

-- | The harness over an app and the pools you supply.
runServeHarness
  :: ServeConfig
  -> ArbiterServerConfig registry
  -> (ArbiterServerConfig registry -> Application)
  -> [ServePool]
  -> IO ()
runServeHarness sc apiCfg0 mkApp pools = do
  -- Selects on every queue the server knows, so one fleet-wide value of the variable is
  -- accepted by a process that pools a subset of them.
  enabled <-
    getEnabledQueuesFrom "ARBITER_ENABLED_QUEUES" $
      Set.toList (Set.fromList (Map.keys (serverQueues apiCfg0) <> map poolQueue pools))
  let live = filter ((`elem` enabled) . poolQueue) pools
      apiCfg = withPoolSpecs pools apiCfg0
  runState <- newTVarIO Running
  readyCache <- newCacheCell
  probeConn <- newMVar Nothing
  let readyCheck =
        readTVarIO runState >>= \case
          ShuttingDown -> pure False
          _ -> cachedFor readyCacheTtl readyCache (dbOk (connStr sc) probeConn)

  let onSignal = signalShutdown runState
      withShutdown = setInstallShutdownHandler (\close -> void $ forkIO $ awaitShutdown runState >> close)
  traverse_ (\s -> installHandler s (Catch onSignal) Nothing) [sigTERM, sigINT]

  let mkSettings p =
        withShutdown $
          setHost (fromString (T.unpack (host sc))) $
            setPort p $
              setGracefulShutdownTimeout (Just (ceiling (shutdownGraceSecs sc))) defaultSettings
      settings = setTimeout 30 (mkSettings (port sc))
      metricsSettings = mkSettings (metricsPort sc)

  let scrape = metricsScrape sc && managedTelemetry (telemetry sc)
      serveWith mTel = do
        -- Resolved inside the telemetry bracket, where the global provider is live.
        tracer <- resolveTracer
        let apiCfgT = apiCfg {serverTracer = tracer}
            env =
              ServeEnv
                { stateVar = runState
                , meters = Tel.meters <$> mTel
                , logs = Tel.logDestination =<< mTel
                }
            app =
              (withHealth . withReady readyCheck) $
                maybe id Tel.waiMiddleware mTel (mkApp (instrumentQueues env apiCfgT))
            core = concurrently_ (runSettings settings app) (mapConcurrently_ (`poolRun` env) live)
            withTel t = do
              gaugeLoop <-
                startGauges t (connStr sc) (schema sc) (preparedStatements sc) (gaugeInterval sc)
              let withScrape =
                    if scrape
                      then concurrently_ core (runSettings metricsSettings (Tel.metricsApp t))
                      else core
              race_ withScrape gaugeLoop
        putStrLn $
          "arbiter serving on "
            <> T.unpack (host sc)
            <> ":"
            <> show (port sc)
            <> (if scrape then ", metrics on :" <> show (metricsPort sc) else "")
        maybe core withTel mTel

  case telemetry sc of
    TelemetryOff -> serveWith Nothing
    TelemetryManaged -> Tel.withTelemetry (serveWith . Just)
    TelemetryExternal -> Tel.withExternalTelemetry (serveWith . Just)

-- | Rewire every queue this server serves.
overQueues
  :: (Text -> QueueSpec -> QueueSpec)
  -> ArbiterServerConfig registry
  -> ArbiterServerConfig registry
overQueues wire config = config {serverQueues = Map.mapWithKey wire (serverQueues config)}

-- | Record the consumer routes' jobs into the same meters the pools use.
instrumentQueues :: ServeEnv -> ArbiterServerConfig registry -> ArbiterServerConfig registry
instrumentQueues env =
  overQueues $ \queue spec -> spec {queueHooks = withOtelHooks (meters env) queue (queueHooks spec)}

-- | Point each queue's retry policy and lifecycle hooks at the pool claiming it. Every
-- declared pool wires its queue, not just the enabled ones.
withPoolSpecs :: [ServePool] -> ArbiterServerConfig registry -> ArbiterServerConfig registry
withPoolSpecs pools =
  overQueues $ \queue spec ->
    case Map.lookup queue byQueue of
      Nothing -> spec
      Just p -> spec {queueRetry = poolRetry p, queueHooks = poolHooks p}
  where
    byQueue = Map.fromList [(poolQueue p, p) | p <- pools]

probe :: Text -> BL.ByteString -> IO Bool -> Middleware
probe path body check app req respond
  | requestMethod req == methodGet && pathInfo req == [path] = do
      ok <- check
      let (status, reply) = if ok then (status200, body) else (status503, "not ready")
      respond $ responseLBS status [("Content-Type", "text/plain")] reply
  | otherwise = app req respond

withHealth :: Middleware
withHealth = probe "healthz" "ok" (pure True)

withReady :: IO Bool -> Middleware
withReady = probe "readyz" "ready"

-- | How long a readiness verdict is served before the database is probed again.
readyCacheTtl :: NominalDiffTime
readyCacheTtl = 5

-- | Is the database reachable? A @select 1@ on a connection the probe keeps.
dbOk :: ByteString -> MVar (Maybe HC.Connection) -> IO Bool
dbOk cs cell = modifyMVar cell $ \held -> do
  answered <- traverse ping held
  case answered of
    Just True -> pure (held, True)
    _ -> traverse_ HC.release held >> reconnect
  where
    ping conn = isRight <$> try @SomeException (Compat.runSQL conn "select 1")
    reconnect = HC.acquire (hasqlSettings cs) >>= either (const (pure (Nothing, False))) verify
    verify conn =
      ping conn >>= \case
        True -> pure (Just conn, True)
        False -> (Nothing, False) <$ HC.release conn

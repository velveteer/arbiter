{-# LANGUAGE OverloadedStrings #-}

-- | TOML configuration for the @arbiter@ binary. See @arbiter.example.toml@.
--
-- A secret-bearing value resolves in precedence order: a @*_file@ sibling, a
-- @${ENV_VAR}@ reference, then a literal.
module Arbiter.Cli.Config
  ( Config (..)
  , DatabaseC (..)
  , ServerC (..)
  , QueueC (..)
  , WebhookC (..)
  , WorkerC (..)
  , BackoffC (..)
  , CronC (..)
  , queueCronJobs
  , WorkerSettings (..)
  , workerSettings
  , RateLimitPolicyC (..)
  , ConcurrencyPolicyC (..)
  , TelemetryC (..)
  , eventsEnabled
  , serverHost
  , serverPort
  , serverShutdownGraceSecs
  , loadConfig
  , validateConfig
  , parseWebhookUrl
  ) where

import Arbiter.Core.Job.Types (defaultJob)
import Arbiter.Worker
  ( Jitter (..)
  , RetryPolicy (..)
  , WorkerTimings (..)
  , constantBackoff
  , defaultRetryPolicy
  , defaultWorkerTimings
  , exponentialBackoff
  , linearBackoff
  )
import Arbiter.Worker.Cron
  ( BackfillPolicy (..)
  , CronJob (..)
  , OverlapPolicy (..)
  , cronJob
  , cronJobInTimezone
  )
import Control.Exception (Exception, IOException, throwIO, try)
import Control.Monad (void)
import Data.Aeson (Value)
import Data.Aeson qualified as Aeson
import Data.Aeson.Key qualified as Key
import Data.Aeson.KeyMap qualified as KM
import Data.Bifunctor (first)
import Data.ByteString qualified as BS
import Data.Char (isAsciiLower, isAsciiUpper, isDigit)
import Data.Foldable (fold, traverse_)
import Data.Int (Int32)
import Data.Map.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Text.Encoding qualified as TE
import Data.Time (NominalDiffTime)
import Network.HTTP.Client (Request, parseRequest)
import System.Environment (lookupEnv)
import Toml (Result (..), Table' (..), Value' (..), decode)
import Toml.Schema (FromValue (..), Matcher, optKey, parseTableFromValue, reqKey)

-- | Top-level config.
data Config = Config
  { cfgDatabase :: DatabaseC
  , cfgServer :: Maybe ServerC
  , cfgQueues :: [QueueC]
  , cfgRateLimits :: [RateLimitPolicyC]
  , cfgConcurrency :: [ConcurrencyPolicyC]
  , cfgTelemetry :: TelemetryC
  }
  deriving stock (Show)

instance FromValue Config where
  fromValue =
    parseTableFromValue $
      Config
        <$> reqKey "database"
        <*> optKey "server"
        <*> (fold <$> optKey "queue")
        <*> (fold <$> optKey "ratelimit")
        <*> (fold <$> optKey "concurrency")
        <*> (fromMaybe defaultTelemetry <$> optKey "telemetry")

-- | @[telemetry]@ - OpenTelemetry. The collector is set by the standard @OTEL_*@ variables.
data TelemetryC = TelemetryC
  { tcEnabled :: Bool
  -- ^ Set up OpenTelemetry at all (tracing, metrics, OTLP export).
  , tcMetrics :: Bool
  -- ^ Serve the Prometheus @/metrics@ scrape endpoint. Tracing and OTLP export are separate.
  , tcMetricsPort :: Int
  -- ^ Dedicated port for @/metrics@, kept off the public ingress.
  , tcGaugeIntervalSecs :: Double
  -- ^ How often a replica rescans every queue's depth and the database's health.
  }
  deriving stock (Show)

defaultTelemetry :: TelemetryC
defaultTelemetry =
  TelemetryC {tcEnabled = True, tcMetrics = True, tcMetricsPort = 9464, tcGaugeIntervalSecs = 60}

instance FromValue TelemetryC where
  fromValue =
    parseTableFromValue $
      TelemetryC
        <$> (fromMaybe (tcEnabled defaultTelemetry) <$> optKey "enabled")
        <*> (fromMaybe (tcMetrics defaultTelemetry) <$> optKey "metrics")
        <*> (fromMaybe (tcMetricsPort defaultTelemetry) <$> optKey "metrics_port")
        <*> (fromMaybe (tcGaugeIntervalSecs defaultTelemetry) <$> optKey "gauge_interval_secs")

-- | @[[ratelimit]]@ - a token bucket: burst @max@, refilling @refill@ every @interval@ seconds.
data RateLimitPolicyC = RateLimitPolicyC
  { rlpPrefix :: Text
  , rlpMax :: Double
  , rlpRefill :: Double
  , rlpInterval :: Double
  }
  deriving stock (Show)

instance FromValue RateLimitPolicyC where
  fromValue =
    parseTableFromValue $
      RateLimitPolicyC <$> reqKey "prefix" <*> reqKey "max" <*> reqKey "refill" <*> reqKey "interval"

-- | @[[concurrency]]@ - at most @limit@ jobs in flight per key under @prefix@.
data ConcurrencyPolicyC = ConcurrencyPolicyC
  { cpcPrefix :: Text
  , cpcLimit :: Int
  }
  deriving stock (Show)

instance FromValue ConcurrencyPolicyC where
  fromValue = parseTableFromValue (ConcurrencyPolicyC <$> reqKey "prefix" <*> reqKey "limit")

-- | @[database]@ - the libpq connection string and schema.
data DatabaseC = DatabaseC
  { dbUrl :: Text
  , dbUrlFile :: Maybe Text
  , dbSchema :: Maybe Text
  , dbPreparedStatements :: Bool
  -- ^ Plan the claim once per connection. Off behind a pooler that lacks prepared statements.
  }
  deriving stock (Show)

instance FromValue DatabaseC where
  fromValue =
    parseTableFromValue $
      DatabaseC
        <$> (fromMaybe "" <$> optKey "url")
        <*> optKey "url_file"
        <*> optKey "schema"
        <*> (fromMaybe True <$> optKey "prepared_statements")

-- | @[server]@ - bind address and port.
data ServerC = ServerC
  { srvHost :: Maybe Text
  , srvPort :: Maybe Int
  , srvEvents :: Maybe Bool
  -- ^ Stream job events to the admin UI. Off by default: it triggers on every row write.
  , srvShutdownGraceSecs :: Maybe Double
  -- ^ Seconds Warp waits for open connections to close on shutdown.
  }
  deriving stock (Show)

instance FromValue ServerC where
  fromValue =
    parseTableFromValue
      ( ServerC
          <$> optKey "host"
          <*> optKey "port"
          <*> optKey "events"
          <*> optKey "shutdown_grace_secs"
      )

-- | A @[server]@ field, or its default when the table or the field is absent.
serverOr :: a -> (ServerC -> Maybe a) -> Config -> a
serverOr def field cfg = fromMaybe def (cfgServer cfg >>= field)

eventsEnabled :: Config -> Bool
eventsEnabled = serverOr False srvEvents

serverHost :: Config -> Text
serverHost = serverOr "127.0.0.1" srvHost

serverPort :: Config -> Int
serverPort = serverOr 8080 srvPort

serverShutdownGraceSecs :: Config -> Double
serverShutdownGraceSecs = serverOr 5 srvShutdownGraceSecs

-- | @[[queue]]@ - a queue, optionally with a webhook worker.
data QueueC = QueueC
  { qName :: Text
  , qWebhook :: Maybe WebhookC
  , qWorker :: Maybe WorkerC
  , qCron :: [CronC]
  }
  deriving stock (Show)

instance FromValue QueueC where
  fromValue =
    parseTableFromValue $
      QueueC
        <$> reqKey "name"
        <*> optKey "webhook"
        <*> optKey "worker"
        <*> (fold <$> optKey "cron")

-- | @[[queue.cron]]@ - enqueue a job onto this queue on a 5-field cron expression.
data CronC = CronC
  { cnName :: Text
  -- ^ Unique per schema: it keys the @cron_schedules@ row and the tick's dedup key.
  , cnExpression :: Text
  , cnTimezone :: Maybe Text
  -- ^ IANA name (e.g. @America\/New_York@). Absent means UTC.
  , cnOverlap :: Maybe OverlapC
  , cnBackfill :: Maybe Double
  -- ^ Replay ticks missed within this many seconds. Absent means no backfill.
  , cnPayload :: JsonObjC
  -- ^ The job payload. The tick time is added to it as @tick@.
  }
  deriving stock (Show)

instance FromValue CronC where
  fromValue =
    parseTableFromValue $
      CronC
        <$> reqKey "name"
        <*> reqKey "expression"
        <*> optKey "timezone"
        <*> optKey "overlap"
        <*> optKey "backfill"
        <*> (fold <$> optKey "payload")

-- | Wraps 'OverlapPolicy' so its TOML name parses without an orphan instance.
newtype OverlapC = OverlapC {unOverlapC :: OverlapPolicy}
  deriving stock (Show)

instance FromValue OverlapC where
  fromValue = fmap OverlapC . named "cron.overlap" [("skip", SkipOverlap), ("allow", AllowOverlap)]

-- | A TOML table read as a JSON object.
newtype JsonObjC = JsonObjC {unJsonObjC :: Aeson.Object}
  deriving stock (Show)
  deriving newtype (Monoid, Semigroup)

instance FromValue JsonObjC where
  fromValue = \case
    Table' _ t -> pure (JsonObjC (tomlObject t))
    _ -> fail "cron.payload must be a table"

tomlObject :: Table' l -> Aeson.Object
tomlObject (MkTable t) = KM.fromList [(Key.fromText k, tomlJson v) | (k, (_, v)) <- Map.toList t]

-- | A TOML value as JSON. A TOML date or time becomes its ISO 8601 text.
tomlJson :: Value' l -> Value
tomlJson = \case
  Integer' _ n -> Aeson.toJSON n
  Double' _ d -> Aeson.toJSON d
  Bool' _ b -> Aeson.toJSON b
  Text' _ t -> Aeson.toJSON t
  List' _ vs -> Aeson.toJSON (map tomlJson vs)
  Table' _ t -> Aeson.Object (tomlObject t)
  TimeOfDay' _ x -> Aeson.toJSON x
  ZonedTime' _ x -> Aeson.toJSON x
  LocalTime' _ x -> Aeson.toJSON x
  Day' _ x -> Aeson.toJSON x

cronAt :: QueueC -> CronC -> String
cronAt q c = "queue \"" <> T.unpack (qName q) <> "\" cron \"" <> T.unpack (cnName c) <> "\""

-- | The schedules a queue declares, as worker-pool cron jobs.
queueCronJobs :: QueueC -> Either String [CronJob Value]
queueCronJobs q = traverse (toCronJob q) (qCron q)

-- | Build one schedule. The tick time is injected into the payload as @tick@.
toCronJob :: QueueC -> CronC -> Either String (CronJob Value)
toCronJob q c =
  first (\e -> cronAt q c <> ": " <> e) $
    withBackfill <$> build (cnName c) (cnExpression c) (maybe SkipOverlap unOverlapC (cnOverlap c)) builder
  where
    build = maybe cronJob (flip cronJobInTimezone) (cnTimezone c)
    withBackfill cj = cj {backfill = maybe NoBackfill (Backfill . secs) (cnBackfill c)}
    body = unJsonObjC (cnPayload c)
    builder _kind tick = defaultJob (Aeson.Object (KM.insert "tick" (Aeson.toJSON tick) body))

-- | @[queue.worker]@ - the pool's timing and retry knobs, in seconds.
data WorkerC = WorkerC
  { wkPollInterval :: Maybe Double
  , wkVisibilityTimeout :: Maybe Double
  , wkJobHeartbeatInterval :: Maybe Double
  , wkWorkerHeartbeatInterval :: Maybe Double
  , wkWorkerStaleThreshold :: Maybe Double
  , wkReaperInterval :: Maybe Double
  , wkReaperTimeout :: Maybe Double
  , wkGracefulShutdownTimeout :: Maybe Double
  -- ^ @0@ waits for in-flight jobs indefinitely.
  , wkJitter :: Maybe JitterC
  , wkBackoff :: Maybe BackoffC
  }
  deriving stock (Show)

instance FromValue WorkerC where
  fromValue =
    parseTableFromValue $
      WorkerC
        <$> optKey "poll_interval"
        <*> optKey "visibility_timeout"
        <*> optKey "job_heartbeat_interval"
        <*> optKey "worker_heartbeat_interval"
        <*> optKey "worker_stale_threshold"
        <*> optKey "reaper_interval"
        <*> optKey "reaper_timeout"
        <*> optKey "graceful_shutdown_timeout"
        <*> optKey "jitter"
        <*> optKey "backoff"

-- | @[queue.worker.backoff]@ - how a retry delay grows with the attempt count.
data BackoffC = BackoffC
  { bkStrategy :: Maybe StrategyName
  , bkBase :: Maybe Double
  , bkIncrement :: Maybe Double
  , bkDelay :: Maybe Double
  , bkCap :: Maybe Double
  }
  deriving stock (Show)

instance FromValue BackoffC where
  fromValue =
    parseTableFromValue $
      BackoffC
        <$> optKey "strategy"
        <*> optKey "base"
        <*> optKey "increment"
        <*> optKey "delay"
        <*> optKey "cap"

data StrategyName = ExponentialS | LinearS | ConstantS
  deriving stock (Eq, Show)

-- | Wraps 'Jitter' so its TOML name parses without an orphan instance.
newtype JitterC = JitterC {unJitterC :: Jitter}
  deriving stock (Show)

-- | The keys that belong to each strategy.
strategyKeysFor :: StrategyName -> [Text]
strategyKeysFor = \case
  ExponentialS -> ["base", "cap"]
  LinearS -> ["increment", "cap"]
  ConstantS -> ["delay"]

-- | Every backoff key with the value it was set to, if any.
backoffKeys :: BackoffC -> [(Text, Maybe Double)]
backoffKeys b = [("base", bkBase b), ("increment", bkIncrement b), ("delay", bkDelay b), ("cap", bkCap b)]

instance FromValue StrategyName where
  fromValue = named "backoff.strategy" [("exponential", ExponentialS), ("linear", LinearS), ("constant", ConstantS)]

instance FromValue JitterC where
  fromValue = fmap JitterC . named "jitter" [("equal", EqualJitter), ("full", FullJitter), ("none", NoJitter)]

named :: String -> [(Text, a)] -> Value' l -> Matcher l a
named what table v = do
  key <- fromValue v
  maybe (fail (what <> " must be one of " <> show (map fst table) <> ": " <> T.unpack key)) pure (lookup key table)

-- | A queue's worker knobs, resolved against the library defaults.
data WorkerSettings = WorkerSettings
  { wsTimings :: WorkerTimings
  , wsRetry :: RetryPolicy
  }

-- | Resolve a queue's @[queue.worker]@ block against the library defaults.
workerSettings :: Maybe WorkerC -> WorkerSettings
workerSettings mw =
  WorkerSettings
    { wsTimings =
        WorkerTimings
          { timingPollInterval = pick wkPollInterval timingPollInterval
          , timingVisibilityTimeout = pick wkVisibilityTimeout timingVisibilityTimeout
          , timingJobHeartbeatInterval = pick wkJobHeartbeatInterval timingJobHeartbeatInterval
          , timingWorkerHeartbeatInterval = pick wkWorkerHeartbeatInterval timingWorkerHeartbeatInterval
          , timingWorkerStaleThreshold = pick wkWorkerStaleThreshold timingWorkerStaleThreshold
          , timingReaperInterval = pick wkReaperInterval timingReaperInterval
          , timingReaperTimeout = pick wkReaperTimeout timingReaperTimeout
          , timingGracefulShutdownTimeout = case mw >>= wkGracefulShutdownTimeout of
              Nothing -> timingGracefulShutdownTimeout defaultWorkerTimings
              Just 0 -> Nothing
              Just n -> Just (secs n)
          }
    , wsRetry =
        RetryPolicy
          { retryBackoff = case fromMaybe ExponentialS (backoffKey bkStrategy) of
              LinearS -> linearBackoff (backoff 30 bkIncrement) (backoff 1_048_576 bkCap)
              ConstantS -> constantBackoff (backoff 60 bkDelay)
              ExponentialS -> exponentialBackoff (fromMaybe 2 (backoffKey bkBase)) (backoff 1_048_576 bkCap)
          , retryJitter = maybe (retryJitter defaultRetryPolicy) unJitterC (mw >>= wkJitter)
          }
    }
  where
    pick field def = maybe (def defaultWorkerTimings) secs (mw >>= field)
    backoffKey :: (BackoffC -> Maybe a) -> Maybe a
    backoffKey field = mw >>= wkBackoff >>= field
    backoff def field = secs (fromMaybe def (backoffKey field))

secs :: Double -> NominalDiffTime
secs = realToFrac

-- | @[queue.webhook]@ - an external HTTP handler for a queue.
data WebhookC = WebhookC
  { wcUrl :: Text
  , wcWorkers :: Maybe Int
  -- ^ Jobs dispatched to the endpoint concurrently. Absent means 1.
  , wcSecret :: Maybe Text
  , wcSecretFile :: Maybe Text
  , wcTimeout :: Maybe Int
  }
  deriving stock (Show)

instance FromValue WebhookC where
  fromValue =
    parseTableFromValue $
      WebhookC
        <$> reqKey "url"
        <*> optKey "workers"
        <*> optKey "secret"
        <*> optKey "secret_file"
        <*> optKey "timeout_secs"

-- | A config that cannot be read, decoded, resolved, or validated.
newtype ConfigError = ConfigError String
  deriving stock (Show)

instance Exception ConfigError

configError :: String -> IO a
configError = throwIO . ConfigError

orFail :: Either String a -> IO a
orFail = either configError pure

-- | Read, decode, resolve (env references and @*_file@ secrets), and validate.
loadConfig :: FilePath -> IO (Either String Config)
loadConfig path = fmap (first message) . try $ do
  cfg <- orFail . decodeStrict =<< readUtf8 path
  resolved <- resolveConfig cfg
  resolved <$ orFail (validateConfig resolved)
  where
    message (ConfigError e) = e
    decodeStrict src = case decode src of
      Failure errs -> Left (unlines errs)
      Success [] cfg -> Right cfg
      Success warnings _ -> Left (unlines warnings)

-- | Static sanity checks. Secret non-emptiness is enforced in resolution.
validateConfig :: Config -> Either String ()
validateConfig cfg =
  sequence_ $
    [uniqueQueues, uniqueCrons, validPort, validMetricsPort, distinctPorts, validGaugeInterval, validShutdownGrace]
      <> foldMap (\q -> [validQueueName (qName q), validWorker q, validWebhook q, validCron q]) (cfgQueues cfg)
      <> map validRateLimit (cfgRateLimits cfg)
      <> map validConcurrency (cfgConcurrency cfg)
  where
    uniqueQueues =
      let qs = map qName (cfgQueues cfg)
       in if Set.size (Set.fromList qs) == length qs then Right () else Left "duplicate [[queue]] name"
    -- A queue name is a table name and a NOTIFY channel, so hold it to an identifier.
    validQueueName q
      | not (validIdent q) =
          Left ("invalid [[queue]] name: " <> T.unpack q <> " (letters, digits and underscore, not leading with a digit)")
      | "arbiter_" `T.isPrefixOf` T.toLower q =
          Left ("reserved [[queue]] name: " <> T.unpack q <> " (the arbiter_ prefix names internal tables)")
      | T.toLower q `Set.member` reservedNames =
          Left ("reserved [[queue]] name: " <> T.unpack q <> " (it shadows a top-level API path)")
      | otherwise = Right ()
    validIdent q = case T.uncons q of
      Just (c, _) -> not (isDigit c) && T.all okChar q
      Nothing -> False
    okChar c = isAsciiLower c || isAsciiUpper c || isDigit c || c == '_'
    -- Valid-identifier segments served alongside the per-queue routes.
    reservedNames = Set.fromList ["queues", "events", "cron", "workers", "concurrency"]
    uniqueCrons =
      let ns = [cnName c | q <- cfgQueues cfg, c <- qCron q]
       in if Set.size (Set.fromList ns) == length ns
            then Right ()
            else Left "duplicate cron name: a schedule name must be unique across every [[queue]]"
    tel = cfgTelemetry cfg
    apiPort = serverPort cfg
    metricsPort = tcMetricsPort tel
    metricsBound = tcEnabled tel && tcMetrics tel
    inRange name p
      | p < 1 || p > 65535 = Left (name <> " out of range: " <> show p)
      | otherwise = Right ()
    validPort = inRange "server.port" apiPort
    validMetricsPort = if metricsBound then inRange "telemetry.metrics_port" metricsPort else Right ()
    validGaugeInterval =
      if tcEnabled tel then positive "telemetry.gauge_interval_secs" (tcGaugeIntervalSecs tel) else Right ()
    validShutdownGrace = nonNegative "server.shutdown_grace_secs" (serverShutdownGraceSecs cfg)
    distinctPorts
      | metricsBound && metricsPort == apiPort =
          Left ("telemetry.metrics_port collides with server.port: " <> show metricsPort)
      | otherwise = Right ()

-- | A configured value must be above zero, reported against the key that set it.
positive :: (Num a, Ord a, Show a) => String -> a -> Either String ()
positive at v = if v > 0 then Right () else Left (at <> " must be positive: " <> show v)

-- | As 'positive', where zero is a meaningful setting.
nonNegative :: (Num a, Ord a, Show a) => String -> a -> Either String ()
nonNegative at v = if v >= 0 then Right () else Left (at <> " must not be negative: " <> show v)

-- | The request template a webhook url resolves to.
parseWebhookUrl :: String -> Maybe Request
parseWebhookUrl url = parseRequest ("POST " <> url)

-- | Check a queue's @[queue.webhook]@ block.
validWebhook :: QueueC -> Either String ()
validWebhook q = traverse_ (\wh -> sequence_ [validUrl wh, validTimeout wh, validWorkers wh]) (qWebhook q)
  where
    at key = "queue \"" <> T.unpack (qName q) <> "\" webhook." <> key
    validUrl wh = case parseWebhookUrl (T.unpack (wcUrl wh)) of
      Nothing -> Left (at "url" <> " is not a valid absolute http(s) url: " <> T.unpack (wcUrl wh))
      Just _ -> Right ()
    validTimeout wh = traverse_ (positive (at "timeout_secs")) (wcTimeout wh)
    validWorkers wh = traverse_ inRange (wcWorkers wh)
      where
        inRange w
          | w < 1 || w > 4096 = Left (at "workers" <> " out of range (1-4096): " <> show w)
          | otherwise = Right ()

-- | Check a queue's @[[queue.cron]]@ blocks. An invalid expression fails at load.
validCron :: QueueC -> Either String ()
validCron q = traverse_ validBackfill (qCron q) *> void (queueCronJobs q)
  where
    validBackfill c = traverse_ (positive (cronAt q c <> ".backfill")) (cnBackfill c)

-- | Check a @[[ratelimit]]@ block. A zero @max@ admits nothing, which is allowed.
validRateLimit :: RateLimitPolicyC -> Either String ()
validRateLimit p =
  sequence_
    [ nonNegative (at "max") (rlpMax p)
    , nonNegative (at "refill") (rlpRefill p)
    , positive (at "interval") (rlpInterval p)
    ]
  where
    at key = "ratelimit \"" <> T.unpack (rlpPrefix p) <> "\"." <> key

-- | Check a @[[concurrency]]@ block. The limit is bounded by its 32-bit storage.
validConcurrency :: ConcurrencyPolicyC -> Either String ()
validConcurrency p
  | limit < 1 || limit > maxLimit =
      Left (at <> " out of range (1-" <> show maxLimit <> "): " <> show limit)
  | otherwise = Right ()
  where
    limit = cpcLimit p
    at = "concurrency \"" <> T.unpack (cpcPrefix p) <> "\".limit"
    maxLimit = fromIntegral (maxBound :: Int32)

-- | Check a queue's @[queue.worker]@ block, including the timing interlocks.
validWorker :: QueueC -> Either String ()
validWorker q = sequence_ (positives <> backoffs <> [order, strategyKeys, drain])
  where
    t = wsTimings (workerSettings (qWorker q))
    at key = "queue \"" <> T.unpack (qName q) <> "\" worker." <> key
    drain =
      traverse_
        (nonNegative (at "graceful_shutdown_timeout"))
        (qWorker q >>= wkGracefulShutdownTimeout)
    positives =
      [ positive (at key) v
      | (key, v) <-
          [ ("poll_interval", timingPollInterval t)
          , ("visibility_timeout", timingVisibilityTimeout t)
          , ("job_heartbeat_interval", timingJobHeartbeatInterval t)
          , ("worker_heartbeat_interval", timingWorkerHeartbeatInterval t)
          , ("worker_stale_threshold", timingWorkerStaleThreshold t)
          , ("reaper_interval", timingReaperInterval t)
          , ("reaper_timeout", timingReaperTimeout t)
          ]
      ]
    backoffs = foldMap positiveValues (qWorker q >>= wkBackoff)
    positiveValues b = [positive (at ("backoff." <> T.unpack key)) v | (key, Just v) <- backoffKeys b]
    order
      | timingJobHeartbeatInterval t >= timingVisibilityTimeout t =
          Left (at "job_heartbeat_interval" <> " must be below visibility_timeout, or a running job is reclaimed")
      | timingWorkerHeartbeatInterval t >= timingWorkerStaleThreshold t =
          Left (at "worker_heartbeat_interval" <> " must be below worker_stale_threshold, or a live worker is swept")
      | otherwise = Right ()
    strategyKeys = traverse_ reject (foldMap setKeys (qWorker q >>= wkBackoff))
      where
        strategy = fromMaybe ExponentialS (qWorker q >>= wkBackoff >>= bkStrategy)
        reject key
          | key `elem` strategyKeysFor strategy = Right ()
          | otherwise = Left (at ("backoff." <> T.unpack key) <> " does not apply to strategy " <> show strategy)
    setKeys b = [key | (key, Just _) <- backoffKeys b]

-- | Resolve every secret reference and @${ENV}@ interpolation into literal values.
resolveConfig :: Config -> IO Config
resolveConfig cfg = do
  let db = cfgDatabase cfg
  url <- resolveReq "database.url" (dbUrlFile db) (dbUrl db)
  sch <- traverse interpolate (dbSchema db)
  srv <- traverse resolveServer (cfgServer cfg)
  qs <- traverse resolveQueue (cfgQueues cfg)
  pure
    cfg
      { cfgDatabase = db {dbUrl = url, dbSchema = sch}
      , cfgServer = srv
      , cfgQueues = qs
      }

resolveServer :: ServerC -> IO ServerC
resolveServer s = (\h -> s {srvHost = h}) <$> traverse interpolate (srvHost s)

resolveQueue :: QueueC -> IO QueueC
resolveQueue q = (\wh -> q {qWebhook = wh}) <$> traverse resolveWebhook (qWebhook q)
  where
    resolveWebhook wh = do
      u <- interpolate (wcUrl wh)
      s <- resolveOpt "webhook.secret" (wcSecretFile wh) (wcSecret wh)
      pure wh {wcUrl = u, wcSecret = s}

-- | A required value from a @*_file@ or an interpolated inline value. Empty is rejected.
resolveReq :: String -> Maybe Text -> Text -> IO Text
resolveReq label mFile inline = do
  raw <- case mFile of
    Just p -> readSecretFile p
    Nothing
      | T.null inline -> configError (label <> " is required: set it, its *_file, or a ${ENV} reference")
      | otherwise -> interpolate inline
  if T.null raw then configError (label <> " resolved to an empty value") else pure raw

-- | An optional value. When configured (file or inline) it must be non-empty.
resolveOpt :: String -> Maybe Text -> Maybe Text -> IO (Maybe Text)
resolveOpt label mFile mInline = case (mFile, mInline) of
  (Nothing, Nothing) -> pure Nothing
  _ -> Just <$> resolveReq label mFile (fromMaybe "" mInline)

readSecretFile :: Text -> IO Text
readSecretFile p = T.strip <$> readUtf8 (T.unpack p)

-- | Read a file as UTF-8 whatever the process locale is.
readUtf8 :: FilePath -> IO Text
readUtf8 path = do
  bytes <- try (BS.readFile path) :: IO (Either IOException BS.ByteString)
  case bytes of
    Left err -> configError ("cannot read " <> path <> ": " <> show err)
    Right raw -> orFail (first (\err -> path <> " is not valid UTF-8: " <> show err) (TE.decodeUtf8' raw))

-- | Replace every @${NAME}@ with its environment value, or fail if unset.
interpolate :: Text -> IO Text
interpolate t = case T.breakOn "${" t of
  (_, rest) | T.null rest -> pure t
  (before, rest) -> case T.breakOn "}" (T.drop 2 rest) of
    (_, close) | T.null close -> configError "unterminated ${...} in config value"
    (name, close) ->
      lookupEnv (T.unpack name)
        >>= maybe
          (configError ("environment variable not set: " <> T.unpack name))
          (\val -> ((before <> T.pack val) <>) <$> interpolate (T.drop 1 close))

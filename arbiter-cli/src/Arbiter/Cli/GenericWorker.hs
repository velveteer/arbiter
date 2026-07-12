{-# LANGUAGE DataKinds #-}
{-# LANGUAGE RankNTypes #-}

-- | Worker pools over queues named at runtime, with raw-JSON payloads. The queue name is
-- reflected to a type-level symbol, so the standard worker machinery runs unchanged.
module Arbiter.Cli.GenericWorker
  ( GenericHandler
  , PoolEnv (..)
  , genericPool
  , queuePoolSize
  , ClaimAdmission (..)
  ) where

import Arbiter.Core.Job.Types (JobRead, defaultObservabilityHooks)
import Arbiter.Core.PoolConfig (PoolConfig (..), poolConfigForWorkers)
import Arbiter.Hasql (HasqlEnv, HasqlPool, createHasqlEnvWithPool, runHasqlDb, setPreparedStatements)
import Arbiter.Serve (ServePool (..), instrumentPool)
import Arbiter.Worker
  ( BatchCallbacks (..)
  , ClaimAdmission (..)
  , WorkerConfig (..)
  , applyRetryPolicy
  , applyWorkerTimings
  , defaultBatchedWorkerConfig
  , defaultMaintenanceConfig
  , runWorkerPool
  )
import Arbiter.Worker.Cron (CronJob)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (Value)
import Data.ByteString (ByteString)
import Data.Function ((&))
import Data.List.NonEmpty (NonEmpty (..))
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import GHC.TypeLits (KnownSymbol, SomeSymbol (..), someSymbolVal)

import Arbiter.Cli.Config (WorkerSettings (..))

-- | Handle one claimed job whose payload is raw JSON. An exception retries it.
type GenericHandler = JobRead Value -> IO ()

-- | What every pool in the process shares, so only a queue's own knobs vary per pool.
data PoolEnv = PoolEnv
  { peConnStr :: ByteString
  -- ^ PostgreSQL connection string (also used for LISTEN\/NOTIFY).
  , pePool :: HasqlPool
  -- ^ Connections every queue's pool draws from, so a quiet queue holds none.
  , peSchema :: Text
  , peQueues :: [Text]
  -- ^ Every queue this process serves. See 'maintenanceQueues'.
  , peAdmission :: ClaimAdmission
  -- ^ Admission the claim enforces. A raw-JSON payload carries no selectors to derive it from.
  , pePrepared :: Bool
  -- ^ Prepare the claim once per connection. Off behind a pooler that cannot.
  }

-- | A pool for one runtime-named queue, plus its cron schedules. Without a handler it runs
-- only the background loops, for a queue served over HTTP instead.
genericPool :: PoolEnv -> Text -> WorkerSettings -> [CronJob Value] -> Maybe (Int, GenericHandler) -> IO ServePool
genericPool pe queue ws crons mHandler =
  withQueueEnv pe queue $ \env -> do
    config <- case mHandler of
      Nothing -> defaultMaintenanceConfig (peConnStr pe)
      -- Batched mode over one job: the outbound call must not hold a pooled connection open.
      Just (workers, handler) ->
        defaultBatchedWorkerConfig (peConnStr pe) workers 1 $ \(job :| _) cbs -> do
          liftIO (handler job)
          ack cbs job
    let cfg =
          (config & applyWorkerTimings (wsTimings ws) & applyRetryPolicy (wsRetry ws))
            { claimAdmissionOverride = Just (peAdmission pe)
            , maintenanceQueues = Just (peQueues pe)
            , livenessFile = Nothing
            , cronJobs = crons
            }
    pure
      ServePool
        { poolQueue = queue
        , poolRetry = wsRetry ws
        , poolHooks = defaultObservabilityHooks
        , poolRun = \senv -> runHasqlDb env (runWorkerPool (instrumentPool senv queue cfg))
        }

-- | Connections a queue's pool needs of the process-wide pool.
queuePoolSize :: Maybe Int -> [CronJob Value] -> Int
queuePoolSize mWorkers crons =
  maybe 1 (poolSize . poolConfigForWorkers) mWorkers + if null crons then 0 else 1

-- | Reflect the runtime queue name to a type-level symbol.
withQueueEnv
  :: PoolEnv
  -> Text
  -> (forall sym. (KnownSymbol sym) => HasqlEnv '[ '(sym, Value)] -> IO a)
  -> IO a
withQueueEnv pe queue act =
  case someSymbolVal (T.unpack queue) of
    SomeSymbol (Proxy :: Proxy sym) ->
      act
        . setPreparedStatements (pePrepared pe)
        $ createHasqlEnvWithPool (Proxy @'[ '(sym, Value)]) (pePool pe) (peSchema pe)

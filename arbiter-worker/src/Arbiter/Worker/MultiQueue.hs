{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Naming, sizing, and coordinated lifecycle for multiple worker pools.
module Arbiter.Worker.MultiQueue
  ( NamedWorkerPool (..)
  , namedWorkerPool
  , shutdownPools
  , runWorkerPools
  , runSelectedWorkerPools
  , poolConfigForWorkers
  ) where

import Arbiter.Core.HighLevel (QueueOperation)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Types (RegistryAdmissionPolicies)
import Arbiter.Core.JobResult (EncodeJobResult)
import Arbiter.Core.MonadArbiter (MonadArbiter (..), ResultOf)
import Arbiter.Core.PoolConfig (PoolConfig (..), defaultPoolConfig)
import Arbiter.Core.QueueRegistry (RegistryTables)
import Arbiter.Core.Threads (labelArbiterThread)
import Control.Exception qualified as E
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Foldable (traverse_)
import Data.Text (Text)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Async qualified as Async
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config (WorkerConfig (..))
import Arbiter.Worker.EnabledQueues (enabledQueuesForMonad)
import Arbiter.Worker.Logger (LogConfig (..), (.=))
import Arbiter.Worker.Pool (runWorkerPool)
import Arbiter.Worker.WorkerState (WorkerState (ShuttingDown))

-- | A worker pool paired with its registry-derived queue name.
data NamedWorkerPool m
  = forall payload.
  ( EncodeJobResult (ResultOf m payload)
  , QueueOperation m payload
  , RegistryAdmissionPolicies (RegistryOf m)
  , RegistryTables (RegistryOf m)
  ) =>
  NamedWorkerPool
  { workerPoolName :: Text
  , workerPoolConfig :: WorkerConfig m payload
  }

-- | Name a pool from its payload's registry entry.
namedWorkerPool
  :: forall payload m
   . ( EncodeJobResult (ResultOf m payload)
     , QueueOperation m payload
     , RegistryAdmissionPolicies (RegistryOf m)
     , RegistryTables (RegistryOf m)
     )
  => WorkerConfig m payload
  -> NamedWorkerPool m
namedWorkerPool cfg = NamedWorkerPool (Arb.queueTable @payload @m) cfg

-- | Run the pools selected by @ARBITER_ENABLED_QUEUES@.
runWorkerPools
  :: forall m
   . (MonadUnliftIO m, RegistryTables (RegistryOf m))
  => [NamedWorkerPool m]
  -> m ()
runWorkerPools pools = do
  enabled <- liftIO $ enabledQueuesForMonad @m
  runSelectedWorkerPools enabled pools

-- | Signal graceful shutdown to every pool atomically.
shutdownPools :: (MonadIO m) => [NamedWorkerPool m'] -> m ()
shutdownPools pools =
  liftIO . STM.atomically $
    traverse_ (`STM.writeTVar` ShuttingDown) [workerStateVar cfg | NamedWorkerPool _ cfg <- pools]

-- | Run only named pools. A pool that exits winds down its peers. If it failed,
-- its exception is rethrown after every peer has been joined.
runSelectedWorkerPools
  :: forall m
   . (MonadUnliftIO m)
  => [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPools enabled pools =
  case filter (\(NamedWorkerPool name _) -> name `elem` enabled) pools of
    [] -> pure ()
    selected -> evalContT $ do
      asyncs <- traverse withPoolAsync selected
      lift $ do
        (_, firstResult) <- Async.waitAnyCatch asyncs
        shutdownPools selected
        traverse_ Async.waitCatch asyncs
        either (liftIO . E.throwIO) pure firstResult
  where
    withPoolAsync :: NamedWorkerPool m -> ContT () m (Async.Async ())
    withPoolAsync (NamedWorkerPool name cfg) =
      let cfg' = cfg {logConfig = withPoolContext name (logConfig cfg)}
       in ContT $ Async.withAsync (labelArbiterThread "pool" (Just name) >> runWorkerPool cfg')

-- | Inject the pool name into log context. User context wins on collision.
withPoolContext :: Text -> LogConfig -> LogConfig
withPoolContext poolName lc =
  lc {additionalContext = (("pool" .= poolName) :) <$> additionalContext lc}

-- | A single-stripe pool sized at twice the enabled worker count plus one for
-- the listener, with a minimum size of three.
poolConfigForWorkers
  :: forall m
   . (RegistryTables (RegistryOf m))
  => [NamedWorkerPool m]
  -> IO PoolConfig
poolConfigForWorkers pools = do
  enabled <- enabledQueuesForMonad @m
  let n = sum [workerCount cfg | NamedWorkerPool name cfg <- pools, name `elem` enabled]
  pure defaultPoolConfig {poolSize = max 2 (2 * n) + 1}

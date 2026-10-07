{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | Naming, sizing, and coordinated lifecycle for multiple worker pools.
module Arbiter.Worker.MultiQueue
  ( NamedWorkerPool (..)
  , namedWorkerPool
  , WorkerPoolSelectionException (..)
  , shutdownWorkerPools
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
import Control.Monad (unless)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Cont (ContT (..), evalContT)
import Data.Foldable (traverse_)
import Data.Maybe (fromMaybe)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import UnliftIO (MonadUnliftIO)
import UnliftIO.Async qualified as Async
import UnliftIO.STM qualified as STM

import Arbiter.Worker.Config
  ( WorkerConfig (..)
  , WorkerConfigException (..)
  , validateWorkerConfig
  , workerStateVar
  )
import Arbiter.Worker.EnabledQueues (getEnabledQueues, requestedQueues)
import Arbiter.Worker.Logger (LogConfig (..), (.=))
import Arbiter.Worker.Pool (runWorkerPool)
import Arbiter.Worker.WorkerState (WorkerState (ShuttingDown))

-- | A requested queue has no configured pool, or selection is empty.
newtype WorkerPoolSelectionException = WorkerPoolSelectionException Text
  deriving stock (Eq, Show)
  deriving anyclass (E.Exception)

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
  -- ^ The queue name from the registry.
  , workerPoolConfig :: WorkerConfig m payload
  -- ^ The pool's config. Get it by pattern match.
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

-- | Run the pools selected by @ARBITER_ENABLED_QUEUES@, or every configured
-- pool when it is unset or blank. Throws 'Arbiter.Core.Exceptions.InternalException'
-- when @ARBITER_ENABLED_QUEUES@ has an unknown name or names no queues, and
-- 'WorkerPoolSelectionException' for a selected queue with no pool, or when no pool
-- is selected.
runWorkerPools
  :: forall m
   . (MonadUnliftIO m, RegistryTables (RegistryOf m))
  => [NamedWorkerPool m]
  -> m ()
runWorkerPools pools = do
  requested <- liftIO $ requestedQueues (Proxy @(RegistryOf m))
  runSelectedWorkerPools (fromMaybe [name | NamedWorkerPool name _ <- pools] requested) pools

-- | Signal graceful shutdown to every pool atomically.
shutdownWorkerPools :: (MonadIO m) => [NamedWorkerPool m'] -> m ()
shutdownWorkerPools pools =
  liftIO . STM.atomically $
    traverse_ (`STM.writeTVar` ShuttingDown) [workerStateVar cfg | NamedWorkerPool _ cfg <- pools]

-- | Run only named pools. A pool that exits winds down its peers. After every peer has
-- been joined, the failure of the first failed pool in list order is rethrown. Throws
-- 'WorkerPoolSelectionException' for a name with no pool, or when no pool is selected.
runSelectedWorkerPools
  :: forall m
   . (MonadUnliftIO m)
  => [Text]
  -> [NamedWorkerPool m]
  -> m ()
runSelectedWorkerPools enabled pools = do
  let available = Set.fromList [name | NamedWorkerPool name _ <- pools]
      missing = Set.toList (Set.fromList enabled `Set.difference` available)
  unless (null missing)
    $ liftIO . E.throwIO . WorkerPoolSelectionException
    $ "No worker pool configured for: " <> T.intercalate ", " missing
  case filter (\(NamedWorkerPool name _) -> name `elem` enabled) pools of
    [] -> liftIO $ E.throwIO (WorkerPoolSelectionException "No worker pools selected")
    selected -> evalContT $ do
      asyncs <- traverse withPoolAsync selected
      lift $ do
        _ <- Async.waitAnyCatch asyncs
        shutdownWorkerPools selected
        results <- traverse Async.waitCatch asyncs
        either (liftIO . E.throwIO) pure (sequence_ results)
  where
    withPoolAsync :: NamedWorkerPool m -> ContT () m (Async.Async ())
    withPoolAsync (NamedWorkerPool name cfg) =
      let cfg' = cfg {logConfig = withPoolContext name (logConfig cfg)}
       in ContT $ Async.withAsync (labelArbiterThread "pool" (Just name) >> runWorkerPool cfg')

-- | Name the pool a message came from.
withPoolContext :: Text -> LogConfig -> LogConfig
withPoolContext poolName logCfg =
  logCfg {identityContext = identityContext logCfg <> ["pool" .= poolName]}

-- | Pool connections per handler thread of an enabled pool.
connectionsPerWorker :: Int
connectionsPerWorker = 2

-- | Floor on the worker share of the pool.
minWorkerConnections :: Int
minWorkerConnections = 2

-- | Pool connections for the listener.
listenerConnections :: Int
listenerConnections = 1

-- | A single-stripe connection pool sized at twice the enabled pools' thread count plus one
-- for the listener, with a minimum size of three. Unset or blank
-- @ARBITER_ENABLED_QUEUES@ enables all queues. Throws 'WorkerConfigException' for an
-- invalid enabled pool config, and 'Arbiter.Core.Exceptions.InternalException' when
-- @ARBITER_ENABLED_QUEUES@ has an unknown name or names no queues.
poolConfigForWorkers
  :: forall m
   . (RegistryTables (RegistryOf m))
  => [NamedWorkerPool m]
  -> IO PoolConfig
poolConfigForWorkers pools = do
  enabled <- getEnabledQueues (Proxy @(RegistryOf m))
  traverse_ (validateSelected enabled) pools
  let enabledWorkers = sum [workerCount cfg | NamedWorkerPool name cfg <- pools, name `elem` enabled]
  pure
    defaultPoolConfig {poolSize = max minWorkerConnections (connectionsPerWorker * enabledWorkers) + listenerConnections}
  where
    validateSelected :: [Text] -> NamedWorkerPool m -> IO ()
    validateSelected enabled (NamedWorkerPool name cfg)
      | name `notElem` enabled = pure ()
      | otherwise = either (E.throwIO . WorkerConfigException) pure (validateWorkerConfig cfg)

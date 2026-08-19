{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

-- | Schema-wide maintenance coordinated across worker pools.
module Arbiter.Worker.Reaper
  ( reaperLoop
  , runReaperOp
  ) where

import Arbiter.Core.Concurrency.Spec (registryConcurrencyPolicies)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Arbiter.Core.RateLimit.Spec (registryRateLimitPolicies)
import Control.Monad (forever, when)
import Data.Aeson (FromJSON, ToJSON)
import Data.Foldable (fold, traverse_)
import Data.Int (Int64)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import UnliftIO (MonadUnliftIO, tryAny)
import UnliftIO.Concurrent (threadDelay)

import Arbiter.Worker.Config (MaintenanceOp (..), maintenanceOpName)
import Arbiter.Worker.Logger (LogConfig, LogLevel (..), tryLog, warnEx)
import Arbiter.Worker.Logger.Internal (runHook)

-- | Run schema-wide maintenance, with each operation independently gated
-- across all worker pools.
reaperLoop
  :: forall m
   . ( Arb.RegistryAdmissionPolicies (RegistryOf m)
     , MonadArbiter m
     , RegistryTables (RegistryOf m)
     )
  => LogConfig
  -> (MaintenanceOp -> Int64 -> m ())
  -> NominalDiffTime
  -> NominalDiffTime
  -> m ()
reaperLoop logCfg report interval stmtTimeout = do
  let reaped op n = runHook logCfg "onMaintenance" $ report op n
      intervalMicros = ceiling interval * 1_000_000
      queues = registryTableNames (Proxy @(RegistryOf m))
      pruneInterval = interval * 12
      hasConcurrency = not (Set.null (registryConcurrencyPolicies @(RegistryOf m)))
      hasRateLimit = not (Set.null (registryRateLimitPolicies @(RegistryOf m)))
  schema <- Arb.getSchema
  let gatedCount op every work =
        runReaperOp logCfg schema stmtTimeout (maintenanceOpName op) every work
          >>= traverse_ (reaped op)
      reportFailed op =
        traverse_ (\queue -> tryLog logCfg Warning $ maintenanceOpName op <> " failed for queue: " <> queue)
      reportSwept op done =
        traverse_ (\(n, failed) -> reaped op n >> reportFailed op failed >> when (n > 0) (done n))
      sweep op every done work =
        runReaperOp logCfg schema stmtTimeout (maintenanceOpName op) every work
          >>= reportSwept op done
      refreshGroups =
        runReaperStateOp
          logCfg
          schema
          stmtTimeout
          (maintenanceOpName RefreshGroups)
          interval
          (Ops.refreshAllGroups schema queues . fold)
          >>= reportSwept RefreshGroups (const (pure ()))
  forever $ do
    refreshGroups
    gatedCount SweepStaleWorkers interval $ Ops.sweepStaleWorkers schema
    sweep
      SweepExhaustedJobs
      interval
      (\n -> tryLog logCfg Warning $ "Reaper moved " <> T.pack (show n) <> " exhausted job(s) to the DLQ")
      $ Ops.sweepExhaustedJobs schema queues
    sweep
      SweepCancelledJobs
      interval
      (\n -> tryLog logCfg Info $ "Reaper deleted " <> T.pack (show n) <> " orphaned cancelled job(s)")
      $ Ops.sweepCancelledJobs schema queues
    when hasRateLimit
      $ gatedCount PruneRateLimitBuckets pruneInterval
      $ Arb.pruneRateLimitBuckets @m interval
    when hasConcurrency $ do
      gatedCount ReconcileConcurrencyStale interval $ Arb.reconcileConcurrencyCountsIfStale @m
      gatedCount ReconcilePruneConcurrency pruneInterval $ Arb.reconcileAndPruneConcurrency @m
    sweep
      PurgeArchives
      interval
      (\n -> tryLog logCfg Info $ "Reaper purged " <> T.pack (show n) <> " archived job(s)")
      $ Ops.purgeArchives schema queues
    threadDelay intervalMicros

-- | Run one gated maintenance operation. Database statement timeouts bound
-- individual statements. Failures are logged and do not stop the loop.
runReaperOp
  :: (MonadArbiter m)
  => LogConfig
  -> SchemaName
  -> NominalDiffTime
  -> Text
  -> NominalDiffTime
  -> m a
  -> m (Maybe a)
runReaperOp logCfg schema stmtTimeout task every work =
  reaperGate logCfg task $
    Ops.runGatedBounded schema task every stmtTimeout work

runReaperStateOp
  :: (FromJSON s, MonadArbiter m, ToJSON s)
  => LogConfig
  -> SchemaName
  -> NominalDiffTime
  -> Text
  -> NominalDiffTime
  -> (Maybe s -> m (a, s))
  -> m (Maybe a)
runReaperStateOp logCfg schema stmtTimeout task every work =
  reaperGate logCfg task $
    Ops.runGatedStateBounded schema task every stmtTimeout work

reaperGate :: (MonadUnliftIO m) => LogConfig -> Text -> m (Maybe a) -> m (Maybe a)
reaperGate logCfg task action =
  tryAny action >>= either (\e -> Nothing <$ warnEx logCfg ("Reaper op failed: " <> task) e) pure

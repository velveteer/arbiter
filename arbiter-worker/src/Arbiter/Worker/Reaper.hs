{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeApplications #-}
{-# LANGUAGE TypeFamilies #-}

-- | Schema-wide maintenance coordinated across worker pools.
module Arbiter.Worker.Reaper
  ( reaperLoop
  , runMaintenancePass
  , MaintenancePace (..)
  , runReaperOp
  ) where

import Arbiter.Core.Concurrency.Spec (registryConcurrencyPolicies)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Arbiter.Core.RateLimit.Spec (registryRateLimitPolicies)
import Control.Monad (forever, void, when)
import Data.Aeson (FromJSON, ToJSON)
import Data.Either (fromRight)
import Data.Foldable (fold, traverse_)
import Data.Int (Int64)
import Data.Maybe (catMaybes)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import UnliftIO (MonadUnliftIO, SomeException, tryAny)
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
  -> MaintenancePace
  -> NominalDiffTime
  -> m ()
reaperLoop logCfg report pace stmtTimeout =
  forever $ do
    void $ runMaintenancePass logCfg report pace stmtTimeout
    threadDelay (ceiling (paceWindow pace) * 1_000_000)

-- | Gaps a caller holds between runs of each kind of work. A zero gap runs it every pass.
data MaintenancePace = MaintenancePace
  { paceWindow :: NominalDiffTime
  -- ^ Gap between runs of one ordinary operation.
  , paceSparseWindow :: NominalDiffTime
  -- ^ Gap between runs of one whole-schema operation.
  , paceBucketIdle :: NominalDiffTime
  -- ^ Idle age at which a prune collects a rate-limit bucket.
  }
  deriving stock (Eq, Show)

-- | One pass of the maintenance the reaper runs, with each operation independently
-- gated across all callers. An operation whose window has not elapsed is skipped.
-- Returns the operations that failed. A failure does not stop the pass.
runMaintenancePass
  :: forall m
   . ( Arb.RegistryAdmissionPolicies (RegistryOf m)
     , MonadArbiter m
     , RegistryTables (RegistryOf m)
     )
  => LogConfig
  -> (MaintenanceOp -> Int64 -> m ())
  -> MaintenancePace
  -> NominalDiffTime
  -> m [MaintenanceOp]
runMaintenancePass logCfg report pace stmtTimeout = do
  let reaped operation count = runHook logCfg "onMaintenance" $ report operation count
      queues = registryTableNames (Proxy @(RegistryOf m))
      window = paceWindow pace
      sparseWindow = paceSparseWindow pace
      hasConcurrency = not (Set.null (registryConcurrencyPolicies @(RegistryOf m)))
      hasRateLimit = not (Set.null (registryRateLimitPolicies @(RegistryOf m)))
  schema <- Arb.getSchema
  let gatedCount operation every work =
        tryReaperOp logCfg schema stmtTimeout (maintenanceOpName operation) every work
          >>= reportOutcome operation (traverse_ (reaped operation))
      reportFailed operation =
        traverse_ (\queue -> tryLog logCfg Warning $ maintenanceOpName operation <> " failed for queue: " <> queue)
      reportSwept operation done =
        traverse_ (\(count, failed) -> reaped operation count >> reportFailed operation failed >> when (count > 0) (done count))
      sweep operation every done work =
        tryReaperOp logCfg schema stmtTimeout (maintenanceOpName operation) every work
          >>= reportOutcome operation (reportSwept operation done)
      refreshGroups =
        runReaperStateOp
          logCfg
          schema
          stmtTimeout
          (maintenanceOpName RefreshGroups)
          window
          (Ops.refreshAllGroups schema queues . fold)
          >>= reportOutcome RefreshGroups (reportSwept RefreshGroups (const (pure ())))
  fmap catMaybes . sequence $
    [ refreshGroups
    , gatedCount SweepStaleWorkers window $ Ops.sweepStaleWorkers schema
    , sweep
        SweepExhaustedJobs
        window
        (\count -> tryLog logCfg Warning $ "Reaper moved " <> T.pack (show count) <> " exhausted job(s) to the DLQ")
        $ Ops.sweepExhaustedJobs schema queues
    , sweep
        SweepCancelledJobs
        window
        (\count -> tryLog logCfg Info $ "Reaper deleted " <> T.pack (show count) <> " orphaned cancelled job(s)")
        $ Ops.sweepCancelledJobs schema queues
    ]
      <> [gatedCount PruneRateLimitBuckets sparseWindow (Arb.pruneRateLimitBuckets @m (paceBucketIdle pace)) | hasRateLimit]
      <> [gatedCount ReconcileConcurrencyStale window (Arb.reconcileConcurrencyCountsIfStale @m) | hasConcurrency]
      <> [gatedCount ReconcilePruneConcurrency sparseWindow (Arb.reconcileAndPruneConcurrency @m) | hasConcurrency]
      <> [ sweep
             PurgeArchives
             window
             (\count -> tryLog logCfg Info $ "Reaper purged " <> T.pack (show count) <> " archived job(s)")
             $ Ops.purgeArchives schema queues
         ]

-- | Name a failed operation. Report a completed one through @emit@.
reportOutcome
  :: (Monad m)
  => MaintenanceOp
  -> (a -> m ())
  -> Either SomeException a
  -> m (Maybe MaintenanceOp)
reportOutcome operation emit = either (const (pure (Just operation))) (\ran -> Nothing <$ emit ran)

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
  fromRight Nothing <$> tryReaperOp logCfg schema stmtTimeout task every work

-- | 'runReaperOp', keeping the failure. @Right Nothing@ is an operation already running.
tryReaperOp
  :: (MonadArbiter m)
  => LogConfig
  -> SchemaName
  -> NominalDiffTime
  -> Text
  -> NominalDiffTime
  -> m a
  -> m (Either SomeException (Maybe a))
tryReaperOp logCfg schema stmtTimeout task every work =
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
  -> m (Either SomeException (Maybe a))
runReaperStateOp logCfg schema stmtTimeout task every work =
  reaperGate logCfg task $
    Ops.runGatedStateBounded schema task every stmtTimeout work

reaperGate :: (MonadUnliftIO m) => LogConfig -> Text -> m a -> m (Either SomeException a)
reaperGate logCfg task action =
  tryAny action
    >>= either (\exception -> Left exception <$ warnEx logCfg ("Reaper op failed: " <> task) exception) (pure . Right)

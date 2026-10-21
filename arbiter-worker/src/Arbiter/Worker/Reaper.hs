{-# LANGUAGE OverloadedStrings #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Internal to the arbiter packages. Not covered by the PVP.
-- 'runMaintenancePass' and 'MaintenancePace' are public through "Arbiter.Worker".
--
-- Schema-wide maintenance coordinated across worker pools.
module Arbiter.Worker.Reaper
  ( reaperLoop
  , runMaintenancePass
  , MaintenancePace (..)
  , runReaperOp
  ) where

import Arbiter.Core.Concurrency.Spec (registryConcurrencyPolicies)
import Arbiter.Core.Enum (showText)
import Arbiter.Core.HighLevel qualified as Arb
import Arbiter.Core.Job.Schema (SchemaName)
import Arbiter.Core.MonadArbiter (MonadArbiter, RegistryOf)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (RegistryTables (..))
import Arbiter.Core.RateLimit.Spec (registryRateLimitPolicies)
import Control.Monad (forever, void, when)
import Data.Either (fromRight)
import Data.Foldable (fold, traverse_)
import Data.Int (Int64)
import Data.Maybe (catMaybes)
import Data.Proxy (Proxy (..))
import Data.Set qualified as Set
import Data.Text (Text)
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
    threadDelay (Ops.micros (paceWindow pace))

-- | Gaps between runs of each kind of work, and the bucket idle age. A zero gap
-- runs that work every pass.
data MaintenancePace = MaintenancePace
  { paceWindow :: NominalDiffTime
  -- ^ Gap in seconds between runs of one ordinary operation.
  , paceSparseWindow :: NominalDiffTime
  -- ^ Gap in seconds between runs of 'PruneRateLimitBuckets' and
  -- 'ReconcilePruneConcurrency'.
  , paceBucketIdle :: NominalDiffTime
  -- ^ Idle age in seconds at which a prune collects a full rate-limit bucket.
  }
  deriving stock (Eq, Show)

-- | One pass of the maintenance the reaper runs, with each operation independently
-- gated across all callers. An operation whose window has not elapsed is skipped.
-- The pass covers every queue in the schema ('Ops.schemaQueues'), not only the
-- caller's registry. Which caller wins a gate does not change the result.
-- Returns the operations that threw. A failure for one queue is logged and not
-- returned. A failure does not stop the pass. When the schema's queues cannot be
-- read, no operation runs and every operation is returned.
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
  -- ^ Statement timeout
  -> m [MaintenanceOp]
runMaintenancePass logCfg report pace stmtTimeout = do
  schema <- Arb.getSchema
  tryAny (Ops.schemaQueues schema (registryTableNames (Proxy @(RegistryOf m))))
    >>= either
      (\exception -> [minBound .. maxBound] <$ warnEx logCfg "Reaper queue discovery failed, pass skipped" exception)
      (maintainQueues logCfg report pace stmtTimeout schema)

-- | 'runMaintenancePass' over the given queues.
maintainQueues
  :: forall m
   . ( Arb.RegistryAdmissionPolicies (RegistryOf m)
     , MonadArbiter m
     )
  => LogConfig
  -> (MaintenanceOp -> Int64 -> m ())
  -> MaintenancePace
  -> NominalDiffTime
  -> SchemaName
  -> [Text]
  -> m [MaintenanceOp]
maintainQueues logCfg report pace stmtTimeout schema queues = do
  let reaped operation count = runHook logCfg "onMaintenance" $ report operation count
      window = paceWindow pace
      sparseWindow = paceSparseWindow pace
      hasConcurrency = not (Set.null (registryConcurrencyPolicies @(RegistryOf m)))
      hasRateLimit = not (Set.null (registryRateLimitPolicies @(RegistryOf m)))
      gated :: MaintenanceOp -> (Maybe a -> m ()) -> NominalDiffTime -> m a -> m (Maybe MaintenanceOp)
      gated operation emit every work =
        tryReaperOp logCfg schema stmtTimeout (maintenanceOpName operation) every work >>= failedOp operation emit
      gatedCount operation = gated operation (traverse_ (reaped operation))
      logSwept level verb noun count =
        tryLog logCfg level $ "Reaper " <> verb <> " " <> showText count <> " " <> noun
      reportFailed operation =
        traverse_ (\queue -> tryLog logCfg Warning $ maintenanceOpName operation <> " failed for queue: " <> queue)
      reportSwept operation done =
        traverse_ (\(count, failed) -> reaped operation count >> reportFailed operation failed >> when (count > 0) (done count))
      sweep operation every done = gated operation (reportSwept operation done) every
      refreshGroups =
        let task = maintenanceOpName RefreshGroups
         in reaperGate
              logCfg
              task
              (Ops.runGatedStateBounded schema task window stmtTimeout (Ops.refreshAllGroups schema queues . fold))
              >>= failedOp RefreshGroups (reportSwept RefreshGroups (const (pure ())))
  fmap catMaybes . sequence $
    [ refreshGroups
    , gatedCount SweepStaleWorkers window $ Ops.sweepStaleWorkers schema
    , sweep
        SweepExhaustedJobs
        window
        (logSwept Warning "moved" "exhausted job(s) to the DLQ")
        $ Ops.sweepExhaustedJobs schema queues
    , sweep
        SweepCancelledJobs
        window
        (logSwept Info "deleted" "orphaned cancelled job(s)")
        $ Ops.sweepCancelledJobs schema queues
    ]
      <> [gatedCount PruneRateLimitBuckets sparseWindow (Arb.pruneRateLimitBuckets @m (paceBucketIdle pace)) | hasRateLimit]
      <> [gatedCount ReconcileConcurrencyStale window (Ops.reconcileConcurrencyCountsIfStale schema queues) | hasConcurrency]
      <> [gatedCount ReconcilePruneConcurrency sparseWindow (Ops.reconcileAndPruneConcurrency schema queues) | hasConcurrency]
      <> [ sweep
             PurgeArchives
             window
             (logSwept Info "purged" "archived job(s)")
             $ Ops.purgeArchives schema queues
         ]
      <> [ sweep
             PurgeDLQ
             window
             (logSwept Info "purged" "DLQ job(s)")
             $ Ops.purgeDLQ schema queues
         ]

-- | Name a failed operation. Report a completed one through @emit@.
failedOp
  :: (Monad m)
  => MaintenanceOp
  -> (a -> m ())
  -> Either SomeException a
  -> m (Maybe MaintenanceOp)
failedOp operation emit = either (const (pure (Just operation))) (\ran -> Nothing <$ emit ran)

-- | Run one gated maintenance operation. 'Nothing' when it failed, is running
-- elsewhere, or its window has not elapsed. Failures are logged.
runReaperOp
  :: (MonadArbiter m)
  => LogConfig
  -> NominalDiffTime
  -- ^ Statement timeout
  -> Text
  -- ^ Task name
  -> NominalDiffTime
  -- ^ Gate window: minimum gap between runs across all callers
  -> m a
  -> m (Maybe a)
runReaperOp logCfg stmtTimeout task every work = do
  schema <- Arb.getSchema
  fromRight Nothing <$> tryReaperOp logCfg schema stmtTimeout task every work

-- | 'runReaperOp', keeping the failure. @Right Nothing@ is a gate that ran too recently or is held elsewhere.
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

reaperGate :: (MonadUnliftIO m) => LogConfig -> Text -> m a -> m (Either SomeException a)
reaperGate logCfg task action =
  tryAny action
    >>= either (\exception -> Left exception <$ warnEx logCfg ("Reaper op failed: " <> task) exception) (pure . Right)

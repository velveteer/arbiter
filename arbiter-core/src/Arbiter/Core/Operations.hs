{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

module Arbiter.Core.Operations
  ( -- * Job Insertion
    insertJob
  , insertJobUnsafe
  , insertJobUnsafeStamped
  , insertJobsBatch
  , insertJobsBatchStamped
  , insertJobsBatch_
  , TraceStamp
  , traceStamp
  , insertResult
  , insertResultsBatch
  , getResultsByParent
  , getDLQChildErrorsByParent
  , persistParentState
  , claimNextVisibleJobs
  , claimNextVisibleJobsAs
  , claimNextVisibleJobsBatched
  , ClaimSql (..)
  , mkClaimSql
  , claimJobsCached
  , claimJobsBatchedCached
  , addRateLimitTokens
  , pruneRateLimitBuckets
  , resetRateLimitBuckets
  , wakeThrottledJobs
  , wakeThrottledJobsForKey
  , listRateLimitPolicies
  , getRateLimitPolicy
  , rateLimitPolicyExists
  , listRateLimitBuckets
  , listConcurrencyPolicies
  , getConcurrencyPolicy
  , listConcurrencyKeys
  , updateRateLimitPolicyOverrides
  , updateConcurrencyPolicyOverrides
  , pruneConcurrencyKeys
  , reconcileConcurrencyCounts
  , reconcileConcurrencyCountsIfStale
  , reconcileAndPruneConcurrency
  , ackJob
  , ackJobsBatch
  , lockJobParents
  , lockJobTrees
  , lockJobTreesFromRoot
  , TreeLocks (..)
  , archivesOnAck
  , setVisibilityTimeout
  , setVisibilityTimeoutBatch
  , VisibilityUpdateInfo (..)
  , updateJobForRetry
  , nackJob
  , nackJobsBatch
  , moveToDLQ
  , moveToDLQFields
  , moveToDLQBatch
  , retryFromDLQ
  , dlqJobExists
  , listDLQJobs
  , listDLQJobsByParent
  , countDLQJobsByParent
  , deleteDLQJob
  , deleteDLQJobsBatch
  , deleteCancelledJobs

    -- * Completed-Job Archive
  , listArchiveJobs
  , listArchiveFiltered
  , getArchivedJobById
  , listArchivedJobsByGroupKey
  , countArchiveFiltered
  , purgeArchives
  , deleteArchiveJob
  , deleteArchiveJobsBatch
  , reEnqueueFromArchive
  , updateArchiveResult
  , updateArchiveResultsBatch

    -- * Filtered Query Operations
  , Tmpl.JobFilter (..)
  , listJobsFiltered
  , listJobsFilteredOrdered
  , listJobsWithStatus
  , countJobsFiltered
  , listDLQFiltered
  , listDLQFilteredOrdered
  , countDLQFiltered

    -- * Admin Operations
  , listJobs
  , jobExists
  , getJobById
  , getJobByIdWithStatus
  , getJobByDedupKey
  , getJobsByGroup
  , cancelJob
  , cancelJobsBatch
  , promoteJob
  , QueueStats (..)
  , queueStatusCounts
  , QueueOverview (..)
  , getQueueStats
  , getAllQueueStats

    -- * Count Operations
  , countJobs
  , countJobsByGroup
  , countDLQJobs

    -- * Parent-Child Operations
  , getJobsByParent
  , countJobsByParent
  , countChildrenBatch
  , countDLQChildrenBatch

    -- * Job Dependency Operations
  , pauseChildren
  , resumeChildren
  , cancelJobCascade
  , cancelJobTree
  , forceCancelJob

    -- * Suspend/Resume Operations
  , suspendJob
  , resumeJob

    -- * Groups Table Operations
  , refreshGroupsForQueue
  , refreshAllGroups
  , refreshAllGroupsFully
  , sweepExhaustedJobs
  , sweepCancelledJobs

    -- * Cron Schedule Operations
  , upsertCronDefault
  , listCronSchedules
  , getCronScheduleByName
  , updateCronSchedule
  , touchCronLastFired
  , touchCronChecked
  , tryFireCronGate
  , tryAcquireCronLeader
  , RunRequestOutcome (..)
  , requestCronRun
  , claimCronRun
  , touchCronManualRun
  , pendingCronRuns

    -- * Worker Registry Operations
  , registerWorker
  , heartbeatWorker
  , setWorkerPaused
  , markWorkerShuttingDown
  , deregisterWorker
  , listWorkers
  , sweepStaleWorkers

    -- * Queue Operations
  , ensureQueue
  , setQueuePaused
  , getQueue
  , listQueues

    -- * Global Gate Operations
  , runGated
  , runGatedBounded
  , runGatedShared
  , runGatedState
  , runGatedStateBounded
  , setLocalStatementTimeout
  , micros
  , gateNameFor
  , Shared (..)

    -- * Internal Operations
  , getParentStateSnapshot
  , readChildResultsRaw
  , mergeRawChildResults
  ) where

import Control.Exception qualified as E
import Control.Monad (foldM, unless, void, when)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Data.Aeson (FromJSON (..), Result (..), ToJSON (..), Value, fromJSON, object, withObject, (.:), (.=))
import Data.Aeson.Types (parseEither, parseMaybe)
import Data.Bifunctor (bimap, first, second)
import Data.Bitraversable (bitraverse)
import Data.Either (fromRight, partitionEithers)
import Data.Foldable (for_, toList, traverse_)
import Data.Int (Int32, Int64)
import Data.IntMap qualified as IntMap
import Data.List (groupBy, sort, sortOn)
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, listToMaybe, mapMaybe)
import Data.Monoid (Ap (..), Sum (..))
import Data.Proxy (Proxy (..))
import Data.Sequence ((|>))
import Data.Sequence qualified as Seq
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime)
import Data.UUID.Types (UUID)
import GHC.Generics (Generic)
import UnliftIO (MonadUnliftIO, tryAny, withRunInIO)
import UnliftIO.Timeout qualified as UIO

import Arbiter.Core.Codec
  ( Col (..)
  , RowCodec
  , col
  , jobCodec
  , jobRowCodec
  , ncol
  )
import Arbiter.Core.Concurrency.Spec
  ( ConcurrencyKey (..)
  , HasConcurrency
  , concurrencyFor
  , concurrencyKeyText
  , runConcurrencyFor
  )
import Arbiter.Core.Concurrency.Stats (ConcurrencyKeyView, ConcurrencyPolicyUpdate (..), ConcurrencyPolicyView)
import Arbiter.Core.CronSchedule (CronScheduleRow, CronScheduleUpdate (..))
import Arbiter.Core.CronSchedule qualified as CS
import Arbiter.Core.Exceptions (throwParsing)
import Arbiter.Core.Job.Archive qualified as Archive
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema (SchemaName, TableName)
import Arbiter.Core.Job.Types
  ( AdmissionColumns (..)
  , ClaimSeq
  , DedupKey (IgnoreDuplicate, ReplaceDuplicate)
  , Job (..)
  , JobId
  , JobPayload
  , JobRead
  , JobStatus (..)
  , JobWrite
  , dedupParts
  , isRollup
  , jobStatusFromText
  , jobStatusToText
  )
import Arbiter.Core.MonadArbiter (MonadArbiter, withDbTransaction)
import Arbiter.Core.MonadArbiter qualified as MA
import Arbiter.Core.Queues (QueueRow)
import Arbiter.Core.RateLimit.Spec
  ( HasRateLimit
  , RateLimitKey (..)
  , rateLimitCost
  , rateLimitFor
  , rateLimitKeyText
  , runRateLimitFor
  )
import Arbiter.Core.RateLimit.Stats (RateLimitBucketView, RateLimitPolicyUpdate (..), RateLimitPolicyView)
import Arbiter.Core.Selector (usesAnyPolicy)
import Arbiter.Core.Sql.Archive qualified as Tmpl
import Arbiter.Core.Sql.Claim qualified as Claim
import Arbiter.Core.Sql.Concurrency qualified as Tmpl
import Arbiter.Core.Sql.Cron qualified as Tmpl
import Arbiter.Core.Sql.DLQ qualified as Tmpl
import Arbiter.Core.Sql.Gates qualified as Tmpl
import Arbiter.Core.Sql.Groups qualified as Tmpl
import Arbiter.Core.Sql.Insert (batchFrag, insertFrag)
import Arbiter.Core.Sql.Jobs qualified as Tmpl
import Arbiter.Core.Sql.Lifecycle qualified as Tmpl
import Arbiter.Core.Sql.QQ qualified as QQ
import Arbiter.Core.Sql.Query qualified as Q
import Arbiter.Core.Sql.Queues qualified as Tmpl
import Arbiter.Core.Sql.RateLimit qualified as Tmpl
import Arbiter.Core.Sql.Stats qualified as Tmpl
import Arbiter.Core.Sql.Tree qualified as Tmpl
import Arbiter.Core.Sql.Workers qualified as Tmpl
import Arbiter.Core.Trace (currentTraceContext, stampTraceContext)
import Arbiter.Core.Worker (WorkerRow)

decodePayload :: (JobPayload payload, MonadArbiter m) => JobRead Value -> m (JobRead payload)
decodePayload job = case fromJSON (payload job) of
  Success p -> pure $ job {payload = p}
  Error e -> throwParsing $ "Failed to decode job payload: " <> T.pack e

visibilityUpdateCodec :: RowCodec VisibilityUpdateInfo
visibilityUpdateCodec =
  VisibilityUpdateInfo
    <$> col "id" CInt8
    <*> col "was_heartbeated" CBool
    <*> ncol "current_db_claim_seq" CInt8
    <*> col "cancel_requested" CBool
    <*> col "suspended" CBool
    <*> ncol "claimed_by" CUuid

parentCountCodec :: RowCodec (Int64, (Int64, Int64))
parentCountCodec =
  (\pid cnt paused -> (pid, (cnt, paused)))
    <$> col "parent_id" CInt8
    <*> col "count" CInt8
    <*> col "count_suspended" CInt8

buildWhereClause :: [Tmpl.JobFilter] -> Q.Query ()
buildWhereClause [] = mempty
buildWhereClause filters = Q.raw "WHERE " <> Q.sepBy " AND " (map filterToClause filters)

filterToClause :: Tmpl.JobFilter -> Q.Query ()
filterToClause (Tmpl.FilterGroupKey gk) = [QQ.sql|group_key = #{gk :: CText}|]
filterToClause (Tmpl.FilterParentId pid) = [QQ.sql|parent_id = #{pid :: CInt8}|]
filterToClause Tmpl.FilterRootsOnly = Q.raw "parent_id IS NULL"
filterToClause (Tmpl.FilterStatus s) = [QQ.sql|status = #{st :: CText}|]
  where
    st = jobStatusToText s
filterToClause (Tmpl.FilterId i) = [QQ.sql|id = #{i :: CInt8}|]
filterToClause (Tmpl.FilterJobId i) = [QQ.sql|job_id = #{i :: CInt8}|]

-- | Run a single-row count 'Query', throwing a parse error on unexpected results.
countStrict :: (MonadArbiter m) => Text -> Q.Query Int64 -> m Int64
countStrict label q = do
  rows <- MA.executeQuery q
  case rows of
    [n] -> pure n
    _ -> throwParsing $ label <> ": unexpected result"

-- | Run a single-row count 'Query', returning 0 on an empty or unexpected result.
countOr0 :: (MonadArbiter m) => Q.Query Int64 -> m Int64
countOr0 q = do
  rows <- MA.executeQuery q
  pure $ case rows of
    [n] -> n
    _ -> 0

-- | An interval in microseconds, for the timeout and delay primitives.
micros :: NominalDiffTime -> Int
micros t = round (realToFrac t * 1_000_000 :: Double)

-- | Take a transaction-scoped advisory lock keyed by a @schema.table@ string and a job id.
advisoryXactLockSQL :: Text -> Int64 -> Q.Query (Maybe Text)
advisoryXactLockSQL key pid =
  [QQ.sql|SELECT pg_advisory_xact_lock(hashtextextended(#{key :: CText}, #{pid :: CInt8}))::text AS @{result :: Maybe CText}|]

-- | 'advisoryXactLockSQL' over many ids, ascending, in one round trip.
advisoryXactLockManySQL :: Text -> [Int64] -> Q.Query (Maybe Text)
advisoryXactLockManySQL key pids =
  [QQ.sql|SELECT pg_advisory_xact_lock(hashtextextended(#{key :: CText}, id))::text AS @{result :: Maybe CText} FROM unnest(#{pids :: [CInt8]}::bigint[]) AS t(id) ORDER BY id|]

-- | The admission columns for a job, derived from its payload.
admissionColumns
  :: forall payload. (HasConcurrency payload, HasRateLimit payload) => payload -> AdmissionColumns
admissionColumns p =
  let rlKey = runRateLimitFor p (rateLimitFor @payload)
      ccKey = runConcurrencyFor p (concurrencyFor @payload)
   in AdmissionColumns
        { acRateLimitKey = rateLimitKeyText <$> rlKey
        , acRateLimitPrefix = rlkPrefix <$> rlKey
        , acRateLimitCost = rateLimitCost p
        , acConcurrencyKey = concurrencyKeyText <$> ccKey
        , acConcurrencyPrefix = ckPrefix <$> ccKey
        }

-- | What an insert path puts on its jobs, carrying the ambient trace context.
type TraceStamp payload = JobWrite payload -> JobWrite payload

-- | The stamp for the context in scope. One read covers every job inserted under it.
traceStamp :: (MonadIO m) => m (TraceStamp payload)
traceStamp = stampTraceContext <$> liftIO currentTraceContext

stampedRow
  :: (JobPayload payload)
  => TraceStamp payload
  -> JobWrite payload
  -> (JobWrite payload, AdmissionColumns)
stampedRow stamp job = (stamp job, admissionColumns (payload job))

-- | Insert a job without validating that the parent exists.
--
-- This is an internal fast path for callers that already guarantee the parent
-- is present (e.g. 'insertJobTree'). External callers
-- should use 'insertJob' which validates the parent first.
insertJobUnsafe
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> JobWrite payload
  -> m (Maybe (JobRead payload))
insertJobUnsafe schemaName tableName job =
  traceStamp >>= \stamp -> insertJobUnsafeStamped schemaName tableName stamp job

-- | 'insertJobUnsafe' over a stamp the caller shares across its inserts.
insertJobUnsafeStamped
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> TraceStamp payload
  -> JobWrite payload
  -> m (Maybe (JobRead payload))
insertJobUnsafeStamped schemaName tableName stamp job = do
  let valuesFrag = insertFrag (jobCodec tableName) (stampedRow stamp job)
      query = case dedupKey job of
        Just (ReplaceDuplicate _) -> Tmpl.insertJobReplaceSQL schemaName tableName valuesFrag
        _ -> Tmpl.insertJobSQL schemaName tableName valuesFrag

  withDbTransaction $ do
    rawJobs <- MA.executeQuery query
    case rawJobs of
      [] -> case dedupKey job of
        Just (IgnoreDuplicate _) -> pure Nothing
        Just (ReplaceDuplicate _) -> pure Nothing
        Nothing -> throwParsing "insertJob: No rows returned from INSERT"
      (raw : _) -> Just <$> decodePayload raw

-- | Add tokens to a key's bucket, capped at its max. For operator top-ups and
-- manually-refilled policies.
addRateLimitTokens :: (MonadArbiter m) => SchemaName -> RateLimitKey -> Double -> m ()
addRateLimitTokens schemaName key amount =
  void $
    MA.executeStatement
      (Tmpl.addRateLimitTokensSQL schemaName (rateLimitKeyText key) (rlkPrefix key) amount)

-- | Delete full, idle rate-limit buckets. Returns the number pruned.
pruneRateLimitBuckets :: (MonadArbiter m) => SchemaName -> NominalDiffTime -> m Int64
pruneRateLimitBuckets schemaName idle =
  MA.executeStatement
    (Tmpl.pruneRateLimitBucketsSQL schemaName (realToFrac idle))

-- | Refill every bucket under a prefix to full. Returns the number refilled. Used to
-- build a fixed window from a manual policy plus a cron.
resetRateLimitBuckets :: (MonadArbiter m) => SchemaName -> Text -> m Int64
resetRateLimitBuckets schemaName prefix =
  MA.executeStatement
    (Tmpl.resetRateLimitBucketsSQL schemaName prefix)

-- | Make a prefix's throttled jobs claimable again across the given queue tables,
-- in one statement. Returns the number woken.
wakeThrottledJobs :: (MonadArbiter m) => SchemaName -> [TableName] -> Text -> m Int64
wakeThrottledJobs _ [] _ = pure 0
wakeThrottledJobs schemaName tableNames prefix =
  countStrict "wakeThrottledJobs" (Tmpl.wakeThrottledJobsSQL schemaName tableNames prefix)

-- | Wake one key's throttled jobs across the given tables, in one statement.
-- Returns the count.
wakeThrottledJobsForKey :: (MonadArbiter m) => SchemaName -> [TableName] -> RateLimitKey -> m Int64
wakeThrottledJobsForKey _ [] _ = pure 0
wakeThrottledJobsForKey schemaName tableNames key =
  countStrict
    "wakeThrottledJobsForKey"
    (Tmpl.wakeThrottledJobsForKeySQL schemaName tableNames (rlkPrefix key) (rateLimitKeyText key))

-- | List every policy with its default/override params, bucket aggregates, and
-- live throttled count across the given queue tables.
listRateLimitPolicies :: (MonadArbiter m) => SchemaName -> [TableName] -> m [RateLimitPolicyView]
listRateLimitPolicies schemaName tableNames =
  MA.executeQuery (Tmpl.listRateLimitPoliciesSQL schemaName tableNames)

-- | One prefix's policy view with bucket aggregates and live throttled count.
getRateLimitPolicy :: (MonadArbiter m) => SchemaName -> [TableName] -> Text -> m (Maybe RateLimitPolicyView)
getRateLimitPolicy schemaName tableNames prefix =
  listToMaybe
    <$> MA.executeQuery (Tmpl.getRateLimitPolicySQL schemaName tableNames prefix)

-- | Whether a rate-limit policy exists for a prefix.
rateLimitPolicyExists :: (MonadArbiter m) => SchemaName -> Text -> m Bool
rateLimitPolicyExists schemaName prefix =
  or <$> MA.executeQuery (Tmpl.rateLimitPolicyExistsSQL schemaName prefix)

-- | List a prefix's buckets with effective max and fill fraction, paginated.
listRateLimitBuckets :: (MonadArbiter m) => SchemaName -> Text -> Int -> Int -> m [RateLimitBucketView]
listRateLimitBuckets schemaName prefix limit offset =
  MA.executeQuery
    (Tmpl.listRateLimitBucketsSQL schemaName prefix (fromIntegral limit) (fromIntegral offset))

-- | Set or clear a policy's override params. Returns rows affected (0 if absent).
updateRateLimitPolicyOverrides :: (MonadArbiter m) => SchemaName -> Text -> RateLimitPolicyUpdate -> m Int64
updateRateLimitPolicyOverrides schemaName prefix (RateLimitPolicyUpdate mMax mRefill mIv) =
  MA.executeStatement
    (Tmpl.updateRateLimitOverridesSQL schemaName mMax mRefill mIv prefix)

-- | Apply a pool's override-limit patch (retunes every key under the prefix).
-- Returns rows affected.
updateConcurrencyPolicyOverrides :: (MonadArbiter m) => SchemaName -> Text -> ConcurrencyPolicyUpdate -> m Int64
updateConcurrencyPolicyOverrides schemaName prefix (ConcurrencyPolicyUpdate mLim) =
  MA.executeStatement
    (Tmpl.updateConcurrencyPolicyOverrideSQL schemaName mLim prefix)

-- | List every concurrency pool with its default/override limit and live key and
-- in-flight aggregates.
listConcurrencyPolicies :: (MonadArbiter m) => SchemaName -> m [ConcurrencyPolicyView]
listConcurrencyPolicies schemaName =
  MA.executeQuery (Tmpl.listConcurrencyPoliciesSQL schemaName)

-- | One prefix's concurrency pool view with live aggregates.
getConcurrencyPolicy :: (MonadArbiter m) => SchemaName -> Text -> m (Maybe ConcurrencyPolicyView)
getConcurrencyPolicy schemaName prefix =
  listToMaybe
    <$> MA.executeQuery (Tmpl.getConcurrencyPolicySQL schemaName prefix)

-- | List a prefix's keys with effective cap and fill fraction, paginated.
listConcurrencyKeys :: (MonadArbiter m) => SchemaName -> Text -> Int -> Int -> m [ConcurrencyKeyView]
listConcurrencyKeys schemaName prefix limit offset =
  MA.executeQuery
    (Tmpl.listConcurrencyKeysSQL schemaName prefix (fromIntegral limit) (fromIntegral offset))

-- | Delete drained concurrency rows with no live job across the given tables. Returns
-- the number pruned. A key whose advisory try-lock is contended is skipped until the next pass.
pruneConcurrencyKeys :: (MonadArbiter m) => SchemaName -> [TableName] -> m Int64
pruneConcurrencyKeys _ [] = pure 0
pruneConcurrencyKeys schemaName tableNames = withDbTransaction $ do
  dead <-
    MA.executeQuery (Tmpl.lockDeadConcurrencyKeysSQL schemaName tableNames)
  if null dead
    then pure 0
    else do
      locked <- MA.executeQuery (Tmpl.tryLockDeadConcurrencyAdvisorySQL dead)
      if null locked
        then pure 0
        else MA.executeStatement (Tmpl.pruneLockedConcurrencyKeysSQL schemaName tableNames locked)

-- | Lock the count rows, then recount those keys under the lock so a claim's increment
-- is not overwritten. A key seeded after the lock pass is left to its triggers.
reconcileConcurrencyCounts :: (MonadArbiter m) => SchemaName -> [TableName] -> m Int64
reconcileConcurrencyCounts _ [] = pure 0
reconcileConcurrencyCounts schemaName tableNames = withDbTransaction $ do
  held <- MA.executeQuery (Tmpl.lockConcurrencyCountsSQL schemaName)
  rows <- MA.executeQuery (Tmpl.reconcileConcurrencyCountsSQL schemaName tableNames held)
  pure (fromMaybe 0 (listToMaybe rows))

-- | Rebuild the counts only if a crash truncated the UNLOGGED table. Returns the
-- rows it recounted.
reconcileConcurrencyCountsIfStale :: (MonadArbiter m) => SchemaName -> [TableName] -> m Int64
reconcileConcurrencyCountsIfStale _ [] = pure 0
reconcileConcurrencyCountsIfStale schemaName tableNames = do
  stale <- MA.executeQuery (Tmpl.concurrencyCountsStaleSQL schemaName tableNames)
  if or stale then reconcileConcurrencyCounts schemaName tableNames else pure 0

-- | Reconcile then prune, skipped entirely when no concurrency key exists. Returns
-- the rows recounted and pruned.
reconcileAndPruneConcurrency :: (MonadArbiter m) => SchemaName -> [TableName] -> m Int64
reconcileAndPruneConcurrency schemaName tableNames = do
  hasKeys <- MA.executeQuery (Tmpl.concurrencyHasAnyKeySQL schemaName)
  if not (or hasKeys)
    then pure 0
    else
      (+)
        <$> reconcileConcurrencyCounts schemaName tableNames
        <*> pruneConcurrencyKeys schemaName tableNames

-- | Inserts a job into the queue.
--
-- Returns the inserted job with database-generated fields populated.
--
-- __Ordering and concurrency__
--
-- Standalone (ungrouped) jobs are claimed in @(priority ASC, id ASC)@ order.
-- Grouped jobs are claimed one-per-group, with the head of each group chosen
-- by @(attempts DESC, priority ASC, id ASC)@. The @attempts DESC@ prefix
-- prioritises a chronically-failing job so it either succeeds or reaches
-- @maxAttempts@ and moves to the DLQ, rather than starving fresh jobs behind
-- it indefinitely. Concurrent inserts to the same group are serialized at
-- the trigger level: the AFTER INSERT trigger's @ON CONFLICT DO UPDATE@ on
-- the groups table takes a row-level lock, preventing out-of-order commits
-- within a group.
--
-- __Deduplication__
--
-- * @Nothing@: Always insert (dedup_key is NULL)
-- * @Just (IgnoreDuplicate k)@: Skip if dedup_key exists, return Nothing
-- * @Just (ReplaceDuplicate k)@: Replace existing job unless actively in-flight on its
--   first attempt. Returns Nothing only when @attempts > 0@,
--   @not_visible_until > NOW()@, and @last_error IS NULL@ (i.e., the job is
--   being processed for the first time). Jobs that have previously failed
--   (@last_error IS NOT NULL@) can always be replaced, even if currently
--   in-flight on a retry attempt - this is by design, so that a fresh
--   replacement takes priority over a failing job.
--
-- @parentId@ is validated: if set to a non-existent job ID, returns @Nothing@.
-- For building parent-child trees, prefer @insertJobTree@ which handles
-- @parentId@, @isRollup@, and @suspended@ atomically.
insertJob
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> JobWrite payload
  -> m (Maybe (JobRead payload))
insertJob schemaName tableName job = do
  parentOk <- maybe (pure True) (jobExists schemaName tableName) (parentId job)
  if not parentOk
    then pure Nothing
    else insertJobUnsafe schemaName tableName job

-- | Insert multiple jobs in a single batch operation.
--
-- Supports dedup keys: 'IgnoreDuplicate' jobs are silently skipped on
-- conflict, 'ReplaceDuplicate' jobs update the existing row (unless
-- actively in-flight). Only actually inserted or replaced jobs are returned.
--
-- If multiple jobs in the batch share the same dedup key, only the last
-- occurrence is kept (last writer wins), consistent with sequential
-- 'insertJob' calls.
--
-- Does not validate @parentId@ - callers must ensure referenced parents
-- exist. For parent-child trees, use @insertJobTree@ instead.
insertJobsBatch
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [JobWrite payload]
  -- ^ Jobs to insert
  -> m [JobRead payload]
insertJobsBatch _ _ [] = pure []
insertJobsBatch schemaName tableName jobs =
  traceStamp >>= \stamp -> insertJobsBatchStamped schemaName tableName stamp jobs

-- | 'insertJobsBatch' over a stamp the caller shares across its inserts.
insertJobsBatchStamped
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> TraceStamp payload
  -> [JobWrite payload]
  -> m [JobRead payload]
insertJobsBatchStamped _ _ _ [] = pure []
insertJobsBatchStamped schemaName tableName stamp jobs = do
  let batchSrc = batchFrag (jobCodec tableName) (map (stampedRow stamp) (dedupBatch jobs))
  withDbTransaction $ do
    rawJobs <- MA.executeQuery (Tmpl.insertJobsBatchSQL schemaName tableName batchSrc)
    traverse decodePayload rawJobs

insertJobsBatch_
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -> Text
  -> [JobWrite payload]
  -> m Int64
insertJobsBatch_ _ _ [] = pure 0
insertJobsBatch_ schemaName tableName jobs = do
  stamp <- traceStamp
  let batchSrc = batchFrag (jobCodec tableName) (map (stampedRow stamp) (dedupBatch jobs))
  withDbTransaction (MA.executeStatement (Tmpl.insertJobsBatchSQL_ schemaName tableName batchSrc))

-- | Insert a child's result into the results table.
--
-- Each child gets its own row keyed by @(parent_id, child_id)@.
-- The FK @ON DELETE CASCADE@ ensures cleanup when the parent is acked.
--
-- Returns the number of rows inserted (1 on success).
insertResult
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID
  -> Int64
  -- ^ Child job ID
  -> Value
  -- ^ Encoded result value
  -> m Int64
insertResult schemaName tableName parentJobId childId result =
  MA.executeStatement
    (Tmpl.insertResultSQL schemaName tableName parentJobId childId result)

-- | 'insertResult' for several @(parent id, child id, result)@ rows in one statement.
insertResultsBatch
  :: (MonadArbiter m) => SchemaName -> TableName -> [(Int64, Int64, Value)] -> m Int64
insertResultsBatch _ _ [] = pure 0
insertResultsBatch schemaName tableName rows =
  let (parentIds, childIds, results) = unzip3 rows
   in MA.executeStatement
        (Tmpl.insertResultsBatchSQL schemaName tableName parentIds childIds results)

-- | Get all child results for a parent from the results table.
--
-- Returns a 'Map' from child ID to the result 'Value'.
getResultsByParent
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID
  -> m (Map.Map Int64 Value)
getResultsByParent schemaName tableName parentJobId = do
  rows <- MA.executeQuery (Tmpl.getResultsByParentSQL schemaName tableName parentJobId)
  pure $ Map.fromList rows

-- | Get DLQ child errors for a parent.
--
-- Returns a 'Map' from child job ID to the last error message.
getDLQChildErrorsByParent
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID
  -> m (Map.Map Int64 Text)
getDLQChildErrorsByParent schemaName tableName parentJobId = do
  rows <- MA.executeQuery (Tmpl.getDLQChildErrorsByParentSQL schemaName tableName parentJobId)
  pure $ Map.fromList $ mapMaybe (\(jid, mErr) -> (jid,) <$> mErr) rows

-- | Snapshot results into @parent_state@ before DLQ move.
persistParentState
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Job ID
  -> Value
  -- ^ The pre-populated parent state to persist
  -> m Int64
persistParentState schemaName tableName jobId state =
  MA.executeStatement
    (Tmpl.persistParentStateSQL schemaName tableName state jobId)

-- | Deduplicate a batch within itself, preserving input order.
--
-- A single fold over the input list. Non-keyed jobs are appended. Keyed jobs
-- occupy the slot of their first occurrence, with an O(log n) positional update
-- via 'Seq' when a later 'ReplaceDuplicate' overwrites an earlier entry.
--
-- Dedup semantics (matching sequential 'insertJob' behaviour):
--
--   * All 'IgnoreDuplicate' for a key -> first occurrence wins
--   * All 'ReplaceDuplicate' for a key -> last occurrence wins
--   * Mixed strategies for the same key -> 'ReplaceDuplicate' takes precedence
dedupBatch :: [JobWrite payload] -> [JobWrite payload]
dedupBatch = toList . snd . foldl' step (Map.empty, Seq.empty)
  where
    step (!seen, !rows) job = case dedupKeyText (dedupKey job) of
      Nothing -> (seen, rows |> job)
      Just k -> case Map.lookup k seen of
        Nothing -> (Map.insert k (Seq.length rows) seen, rows |> job)
        Just idx
          | isReplace job -> (seen, Seq.update idx job rows)
          | otherwise -> (seen, rows)

    isReplace job = case dedupKey job of
      Just (ReplaceDuplicate _) -> True
      _ -> False

dedupKeyText :: Maybe DedupKey -> Maybe Text
dedupKeyText = fst . dedupParts

-- | The 'Claim.ClaimAdmission' for this payload type.
claimAdmissionFor :: forall payload. (HasConcurrency payload, HasRateLimit payload) => Claim.ClaimAdmission
claimAdmissionFor =
  Claim.ClaimAdmission
    { Claim.admitRateLimited = usesAnyPolicy (rateLimitFor @payload)
    , Claim.admitConcurrent = usesAnyPolicy (concurrencyFor @payload)
    }

-- | Claim up to @maxJobs@ visible jobs, respecting per-group ordering
-- (one job per group). Uses a single-CTE claim with the groups table.
-- Leaves @claimed_by@ NULL.
claimNextVisibleJobs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Int
  -> NominalDiffTime
  -> m [JobRead payload]
claimNextVisibleJobs schemaName tableName maxJobs timeout =
  claimJobs schemaName tableName maxJobs timeout Nothing

-- | 'claimNextVisibleJobs' with claim attribution. Stamps the given worker
-- UUID onto every claimed row's @claimed_by@ column.
claimNextVisibleJobsAs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Int
  -> NominalDiffTime
  -> UUID
  -> m [JobRead payload]
claimNextVisibleJobsAs schemaName tableName maxJobs timeout workerId =
  claimJobs schemaName tableName maxJobs timeout (Just workerId)

claimJobs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Int
  -> NominalDiffTime
  -> Maybe UUID
  -> m [JobRead payload]
claimJobs schemaName tableName maxJobs timeout mWorkerId =
  -- Batch size 1 is the single-job claim.
  claimJobsCached (mkClaimSql (Proxy @payload) schemaName tableName 1 0 timeout mWorkerId) maxJobs

-- | A pool's claim statements, rendered once per capacity in @[1 .. poolSize]@.
-- 'claimSqlFor' falls back to a fresh render outside that range.
data ClaimSql = ClaimSql
  { claimSqlTable :: TableName
  , claimSqlBatchSize :: Int
  , claimSqlFor :: Int -> Text
  }

-- | Assemble a pool's claim statements. Every input except the per-poll capacity
-- is constant for the pool's lifetime, so the dispatcher builds this once.
mkClaimSql
  :: forall payload proxy
   . (JobPayload payload)
  => proxy payload
  -> SchemaName
  -> TableName
  -> Int
  -> Int
  -> NominalDiffTime
  -> Maybe UUID
  -> ClaimSql
mkClaimSql _ schemaName tableName batchSize poolSize timeout mWorkerId =
  let admission = claimAdmissionFor @payload
      render n = Claim.claimJobsBatchedSQL schemaName tableName admission batchSize n timeout mWorkerId
      cache = IntMap.fromList [(n, render n) | n <- [1 .. poolSize]]
   in ClaimSql
        { claimSqlTable = tableName
        , claimSqlBatchSize = batchSize
        , claimSqlFor = \n -> IntMap.findWithDefault (render n) n cache
        }

-- | 'claimJobs' over a prebuilt 'ClaimSql'.
claimJobsCached
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => ClaimSql
  -> Int
  -> m [JobRead payload]
claimJobsCached cs maxJobs = withDbTransaction $ do
  rawJobs <- MA.executeQueryPrepared (Q.rawRows (jobRowCodec (claimSqlTable cs)) (claimSqlFor cs maxJobs))
  traverse decodePayload rawJobs

-- | Batched variant of 'claimNextVisibleJobs' - claims up to @batchSize@ jobs
-- per group, across up to @maxBatches@ groups. Leaves @claimed_by@ NULL.
claimNextVisibleJobsBatched
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Int
  -> Int
  -> NominalDiffTime
  -> m [NonEmpty (JobRead payload)]
claimNextVisibleJobsBatched schemaName tableName batchSize maxBatches timeout =
  claimJobsBatched schemaName tableName batchSize maxBatches timeout Nothing

claimJobsBatched
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Int
  -> Int
  -> NominalDiffTime
  -> Maybe UUID
  -> m [NonEmpty (JobRead payload)]
claimJobsBatched schemaName tableName batchSize maxBatches timeout mWorkerId =
  claimJobsBatchedCached (mkClaimSql (Proxy @payload) schemaName tableName batchSize 0 timeout mWorkerId) maxBatches

-- | 'claimJobsBatched' over a prebuilt 'ClaimSql'.
claimJobsBatchedCached
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => ClaimSql
  -> Int
  -> m [NonEmpty (JobRead payload)]
claimJobsBatchedCached cs maxBatches
  | claimSqlBatchSize cs < 1 = pure []
  | maxBatches < 1 = pure []
  | otherwise = withDbTransaction $ do
      rawJobs <- MA.executeQueryPrepared (Q.rawRows (jobRowCodec (claimSqlTable cs)) (claimSqlFor cs maxBatches))
      jobs <- traverse decodePayload rawJobs
      let sorted = sortOn groupKey jobs
          groups = groupBy (\j1 j2 -> groupKey j1 == groupKey j2) sorted
      pure $ concatMap (chunksOfNE (claimSqlBatchSize cs)) $ mapMaybe NE.nonEmpty groups

-- | Split a NonEmpty list into chunks of at most @n@ elements.
chunksOfNE :: Int -> NonEmpty a -> [NonEmpty a]
chunksOfNE n (x :| xs) = go (x : xs)
  where
    go [] = []
    go (y : ys) =
      let (chunk, rest) = splitAt (n - 1) ys
       in (y :| chunk) : go rest

-- | Whether acking this job tees it into the archive (positive @archiveFor@).
archivesOnAck :: JobRead payload -> Bool
archivesOnAck = maybe False (> 0) . archiveFor

-- | Acknowledge a job as completed (smart ack).
--
-- Deletes standalone jobs. Suspends parents waiting for children. Wakes
-- parents when the last sibling completes. Uses an advisory lock for
-- child jobs to serialize with concurrent sibling acks.
--
-- Returns 1 on success, 0 if the job was already gone.
ackJob
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> JobRead payload
  -> m Int64
ackJob schemaName tableName job = withDbTransaction $ ackJobInner schemaName tableName job

-- | Inner ack logic (must be called within an existing transaction).
ackJobInner
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName -> TableName -> JobRead payload -> m Int64
ackJobInner schemaName tableName job = do
  lockJobParents schemaName tableName [parentId job]
  let jid = primaryKey job
      cseq = claimSeq job
  countOr0 (Tmpl.smartAckJobSQL (archivesOnAck job) schemaName tableName jid cseq)

-- | Take the advisory lock of every distinct parent named, ascending, before any
-- row lock the caller goes on to take.
lockJobParents :: (MonadArbiter m) => SchemaName -> TableName -> [Maybe Int64] -> m ()
lockJobParents schemaName tableName parents =
  unless (null pids) $
    void $
      MA.executeQuery (advisoryXactLockManySQL (schemaName <> "." <> tableName) pids)
  where
    pids = Set.toAscList (Set.fromList (catMaybes parents))

-- | Read a job's parent and take its advisory lock, before any row lock the caller takes.
lockParentOf :: (MonadArbiter m) => SchemaName -> TableName -> Int64 -> m (Maybe Int64)
lockParentOf schemaName tableName jobId = do
  parentRows <- MA.executeQuery (Tmpl.getParentIdSQL schemaName tableName jobId)
  let mParentId = case parentRows of
        [Just pid] -> Just pid
        _ -> Nothing
  mParentId <$ lockJobParents schemaName tableName [mParentId]

-- | Wake every distinct parent named, ascending, matching the order the locks were taken.
resumeJobParents :: (MonadArbiter m) => TreeLocks -> SchemaName -> TableName -> [Maybe Int64] -> m ()
resumeJobParents locks schemaName tableName =
  traverse_ (tryResumeParent locks schemaName tableName) . Set.toAscList . Set.fromList . catMaybes

-- | Lock every job named and all of its descendants, in one descending pass.
lockJobTrees :: (MonadArbiter m) => SchemaName -> TableName -> [Int64] -> m ()
lockJobTrees _ _ [] = pure ()
lockJobTrees schemaName tableName ids = void $ countOr0 (Tmpl.lockJobTreesSQL schemaName tableName ids)

-- | 'lockJobTrees' widened to each named job's whole tree, for a caller that goes on to
-- cancel trees rather than settle single jobs.
lockJobTreesFromRoot :: (MonadArbiter m) => SchemaName -> TableName -> [Int64] -> m ()
lockJobTreesFromRoot _ _ [] = pure ()
lockJobTreesFromRoot schemaName tableName ids =
  void $ countOr0 (Tmpl.lockJobTreesFromRootSQL schemaName tableName ids)

-- | Wake a suspended parent if all children are done.
tryResumeParent :: (MonadArbiter m) => TreeLocks -> SchemaName -> TableName -> Int64 -> m ()
tryResumeParent locks schemaName tableName pid = do
  when (locks == TakeLocks) $
    void $
      MA.executeQuery
        (advisoryXactLockSQL (schemaName <> "." <> tableName) pid)
  void $
    MA.executeStatement
      (Tmpl.tryWakeAncestorSQL schemaName tableName pid)

-- | Acknowledge a list of jobs as completed in one statement.
--
-- Set-based smart ack: deletes leaves, suspends finalizers that still have
-- children, and wakes parents whose last child just completed. Locks the
-- distinct parents to serialize with concurrent sibling acks.
ackJobsBatch
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [JobRead payload]
  -> m [Int64]
  -- ^ Ids actually acked (deleted or suspended). Reclaimed jobs are absent.
ackJobsBatch _ _ [] = pure []
ackJobsBatch schemaName tableName jobs = withDbTransaction $ do
  lockJobParents schemaName tableName (map parentId jobs)
  MA.executeQuery
    (Tmpl.smartAckJobsBatchSQL (any archivesOnAck jobs) schemaName tableName (map primaryKey jobs) (map claimSeq jobs))

-- | Set the visibility timeout for a job
setVisibilityTimeout
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> NominalDiffTime
  -- ^ Timeout in seconds
  -> JobRead payload
  -> m Int64
  -- ^ Rows updated. 0 for a row that is gone, reclaimed, or suspended.
setVisibilityTimeout schemaName tableName timeout job =
  MA.executeStatement
    (Tmpl.setVisibilityTimeoutSQL schemaName tableName (realToFrac timeout) (primaryKey job) (claimSeq job))

-- | Detailed information about the result of a visibility update operation for a single job.
data VisibilityUpdateInfo = VisibilityUpdateInfo
  { vuiJobId :: JobId
  -- ^ The ID of the job that was targeted.
  , vuiWasHeartbeated :: Bool
  -- ^ Whether the update actually extended this row.
  , vuiCurrentDbClaimSeq :: Maybe ClaimSeq
  -- ^ The row's claim token now. 'Nothing' when the row is gone.
  , vuiCancelRequested :: Bool
  -- ^ 'True' if a force-cancel has flagged this job.
  , vuiSuspended :: Bool
  -- ^ 'True' if the row is a finalizer waiting on its children.
  , vuiClaimedBy :: Maybe UUID
  -- ^ Who holds the row's claim now.
  }
  deriving stock (Eq, Generic, Show)

-- | Batch variant of 'setVisibilityTimeout'. Returns each row's state as
-- 'VisibilityUpdateInfo' records it.
setVisibilityTimeoutBatch
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> NominalDiffTime
  -- ^ Timeout in seconds
  -> [JobRead payload]
  -> m [VisibilityUpdateInfo]
  -- ^ Returns a list of status records, one for each job that was targeted.
setVisibilityTimeoutBatch _ _ _ [] = pure []
setVisibilityTimeoutBatch schemaName tableName timeout jobs = do
  let valuesFrag =
        Q.sepBy
          ","
          [ [QQ.sql|(#{jid :: CInt8}, #{cseq :: CInt8}, #{holder :: Maybe CUuid})|]
          | job <- jobs
          , let jid = primaryKey job
          , let cseq = claimSeq job
          , let holder = claimedBy job
          ]

  MA.executeQuery $
    Q.rows visibilityUpdateCodec $
      Tmpl.setVisibilityTimeoutBatchSQL schemaName tableName valuesFrag (map primaryKey jobs) (realToFrac timeout)

-- | Update a job for retry with backoff and error tracking
--
-- Returns the number of rows updated (0 if job was already claimed by another worker).
updateJobForRetry
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> NominalDiffTime
  -- ^ Backoff timeout in seconds
  -> Text
  -- ^ Error message
  -> JobRead payload
  -> m Int64
updateJobForRetry schemaName tableName backoff errorMsg job =
  MA.executeStatement
    (Tmpl.updateJobForRetrySQL schemaName tableName (ceiling backoff) errorMsg (primaryKey job) (claimSeq job))

-- | Soft-nack a job: decrement the attempt the claim consumed so the reprocess
-- does not count against the retry budget, without recording a failure.
--
-- Returns the number of rows updated (0 if the job was already claimed by
-- another worker).
nackJob
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> JobRead payload
  -> m Int64
nackJob schemaName tableName job =
  MA.executeStatement
    (Tmpl.nackJobSQL schemaName tableName (primaryKey job) (claimSeq job) (attempts job))

-- | 'nackJob' over a batch in one statement, returning the ids nacked. Jobs another
-- worker holds are absent.
nackJobsBatch
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [JobRead payload]
  -> m [Int64]
nackJobsBatch _ _ [] = pure []
nackJobsBatch schemaName tableName jobs =
  MA.executeQuery
    (Tmpl.nackJobsBatchSQL schemaName tableName (map primaryKey jobs) (map claimSeq jobs) (map attempts jobs))

-- | Whether the caller already took the parent and tree locks over its whole set.
data TreeLocks
  = TakeLocks
  | LocksHeld
  deriving stock (Eq, Show)

-- | Move a job to the DLQ. Cascades descendants for rollup parents.
-- Wakes the parent if this was a child job.
--
-- Returns 0 if the job was already claimed by another worker.
moveToDLQ
  :: forall m payload
   . (MonadArbiter m)
  => TreeLocks
  -> SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Text
  -- ^ Error message (the final error that caused the DLQ move)
  -> JobRead payload
  -> m Int64
moveToDLQ locks schemaName tableName errorMsg job =
  moveToDLQFields
    locks
    Tmpl.MoveNow
    schemaName
    tableName
    errorMsg
    (primaryKey job)
    (claimSeq job)
    (parentId job)
    (isRollup job)

-- | 'moveToDLQ' driven by scalar fields, so callers without a typed 'JobRead'
-- (the reaper sweep) can reuse the tree-aware move.
moveToDLQFields
  :: (MonadArbiter m)
  => TreeLocks
  -> Tmpl.DLQMove
  -> SchemaName
  -> TableName
  -> Text
  -- ^ Error message (the final error that caused the DLQ move)
  -> Int64
  -- ^ Job id
  -> Int64
  -- ^ Claim token (for the optimistic move check)
  -> Maybe Int64
  -- ^ Parent id, if a child
  -> Bool
  -- ^ Whether the job is a rollup finalizer
  -> m Int64
moveToDLQFields locks move schemaName tableName errorMsg jobId cseq mParentId rollup = withDbTransaction $ do
  when (locks == TakeLocks) $ do
    lockJobParents schemaName tableName [mParentId]
    when rollup $ lockJobTrees schemaName tableName [jobId]
  when rollup $ snapshotTreeRollups schemaName tableName jobId
  rows <- countOr0 (Tmpl.moveToDLQSQL move schemaName tableName jobId cseq errorMsg)
  when (rows > 0) $ do
    when rollup $ void $ cascadeChildrenToDLQ schemaName tableName jobId "Parent moved to DLQ"
    for_ mParentId $ \pid ->
      tryResumeParent LocksHeld schemaName tableName pid
  pure rows

-- | Cascade all descendants of a rollup parent to the DLQ.
--
-- Recursively finds all descendants in the main queue and moves them
-- to the DLQ with the given error message.
--
-- Returns the number of children moved to DLQ.
cascadeChildrenToDLQ
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID
  -> Text
  -- ^ Error message for cascaded children
  -> m Int64
cascadeChildrenToDLQ schemaName tableName parentJobId errorMsg =
  countStrict
    "cascadeChildrenToDLQ"
    (Tmpl.cascadeChildrenToDLQSQL schemaName tableName parentJobId errorMsg)

-- | Snapshot child results for every rollup finalizer in a job's tree, the job
-- included. Persists accumulated results into @parent_state@ so they survive deletion.
snapshotTreeRollups
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Root of the tree being moved
  -> m ()
snapshotTreeRollups schemaName tableName parentJobId = do
  rollupIds <- MA.executeQuery (Tmpl.treeRollupIdsSQL schemaName tableName parentJobId)
  for_ rollupIds $ \rid -> do
    (results, errors, snap, _) <- readChildResultsRaw schemaName tableName rid
    let merged = mergeRawChildResults results errors snap
    when (not $ Map.null merged) $
      void $
        persistParentState schemaName tableName rid (toJSON merged)

-- | Moves multiple jobs from the main queue to the dead-letter queue.
--
-- Each job is moved with its own error message. Jobs that have already been
-- reclaimed by another worker (a different claim token) are silently skipped.
--
-- Returns the total number of jobs moved to DLQ.
moveToDLQBatch
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [(JobRead payload, Text)]
  -- ^ List of (job, error message) pairs
  -> m Int64
moveToDLQBatch _ _ [] = pure 0
moveToDLQBatch schemaName tableName jobsWithErrors = withDbTransaction $ do
  let ids = map (primaryKey . fst) jobsWithErrors
      cseqs = map (claimSeq . fst) jobsWithErrors
      msgs = map snd jobsWithErrors
      rollupIds = Set.toList . Set.fromList $ map (primaryKey . fst) (filter (isRollup . fst) jobsWithErrors)
  lockJobParents schemaName tableName (map (parentId . fst) jobsWithErrors)
  unless (null rollupIds) $ do
    -- Every row the move will lock, plus the trees, in one descending pass.
    lockJobTrees schemaName tableName ids
    -- Before the move, which takes a named rollup's results with it.
    for_ rollupIds $ snapshotTreeRollups schemaName tableName
  moved <- Set.fromList <$> MA.executeQuery (Tmpl.moveToDLQBatchSQL schemaName tableName ids cseqs msgs)
  let movedJobs = filter (flip Set.member moved . primaryKey . fst) jobsWithErrors
  for_ movedJobs $ \(job, _) ->
    when (isRollup job) $
      void $
        cascadeChildrenToDLQ schemaName tableName (primaryKey job) "Parent moved to DLQ"
  resumeJobParents LocksHeld schemaName tableName (map (parentId . fst) movedJobs)
  pure (fromIntegral (Set.size moved))

-- * Dead Letter Queue Operations

-- | Retry a job from the DLQ (re-inserts with attempts reset to 0).
-- The dedup_key is NOT restored - retried jobs won't conflict with new dedup inserts.
retryFromDLQ
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ DLQ job ID
  -> m (Maybe (JobRead payload))
retryFromDLQ schemaName tableName dlqId = withDbTransaction $ do
  rawJobs <- MA.executeQuery (Tmpl.retryFromDLQSQL schemaName tableName dlqId)
  case rawJobs of
    [] -> pure Nothing
    (raw : _) -> Just <$> decodePayload raw

-- | Check whether a DLQ job exists by ID.
dlqJobExists
  :: (MonadArbiter m)
  => Text
  -> Text
  -> Int64
  -> m Bool
dlqJobExists schemaName tableName dlqId = do
  rows <- MA.executeQuery (Tmpl.dlqJobExistsSQL schemaName tableName dlqId)
  case rows of
    [b] -> pure b
    _ -> pure False

-- ---------------------------------------------------------------------------
-- Filtered Query Operations
-- ---------------------------------------------------------------------------

-- | List jobs with composable filters and an explicit sort spec.
--
-- @Nothing@ for both sort args yields the default ordering (@id DESC@).
listJobsFilteredOrdered
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> Maybe Tmpl.JobSortColumn
  -- ^ Sort column (defaults to 'Tmpl.JsId')
  -> Maybe Tmpl.SortDir
  -- ^ Sort direction (defaults to 'Tmpl.SortDesc')
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
listJobsFilteredOrdered schemaName tableName filters mSortBy mSortDir limit offset
  | any isStatusFilter filters =
      map fst <$> listJobsWithStatus schemaName tableName filters mSortBy mSortDir limit offset
  | otherwise = do
      let orderBy = Tmpl.buildJobsOrderBy mSortBy mSortDir
      rawJobs <-
        MA.executeQuery $
          Tmpl.listJobsFilteredSQL
            schemaName
            tableName
            (buildWhereClause filters)
            orderBy
            (fromIntegral limit)
            (fromIntegral offset)
      traverse decodePayload rawJobs

isStatusFilter :: Tmpl.JobFilter -> Bool
isStatusFilter (Tmpl.FilterStatus _) = True
isStatusFilter _ = False

-- | Decode a job row plus its derived @status@ trailing column.
jobRowWithStatusCodec :: TableName -> RowCodec (JobRead Value, JobStatus)
jobRowWithStatusCodec tableName =
  (,) <$> jobRowCodec tableName <*> (jobStatusFromText <$> col "status" CText)

-- | 'listJobsFilteredOrdered' that also returns each job's derived status.
listJobsWithStatus
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> [Tmpl.JobFilter]
  -> Maybe Tmpl.JobSortColumn
  -> Maybe Tmpl.SortDir
  -> Int
  -> Int
  -> m [(JobRead payload, JobStatus)]
listJobsWithStatus schemaName tableName filters mSortBy mSortDir limit offset = do
  let orderBy = Tmpl.buildJobsOrderBy mSortBy mSortDir
      query =
        Tmpl.listJobsWithStatusSQL
          schemaName
          tableName
          (buildWhereClause filters)
          orderBy
          (fromIntegral limit)
          (fromIntegral offset)
  rows <- MA.executeQuery (Q.rows (jobRowWithStatusCodec tableName) query)
  traverse (bitraverse decodePayload pure) rows

-- | List jobs with composable filters.
--
-- Returns jobs ordered by ID (descending, newest first).
listJobsFiltered
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
listJobsFiltered schemaName tableName filters =
  listJobsFilteredOrdered schemaName tableName filters Nothing Nothing

-- | Count jobs with composable filters.
countJobsFiltered
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> m Int64
countJobsFiltered schemaName tableName filters = do
  countStrict "countJobsFiltered" (Tmpl.countJobsFilteredSQL schemaName tableName (buildWhereClause filters))

-- | List DLQ jobs with composable filters and an explicit sort spec.
--
-- @Nothing@ for both sort args yields the default ordering
-- (@failed_at DESC@).
listDLQFilteredOrdered
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> Maybe Tmpl.DLQSortColumn
  -- ^ Sort column (defaults to 'Tmpl.DlqFailedAt')
  -> Maybe Tmpl.SortDir
  -- ^ Sort direction (defaults to 'Tmpl.SortDesc')
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [DLQ.DLQJob payload]
listDLQFilteredOrdered schemaName tableName filters mSortBy mSortDir limit offset = do
  let orderBy = Tmpl.buildDLQOrderBy mSortBy mSortDir
  rawRows <-
    MA.executeQuery $
      Tmpl.listDLQFilteredSQL
        schemaName
        tableName
        (buildWhereClause filters)
        orderBy
        (fromIntegral limit)
        (fromIntegral offset)
  traverse decodeDLQRow rawRows

-- | List DLQ jobs with composable filters.
--
-- Returns jobs ordered by failed_at (most recent first).
listDLQFiltered
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [DLQ.DLQJob payload]
listDLQFiltered schemaName tableName filters =
  listDLQFilteredOrdered schemaName tableName filters Nothing Nothing

-- | Count DLQ jobs with composable filters.
countDLQFiltered
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> m Int64
countDLQFiltered schemaName tableName filters =
  countStrict "countDLQFiltered" (Tmpl.countDLQFilteredSQL schemaName tableName (buildWhereClause filters))

decodeDLQRow
  :: (JobPayload payload, MonadArbiter m)
  => (Int64, UTCTime, JobRead Value)
  -> m (DLQ.DLQJob payload)
decodeDLQRow (dlqId, dlqFailedAt, rawJob) = do
  jobSnapshot <- decodePayload rawJob
  pure $
    DLQ.DLQJob
      { DLQ.dlqPrimaryKey = dlqId
      , DLQ.failedAt = dlqFailedAt
      , DLQ.jobSnapshot = jobSnapshot
      }

-- | List archived (completed) jobs with composable filters and a typed sort
-- (defaulting to most recent first).
listArchiveFiltered
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> [Tmpl.JobFilter]
  -> Maybe Tmpl.ArchiveSortColumn
  -> Maybe Tmpl.SortDir
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [Archive.ArchiveJob payload]
listArchiveFiltered schemaName tableName filters mSortBy mSortDir limit offset = do
  let orderBy = Tmpl.buildArchiveOrderBy mSortBy mSortDir
  rawRows <-
    MA.executeQuery $
      Tmpl.listArchiveFilteredSQL
        schemaName
        tableName
        (buildWhereClause filters)
        orderBy
        (fromIntegral limit)
        (fromIntegral offset)
  traverse decodeArchiveRow rawRows

-- | List archived jobs (most recent first).
listArchiveJobs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Int
  -> Int
  -> m [Archive.ArchiveJob payload]
listArchiveJobs schemaName tableName = listArchiveFiltered schemaName tableName [] Nothing Nothing

-- | Fetch a single archived job by its original job id.
getArchivedJobById
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Int64
  -> m (Maybe (Archive.ArchiveJob payload))
getArchivedJobById schemaName tableName jobId =
  listToMaybe
    <$> listArchiveFiltered schemaName tableName [Tmpl.FilterJobId jobId] Nothing Nothing 1 0

-- | Delete one archived job by its archive primary key. Returns rows deleted.
deleteArchiveJob :: (MonadArbiter m) => SchemaName -> TableName -> Int64 -> m Int64
deleteArchiveJob schemaName tableName archiveId =
  MA.executeStatement (Tmpl.deleteArchiveJobSQL schemaName tableName archiveId)

-- | Delete archived jobs by archive primary key. Returns rows deleted.
deleteArchiveJobsBatch :: (MonadArbiter m) => SchemaName -> TableName -> [Int64] -> m Int64
deleteArchiveJobsBatch _ _ [] = pure 0
deleteArchiveJobsBatch schemaName tableName archiveIds =
  MA.executeStatement (Tmpl.deleteArchiveJobsBatchSQL schemaName tableName archiveIds)

-- | Re-enqueue an archived job as a fresh standalone job, keeping the archive
-- row. Returns the new job, or @Nothing@ if the archive row no longer exists.
reEnqueueFromArchive
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName -> TableName -> Int64 -> m (Maybe (JobRead payload))
reEnqueueFromArchive schemaName tableName archiveId = withDbTransaction $ do
  rawJobs <- MA.executeQuery (Tmpl.reEnqueueFromArchiveSQL schemaName tableName archiveId)
  traverse decodePayload (listToMaybe rawJobs)

-- | Store a completed root job's result on its archive row. No-ops when the job
-- was not archived. Returns rows updated.
updateArchiveResult
  :: (MonadArbiter m) => SchemaName -> TableName -> Int64 -> Value -> m Int64
updateArchiveResult schemaName tableName jobId result =
  MA.executeStatement
    (Tmpl.updateArchiveResultSQL schemaName tableName result jobId)

-- | 'updateArchiveResult' for several @(job id, result)@ pairs in one statement.
updateArchiveResultsBatch
  :: (MonadArbiter m) => SchemaName -> TableName -> [(Int64, Value)] -> m Int64
updateArchiveResultsBatch _ _ [] = pure 0
updateArchiveResultsBatch schemaName tableName pairs =
  MA.executeStatement
    (uncurry (Tmpl.updateArchiveResultsBatchSQL schemaName tableName) (unzip pairs))

-- | List archived jobs in a group, most recent first.
listArchivedJobsByGroupKey
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Text
  -> Int
  -> Int
  -> m [Archive.ArchiveJob payload]
listArchivedJobsByGroupKey schemaName tableName groupKey =
  listArchiveFiltered schemaName tableName [Tmpl.FilterGroupKey groupKey] Nothing Nothing

-- | Count archived jobs with composable filters.
countArchiveFiltered
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> [Tmpl.JobFilter]
  -> m Int64
countArchiveFiltered schemaName tableName filters =
  countStrict "countArchiveFiltered" (Tmpl.countArchiveFilteredSQL schemaName tableName (buildWhereClause filters))

decodeArchiveRow
  :: (JobPayload payload, MonadArbiter m)
  => (Int64, UTCTime, JobRead Value, Maybe Value)
  -> m (Archive.ArchiveJob payload)
decodeArchiveRow (aId, aCompletedAt, rawJob, aResult) = do
  snapshot <- decodePayload rawJob
  pure $
    Archive.ArchiveJob
      { Archive.archivePrimaryKey = aId
      , Archive.completedAt = aCompletedAt
      , Archive.jobSnapshot = snapshot
      , Archive.archivedResult = aResult
      }

-- | Purge expired archived jobs (per-row @archive_expires_at@) across all queues.
-- Returns the total rows purged and the queues whose purge errored. Designed to
-- run once per reaper tick, so steady-state each call deletes only a small slice.
purgeArchives
  :: (MonadArbiter m)
  => SchemaName -> [TableName] -> m (Int64, [Text])
purgeArchives =
  sweepQueues $ \schemaName queue ->
    withDbTransaction (MA.executeStatement (Q.raw (Tmpl.purgeArchiveSQL schemaName queue)))

-- | List jobs in the dead letter queue
--
-- Returns jobs ordered by failed_at (most recent first).
listDLQJobs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [DLQ.DLQJob payload]
listDLQJobs schemaName tableName = listDLQFiltered schemaName tableName []

-- | List DLQ jobs filtered by parent_id.
--
-- Returns jobs ordered by failed_at (most recent first).
listDLQJobsByParent
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [DLQ.DLQJob payload]
listDLQJobsByParent schemaName tableName parentJobId =
  listDLQFiltered schemaName tableName [Tmpl.FilterParentId parentJobId]

-- | Count DLQ jobs matching a parent_id.
countDLQJobsByParent
  :: (MonadArbiter m)
  => SchemaName -> TableName -> Int64 -> m Int64
countDLQJobsByParent schemaName tableName parentJobId =
  countDLQFiltered schemaName tableName [Tmpl.FilterParentId parentJobId]

-- | Delete a job from the DLQ. If the job was a child, tries to resume
-- the parent when no siblings remain.
deleteDLQJob
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ DLQ job ID
  -> m Int64
deleteDLQJob schemaName tableName dlqId = withDbTransaction $ do
  rows <- MA.executeQuery (Tmpl.deleteDLQJobSQL schemaName tableName dlqId)
  case rows of
    [] -> pure 0
    (Just pid : _) -> do
      tryResumeParent TakeLocks schemaName tableName pid
      pure 1
    _ -> pure 1

-- | Delete jobs by id via the given query builder, then resume any parents left
-- childless. The query must return each deleted row's id and parent_id. Returns
-- the ids deleted.
deleteJobsResumingParents
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> ([Int64] -> Q.Query (Int64, Maybe Int64))
  -> [Int64]
  -> m [Int64]
deleteJobsResumingParents _ _ _ [] = pure []
deleteJobsResumingParents schemaName tableName mkSql jobIds = withDbTransaction $ do
  rows <- MA.executeQuery (mkSql jobIds)
  resumeJobParents TakeLocks schemaName tableName (map snd rows)
  pure (map fst rows)

-- | Delete multiple jobs from the dead letter queue, resuming any parents left
-- childless. Returns the total number of DLQ jobs deleted.
deleteDLQJobsBatch
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [Int64]
  -- ^ DLQ job IDs
  -> m Int64
deleteDLQJobsBatch schemaName tableName dlqIds =
  fromIntegral . length
    <$> deleteJobsResumingParents schemaName tableName (Tmpl.deleteDLQJobsBatchSQL schemaName tableName) dlqIds

-- | Delete force-cancel-flagged jobs @owner@ holds or no live lease holds, resuming
-- any parents left childless. Returns the ids it deleted.
deleteCancelledJobs
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> Maybe UUID
  -> [Int64]
  -> m [Int64]
deleteCancelledJobs schemaName tableName owner =
  deleteJobsResumingParents schemaName tableName (Tmpl.deleteCancelledJobsSQL schemaName tableName owner)

-- * Admin Operations

-- | List jobs in the queue with pagination.
--
-- Returns jobs ordered by ID (descending).
listJobs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
listJobs schemaName tableName = listJobsFiltered schemaName tableName []

-- | Whether a job with the given id exists in the table, without decoding it.
jobExists :: (MonadArbiter m) => SchemaName -> TableName -> Int64 -> m Bool
jobExists schemaName tableName jobId =
  or <$> MA.executeQuery (Tmpl.jobExistsSQL schemaName tableName jobId)

-- | Get a single job by its ID.
getJobById
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Job ID
  -> m (Maybe (JobRead payload))
getJobById schemaName tableName jobId = do
  rawJobs <- MA.executeQuery (Tmpl.getJobByIdSQL schemaName tableName jobId)
  case rawJobs of
    [] -> pure Nothing
    (raw : _) -> Just <$> decodePayload raw

-- | 'getJobById' that also returns the job's derived status.
getJobByIdWithStatus
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Int64
  -> m (Maybe (JobRead payload, JobStatus))
getJobByIdWithStatus schemaName tableName jobId = do
  rows <-
    MA.executeQuery $
      Q.rows (jobRowWithStatusCodec tableName) (Tmpl.getJobByIdWithStatusSQL schemaName tableName jobId)
  traverse (bitraverse decodePayload pure) (listToMaybe rows)

-- | Get a single job by its dedup key.
getJobByDedupKey
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -> TableName
  -> Text
  -> m (Maybe (JobRead payload))
getJobByDedupKey schemaName tableName key = do
  rawJobs <- MA.executeQuery (Tmpl.getJobByDedupKeySQL schemaName tableName key)
  case rawJobs of
    [] -> pure Nothing
    (raw : _) -> Just <$> decodePayload raw

-- | Get all jobs for a specific group key.
getJobsByGroup
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Text
  -- ^ Group key to filter by
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
getJobsByGroup schemaName tableName gk =
  listJobsFiltered schemaName tableName [Tmpl.FilterGroupKey gk]

-- | Cancels (deletes) a job by ID.
--
-- Returns 0 if the job has children - use 'cancelJobCascade' to delete
-- a parent and all its descendants.
--
-- If the deleted job was a child and no siblings remain, the parent is
-- resumed for its completion round.
cancelJob
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Job ID
  -> m Int64
cancelJob schemaName tableName jobId = withDbTransaction $ cancelJobInner schemaName tableName jobId

-- | Inner cancel logic (must be called within an existing transaction).
cancelJobInner
  :: (MonadArbiter m)
  => SchemaName -> TableName -> Int64 -> m Int64
cancelJobInner schemaName tableName jobId = do
  void $ lockParentOf schemaName tableName jobId
  rows <- MA.executeQuery (Tmpl.cancelJobSQL schemaName tableName jobId)
  case rows of
    [n] -> pure n
    _ -> pure 0

-- | Cancels (deletes) multiple jobs by ID.
--
-- Each job gets full wake-parent logic (same as 'cancelJob').
-- Wrapped in a transaction so that cancelling multiple children of the
-- same parent sees a consistent view - the last cancel's CTE correctly
-- detects no remaining siblings and resumes the parent.
-- Returns the total number of jobs deleted.
cancelJobsBatch
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> [Int64]
  -- ^ Job IDs
  -> m Int64
cancelJobsBatch _ _ [] = pure 0
cancelJobsBatch schemaName tableName jobIds =
  withDbTransaction $ sum <$> traverse (cancelJobInner schemaName tableName) jobIds

-- | Promote a delayed or retrying job to be immediately visible.
--
-- Refuses in-flight jobs (attempts > 0 with no last_error).
-- Returns 1 on success, 0 if not found, already visible, or in-flight.
promoteJob
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Job ID
  -> m Int64
  -- ^ Number of rows updated
promoteJob schemaName tableName jobId =
  MA.executeStatement
    (Tmpl.promoteJobSQL schemaName tableName jobId)

-- | Per-status breakdown of a queue. The per-status counts partition the queue
-- and sum to 'totalJobs', mirroring the derived job status taxonomy.
data QueueStats = QueueStats
  { totalJobs :: Int64
  -- ^ Total number of jobs in the queue
  , readyJobs :: Int64
  -- ^ Jobs claimable right now (visible, not leased)
  , inFlightJobs :: Int64
  -- ^ Jobs currently leased by a worker (a retry attempt in progress)
  , scheduledJobs :: Int64
  -- ^ Jobs delayed until a future @not_visible_until@ (never yet attempted)
  , backoffJobs :: Int64
  -- ^ Failed jobs waiting out a retry backoff delay
  , throttledJobs :: Int64
  -- ^ Jobs parked by a rate limit until tokens refill
  , suspendedJobs :: Int64
  -- ^ Suspended jobs (e.g. rollup finalizers awaiting their children)
  , cancelledJobs :: Int64
  -- ^ Force-cancelled jobs flagged for teardown, not yet reaped
  , oldestReadyAgeSeconds :: Maybe Double
  -- ^ Age in seconds of the oldest @ready@ job (Nothing if none are ready).
  -- Scheduled/backoff/leased jobs are excluded so a far-future delayed job
  -- does not inflate the queue's apparent backlog latency.
  , oldestInFlightAgeSeconds :: Maybe Double
  -- ^ Seconds since the oldest in-flight job was claimed (Nothing if none are
  -- leased). Measures work still running, which a handler-duration sample
  -- taken on return cannot: a hung handler never returns one.
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | The per-status depths a 'QueueStats' carries.
queueStatusCounts :: QueueStats -> [(JobStatus, Int64)]
queueStatusCounts s =
  [ (Ready, readyJobs s)
  , (InFlight, inFlightJobs s)
  , (Scheduled, scheduledJobs s)
  , (Backoff, backoffJobs s)
  , (Throttled, throttledJobs s)
  , (Suspended, suspendedJobs s)
  , (Cancelled, cancelledJobs s)
  ]

-- | Decodes the single aggregate row produced by 'Tmpl.getQueueStatsSQL'
-- straight into 'QueueStats', so column names map to fields by name.
statsRowCodec :: RowCodec QueueStats
statsRowCodec =
  QueueStats
    <$> col "total_jobs" CInt8
    <*> col "ready_jobs" CInt8
    <*> col "in_flight_jobs" CInt8
    <*> col "scheduled_jobs" CInt8
    <*> col "backoff_jobs" CInt8
    <*> col "throttled_jobs" CInt8
    <*> col "suspended_jobs" CInt8
    <*> col "cancelled_jobs" CInt8
    <*> ncol "oldest_ready_age_seconds" CFloat8
    <*> ncol "oldest_in_flight_age_seconds" CFloat8

-- | Get statistics about the job queue
getQueueStats
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> m QueueStats
getQueueStats schemaName tableName = do
  rows <-
    MA.executeQuery $
      Q.rawRows statsRowCodec (Tmpl.getQueueStatsSQL schemaName tableName)
  -- The aggregate query always returns exactly one row. The empty fallback
  -- only guards against an unexpected truncation.
  pure $ case rows of
    (s : _) -> s
    [] -> QueueStats 0 0 0 0 0 0 0 0 Nothing Nothing

-- | A landing-overview row: a queue's stats plus its pause state.
data QueueOverview = QueueOverview
  { overviewQueue :: Text
  , overviewStats :: QueueStats
  , overviewQueuePaused :: Bool
  , overviewWorkersLive :: Int64
  , overviewWorkersPaused :: Int64
  }
  deriving stock (Eq, Generic, Show)

instance ToJSON QueueOverview where
  toJSON o =
    object
      [ "queue" .= overviewQueue o
      , "stats" .= overviewStats o
      , "paused" .= overviewQueuePaused o
      , "workersLive" .= overviewWorkersLive o
      , "workersPaused" .= overviewWorkersPaused o
      ]

instance FromJSON QueueOverview where
  parseJSON = withObject "QueueOverview" $ \o ->
    QueueOverview
      <$> o .: "queue"
      <*> o .: "stats"
      <*> o .: "paused"
      <*> o .: "workersLive"
      <*> o .: "workersPaused"

allStatsRowCodec :: RowCodec QueueOverview
allStatsRowCodec =
  QueueOverview
    <$> col "queue" CText
    <*> statsRowCodec
    <*> col "queue_paused" CBool
    <*> col "workers_live" CInt8
    <*> col "workers_paused" CInt8

-- | Every queue's stats plus pause state in one query, for the landing overview.
getAllQueueStats
  :: (MonadArbiter m)
  => SchemaName
  -> [TableName]
  -> m [QueueOverview]
getAllQueueStats _ [] = pure []
getAllQueueStats schemaName tableNames =
  MA.executeQuery (Q.rawRows allStatsRowCodec (Tmpl.allQueueStatsSQL schemaName tableNames))

-- ---------------------------------------------------------------------------
-- Count Operations
-- ---------------------------------------------------------------------------

-- | Count all jobs in a table
countJobs
  :: (MonadArbiter m)
  => SchemaName -> TableName -> m Int64
countJobs schemaName tableName = countJobsFiltered schemaName tableName []

-- | Count jobs matching a group key
countJobsByGroup
  :: (MonadArbiter m)
  => SchemaName -> TableName -> Text -> m Int64
countJobsByGroup schemaName tableName gk =
  countJobsFiltered schemaName tableName [Tmpl.FilterGroupKey gk]

-- | Count DLQ jobs
countDLQJobs
  :: (MonadArbiter m)
  => SchemaName -> TableName -> m Int64
countDLQJobs schemaName tableName = countDLQFiltered schemaName tableName []

-- ---------------------------------------------------------------------------
-- Parent-Child Operations
-- ---------------------------------------------------------------------------

-- | List jobs filtered by parent_id with pagination.
getJobsByParent
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent ID
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
getJobsByParent schemaName tableName pid =
  listJobsFiltered schemaName tableName [Tmpl.FilterParentId pid]

-- | Count jobs matching a parent_id.
countJobsByParent
  :: (MonadArbiter m)
  => SchemaName -> TableName -> Int64 -> m Int64
countJobsByParent schemaName tableName pid =
  countJobsFiltered schemaName tableName [Tmpl.FilterParentId pid]

-- | Count children for a batch of potential parent IDs.
--
-- Returns a Map from parent_id to @(total, paused)@ counts (only non-zero entries).
countChildrenBatch
  :: (MonadArbiter m)
  => SchemaName -> TableName -> [Int64] -> m (Map.Map Int64 (Int64, Int64))
countChildrenBatch _ _ [] = pure Map.empty
countChildrenBatch schemaName tableName ids = do
  rows <-
    MA.executeQuery $
      Q.rows parentCountCodec (Tmpl.countChildrenBatchSQL schemaName tableName ids)
  pure $ Map.fromList rows

-- | Count children in the DLQ for a batch of potential parent IDs.
--
-- Returns a Map from parent_id to DLQ child count (only non-zero entries).
countDLQChildrenBatch
  :: (MonadArbiter m)
  => SchemaName -> TableName -> [Int64] -> m (Map.Map Int64 Int64)
countDLQChildrenBatch _ _ [] = pure Map.empty
countDLQChildrenBatch schemaName tableName ids = do
  rows <- MA.executeQuery (Tmpl.countDLQChildrenBatchSQL schemaName tableName ids)
  pure $ Map.fromList rows

-- ---------------------------------------------------------------------------
-- Job Dependency Operations
-- ---------------------------------------------------------------------------

-- | Pause all visible children of a parent job.
--
-- Sets suspended = TRUE for claimable children, making them
-- unclaimable. In-flight children (currently being processed by workers)
-- are left alone so their visibility timeout can expire normally if the
-- worker crashes.
-- Returns the number of children paused.
pauseChildren
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID
  -> m Int64
pauseChildren schemaName tableName parentJobId =
  MA.executeStatement
    (Tmpl.pauseChildrenSQL schemaName tableName parentJobId)

-- | Resume all suspended children of a parent job.
--
-- Only affects children whose suspended = TRUE.
-- Returns the number of children resumed.
resumeChildren
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID
  -> m Int64
resumeChildren schemaName tableName parentJobId =
  MA.executeStatement
    (Tmpl.resumeChildrenSQL schemaName tableName parentJobId)

-- | Cancel a job and all its descendants recursively.
--
-- Uses a recursive CTE to find all descendants and deletes them all.
-- If the root job itself is a child, resumes its parent for a completion round.
-- Returns the total number of jobs deleted (parent + all descendants).
cancelJobCascade
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Root job ID
  -> m Int64
cancelJobCascade = cascadeDeleteJob Tmpl.cancelJobCascadeSQL

-- | Transactional wrapper for cascade-delete SQL: read the root's parent,
-- run the supplied delete template, and wake the parent for a completion
-- round if anything was deleted. 'cancelJobCascade' and 'forceCancelJob'
-- share this shell and differ only in the SQL template they pass in.
cascadeDeleteJob
  :: (MonadArbiter m)
  => (SchemaName -> TableName -> Int64 -> Q.Query Int64)
  -- ^ Cascade-delete query builder (returns the deleted count).
  -> SchemaName
  -> TableName
  -> Int64
  -> m Int64
cascadeDeleteJob mkSql schemaName tableName jobId = withDbTransaction $ do
  rootParentId <- lockParentOf schemaName tableName jobId
  deleted <- countOr0 (mkSql schemaName tableName jobId)

  when (deleted > 0) $
    for_ rootParentId $
      tryResumeParent LocksHeld schemaName tableName

  pure deleted

-- | Cancel an entire job tree by walking up from any node to the root,
-- then cascade-deleting everything from the root down.
--
-- Unlike 'cancelJobCascade', this does NOT call 'tryResumeParent' - the root
-- by definition has no parent. Returns the total number of jobs deleted.
cancelJobTree
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Any job ID within the tree
  -> m Int64
cancelJobTree schemaName tableName jobId =
  countStrict "cancelJobTree" (Tmpl.cancelJobTreeSQL schemaName tableName jobId)

-- | Cascade-cancel a job subtree: flag still-live claimed jobs, delete the rest,
-- and NOTIFY the queue's cancel channel for every claimed job affected. Workers
-- async-cancel the matching handler thread on receipt.
forceCancelJob
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Root job ID
  -> m Int64
forceCancelJob = cascadeDeleteJob Tmpl.forceCancelJobSQL

-- ---------------------------------------------------------------------------
-- Suspend/Resume Operations
-- ---------------------------------------------------------------------------

-- | Suspend a job, making it unclaimable.
--
-- Only suspends non-in-flight jobs (not currently being processed by workers).
-- Returns the number of rows updated (0 if job doesn't exist, is in-flight,
-- or already suspended).
suspendJob
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Job ID
  -> m Int64
suspendJob schemaName tableName jobId =
  MA.executeStatement
    (Tmpl.suspendJobSQL schemaName tableName jobId)

-- | Resume a suspended job, making it claimable again.
--
-- Returns the number of rows updated (0 if job doesn't exist or isn't suspended).
resumeJob
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Job ID
  -> m Int64
resumeJob schemaName tableName jobId =
  MA.executeStatement
    (Tmpl.resumeJobSQL schemaName tableName jobId)

-- | Recompute of the groups table from the main queue, over one bounded batch of
-- rows past @cursor@.
--
-- Locks the window's groups rows and the emptied ones (FOR UPDATE SKIP LOCKED) to avoid
-- fighting with live job claims, then rewrites them, which deletes the emptied, and
-- inserts the summary rows missing over the same key window. Returns the key to resume
-- from, or 'Nothing' once the pass found nothing past @cursor@. The caller is
-- responsible for any cross-pool coordination (see 'runGatedState' and
-- 'refreshAllGroups').
refreshGroupsForQueue
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> Int
  -- ^ Rows this pass covers.
  -> Maybe Text
  -- ^ Resume past this key, or start at the first.
  -> m (Int64, Maybe Text)
refreshGroupsForQueue schemaName tableName batch cursor = withDbTransaction $ do
  window <- MA.executeQuery (Tmpl.groupsWindowSQL schemaName tableName batch cursor)
  let windowEnd = lastRow window
  locked <- MA.executeQuery (Tmpl.lockGroupsSQL schemaName tableName batch cursor windowEnd)
  rewritten <-
    if null locked
      then pure 0
      else sum <$> MA.executeQuery (Tmpl.refreshGroupsSQL schemaName tableName locked)
  repaired <- MA.executeQuery (Tmpl.insertMissingGroupsSQL schemaName tableName batch cursor windowEnd)
  -- A filled repair batch left keys behind.
  let next = if length repaired >= batch then lastRow (map fst repaired) else windowEnd
  pure (rewritten + fromIntegral (length (filter snd repaired)), next)

-- | The last row, in the order the database returned it.
lastRow :: [a] -> Maybe a
lastRow = foldl' (\_ x -> Just x) Nothing

-- | Groups rows one refresh transaction recomputes, split across the queues sharing
-- it, so the locks it holds do not grow with the number of queues.
groupsRefreshBatch :: Int
groupsRefreshBatch = 20000

-- | Schema-wide groups-table refresh, 'groupsRefreshBatch' rows for the pass. Wrap in
-- 'runGatedState' so only one pool runs it per interval and every pool resumes from the
-- same cursors: a caller that discards them refreshes the same head of each table
-- forever. Each queue runs in a savepoint, so one queue's failure leaves the rest, but
-- its row locks stand until the caller's transaction ends. Returns the rows rewritten,
-- the queue names that failed, and where each queue resumes.
refreshAllGroups
  :: (MonadArbiter m)
  => SchemaName
  -> [TableName]
  -> Map.Map TableName Text
  -- ^ Where the previous pass left off, per queue.
  -> m ((Int64, [Text]), Map.Map TableName Text)
refreshAllGroups schemaName queues cursors = do
  (refreshed, failed) <- sweepEachQueue one schemaName queues
  let rewritten = sum [n | (_, (n, _)) <- refreshed]
      resumed = foldl' (\acc (tbl, (_, next)) -> Map.alter (const next) tbl acc) cursors refreshed
  pure ((rewritten, failed), resumed)
  where
    perQueue = max 1 (groupsRefreshBatch `div` max 1 (length queues))
    one schema tbl = refreshGroupsForQueue schema tbl perQueue (Map.lookup tbl cursors)

-- | 'refreshAllGroups' run to completion: each queue's groups table walked to the end,
-- one batch and one transaction per pass. A deliberate repair, rather than the reaper's
-- single batch per tick.
refreshAllGroupsFully
  :: (MonadArbiter m)
  => SchemaName
  -> [TableName]
  -> m (Int64, [Text])
refreshAllGroupsFully = sweepQueues walk
  where
    walk schema tbl = go 0 Nothing
      where
        go acc cursor = do
          (n, next) <- refreshGroupsForQueue schema tbl groupsRefreshBatch cursor
          let total = acc + n
          maybe (pure total) (go total . Just) next

-- | Run a per-queue sweep over every queue, returning what each one swept and the
-- names of queues whose sweep threw.
sweepEachQueue
  :: (MonadUnliftIO m)
  => (SchemaName -> TableName -> m a)
  -> SchemaName
  -> [TableName]
  -> m ([(TableName, a)], [Text])
sweepEachQueue sweepOne schemaName queues = do
  (failures, swept) <- partitionEithers <$> traverse run queues
  pure (swept, failures)
  where
    run queue = bimap (const queue) (queue,) <$> tryAny (sweepOne schemaName queue)

-- | 'sweepEachQueue' for a sweep reporting the rows it touched, totalled.
sweepQueues
  :: (MonadUnliftIO m)
  => (SchemaName -> TableName -> m Int64)
  -> SchemaName
  -> [TableName]
  -> m (Int64, [Text])
sweepQueues sweepOne schemaName queues =
  first (sum . map snd) <$> sweepEachQueue sweepOne schemaName queues

-- | Sweep exhausted jobs across all queues. Returns the total moved and the
-- names of queues whose sweep failed.
sweepExhaustedJobs
  :: (MonadArbiter m)
  => SchemaName
  -> [TableName]
  -> m (Int64, [Text])
sweepExhaustedJobs = sweepQueues sweepExhaustedForQueue

-- | Move each exhausted job to the DLQ via the tree-aware 'moveToDLQFields', so
-- cascades and parent resumes are handled. One transaction for the pass, taking every
-- parent and every tree the moves will touch up front.
sweepExhaustedForQueue
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> m Int64
sweepExhaustedForQueue schemaName tableName = withDbTransaction $ do
  exhausted <- MA.executeQuery (Tmpl.selectExhaustedJobsSQL schemaName tableName exhaustedSweepBatch)
  let ids = [jobId | (jobId, _, _, _) <- exhausted]
  lockJobParents schemaName tableName [mParentId | (_, _, mParentId, _) <- exhausted]
  lockJobTrees schemaName tableName ids
  getSum <$> getAp (foldMap moveOne exhausted)
  where
    moveOne (jobId, cseq, mParentId, rollup) =
      Ap $
        Sum . fromRight 0
          <$> tryAny (moveToDLQFields LocksHeld Tmpl.MoveIfExhausted schemaName tableName sweepError jobId cseq mParentId rollup)
    sweepError = "max attempts exceeded (reaper sweep)"

-- | Per-queue cap on jobs swept to the DLQ in one reaper pass, so a large
-- backlog drains over several intervals instead of one unbounded fetch.
exhaustedSweepBatch :: Int
exhaustedSweepBatch = 1000

-- | Sweep force-cancel-flagged jobs whose lease has lapsed across all queues.
-- A live worker's heartbeat keeps its jobs' lease in the future, so those are
-- left for the worker's own cancel handler. Returns the total deleted and the
-- names of queues whose sweep failed.
sweepCancelledJobs
  :: (MonadArbiter m)
  => SchemaName
  -> [TableName]
  -> m (Int64, [Text])
sweepCancelledJobs = sweepQueues sweepCancelledForQueue

-- | Delete one queue's lease-lapsed flagged jobs, resuming any parents left
-- childless. 'deleteCancelledJobs' runs in its own transaction and re-checks
-- the lease under the row lock, so a job reclaimed since the select is left alone.
sweepCancelledForQueue
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> m Int64
sweepCancelledForQueue schemaName tableName = do
  ids <- MA.executeQuery (Tmpl.selectCancelledReapableJobsSQL schemaName tableName cancelledSweepBatch)
  fromIntegral . length <$> deleteCancelledJobs schemaName tableName Nothing ids

-- | Per-queue cap on flagged jobs reaped in one pass.
cancelledSweepBatch :: Int
cancelledSweepBatch = 1000

-- | Upsert a cron schedule's default expression and overlap policy.
--
-- Preserves user overrides and enabled state. @queue_name@ is overwritten on
-- conflict so a row whose @queue_name@ is @"pre-migration"@ heals to the live
-- queue on the next scheduler startup.
upsertCronDefault
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Text
  -- ^ Schedule name
  -> Text
  -- ^ Queue name
  -> Text
  -- ^ Default cron expression
  -> Text
  -- ^ Default overlap policy
  -> Maybe Text
  -- ^ Default IANA tz name (@Nothing@ = UTC).
  -> m Int64
upsertCronDefault schemaName scheduleName queueName defaultExpr defaultOv defaultTz =
  MA.executeStatement
    (Tmpl.upsertCronDefaultSQL schemaName scheduleName queueName defaultExpr defaultOv defaultTz)

-- | List cron schedules ordered by name, optionally filtered by queue.
listCronSchedules
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Maybe Text
  -- ^ Queue filter. 'Nothing' returns schedules for all queues.
  -> m [CronScheduleRow]
listCronSchedules schemaName mQueue =
  MA.executeQuery (Tmpl.listCronSchedulesSQL schemaName mQueue)

-- | Get a single cron schedule by name.
getCronScheduleByName
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Text
  -- ^ Schedule name
  -> m (Maybe CronScheduleRow)
getCronScheduleByName schemaName scheduleName = do
  rows <- MA.executeQuery (Tmpl.getCronScheduleByNameSQL schemaName scheduleName)
  pure $ case rows of
    [row] -> Just row
    _ -> Nothing

-- | Update a cron schedule (patch semantics).
--
-- Returns the number of rows affected (0 = not found, 1 = updated).
updateCronSchedule
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Text
  -- ^ Schedule name
  -> CronScheduleUpdate
  -> m Int64
updateCronSchedule schemaName scheduleName (CronScheduleUpdate mExpr mOverlap mTz mEnabled) = do
  let clauses :: [Q.Query ()]
      clauses =
        concat
          [ case mExpr of
              Nothing -> []
              Just Nothing -> [Q.raw "override_expression = NULL"]
              Just (Just expr) -> [[QQ.sql|override_expression = #{expr :: CText}|]]
          , case mOverlap of
              Nothing -> []
              Just Nothing -> [Q.raw "override_overlap = NULL"]
              Just (Just ov) -> [[QQ.sql|override_overlap = #{ov :: CText}|]]
          , case mTz of
              Nothing -> []
              Just Nothing -> [Q.raw "override_timezone = NULL"]
              Just (Just tz) -> [[QQ.sql|override_timezone = #{tz :: CText}|]]
          , case mEnabled of
              Nothing -> []
              Just True -> [Q.raw "enabled = TRUE"]
              Just False -> [Q.raw "enabled = FALSE", Q.raw "run_requested_at = NULL"]
          ]
  if null clauses
    then pure 0
    else do
      let tbl = CS.cronSchedulesTable schemaName
          setFrag = Q.sepBy ", " (clauses <> [Q.raw "updated_at = NOW()"])
      MA.executeStatement [QQ.sql|UPDATE ${tbl} SET ${setFrag} WHERE name = #{scheduleName :: CText}|]

-- | Update @last_fired_at@ to NOW() for a cron schedule.
touchCronLastFired
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Text
  -- ^ Schedule name
  -> m Int64
touchCronLastFired schemaName scheduleName =
  MA.executeStatement
    (Tmpl.touchCronLastFiredSQL schemaName scheduleName)

-- | Advance @last_checked_at@ to the supplied watermark for the given cron
-- schedule names. The watermark must be the minute boundary the scheduler
-- finished evaluating, not @NOW()@. A wrapping @GREATEST@ in the SQL keeps
-- the column monotonic when concurrent worker pools race.
touchCronChecked
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> UTCTime
  -- ^ Watermark (the minute the scheduler is advancing to)
  -> [Text]
  -- ^ Schedule names
  -> m Int64
touchCronChecked _ _ [] = pure 0
touchCronChecked schemaName watermark names =
  MA.executeStatement
    (Tmpl.touchCronCheckedSQL schemaName watermark names)

-- | Claim a minute floor for a schedule. 'True' = caller proceeds with the
-- insert. 'False' = another pool already fired this minute, skip.
tryFireCronGate
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Text
  -- ^ Schedule name
  -> UTCTime
  -- ^ Minute floor for the tick being attempted
  -> m Bool
tryFireCronGate schemaName scheduleName minuteFloor = do
  rows <- MA.executeStatement (Tmpl.tryFireCronGateSQL schemaName minuteFloor scheduleName)
  pure (rows > 0)

-- | Try to acquire the (schema, queue, name) cron leader lock. Must be inside a transaction.
tryAcquireCronLeader
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -- ^ Queue name
  -> Text
  -- ^ Schedule name
  -> m Bool
tryAcquireCronLeader schemaName queueName scheduleName = do
  rows <- MA.executeQuery (Tmpl.tryAcquireCronLeaderSQL schemaName queueName scheduleName)
  pure $ case rows of
    (True : _) -> True
    _ -> False

-- | Result of a manual run request.
data RunRequestOutcome = RunReqNotFound | RunReqDisabled | RunReqStamped | RunReqPending
  deriving stock (Eq, Show)

-- | Stamp a manual run request on an enabled schedule and NOTIFY the run-now
-- channel. A request already pending is left as it stands, unless it has gone
-- unclaimed long enough to expire.
requestCronRun
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Text
  -- ^ Schedule name
  -> m RunRequestOutcome
requestCronRun schemaName scheduleName = do
  rows <- MA.executeQuery (Tmpl.requestCronRunSQL schemaName scheduleName)
  pure $ case listToMaybe rows of
    Just "stamped" -> RunReqStamped
    Just "pending" -> RunReqPending
    Just "disabled" -> RunReqDisabled
    _ -> RunReqNotFound

-- | Claim a pending run request, returning the claimed row. 'Nothing' = another
-- pool won the claim, or the schedule was disabled meanwhile.
claimCronRun
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Text
  -- ^ Schedule name
  -> m (Maybe CronScheduleRow)
claimCronRun schemaName scheduleName = do
  rows <- MA.executeQuery (Tmpl.claimCronRunSQL schemaName scheduleName)
  pure $ listToMaybe rows

-- | Record when a manual run last fired a job for a cron schedule.
touchCronManualRun
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> UTCTime
  -- ^ When the manual run fired
  -> Text
  -- ^ Schedule name
  -> m Int64
touchCronManualRun schemaName firedAt scheduleName =
  MA.executeStatement
    (Tmpl.touchCronManualRunSQL schemaName firedAt scheduleName)

-- | Enabled schedules among @names@ that have a pending run request.
pendingCronRuns
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> [Text]
  -- ^ Schedule names
  -> m [Text]
pendingCronRuns _ [] = pure []
pendingCronRuns schemaName names =
  MA.executeQuery (Tmpl.pendingCronRunsSQL schemaName names)

-- ---------------------------------------------------------------------------
-- Worker Registry Operations
-- ---------------------------------------------------------------------------

-- | Register a worker pool in the @arbiter_workers@ table, or refresh its metadata
-- if already registered. Bumps @last_heartbeat@ and clears @shutting_down@
-- either way. Returns the worker's effective paused state so callers can seed
-- local state without a second round-trip.
registerWorker
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -- ^ Worker pool UUID
  -> Text
  -- ^ Queue name
  -> Maybe Text
  -- ^ Host name
  -> Maybe Int32
  -- ^ Worker thread count
  -> NominalDiffTime
  -- ^ Stale threshold in seconds (recorded on the row so the UI can compute liveness).
  -> Maybe Value
  -- ^ Extra JSONB metadata
  -> m (Maybe Bool)
registerWorker schemaName workerId queue host threads staleThreshold metadata = do
  rows <-
    MA.executeQuery
      (Tmpl.upsertWorkerSQL schemaName workerId queue host threads (realToFrac staleThreshold) metadata)
  pure $ listToMaybe rows

-- | Bump @last_heartbeat@ for a registered worker and return the effective
-- paused state (per-worker OR per-queue). 'Nothing' if no row exists for the
-- given UUID.
heartbeatWorker
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -- ^ Worker pool UUID
  -> m (Maybe Bool)
heartbeatWorker schemaName workerId = do
  rows <- MA.executeQuery (Tmpl.heartbeatWorkerSQL schemaName workerId)
  pure $ listToMaybe rows

-- | Set the @paused@ flag for a registered worker.
setWorkerPaused
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -> Bool
  -> m Int64
setWorkerPaused schemaName workerId p =
  countOr0 (Tmpl.setWorkerPausedSQL schemaName p workerId)

-- | Mark a worker as gracefully draining. The row is left in place so the UI
-- can distinguish drained workers from ones that vanished.
markWorkerShuttingDown
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -> m Int64
markWorkerShuttingDown schemaName workerId =
  MA.executeStatement
    (Tmpl.markWorkerShuttingDownSQL schemaName workerId)

-- | Remove a worker row outright.
deregisterWorker
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -> m Int64
deregisterWorker schemaName workerId =
  MA.executeStatement
    (Tmpl.deleteWorkerSQL schemaName workerId)

-- | List workers with optional filters: scope to a single queue, restrict to
-- recent heartbeats, both, or neither.
listWorkers
  :: (MonadArbiter m)
  => SchemaName
  -> Maybe Text
  -- ^ Queue name. 'Nothing' returns workers from all queues.
  -> Maybe NominalDiffTime
  -- ^ Liveness threshold in seconds. 'Nothing' returns workers regardless of heartbeat age.
  -> m [WorkerRow]
listWorkers schemaName mQueue mLiveSecs =
  MA.executeQuery
    (Tmpl.listWorkersSQL schemaName mQueue (realToFrac <$> mLiveSecs))

-- | Delete worker rows (including paused ones) whose @last_heartbeat@ is older
-- than each row's own @stale_threshold_secs@.
sweepStaleWorkers
  :: (MonadArbiter m)
  => SchemaName
  -> m Int64
sweepStaleWorkers schemaName =
  MA.executeStatement (Tmpl.deleteStaleWorkersSQL schemaName)

-- ---------------------------------------------------------------------------
-- Queue Operations
-- ---------------------------------------------------------------------------

-- | Insert an arbiter_queues row with defaults if one doesn't already exist.
ensureQueue
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -- ^ Queue name
  -> m Int64
ensureQueue schemaName queue =
  MA.executeStatement
    (Tmpl.ensureQueueSQL schemaName queue)

-- | Set the queue's @paused@ flag, creating the row if missing.
setQueuePaused
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -- ^ Queue name
  -> Bool
  -> m Int64
setQueuePaused schemaName queue p =
  countOr0 (Tmpl.setQueuePausedSQL schemaName queue p)

-- | Get the arbiter_queues row for a single queue. 'Nothing' if absent.
getQueue
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -- ^ Queue name
  -> m (Maybe QueueRow)
getQueue schemaName queue = do
  rows <- MA.executeQuery (Tmpl.getQueueSQL schemaName queue)
  pure $ listToMaybe rows

-- | List all arbiter_queues rows, ordered by queue name.
listQueues
  :: (MonadArbiter m)
  => SchemaName
  -> m [QueueRow]
listQueues schemaName =
  MA.executeQuery (Tmpl.listQueuesSQL schemaName)

-- ---------------------------------------------------------------------------
-- Global Gate Operations
-- ---------------------------------------------------------------------------

-- | Bound the current transaction's statements to a wall-clock limit, so a stuck op aborts at the DB rather than hanging the caller.
setLocalStatementTimeout :: (MonadArbiter m) => NominalDiffTime -> m ()
setLocalStatementTimeout limit =
  let ms = ceiling (realToFrac limit * 1000 :: Double) :: Int
      msTxt = T.pack (show ms)
   in void $
        MA.executeQuery
          [QQ.sql|SELECT set_config('statement_timeout', '${msTxt}', true) AS @{set_config :: CText}|]

-- | 'runGated' with each statement of @work@ bounded by @limit@. The bound is
-- transaction-local, which the gate transaction 'work' runs in makes effective.
runGatedBounded :: (MonadArbiter m) => SchemaName -> Text -> NominalDiffTime -> NominalDiffTime -> m a -> m (Maybe a)
runGatedBounded schemaName task interval limit work =
  runGated schemaName task interval (setLocalStatementTimeout limit >> work)

-- | Run @work@ at most once per @interval@ across every worker pool sharing
-- the same schema, keyed by @task@. Uses a watermark row in @arbiter_gates@
-- claimed via @SELECT FOR UPDATE SKIP LOCKED@, so the interval check and the
-- mutual exclusion happen in one statement. Returns @Just@ the work's result
-- if it ran, @Nothing@ if either the gate said "too recent" or another pool
-- was already running the task.
runGated
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -- ^ Task identifier (used as the gate row key).
  -> NominalDiffTime
  -- ^ Minimum interval between runs, in seconds.
  -> m a
  -- ^ Work to perform when this caller wins the gate.
  -> m (Maybe a)
runGated schemaName task interval work =
  runGatedInner schemaName task interval (const ((,Nothing) <$> work))

-- | 'runGated' where the task resumes from the state its last run left in the gate row,
-- read under the claim and written with the watermark. A payload that no longer parses
-- reads as no state.
runGatedState
  :: (FromJSON s, MonadArbiter m, ToJSON s)
  => SchemaName
  -> Text
  -> NominalDiffTime
  -> (Maybe s -> m (a, s))
  -> m (Maybe a)
runGatedState schemaName task interval work =
  runGatedInner schemaName task interval (fmap (second (Just . toJSON)) . work . (>>= parseMaybe parseJSON))

-- | 'runGatedState' with each statement of @work@ bounded by @limit@, as
-- 'runGatedBounded' bounds 'runGated'.
runGatedStateBounded
  :: (FromJSON s, MonadArbiter m, ToJSON s)
  => SchemaName
  -> Text
  -> NominalDiffTime
  -> NominalDiffTime
  -> (Maybe s -> m (a, s))
  -> m (Maybe a)
runGatedStateBounded schemaName task interval limit work =
  runGatedState schemaName task interval (\state -> setLocalStatementTimeout limit >> work state)

-- | The gate body both forms share. 'Nothing' back from @work@ leaves the row's state
-- as it was.
runGatedInner
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -> NominalDiffTime
  -> (Maybe Value -> m (a, Maybe Value))
  -> m (Maybe a)
runGatedInner schemaName task interval work = do
  _ <-
    MA.executeStatement
      (Tmpl.ensureGateRowSQL schemaName task)
  gateOpen <- checkGateOuter
  if not gateOpen
    then pure Nothing
    else withDbTransaction $ tryClaimGate >>= traverse ran
  where
    intervalSecs = realToFrac interval :: Double

    checkGateOuter = do
      rows <- MA.executeQuery (Tmpl.checkGateSQL schemaName intervalSecs task)
      pure $ fromMaybe True (listToMaybe rows)

    tryClaimGate = listToMaybe <$> MA.executeQuery (Tmpl.tryClaimGateSQL schemaName task intervalSecs)

    ran state = do
      (r, next) <- work state
      r <$ MA.executeStatement (maybe (Tmpl.bumpGateSQL schemaName task) (Tmpl.bumpGateStateSQL schemaName task) next)

-- | A gate name for a set of parts: the sorted set itself while it fits the gate's
-- key, an md5 digest of it beyond that.
gateNameFor :: (MonadArbiter m) => Text -> [Text] -> m Text
gateNameFor prefix parts
  | T.length joined <= maxGateNameLength = pure (prefix <> ":" <> joined)
  | otherwise = do
      rows <- MA.executeQuery (Tmpl.gateNameDigestSQL joined)
      pure (prefix <> ":#" <> fromMaybe joined (listToMaybe rows))
  where
    joined = T.intercalate "," (sort parts)

-- | Well under the btree index-row limit the gates table's primary key sits on.
maxGateNameLength :: Int
maxGateNameLength = 200

-- | Where a shared result came from.
data Shared a
  = -- | This caller won the gate and ran the work itself.
    Ran a
  | -- | Read from the gate, with its age in seconds.
    Published Double a
  | -- | A published result this caller could not decode, with the parse error.
    Unreadable Text
  deriving stock (Eq, Functor, Show)

-- | 'runGated' where the callers that lost the gate read the winner's published
-- result. 'Nothing' once none is fresh within @maxAge@. The winner runs @work@
-- after the gate transaction commits, so a slow scan holds neither the gate row
-- nor a read snapshot. Exclusion is by interval rather than by lock, and the interval
-- restarts from the publish. A run or publish that throws puts the watermark back, so a
-- winner that keeps failing does not keep every other caller from running. That
-- compensation is bounded by @interval@.
runGatedShared
  :: (FromJSON a, MonadArbiter m, ToJSON a)
  => SchemaName
  -> Text
  -> NominalDiffTime
  -- ^ Minimum interval between runs.
  -> NominalDiffTime
  -- ^ How long a published result stands.
  -> m a
  -> m (Maybe (Shared a))
runGatedShared schemaName task interval maxAge work =
  MA.executeQuery claimOrRead >>= maybe (pure Nothing) shared . listToMaybe
  where
    claimOrRead =
      Tmpl.claimOrReadGateSQL schemaName task (realToFrac interval) (realToFrac maxAge)
    shared (mClaimedAt, mPrevious, mPayload, mAge) = case (mClaimedAt, mPrevious) of
      (Just at, Just previous) -> Just . Ran <$> publish at previous
      _ -> pure (decoded <$> mPayload <*> mAge)
    -- Base onException: UnliftIO's masks the handler uninterruptibly.
    publish at previous = withRunInIO $ \run ->
      run (work >>= \a -> a <$ MA.executeStatement (Tmpl.setGateMetadataSQL schemaName (toJSON a) at task))
        `E.onException` run (reopen at previous)
    reopen at previous =
      void (tryAny (UIO.timeout (micros interval) (MA.executeStatement (Tmpl.releaseGateSQL schemaName task at previous))))
    decoded v age = either (Unreadable . (\e -> task <> " gate payload: " <> T.pack e)) (Published age) (parseEither parseJSON v)

-- | Read child results, DLQ errors, parent_state snapshot, and DLQ failures
-- for a rollup finalizer in a single query.
readChildResultsRaw
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID
  -> m (Map.Map Int64 Value, Map.Map Int64 Text, Maybe Value, Map.Map Int64 Text)
readChildResultsRaw schemaName tableName parentJobId = do
  rows <- MA.executeQuery (Tmpl.readChildResultsSQL schemaName tableName parentJobId)
  foldM parseRow (Map.empty, Map.empty, Nothing, Map.empty) rows
  where
    parseRow (!results, !errors, !snap, !dlqFailures) row = case row of
      ("r", Just cid, Just val, _, _) ->
        pure (Map.insert cid val results, errors, snap, dlqFailures)
      ("e", Just jid, _, Just err, Just dlqPk) ->
        pure (results, Map.insert jid err errors, snap, Map.insert dlqPk err dlqFailures)
      ("e", Just jid, _, Nothing, Just dlqPk) ->
        pure (results, Map.insert jid "" errors, snap, Map.insert dlqPk "" dlqFailures)
      ("s", _, Just val, _, _) ->
        pure (results, errors, Just val, dlqFailures)
      _ -> throwParsing $ "readChildResultsRaw: unexpected row: " <> T.pack (show row)

-- | Read the raw @parent_state@ snapshot from the DB.
--
-- Internal operation used by the worker for DLQ-retried finalizers
-- that have a persisted snapshot.
getParentStateSnapshot
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Job ID
  -> m (Maybe Value)
getParentStateSnapshot schemaName tableName jobId = do
  rows <- MA.executeQuery (Tmpl.getParentStateSnapshotSQL schemaName tableName jobId)
  case rows of
    [val] -> pure val
    _ -> pure Nothing

-- | Merge raw child results from three sources.
--
-- Precedence (left-biased union): DLQ errors > results > snapshot.
mergeRawChildResults
  :: Map.Map Int64 Value
  -> Map.Map Int64 Text
  -> Maybe Value
  -> Map.Map Int64 (Either Text Value)
mergeRawChildResults results failures mSnapshot =
  Map.map Left failures
    `Map.union` Map.map Right results
    `Map.union` base
  where
    base = case mSnapshot of
      Just val | Success m <- fromJSON val -> m
      _ -> Map.empty

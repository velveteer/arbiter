{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingVia #-}
{-# LANGUAGE OverloadedStrings #-}

module Arbiter.Core.Operations
  ( -- * Job Insertion
    insertJob
  , insertJobUnsafe
  , insertJobsBatch
  , insertJobsBatch_
  , insertResult
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
  , archivesOnAck
  , setVisibilityTimeout
  , setVisibilityTimeoutBatch
  , VisibilityUpdateInfo (..)
  , updateJobForRetry
  , nackJob
  , moveToDLQ
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

    -- * Internal Operations
  , getParentStateSnapshot
  , readChildResultsRaw
  , mergeRawChildResults
  ) where

import Control.Monad (foldM, join, void, when)
import Data.Aeson (FromJSON, Result (..), ToJSON, Value, fromJSON, toJSON)
import Data.Bifunctor (first)
import Data.Bitraversable (bitraverse)
import Data.Either (partitionEithers)
import Data.Foldable (for_, toList)
import Data.Functor qualified as Functor
import Data.Int (Int32, Int64)
import Data.IntMap qualified as IntMap
import Data.List (groupBy, sortOn)
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, fromMaybe, isJust, listToMaybe, mapMaybe)
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
import UnliftIO (MonadUnliftIO, tryAny)

import Arbiter.Core.Codec
  ( Col (..)
  , Params
  , RowCodec
  , archiveRowCodec
  , cArray
  , cDecode
  , cScalar
  , col
  , concurrencyKeyViewCodec
  , concurrencyPolicyViewCodec
  , countCodec
  , cronScheduleRowCodec
  , dlqRowCodec
  , jobCodec
  , jobRowCodec
  , ncol
  , parr
  , pnul
  , pval
  , queueRowCodec
  , rateLimitBucketCodec
  , rateLimitPolicyViewCodec
  , workerRowCodec
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
  , DedupKey (IgnoreDuplicate, ReplaceDuplicate)
  , Job (..)
  , JobPayload
  , JobRead
  , JobStatus
  , JobWrite
  , dedupParts
  , isRollup
  , jobStatusFromText
  , jobStatusToText
  )
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
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
import Arbiter.Core.Sql.Jobs qualified as Tmpl
import Arbiter.Core.Sql.Lifecycle qualified as Tmpl
import Arbiter.Core.Sql.Queues qualified as Tmpl
import Arbiter.Core.Sql.RateLimit qualified as Tmpl
import Arbiter.Core.Sql.Stats qualified as Tmpl
import Arbiter.Core.Sql.Tree qualified as Tmpl
import Arbiter.Core.Sql.Workers qualified as Tmpl
import Arbiter.Core.Worker (WorkerRow)

decodePayload :: (JobPayload payload, MonadArbiter m) => JobRead Value -> m (JobRead payload)
decodePayload job = case fromJSON (payload job) of
  Success p -> pure $ job {payload = p}
  Error e -> throwParsing $ "Failed to decode job payload: " <> T.pack e

boolCodec :: RowCodec Bool
boolCodec = col "result" CBool

int64Codec :: RowCodec Int64
int64Codec = col "result" CInt8

resultsRowCodec :: RowCodec (Int64, Value)
resultsRowCodec = (,) <$> col "child_id" CInt8 <*> col "result" CJsonb

errorResultsRowCodec :: RowCodec (Int64, Maybe Text)
errorResultsRowCodec = (,) <$> col "job_id" CInt8 <*> ncol "last_error" CText

visibilityUpdateCodec :: RowCodec VisibilityUpdateInfo
visibilityUpdateCodec =
  VisibilityUpdateInfo
    <$> col "id" CInt8
    <*> col "was_heartbeated" CBool
    <*> ncol "current_db_attempts" CInt4
    <*> col "cancel_requested" CBool

parentCountCodec :: RowCodec (Int64, (Int64, Int64))
parentCountCodec =
  (\pid cnt paused -> (pid, (cnt, paused)))
    <$> col "parent_id" CInt8
    <*> col "count" CInt8
    <*> col "count_suspended" CInt8

dlqParentCountCodec :: RowCodec (Int64, Int64)
dlqParentCountCodec = (,) <$> col "parent_id" CInt8 <*> col "count" CInt8

childResultsRowCodec :: RowCodec (Text, Maybe Int64, Maybe Value, Maybe Text, Maybe Int64)
childResultsRowCodec =
  (,,,,)
    <$> col "source" CText
    <*> ncol "child_id" CInt8
    <*> ncol "result" CJsonb
    <*> ncol "error" CText
    <*> ncol "dlq_pk" CInt8

nullableInt64Codec :: RowCodec (Maybe Int64)
nullableInt64Codec = ncol "parent_id" CInt8

buildWhereClause :: [Tmpl.JobFilter] -> (Text, Params)
buildWhereClause [] = ("", [])
buildWhereClause filters =
  let (clauses, params) = Functor.unzip $ map filterToClause filters
   in ("WHERE " <> T.intercalate " AND " clauses, concat params)

filterToClause :: Tmpl.JobFilter -> (Text, Params)
filterToClause (Tmpl.FilterGroupKey gk) = ("group_key = ?", [pval CText gk])
filterToClause (Tmpl.FilterParentId pid) = ("parent_id = ?", [pval CInt8 pid])
filterToClause Tmpl.FilterRootsOnly = ("parent_id IS NULL", [])
filterToClause (Tmpl.FilterStatus s) = ("status = ?", [pval CText (jobStatusToText s)])
filterToClause (Tmpl.FilterId i) = ("id = ?", [pval CInt8 i])
filterToClause (Tmpl.FilterJobId i) = ("job_id = ?", [pval CInt8 i])

-- | Execute a count/rows-affected query returning a single Int64.
-- Returns 0 if the result set is empty or unexpected.
queryCount :: (MonadArbiter m) => Text -> Params -> m Int64
queryCount sql params = do
  rows <- executeQuery sql params countCodec
  case rows of
    [n] -> pure n
    _ -> pure 0

-- | Like 'queryCount' but throws a parse error on unexpected results.
queryCountStrict :: (MonadArbiter m) => Text -> Text -> Params -> m Int64
queryCountStrict label sql params = do
  rows <- executeQuery sql params countCodec
  case rows of
    [n] -> pure n
    _ -> throwParsing $ label <> ": unexpected result"

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
insertJobUnsafe schemaName tableName job = withDbTransaction $ do
  let codec = jobCodec tableName
      sql = case dedupKey job of
        Just (ReplaceDuplicate _) -> Tmpl.insertJobReplaceSQL schemaName tableName
        _ -> Tmpl.insertJobSQL schemaName tableName
      params = cScalar codec (job, admissionColumns (payload job))

  rawJobs <- executeQuery sql params (cDecode codec)
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
    executeStatement
      (Tmpl.addRateLimitTokensSQL schemaName)
      [pval CText (rateLimitKeyText key), pval CText (rlkPrefix key), pval CFloat8 amount]

-- | Delete full, idle rate-limit buckets. Returns the number pruned.
pruneRateLimitBuckets :: (MonadArbiter m) => SchemaName -> NominalDiffTime -> m Int64
pruneRateLimitBuckets schemaName idle =
  executeStatement
    (Tmpl.pruneRateLimitBucketsSQL schemaName)
    [pval CFloat8 (realToFrac idle)]

-- | Refill every bucket under a prefix to full. Returns the number refilled. Used to
-- build a fixed window from a manual policy plus a cron.
resetRateLimitBuckets :: (MonadArbiter m) => SchemaName -> Text -> m Int64
resetRateLimitBuckets schemaName prefix =
  executeStatement
    (Tmpl.resetRateLimitBucketsSQL schemaName)
    [pval CText prefix]

-- | Make a prefix's throttled jobs claimable again across the given queue tables,
-- in one statement. Returns the number woken.
wakeThrottledJobs :: (MonadArbiter m) => SchemaName -> [TableName] -> Text -> m Int64
wakeThrottledJobs _ [] _ = pure 0
wakeThrottledJobs schemaName tableNames prefix =
  queryCountStrict
    "wakeThrottledJobs"
    (Tmpl.wakeThrottledJobsSQL schemaName tableNames)
    (map (const (pval CText prefix)) tableNames)

-- | Wake one key's throttled jobs across the given tables, in one statement.
-- Returns the count.
wakeThrottledJobsForKey :: (MonadArbiter m) => SchemaName -> [TableName] -> RateLimitKey -> m Int64
wakeThrottledJobsForKey _ [] _ = pure 0
wakeThrottledJobsForKey schemaName tableNames key =
  queryCountStrict
    "wakeThrottledJobsForKey"
    (Tmpl.wakeThrottledJobsForKeySQL schemaName tableNames)
    (concatMap (const [pval CText (rlkPrefix key), pval CText (rateLimitKeyText key)]) tableNames)

-- | List every policy with its default/override params, bucket aggregates, and
-- live throttled count across the given queue tables.
listRateLimitPolicies :: (MonadArbiter m) => SchemaName -> [TableName] -> m [RateLimitPolicyView]
listRateLimitPolicies schemaName tableNames =
  executeQuery (Tmpl.listRateLimitPoliciesSQL schemaName tableNames) [] rateLimitPolicyViewCodec

-- | One prefix's policy view with bucket aggregates and live throttled count.
getRateLimitPolicy :: (MonadArbiter m) => SchemaName -> [TableName] -> Text -> m (Maybe RateLimitPolicyView)
getRateLimitPolicy schemaName tableNames prefix =
  listToMaybe
    <$> executeQuery (Tmpl.getRateLimitPolicySQL schemaName tableNames) [pval CText prefix] rateLimitPolicyViewCodec

-- | Whether a rate-limit policy exists for a prefix.
rateLimitPolicyExists :: (MonadArbiter m) => SchemaName -> Text -> m Bool
rateLimitPolicyExists schemaName prefix =
  or <$> executeQuery (Tmpl.rateLimitPolicyExistsSQL schemaName) [pval CText prefix] boolCodec

-- | List a prefix's buckets with effective max and fill fraction, paginated.
listRateLimitBuckets :: (MonadArbiter m) => SchemaName -> Text -> Int -> Int -> m [RateLimitBucketView]
listRateLimitBuckets schemaName prefix limit offset =
  executeQuery
    (Tmpl.listRateLimitBucketsSQL schemaName)
    [pval CText prefix, pval CInt8 (fromIntegral limit), pval CInt8 (fromIntegral offset)]
    rateLimitBucketCodec

-- | Set or clear a policy's override params. Returns rows affected (0 if absent).
updateRateLimitPolicyOverrides :: (MonadArbiter m) => SchemaName -> Text -> RateLimitPolicyUpdate -> m Int64
updateRateLimitPolicyOverrides schemaName prefix (RateLimitPolicyUpdate mMax mRefill mIv) =
  executeStatement
    (Tmpl.updateRateLimitOverridesSQL schemaName)
    [ pval CBool (isJust mMax)
    , pnul CFloat8 (join mMax)
    , pval CBool (isJust mRefill)
    , pnul CFloat8 (join mRefill)
    , pval CBool (isJust mIv)
    , pnul CFloat8 (join mIv)
    , pval CText prefix
    ]

-- | Apply a pool's override-limit patch (retunes every key under the prefix).
-- Returns rows affected.
updateConcurrencyPolicyOverrides :: (MonadArbiter m) => SchemaName -> Text -> ConcurrencyPolicyUpdate -> m Int64
updateConcurrencyPolicyOverrides schemaName prefix (ConcurrencyPolicyUpdate mLim) =
  executeStatement
    (Tmpl.updateConcurrencyPolicyOverrideSQL schemaName)
    [pval CBool (isJust mLim), pnul CInt4 (join mLim), pval CText prefix]

-- | List every concurrency pool with its default/override limit and live key and
-- in-flight aggregates.
listConcurrencyPolicies :: (MonadArbiter m) => SchemaName -> m [ConcurrencyPolicyView]
listConcurrencyPolicies schemaName =
  executeQuery (Tmpl.listConcurrencyPoliciesSQL schemaName) [] concurrencyPolicyViewCodec

-- | One prefix's concurrency pool view with live aggregates.
getConcurrencyPolicy :: (MonadArbiter m) => SchemaName -> Text -> m (Maybe ConcurrencyPolicyView)
getConcurrencyPolicy schemaName prefix =
  listToMaybe
    <$> executeQuery (Tmpl.getConcurrencyPolicySQL schemaName) [pval CText prefix] concurrencyPolicyViewCodec

-- | List a prefix's keys with effective cap and fill fraction, paginated.
listConcurrencyKeys :: (MonadArbiter m) => SchemaName -> Text -> Int -> Int -> m [ConcurrencyKeyView]
listConcurrencyKeys schemaName prefix limit offset =
  executeQuery
    (Tmpl.listConcurrencyKeysSQL schemaName)
    [pval CText prefix, pval CInt8 (fromIntegral limit), pval CInt8 (fromIntegral offset)]
    concurrencyKeyViewCodec

-- | Delete drained concurrency rows with no live job across the given tables. Returns
-- the number pruned. A key whose advisory try-lock is contended is skipped until the next pass.
pruneConcurrencyKeys :: (MonadArbiter m) => SchemaName -> [TableName] -> m Int64
pruneConcurrencyKeys _ [] = pure 0
pruneConcurrencyKeys schemaName tableNames = withDbTransaction $ do
  dead <- executeQuery (Tmpl.lockDeadConcurrencyKeysSQL schemaName tableNames) [] (col "concurrency_key" CText)
  if null dead
    then pure 0
    else do
      locked <- executeQuery Tmpl.tryLockDeadConcurrencyAdvisorySQL [parr CText dead] (col "concurrency_key" CText)
      if null locked
        then pure 0
        else executeStatement (Tmpl.pruneLockedConcurrencyKeysSQL schemaName tableNames) [parr CText locked]

-- | Lock the count rows, then recount those keys under the lock so a claim's increment
-- is not overwritten. A key seeded after the lock pass is left to its triggers.
reconcileConcurrencyCounts :: (MonadArbiter m) => SchemaName -> [TableName] -> m Int64
reconcileConcurrencyCounts _ [] = pure 0
reconcileConcurrencyCounts schemaName tableNames = withDbTransaction $ do
  held <- executeQuery (Tmpl.lockConcurrencyCountsSQL schemaName) [] (col "concurrency_key" CText)
  rows <-
    executeQuery
      (Tmpl.reconcileConcurrencyCountsSQL schemaName tableNames)
      [parr CText held]
      (col "reconciled" CInt8)
  pure (fromMaybe 0 (listToMaybe rows))

-- | Rebuild the counts only if a crash truncated the UNLOGGED table.
reconcileConcurrencyCountsIfStale :: (MonadArbiter m) => SchemaName -> [TableName] -> m ()
reconcileConcurrencyCountsIfStale _ [] = pure ()
reconcileConcurrencyCountsIfStale schemaName tableNames = do
  stale <- executeQuery (Tmpl.concurrencyCountsStaleSQL schemaName tableNames) [] (col "stale" CBool)
  when (or stale) (void (reconcileConcurrencyCounts schemaName tableNames))

-- | Reconcile then prune, skipped entirely when no concurrency key exists.
reconcileAndPruneConcurrency :: (MonadArbiter m) => SchemaName -> [TableName] -> m ()
reconcileAndPruneConcurrency schemaName tableNames = do
  hasKeys <- executeQuery (Tmpl.concurrencyHasAnyKeySQL schemaName) [] (col "present" CBool)
  when (or hasKeys) $ do
    void $ reconcileConcurrencyCounts schemaName tableNames
    void $ pruneConcurrencyKeys schemaName tableNames

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
insertJobsBatch schemaName tableName jobs = withDbTransaction $ do
  let codec = jobCodec tableName
      params = cArray codec [(j, admissionColumns (payload j)) | j <- dedupBatch jobs]

  rawJobs <- executeQuery (Tmpl.insertJobsBatchSQL schemaName tableName) params (cDecode codec)
  traverse decodePayload rawJobs

insertJobsBatch_
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -> Text
  -> [JobWrite payload]
  -> m Int64
insertJobsBatch_ _ _ [] = pure 0
insertJobsBatch_ schemaName tableName jobs = withDbTransaction $ do
  let params = cArray (jobCodec tableName) [(j, admissionColumns (payload j)) | j <- dedupBatch jobs]
  executeStatement (Tmpl.insertJobsBatchSQL_ schemaName tableName) params

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
  executeStatement
    (Tmpl.insertResultSQL schemaName tableName)
    [ pval CInt8 parentJobId
    , pval CInt8 childId
    , pval CJsonb result
    ]

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
  rows <-
    executeQuery
      (Tmpl.getResultsByParentSQL schemaName tableName)
      [pval CInt8 parentJobId]
      resultsRowCodec
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
  rows <-
    executeQuery
      (Tmpl.getDLQChildErrorsByParentSQL schemaName tableName)
      [pval CInt8 parentJobId]
      errorResultsRowCodec
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
  executeStatement
    (Tmpl.persistParentStateSQL schemaName tableName)
    [pval CJsonb state, pval CInt8 jobId]

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
  rawJobs <- executeQueryPrepared (claimSqlFor cs maxJobs) [] (jobRowCodec (claimSqlTable cs))
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
      rawJobs <- executeQueryPrepared (claimSqlFor cs maxBatches) [] (jobRowCodec (claimSqlTable cs))
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
  for_ (parentId job) $ \pid ->
    void $
      executeQuery
        "SELECT pg_advisory_xact_lock(hashtextextended(?, ?))::text AS result"
        [pval CText (schemaName <> "." <> tableName), pval CInt8 pid]
        (ncol "result" CText)
  let jid = primaryKey job
      jatt = attempts job
      params = [pval CInt8 jid, pval CInt4 jatt, pval CInt8 jid, pval CInt8 jid, pval CInt4 jatt, pval CInt8 jid]
  rows <- executeQuery (Tmpl.smartAckJobSQL (archivesOnAck job) schemaName tableName) params int64Codec
  case rows of
    [n] -> pure n
    _ -> pure 0

-- | Wake a suspended parent if all children are done.
tryResumeParent :: (MonadArbiter m) => SchemaName -> TableName -> Int64 -> m ()
tryResumeParent schemaName tableName pid = do
  void $
    executeQuery
      "SELECT pg_advisory_xact_lock(hashtextextended(?, ?))::text AS result"
      [pval CText (schemaName <> "." <> tableName), pval CInt8 pid]
      (ncol "result" CText)
  void $
    executeStatement
      (Tmpl.tryWakeAncestorSQL schemaName tableName)
      [pval CInt8 pid, pval CInt8 pid]

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
  -- Serialize concurrent sibling acks by locking the distinct parents in a
  -- stable order (deadlock-free), then ack the whole set in one statement.
  let parents = Set.toAscList $ Set.fromList [pid | job <- jobs, Just pid <- [parentId job]]
  for_ parents $ \pid ->
    void $
      executeQuery
        "SELECT pg_advisory_xact_lock(hashtextextended(?, ?))::text AS result"
        [pval CText (schemaName <> "." <> tableName), pval CInt8 pid]
        (ncol "result" CText)
  executeQuery
    (Tmpl.smartAckJobsBatchSQL (any archivesOnAck jobs) schemaName tableName)
    [parr CInt8 (map primaryKey jobs), parr CInt4 (map attempts jobs)]
    (col "id" CInt8)

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
  -- ^ Returns the number of rows updated (0 if job was reclaimed by another worker)
setVisibilityTimeout schemaName tableName timeout job =
  executeStatement
    (Tmpl.setVisibilityTimeoutSQL schemaName tableName)
    [ pval CFloat8 (realToFrac timeout)
    , pval CFloat8 (realToFrac timeout)
    , pval CInt8 (primaryKey job)
    , pval CInt4 (attempts job)
    ]

-- | Detailed information about the result of a visibility update operation for a single job.
data VisibilityUpdateInfo = VisibilityUpdateInfo
  { vuiJobId :: Int64
  -- ^ The ID of the job that was targeted.
  , vuiWasUpdated :: Bool
  -- ^ 'True' if the job's visibility timeout was successfully extended.
  , vuiCurrentDbAttempts :: Maybe Int32
  -- ^ The current attempt count of the job in the database.
  -- This is used to distinguish between a stolen job (attempts changed)
  -- and an acked job (row is missing, so this is 'Nothing').
  , vuiCancelRequested :: Bool
  -- ^ 'True' if a force-cancel has flagged this job.
  }
  deriving stock (Eq, Generic, Show)

-- | Batch variant of 'setVisibilityTimeout'. Returns per-job status
-- (success, acked, or stolen).
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
  let valuesPlaceholder = T.intercalate "," $ replicate (length jobs) "(?,?)"
      jobParams = concatMap (\job -> [pval CInt8 (primaryKey job), pval CInt4 (attempts job)]) jobs
      params = jobParams <> [pval CFloat8 (realToFrac timeout), pval CFloat8 (realToFrac timeout)]

  executeQuery
    (Tmpl.setVisibilityTimeoutBatchSQL schemaName tableName valuesPlaceholder)
    params
    visibilityUpdateCodec

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
  executeStatement
    (Tmpl.updateJobForRetrySQL schemaName tableName)
    [ pval CInt8 (ceiling backoff)
    , pval CText errorMsg
    , pval CInt8 (primaryKey job)
    , pval CInt4 (attempts job)
    ]

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
  executeStatement
    (Tmpl.nackJobSQL schemaName tableName)
    [ pval CInt8 (primaryKey job)
    , pval CInt4 (attempts job)
    ]

-- | Move a job to the DLQ. Cascades descendants for rollup parents.
-- Wakes the parent if this was a child job.
--
-- Returns 0 if the job was already claimed by another worker.
moveToDLQ
  :: forall m payload
   . (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Text
  -- ^ Error message (the final error that caused the DLQ move)
  -> JobRead payload
  -> m Int64
moveToDLQ schemaName tableName errorMsg job =
  moveToDLQFields schemaName tableName errorMsg (primaryKey job) (attempts job) (parentId job) (isRollup job)

-- | 'moveToDLQ' driven by scalar fields, so callers without a typed 'JobRead'
-- (the reaper sweep) can reuse the tree-aware move.
moveToDLQFields
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> Text
  -- ^ Error message (the final error that caused the DLQ move)
  -> Int64
  -- ^ Job id
  -> Int32
  -- ^ Attempts (for the optimistic move check)
  -> Maybe Int64
  -- ^ Parent id, if a child
  -> Bool
  -- ^ Whether the job is a rollup finalizer
  -> m Int64
moveToDLQFields schemaName tableName errorMsg jobId atts mParentId rollup = withDbTransaction $ do
  when rollup $ do
    snapshotDescendantRollups schemaName tableName jobId
    void $ cascadeChildrenToDLQ schemaName tableName jobId "Parent moved to DLQ"
  rows <-
    queryCount
      (Tmpl.moveToDLQSQL schemaName tableName)
      [ pval CInt8 jobId
      , pval CInt4 atts
      , pval CText errorMsg
      ]
  when (rows > 0) $
    for_ mParentId $ \pid ->
      tryResumeParent schemaName tableName pid
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
  queryCountStrict
    "cascadeChildrenToDLQ"
    (Tmpl.cascadeChildrenToDLQSQL schemaName tableName)
    [pval CInt8 parentJobId, pval CText errorMsg]

-- | Snapshot child results for descendant rollup finalizers before cascade-DLQ.
-- Persists accumulated results into @parent_state@ so they survive deletion.
snapshotDescendantRollups
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> TableName
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID (root of the subtree being cascaded)
  -> m ()
snapshotDescendantRollups schemaName tableName parentJobId = do
  rollupIds <-
    executeQuery
      (Tmpl.descendantRollupIdsSQL schemaName tableName)
      [pval CInt8 parentJobId]
      int64Codec
  for_ rollupIds $ \rid -> do
    (results, errors, snap, _) <- readChildResultsRaw schemaName tableName rid
    let merged = mergeRawChildResults results errors snap
    when (not $ Map.null merged) $
      void $
        persistParentState schemaName tableName rid (toJSON merged)

-- | Moves multiple jobs from the main queue to the dead-letter queue.
--
-- Each job is moved with its own error message. Jobs that have already been
-- claimed by another worker (attempts mismatch) are silently skipped.
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
  for_ jobsWithErrors $ \(job, _) ->
    when (isRollup job) $ do
      snapshotDescendantRollups schemaName tableName (primaryKey job)
      void $ cascadeChildrenToDLQ schemaName tableName (primaryKey job) "Parent moved to DLQ"
  let ids = map (primaryKey . fst) jobsWithErrors
      atts = map (attempts . fst) jobsWithErrors
      msgs = map snd jobsWithErrors
  rows <-
    queryCount
      (Tmpl.moveToDLQBatchSQL schemaName tableName)
      [ parr CInt8 ids
      , parr CInt4 atts
      , parr CText msgs
      ]
  when (rows > 0) $ do
    let parentIds = Set.toAscList . Set.fromList $ mapMaybe (parentId . fst) jobsWithErrors
    for_ parentIds $ tryResumeParent schemaName tableName
  pure rows

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
  rawJobs <-
    executeQuery
      (Tmpl.retryFromDLQSQL schemaName tableName)
      [pval CInt8 dlqId]
      (jobRowCodec tableName)
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
  rows <-
    executeQuery
      (Tmpl.dlqJobExistsSQL schemaName tableName)
      [pval CInt8 dlqId]
      boolCodec
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
      let (whereClause, orderBy, params) = listQueryParts filters mSortBy mSortDir limit offset
          sql = Tmpl.listJobsFilteredSQL schemaName tableName whereClause orderBy
      rawJobs <- executeQuery sql params (jobRowCodec tableName)
      traverse decodePayload rawJobs

isStatusFilter :: Tmpl.JobFilter -> Bool
isStatusFilter (Tmpl.FilterStatus _) = True
isStatusFilter _ = False

-- | Decode a job row plus its derived @status@ trailing column.
jobRowWithStatusCodec :: TableName -> RowCodec (JobRead Value, JobStatus)
jobRowWithStatusCodec tableName =
  (,) <$> jobRowCodec tableName <*> (jobStatusFromText <$> col "status" CText)

-- | Shared WHERE clause, ORDER BY, and limit+offset params for job listings.
listQueryParts
  :: [Tmpl.JobFilter] -> Maybe Tmpl.JobSortColumn -> Maybe Tmpl.SortDir -> Int -> Int -> (Text, Text, Params)
listQueryParts filters mSortBy mSortDir limit offset =
  let (whereClause, filterParams) = buildWhereClause filters
      orderBy = Tmpl.buildJobsOrderBy mSortBy mSortDir
      params = filterParams <> [pval CInt8 (fromIntegral limit), pval CInt8 (fromIntegral offset)]
   in (whereClause, orderBy, params)

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
  let (whereClause, orderBy, params) = listQueryParts filters mSortBy mSortDir limit offset
      sql = Tmpl.listJobsWithStatusSQL schemaName tableName whereClause orderBy
  rows <- executeQuery sql params (jobRowWithStatusCodec tableName)
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
  let (whereClause, filterParams) = buildWhereClause filters
      sql = Tmpl.countJobsFilteredSQL schemaName tableName whereClause
  queryCountStrict "countJobsFiltered" sql filterParams

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
  let (whereClause, filterParams) = buildWhereClause filters
      orderBy = Tmpl.buildDLQOrderBy mSortBy mSortDir
      sql = Tmpl.listDLQFilteredSQL schemaName tableName whereClause orderBy
      params = filterParams <> [pval CInt8 (fromIntegral limit), pval CInt8 (fromIntegral offset)]
  rawRows <- executeQuery sql params (dlqRowCodec tableName)
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
countDLQFiltered schemaName tableName filters = do
  let (whereClause, filterParams) = buildWhereClause filters
      sql = Tmpl.countDLQFilteredSQL schemaName tableName whereClause
  queryCountStrict "countDLQFiltered" sql filterParams

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
  let (whereClause, filterParams) = buildWhereClause filters
      orderBy = Tmpl.buildArchiveOrderBy mSortBy mSortDir
      sql = Tmpl.listArchiveFilteredSQL schemaName tableName whereClause orderBy
      params = filterParams <> [pval CInt8 (fromIntegral limit), pval CInt8 (fromIntegral offset)]
  rawRows <- executeQuery sql params (archiveRowCodec tableName)
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
  executeStatement (Tmpl.deleteArchiveJobSQL schemaName tableName) [pval CInt8 archiveId]

-- | Delete archived jobs by archive primary key. Returns rows deleted.
deleteArchiveJobsBatch :: (MonadArbiter m) => SchemaName -> TableName -> [Int64] -> m Int64
deleteArchiveJobsBatch _ _ [] = pure 0
deleteArchiveJobsBatch schemaName tableName archiveIds =
  executeStatement (Tmpl.deleteArchiveJobsBatchSQL schemaName tableName) [parr CInt8 archiveIds]

-- | Re-enqueue an archived job as a fresh standalone job, keeping the archive
-- row. Returns the new job, or @Nothing@ if the archive row no longer exists.
reEnqueueFromArchive
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => SchemaName -> TableName -> Int64 -> m (Maybe (JobRead payload))
reEnqueueFromArchive schemaName tableName archiveId = withDbTransaction $ do
  rawJobs <-
    executeQuery
      (Tmpl.reEnqueueFromArchiveSQL schemaName tableName)
      [pval CInt8 archiveId]
      (jobRowCodec tableName)
  traverse decodePayload (listToMaybe rawJobs)

-- | Store a completed root job's result on its archive row. No-ops when the job
-- was not archived. Returns rows updated.
updateArchiveResult
  :: (MonadArbiter m) => SchemaName -> TableName -> Int64 -> Value -> m Int64
updateArchiveResult schemaName tableName jobId result =
  executeStatement
    (Tmpl.updateArchiveResultSQL schemaName tableName)
    [pval CJsonb result, pval CInt8 jobId]

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
countArchiveFiltered schemaName tableName filters = do
  let (whereClause, filterParams) = buildWhereClause filters
      sql = Tmpl.countArchiveFilteredSQL schemaName tableName whereClause
  queryCountStrict "countArchiveFiltered" sql filterParams

decodeArchiveRow
  :: (JobPayload payload, MonadArbiter m)
  => (Int64, UTCTime, JobRead Value, Maybe Value)
  -> m (Archive.ArchiveJob payload)
decodeArchiveRow (aId, aCompletedAt, rawJob, aResult) = do
  jobSnapshot <- decodePayload rawJob
  pure $
    Archive.ArchiveJob
      { Archive.archivePrimaryKey = aId
      , Archive.completedAt = aCompletedAt
      , Archive.jobSnapshot = jobSnapshot
      , Archive.archivedResult = aResult
      }

-- | Purge expired archived jobs (per-row @archive_expires_at@) across all queues.
-- Returns the total rows purged and the queues whose purge errored. Designed to
-- run once per reaper tick, so steady-state each call deletes only a small slice.
purgeArchives
  :: (MonadArbiter m, MonadUnliftIO m)
  => SchemaName -> [TableName] -> m (Int64, [Text])
purgeArchives =
  sweepQueues $ \schemaName queue ->
    withDbTransaction (executeStatement (Tmpl.purgeArchiveSQL schemaName queue) [])

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
  rows <-
    executeQuery
      (Tmpl.deleteDLQJobSQL schemaName tableName)
      [pval CInt8 dlqId]
      nullableInt64Codec
  case rows of
    [] -> pure 0
    (Just pid : _) -> do
      tryResumeParent schemaName tableName pid
      pure 1
    _ -> pure 1

-- | Delete jobs by id via the given SQL, then resume any parents left childless.
-- The SQL must return each deleted row's parent_id. Returns the rows deleted.
deleteJobsResumingParents
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> Text
  -> [Int64]
  -> m Int
deleteJobsResumingParents _ _ _ [] = pure 0
deleteJobsResumingParents schemaName tableName sql jobIds = withDbTransaction $ do
  rows <- executeQuery sql [parr CInt8 jobIds] nullableInt64Codec
  let parentIds = Set.toAscList . Set.fromList $ catMaybes rows
  for_ parentIds $ tryResumeParent schemaName tableName
  pure (length rows)

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
  fromIntegral
    <$> deleteJobsResumingParents schemaName tableName (Tmpl.deleteDLQJobsBatchSQL schemaName tableName) dlqIds

-- | Delete force-cancel-flagged jobs by id and resume any parents left
-- childless. Returns the number of rows actually deleted.
deleteCancelledJobs
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> [Int64]
  -> m Int
deleteCancelledJobs schemaName tableName jobIds =
  deleteJobsResumingParents schemaName tableName (Tmpl.deleteCancelledJobsSQL schemaName tableName) jobIds

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
  or <$> executeQuery (Tmpl.jobExistsSQL schemaName tableName) [pval CInt8 jobId] boolCodec

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
  rawJobs <-
    executeQuery
      (Tmpl.getJobByIdSQL schemaName tableName)
      [pval CInt8 jobId]
      (jobRowCodec tableName)
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
    executeQuery
      (Tmpl.getJobByIdWithStatusSQL schemaName tableName)
      [pval CInt8 jobId]
      (jobRowWithStatusCodec tableName)
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
  rawJobs <-
    executeQuery
      (Tmpl.getJobByDedupKeySQL schemaName tableName)
      [pval CText key]
      (jobRowCodec tableName)
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
  parentRows <-
    executeQuery
      (Tmpl.getParentIdSQL schemaName tableName)
      [pval CInt8 jobId]
      nullableInt64Codec
  let mParentId = case parentRows of
        [Just pid] -> Just pid
        _ -> Nothing
  for_ mParentId $ \pid ->
    void $
      executeQuery
        "SELECT pg_advisory_xact_lock(hashtextextended(?, ?))::text AS result"
        [pval CText (schemaName <> "." <> tableName), pval CInt8 pid]
        (ncol "result" CText)
  rows <-
    executeQuery
      (Tmpl.cancelJobSQL schemaName tableName)
      [pval CInt8 jobId, pval CInt8 jobId]
      int64Codec
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
  executeStatement
    (Tmpl.promoteJobSQL schemaName tableName)
    [pval CInt8 jobId]

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
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

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
    executeQuery
      (Tmpl.getQueueStatsSQL schemaName tableName)
      []
      statsRowCodec
  -- The aggregate query always returns exactly one row. The empty fallback
  -- only guards against an unexpected truncation.
  pure $ case rows of
    (s : _) -> s
    [] -> QueueStats 0 0 0 0 0 0 0 0 Nothing

-- | A landing-overview row: a queue's stats plus its pause state.
data QueueOverview = QueueOverview
  { overviewQueue :: Text
  , overviewStats :: QueueStats
  , overviewQueuePaused :: Bool
  , overviewWorkersLive :: Int64
  , overviewWorkersPaused :: Int64
  }
  deriving stock (Eq, Generic, Show)

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
  executeQuery (Tmpl.allQueueStatsSQL schemaName tableNames) [] allStatsRowCodec

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
    executeQuery
      (Tmpl.countChildrenBatchSQL schemaName tableName)
      [parr CInt8 ids]
      parentCountCodec
  pure $ Map.fromList rows

-- | Count children in the DLQ for a batch of potential parent IDs.
--
-- Returns a Map from parent_id to DLQ child count (only non-zero entries).
countDLQChildrenBatch
  :: (MonadArbiter m)
  => SchemaName -> TableName -> [Int64] -> m (Map.Map Int64 Int64)
countDLQChildrenBatch _ _ [] = pure Map.empty
countDLQChildrenBatch schemaName tableName ids = do
  rows <-
    executeQuery
      (Tmpl.countDLQChildrenBatchSQL schemaName tableName)
      [parr CInt8 ids]
      dlqParentCountCodec
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
  executeStatement
    (Tmpl.pauseChildrenSQL schemaName tableName)
    [pval CInt8 parentJobId]

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
  executeStatement
    (Tmpl.resumeChildrenSQL schemaName tableName)
    [pval CInt8 parentJobId]

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
  => (SchemaName -> TableName -> Text)
  -- ^ Cascade-delete SQL template (param: job_id, returns deleted count).
  -> SchemaName
  -> TableName
  -> Int64
  -> m Int64
cascadeDeleteJob mkSql schemaName tableName jobId = withDbTransaction $ do
  parentRows <-
    executeQuery
      (Tmpl.getParentIdSQL schemaName tableName)
      [pval CInt8 jobId]
      nullableInt64Codec
  let rootParentId = case parentRows of
        [Just pid] -> Just pid
        _ -> Nothing

  deleted <- queryCount (mkSql schemaName tableName) [pval CInt8 jobId]

  when (deleted > 0) $
    for_ rootParentId $
      tryResumeParent schemaName tableName

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
  queryCountStrict
    "cancelJobTree"
    (Tmpl.cancelJobTreeSQL schemaName tableName)
    [pval CInt8 jobId]

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
  executeStatement
    (Tmpl.suspendJobSQL schemaName tableName)
    [pval CInt8 jobId]

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
  executeStatement
    (Tmpl.resumeJobSQL schemaName tableName)
    [pval CInt8 jobId]

-- | Full recompute of the groups table from the main queue.
--
-- Locks the groups rows currently free (FOR UPDATE SKIP LOCKED) to avoid
-- fighting with live job claims, then rewrites the table. The caller is
-- responsible for any cross-pool coordination (see 'runGated' and
-- 'refreshAllGroups').
refreshGroupsForQueue
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> m ()
refreshGroupsForQueue schemaName tableName = withDbTransaction $ do
  keys <- executeQuery (Tmpl.lockGroupsSQL schemaName tableName) [] (col "group_key" CText)
  void $ executeStatement (Tmpl.refreshGroupsSQL schemaName tableName) [parr CText keys]

-- | Schema-wide groups-table refresh. Refreshes each given queue in its own
-- savepoint, so one queue's failure is isolated and the rest still commit.
-- Wrap in 'runGated' so only one pool runs it per interval. Returns the queue
-- names that failed.
refreshAllGroups
  :: (MonadArbiter m, MonadUnliftIO m)
  => SchemaName
  -> [TableName]
  -> m [Text]
refreshAllGroups schemaName queues = do
  outcomes <- traverse refreshOne queues
  pure (catMaybes outcomes)
  where
    refreshOne queue = do
      result <- tryAny (refreshGroupsForQueue schemaName queue)
      pure (either (const (Just queue)) (const Nothing) result)

-- | Run a per-queue sweep over every queue, returning the total and the names
-- of queues whose sweep threw.
sweepQueues
  :: (MonadUnliftIO m)
  => (SchemaName -> TableName -> m Int64)
  -> SchemaName
  -> [TableName]
  -> m (Int64, [Text])
sweepQueues sweepOne schemaName queues = do
  (failures, counts) <- partitionEithers <$> traverse run queues
  pure (sum counts, failures)
  where
    run queue = first (const queue) <$> tryAny (sweepOne schemaName queue)

-- | Sweep exhausted jobs across all queues. Returns the total moved and the
-- names of queues whose sweep failed.
sweepExhaustedJobs
  :: (MonadArbiter m, MonadUnliftIO m)
  => SchemaName
  -> [TableName]
  -> m (Int64, [Text])
sweepExhaustedJobs = sweepQueues sweepExhaustedForQueue

-- | Move each exhausted job to the DLQ via the tree-aware 'moveToDLQFields',
-- one transaction per job, so cascades and parent resumes are handled.
sweepExhaustedForQueue
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> m Int64
sweepExhaustedForQueue schemaName tableName = do
  exhausted <- executeQuery (Tmpl.selectExhaustedJobsSQL schemaName tableName exhaustedSweepBatch) [] exhaustedJobCodec
  getSum <$> getAp (foldMap moveOne exhausted)
  where
    moveOne (jobId, atts, mParentId, rollup) =
      Ap $ Sum <$> moveToDLQFields schemaName tableName "max attempts exceeded (reaper sweep)" jobId atts mParentId rollup

-- | Per-queue cap on jobs swept to the DLQ in one reaper pass, so a large
-- backlog drains over several intervals instead of one unbounded fetch.
exhaustedSweepBatch :: Int
exhaustedSweepBatch = 1000

exhaustedJobCodec :: RowCodec (Int64, Int32, Maybe Int64, Bool)
exhaustedJobCodec =
  (,,,)
    <$> col "id" CInt8
    <*> col "attempts" CInt4
    <*> ncol "parent_id" CInt8
    <*> col "is_rollup" CBool

-- | Sweep force-cancel-flagged jobs whose lease has lapsed across all queues.
-- A live worker's heartbeat keeps its jobs' lease in the future, so those are
-- left for the worker's own cancel handler. Returns the total deleted and the
-- names of queues whose sweep failed.
sweepCancelledJobs
  :: (MonadArbiter m, MonadUnliftIO m)
  => SchemaName
  -> [TableName]
  -> m (Int64, [Text])
sweepCancelledJobs = sweepQueues sweepCancelledForQueue

-- | Delete one queue's lease-lapsed flagged jobs, resuming any parents left
-- childless. 'deleteCancelledJobs' runs in its own transaction and re-checks
-- the flag, so a concurrent worker cancel handler is harmless.
sweepCancelledForQueue
  :: (MonadArbiter m)
  => SchemaName
  -> TableName
  -> m Int64
sweepCancelledForQueue schemaName tableName = do
  ids <-
    executeQuery
      (Tmpl.selectCancelledReapableJobsSQL schemaName tableName cancelledSweepBatch)
      []
      (col "id" CInt8)
  fromIntegral <$> deleteCancelledJobs schemaName tableName ids

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
  executeStatement
    (Tmpl.upsertCronDefaultSQL schemaName)
    [ pval CText scheduleName
    , pval CText queueName
    , pval CText defaultExpr
    , pval CText defaultOv
    , pnul CText defaultTz
    ]

-- | List cron schedules ordered by name, optionally filtered by queue.
listCronSchedules
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Maybe Text
  -- ^ Queue filter. 'Nothing' returns schedules for all queues.
  -> m [CronScheduleRow]
listCronSchedules schemaName mQueue =
  executeQuery
    (Tmpl.listCronSchedulesSQL schemaName)
    [pnul CText mQueue, pnul CText mQueue]
    cronScheduleRowCodec

-- | Get a single cron schedule by name.
getCronScheduleByName
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Text
  -- ^ Schedule name
  -> m (Maybe CronScheduleRow)
getCronScheduleByName schemaName scheduleName = do
  rows <-
    executeQuery
      (Tmpl.getCronScheduleByNameSQL schemaName)
      [pval CText scheduleName]
      cronScheduleRowCodec
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
  let (clauses, params) =
        mconcat
          [ case mExpr of
              Nothing -> ([], [])
              Just Nothing -> (["override_expression = NULL"], [])
              Just (Just expr) -> (["override_expression = ?"], [pval CText expr])
          , case mOverlap of
              Nothing -> ([], [])
              Just Nothing -> (["override_overlap = NULL"], [])
              Just (Just ov) -> (["override_overlap = ?"], [pval CText ov])
          , case mTz of
              Nothing -> ([], [])
              Just Nothing -> (["override_timezone = NULL"], [])
              Just (Just tz) -> (["override_timezone = ?"], [pval CText tz])
          , case mEnabled of
              Nothing -> ([], [])
              Just True -> (["enabled = TRUE"], [])
              Just False -> (["enabled = FALSE", "run_requested_at = NULL"], [])
          ]
  if null clauses
    then pure 0
    else do
      let setSQL = T.intercalate ", " clauses <> ", updated_at = NOW()"
          sql =
            "UPDATE "
              <> CS.cronSchedulesTable schemaName
              <> " SET "
              <> setSQL
              <> " WHERE name = ?"
      executeStatement sql (params <> [pval CText scheduleName])

-- | Update @last_fired_at@ to NOW() for a cron schedule.
touchCronLastFired
  :: (MonadArbiter m)
  => SchemaName
  -- ^ Schema name
  -> Text
  -- ^ Schedule name
  -> m Int64
touchCronLastFired schemaName scheduleName =
  executeStatement
    (Tmpl.touchCronLastFiredSQL schemaName)
    [pval CText scheduleName]

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
  executeStatement
    (Tmpl.touchCronCheckedSQL schemaName)
    [pval CTimestamptz watermark, parr CText names]

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
  rows <-
    executeStatement
      (Tmpl.tryFireCronGateSQL schemaName)
      [pval CTimestamptz minuteFloor, pval CText scheduleName, pval CTimestamptz minuteFloor]
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
  rows <-
    executeQuery
      Tmpl.tryAcquireCronLeaderSQL
      [pval CText schemaName, pval CText queueName, pval CText scheduleName]
      boolCodec
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
  rows <-
    executeQuery
      (Tmpl.requestCronRunSQL schemaName)
      [pval CText scheduleName, pval CText scheduleName]
      statusCodec
  pure $ case listToMaybe rows of
    Just "stamped" -> RunReqStamped
    Just "pending" -> RunReqPending
    Just "disabled" -> RunReqDisabled
    _ -> RunReqNotFound
  where
    statusCodec :: RowCodec Text
    statusCodec = col "status" CText

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
  rows <-
    executeQuery
      (Tmpl.claimCronRunSQL schemaName)
      [pval CText scheduleName]
      cronScheduleRowCodec
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
  executeStatement
    (Tmpl.touchCronManualRunSQL schemaName)
    [pval CTimestamptz firedAt, pval CText scheduleName]

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
  executeQuery (Tmpl.pendingCronRunsSQL schemaName) [parr CText names] (col "name" CText)

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
    executeQuery
      (Tmpl.upsertWorkerSQL schemaName)
      [ pval CUuid workerId
      , pval CText queue
      , pnul CText host
      , pnul CInt4 threads
      , pval CFloat8 (realToFrac staleThreshold)
      , pnul CJsonb metadata
      ]
      (col "effective_paused" CBool)
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
  rows <-
    executeQuery
      (Tmpl.heartbeatWorkerSQL schemaName)
      [pval CUuid workerId]
      (col "effective_paused" CBool)
  pure $ listToMaybe rows

-- | Set the @paused@ flag for a registered worker.
setWorkerPaused
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -> Bool
  -> m Int64
setWorkerPaused schemaName workerId p =
  queryCount
    (Tmpl.setWorkerPausedSQL schemaName)
    [pval CBool p, pval CUuid workerId]

-- | Mark a worker as gracefully draining. The row is left in place so the UI
-- can distinguish drained workers from ones that vanished.
markWorkerShuttingDown
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -> m Int64
markWorkerShuttingDown schemaName workerId =
  executeStatement
    (Tmpl.markWorkerShuttingDownSQL schemaName)
    [pval CUuid workerId]

-- | Remove a worker row outright.
deregisterWorker
  :: (MonadArbiter m)
  => SchemaName
  -> UUID
  -> m Int64
deregisterWorker schemaName workerId =
  executeStatement
    (Tmpl.deleteWorkerSQL schemaName)
    [pval CUuid workerId]

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
  executeQuery
    (Tmpl.listWorkersSQL schemaName)
    [pnul CText mQueue, pnul CFloat8 (realToFrac <$> mLiveSecs)]
    workerRowCodec

-- | Delete worker rows (including paused ones) whose @last_heartbeat@ is older
-- than each row's own @stale_threshold_secs@.
sweepStaleWorkers
  :: (MonadArbiter m)
  => SchemaName
  -> m Int64
sweepStaleWorkers schemaName =
  executeStatement (Tmpl.deleteStaleWorkersSQL schemaName) []

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
  executeStatement
    (Tmpl.ensureQueueSQL schemaName)
    [pval CText queue]

-- | Set the queue's @paused@ flag, creating the row if missing.
setQueuePaused
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -- ^ Queue name
  -> Bool
  -> m Int64
setQueuePaused schemaName queue p =
  queryCount
    (Tmpl.setQueuePausedSQL schemaName)
    [pval CText queue, pval CBool p, pval CBool p]

-- | Get the arbiter_queues row for a single queue. 'Nothing' if absent.
getQueue
  :: (MonadArbiter m)
  => SchemaName
  -> Text
  -- ^ Queue name
  -> m (Maybe QueueRow)
getQueue schemaName queue = do
  rows <-
    executeQuery
      (Tmpl.getQueueSQL schemaName)
      [pval CText queue]
      queueRowCodec
  pure $ listToMaybe rows

-- | List all arbiter_queues rows, ordered by queue name.
listQueues
  :: (MonadArbiter m)
  => SchemaName
  -> m [QueueRow]
listQueues schemaName =
  executeQuery (Tmpl.listQueuesSQL schemaName) [] queueRowCodec

-- ---------------------------------------------------------------------------
-- Global Gate Operations
-- ---------------------------------------------------------------------------

-- | Bound the current transaction's statements to a wall-clock limit, so a stuck op aborts at the DB rather than hanging the caller.
setLocalStatementTimeout :: (MonadArbiter m) => NominalDiffTime -> m ()
setLocalStatementTimeout limit =
  let ms = ceiling (realToFrac limit * 1000 :: Double) :: Int
   in void $
        executeQuery
          ("SELECT set_config('statement_timeout', '" <> T.pack (show ms) <> "', true)")
          []
          (col "set_config" CText)

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
runGated schemaName task interval work = do
  _ <-
    executeStatement
      (Tmpl.ensureGateRowSQL schemaName)
      [pval CText task]
  gateOpen <- checkGateOuter
  if not gateOpen
    then pure Nothing
    else withDbTransaction $ do
      claimed <- tryClaimGate
      if not claimed
        then pure Nothing
        else do
          r <- work
          _ <-
            executeStatement
              (Tmpl.bumpGateSQL schemaName)
              [pval CText task]
          pure (Just r)
  where
    intervalSecs = realToFrac interval :: Double

    checkGateOuter = do
      rows <-
        executeQuery
          (Tmpl.checkGateSQL schemaName)
          [pval CFloat8 intervalSecs, pval CText task]
          boolCodec
      pure $ fromMaybe True (listToMaybe rows)

    tryClaimGate = do
      rows <-
        executeQuery
          (Tmpl.tryClaimGateSQL schemaName)
          [pval CText task, pval CFloat8 intervalSecs]
          int64Codec
      pure $ not (null rows)

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
  rows <-
    executeQuery
      (Tmpl.readChildResultsSQL schemaName tableName)
      [pval CInt8 parentJobId, pval CInt8 parentJobId, pval CInt8 parentJobId]
      childResultsRowCodec
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
  rows <-
    executeQuery
      (Tmpl.getParentStateSnapshotSQL schemaName tableName)
      [pval CInt8 jobId]
      (ncol "parent_state" CJsonb)
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

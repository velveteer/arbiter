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
  , claimNextVisibleJobsBatched
  , ackJob
  , ackJobsBatch
  , ackJobsBulk
  , setVisibilityTimeout
  , setVisibilityTimeoutBatch
  , VisibilityUpdateInfo (..)
  , updateJobForRetry
  , moveToDLQ
  , moveToDLQBatch
  , retryFromDLQ
  , dlqJobExists
  , listDLQJobs
  , listDLQJobsByParent
  , countDLQJobsByParent
  , deleteDLQJob
  , deleteDLQJobsBatch

    -- * Filtered Query Operations
  , Tmpl.JobFilter (..)
  , listJobsFiltered
  , countJobsFiltered
  , listDLQFiltered
  , countDLQFiltered

    -- * Admin Operations
  , listJobs
  , getJobById
  , getJobsByGroup
  , getInFlightJobs
  , cancelJob
  , cancelJobsBatch
  , promoteJob
  , QueueStats (..)
  , getQueueStats

    -- * Count Operations
  , countJobs
  , countJobsByGroup
  , countInFlightJobs
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

    -- * Suspend/Resume Operations
  , suspendJob
  , resumeJob

    -- * Groups Table Operations
  , refreshGroups

    -- * Internal Operations
  , getParentStateSnapshot
  , readChildResultsRaw
  , mergeRawChildResults
  ) where

import Control.Monad (foldM, void, when)
import Data.Aeson (FromJSON, Result (..), ToJSON, Value (Object), fromJSON, toJSON)
import Data.Foldable (for_, toList)
import Data.Functor qualified as Functor
import Data.Int (Int32, Int64)
import Data.List (groupBy, sortOn)
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Map.Strict qualified as Map
import Data.Maybe (catMaybes, mapMaybe)
import Data.Sequence (Seq, (|>))
import Data.Sequence qualified as Seq
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime, UTCTime)
import GHC.Generics (Generic, Generically (..))

import Arbiter.Core.Codec
  ( Col (..)
  , Params
  , RowCodec
  , col
  , countCodec
  , dlqRowCodec
  , jobRowCodec
  , ncol
  , parr
  , pnarr
  , pnul
  , pval
  , statsRowCodec
  )
import Arbiter.Core.Exceptions (throwParsing)
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Schema (jobQueueTable)
import Arbiter.Core.Job.Types
  ( DedupKey (IgnoreDuplicate, ReplaceDuplicate)
  , Job (..)
  , JobPayload
  , JobRead
  , JobWrite
  )
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.SqlTemplates qualified as Tmpl

decodePayload :: (JobPayload payload, MonadArbiter m) => Job Value Int64 Text UTCTime -> m (JobRead payload)
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
filterToClause (Tmpl.FilterSuspended b) = ("suspended = ?", [pval CBool b])
filterToClause Tmpl.FilterInFlight =
  ("attempts > 0 AND NOT suspended AND not_visible_until IS NOT NULL AND not_visible_until > NOW()", [])

-- | Insert a job without validating that the parent exists.
--
-- This is an internal fast path for callers that already guarantee the parent
-- is present (e.g. 'insertJobTree'). External callers
-- should use 'insertJob' which validates the parent first.
insertJobUnsafe
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> JobWrite payload
  -> m (Maybe (JobRead payload))
insertJobUnsafe schemaName tableName job = withDbTransaction $ do
  let (sql, dedupKeyParam, dedupStrategyParam) = case dedupKey job of
        Nothing -> (Tmpl.insertJobSQL schemaName tableName, pnul CText Nothing, pnul CText Nothing)
        Just (IgnoreDuplicate k) -> (Tmpl.insertJobSQL schemaName tableName, pnul CText (Just k), pnul CText (Just "ignore"))
        Just (ReplaceDuplicate k) -> (Tmpl.insertJobReplaceSQL schemaName tableName, pnul CText (Just k), pnul CText (Just "replace"))

      parentStateVal = if isRollup job then Just (Object mempty) else Nothing

      params =
        [ pval CJsonb (toJSON $ payload job)
        , pnul CText (groupKey job)
        , pval CInt4 (attempts job)
        , pnul CText (lastError job)
        , pval CInt4 (priority job)
        , dedupKeyParam
        , dedupStrategyParam
        , pnul CInt4 (maxAttempts job)
        , pnul CInt8 (parentId job)
        , pnul CJsonb parentStateVal
        , pval CBool (suspended job)
        , pnul CTimestamptz (notVisibleUntil job)
        ]

  rawJobs <- executeQuery sql params (jobRowCodec tableName)
  case rawJobs of
    [] -> case dedupKey job of
      Just (IgnoreDuplicate _) -> pure Nothing
      Just (ReplaceDuplicate _) -> pure Nothing
      Nothing -> throwParsing "insertJob: No rows returned from INSERT"
    (raw : _) -> Just <$> decodePayload raw

-- | Inserts a job into the queue.
--
-- Returns the inserted job with database-generated fields populated.
--
-- __Ordering and concurrency__
--
-- Jobs are claimed in ID order (lowest ID first within a priority level).
-- Concurrent inserts to the same group are serialized at the trigger level:
-- the AFTER INSERT trigger's @ON CONFLICT DO UPDATE@ on the groups table
-- takes a row-level lock, preventing out-of-order commits within a group.
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
--   in-flight on a retry attempt — this is by design, so that a fresh
--   replacement takes priority over a failing job.
--
-- @parentId@ is validated: if set to a non-existent job ID, returns @Nothing@.
-- For building parent-child trees, prefer @insertJobTree@ which handles
-- @parentId@, @isRollup@, and @suspended@ atomically.
insertJob
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> JobWrite payload
  -> m (Maybe (JobRead payload))
insertJob schemaName tableName job = do
  parentOk <- case parentId job of
    Nothing -> pure True
    Just pid -> do
      checkRows <- executeQuery (Tmpl.parentExistsSQL schemaName tableName) [pval CInt8 pid] boolCodec
      case checkRows of
        [True] -> pure True
        _ -> pure False
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
-- Does not validate @parentId@ — callers must ensure referenced parents
-- exist. For parent-child trees, use @insertJobTree@ instead.
insertJobsBatch
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> [JobWrite payload]
  -- ^ Jobs to insert
  -> m [JobRead payload]
insertJobsBatch _ _ [] = pure []
insertJobsBatch schemaName tableName jobs = withDbTransaction $ do
  let cols = buildBatchColumns jobs
      params =
        [ parr CJsonb (toList $ colPayloads cols)
        , pnarr CText (toList $ colGroupKeys cols)
        , parr CInt4 (toList $ colPriorities cols)
        , pnarr CText (toList $ colDedupKeys cols)
        , pnarr CText (toList $ colDedupStrategies cols)
        , pnarr CInt4 (toList $ colMaxAttempts cols)
        , pnarr CInt8 (toList $ colParentIds cols)
        , pnarr CJsonb (toList $ colParentStates cols)
        , parr CBool (toList $ colSuspended cols)
        , pnarr CTimestamptz (toList $ colNotVisibleUntils cols)
        ]

  rawJobs <- executeQuery (Tmpl.insertJobsBatchSQL schemaName tableName) params (jobRowCodec tableName)
  mapM decodePayload rawJobs

insertJobsBatch_
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -> Text
  -> [JobWrite payload]
  -> m Int64
insertJobsBatch_ _ _ [] = pure 0
insertJobsBatch_ schemaName tableName jobs = withDbTransaction $ do
  let cols = buildBatchColumns jobs
      params =
        [ parr CJsonb (toList $ colPayloads cols)
        , pnarr CText (toList $ colGroupKeys cols)
        , parr CInt4 (toList $ colPriorities cols)
        , pnarr CText (toList $ colDedupKeys cols)
        , pnarr CText (toList $ colDedupStrategies cols)
        , pnarr CInt4 (toList $ colMaxAttempts cols)
        , pnarr CInt8 (toList $ colParentIds cols)
        , pnarr CJsonb (toList $ colParentStates cols)
        , parr CBool (toList $ colSuspended cols)
        , pnarr CTimestamptz (toList $ colNotVisibleUntils cols)
        ]
  executeStatement (Tmpl.insertJobsBatchSQL_ schemaName tableName) params

-- | Insert a child's result into the results table.
--
-- Each child gets its own row keyed by @(parent_id, child_id)@.
-- The FK @ON DELETE CASCADE@ ensures cleanup when the parent is acked.
--
-- Returns the number of rows inserted (1 on success).
insertResult
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
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
  => Text
  -- ^ PostgreSQL schema name
  -> Text
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
  => Text
  -- ^ PostgreSQL schema name
  -> Text
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
  => Text
  -- ^ PostgreSQL schema name
  -> Text
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

-- | Build batch columns with within-batch dedup, preserving input order.
--
-- A single fold over the input list. Non-keyed jobs are appended; keyed
-- jobs occupy the slot of their first occurrence, with O(log n) positional
-- updates via 'Seq' when a later 'ReplaceDuplicate' overwrites an earlier
-- entry for the same key.
--
-- Dedup semantics (matching sequential 'insertJob' behaviour):
--
--   * All 'IgnoreDuplicate' for a key -> first occurrence wins
--   * All 'ReplaceDuplicate' for a key -> last occurrence wins
--   * Mixed strategies for the same key -> 'ReplaceDuplicate' takes precedence
buildBatchColumns :: forall payload. (ToJSON payload) => [JobWrite payload] -> BatchColumns
buildBatchColumns = extractCols . foldl' step (Map.empty, 0, emptyColumns)
  where
    extractCols (_, _, cols) = cols

    step (!seen, !n, !cols) job = case dedupKeyText (dedupKey job) of
      Nothing -> (seen, n + 1, snocJob cols job)
      Just k -> case Map.lookup k seen of
        Nothing -> (Map.insert k n seen, n + 1, snocJob cols job)
        Just idx
          | isReplace job -> (seen, n, updateJob idx cols job)
          | otherwise -> (seen, n, cols)

    isReplace job = case dedupKey job of
      Just (ReplaceDuplicate _) -> True
      _ -> False

    snocJob = withJob (flip (|>))
    updateJob idx = withJob (Seq.update idx)

    withJob :: (forall a. a -> Seq a -> Seq a) -> BatchColumns -> JobWrite payload -> BatchColumns
    withJob f cols job =
      let (dk, ds) = dedupParts (dedupKey job)
       in BatchColumns
            { colPayloads = f (toJSON (payload job)) (colPayloads cols)
            , colGroupKeys = f (groupKey job) (colGroupKeys cols)
            , colPriorities = f (priority job) (colPriorities cols)
            , colDedupKeys = f dk (colDedupKeys cols)
            , colDedupStrategies = f ds (colDedupStrategies cols)
            , colMaxAttempts = f (maxAttempts job) (colMaxAttempts cols)
            , colParentIds = f (parentId job) (colParentIds cols)
            , colParentStates = f (if isRollup job then Just (Object mempty) else Nothing) (colParentStates cols)
            , colSuspended = f (suspended job) (colSuspended cols)
            , colNotVisibleUntils = f (notVisibleUntil job) (colNotVisibleUntils cols)
            }

dedupParts :: Maybe DedupKey -> (Maybe Text, Maybe Text)
dedupParts Nothing = (Nothing, Nothing)
dedupParts (Just (IgnoreDuplicate k)) = (Just k, Just "ignore")
dedupParts (Just (ReplaceDuplicate k)) = (Just k, Just "replace")

dedupKeyText :: Maybe DedupKey -> Maybe Text
dedupKeyText Nothing = Nothing
dedupKeyText (Just (IgnoreDuplicate k)) = Just k
dedupKeyText (Just (ReplaceDuplicate k)) = Just k

data BatchColumns = BatchColumns
  { colPayloads :: !(Seq Value)
  , colGroupKeys :: !(Seq (Maybe Text))
  , colPriorities :: !(Seq Int32)
  , colDedupKeys :: !(Seq (Maybe Text))
  , colDedupStrategies :: !(Seq (Maybe Text))
  , colMaxAttempts :: !(Seq (Maybe Int32))
  , colParentIds :: !(Seq (Maybe Int64))
  , colParentStates :: !(Seq (Maybe Value))
  , colSuspended :: !(Seq Bool)
  , colNotVisibleUntils :: !(Seq (Maybe UTCTime))
  }
  deriving stock (Generic)
  deriving (Monoid, Semigroup) via Generically BatchColumns

emptyColumns :: BatchColumns
emptyColumns = mempty

-- | Claim up to @maxJobs@ visible jobs, respecting head-of-line blocking
-- (one job per group). Uses a single-CTE claim with the groups table.
claimNextVisibleJobs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> Int
  -- ^ Maximum number of jobs to claim
  -> NominalDiffTime
  -- ^ Visibility timeout in seconds
  -> m [JobRead payload]
claimNextVisibleJobs schemaName tableName maxJobs timeout = withDbTransaction $ do
  let claimSql = Tmpl.claimJobsSQL schemaName tableName maxJobs (ceiling timeout)
  rawJobs <- executeQuery claimSql [] (jobRowCodec tableName)
  mapM decodePayload rawJobs

-- | Batched variant of 'claimNextVisibleJobs' — claims up to @batchSize@ jobs
-- per group, across up to @maxBatches@ groups.
claimNextVisibleJobsBatched
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> Int
  -- ^ Batch size per group (how many jobs to claim from each group)
  -> Int
  -- ^ Maximum number of groups/batches to claim
  -> NominalDiffTime
  -- ^ Visibility timeout in seconds
  -> m [NonEmpty (JobRead payload)]
claimNextVisibleJobsBatched schemaName tableName batchSize maxBatches timeout
  | batchSize < 1 = pure []
  | maxBatches < 1 = pure []
  | otherwise = withDbTransaction $ do
      let claimSql = Tmpl.claimJobsBatchedSQL schemaName tableName batchSize maxBatches (ceiling timeout)
      rawJobs <- executeQuery claimSql [] (jobRowCodec tableName)
      jobs <- mapM decodePayload rawJobs
      let sorted = sortOn groupKey jobs
          groups = groupBy (\j1 j2 -> groupKey j1 == groupKey j2) sorted
      pure $ concatMap (chunksOfNE batchSize) $ mapMaybe NE.nonEmpty groups

-- | Split a NonEmpty list into chunks of at most @n@ elements.
chunksOfNE :: Int -> NonEmpty a -> [NonEmpty a]
chunksOfNE n (x :| xs) = go (x : xs)
  where
    go [] = []
    go (y : ys) =
      let (chunk, rest) = splitAt (n - 1) ys
       in (y :| chunk) : go rest

-- | Acknowledge a job as completed (smart ack).
--
-- Deletes standalone jobs; suspends parents waiting for children; wakes
-- parents when the last sibling completes. Uses an advisory lock for
-- child jobs to serialize with concurrent sibling acks.
--
-- Returns 1 on success, 0 if the job was already gone.
ackJob
  :: forall m payload
   . (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> JobRead payload
  -> m Int64
ackJob schemaName tableName job = withDbTransaction $ ackJobInner schemaName tableName job

-- | Inner ack logic (must be called within an existing transaction).
ackJobInner
  :: forall m payload
   . (MonadArbiter m)
  => Text -> Text -> JobRead payload -> m Int64
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
  rows <- executeQuery (Tmpl.smartAckJobSQL schemaName tableName) params int64Codec
  case rows of
    [n] -> pure n
    _ -> pure 0

-- | Wake a suspended parent if all children are done.
tryResumeParent :: (MonadArbiter m) => Text -> Text -> Int64 -> m ()
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

-- | Acknowledge multiple jobs as completed.
--
-- Iterates calling 'ackJob' so every job gets smart-ack treatment
-- (parent suspend/wake logic).
-- Returns the total number of rows affected.
ackJobsBatch
  :: forall m payload
   . (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> [JobRead payload]
  -> m Int64
ackJobsBatch _ _ [] = pure 0
ackJobsBatch schemaName tableName jobs =
  withDbTransaction $ sum <$> mapM (ackJobInner schemaName tableName) jobs

-- | Bulk ack for standalone jobs (no parent, no tree logic).
--
-- A single DELETE with unnest — one round trip for N jobs.
-- Only valid for jobs claimed in 'BatchedJobsMode', which guarantees
-- @parent_id IS NULL AND parent_state IS NULL@.
--
-- Returns the number of rows deleted.
ackJobsBulk
  :: forall m payload
   . (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> [JobRead payload]
  -> m Int64
ackJobsBulk _ _ [] = pure 0
ackJobsBulk schemaName tableName jobs = do
  let ids = map primaryKey jobs
      atts = map attempts jobs
  rows <-
    executeQuery
      (Tmpl.ackJobsBulkSQL schemaName tableName)
      [ parr CInt8 ids
      , parr CInt4 atts
      ]
      countCodec
  case rows of
    [n] -> pure n
    _ -> throwParsing "ackJobsBulk: unexpected result"

-- | Set the visibility timeout for a job
setVisibilityTimeout
  :: forall m payload
   . (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> NominalDiffTime
  -- ^ Timeout in seconds
  -> JobRead payload
  -> m Int64
  -- ^ Returns the number of rows updated (0 if job was reclaimed by another worker)
setVisibilityTimeout schemaName tableName timeout job =
  executeStatement
    (Tmpl.setVisibilityTimeoutSQL schemaName tableName)
    [ pval CInt8 (ceiling timeout)
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
  }
  deriving stock (Eq, Generic, Show)

-- | Batch variant of 'setVisibilityTimeout'. Returns per-job status
-- (success, acked, or stolen).
setVisibilityTimeoutBatch
  :: forall m payload
   . (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> NominalDiffTime
  -- ^ Timeout in seconds
  -> [JobRead payload]
  -> m [VisibilityUpdateInfo]
  -- ^ Returns a list of status records, one for each job that was targeted.
setVisibilityTimeoutBatch _ _ _ [] = pure []
setVisibilityTimeoutBatch schemaName tableName timeout jobs = do
  let valuesPlaceholder = T.intercalate "," $ replicate (length jobs) "(?,?)"
      jobParams = concatMap (\job -> [pval CInt8 (primaryKey job), pval CInt4 (attempts job)]) jobs
      params = jobParams <> [pval CInt8 (ceiling timeout)]

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
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
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

-- | Move a job to the DLQ. Cascades descendants for rollup parents.
-- Wakes the parent if this was a child job.
--
-- Returns 0 if the job was already claimed by another worker.
moveToDLQ
  :: forall m payload
   . (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> Text
  -- ^ Error message (the final error that caused the DLQ move)
  -> JobRead payload
  -> m Int64
moveToDLQ schemaName tableName errorMsg job = withDbTransaction $ do
  when (isRollup job) $ do
    snapshotDescendantRollups schemaName tableName (primaryKey job)
    void $ cascadeChildrenToDLQ schemaName tableName (primaryKey job) "Parent moved to DLQ"
  countRows <-
    executeQuery
      (Tmpl.moveToDLQSQL schemaName tableName)
      [ pval CInt8 (primaryKey job)
      , pval CInt4 (attempts job)
      , pval CText errorMsg
      ]
      countCodec
  let rows = case countRows of
        [n] -> n
        _ -> 0
  when (rows > 0) $
    for_ (parentId job) $ \pid ->
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
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> Int64
  -- ^ Parent job ID
  -> Text
  -- ^ Error message for cascaded children
  -> m Int64
cascadeChildrenToDLQ schemaName tableName parentJobId errorMsg = do
  countRows <-
    executeQuery
      (Tmpl.cascadeChildrenToDLQSQL schemaName tableName)
      [pval CInt8 parentJobId, pval CText errorMsg]
      countCodec
  case countRows of
    [n] -> pure n
    _ -> throwParsing "cascadeChildrenToDLQ: unexpected result"

-- | Snapshot child results for descendant rollup finalizers before cascade-DLQ.
-- Persists accumulated results into @parent_state@ so they survive deletion.
snapshotDescendantRollups
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
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
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
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
  countRows <-
    executeQuery
      (Tmpl.moveToDLQBatchSQL schemaName tableName)
      [ parr CInt8 ids
      , parr CInt4 atts
      , parr CText msgs
      ]
      countCodec
  let rows = case countRows of
        [n] -> n
        _ -> 0
  when (rows > 0) $ do
    let parentIds = Set.toAscList . Set.fromList $ mapMaybe (parentId . fst) jobsWithErrors
    for_ parentIds $ tryResumeParent schemaName tableName
  pure rows

-- * Dead Letter Queue Operations

-- | Retry a job from the DLQ (re-inserts with attempts reset to 0).
-- The dedup_key is NOT restored — retried jobs won't conflict with new dedup inserts.
retryFromDLQ
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
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

-- | List jobs with composable filters.
--
-- Returns jobs ordered by ID (descending, newest first).
listJobsFiltered
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
listJobsFiltered schemaName tableName filters limit offset = do
  let (whereClause, filterParams) = buildWhereClause filters
      sql = Tmpl.listJobsFilteredSQL schemaName tableName whereClause
      params = filterParams <> [pval CInt8 (fromIntegral limit), pval CInt8 (fromIntegral offset)]
  rawJobs <- executeQuery sql params (jobRowCodec tableName)
  mapM decodePayload rawJobs

-- | Count jobs with composable filters.
countJobsFiltered
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> m Int64
countJobsFiltered schemaName tableName filters = do
  let (whereClause, filterParams) = buildWhereClause filters
      sql = Tmpl.countJobsFilteredSQL schemaName tableName whereClause
  rows <- executeQuery sql filterParams countCodec
  case rows of
    [n] -> pure n
    _ -> throwParsing "countJobsFiltered: unexpected result"

-- | List DLQ jobs with composable filters.
--
-- Returns jobs ordered by failed_at (most recent first).
listDLQFiltered
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [DLQ.DLQJob payload]
listDLQFiltered schemaName tableName filters limit offset = do
  let (whereClause, filterParams) = buildWhereClause filters
      sql = Tmpl.listDLQFilteredSQL schemaName tableName whereClause
      params = filterParams <> [pval CInt8 (fromIntegral limit), pval CInt8 (fromIntegral offset)]
  rawRows <- executeQuery sql params (dlqRowCodec tableName)
  mapM decodeDLQRow rawRows

-- | Count DLQ jobs with composable filters.
countDLQFiltered
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> [Tmpl.JobFilter]
  -- ^ Composable filters
  -> m Int64
countDLQFiltered schemaName tableName filters = do
  let (whereClause, filterParams) = buildWhereClause filters
      sql = Tmpl.countDLQFilteredSQL schemaName tableName whereClause
  rows <- executeQuery sql filterParams countCodec
  case rows of
    [n] -> pure n
    _ -> throwParsing "countDLQFiltered: unexpected result"

decodeDLQRow
  :: (JobPayload payload, MonadArbiter m)
  => (Int64, UTCTime, Job Value Int64 Text UTCTime)
  -> m (DLQ.DLQJob payload)
decodeDLQRow (dlqId, dlqFailedAt, rawJob) = do
  jobSnapshot <- decodePayload rawJob
  pure $
    DLQ.DLQJob
      { DLQ.dlqPrimaryKey = dlqId
      , DLQ.failedAt = dlqFailedAt
      , DLQ.jobSnapshot = jobSnapshot
      }

-- | List jobs in the dead letter queue
--
-- Returns jobs ordered by failed_at (most recent first).
listDLQJobs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
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
  => Text
  -- ^ PostgreSQL schema name
  -> Text
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
  => Text -> Text -> Int64 -> m Int64
countDLQJobsByParent schemaName tableName parentJobId =
  countDLQFiltered schemaName tableName [Tmpl.FilterParentId parentJobId]

-- | Delete a job from the dead letter queue.
--
-- This permanently removes the job from the DLQ without retrying it.
-- If the deleted job was a child, tries to resume the parent when no
-- siblings remain in the main queue.
deleteDLQJob
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
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

-- | Delete multiple jobs from the dead letter queue.
--
-- If any deleted jobs were children, tries to resume their parents when
-- no siblings remain. Parent IDs are deduplicated and sorted to prevent
-- deadlocks between concurrent batch deletes.
--
-- Returns the total number of DLQ jobs deleted.
deleteDLQJobsBatch
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> [Int64]
  -- ^ DLQ job IDs
  -> m Int64
deleteDLQJobsBatch _ _ [] = pure 0
deleteDLQJobsBatch schemaName tableName dlqIds = withDbTransaction $ do
  rows <-
    executeQuery
      (Tmpl.deleteDLQJobsBatchSQL schemaName tableName)
      [parr CInt8 dlqIds]
      nullableInt64Codec
  let parentIds = Set.toAscList . Set.fromList $ catMaybes rows
  for_ parentIds $ tryResumeParent schemaName tableName
  pure $ fromIntegral (length rows)

-- * Admin Operations

-- | List jobs in the queue with pagination.
--
-- Returns jobs ordered by ID (descending).
listJobs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
listJobs schemaName tableName = listJobsFiltered schemaName tableName []

-- | Get a single job by its ID
getJobById
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
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

-- | Get all jobs for a specific group key
--
-- Useful for debugging or admin UI to see all jobs for a specific entity.
getJobsByGroup
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> Text
  -- ^ Group key to filter by
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
getJobsByGroup schemaName tableName gk =
  listJobsFiltered schemaName tableName [Tmpl.FilterGroupKey gk]

-- | Get all in-flight jobs (currently being processed by workers)
--
-- A job is considered in-flight if it has been claimed (attempts > 0) and
-- its visibility timeout hasn't expired yet.
--
-- Useful for monitoring active work and detecting stuck jobs.
getInFlightJobs
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> Int
  -- ^ Limit
  -> Int
  -- ^ Offset
  -> m [JobRead payload]
getInFlightJobs schemaName tableName =
  listJobsFiltered schemaName tableName [Tmpl.FilterInFlight]

-- | Cancels (deletes) a job by ID.
--
-- Returns 0 if the job has children — use 'cancelJobCascade' to delete
-- a parent and all its descendants.
--
-- If the deleted job was a child and no siblings remain, the parent is
-- resumed for its completion round.
cancelJob
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> Text
  -- ^ Table name
  -> Int64
  -- ^ Job ID
  -> m Int64
cancelJob schemaName tableName jobId = withDbTransaction $ cancelJobInner schemaName tableName jobId

-- | Inner cancel logic (must be called within an existing transaction).
cancelJobInner
  :: (MonadArbiter m)
  => Text -> Text -> Int64 -> m Int64
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
-- same parent sees a consistent view — the last cancel's CTE correctly
-- detects no remaining siblings and resumes the parent.
-- Returns the total number of jobs deleted.
cancelJobsBatch
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> Text
  -- ^ Table name
  -> [Int64]
  -- ^ Job IDs
  -> m Int64
cancelJobsBatch _ _ [] = pure 0
cancelJobsBatch schemaName tableName jobIds =
  withDbTransaction $ sum <$> mapM (cancelJobInner schemaName tableName) jobIds

-- | Promote a delayed or retrying job to be immediately visible.
--
-- Refuses in-flight jobs (attempts > 0 with no last_error).
-- Returns 1 on success, 0 if not found, already visible, or in-flight.
promoteJob
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> Int64
  -- ^ Job ID
  -> m Int64
  -- ^ Number of rows updated
promoteJob schemaName tableName jobId =
  executeStatement
    (Tmpl.promoteJobSQL schemaName tableName)
    [pval CInt8 jobId]

-- | Statistics about the job queue
data QueueStats = QueueStats
  { totalJobs :: Int64
  -- ^ Total number of jobs in the queue
  , visibleJobs :: Int64
  -- ^ Number of jobs that are visible (can be claimed)
  , invisibleJobs :: Int64
  -- ^ Number of jobs that are invisible (claimed or delayed)
  , oldestJobAgeSeconds :: Maybe Double
  -- ^ Age in seconds of the oldest job (Nothing if queue is empty)
  }
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

-- | Get statistics about the job queue
getQueueStats
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name (e.g., "arbiter")
  -> Text
  -- ^ Table name (e.g., "email_jobs")
  -> m QueueStats
getQueueStats schemaName tableName = do
  rows <-
    executeQuery
      (Tmpl.getQueueStatsSQL schemaName tableName)
      []
      statsRowCodec
  case rows of
    [] -> pure $ QueueStats 0 0 0 Nothing
    ((total, visible, age) : _) -> pure $ QueueStats total visible (total - visible) age

-- ---------------------------------------------------------------------------
-- Count Operations
-- ---------------------------------------------------------------------------

-- | Count all jobs in a table
countJobs
  :: (MonadArbiter m)
  => Text -> Text -> m Int64
countJobs schemaName tableName = countJobsFiltered schemaName tableName []

-- | Count jobs matching a group key
countJobsByGroup
  :: (MonadArbiter m)
  => Text -> Text -> Text -> m Int64
countJobsByGroup schemaName tableName gk =
  countJobsFiltered schemaName tableName [Tmpl.FilterGroupKey gk]

-- | Count in-flight jobs
countInFlightJobs
  :: (MonadArbiter m)
  => Text -> Text -> m Int64
countInFlightJobs schemaName tableName =
  countJobsFiltered schemaName tableName [Tmpl.FilterInFlight]

-- | Count DLQ jobs
countDLQJobs
  :: (MonadArbiter m)
  => Text -> Text -> m Int64
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
  -> Text
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
  => Text -> Text -> Int64 -> m Int64
countJobsByParent schemaName tableName pid =
  countJobsFiltered schemaName tableName [Tmpl.FilterParentId pid]

-- | Count children for a batch of potential parent IDs.
--
-- Returns a Map from parent_id to @(total, paused)@ counts (only non-zero entries).
countChildrenBatch
  :: (MonadArbiter m)
  => Text -> Text -> [Int64] -> m (Map.Map Int64 (Int64, Int64))
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
  => Text -> Text -> [Int64] -> m (Map.Map Int64 Int64)
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
  -> Text
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
  -> Text
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
  -> Text
  -- ^ Table name
  -> Int64
  -- ^ Root job ID
  -> m Int64
cancelJobCascade schemaName tableName jobId = withDbTransaction $ do
  parentRows <-
    executeQuery
      (Tmpl.getParentIdSQL schemaName tableName)
      [pval CInt8 jobId]
      nullableInt64Codec
  let rootParentId = case parentRows of
        [Just pid] -> Just pid
        _ -> Nothing

  countRows <-
    executeQuery
      (Tmpl.cancelJobCascadeSQL schemaName tableName)
      [pval CInt8 jobId]
      countCodec
  let deleted = case countRows of
        [n] -> n
        _ -> 0

  when (deleted > 0) $
    for_ rootParentId $
      tryResumeParent schemaName tableName

  pure deleted

-- | Cancel an entire job tree by walking up from any node to the root,
-- then cascade-deleting everything from the root down.
--
-- Unlike 'cancelJobCascade', this does NOT call 'tryResumeParent' — the root
-- by definition has no parent. Returns the total number of jobs deleted.
cancelJobTree
  :: (MonadArbiter m)
  => Text
  -- ^ Schema name
  -> Text
  -- ^ Table name
  -> Int64
  -- ^ Any job ID within the tree
  -> m Int64
cancelJobTree schemaName tableName jobId = do
  countRows <-
    executeQuery
      (Tmpl.cancelJobTreeSQL schemaName tableName)
      [pval CInt8 jobId]
      countCodec
  case countRows of
    [n] -> pure n
    _ -> throwParsing "cancelJobTree: unexpected result"

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
  -> Text
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
  -> Text
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
-- Corrects any drift in job_count, min_priority, min_id, and in_flight_until.
-- Checks the reaper sequence to skip if a recent run occurred within the
-- given interval. Uses an advisory lock to serialize concurrent attempts,
-- then locks all groups rows to prevent trigger interleaving.
refreshGroups
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> Int
  -- ^ Minimum interval between runs (seconds)
  -> m ()
refreshGroups schemaName tableName intervalSecs = do
  seqResult <- executeQuery (Tmpl.checkReaperSeqSQL schemaName tableName) [] int64Codec
  case seqResult of
    [lastEpoch] -> do
      nowResult <- executeQuery "SELECT extract(epoch FROM now())::bigint AS result" [] int64Codec
      case nowResult of
        [nowEpoch]
          | nowEpoch - lastEpoch < fromIntegral intervalSecs -> pure ()
        _ -> doRefresh
    _ -> doRefresh
  where
    doRefresh = withDbTransaction $ do
      let tbl = jobQueueTable schemaName tableName
      acquired <-
        executeQuery
          "SELECT pg_try_advisory_xact_lock(hashtextextended(?, ?)) AS result"
          [pval CText tbl, pval CInt8 2]
          boolCodec
      case acquired of
        [True] -> do
          void $ executeQuery (Tmpl.lockGroupsSQL schemaName tableName) [] int64Codec
          void $ executeStatement (Tmpl.refreshGroupsSQL schemaName tableName) []
          void $ executeQuery (Tmpl.updateReaperSeqSQL schemaName tableName) [] int64Codec
        _ -> pure ()

-- | Read child results, DLQ errors, parent_state snapshot, and DLQ failures
-- for a rollup finalizer in a single query.
readChildResultsRaw
  :: (MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
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
  => Text
  -- ^ PostgreSQL schema name
  -> Text
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

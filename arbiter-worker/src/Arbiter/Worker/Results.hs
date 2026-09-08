{-# LANGUAGE TypeFamilies #-}

-- | Reading, combining, and storing the results of a rollup job's children.
module Arbiter.Worker.Results
  ( childResults
  , mergedChildResults
  , mergeChildResults
  , storeJobResult
  , storeEncodedResult
  , storeEncodedResults
  , settleAck
  , settleDeadLetter
  ) where

import Arbiter.Core.Job.Types (JobRead, parentId, primaryKey, queueName)
import Arbiter.Core.JobResult (EncodeJobResult, decodeJobResult, encodeJobResult)
import Arbiter.Core.MonadArbiter (MonadArbiter, ResultOf, getSchema)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.Settled (JobSettledHook, SettledJob (..), SettledOutcome (..), fireJobSettled)
import Control.Monad (void)
import Data.Aeson (FromJSON, Value)
import Data.Either (partitionEithers)
import Data.Foldable (fold, foldMap')
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (mapMaybe)
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)

-- | A rollup parent's immediate child results, keyed by child id, and its DLQ
-- errors, keyed by DLQ row id for 'Arbiter.Core.HighLevel.retryFromDLQ'. A
-- decode failure is returned as 'Left'.
childResults
  :: (FromJSON (ResultOf m payload), MonadArbiter m)
  => JobRead payload
  -> m (Map Int64 (Either Text (ResultOf m payload)), Map Int64 Text)
childResults job = do
  schema <- getSchema
  (results, failures, snapshot, dlqFailures) <-
    Ops.readChildResultsRaw schema (queueName job) (primaryKey job)
  let raw = Ops.mergeRawChildResults results failures snapshot
  pure (Map.map (>>= decodeJobResult) raw, dlqFailures)

-- | 'childResults' with successfully decoded values combined through 'Monoid'.
mergedChildResults
  :: ( FromJSON (ResultOf m payload)
     , MonadArbiter m
     , Monoid (ResultOf m payload)
     )
  => JobRead payload
  -> m (ResultOf m payload, Map Int64 Text)
mergedChildResults job = do
  (results, dlqFailures) <- childResults job
  pure (mergeChildResults results, dlqFailures)

-- | Combine successful child results, treating decode failures as 'mempty'.
mergeChildResults :: (Monoid a) => Map Int64 (Either Text a) -> a
mergeChildResults = foldMap' fold

-- | Store a job's result for its parent rollup, if it has one.
storeJobResult
  :: (EncodeJobResult result, MonadArbiter m)
  => Text
  -> JobRead payload
  -> result
  -> m ()
storeJobResult schemaName job = storeEncodedResult schemaName job . encodeJobResult

-- | 'storeJobResult' on an already-encoded result. 'Nothing' stores nothing.
storeEncodedResult
  :: (MonadArbiter m)
  => Text
  -> JobRead payload
  -> Maybe Value
  -> m ()
storeEncodedResult schemaName job mVal =
  case (parentId job, mVal) of
    (Just pid, Just val) ->
      void $ Ops.insertResult schemaName (queueName job) pid (primaryKey job) val
    (Nothing, Just val)
      | Ops.archivesOnAck job ->
          void $ Ops.updateArchiveResult schemaName (queueName job) (primaryKey job) val
    _ -> pure ()

-- | 'storeEncodedResult' over a batch from one queue. One statement stores the
-- child results and one the archived roots.
storeEncodedResults
  :: (MonadArbiter m)
  => Text
  -> [(JobRead payload, Maybe Value)]
  -> m ()
storeEncodedResults _ [] = pure ()
storeEncodedResults schemaName pairs@((firstJob, _) : _) = do
  let (childRows, rootRows) = partitionEithers (mapMaybe resultRow pairs)
      queue = queueName firstJob
  void $ Ops.insertResultsBatch schemaName queue childRows
  void $ Ops.updateArchiveResultsBatch schemaName queue rootRows
  where
    resultRow (job, mVal) = do
      val <- mVal
      case parentId job of
        Just pid -> Just (Left (pid, primaryKey job, val))
        Nothing
          | Ops.archivesOnAck job -> Just (Right (primaryKey job, val))
          | otherwise -> Nothing

-- | What an ack owes the rest of the pool: the results of the rows it took, and the
-- settled hook for the rows it deleted. Every ack goes through here, so the two stay
-- in step. Reports the ids the ack took, which a batch partitions on.
settleAck
  :: (MonadArbiter m)
  => Text
  -> JobSettledHook m
  -> [Ops.AckedRow]
  -> [(JobRead payload, Maybe Value)]
  -> m (Set Int64)
settleAck schemaName hook rows pairs = do
  store (filter (took . fst) pairs)
  fireJobSettled
    hook
    [ SettledJob (queueName job) (primaryKey job) stored JobAcked
    | (job, stored) <- pairs
    , primaryKey job `Set.member` deleted
    ]
  pure taken
  where
    taken = Set.fromList (map Ops.ackedId rows)
    deleted = Set.fromList [Ops.ackedId row | row <- rows, Ops.ackedDeleted row]
    took = (`Set.member` taken) . primaryKey
    store [(job, stored)] = storeEncodedResult schemaName job stored
    store many = storeEncodedResults schemaName many

-- | What a move to the dead-letter queue owes the rest of the pool: the settled hook
-- for the row it took. Every such move goes through here, as every ack goes through
-- 'settleAck', so the hook cannot be forgotten. Reports the rows the move wrote.
settleDeadLetter
  :: (MonadArbiter m)
  => JobSettledHook m
  -> Ops.TreeLocks
  -> Text
  -- ^ Schema name
  -> Text
  -- ^ Queue name
  -> Text
  -- ^ The failure that sent the job to the queue
  -> JobRead payload
  -> m Int64
settleDeadLetter hook locks schemaName queue errorMsg job = do
  moved <- Ops.moveToDLQ locks schemaName queue errorMsg job
  moved <$ fireJobSettled hook [SettledJob queue (primaryKey job) Nothing JobDeadLettered | moved > 0]

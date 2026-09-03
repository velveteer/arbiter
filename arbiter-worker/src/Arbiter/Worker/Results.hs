{-# LANGUAGE TypeFamilies #-}

-- | Reading, combining, and storing the results of a rollup job's children.
module Arbiter.Worker.Results
  ( childResults
  , mergedChildResults
  , mergeChildResults
  , storeJobResult
  , storeEncodedResult
  , storeEncodedResults
  ) where

import Arbiter.Core.Job.Types (JobRead, parentId, primaryKey, queueName)
import Arbiter.Core.JobResult (EncodeJobResult, decodeJobResult, encodeJobResult)
import Arbiter.Core.MonadArbiter (MonadArbiter, ResultOf, getSchema)
import Arbiter.Core.Operations qualified as Ops
import Control.Monad (void)
import Data.Aeson (FromJSON, Value)
import Data.Either (partitionEithers)
import Data.Foldable (fold, foldMap')
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (mapMaybe)
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

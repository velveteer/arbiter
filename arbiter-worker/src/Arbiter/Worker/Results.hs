{-# LANGUAGE TypeFamilies #-}

-- | Reading and combining the immediate child results of a rollup job.
module Arbiter.Worker.Results
  ( childResults
  , mergedChildResults
  , mergeChildResults
  ) where

import Arbiter.Core.Job.Types (JobRead, primaryKey, queueName)
import Arbiter.Core.JobResult (decodeJobResult)
import Arbiter.Core.MonadArbiter (MonadArbiter, ResultOf, getSchema)
import Arbiter.Core.Operations qualified as Ops
import Data.Aeson (FromJSON)
import Data.Foldable (fold, foldMap')
import Data.Int (Int64)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)

-- | A rollup parent's immediate child results and DLQ errors, both keyed by
-- child ID. A decode failure is returned as 'Left'.
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

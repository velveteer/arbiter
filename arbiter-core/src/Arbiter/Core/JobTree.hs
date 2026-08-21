{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Atomic parent-child job trees. Children run before their suspended
-- finalizer, which becomes claimable when no children remain in the main queue.
-- Child results are transient and are deleted when the finalizer is acked.
-- Finalizers must persist any results that need to outlive the tree.
module Arbiter.Core.JobTree
  ( -- * Tree type
    JobTree

    -- * Smart constructors
  , leaf
  , rollup

    -- * Operators
  , (<~~)

    -- * Interpreter
  , insertJobTree
  ) where

import Control.Exception (Exception)
import Control.Monad (when)
import Data.Aeson (Object, Value (..))
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Text (Text)
import UnliftIO.Exception qualified as UE

import Arbiter.Core.Job.Types
  ( JobPayload
  , JobRead
  , JobWrite
  , primaryKey
  )
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.Trace (markSpanError, withPublishSpan)

-- | Aborts a tree insertion transaction. 'insertJobTree' catches it and returns @Left@.
newtype TreeInsertFailed = TreeInsertFailed Text
  deriving stock (Show)
  deriving anyclass (Exception)

-- | A tree of jobs. Leaves are single jobs. Finalizers are parents with
-- children that run immediately while the parent waits for completion.
data JobTree payload
  = -- | A single job with no children.
    Leaf (JobWrite payload)
  | -- | A finalizer job with children. The finalizer is suspended until
    -- all children complete, then it becomes claimable for a completion round.
    Finalizer (JobWrite payload) (NonEmpty (JobTree payload))

-- | A single job with no children.
leaf :: JobWrite payload -> JobTree payload
leaf = Leaf

-- | A finalizer running once no child of it is left in the main queue. Nested rollups
-- do not merge on their own: an intermediate finalizer has to return the merged value
-- itself for results to travel upward.
--
-- @
-- rollup (defaultJob root)
--   ( leaf (defaultJob leaf1)
--   :| [leaf (defaultJob leaf2)]
--   )
-- @
rollup :: JobWrite payload -> NonEmpty (JobTree payload) -> JobTree payload
rollup = Finalizer

-- | The empty rollup snapshot @{}@ every finalizer is inserted with. A non-null snapshot
-- is what marks a job as a finalizer, and the value is overwritten with the merged child
-- results before a DLQ move.
emptyState :: Value
emptyState = Object (mempty :: Object)

-- | Infix 'rollup' for leaf-only children.
--
-- @
-- defaultJob reducer \<~~ (defaultJob mapper1 :| [defaultJob mapper2])
-- @
infixr 6 <~~

(<~~) :: JobWrite payload -> NonEmpty (JobWrite payload) -> JobTree payload
parent <~~ children = Finalizer parent (fmap Leaf children)

-- | Insert a 'JobTree' in one transaction, returning every inserted job root-first.
-- @Left@ on any failure, such as a dedup conflict, with nothing committed.
insertJobTree
  :: forall m payload
   . (JobPayload payload, MonadArbiter m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> JobTree payload
  -> m (Either Text (NonEmpty (JobRead payload)))
insertJobTree schemaName tableName tree =
  withPublishSpan tableName (treeWrites tree) $ do
    inserted <- UE.try $ do
      stamp <- Ops.traceStamp
      withDbTransaction $ go stamp Nothing (rootSuspended tree) tree
    either (\(TreeInsertFailed msg) -> Left msg <$ markSpanError msg) (pure . Right) inserted
  where
    -- Finalizer roots are suspended (waiting for children to complete).
    rootSuspended :: JobTree payload -> Bool
    rootSuspended (Finalizer _ _) = True
    rootSuspended _ = False

    treeWrites :: JobTree payload -> [JobWrite payload]
    treeWrites (Leaf j) = [j]
    treeWrites (Finalizer j children) = j : foldMap treeWrites children

    go
      :: Ops.TraceStamp payload
      -> Maybe Int64
      -- \^ Parent primary key (Nothing for root)
      -> Bool
      -- \^ Whether this node should be inserted suspended
      -> JobTree payload
      -> m (NonEmpty (JobRead payload))
    go stamp mParentId susp (Leaf jobW) = do
      mInserted <- Ops.insertJobTreeNodeStamped schemaName tableName stamp mParentId Nothing susp jobW
      case mInserted of
        Nothing -> UE.throwIO $ TreeInsertFailed "insertJobTree: job insert failed (dedup conflict)"
        Just inserted -> pure (inserted :| [])
    go stamp mParentId susp (Finalizer jobW children) = do
      mInserted <- Ops.insertJobTreeNodeStamped schemaName tableName stamp mParentId (Just emptyState) susp jobW
      case mInserted of
        Nothing -> UE.throwIO $ TreeInsertFailed "insertJobTree: parent insert failed (dedup conflict)"
        Just inserted -> do
          descendants <- insertChildren (primaryKey inserted) (NE.toList children)
          pure (inserted :| descendants)
      where
        -- Batch adjacent leaves without reordering them around nested trees.
        insertChildren _ [] = pure []
        insertChildren parentPK children'@(Leaf _ : _) = do
          let (leaves, rest) = span isLeaf children'
              leafWrites = [job | Leaf job <- leaves]
          leafJobs <- Ops.insertJobTreeLeavesStamped schemaName tableName stamp parentPK leafWrites
          when (length leafJobs /= length leafWrites)
            $ UE.throwIO
            $ TreeInsertFailed "insertJobTree: leaf batch insert had dedup conflicts"
          (leafJobs <>) <$> insertChildren parentPK rest
        insertChildren parentPK (subTree : rest) = do
          subTreeJobs <- go stamp (Just parentPK) True subTree
          remaining <- insertChildren parentPK rest
          pure (NE.toList subTreeJobs <> remaining)

        isLeaf (Leaf _) = True
        isLeaf _ = False

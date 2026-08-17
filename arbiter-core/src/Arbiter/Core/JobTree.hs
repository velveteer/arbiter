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
  ( Job (..)
  , JobPayload
  , JobRead
  , JobWrite
  )
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.Trace (markSpanError, withPublishSpan)

-- | Internal exception used to abort a tree insertion transaction.
-- Not exported - caught and converted to @Left@ by 'insertJobTree'.
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

-- | A single job (leaf node) - a terminal node with no children.
leaf :: JobWrite payload -> JobTree payload
leaf = Leaf

-- | Finalizer that runs after all children finish.
--
-- For nested rollups, intermediate finalizers must explicitly return the
-- merged value to propagate results upward. This is not automatic. When no
-- children remain in the main queue (all completed or DLQ'd), the finalizer
-- wakes.
--
-- @
-- rollup (defaultJob root)
--   ( leaf (defaultJob leaf1)
--   :| [leaf (defaultJob leaf2)]
--   )
-- @
rollup :: JobWrite payload -> NonEmpty (JobTree payload) -> JobTree payload
rollup parent children =
  Finalizer (parent {parentState = Just emptyState}) children

-- | An empty rollup snapshot @{}@. The DB stores this on insert for any
-- rollup finalizer. Presence-of-non-null is the canonical signal of
-- rollup-ness, and the value is overwritten with merged child results
-- before a DLQ move.
emptyState :: Value
emptyState = Object (mempty :: Object)

-- | Infix 'rollup' for leaf-only children.
--
-- @
-- defaultJob reducer \<~~ (defaultJob mapper1 :| [defaultJob mapper2])
-- @
infixr 6 <~~

(<~~) :: JobWrite payload -> NonEmpty (JobWrite payload) -> JobTree payload
parent <~~ children = Finalizer (parent {parentState = Just emptyState}) (fmap Leaf children)

-- | Insert a 'JobTree' atomically in a single transaction.
--
-- Returns a flat 'NonEmpty' list of all inserted jobs (pre-order: root first).
-- Returns @Left errMsg@ if any insertion fails (e.g. dedup conflict on root,
-- phantom parent). The entire transaction is rolled back on failure - no
-- partial trees are committed.
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
      let jobW' = jobW {parentId = mParentId, suspended = susp}
      mInserted <- Ops.insertJobUnsafeStamped schemaName tableName stamp jobW'
      case mInserted of
        Nothing -> UE.throwIO $ TreeInsertFailed "insertJobTree: job insert failed (dedup conflict or invalid parent)"
        Just inserted -> pure (inserted :| [])
    go stamp mParentId susp (Finalizer jobW children) = do
      let jobW' = jobW {parentId = mParentId, suspended = susp}
      mInserted <- Ops.insertJobUnsafeStamped schemaName tableName stamp jobW'
      case mInserted of
        Nothing -> UE.throwIO $ TreeInsertFailed "insertJobTree: parent insert failed (dedup conflict or invalid parent)"
        Just inserted -> do
          descendants <- insertChildren (primaryKey inserted) (NE.toList children)
          pure (inserted :| descendants)
      where
        -- Batch adjacent leaves without reordering them around nested trees.
        insertChildren _ [] = pure []
        insertChildren parentPK children'@(Leaf _ : _) = do
          let (leaves, rest) = span isLeaf children'
              leafWrites =
                [ job {parentId = Just parentPK, suspended = False}
                | Leaf job <- leaves
                ]
          leafJobs <- Ops.insertJobsBatchStamped schemaName tableName stamp leafWrites
          when (length leafJobs /= length leafWrites) $
            UE.throwIO $
              TreeInsertFailed "insertJobTree: leaf batch insert had dedup conflicts"
          (leafJobs <>) <$> insertChildren parentPK rest
        insertChildren parentPK (subTree : rest) = do
          subTreeJobs <- go stamp (Just parentPK) True subTree
          remaining <- insertChildren parentPK rest
          pure (NE.toList subTreeJobs <> remaining)

        isLeaf (Leaf _) = True
        isLeaf _ = False

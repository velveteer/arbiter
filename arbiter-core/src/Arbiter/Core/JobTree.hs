{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}

-- | Compositional DSL for building multi-level job trees.
--
-- A 'JobTree' describes a hierarchy of parent-child jobs that is inserted
-- atomically (in a single transaction). Trees can be arbitrarily deep.
--
-- Children run immediately; the finalizer (parent) is suspended until all
-- children complete, then becomes claimable for a completion round.
-- Child results are auto-stored in @{queue}_results@ and passed to the
-- finalizer handler as @Map Int64 (Either Text result)@.
--
-- __Important:__ The results table is a transient coordination buffer, not
-- persistent storage. When the finalizer is acked (deleted), @ON DELETE CASCADE@
-- wipes all associated result rows. If you need results to survive beyond the
-- job tree's lifetime, persist them in your finalizer handler (e.g. write to
-- your own database table, publish to a message broker, etc.).
--
-- @
-- import Arbiter.Core.JobTree
--
-- -- Flat (leaf-only children):
-- myTree = defaultJob (CompileReport "q4")
--   \<~~ (defaultJob (RenderChart "sales") :| [defaultJob (RenderChart "growth")])
--
-- -- Nested:
-- myTree = rollup (defaultJob root)
--   ( (defaultJob mid \<~~ (defaultJob leaf1 :| [defaultJob leaf2]))
--   :| [leaf (defaultJob leaf3)]
--   )
--
-- result <- insertJobTree "arbiter" "reports" myTree
-- @
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
import Data.Either (partitionEithers)
import Data.Int (Int64)
import Data.List.NonEmpty (NonEmpty (..))
import Data.List.NonEmpty qualified as NE
import Data.Text (Text)
import Data.Typeable (Typeable)
import UnliftIO (MonadUnliftIO)
import UnliftIO.Exception qualified as UE

import Arbiter.Core.Job.Types
  ( Job (..)
  , JobPayload
  , JobRead
  , JobWrite
  )
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops

-- | Internal exception used to abort a tree insertion transaction.
-- Not exported — caught and converted to @Left@ by 'insertJobTree'.
newtype TreeInsertFailed = TreeInsertFailed Text
  deriving stock (Show, Typeable)
  deriving anyclass (Exception)

-- | A tree of jobs. Leaves are single jobs; finalizers are parents with
-- children that run immediately while the parent waits for completion.
data JobTree payload
  = -- | A single job with no children.
    Leaf (JobWrite payload)
  | -- | A finalizer job with children. The finalizer is suspended until
    -- all children complete, then it becomes claimable for a completion round.
    Finalizer (JobWrite payload) (NonEmpty (JobTree payload))

-- ---------------------------------------------------------------------------
-- Smart constructors
-- ---------------------------------------------------------------------------

-- | A single job (leaf node) — a terminal node with no children.
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
  Finalizer (parent {isRollup = True}) children

-- ---------------------------------------------------------------------------
-- Operators
-- ---------------------------------------------------------------------------

-- | Infix 'rollup' for leaf-only children.
--
-- @
-- defaultJob reducer \<~~ (defaultJob mapper1 :| [defaultJob mapper2])
-- @
infixr 6 <~~

(<~~) :: JobWrite payload -> NonEmpty (JobWrite payload) -> JobTree payload
parent <~~ children = Finalizer (parent {isRollup = True}) (fmap Leaf children)

-- ---------------------------------------------------------------------------
-- Interpreter
-- ---------------------------------------------------------------------------

-- | Insert a 'JobTree' atomically in a single transaction.
--
-- Returns a flat 'NonEmpty' list of all inserted jobs (pre-order: root first).
-- Returns @Left errMsg@ if any insertion fails (e.g. dedup conflict on root,
-- phantom parent). The entire transaction is rolled back on failure — no
-- partial trees are committed.
insertJobTree
  :: forall m payload
   . (JobPayload payload, MonadArbiter m, MonadUnliftIO m)
  => Text
  -- ^ PostgreSQL schema name
  -> Text
  -- ^ Table name
  -> JobTree payload
  -> m (Either Text (NonEmpty (JobRead payload)))
insertJobTree schemaName tableName tree = do
  result <- UE.try $ withDbTransaction $ go Nothing (rootSuspended tree) tree
  pure $ case result of
    Left (TreeInsertFailed msg) -> Left msg
    Right jobs -> Right jobs
  where
    -- Finalizer roots are suspended (waiting for children to complete).
    rootSuspended :: JobTree payload -> Bool
    rootSuspended (Finalizer _ _) = True
    rootSuspended _ = False

    go
      :: Maybe Int64
      -- \^ Parent primary key (Nothing for root)
      -> Bool
      -- \^ Whether this node should be inserted suspended
      -> JobTree payload
      -> m (NonEmpty (JobRead payload))
    go mParentId susp (Leaf jobW) = do
      let jobW' = jobW {parentId = mParentId, suspended = susp}
      mInserted <- Ops.insertJobUnsafe schemaName tableName jobW'
      case mInserted of
        Nothing -> UE.throwIO $ TreeInsertFailed "insertJobTree: job insert failed (dedup conflict or invalid parent)"
        Just inserted -> pure (inserted :| [])
    go mParentId susp (Finalizer jobW children) = do
      let jobW' = jobW {parentId = mParentId, suspended = susp}
      mInserted <- Ops.insertJobUnsafe schemaName tableName jobW'
      case mInserted of
        Nothing -> UE.throwIO $ TreeInsertFailed "insertJobTree: parent insert failed (dedup conflict or invalid parent)"
        Just inserted -> do
          let parentPK = primaryKey inserted
              (leaves, subTrees) =
                partitionEithers
                  [case c of Leaf j -> Left j; t -> Right t | c <- NE.toList children]

          -- Recursively insert sub-finalizers first (preserves pre-order)
          subTreeJobs <- mapM (\child -> go (Just parentPK) True child) subTrees

          -- Batch-insert all leaf children in one roundtrip
          let leafWrites = [j {parentId = Just parentPK, suspended = False} | j <- leaves]
          leafJobs <- Ops.insertJobsBatch schemaName tableName leafWrites
          when (length leafJobs /= length leaves) $
            UE.throwIO $
              TreeInsertFailed "insertJobTree: leaf batch insert had dedup conflicts"

          pure (inserted :| concatMap NE.toList subTreeJobs <> leafJobs)

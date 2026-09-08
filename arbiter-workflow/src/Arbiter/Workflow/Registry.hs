{-# LANGUAGE GADTs #-}

-- | The definitions a deployment knows, by name and version.
module Arbiter.Workflow.Registry
  ( SomeWorkflow (..)
  , WorkflowRegistry
  , workflow
  , workflows
  , lookupWorkflow
  , latestVersion
  , registeredWorkflows
  ) where

import Data.Int (Int32)
import Data.List.NonEmpty (nonEmpty)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Text (Text)

import Arbiter.Workflow.Graph (Workflow (..))

-- | A definition with its input and output types hidden.
data SomeWorkflow registry where
  SomeWorkflow :: Workflow registry input output -> SomeWorkflow registry

-- | Definitions by name and version. A name maps to many versions: a run always
-- interprets the one it started under.
newtype WorkflowRegistry registry = WorkflowRegistry (Map (Text, Int32) (SomeWorkflow registry))

-- | A repeated @(name, version)@ keeps the one on the right, as 'workflows' keeps the
-- last one given.
instance Semigroup (WorkflowRegistry registry) where
  WorkflowRegistry left <> WorkflowRegistry right = WorkflowRegistry (right <> left)

instance Monoid (WorkflowRegistry registry) where
  mempty = WorkflowRegistry Map.empty

-- | One definition, ready to register.
workflow :: Workflow registry input output -> SomeWorkflow registry
workflow = SomeWorkflow

-- | Collect definitions. A repeated @(name, version)@ keeps the last one given.
workflows :: [SomeWorkflow registry] -> WorkflowRegistry registry
workflows = WorkflowRegistry . Map.fromList . map entry
  where
    entry some@(SomeWorkflow definition) = ((workflowName definition, workflowVersion definition), some)

-- | The definition a run interprets.
lookupWorkflow :: WorkflowRegistry registry -> Text -> Int32 -> Maybe (SomeWorkflow registry)
lookupWorkflow (WorkflowRegistry byKey) name version = Map.lookup (name, version) byKey

-- | The highest version registered under a name.
latestVersion :: WorkflowRegistry registry -> Text -> Maybe Int32
latestVersion (WorkflowRegistry byKey) name =
  maximum <$> nonEmpty [version | (registered, version) <- Map.keys byKey, registered == name]

-- | Every registered definition, by name and version.
registeredWorkflows :: WorkflowRegistry registry -> [(Text, Int32)]
registeredWorkflows (WorkflowRegistry byKey) = Map.keys byKey

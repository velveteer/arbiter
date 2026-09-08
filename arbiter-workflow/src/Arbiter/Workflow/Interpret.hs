-- | The interpreters over a definition. The builder is pure given the stored outputs
-- of the virtual steps, so one pass rebuilds the graph a run materialized.
module Arbiter.Workflow.Interpret
  ( Materialized (..)
  , materialize
  , deadlineAt
  , nodeInput
  , Rendered (..)
  , render
  ) where

import Arbiter.Core.Job.Types (JobWrite)
import Data.Aeson (Value)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Time (NominalDiffTime, UTCTime, addUTCTime)

import Arbiter.Workflow.Expr (toRef)
import Arbiter.Workflow.Graph
  ( BuildMode (..)
  , Built (..)
  , Node (..)
  , Workflow (..)
  , bareNode
  , inputStepName
  , runGraph
  )
import Arbiter.Workflow.Types (StepKind (..), StepName)

-- | The graph a run has materialized so far.
data Materialized = Materialized
  { materializedNodes :: [Node]
  , materializedComplete :: Bool
  -- ^ 'False' while a virtual step has yet to settle, so more steps are to come.
  , materializedOutput :: Maybe StepName
  -- ^ The run's sink, once the graph is complete.
  }

-- | Rebuild a run's graph from its definition and its stored outputs.
materialize
  :: Workflow registry input output
  -> Value
  -- ^ The run input.
  -> Map StepName Value
  -- ^ The outputs stored so far, by step name.
  -> Either Text Materialized
materialize (Workflow _ _ graph) input stored = do
  built <- runGraph (Materialize known) (graph (toRef inputStepName))
  pure
    Materialized
      { materializedNodes = inputNode (Just input) : builtNodes built
      , materializedComplete = isJust (builtSink built)
      , materializedOutput = builtSink built
      }
  where
    known = Map.insert inputStepName input stored

-- | A definition's static graph.
data Rendered = Rendered
  { renderedNodes :: [(StepName, StepKind)]
  , renderedEdges :: [(StepName, StepName)]
  }
  deriving stock (Eq, Show)

-- | The definition's static shape: both arms, one opaque node per continuation.
render :: Workflow registry input output -> Either Text Rendered
render (Workflow _ _ graph) = do
  built <- runGraph Render (graph (toRef inputStepName))
  let nodes = inputNode Nothing : builtNodes built
  pure
    Rendered
      { renderedNodes = [(nodeName node, nodeKind node) | node <- nodes]
      , renderedEdges = [(from, nodeName node) | node <- nodes, from <- nodeAfter node]
      }

-- | The run input's step, done from the start.
inputNode :: Maybe Value -> Node
inputNode output = (bareNode inputStepName KindInput) {nodeOutput = output}

-- | A step's job, or 'Nothing' when it holds none or its inputs are not stored yet.
nodeInput :: Map StepName Value -> Node -> Maybe (JobWrite Value)
nodeInput stored node = case ($ stored) <$> nodeJob node of
  Just (Right job) -> Just job
  _ -> Nothing

deadlineAt :: UTCTime -> Maybe NominalDiffTime -> Maybe UTCTime
deadlineAt now = fmap (`addUTCTime` now)

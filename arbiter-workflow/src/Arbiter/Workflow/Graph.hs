{-# LANGUAGE AllowAmbiguousTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RequiredTypeArguments #-}
{-# LANGUAGE TypeFamilies #-}
{-# LANGUAGE UndecidableInstances #-}

-- | The graph builder. It runs at definition time, at each materialization, and at
-- render time.
module Arbiter.Workflow.Graph
  ( Graph
  , (>>=)
  , (>>)
  , (<*>)
  , fmap
  , join
  , pure
  , return
  , Absent
  , Workflow (..)
  , step
  , stepWith
  , embed
  , under
  , signal
  , branch
  , choose
  , expand
  , forEach
  , Node (..)
  , bareNode
  , BuildMode (..)
  , Built (..)
  , runGraph
  , inputStepName
  ) where

import Arbiter.Core.Job.Types (JobWrite, defaultJob, mapPayload)
import Arbiter.Core.QueueRegistry (ResultFor, TableForPayload)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT, runExceptT, throwE)
import Control.Monad.Trans.State.Strict (State, gets, modify', runState)
import Data.Aeson (FromJSON, Result (..), ToJSON, Value (Array), fromJSON, toJSON)
import Data.Int (Int32)
import Data.Kind (Constraint, Type)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Proxy (Proxy (..))
import Data.Set (Set)
import Data.Set qualified as Set
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Data.Vector qualified as Vector
import GHC.TypeLits (ErrorMessage (..), KnownSymbol, Symbol, TypeError, UnconsSymbol, symbolVal)
import Prelude hiding (fmap, pure, return, (<*>), (>>), (>>=))
import Prelude qualified as P

import Arbiter.Workflow.Expr (Expr, Ref, evalExpr, refName, refsOf, toRef)
import Arbiter.Workflow.Types (StepKind (..), StepName (..))

-- | A definition, at one version. The names its graph takes are checked when it is
-- written and hidden here, so a signature over a definition never spells them.
data Workflow registry input output
  = forall (used :: [Symbol]).
  Workflow
  { workflowName :: Text
  , workflowVersion :: Int32
  , workflowGraph :: forall (s :: Type). Ref s input -> Graph s registry '[] used (Ref s output)
  -- ^ Rank-2 in @s@, so a ref of one definition cannot be read in another, and none
  -- of them escapes the graph that made it. Read it by pattern match.
  }

-- | One step the builder emitted.
data Node = Node
  { nodeName :: StepName
  , nodeKind :: StepKind
  , nodeQueue :: Maybe Text
  , nodeAfter :: [StepName]
  -- ^ Predecessors, in the order the step reads them.
  , nodeIndex :: Maybe Int32
  , nodeSignalKey :: Maybe Text
  , nodeDeadline :: Maybe NominalDiffTime
  , nodeOutput :: Maybe Value
  -- ^ Set for a node the builder already knows the output of.
  , nodeJob :: Maybe (Map StepName Value -> Either Text (JobWrite Value))
  -- ^ The job to enqueue, once the outputs it reads are stored.
  , nodeCompute :: Maybe (Map StepName Value -> Either Text Value)
  -- ^ What a virtual node settles to, once the outputs it reads are stored.
  }

-- | What the builder is doing.
data BuildMode
  = -- | Follow the arms and continuations these outputs decide.
    Materialize (Map StepName Value)
  | -- | Show the definition: both arms, one opaque node per expand.
    Render

-- | What one pass built.
data Built = Built
  { builtNodes :: [Node]
  , builtSink :: Maybe StepName
  -- ^ 'Nothing' when a virtual node with no stored output stopped the pass.
  }

-- | The builder's own monad. Every internal combinator is written in it.
type Build = ExceptT Halt (State BuildState)

-- | The builder. A pass finishes, or stops at the first virtual node with no stored
-- output. It carries the names taken so far, so a name two siblings share is a type
-- error. Write a graph with @QualifiedDo@ over this module.
newtype Graph (s :: Type) registry (used :: [Symbol]) (used' :: [Symbol]) a = Graph (Build a)

unGraph :: Graph s registry i j a -> Build a
unGraph (Graph action) = action

-- | A name that is one path segment none of its siblings has taken.
type family Absent (name :: Symbol) (used :: [Symbol]) :: Constraint where
  Absent name used = (OneSegment name, NotTaken name used)

-- | A name carrying the separator would read as a path and could name a step of a
-- subgraph. A name carrying @\@@ could take the name of a step the builder makes for
-- a branch, a continuation or a signal.
type family OneSegment (name :: Symbol) :: Constraint where
  OneSegment name = SegmentOf name (UnconsSymbol name)

type family SegmentOf (name :: Symbol) (parts :: Maybe (Char, Symbol)) :: Constraint where
  SegmentOf _ 'Nothing = ()
  SegmentOf name ('Just '( '.', _)) =
    TypeError ('Text "the name " ':<>: 'Text name ':<>: 'Text " carries ., which separates the path")
  SegmentOf name ('Just '( '@', _)) =
    TypeError
      ('Text "the name " ':<>: 'Text name ':<>: 'Text " carries @, which the builder names its own steps with")
  SegmentOf name ('Just '(_, rest)) = SegmentOf name (UnconsSymbol rest)

type family NotTaken (name :: Symbol) (used :: [Symbol]) :: Constraint where
  NotTaken _ '[] = ()
  NotTaken name (name : _) = TypeError ('Text "two steps are named " ':<>: 'Text name)
  NotTaken name (_ : rest) = NotTaken name rest

(>>=) :: Graph s registry i j a -> (a -> Graph s registry j k b) -> Graph s registry i k b
Graph action >>= next = Graph (action P.>>= (unGraph . next))

(>>) :: Graph s registry i j a -> Graph s registry j k b -> Graph s registry i k b
before >> after = before Arbiter.Workflow.Graph.>>= P.const after

fmap :: (a -> b) -> Graph s registry i j a -> Graph s registry i j b
fmap f (Graph action) = Graph (P.fmap f action)

(<*>) :: Graph s registry i j (a -> b) -> Graph s registry j k a -> Graph s registry i k b
Graph f <*> Graph a = Graph (f P.<*> a)

join :: Graph s registry i j (Graph s registry j k a) -> Graph s registry i k a
join outer = outer Arbiter.Workflow.Graph.>>= id

pure :: a -> Graph s registry i i a
pure = Graph . P.pure

return :: a -> Graph s registry i i a
return = pure

-- | Why a pass stopped.
data Halt
  = Blocked
  | Failed Text

data BuildState = BuildState
  { bsMode :: BuildMode
  , bsAnchor :: Maybe StepName
  , bsPath :: [Text]
  , bsNodes :: [Node]
  , bsOrdinals :: Map (Text, Text) Int
  , bsNames :: Set StepName
  }

-- | The run input's own step. Every graph starts from it.
inputStepName :: StepName
inputStepName = StepName "input"

-- | Run one pass of a builder. The pass is polymorphic in its graph, so only the
-- sink's name leaves it, never a ref.
runGraph
  :: forall registry output (used :: [Symbol])
   . BuildMode
  -> (forall (s :: Type). Graph s registry '[] used (Ref s output))
  -> Either Text Built
runGraph mode graph =
  case outcome of
    Left (Failed err) -> Left err
    Left Blocked -> Right (Built emitted Nothing)
    Right sink -> Right (Built emitted (Just (refName sink)))
  where
    (outcome, final) = runState (runExceptT (unGraph graph)) (initialState mode)
    emitted = reverse (bsNodes final)

initialState :: BuildMode -> BuildState
initialState mode =
  BuildState
    { bsMode = mode
    , bsAnchor = Nothing
    , bsPath = []
    , bsNodes = []
    , bsOrdinals = Map.empty
    , bsNames = Set.empty
    }

-- | A step in the queue its payload names in the registry. Its name is unique among
-- its siblings; the builder prefixes it with the path.
step
  :: forall s registry payload i (used :: [Symbol])
   . (KnownSymbol (TableForPayload payload registry), ToJSON payload)
  => forall (name :: Symbol)
  ->(Absent name used, KnownSymbol name)
  => (i -> payload)
  -> Expr s i
  -> Graph s registry used (name : used) (Ref s (ResultFor payload registry))
step name build = Graph . jobStep @payload @registry (symbolText @name) (defaultJob . build)

-- | 'step' building its whole job, so it sets its own group key, priority, dedup key
-- and attempt budget.
stepWith
  :: forall s registry payload i (used :: [Symbol])
   . (KnownSymbol (TableForPayload payload registry), ToJSON payload)
  => forall (name :: Symbol)
  ->(Absent name used, KnownSymbol name)
  => (i -> JobWrite payload)
  -> Expr s i
  -> Graph s registry used (name : used) (Ref s (ResultFor payload registry))
stepWith name build = Graph . jobStep @payload @registry (symbolText @name) build

jobStep
  :: forall payload registry s i
   . (KnownSymbol (TableForPayload payload registry), ToJSON payload)
  => Text
  -> (i -> JobWrite payload)
  -> Expr s i
  -> Build (Ref s (ResultFor payload registry))
jobStep given build expr = do
  stepName <- freshName given
  emit
    (bareNode stepName KindJob)
      { nodeQueue = Just (T.pack (symbolVal (Proxy @(TableForPayload payload registry))))
      , nodeAfter = refsOf expr
      , nodeJob = Just (\stored -> mapPayload toJSON . build <$> evalExpr stored expr)
      }
  P.pure (toRef stepName)

-- | Another definition's graph, under a name of its own, over an input computed here.
-- Its steps are named @\<given name\>.\<their own\>@, so one definition embeds twice
-- without a clash, and the node holding its input takes the given name itself.
embed
  :: forall s registry input output (used :: [Symbol])
   . (ToJSON input)
  => forall (name :: Symbol)
  ->(Absent name used, KnownSymbol name)
  => Workflow registry input output
  -> Expr s input
  -> Graph s registry used (name : used) (Ref s output)
embed name (Workflow _ _ graph) source = Graph (underPath (symbolText @name) inner)
  where
    inner = do
      given <- pathName
      emit
        (bareNode given KindExpand)
          { nodeAfter = refsOf source
          , nodeCompute = Just (\stored -> toJSON <$> evalExpr stored source)
          }
      revealedBy given (unGraph (graph (toRef given)))

-- | A subgraph under a name of its own. Its steps are named @\<given name\>.\<their
-- own\>@, so the names inside are checked among themselves.
under
  :: forall s registry (used :: [Symbol]) (inner :: [Symbol]) a
   . forall (name :: Symbol)
  ->(Absent name used, KnownSymbol name)
  => Graph s registry '[] inner a
  -> Graph s registry used (name : used) a
under name body = Graph (underPath (symbolText @name) (unGraph body))

-- | A wait on an external value, with a deadline.
signal :: Text -> NominalDiffTime -> Graph s registry used used (Ref s a)
signal key deadline = Graph $ do
  name <- freshVirtual "signal"
  emit (bareNode name KindSignal) {nodeSignalKey = Just key, nodeDeadline = Just deadline}
  P.pure (toRef name)

-- | A finite branch. A run materializes the arm its condition decided. Each arm names
-- its steps among its own.
branch
  :: forall s registry a b c (used :: [Symbol]) (left :: [Symbol]) (right :: [Symbol])
   . (FromJSON a, FromJSON b, ToJSON a, ToJSON b)
  => Expr s (Either a b)
  -> (Ref s a -> Graph s registry '[] left (Ref s c))
  -> (Ref s b -> Graph s registry '[] right (Ref s c))
  -> Graph s registry used used (Ref s c)
branch condition onLeft onRight = Graph $ do
  name <- freshVirtual "branch"
  emit
    (bareNode name KindBranch)
      { nodeAfter = refsOf condition
      , nodeCompute = Just (\stored -> toJSON <$> evalExpr stored condition)
      }
  mode <- currentMode
  case mode of
    Render -> do
      _ <- arm name "left" (Nothing :: Maybe a) (unGraph . onLeft)
      arm name "right" (Nothing :: Maybe b) (unGraph . onRight)
    Materialize stored -> case Map.lookup name stored of
      Nothing -> halt Blocked
      Just value -> case fromJSON value of
        Error err -> failed ("the branch at " <> stepNameText name <> " does not decode: " <> T.pack err)
        Success (Left taken :: Either a b) -> arm name "left" (Just taken) (unGraph . onLeft)
        Success (Right taken) -> arm name "right" (Just taken) (unGraph . onRight)

-- | One arm: a node holding the condition's value, then the arm's steps under its path.
arm
  :: (ToJSON a)
  => StepName
  -> Text
  -> Maybe a
  -> (Ref s a -> Build (Ref s c))
  -> Build (Ref s c)
arm decision side taken build =
  underPath (lastSegment decision) $
    underPath side $ do
      armName <- pathName
      emit
        (bareNode armName KindBranch)
          { nodeAfter = [decision]
          , nodeOutput = toJSON <$> taken
          }
      revealedBy armName (build (toRef armName))

-- | A branch on a condition with nothing to carry into its arms.
choose
  :: forall s registry c (used :: [Symbol]) (left :: [Symbol]) (right :: [Symbol])
   . Expr s Bool
  -> Graph s registry '[] left (Ref s c)
  -> Graph s registry '[] right (Ref s c)
  -> Graph s registry used used (Ref s c)
choose condition whenTrue whenFalse =
  branch (arms <$> condition) (const whenTrue) (const whenFalse)
  where
    arms taken = if taken then Left () else Right ()

-- | A continuation. The function runs at settle time with a real value, and names its
-- steps among its own.
expand
  :: forall s registry a b (used :: [Symbol]) (inner :: [Symbol])
   . (FromJSON a, ToJSON a)
  => Expr s a
  -> (a -> Graph s registry '[] inner (Ref s b))
  -> Graph s registry used used (Ref s b)
expand source build = Graph $ do
  name <- expandNode source
  mode <- currentMode
  case mode of
    Render -> P.pure (toRef name)
    Materialize stored -> case Map.lookup name stored of
      Nothing -> halt Blocked
      Just value -> case fromJSON value of
        Error err -> failed (continuationError name err)
        Success (decoded :: a) -> underPath (lastSegment name) (revealedBy name (unGraph (build decoded)))

-- | Map over a runtime list, then merge the children's outputs in order. Each child
-- names its steps among its own.
forEach
  :: forall s registry a b (used :: [Symbol]) (inner :: [Symbol])
   . (FromJSON a, ToJSON a)
  => Expr s [a]
  -> (Ref s a -> Graph s registry '[] inner (Ref s b))
  -> Graph s registry used used (Ref s [b])
forEach source build = Graph $ do
  name <- expandNode source
  mode <- currentMode
  case mode of
    Render -> mergeNode name []
    Materialize stored -> case Map.lookup name stored of
      Nothing -> halt Blocked
      Just value -> case fromJSON value of
        Error err -> failed (continuationError name err)
        Success (elements :: [a]) -> do
          children <- underPath (lastSegment name) (traverse (child name) (zip [0 ..] elements))
          mergeNode name children
  where
    child name (index, element) =
      underPath (T.pack (show (index :: Int32))) $ do
        itemName <- pathName
        emit
          (bareNode itemName KindExpand)
            { nodeAfter = [name]
            , nodeIndex = Just index
            , nodeOutput = Just (toJSON element)
            }
        revealedBy itemName (unGraph (build (toRef itemName)))

-- | The node a continuation settles on.
expandNode :: (ToJSON a) => Expr s a -> Build StepName
expandNode source = do
  name <- freshVirtual "expand"
  name
    <$ emit
      (bareNode name KindExpand)
        { nodeAfter = refsOf source
        , nodeCompute = Just (\stored -> toJSON <$> evalExpr stored source)
        }

continuationError :: StepName -> String -> Text
continuationError name err =
  "the continuation at " <> stepNameText name <> " does not decode: " <> T.pack err

-- | The node collecting a @forEach@ child's outputs, in index order.
mergeNode :: StepName -> [Ref s b] -> Build (Ref s [b])
mergeNode expandName children = do
  let name = qualify expandName "merge"
      childNames = map refName children
      after = if null childNames then [expandName] else childNames
  _ <- claim name
  emit
    (bareNode name KindMerge)
      { nodeAfter = after
      , nodeCompute = Just (\stored -> Array . Vector.fromList <$> traverse (lookupOutput stored) childNames)
      }
  P.pure (toRef name)

lookupOutput :: Map StepName Value -> StepName -> Either Text Value
lookupOutput stored name =
  maybe (Left ("no output stored for step " <> stepNameText name)) Right (Map.lookup name stored)

bareNode :: StepName -> StepKind -> Node
bareNode name kind =
  Node
    { nodeName = name
    , nodeKind = kind
    , nodeQueue = Nothing
    , nodeAfter = []
    , nodeIndex = Nothing
    , nodeSignalKey = Nothing
    , nodeDeadline = Nothing
    , nodeOutput = Nothing
    , nodeJob = Nothing
    , nodeCompute = Nothing
    }

symbolText :: forall (name :: Symbol). (KnownSymbol name) => Text
symbolText = T.pack (symbolVal (Proxy @name))

-- | Add a segment to the path for the duration of a subgraph.
underPath :: Text -> Build a -> Build a
underPath segment build = do
  push segment
  result <- build
  result <$ pop

push :: Text -> Build ()
push segment = withState (\state -> state {bsPath = segment : bsPath state})

pop :: Build ()
pop = withState (\state -> state {bsPath = drop 1 (bsPath state)})

-- | A step's name: its path, then the name the caller gave it.
freshName :: Text -> Build StepName
freshName given = do
  path <- readState bsPath
  claim (StepName (T.intercalate "." (reverse (given : path))))

-- | The current path as a name of its own.
pathName :: Build StepName
pathName = readState bsPath P.>>= claim . StepName . T.intercalate "." . reverse

-- | A virtual node's name: its path, its kind, and its position among that kind.
freshVirtual :: Text -> Build StepName
freshVirtual label = do
  path <- readState bsPath
  let prefix = T.intercalate "." (reverse path)
  ordinal <- readState (Map.findWithDefault 0 (prefix, label) . bsOrdinals)
  withState (\state -> state {bsOrdinals = Map.insert (prefix, label) (ordinal + 1) (bsOrdinals state)})
  claim (StepName (T.intercalate "." (reverse ((label <> "@" <> T.pack (show ordinal)) : path))))

-- | Refuse a name two steps share.
claim :: StepName -> Build StepName
claim name = do
  taken <- readState bsNames
  if Set.member name taken
    then failed ("two steps are named " <> stepNameText name)
    else name <$ withState (\state -> state {bsNames = Set.insert name (bsNames state)})

-- | Emit a node. One with no ref of its own hangs off the virtual step that revealed
-- it, so a subgraph is downstream of the decision that materialized it.
emit :: Node -> Build ()
emit node = do
  anchor <- readState bsAnchor
  let anchored = case (nodeAfter node, anchor) of
        ([], Just revealed) -> node {nodeAfter = [revealed]}
        _ -> node
  withState (\state -> state {bsNodes = anchored : bsNodes state})

-- | Build a subgraph under the virtual step that revealed it.
revealedBy :: StepName -> Build a -> Build a
revealedBy revealed build = do
  previous <- readState bsAnchor
  withState (\state -> state {bsAnchor = Just revealed})
  result <- build
  result <$ withState (\state -> state {bsAnchor = previous})

currentMode :: Build BuildMode
currentMode = readState bsMode

readState :: (BuildState -> a) -> Build a
readState = lift . gets

withState :: (BuildState -> BuildState) -> Build ()
withState = lift . modify'

halt :: Halt -> Build a
halt = throwE

failed :: Text -> Build a
failed = halt . Failed

qualify :: StepName -> Text -> StepName
qualify (StepName prefix) segment = StepName (prefix <> "." <> segment)

lastSegment :: StepName -> Text
lastSegment (StepName name) = T.takeWhileEnd (/= '.') name

{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QualifiedDo #-}
{-# LANGUAGE RequiredTypeArguments #-}

-- | The graph builder and its interpreters.
module Test.Arbiter.Workflow.Builder (spec) where

import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Data.Aeson (FromJSON, ToJSON, Value (Number, String), toJSON)
import Data.Bifunctor (bimap)
import Data.Either (isLeft)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Text (Text)
import Data.Text qualified as T
import GHC.Generics (Generic)
import GHC.TypeLits (KnownSymbol, Symbol)
import Test.Hspec
import Test.QuickCheck (Property, forAll, sublistOf, (===))

import Arbiter.Workflow.Expr (Expr, Ref, both, evalExpr, refsOf, toRef, use)
import Arbiter.Workflow.Graph
  ( Absent
  , Graph
  , Node (..)
  , Workflow (..)
  , branch
  , choose
  , embed
  , expand
  , forEach
  , step
  )
import Arbiter.Workflow.Graph qualified as Graph
import Arbiter.Workflow.Interpret
  ( Materialized (..)
  , Rendered (..)
  , materialize
  , nodeInput
  , render
  )
import Arbiter.Workflow.Registry (SomeWorkflow (..), WorkflowRegistry, lookupWorkflow, workflow, workflows)
import Arbiter.Workflow.Types (StepKind (..), StepName (..))

newtype Alpha = Alpha Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype Beta = Beta Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type TestRegistry =
  '[ QueueWithResult "wf_alpha" Alpha Text
   , QueueWithResult "wf_beta" Beta Text
   ]

alpha
  :: forall s (used :: [Symbol])
   . forall (name :: Symbol)
  ->(Absent name used, KnownSymbol name)
  => Expr s Text
  -> Graph s TestRegistry used (name : used) (Ref s Text)
alpha name = step name Alpha

beta
  :: forall s (used :: [Symbol])
   . forall (name :: Symbol)
  ->(Absent name used, KnownSymbol name)
  => Expr s Text
  -> Graph s TestRegistry used (name : used) (Ref s Text)
beta name = step name Beta

-- | Two steps in a line.
linear :: Workflow TestRegistry Text Text
linear = Workflow "linear" 1 $ \source -> Graph.do
  first <- alpha "first" (use source)
  beta "second" ((<>) <$> use source <*> use first)

-- | A definition registered under the name and version 'linear' takes.
shadowed :: Workflow TestRegistry Text Text
shadowed = Workflow "linear" 1 $ \source -> alpha "instead" (use source)

-- | A branch on the run input.
branching :: Workflow TestRegistry (Either Text Text) Text
branching = Workflow "branching" 1 $ \source ->
  branch
    (use source)
    (\left -> alpha "on-left" (use left))
    (\right -> beta "on-right" (use right))

-- | One step per element of a runtime list.
mapping :: Workflow TestRegistry [Text] [Text]
mapping = Workflow "mapping" 1 $ \source ->
  forEach (use source) (\each -> alpha "each" (use each))

-- | A definition worth reusing.
pair :: Workflow TestRegistry Text Text
pair = Workflow "pair" 1 $ \source -> Graph.do
  one <- alpha "one" (use source)
  beta "two" (use one)

-- | The same definition twice, each under a name of its own.
composed :: Workflow TestRegistry Text Text
composed = Workflow "composed" 1 $ \source -> Graph.do
  first <- embed "first" pair (use source)
  embed "second" pair (use first)

-- | A boolean choice with nothing carried into its arms.
choosing :: Workflow TestRegistry Bool Text
choosing = Workflow "choosing" 1 $ \source ->
  choose (use source) (alpha "yes" (pure "yes")) (beta "no" (pure "no"))

-- | What the merge of a materialized fan-out settles to.
mergeOutput :: Materialized -> Either Text Value
mergeOutput built =
  case [node | node <- materializedNodes built, nodeKind node == KindMerge] of
    (merge : _) -> maybe (Left "the merge computes nothing") ($ knownOutputs built) (nodeCompute merge)
    [] -> Left "no merge"
  where
    knownOutputs materialized =
      Map.fromList [(nodeName node, output) | node <- materializedNodes materialized, Just output <- [nodeOutput node]]

-- | A branch inside each child of a fan-out.
nested :: Workflow TestRegistry [Text] [Text]
nested = Workflow "nested" 1 $ \source ->
  forEach (use source) $ \item ->
    branch
      (toEither <$> use item)
      (\left -> alpha "inner" (use left))
      (\right -> beta "other" (use right))

toEither :: Text -> Either Text Text
toEither value = if value == "one" then Left value else Right value

-- | The arms the two children of 'nested' decide.
nestedArms :: Map StepName Value
nestedArms =
  Map.fromList
    [ (StepName "expand@0.0.branch@0", toJSON (Left "one" :: Either Text Text))
    , (StepName "expand@0.1.branch@0", toJSON (Right "two" :: Either Text Text))
    ]

-- | A continuation whose subgraph depends on the value.
continuing :: Workflow TestRegistry Text Text
continuing = Workflow "continuing" 1 $ \source ->
  expand (use source) (\value -> alpha "chosen" (pure value))

spec :: Spec
spec = describe "workflow builder" $ do
  describe "Expr" $ do
    it "collects the refs of a step's input, in order" $
      refsOf twoRefs `shouldBe` [StepName "a", StepName "b"]

    it "folds an expression over the stored outputs" $
      evalExpr (Map.fromList [(StepName "a", String "x"), (StepName "b", String "y")]) twoRefs
        `shouldBe` Right ("x", "y")

    it "reports a ref with no stored output" $
      evalExpr (Map.fromList [(StepName "a", String "x")]) twoRefs `shouldSatisfy` isLeft

    it "builds an input from several refs in do-notation" $
      evalExpr (Map.fromList [(StepName "a", String "x"), (StepName "b", String "y")]) joined
        `shouldBe` Right "x/y"

    it "reads two refs as one input" $
      evalExpr (Map.fromList [(StepName "a", String "x"), (StepName "b", String "y")]) twoBoth
        `shouldBe` Right ("x", "y")

    it "reports a stored output that does not decode" $
      evalExpr (Map.fromList [(StepName "a", Number 1), (StepName "b", String "y")]) twoRefs
        `shouldSatisfy` isLeft

  describe "materialize" $ do
    it "names every step by its path and gives each one its queue" $ do
      built <- built' linear (String "seed") mempty
      names built `shouldBe` ["input", "first", "second"]
      map nodeQueue (materializedNodes built) `shouldBe` [Nothing, Just "wf_alpha", Just "wf_beta"]

    it "makes a step wait on every ref of its input" $ do
      built <- built' linear (String "seed") mempty
      edges built
        `shouldBe` [("input", "first"), ("input", "second"), ("first", "second")]

    it "builds the job of a step whose input is known, and none of the others" $ do
      built <- built' linear (String "seed") mempty
      let stored = Map.singleton (StepName "input") (String "seed")
      map (isJust . nodeInput stored) (materializedNodes built) `shouldBe` [False, True, False]

    it "stops at a branch whose arm is not decided yet" $ do
      built <- built' branching leftInput mempty
      names built `shouldBe` ["input", "branch@0"]
      materializedComplete built `shouldBe` False

    it "follows the arm the branch stored" $ do
      built <- built' branching leftInput (decided leftInput)
      names built `shouldBe` ["input", "branch@0", "branch@0.left", "branch@0.left.on-left"]
      materializedComplete built `shouldBe` True
      materializedOutput built `shouldBe` Just (StepName "branch@0.left.on-left")

    it "follows the other arm when that is the one stored" $ do
      built <- built' branching rightInput (decided rightInput)
      names built `shouldBe` ["input", "branch@0", "branch@0.right", "branch@0.right.on-right"]

    it "holds the arm's own value, so the arm's steps read it directly" $ do
      built <- built' branching leftInput (decided leftInput)
      map nodeOutput (materializedNodes built)
        `shouldBe` [Just leftInput, Nothing, Just (String "l"), Nothing]

    it "gives one child per element and a merge that waits on them all" $ do
      built <- built' mapping listInput (listed listInput)
      names built
        `shouldBe` [ "input"
                   , "expand@0"
                   , "expand@0.0"
                   , "expand@0.0.each"
                   , "expand@0.1"
                   , "expand@0.1.each"
                   , "expand@0.merge"
                   ]
      map nodeKind (materializedNodes built)
        `shouldBe` [KindInput, KindExpand, KindExpand, KindJob, KindExpand, KindJob, KindMerge]
      [(from, to) | (from, to) <- edges built, to == "expand@0.merge"]
        `shouldBe` [("expand@0.0.each", "expand@0.merge"), ("expand@0.1.each", "expand@0.merge")]

    it "carries each child's position, so the merge can order its output" $ do
      built <- built' mapping listInput (listed listInput)
      map nodeIndex (materializedNodes built)
        `shouldBe` [Nothing, Nothing, Just 0, Nothing, Just 1, Nothing, Nothing]

    it "runs a continuation's function with the value it stored" $ do
      built <- built' continuing (String "seed") (Map.fromList [(StepName "expand@0", String "seed")])
      names built `shouldBe` ["input", "expand@0", "expand@0.chosen"]

    it "names a branch inside a forEach child by its full path" $ do
      built <- built' nested listInput (listed listInput <> nestedArms)
      names built
        `shouldBe` [ "input"
                   , "expand@0"
                   , "expand@0.0"
                   , "expand@0.0.branch@0"
                   , "expand@0.0.branch@0.left"
                   , "expand@0.0.branch@0.left.inner"
                   , "expand@0.1"
                   , "expand@0.1.branch@0"
                   , "expand@0.1.branch@0.right"
                   , "expand@0.1.branch@0.right.other"
                   , "expand@0.merge"
                   ]

    it "stops at the first child whose branch is undecided" $ do
      built <- built' nested listInput (listed listInput)
      names built `shouldBe` ["input", "expand@0", "expand@0.0", "expand@0.0.branch@0"]
      materializedComplete built `shouldBe` False

    it "composes one definition's graph into another" $ do
      built <- built' composed (String "seed") mempty
      names built
        `shouldBe` ["input", "first", "first.one", "first.two", "second", "second.one", "second.two"]
      edges built
        `shouldBe` [ ("input", "first")
                   , ("first", "first.one")
                   , ("first.one", "first.two")
                   , ("first.two", "second")
                   , ("second", "second.one")
                   , ("second.one", "second.two")
                   ]

    it "takes the arm a boolean choice decided" $ do
      built <- built' choosing (toJSON True) (Map.fromList [(StepName "branch@0", toJSON (Left () :: Either () ()))])
      names built `shouldBe` ["input", "branch@0", "branch@0.left", "branch@0.left.yes"]

    it "merges an empty fan-out to nothing at all" $ do
      let empty = toJSON ([] :: [Text])
      built <- built' mapping empty (listed empty)
      names built `shouldBe` ["input", "expand@0", "expand@0.merge"]
      mergeOutput built `shouldBe` Right (toJSON ([] :: [Text]))

    it "is the same graph however often it runs" $ do
      first <- built' branching leftInput (decided leftInput)
      again <- built' branching leftInput (decided leftInput)
      names first `shouldBe` names again
      edges first `shouldBe` edges again

    it "only ever adds steps as more virtual outputs land" $
      forAll (sublistOf (Map.toList (listed listInput))) monotone

    it "reports a stored decision that does not decode" $
      isLeft (materialize branching leftInput (Map.fromList [(StepName "branch@0", Number 1)]))
        `shouldBe` True

  describe "registry" $ do
    it "keeps the last of a repeated name and version, however it is collected" $ do
      collected <- registeredNames (workflows [workflow linear, workflow shadowed])
      joined' <- registeredNames (workflows [workflow linear] <> workflows [workflow shadowed])
      collected `shouldBe` ["input", "instead"]
      joined' `shouldBe` collected

  describe "render" $ do
    it "shows both arms of a branch, without a run" $ do
      rendered <- either (fail . T.unpack) pure (render branching)
      map (stepNameText . fst) (renderedNodes rendered)
        `shouldBe` ["input", "branch@0", "branch@0.left", "branch@0.left.on-left", "branch@0.right", "branch@0.right.on-right"]

    it "shows one opaque node for a continuation, and its merge" $ do
      rendered <- either (fail . T.unpack) pure (render mapping)
      map (stepNameText . fst) (renderedNodes rendered) `shouldBe` ["input", "expand@0", "expand@0.merge"]
      map (bimap stepNameText stepNameText) (renderedEdges rendered)
        `shouldBe` [("input", "expand@0"), ("expand@0", "expand@0.merge")]

-- | The step names of the definition a registry resolves @linear@ at version 1 to.
registeredNames :: WorkflowRegistry TestRegistry -> IO [Text]
registeredNames registry = case lookupWorkflow registry "linear" 1 of
  Nothing -> fail "no definition is registered under linear"
  Just (SomeWorkflow definition) ->
    map (stepNameText . fst) . renderedNodes <$> either (fail . T.unpack) pure (render definition)

-- | An input over two refs, written applicatively. An 'Expr' has no @Monad@, so a
-- block that reads a value to decide what to read next does not compile.
joined :: Expr s Text
joined = do
  left <- use (toRef (StepName "a"))
  right <- use (toRef (StepName "b"))
  pure (left <> "/" <> right)

twoBoth :: Expr s (Text, Text)
twoBoth = both (toRef (StepName "a")) (toRef (StepName "b"))

twoRefs :: Expr s (Text, Text)
twoRefs = (,) <$> use (toRef (StepName "a")) <*> use (toRef (StepName "b"))

leftInput :: Value
leftInput = toJSON (Left "l" :: Either Text Text)

rightInput :: Value
rightInput = toJSON (Right "r" :: Either Text Text)

listInput :: Value
listInput = toJSON ["one" :: Text, "two"]

decided :: Value -> Map StepName Value
decided value = Map.fromList [(StepName "branch@0", value)]

listed :: Value -> Map StepName Value
listed value = Map.fromList [(StepName "expand@0", value)]

built' :: Workflow registry input output -> Value -> Map StepName Value -> IO Materialized
built' definition input stored = either (fail . T.unpack) pure (materialize definition input stored)

names :: Materialized -> [Text]
names = map (stepNameText . nodeName) . materializedNodes

edges :: Materialized -> [(Text, Text)]
edges built =
  [(stepNameText from, stepNameText (nodeName node)) | node <- materializedNodes built, from <- nodeAfter node]

-- | More stored outputs never take a step away.
monotone :: [(StepName, Value)] -> Property
monotone stored = fewer === take (length fewer) more
  where
    fewer = namesWith (Map.fromList stored)
    more = namesWith (listed listInput)
    namesWith outputs = either (const []) names (materialize mapping listInput outputs)

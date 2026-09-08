{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QualifiedDo #-}
{-# LANGUAGE TypeFamilies #-}

-- | A run driven end to end by real worker pools over three queues.
module Test.Arbiter.Workflow.EndToEnd (spec) where

import Arbiter.Core.Codec (Col (..), col, pval)
import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.TraceContext (TraceContext (..))
import Arbiter.Core.Job.Types
  ( JobRead
  , claimedBy
  , defaultJob
  , payload
  , primaryKey
  , queueName
  , setGroupKey
  , setMaxAttempts
  , traceContext
  )
import Arbiter.Core.MonadArbiter (JobHandler, getSchema)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Simple (SimpleDb, SimpleEnv, runSimpleDb)
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Test.Setup (createSharedPool, execQuery)
import Arbiter.Worker (childResults, runWorkerPool)
import Arbiter.Worker.Config
  ( BatchCallbacks
  , WorkerConfig (..)
  , ackAllWith
  , defaultBatchedWorkerConfig
  , transactionalWorkerConfig
  )
import Arbiter.Worker.Logger (silentLogConfig)
import Control.Concurrent.MVar (MVar, newEmptyMVar, putMVar, takeMVar)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON, Value (String), object, toJSON, (.=))
import Data.Aeson.Key (Key)
import Data.ByteString (ByteString)
import Data.Either (isLeft, rights)
import Data.Foldable (toList, traverse_)
import Data.IORef (IORef, atomicModifyIORef', newIORef)
import Data.Int (Int64)
import Data.List (find, sort)
import Data.List.NonEmpty (NonEmpty)
import Data.Map.Strict qualified as Map
import Data.Maybe (isJust)
import Data.Pool (Pool)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Test.Hspec
import Text.Read (readMaybe)

import Arbiter.Workflow qualified as WF
import Arbiter.Workflow.Expr (use)
import Arbiter.Workflow.Graph (Workflow (..), branch, embed, expand, forEach, signal, step, stepWith)
import Arbiter.Workflow.Graph qualified as Graph
import Arbiter.Workflow.Registry (WorkflowRegistry, workflow, workflows)
import Arbiter.Workflow.Types (RunId (..), RunStatus (..), StepName (..), StepStatus (..))
import Arbiter.Workflow.Worker
  ( startWorkflow
  , startWorkflowByName
  , startWorkflowWith
  , withWorkflows
  )
import Test.Arbiter.Workflow.Harness (runHasStatus, setupWorkflowSchema, withCleanWorkflowRun)

newtype Alpha = Alpha Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype Beta = Beta Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype Gamma = Gamma Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type Reg =
  '[ QueueWithResult "wfe_alpha" Alpha Text
   , QueueWithResult "wfe_beta" Beta Text
   , QueueWithResult "wfe_gamma" Gamma Text
   ]

testSchema :: Text
testSchema = "arbiter_workflow_e2e"

alphaQueue :: Text
alphaQueue = "wfe_alpha"

betaQueue :: Text
betaQueue = "wfe_beta"

queueNames :: [Text]
queueNames = [alphaQueue, betaQueue, "wfe_gamma"]

-- | Input, then one step in each of the three queues.
pipeline :: Workflow Reg Text Text
pipeline = Workflow "pipeline" 1 $ \source -> Graph.do
  extracted <- step "extract" Alpha (use source)
  shaped <- step "shape" Beta (use extracted)
  step "load" Gamma (use shaped)

-- | A later version of 'pipeline', one step shorter.
pipelineV2 :: Workflow Reg Text Text
pipelineV2 = Workflow "pipeline" 2 $ \source -> Graph.do
  extracted <- step "extract" Alpha (use source)
  step "shape" Beta (use extracted)

-- | One step per element, then one step over the merged list.
scatter :: Workflow Reg [Text] Text
scatter = Workflow "scatter" 1 $ \source -> Graph.do
  each <- forEach (use source) (\item -> step "part" Alpha (use item))
  step "join" Beta (T.intercalate "+" <$> use each)

-- | One queue or the other, decided by the run input.
deciding :: Workflow Reg (Either Text Text) Text
deciding = Workflow "deciding" 1 $ \source ->
  branch
    (use source)
    (\left -> step "cheap" Beta (use left))
    (\right -> step "dear" Gamma (use right))

-- | A step whose handler always throws, with one attempt.
failing :: Workflow Reg Text Text
failing = Workflow "failing" 1 $ \source ->
  stepWith "explode" (\value -> setMaxAttempts (Just 1) (defaultJob (Alpha ("boom" <> value)))) (use source)

-- | 'scatter' with its children in one group, so a batched pool claims them together.
grouped :: Workflow Reg [Text] Text
grouped = Workflow "grouped" 1 $ \source -> Graph.do
  each <- forEach (use source) (\item -> stepWith "part" (setGroupKey (Just "batch") . defaultJob . Alpha) (use item))
  step "join" Beta (T.intercalate "+" <$> use each)

-- | A run that starts another and waits on the signal it sends back.
delegating :: Workflow Reg Text Text
delegating = Workflow "delegating" 1 $ \source -> Graph.do
  kicked <- step "kick" Alpha (("kick:" <>) <$> use source)
  answered <- signal "child-done" 3600
  step "finish" Beta ((\left right -> left <> "+" <> right) <$> use kicked <*> use answered)

-- | The run 'delegating' starts. Its last step signals the run that asked for it.
childOf :: Workflow Reg Text Text
childOf = Workflow "child-of" 1 $ \source ->
  step "report" Gamma (("child:" <>) <$> use source)

-- | The runs started under one definition.
runsOf :: Text -> SimpleDb Reg IO [RunId]
runsOf name =
  map RunId
    <$> execQuery
      ("SELECT id FROM " <> WF.workflowRunsTable testSchema <> " WHERE workflow = ?")
      [pval CText name]
      (col "id" CInt8)

-- | A fragment worth reusing, and a definition that embeds it twice.
oneRound :: Workflow Reg Text Text
oneRound = Workflow "one-round" 1 $ \source -> Graph.do
  extracted <- step "extract" Alpha (use source)
  step "shape" Beta (use extracted)

twice :: Workflow Reg Text Text
twice = Workflow "twice" 1 $ \source -> Graph.do
  first <- embed "first" oneRound (use source)
  embed "second" oneRound (use first)

-- | Five continuations, one inside the next, all settling before any job runs.
deep :: Workflow Reg Text Text
deep = Workflow "deep" 1 $ \source ->
  expand (use source) $ \one ->
    expand (pure (one <> ".1")) $ \two ->
      expand (pure (two <> ".2")) $ \three ->
        expand (pure (three <> ".3")) $ \four ->
          expand (pure (four <> ".4")) $ \five ->
            step "leaf" Alpha (pure (five <> ".5"))

-- | Where 'deep' puts its one job step.
depthPath :: Key
depthPath = "expand@0.expand@0.expand@0.expand@0.expand@0.leaf"

-- | A step whose handler fans its own job out into a tree, then folds it back.
rolling :: Workflow Reg Text Text
rolling = Workflow "rolling" 1 $ \source -> Graph.do
  fanned <- step "fan" (const (Alpha "fan")) (use source)
  step "after" Beta (use fanned)

-- | A signal the run waits on before its last step.
approving :: Workflow Reg Text Text
approving = Workflow "approving" 1 $ \source -> Graph.do
  approved <- signal "approval" 3600
  step "after" Beta ((<>) <$> use source <*> use approved)

-- | A signal whose deadline has already passed when the run starts.
waiting :: Workflow Reg Text Text
waiting = Workflow "waiting" 1 $ \source -> Graph.do
  approved <- signal "approval" 0
  step "after" Beta ((<>) <$> use source <*> use approved)

-- | A signal that only a continuation reveals, its deadline already past.
gated :: Workflow Reg Text Text
gated = Workflow "gated" 1 $ \source -> Graph.do
  decided <- step "decide" Alpha (use source)
  expand (use decided) $ \value -> Graph.do
    approved <- signal "approval" 0
    step "after" Beta ((value <>) <$> use approved)

-- | A fragment whose first step reads nothing.
constant :: Workflow Reg Text Text
constant = Workflow "constant" 1 $ \_source -> step "fixed" (const (Alpha "fixed")) (pure ())

-- | 'constant' embedded, so its first step waits on the embed.
embedding :: Workflow Reg Text Text
embedding = Workflow "embedding" 1 $ \source -> Graph.do
  inner <- embed "frag" constant (use source)
  step "after" Beta (use inner)

-- | A step whose handler waits to be let go.
holding :: Workflow Reg Text Text
holding = Workflow "holding" 1 $ \source -> Graph.do
  held <- step "wait" Alpha (use source)
  step "after" Beta (use held)

definitions :: WorkflowRegistry Reg
definitions =
  workflows
    [ workflow pipeline
    , workflow pipelineV2
    , workflow scatter
    , workflow deciding
    , workflow failing
    , workflow grouped
    , workflow holding
    , workflow waiting
    , workflow gated
    , workflow embedding
    , workflow rolling
    , workflow approving
    , workflow deep
    , workflow twice
    , workflow delegating
    , workflow childOf
    ]

spec :: ByteString -> Spec
spec connStr = beforeAll (setupSchema connStr) $ do
  sharedPool <- runIO (createSharedPool connStr)
  around (withCleanRun sharedPool) $ do
    it "carries a run through three queues and finishes it" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions pipeline "seed")
        waitUntil 20_000 (runIsDone env runId)

        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunDone
        WF.runRowOutput runRow `shouldBe` Just (object ["load" .= String "seed/a/b/c"])

        steps <- run env (WF.listSteps runId)
        outputOf steps "extract" `shouldBe` Just (String "seed/a")
        outputOf steps "shape" `shouldBe` Just (String "seed/a/b")

    it "fans a run out over a list and merges what the children returned" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions scatter ["one", "two", "three"])
        waitUntil 20_000 (runIsDone env runId)

        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunDone
        WF.runRowOutput runRow
          `shouldBe` Just (object ["join" .= String "one/a+two/a+three/a/b"])

        steps <- run env (WF.listSteps runId)
        outputOf steps "expand@0.merge"
          `shouldBe` Just (toJSON ["one/a" :: Text, "two/a", "three/a"])

    it "fails the run when a step exhausts its attempts" $ \env -> do
      handlers <- newHandlers 10
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions failing "seed")
        waitUntil 20_000 (runHasStatus env runId RunFailed)

        steps <- run env (WF.listSteps runId)
        map WF.stepRowStatus steps `shouldBe` [StepDone, StepFailed]

    it "carries a revived run to the end after its dead step is retried" $ \env -> do
      handlers <- newHandlers 1
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions failing "seed")
        waitUntil 20_000 (runHasStatus env runId RunFailed)

        [dead] <- run env (deadLetters alphaQueue)
        void $ run env (Ops.retryFromDLQ @_ @Alpha testSchema alphaQueue (DLQ.dlqPrimaryKey dead))
        run env WF.reviveRetriedRuns `shouldReturn` 1

        waitUntil 20_000 (runHasStatus env runId RunDone)
        Just runRow <- run env (WF.getRun runId)
        WF.runRowOutput runRow `shouldBe` Just (object ["explode" .= String "boomseed/a"])

    it "finishes a fan-out over an empty list" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions scatter [])
        waitUntil 20_000 (runIsDone env runId)

        steps <- run env (WF.listSteps runId)
        outputOf steps "expand@0.merge" `shouldBe` Just (toJSON ([] :: [Text]))
        Just runRow <- run env (WF.getRun runId)
        WF.runRowOutput runRow `shouldBe` Just (object ["join" .= String "/b"])

    it "keeps runs of one definition apart" $ \env -> do
      handlers <- newHandlers 0
      withPoolsAt 8 env handlers $ do
        started <- traverse (\seed -> run env (startWorkflow definitions pipeline seed)) crowd
        let runIds = rights started
        length runIds `shouldBe` length crowd
        traverse_ (\runId -> waitUntil 20_000 (runIsDone env runId)) runIds

        outputs <- traverse (\runId -> fmap (WF.runRowOutput =<<) (run env (WF.getRun runId))) runIds
        outputs `shouldBe` [Just (object ["load" .= String (seed <> "/a/b/c")]) | seed <- crowd]

    it "settles the same job once, however often the settle runs" $ \env -> do
      Right runId <- run env (startWorkflow definitions pipeline "seed")
      [alpha] <- run env (claimFrom alphaQueue)
      let settle = WF.settleWorkflowJob definitions alphaQueue (primaryKey alpha) (Just (String "seed/a"))
      void $ run env settle
      void $ run env settle

      inserted <- run env (jobsIn betaQueue)
      length inserted `shouldBe` 1
      steps <- run env (WF.listSteps runId)
      outputOf steps "extract" `shouldBe` Just (String "seed/a")

    it "advances a run when its signal arrives" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions approving "seed")
        Right _ <- run env (WF.sendWorkflowSignal definitions runId "approval" (String "-ok"))
        waitUntil 20_000 (runIsDone env runId)

        Just runRow <- run env (WF.getRun runId)
        WF.runRowOutput runRow `shouldBe` Just (object ["after" .= String "seed-ok/b"])

    it "lets a step start a run of its own and wait for its answer" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions delegating "seed")
        waitUntil 20_000 (runIsDone env runId)

        Just runRow <- run env (WF.getRun runId)
        WF.runRowOutput runRow `shouldBe` Just (object ["finish" .= String "kicked+child done/b"])

        children <- run env (runsOf "child-of")
        length children `shouldBe` 1
        traverse_ (\child -> waitUntil 20_000 (runHasStatus env child RunDone)) children

    it "carries a run through the same fragment embedded twice" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions twice "seed")
        waitUntil 20_000 (runIsDone env runId)

        steps <- run env (WF.listSteps runId)
        map WF.stepRowName steps
          `shouldBe` map
            StepName
            ["input", "first", "first.extract", "first.shape", "second", "second.extract", "second.shape"]
        Just runRow <- run env (WF.getRun runId)
        WF.runRowOutput runRow `shouldBe` Just (object ["second.shape" .= String "seed/a/b/a/b"])

    it "settles a stack of nested continuations in one pass" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions deep "seed")
        waitUntil 20_000 (runIsDone env runId)

        steps <- run env (WF.listSteps runId)
        length steps `shouldBe` 7
        Just runRow <- run env (WF.getRun runId)
        WF.runRowOutput runRow
          `shouldBe` Just (object [depthPath .= String "seed.1.2.3.4.5/a"])

    it "settles a step whose job fanned out into a tree of its own" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions rolling "seed")
        waitUntil 20_000 (runIsDone env runId)

        steps <- run env (WF.listSteps runId)
        outputOf steps "fan" `shouldBe` Just (String "leaf1/a+leaf2/a")
        Just runRow <- run env (WF.getRun runId)
        WF.runRowOutput runRow `shouldBe` Just (object ["after" .= String "leaf1/a+leaf2/a/b"])

    it "merges children that run at the same time" $ \env -> do
      handlers <- newHandlers 0
      withPoolsAt 4 env handlers $ do
        Right runId <- run env (startWorkflow definitions scatter manyParts)
        waitUntil 20_000 (runIsDone env runId)

        steps <- run env (WF.listSteps runId)
        outputOf steps "expand@0.merge" `shouldBe` Just (toJSON (map (<> "/a") manyParts))

    it "settles a batch of children acked in one call" $ \env -> do
      handlers <- newHandlers 0
      withBatchedPools env handlers $ do
        Right runId <- run env (startWorkflow definitions grouped manyParts)
        waitUntil 20_000 (runIsDone env runId)

        steps <- run env (WF.listSteps runId)
        outputOf steps "expand@0.merge" `shouldBe` Just (toJSON (map (<> "/a") manyParts))

    it "stops a run cancelled while a step is in flight" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions holding "hold")
        waitUntil 20_000 (anyClaimed env alphaQueue)

        run env (WF.cancelRun runId) `shouldReturn` True
        putMVar (holdGate handlers) ()
        waitUntil 20_000 (null <$> run env (jobsIn alphaQueue))

        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunCancelled
        run env (jobsIn betaQueue) `shouldReturn` []

    it "sweeps a stale signal from the pool's own maintenance" $ \env -> do
      Right runId <- run env (startWorkflow definitions waiting "seed")
      withSweeper env $ waitUntil 20_000 (runHasStatus env runId RunFailed)

      steps <- run env (WF.listSteps runId)
      map WF.stepRowStatus steps `shouldBe` [StepDone, StepFailed, StepWaiting]

    it "expires a signal that a continuation revealed" $ \env -> do
      Right runId <- run env (startWorkflow definitions gated "seed")
      [alpha] <- run env (claimFrom alphaQueue)
      void $ run env (WF.settleWorkflowJob definitions alphaQueue (primaryKey alpha) (Just (String "decided")))

      withSweeper env $ waitUntil 20_000 (runHasStatus env runId RunFailed)

      steps <- run env (WF.listSteps runId)
      map WF.stepRowStatus steps `shouldBe` [StepDone, StepDone, StepDone, StepFailed, StepWaiting]

    it "gives a job to a step that waits though its input reads nothing" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions embedding "seed")
        waitUntil 20_000 (runIsDone env runId)

        Just runRow <- run env (WF.getRun runId)
        WF.runRowOutput runRow `shouldBe` Just (object ["after" .= String "fixed/a/b"])

    it "carries the run's span into the jobs the settle inserts" $ \env -> do
      Right runId <- run env (startWorkflowWith (Just runSpan) definitions pipeline "seed")

      started <- run env (jobsIn alphaQueue)
      map traceContext started `shouldBe` [Just runSpan]

      [alpha] <- run env (claimFrom alphaQueue)
      void $ run env (WF.settleWorkflowJob definitions alphaQueue (primaryKey alpha) (Just (String "seed/a")))

      inserted <- run env (jobsIn betaQueue)
      map traceContext inserted `shouldBe` [Just runSpan]
      Just runRow <- run env (WF.getRun runId)
      WF.runRowTraceparent runRow `shouldBe` Just (traceparent runSpan)

    it "fails the run it started when the first decision cannot read its input" $ \env -> do
      outcome <- run env (startWorkflowByName definitions "deciding" Nothing (String "not-an-either"))
      outcome `shouldSatisfy` isLeft

      run env countRuns `shouldReturn` 1
      statuses <- run env runStatuses
      statuses `shouldBe` [RunFailed]

    it "keeps a run on the version it started under" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right older <- run env (startWorkflowByName definitions "pipeline" (Just 1) (String "v1"))
        Right newest <- run env (startWorkflowByName definitions "pipeline" Nothing (String "latest"))
        traverse_ (\runId -> waitUntil 20_000 (runIsDone env runId)) [older, newest]

        oldSteps <- run env (WF.listSteps older)
        map WF.stepRowName oldSteps `shouldBe` map StepName ["input", "extract", "shape", "load"]

        newSteps <- run env (WF.listSteps newest)
        map WF.stepRowName newSteps `shouldBe` map StepName ["input", "extract", "shape"]

    it "fails a run whose definition is no longer registered" $ \env -> do
      Right runId <- run env (startWorkflow definitions pipeline "seed")
      [alpha] <- run env (claimFrom alphaQueue)
      outcome <- run env (WF.settleWorkflowJob forgotten alphaQueue (primaryKey alpha) (Just (String "seed/a")))
      outcome `shouldSatisfy` isLeft

      Just runRow <- run env (WF.getRun runId)
      WF.runRowStatus runRow `shouldBe` RunFailed

    it "materializes only the arm its condition decided" $ \env -> do
      handlers <- newHandlers 0
      withPools env handlers $ do
        Right runId <- run env (startWorkflow definitions deciding (Left "cheap-path"))
        waitUntil 20_000 (runIsDone env runId)

        Just runRow <- run env (WF.getRun runId)
        WF.runRowOutput runRow `shouldBe` Just (object ["branch@0.left.cheap" .= String "cheap-path/b"])

        steps <- run env (WF.listSteps runId)
        map WF.stepRowName steps
          `shouldBe` map StepName ["input", "branch@0", "branch@0.left", "branch@0.left.cheap"]

manyParts :: [Text]
manyParts = ["p" <> T.pack (show index) | index <- [1 .. 6 :: Int]]

-- | Enough runs at once to crowd the window between a run's insert and its first advance.
crowd :: [Text]
crowd = ["c" <> T.pack (show index) | index <- [1 .. 16 :: Int]]

anyClaimed :: SimpleEnv Reg -> Text -> IO Bool
anyClaimed env queue = any (isJust . claimedBy) <$> run env (jobsIn queue)

runStatuses :: SimpleDb Reg IO [RunStatus]
runStatuses = do
  stored <- execQuery ("SELECT status FROM " <> WF.workflowRunsTable testSchema) [] (col "status" CText)
  pure (rights (map WF.runStatusFromText stored))

countRuns :: SimpleDb Reg IO Int64
countRuns =
  sum <$> execQuery ("SELECT count(*) FROM " <> WF.workflowRunsTable testSchema) [] (col "count" CInt8)

runSpan :: TraceContext
runSpan = TraceContext "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01" Nothing

-- | A registry that knows none of the definitions the runs started under.
forgotten :: WorkflowRegistry Reg
forgotten = workflows []

jobsIn :: Text -> SimpleDb Reg IO [JobRead Alpha]
jobsIn queue = Ops.listJobs testSchema queue 10 0

claimFrom :: Text -> SimpleDb Reg IO [JobRead Alpha]
claimFrom queue = Ops.claimNextVisibleJobs testSchema queue 1 60

-- | What the alpha handler does beyond its own work: fail a few times, and hold.
data Handlers = Handlers
  { failBudget :: IORef Int
  , holdGate :: MVar ()
  }

newHandlers :: Int -> IO Handlers
newHandlers budget = Handlers <$> newIORef budget <*> newEmptyMVar

-- | A pool for each queue, running for the body.
withPools :: SimpleEnv Reg -> Handlers -> IO a -> IO a
withPools = withPoolsAt 1

-- | 'withPools' with several threads on the alpha queue, so its steps run at once.
withPoolsAt :: Int -> SimpleEnv Reg -> Handlers -> IO a -> IO a
withPoolsAt workers env handlers body = do
  alphaConfig <- transactionalWorkerConfig 10 (alphaHandler handlers)
  betaConfig <- transactionalWorkerConfig 10 betaHandler
  gammaConfig <- transactionalWorkerConfig 10 gammaHandler
  withLinkedAsync (run env (runWorkerPool (tuned workers alphaConfig))) $ \_ ->
    withLinkedAsync (run env (runWorkerPool (tuned 1 betaConfig))) $ \_ ->
      withLinkedAsync (run env (runWorkerPool (tuned 1 gammaConfig))) $ \_ -> body

-- | One pool whose maintenance runs the workflow sweep, on a short cadence.
withSweeper :: SimpleEnv Reg -> IO a -> IO a
withSweeper env body = do
  config <- transactionalWorkerConfig 10 betaHandler
  let paced = config {reaperInterval = 0.2, reaperSparseInterval = 0.2}
  withLinkedAsync (run env (runWorkerPool (tuned 1 paced))) (const body)

-- | The alpha queue claimed in batches, and one beta pool beside it.
withBatchedPools :: SimpleEnv Reg -> Handlers -> IO a -> IO a
withBatchedPools env handlers body = do
  alphaConfig <- defaultBatchedWorkerConfig 10 10 (batchedAlpha handlers)
  betaConfig <- transactionalWorkerConfig 10 betaHandler
  withLinkedAsync (run env (runWorkerPool (tuned 1 alphaConfig))) $ \_ ->
    withLinkedAsync (run env (runWorkerPool (tuned 1 betaConfig))) $ \_ -> body

tuned :: (ToJSON p) => Int -> WorkerConfig (SimpleDb Reg IO) p -> WorkerConfig (SimpleDb Reg IO) p
tuned workers config =
  withWorkflows
    definitions
    config {workerCount = workers, pollInterval = 0.05, logConfig = silentLogConfig}

alphaHandler :: Handlers -> JobHandler (SimpleDb Reg IO) Alpha Text
alphaHandler handlers _conn job = alphaWork handlers job

batchedAlpha
  :: Handlers
  -> NonEmpty (JobRead Alpha)
  -> BatchCallbacks (SimpleDb Reg IO) Alpha Text
  -> SimpleDb Reg IO ()
batchedAlpha handlers jobs callbacks =
  ackAllWith callbacks =<< traverse (\job -> (job,) <$> alphaWork handlers job) (toList jobs)

alphaWork :: Handlers -> JobRead Alpha -> SimpleDb Reg IO Text
alphaWork handlers job = do
  let Alpha value = payload job
  remaining <- liftIO (atomicModifyIORef' (failBudget handlers) (\left -> (max 0 (left - 1), left)))
  liftIO (when (T.isPrefixOf "hold" value) (takeMVar (holdGate handlers)))
  case () of
    ()
      | T.isPrefixOf "boom" value && remaining > 0 -> throwRetryable value
      | value == "fan" -> fanOut job
      | T.isPrefixOf "kick:" value -> kickOff job
      | otherwise -> pure (value <> "/a")

-- | Start a run of another definition, telling it which run to answer.
kickOff :: JobRead Alpha -> SimpleDb Reg IO Text
kickOff job = do
  mine <- WF.runOfJob (queueName job) (primaryKey job)
  case mine of
    Nothing -> throwRetryable "this job belongs to no run"
    Just parent -> do
      started <- WF.startWorkflow definitions childOf (T.pack (show parent))
      either throwRetryable (const (pure "kicked")) started

-- | The first round spawns two children under this job. The ack then suspends it, and
-- the round after the children finish folds their results.
fanOut :: JobRead Alpha -> SimpleDb Reg IO Text
fanOut job = do
  (results, _) <- childResults job
  if Map.null results
    then do
      schemaName <- getSchema
      stamp <- Ops.traceStamp
      leaves <-
        Ops.insertJobTreeLeavesStamped
          schemaName
          alphaQueue
          stamp
          (primaryKey job)
          [defaultJob (Alpha "leaf1"), defaultJob (Alpha "leaf2")]
      "spawned" <$ when (length leaves /= 2) (throwRetryable "the fan-out did not take")
    else pure (T.intercalate "+" (sort (rights (Map.elems results))))

deadLetters :: Text -> SimpleDb Reg IO [DLQ.DLQJob Alpha]
deadLetters queue = Ops.listDLQJobs testSchema queue 10 0

betaHandler :: JobHandler (SimpleDb Reg IO) Beta Text
betaHandler _conn job = let Beta value = payload job in pure (value <> "/b")

gammaHandler :: JobHandler (SimpleDb Reg IO) Gamma Text
gammaHandler _conn job =
  let Gamma value = payload job
   in case T.stripPrefix "child:" value of
        Nothing -> pure (value <> "/c")
        Just parent -> answer parent

-- | Tell the run that asked for this one that it is finished.
answer :: Text -> SimpleDb Reg IO Text
answer parent = case readMaybe (T.unpack parent) of
  Nothing -> throwRetryable ("no run id in " <> parent)
  Just runId -> do
    sent <- WF.sendWorkflowSignal definitions (RunId runId) "child-done" (String "child done")
    either throwRetryable (const (pure "answered")) sent

run :: SimpleEnv Reg -> SimpleDb Reg IO a -> IO a
run = runSimpleDb

runIsDone :: SimpleEnv Reg -> RunId -> IO Bool
runIsDone env runId = runHasStatus env runId RunDone

outputOf :: [WF.StepRow] -> Text -> Maybe Value
outputOf steps name = WF.stepRowOutput =<< find ((== StepName name) . WF.stepRowName) steps

setupSchema :: ByteString -> IO ()
setupSchema connStr = setupWorkflowSchema connStr testSchema queueNames

withCleanRun :: Pool PG.Connection -> (SimpleEnv Reg -> IO a) -> IO a
withCleanRun = withCleanWorkflowRun (Proxy @Reg) testSchema queueNames

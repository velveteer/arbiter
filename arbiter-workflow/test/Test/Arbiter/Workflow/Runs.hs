{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QualifiedDo #-}
{-# LANGUAGE TypeFamilies #-}

-- | The run tables and the operations over them, against a real database. The runs
-- here are driven a settle at a time, with no worker pool between them.
module Test.Arbiter.Workflow.Runs (spec) where

import Arbiter.Core.Codec (Col (..), pval)
import Arbiter.Core.Job.DLQ qualified as DLQ
import Arbiter.Core.Job.Types (JobRead, defaultJob, payload, primaryKey)
import Arbiter.Core.MonadArbiter (MonadArbiter, withDbTransaction)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (QueueSpec (..))
import Arbiter.Simple (SimpleDb, SimpleEnv, runSimpleDb)
import Arbiter.Test.Setup (createSharedPool, execStatement)
import Control.Concurrent (threadDelay)
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON, Value (Null, String), object, (.=))
import Data.ByteString (ByteString)
import Data.Int (Int64)
import Data.List (find, sortOn)
import Data.Maybe (isJust, listToMaybe)
import Data.Pool (Pool)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (NominalDiffTime)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Test.Hspec
import UnliftIO.Async (concurrently)

import Arbiter.Workflow qualified as WF
import Arbiter.Workflow.Expr (both, use)
import Arbiter.Workflow.Graph (Workflow (..), signal, step)
import Arbiter.Workflow.Graph qualified as Graph
import Arbiter.Workflow.Registry (WorkflowRegistry, workflow, workflows)
import Arbiter.Workflow.Types (RunStatus (..), StepName (..), StepStatus (..))
import Arbiter.Workflow.Worker (startWorkflow)
import Test.Arbiter.Workflow.Harness (setupWorkflowSchema, withCleanWorkflowRun)

newtype Alpha = Alpha Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype Beta = Beta Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type Reg =
  '[ QueueWithResult "wf_alpha" Alpha Value
   , QueueWithResult "wf_beta" Beta Value
   ]

testSchema :: Text
testSchema = "arbiter_workflow_test"

alphaQueue :: Text
alphaQueue = "wf_alpha"

betaQueue :: Text
betaQueue = "wf_beta"

spec :: ByteString -> Spec
spec connStr = beforeAll (setupSchema connStr) $ do
  sharedPool <- runIO (createSharedPool connStr)
  around (withCleanRun sharedPool) $ do
    describe "startRun" $ do
      it "inserts the run, its steps and its edges" $ \env -> do
        runId <- start env linear
        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunRunning
        WF.runRowInput runRow `shouldBe` String "seed"

        steps <- run env (WF.listSteps runId)
        map WF.stepRowName steps `shouldBe` map StepName ["input", "a", "b"]
        map WF.stepRowStatus steps `shouldBe` [StepDone, StepReady, StepWaiting]
        map WF.stepRowWaitCount steps `shouldBe` [0, 0, 1]

      it "stores the run input as the input step's output" $ \env -> do
        runId <- start env linear
        steps <- run env (WF.listSteps runId)
        fmap WF.stepRowOutput (find ((== StepName "input") . WF.stepRowName) steps)
          `shouldBe` Just (Just (String "seed"))

      it "gives the head step a job and leaves the rest without one" $ \env -> do
        void $ start env linear
        claimed <- claim env alphaQueue 10
        map payload claimed `shouldBe` [String "a-payload"]
        run env (jobIn betaQueue) `shouldReturn` Nothing

    describe "settleWorkflowJob" $ do
      it "reports a job that is not a step" $ \env -> do
        Just other <- run env (insertPlain alphaQueue (String "loose"))
        settle env alphaQueue (primaryKey other) Nothing `shouldReturn` WF.NotAStep

      it "marks the step done with its result and gives its dependent a job" $ \env -> do
        runId <- start env linear
        [alpha] <- claim env alphaQueue 1
        outcome <- settle env alphaQueue (primaryKey alpha) (Just (String "a-out"))
        WF.advancedRunDone <$> asAdvance outcome `shouldBe` Just False

        steps <- run env (WF.listSteps runId)
        stepStatuses steps
          `shouldBe` [(StepName "input", StepDone), (StepName "a", StepDone), (StepName "b", StepReady)]
        fmap WF.stepRowOutput (find ((== StepName "a") . WF.stepRowName) steps)
          `shouldBe` Just (Just (String "a-out"))

        claimed <- claim env betaQueue 10
        map payload claimed `shouldBe` [String "b-payload"]

      it "readies a fan-in step only once both predecessors settle" $ \env -> do
        runId <- start env fanIn
        [alpha] <- claim env alphaQueue 1
        void $ settle env alphaQueue (primaryKey alpha) (Just (String "left"))
        claim env betaQueue 10 `shouldReturn` []

        [other] <- claim env alphaQueue 1
        void $ settle env alphaQueue (primaryKey other) (Just (String "right"))
        claimed <- claim env betaQueue 10
        map payload claimed `shouldBe` [String "join-payload"]

        steps <- run env (WF.listSteps runId)
        map WF.stepRowWaitCount steps `shouldBe` [0, 0, 0, 0]

      it "finishes the run with the outputs of its sinks" $ \env -> do
        runId <- start env linear
        [alpha] <- claim env alphaQueue 1
        void $ settle env alphaQueue (primaryKey alpha) (Just (String "a-out"))
        [beta] <- claim env betaQueue 1
        outcome <- settle env betaQueue (primaryKey beta) (Just (String "b-out"))
        WF.advancedRunDone <$> asAdvance outcome `shouldBe` Just True

        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunDone
        WF.runRowOutput runRow `shouldBe` Just (object ["b" .= String "b-out"])

      it "records the output but readies nothing once the run is cancelled" $ \env -> do
        runId <- start env linear
        [alpha] <- claim env alphaQueue 1
        void $ run env (WF.cancelRun runId)

        settle env alphaQueue (primaryKey alpha) (Just (String "late"))
          `shouldReturn` WF.RunClosed runId
        claim env betaQueue 10 `shouldReturn` []

      it "finishes the run when its last two steps settle at the same time" $ \env -> do
        runId <- start env forkJoin
        [alpha] <- claim env alphaQueue 1
        [beta] <- claim env betaQueue 1
        void $
          concurrently
            ( run env $ withDbTransaction $ do
                void (WF.settleWorkflowJob definitions alphaQueue (primaryKey alpha) (Just (String "a-out")))
                liftIO (threadDelay 300_000)
            )
            ( do
                threadDelay 50_000
                run env
                  $ withDbTransaction
                  $ void (WF.settleWorkflowJob definitions betaQueue (primaryKey beta) (Just (String "b-out")))
            )

        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunDone
        WF.runRowOutput runRow `shouldBe` Just (object ["a" .= String "a-out", "b" .= String "b-out"])

    describe "cancelRun" $ do
      it "cancels the run, its steps and the jobs behind them" $ \env -> do
        runId <- start env linear
        run env (WF.cancelRun runId) `shouldReturn` True

        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunCancelled
        steps <- run env (WF.listSteps runId)
        map WF.stepRowStatus steps `shouldBe` [StepDone, StepCancelled, StepCancelled]

        run env (jobIn alphaQueue) `shouldReturn` Nothing
        run env (jobIn betaQueue) `shouldReturn` Nothing

      it "refuses a run that already finished" $ \env -> do
        runId <- start env linear
        run env (WF.cancelRun runId) `shouldReturn` True
        run env (WF.cancelRun runId) `shouldReturn` False

    describe "failStepForJob" $ do
      it "fails the step whose job died and the run behind it" $ \env -> do
        runId <- start env linear
        [alpha] <- claim env alphaQueue 1
        run env (WF.failStepForJob alphaQueue (primaryKey alpha)) `shouldReturn` True

        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunFailed
        steps <- run env (WF.listSteps runId)
        map WF.stepRowStatus steps `shouldBe` [StepDone, StepFailed, StepWaiting]

      it "reports nothing for a job no run owns" $ \env -> do
        Just loose <- run env (insertPlain alphaQueue (String "loose"))
        run env (WF.failStepForJob alphaQueue (primaryKey loose)) `shouldReturn` False

      it "leaves a run that already finished alone" $ \env -> do
        runId <- start env linear
        [alpha] <- claim env alphaQueue 1
        void $ run env (WF.cancelRun runId)
        void $ run env (WF.failStepForJob alphaQueue (primaryKey alpha))

        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunCancelled

    describe "sweepSignalDeadlines" $ do
      it "fails a run whose signal deadline has passed" $ \env -> do
        runId <- start env (gated (-60))
        [alpha] <- claim env alphaQueue 1
        void $ settle env alphaQueue (primaryKey alpha) (Just (String "a-out"))

        run env WF.sweepSignalDeadlines `shouldReturn` 1
        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunFailed
        steps <- run env (WF.listSteps runId)
        map WF.stepRowStatus steps `shouldBe` [StepDone, StepDone, StepFailed, StepWaiting]

      it "leaves a signal whose deadline is still ahead" $ \env -> do
        void $ start env (gated 60)
        run env WF.sweepSignalDeadlines `shouldReturn` 0

      it "sweeps a deadline once" $ \env -> do
        void $ start env (gated (-60))
        [alpha] <- claim env alphaQueue 1
        void $ settle env alphaQueue (primaryKey alpha) (Just (String "a-out"))

        run env WF.sweepSignalDeadlines `shouldReturn` 1
        run env WF.sweepSignalDeadlines `shouldReturn` 0

    describe "reviveRetriedRuns" $ do
      it "puts a failed run back to running once its step's job returns" $ \env -> do
        runId <- start env linear
        [alpha] <- claim env alphaQueue 1
        void $ run env (moveToDLQ alphaQueue alpha)
        void $ run env (WF.failStepForJob alphaQueue (primaryKey alpha))
        run env WF.reviveRetriedRuns `shouldReturn` 0

        [dead] <- run env (deadLetters alphaQueue)
        void $ run env (retryDead alphaQueue (DLQ.dlqPrimaryKey dead))

        run env WF.reviveRetriedRuns `shouldReturn` 1
        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunRunning
        steps <- run env (WF.listSteps runId)
        map WF.stepRowStatus steps `shouldBe` [StepDone, StepReady, StepWaiting]

      it "leaves a failed run whose job is still dead" $ \env -> do
        runId <- start env linear
        [alpha] <- claim env alphaQueue 1
        void $ run env (moveToDLQ alphaQueue alpha)
        void $ run env (WF.failStepForJob alphaQueue (primaryKey alpha))

        run env WF.reviveRetriedRuns `shouldReturn` 0
        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunFailed

    describe "purgeFinishedRuns" $ do
      it "deletes a run past its retention, and its steps with it" $ \env -> do
        runId <- start env linear
        void $ run env (WF.cancelRun runId)
        backdateFinish env runId

        run env (WF.purgeFinishedRuns 60) `shouldReturn` 1
        run env (WF.getRun runId) `shouldReturn` Nothing
        run env (WF.listSteps runId) `shouldReturn` []

      it "keeps a run that finished inside its retention" $ \env -> do
        runId <- start env linear
        void $ run env (WF.cancelRun runId)

        run env (WF.purgeFinishedRuns 60) `shouldReturn` 0
        kept <- run env (WF.getRun runId)
        kept `shouldSatisfy` isJust

      it "keeps a run that has yet to finish" $ \env -> do
        runId <- start env linear

        run env (WF.purgeFinishedRuns 0) `shouldReturn` 0
        kept <- run env (WF.getRun runId)
        kept `shouldSatisfy` isJust

    describe "retryRun" $ do
      it "puts a failed run back to work, bringing its step's job out of the dead-letter queue" $ \env -> do
        runId <- start env linear
        [alpha] <- claim env alphaQueue 1
        void $ run env (moveToDLQ alphaQueue alpha)
        void $ run env (WF.failStepForJob alphaQueue (primaryKey alpha))

        run env (WF.retryRun runId) `shouldReturn` Right ()
        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunRunning
        steps <- run env (WF.listSteps runId)
        map WF.stepRowStatus steps `shouldBe` [StepDone, StepReady, StepWaiting]

        claimed <- claim env alphaQueue 1
        map primaryKey claimed `shouldBe` [primaryKey alpha]

      it "puts back a failed run whose job never left its queue" $ \env -> do
        runId <- start env linear
        [alpha] <- claim env alphaQueue 1
        void $ run env (WF.failStepForJob alphaQueue (primaryKey alpha))

        run env (WF.retryRun runId) `shouldReturn` Right ()
        Just runRow <- run env (WF.getRun runId)
        WF.runRowStatus runRow `shouldBe` RunRunning

      it "refuses a run that is not failed" $ \env -> do
        runId <- start env linear
        run env (WF.retryRun runId) `shouldReturn` Left "run is not failed"

      it "refuses a run whose failed step holds no job" $ \env -> do
        runId <- start env (gated (-60))
        [alpha] <- claim env alphaQueue 1
        void $ settle env alphaQueue (primaryKey alpha) (Just (String "a-out"))
        void $ run env WF.sweepSignalDeadlines

        run env (WF.retryRun runId)
          `shouldReturn` Left "step signal@0 holds no job to retry"

    describe "sendWorkflowSignal" $ do
      it "writes the value as the signal step's output and advances the run" $ \env -> do
        runId <- start env (gated 60)
        [alpha] <- claim env alphaQueue 1
        void $ settle env alphaQueue (primaryKey alpha) (Just (String "a-out"))
        claim env betaQueue 10 `shouldReturn` []

        Right outcome <- run env (WF.sendWorkflowSignal definitions runId "approval" (String "yes"))
        WF.advancedRunDone <$> asAdvance outcome `shouldBe` Just False

        steps <- run env (WF.listSteps runId)
        fmap WF.stepRowOutput (find ((== StepName "signal@0") . WF.stepRowName) steps)
          `shouldBe` Just (Just (String "yes"))
        claimed <- claim env betaQueue 10
        map payload claimed `shouldBe` [String "b-payload"]

      it "advances once when a signal and a settle land together" $ \env -> do
        runId <- start env (gated 60)
        [alpha] <- claim env alphaQueue 1

        void $
          concurrently
            ( run env $ withDbTransaction $ do
                void (WF.settleWorkflowJob definitions alphaQueue (primaryKey alpha) (Just (String "a-out")))
                liftIO (threadDelay 300_000)
            )
            ( do
                threadDelay 50_000
                run env (WF.sendWorkflowSignal definitions runId "approval" (String "-ok"))
            )

        steps <- run env (WF.listSteps runId)
        map WF.stepRowStatus steps `shouldBe` [StepDone, StepDone, StepDone, StepReady]
        claimed <- claim env betaQueue 10
        length claimed `shouldBe` 1

      it "refuses a key the run is not waiting on" $ \env -> do
        runId <- start env (gated 60)
        run env (WF.sendWorkflowSignal definitions runId "other" Null)
          `shouldReturn` Left "no step of this run is waiting on signal other"

      it "refuses a signal to a cancelled run" $ \env -> do
        runId <- start env (gated 60)
        void $ run env (WF.cancelRun runId)
        run env (WF.sendWorkflowSignal definitions runId "approval" (String "yes"))
          `shouldReturn` Left "run is not running"

-- | Input, then one step in each queue.
linear :: Workflow Reg Value Value
linear = Workflow "linear" 1 $ \source -> Graph.do
  a <- step "a" (const (Alpha "a-payload")) (use source)
  step "b" (const (Beta "b-payload")) (use a)

-- | Two steps in one queue joining into a third in another.
fanIn :: Workflow Reg Value Value
fanIn = Workflow "fan-in" 1 $ \source -> Graph.do
  left <- step "left" (const (Alpha "left-payload")) (use source)
  right <- step "right" (const (Alpha "right-payload")) (use source)
  step "join" (const (Beta "join-payload")) (both left right)

-- | Two steps with no edge between them, each a sink of the run.
forkJoin :: Workflow Reg Value Value
forkJoin = Workflow "fork-join" 1 $ \source -> Graph.do
  _a <- step "a" (const (Alpha "a-payload")) (use source)
  step "b" (const (Beta "b-payload")) (use source)

-- | A step and a signal that both feed the last step, so the two can settle at once.
gated :: NominalDiffTime -> Workflow Reg Value Value
gated deadline = Workflow "gated" 1 $ \source -> Graph.do
  a <- step "a" (const (Alpha "a-payload")) (use source)
  approved <- signal "approval" deadline
  step "b" (const (Beta "b-payload") :: (Value, Value) -> Beta) (both a approved)

-- | The definitions a settle resolves a run against.
definitions :: WorkflowRegistry Reg
definitions = workflows [workflow linear, workflow fanIn, workflow forkJoin, workflow (gated 60)]

-- | The step statuses, in insertion order.
stepStatuses :: [WF.StepRow] -> [(StepName, StepStatus)]
stepStatuses = map (\stepRow -> (WF.stepRowName stepRow, WF.stepRowStatus stepRow)) . sortOn WF.stepRowId

-- | The advance a settle reported, if it advanced one.
asAdvance :: WF.SettleOutcome -> Maybe WF.Advance
asAdvance (WF.Advanced advance) = Just advance
asAdvance _ = Nothing

run :: SimpleEnv Reg -> SimpleDb Reg IO a -> IO a
run = runSimpleDb

-- | Start a run of one definition on the shared input.
start :: SimpleEnv Reg -> Workflow Reg Value Value -> IO WF.RunId
start env definition =
  run env (startWorkflow definitions definition (String "seed")) >>= either (fail . T.unpack) pure

-- | Advance the run one job finished, failing the test if the settle could not go on.
settle :: SimpleEnv Reg -> Text -> Int64 -> Maybe Value -> IO WF.SettleOutcome
settle env queue jobId result =
  run env (WF.settleWorkflowJob definitions queue jobId result) >>= either (fail . T.unpack) pure

-- | Claim from a queue named at runtime.
claim :: SimpleEnv Reg -> Text -> Int -> IO [JobRead Value]
claim env queue count = run env (Ops.claimNextVisibleJobs testSchema queue count 60)

-- | The first row left in a queue, claimed or not.
jobIn :: (MonadArbiter m) => Text -> m (Maybe (JobRead Value))
jobIn queue = listToMaybe <$> Ops.listJobs testSchema queue 10 0

moveToDLQ :: (MonadArbiter m) => Text -> JobRead Value -> m Int64
moveToDLQ queue job = Ops.moveToDLQ Ops.TakeLocks testSchema queue "boom" job

deadLetters :: (MonadArbiter m) => Text -> m [DLQ.DLQJob Value]
deadLetters queue = Ops.listDLQJobs testSchema queue 10 0

retryDead :: (MonadArbiter m) => Text -> Int64 -> m (Maybe (JobRead Value))
retryDead queue dlqId = Ops.retryFromDLQ testSchema queue dlqId

-- | A job in a queue that no step owns.
insertPlain :: (MonadArbiter m) => Text -> Value -> m (Maybe (JobRead Value))
insertPlain queue value = Ops.insertJob testSchema queue (defaultJob value)

-- | Put a run's finish an hour into the past.
backdateFinish :: SimpleEnv Reg -> WF.RunId -> IO ()
backdateFinish env (WF.RunId runId) =
  void . run env $
    execStatement
      ("UPDATE " <> WF.workflowRunsTable testSchema <> " SET finished_at = NOW() - interval '1 hour' WHERE id = ?")
      [pval CInt8 runId]

-- | The schema, both queues, and the workflow tables.
setupSchema :: ByteString -> IO ()
setupSchema connStr = setupWorkflowSchema connStr testSchema [alphaQueue, betaQueue]

-- | An env over the shared pool, with every run and every job cleared first.
withCleanRun :: Pool PG.Connection -> (SimpleEnv Reg -> IO a) -> IO a
withCleanRun = withCleanWorkflowRun (Proxy @Reg) testSchema [alphaQueue, betaQueue]

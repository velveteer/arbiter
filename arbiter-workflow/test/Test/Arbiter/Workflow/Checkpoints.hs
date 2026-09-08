{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TypeFamilies #-}

-- | A run whose steps the handler records as it goes, rather than a definition
-- materializing them.
module Test.Arbiter.Workflow.Checkpoints (spec) where

import Arbiter.Core.Codec (Col (..), col, pval)
import Arbiter.Core.Exceptions (throwRetryable)
import Arbiter.Core.Job.Types (JobRead, JobWrite, defaultJob, payload, primaryKey, setMaxAttempts)
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (QueueWithResult)
import Arbiter.Simple (SimpleDb, SimpleEnv, runSimpleDb)
import Arbiter.Test.Poll (waitUntil, withLinkedAsync)
import Arbiter.Test.Setup (createSharedPool, execQuery)
import Arbiter.Worker (OverlapPolicy (..), TickKind, cronJob, initCronSchedules, runWorkerPool)
import Arbiter.Worker.BackoffStrategy (BackoffStrategy (..), Jitter (..))
import Arbiter.Worker.Config (BatchCallbacks, WorkerConfig (..), ackWith, manualWorkerConfig)
import Arbiter.Worker.Cron (CronJob)
import Arbiter.Worker.Logger (silentLogConfig)
import Control.Monad (void, when)
import Control.Monad.IO.Class (liftIO)
import Data.Aeson (FromJSON, ToJSON, Value (String))
import Data.ByteString (ByteString)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.Pool (Pool)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Time (UTCTime)
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Test.Hspec

import Arbiter.Workflow
import Test.Arbiter.Workflow.Harness (runHasStatus, setupWorkflowSchema, withCleanWorkflowRun)

newtype Task = Task Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type Reg = '[QueueWithResult "wfc_tasks" Task Text]

testSchema :: Text
testSchema = "arbiter_workflow_checkpoints"

taskQueue :: Text
taskQueue = "wfc_tasks"

-- | No definition is registered. A checkpoint run needs none.
definitions :: WorkflowRegistry Reg
definitions = workflows []

spec :: ByteString -> Spec
spec connStr = beforeAll (setupSchema connStr) $ do
  sharedPool <- runIO (createSharedPool connStr)
  around (withCleanRun sharedPool) $ do
    it "runs each checkpoint once and finishes the run" $ \env -> do
      done <- newIORef ([] :: [Text])
      failures <- newIORef 0
      withPool env done failures $ do
        runId <- run env (startCheckpointRun @Reg "billing" 1 (String "order-1") (defaultJob (Task "go")))
        waitUntil 20_000 (runHasStatus env runId RunDone)

        readIORef done `shouldReturn` ["charge", "invoice"]
        Just runRow <- run env (getRun runId)
        runRowOutput runRow `shouldBe` Just (String "charge/invoice")

        steps <- run env (listSteps runId)
        map stepRowName steps `shouldBe` map StepName ["job", "charge", "invoice"]
        map stepRowKind steps `shouldBe` [KindJob, KindCheckpoint, KindCheckpoint]

    it "skips the checkpoints it already recorded when the handler runs again" $ \env -> do
      done <- newIORef ([] :: [Text])
      failures <- newIORef 1
      withPool env done failures $ do
        runId <- run env (startCheckpointRun @Reg "billing" 1 (String "order-2") (defaultJob (Task "fail-once")))
        waitUntil 20_000 (runHasStatus env runId RunDone)

        readIORef done `shouldReturn` ["charge", "invoice"]
        steps <- run env (listSteps runId)
        map stepRowName steps `shouldBe` map StepName ["job", "charge", "invoice"]

    it "fails the run when its handler exhausts its attempts" $ \env -> do
      done <- newIORef ([] :: [Text])
      failures <- newIORef 99
      withPool env done failures $ do
        runId <-
          run env $
            startCheckpointRun @Reg "billing" 1 (String "order-4") (setMaxAttempts (Just 1) (defaultJob (Task "fail-once")))
        waitUntil 20_000 (runHasStatus env runId RunFailed)

        steps <- run env (listSteps runId)
        map stepRowStatus steps `shouldBe` [StepFailed, StepDone]

    it "records the run of a job when its schedule fires it" $ \env -> do
      done <- newIORef ([] :: [Text])
      failures <- newIORef 1
      nightly <- either fail pure (cronJob "nightly" "0 3 * * *" SkipOverlap fixedTask)
      run env (initCronSchedules testSchema taskQueue [nightly] silentLogConfig)
      void $ run env (Ops.requestCronRun testSchema "nightly")

      withCronPool env done failures nightly $ do
        waitUntil 20_000 (scheduledRunIsDone env)

        readIORef done `shouldReturn` ["charge", "invoice"]
        [runId] <- run env scheduledRuns
        steps <- run env (listSteps (RunId runId))
        map stepRowName steps `shouldBe` map StepName ["job", "charge", "invoice"]

    it "records the checkpoints of a job a schedule inserted" $ \env -> do
      done <- newIORef ([] :: [Text])
      failures <- newIORef 1
      withAdoptingPool env done failures $ do
        Just loose <- run env (insertLoose (Task "fail-once"))
        waitUntil 20_000 (jobIsGone env (primaryKey loose))

        readIORef done `shouldReturn` ["charge", "invoice"]
        Just runId <- run env (runOfJob taskQueue (primaryKey loose))
        Just runRow <- run env (getRun runId)
        runRowStatus runRow `shouldBe` RunDone

        steps <- run env (listSteps runId)
        map stepRowName steps `shouldBe` map StepName ["job", "charge", "invoice"]

    it "records nothing for a job that belongs to no run" $ \env -> do
      done <- newIORef ([] :: [Text])
      failures <- newIORef 0
      withPool env done failures $ do
        Just loose <- run env (insertLoose (Task "go"))
        waitUntil 20_000 (jobIsGone env (primaryKey loose))
        readIORef done `shouldReturn` ["charge", "invoice"]

    it "gives back what a name recorded, whoever asks again" $ \env -> do
      runId <- run env (startCheckpointRun @Reg "billing" 1 (String "order-5") (defaultJob (Task "go")))
      [held] <- run env claimOne
      run env (checkpoint held "twice" (pure ("first" :: Text))) `shouldReturn` "first"
      run env (recordAgain held) `shouldReturn` "first"

      steps <- run env (listSteps runId)
      map stepRowName steps `shouldBe` map StepName ["job", "twice"]

    it "records nothing more once the run is closed" $ \env -> do
      runId <- run env (startCheckpointRun @Reg "billing" 1 (String "order-6") (defaultJob (Task "go")))
      [held] <- run env claimOne
      void $ run env (cancelRun runId)

      outcome <- run env (settleWorkflowJob definitions taskQueue (primaryKey held) (Just (String "late")))
      outcome `shouldBe` Right (RunClosed runId)
      Just runRow <- run env (getRun runId)
      runRowStatus runRow `shouldBe` RunCancelled

    it "cancels a checkpoint run like any other" $ \env -> do
      done <- newIORef ([] :: [Text])
      runId <- run env (startCheckpointRun @Reg "billing" 1 (String "order-3") (defaultJob (Task "go")))
      run env (cancelRun runId) `shouldReturn` True

      Just runRow <- run env (getRun runId)
      runRowStatus runRow `shouldBe` RunCancelled
      readIORef done `shouldReturn` []

-- | Charge, then invoice. The @fail-once@ task throws between them the first time.
handler
  :: IORef [Text]
  -> IORef Int
  -> JobRead Task
  -> BatchCallbacks (SimpleDb Reg IO) Task Text
  -> SimpleDb Reg IO ()
handler done failures job callbacks = do
  charged <- checkpoint job "charge" (record "charge")
  failOnce job
  invoiced <- checkpoint job "invoice" (record "invoice")
  ackWith callbacks job (charged <> "/" <> invoiced)
  where
    record name = name <$ liftIO (atomicModifyIORef' done (\seen -> (seen <> [name], ())))

    failOnce held = case payload held of
      Task "fail-once" -> do
        left <- liftIO (atomicModifyIORef' failures (\budget -> (max 0 (budget - 1), budget)))
        when (left > 0) (throwRetryable "not yet")
      _ -> pure ()

-- | The same work, on a job that a schedule inserted rather than a run.
adopting
  :: IORef [Text]
  -> IORef Int
  -> JobRead Task
  -> BatchCallbacks (SimpleDb Reg IO) Task Text
  -> SimpleDb Reg IO ()
adopting done failures job callbacks =
  withCheckpointRun "nightly" 1 job (handler done failures job callbacks)

withPool :: SimpleEnv Reg -> IORef [Text] -> IORef Int -> IO a -> IO a
withPool env done failures = withPoolOf env (handler done failures)

withAdoptingPool :: SimpleEnv Reg -> IORef [Text] -> IORef Int -> IO a -> IO a
withAdoptingPool env done failures = withPoolOf env (adopting done failures)

-- | The job every tick of the test schedule fires.
fixedTask :: TickKind -> UTCTime -> JobWrite Task
fixedTask _kind _tick = defaultJob (Task "fail-once")

-- | A pool whose schedule records the job it fires as a checkpoint run.
withCronPool :: SimpleEnv Reg -> IORef [Text] -> IORef Int -> CronJob Task -> IO a -> IO a
withCronPool env done failures cron =
  withPoolWith scheduled env (handler done failures)
  where
    scheduled config = config {cronJobs = [cron]}

withPoolOf
  :: SimpleEnv Reg
  -> (JobRead Task -> BatchCallbacks (SimpleDb Reg IO) Task Text -> SimpleDb Reg IO ())
  -> IO a
  -> IO a
withPoolOf = withPoolWith id

withPoolWith
  :: (WorkerConfig (SimpleDb Reg IO) Task -> WorkerConfig (SimpleDb Reg IO) Task)
  -> SimpleEnv Reg
  -> (JobRead Task -> BatchCallbacks (SimpleDb Reg IO) Task Text -> SimpleDb Reg IO ())
  -> IO a
  -> IO a
withPoolWith tune env work body = do
  config <- manualWorkerConfig 1 work
  let paced =
        config
          { pollInterval = 0.05
          , logConfig = silentLogConfig
          , backoffStrategy = Constant 0
          , jitter = NoJitter
          }
  withLinkedAsync (run env (runWorkerPool (tune (withWorkflows definitions paced)))) (const body)

recordAgain :: JobRead Task -> SimpleDb Reg IO Text
recordAgain held = checkpoint held "twice" (pure "second")

-- | The runs the test schedule has started.
scheduledRuns :: SimpleDb Reg IO [Int64]
scheduledRuns =
  execQuery
    ("SELECT id FROM " <> workflowRunsTable testSchema <> " WHERE workflow = ?")
    [pval CText "nightly"]
    (col "id" CInt8)

scheduledRunIsDone :: SimpleEnv Reg -> IO Bool
scheduledRunIsDone env = do
  started <- run env scheduledRuns
  case started of
    [runId] -> runHasStatus env (RunId runId) RunDone
    _ -> pure False

insertLoose :: Task -> SimpleDb Reg IO (Maybe (JobRead Task))
insertLoose task = Ops.insertJob testSchema taskQueue (defaultJob task)

claimOne :: SimpleDb Reg IO [JobRead Task]
claimOne = Ops.claimNextVisibleJobs testSchema taskQueue 1 60

jobIsGone :: SimpleEnv Reg -> Int64 -> IO Bool
jobIsGone env jobId = not <$> run env (Ops.jobExists testSchema taskQueue jobId)

run :: SimpleEnv Reg -> SimpleDb Reg IO a -> IO a
run = runSimpleDb

setupSchema :: ByteString -> IO ()
setupSchema connStr = setupWorkflowSchema connStr testSchema [taskQueue]

withCleanRun :: Pool PG.Connection -> (SimpleEnv Reg -> IO a) -> IO a
withCleanRun = withCleanWorkflowRun (Proxy @Reg) testSchema [taskQueue]

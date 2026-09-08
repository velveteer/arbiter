{-# LANGUAGE DataKinds #-}
{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QualifiedDo #-}
{-# LANGUAGE TypeFamilies #-}

-- | What the operations lock, read off the statements they run against a real
-- database and checked against "Arbiter.Workflow.LockPlan". The simulation explores
-- the plan, so this is what keeps the plan and the code from drifting apart.
module Test.Arbiter.Workflow.LockTrace (spec) where

import Arbiter.Core.Job.Types (JobRead, defaultJob, primaryKey)
import Arbiter.Core.MonadArbiter (MonadArbiter (..))
import Arbiter.Core.Operations qualified as Ops
import Arbiter.Core.QueueRegistry (QueueWithResult)
import Arbiter.Core.Sql.Query (Query, qSql)
import Arbiter.Core.Sql.Tree (forceCancelJobsSQL)
import Arbiter.Simple (SimpleDb, SimpleEnv, runSimpleDb)
import Arbiter.Test.Setup (createSharedPool)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Reader (ReaderT (..), ask)
import Data.Aeson (FromJSON, ToJSON, Value (String))
import Data.ByteString (ByteString)
import Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import Data.Int (Int64)
import Data.Pool (Pool)
import Data.Proxy (Proxy (..))
import Data.Text (Text)
import Data.Text qualified as T
import Database.PostgreSQL.Simple qualified as PG
import GHC.Generics (Generic)
import Test.Hspec
import UnliftIO (MonadUnliftIO)

import Arbiter.Workflow hiding (RunRow, StepRow)
import Arbiter.Workflow.Graph qualified as Graph
import Arbiter.Workflow.LockPlan
  ( HeldRows (..)
  , LockMode (..)
  , LockStep (..)
  , LockTarget (..)
  , advancePlan
  , cancelPlan
  , settlePlan
  )
import Arbiter.Workflow.Sql (advanceDependentsSQL, cancelStepsSQL, reconcileNewStepsSQL)
import Test.Arbiter.Workflow.Harness (setupWorkflowSchema, withCleanWorkflowRun)

newtype One = One Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

newtype Two = Two Text
  deriving stock (Eq, Generic, Show)
  deriving anyclass (FromJSON, ToJSON)

type Reg =
  '[ QueueWithResult "wft_one" One Text
   , QueueWithResult "wft_two" Two Text
   ]

testSchema :: Text
testSchema = "arbiter_workflow_trace"

oneQueue :: Text
oneQueue = "wft_one"

twoQueue :: Text
twoQueue = "wft_two"

pair :: Workflow Reg Text Text
pair = Workflow "pair" 1 $ \source -> Graph.do
  first <- step "first" One (use source)
  step "second" Two (use first)

definitions :: WorkflowRegistry Reg
definitions = workflows [workflow pair]

spec :: ByteString -> Spec
spec connStr = do
  describe "reading a lock off a statement" $
    it "gives the table, the mode and the skip" $ do
      statementLocks (squash "SELECT status FROM \"s\".\"arbiter_workflow_runs\" WHERE id = ? FOR UPDATE")
        `shouldBe` [LockUse "arbiter_workflow_runs" Exclusive Wait]
      statementLocks (squash "SELECT id FROM \"s\".\"jobs\" ORDER BY id DESC FOR UPDATE SKIP LOCKED")
        `shouldBe` [LockUse "jobs" Exclusive Skip]
      statementLocks (squash "SELECT a.id FROM \"s\".\"steps\" step JOIN \"s\".\"edges\" edge ON x FOR UPDATE OF step")
        `shouldBe` [LockUse "steps" Exclusive Wait]
      statementLocks (squash "UPDATE \"s\".\"steps\" SET status = 'done' WHERE id = ?")
        `shouldBe` [LockUse "steps" Exclusive Wait]
      statementLocks (squash "DELETE FROM \"s\".\"jobs\" WHERE id = ?")
        `shouldBe` [LockUse "jobs" Exclusive Wait]

  describe "the order a statement locks in" $ do
    it "takes step rows ascending, the order every path shares" $ do
      lockOrders (advanceDependentsSQL "s" (StepId 1)) `shouldBe` [Ascending]
      lockOrders (reconcileNewStepsSQL "s" [1]) `shouldBe` [Ascending]
      lockOrders (cancelStepsSQL "s" (RunId 1)) `shouldBe` [Ascending]

    it "takes job rows descending, the order the queue's own statements share" $ do
      lockOrders forceCancelJobsQuery `shouldBe` [Descending]

  beforeAll (setupSchema connStr) $ do
    sharedPool <- runIO (createSharedPool connStr)
    around (withCleanRun sharedPool) $ do
      describe "what the operations lock" $ do
        it "takes the new run's step rows to write it, then the runs row to advance it" $ \env -> do
          -- The write takes only rows it inserted itself, which no other transaction
          -- can reach. The advance that follows opens its own transaction.
          taken <- traced env (startWorkflow definitions pair "seed")
          taken `shouldBe` [(stepsTable, Exclusive, Wait), (runsTable, Exclusive, Wait)]
          drop 1 taken `shouldSatisfy` within (advancePlan aRun [StepId 1])

        it "takes the runs row then the step rows to settle, the ack holding the job row" $ \env -> do
          Right _ <- run env (startWorkflow definitions pair "seed")
          [held] <- run env (claimFrom oneQueue)
          taken <- traced env (settleWorkflowJob definitions oneQueue (primaryKey held) (Just (String "one")))
          taken `shouldBe` [(runsTable, Exclusive, Wait), (stepsTable, Exclusive, Wait)]
          taken `shouldSatisfy` within (drop 1 (settlePlan aRun (oneQueue, 1) [StepId 1]))

        it "takes the runs row, the job rows with a skip, then the step rows to cancel" $ \env -> do
          Right runId <- run env (startWorkflow definitions pair "seed")
          taken <- traced env (cancelRun runId)
          taken
            `shouldBe` [(runsTable, Exclusive, Wait), (oneQueue, Exclusive, Skip), (stepsTable, Exclusive, Wait)]
          taken `shouldSatisfy` within (cancelPlan aRun [(oneQueue, 1)] [StepId 1])

      it "takes the runs row then the step row to close a checkpoint run" $ \env -> do
        _ <- run env (startCheckpointRun @Reg "counting" 1 (String "in") (defaultJob (One "go")))
        [held] <- run env (claimFrom oneQueue)
        taken <- traced env (settleWorkflowJob definitions oneQueue (primaryKey held) (Just (String "out")))
        taken `shouldBe` [(runsTable, Exclusive, Wait), (stepsTable, Exclusive, Wait)]
        taken `shouldSatisfy` within (advancePlan aRun [StepId 1])

      it "takes the runs row then the step rows to deliver a signal" $ \env -> do
        Right runId <- run env (startWorkflow definitions pair "seed")
        _ <- run env (sendWorkflowSignal definitions runId "nothing-waits" (String "x"))
        taken <- traced env (sendWorkflowSignal definitions runId "nothing-waits" (String "x"))
        taken `shouldBe` [(runsTable, Exclusive, Wait)]

-- | Which way a lock pass reads the rows it takes.
data LockOrder = Ascending | Descending
  deriving stock (Eq, Show)

-- | The order of every lock pass in a statement, in the order they appear.
lockOrders :: Query a -> [LockOrder]
lockOrders statement = walk (T.words (squash (qSql statement)))
  where
    walk tokens = case tokens of
      [] -> []
      ("ORDER" : "BY" : _ : "DESC" : rest) -> take 1 (Descending <$ locking rest) <> walk rest
      ("ORDER" : "BY" : _ : rest) -> take 1 (Ascending <$ locking rest) <> walk rest
      (_ : rest) -> walk rest
    locking rest = [() | "FOR" `elem` take 4 rest]

forceCancelJobsQuery :: Query Int64
forceCancelJobsQuery = forceCancelJobsSQL "s" "jobs" [1]

-- | One row lock a statement takes, and on which table.
data LockUse = LockUse Text LockMode HeldRows
  deriving stock (Eq, Show)

-- | The tables a run of statements locks, in the order it first takes each. A lock on
-- a row the transaction already holds orders nothing, so only the first counts.
firstTouches :: [Text] -> [(Text, LockMode, HeldRows)]
firstTouches statements = foldl keepFirst [] (concatMap statementLocks statements)
  where
    keepFirst seen (LockUse table mode held)
      | any (\(taken, _, _) -> taken == table) seen = seen
      | otherwise = seen <> [(table, mode, held)]

-- | Whether a run took only locks the plan names, in the plan's order. A run need not
-- take every lock its path allows, but it must take no other and none out of order.
within :: [LockStep] -> [(Text, LockMode, HeldRows)] -> Bool
within plan taken = subsequence taken (planLocks plan)
  where
    subsequence [] _ = True
    subsequence _ [] = False
    subsequence (this : rest) (allowed : others)
      | this == allowed = subsequence rest others
      | otherwise = subsequence (this : rest) others

-- | The same reading, off a plan.
planLocks :: [LockStep] -> [(Text, LockMode, HeldRows)]
planLocks = foldl keepFirst []
  where
    keepFirst seen lockStep
      | any (\(taken, _, _) -> taken == table) seen = seen
      | otherwise = seen <> [(table, lockMode lockStep, lockHeldRows lockStep)]
      where
        table = tableOf (lockTarget lockStep)
    tableOf (RunRow _) = runsTable
    tableOf (StepRow _) = stepsTable
    tableOf (JobRow queue _) = queue

-- | Every row lock one statement takes: an explicit @FOR UPDATE@ or @FOR SHARE@, and
-- the rows an @UPDATE@ or a @DELETE@ takes on its way.
statementLocks :: Text -> [LockUse]
statementLocks = walk [] Nothing . T.words
  where
    walk aliases recent tokens = case tokens of
      [] -> []
      ("FOR" : "UPDATE" : rest) -> forClause aliases recent Exclusive rest <> walk aliases recent rest
      ("FOR" : "SHARE" : rest) -> forClause aliases recent Share rest <> walk aliases recent rest
      ("UPDATE" : target : rest) -> written target <> walk aliases (Just (bareName target)) rest
      ("DELETE" : "FROM" : target : rest) -> written target <> walk aliases (Just (bareName target)) rest
      (token : rest)
        | qualified token ->
            let bound = case rest of
                  (next : _) | plainWord next -> (next, bareName token) : aliases
                  _ -> aliases
             in walk bound (Just (bareName token)) rest
        | otherwise -> walk aliases recent rest

    forClause aliases recent mode rest = case rest of
      ("OF" : alias : rest') ->
        [LockUse table mode (heldRows rest') | table <- take 1 (lookupAlias alias aliases)]
      _ -> [LockUse table mode (heldRows rest) | table <- maybe [] pure recent]

    heldRows ("SKIP" : "LOCKED" : _) = Skip
    heldRows _ = Wait

    written target = [LockUse (bareName target) Exclusive Wait | qualified target]
    lookupAlias alias aliases = [table | (bound, table) <- aliases, bound == alias]

qualified :: Text -> Bool
qualified token = T.isPrefixOf "\"" token && T.isInfixOf "\".\"" token

plainWord :: Text -> Bool
plainWord token = not (T.null token) && T.all (\letter -> letter `elem` ['a' .. 'z']) token

bareName :: Text -> Text
bareName = T.takeWhileEnd (/= '.') . T.filter (/= '"') . T.takeWhile (/= '(')

squash :: Text -> Text
squash = T.unwords . T.words

runsTable :: Text
runsTable = "arbiter_workflow_runs"

stepsTable :: Text
stepsTable = "arbiter_workflow_steps"

aRun :: RunId
aRun = RunId 1

-- | Run an operation, keeping the statements it ran.
traced :: SimpleEnv Reg -> Recording (SimpleDb Reg IO) a -> IO [(Text, LockMode, HeldRows)]
traced env action = do
  seen <- newIORef []
  _ <- runSimpleDb env (runRecording seen action)
  firstTouches <$> readIORef seen

-- | A backend that keeps every statement it is given, then hands it on.
newtype Recording m a = Recording (ReaderT (IORef [Text]) m a)
  deriving newtype (Applicative, Functor, Monad, MonadIO, MonadUnliftIO)

runRecording :: IORef [Text] -> Recording m a -> m a
runRecording seen (Recording action) = runReaderT action seen

instance (MonadArbiter m) => MonadArbiter (Recording m) where
  type RegistryOf (Recording m) = RegistryOf m
  type Handler (Recording m) job result = Handler m job result
  getSchema = inner getSchema
  executeQuery statement = keep statement >> inner (executeQuery statement)
  executeQueryPrepared statement = keep statement >> inner (executeQueryPrepared statement)
  executeStatement statement = keep statement >> inner (executeStatement statement)
  withDbTransaction (Recording action) = Recording (ReaderT (withDbTransaction . runReaderT action))
  runHandlerWithConnection handler job = inner (runHandlerWithConnection handler job)
  getListener = inner getListener

inner :: (Monad m) => m a -> Recording m a
inner = Recording . lift

keep :: (MonadIO m) => Query a -> Recording m ()
keep statement =
  Recording $
    ask >>= \seen -> liftIO (atomicModifyIORef' seen (\kept -> (kept <> [squash (qSql statement)], ())))

run :: SimpleEnv Reg -> SimpleDb Reg IO a -> IO a
run = runSimpleDb

claimFrom :: Text -> SimpleDb Reg IO [JobRead One]
claimFrom queue = Ops.claimNextVisibleJobs testSchema queue 1 60

setupSchema :: ByteString -> IO ()
setupSchema connStr = setupWorkflowSchema connStr testSchema [oneQueue, twoQueue]

withCleanRun :: Pool PG.Connection -> (SimpleEnv Reg -> IO a) -> IO a
withCleanRun = withCleanWorkflowRun (Proxy @Reg) testSchema [oneQueue, twoQueue]

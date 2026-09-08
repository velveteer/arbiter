{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QuasiQuotes #-}

-- | The statements over the three run tables. Each one takes its locks in the order
-- "Arbiter.Workflow.LockPlan" gives.
module Arbiter.Workflow.Sql
  ( RunRow (..)
  , StepRow (..)
  , insertRunSQL
  , NewStep (..)
  , insertStepsSQL
  , insertEdgesSQL
  , lockRunSQL
  , stepForJobSQL
  , completeStepSQL
  , advanceDependentsSQL
  , finishRunSQL
  , closeRunSQL
  , cancelStepsSQL
  , runJobsSQL
  , runDefinitionSQL
  , runModeSQL
  , checkpointOutputSQL
  , insertCheckpointSQL
  , finishRunWithSQL
  , runOutputsSQL
  , runStepIdsSQL
  , setStepJobSQL
  , reconcileNewStepsSQL
  , failStepSQL
  , failedStepsSQL
  , reopenRunSQL
  , reopenStepSQL
  , deadLetterOfSQL
  , failedRunStepsSQL
  , reviveStepsSQL
  , expireSignalStepsSQL
  , presentJobsSQL
  , readyStepsSQL
  , waitingSignalStepSQL
  , purgeFinishedRunsSQL
  , getRunSQL
  , listStepsSQL
  ) where

import Arbiter.Core.Codec (Col (..), RowCodec, col, ncol)
import Arbiter.Core.Job.Schema (SchemaName, jobQueueDLQTable, jobQueueTable)
import Arbiter.Core.Sql.QQ (sql)
import Arbiter.Core.Sql.Query (Query, rows)
import Arbiter.Core.SqlLiterals (textLiteral)
import Data.Aeson (Value)
import Data.Int (Int32, Int64)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Time (UTCTime)

import Arbiter.Workflow.Schema (workflowEdgesTable, workflowRunsTable, workflowStepsTable)
import Arbiter.Workflow.Types
  ( RunId (..)
  , RunMode
  , RunStatus (..)
  , StepId (..)
  , StepKind (..)
  , StepName (..)
  , StepStatus (..)
  , runModeText
  , runStatusFromText
  , runStatusText
  , stepKindFromText
  , stepKindText
  , stepStatusFromText
  , stepStatusText
  )

-- | The stored labels as the SQL literals the predicates match on, so the encoders in
-- "Arbiter.Workflow.Types" are the only place a label is spelled.
runRunningLit, runDoneLit, runFailedLit :: Text
runRunningLit = textLiteral (runStatusText RunRunning)
runDoneLit = textLiteral (runStatusText RunDone)
runFailedLit = textLiteral (runStatusText RunFailed)

stepWaitingLit, stepReadyLit, stepDoneLit, stepFailedLit, stepCancelledLit :: Text
stepWaitingLit = textLiteral (stepStatusText StepWaiting)
stepReadyLit = textLiteral (stepStatusText StepReady)
stepDoneLit = textLiteral (stepStatusText StepDone)
stepFailedLit = textLiteral (stepStatusText StepFailed)
stepCancelledLit = textLiteral (stepStatusText StepCancelled)

kindSignalLit, kindCheckpointLit :: Text
kindSignalLit = textLiteral (stepKindText KindSignal)
kindCheckpointLit = textLiteral (stepKindText KindCheckpoint)

-- | The statuses a step is in before it settles.
unsettledLit :: Text
unsettledLit = "(" <> T.intercalate ", " [stepWaitingLit, stepReadyLit] <> ")"

-- | One run.
data RunRow = RunRow
  { runRowId :: RunId
  , runRowWorkflow :: Text
  , runRowVersion :: Int32
  , runRowStatus :: RunStatus
  , runRowInput :: Value
  , runRowOutput :: Maybe Value
  , runRowTraceparent :: Maybe Text
  , runRowTracestate :: Maybe Text
  , runRowStartedAt :: UTCTime
  , runRowFinishedAt :: Maybe UTCTime
  }
  deriving stock (Eq, Show)

-- | One step of a run.
data StepRow = StepRow
  { stepRowId :: StepId
  , stepRowRun :: RunId
  , stepRowName :: StepName
  , stepRowKind :: StepKind
  , stepRowStatus :: StepStatus
  , stepRowQueue :: Maybe Text
  , stepRowJobId :: Maybe Int64
  , stepRowWaitCount :: Int32
  , stepRowOutput :: Maybe Value
  , stepRowSignalKey :: Maybe Text
  , stepRowDeadline :: Maybe UTCTime
  , stepRowIndex :: Maybe Int32
  }
  deriving stock (Eq, Show)

-- | Insert a run, running. Returns its id.
insertRunSQL :: SchemaName -> Text -> Int32 -> RunMode -> Value -> Maybe Text -> Maybe Text -> Query Int64
insertRunSQL schemaName workflow version mode input traceparent tracestate =
  let runs = workflowRunsTable schemaName
      modeText = runModeText mode
   in [sql|
        INSERT INTO ${runs} (workflow, version, status, mode, input, traceparent, tracestate)
        VALUES (#{workflow :: CText}, #{version :: CInt4}, ${runRunningLit}, #{modeText :: CText}, #{input :: CJsonb},
                #{traceparent :: Maybe CText}, #{tracestate :: Maybe CText})
        RETURNING @{id :: CInt8}
      |]

-- | One step row to insert. Named rather than positional: six of the columns are
-- optional and two of them are 'Maybe' 'Text'.
data NewStep = NewStep
  { newStepName :: Text
  , newStepKind :: Text
  , newStepStatus :: Text
  , newStepQueue :: Maybe Text
  , newStepJobId :: Maybe Int64
  , newStepOutput :: Maybe Value
  , newStepSignalKey :: Maybe Text
  , newStepDeadline :: Maybe UTCTime
  , newStepIndex :: Maybe Int32
  }
  deriving stock (Eq, Show)

-- | Insert a run's steps in one statement. Returns each new id with the name it was
-- written under, so the caller needs no order from the insert.
insertStepsSQL :: SchemaName -> Int64 -> [NewStep] -> Query (Int64, Text)
insertStepsSQL schemaName runId newSteps =
  let steps = workflowStepsTable schemaName
      names = map newStepName newSteps
      kinds = map newStepKind newSteps
      statuses = map newStepStatus newSteps
      queues = map newStepQueue newSteps
      jobIds = map newStepJobId newSteps
      outputs = map newStepOutput newSteps
      signalKeys = map newStepSignalKey newSteps
      deadlines = map newStepDeadline newSteps
      indexes = map newStepIndex newSteps
   in [sql|
        INSERT INTO ${steps}
          (run_id, name, kind, status, queue, job_id, output, signal_key, deadline, step_index)
        SELECT #{runId :: CInt8}, *
        FROM unnest(
          #{names :: [CText]}::text[], #{kinds :: [CText]}::text[], #{statuses :: [CText]}::text[],
          #{queues :: [Maybe CText]}::text[], #{jobIds :: [Maybe CInt8]}::bigint[],
          #{outputs :: [Maybe CJsonb]}::jsonb[], #{signalKeys :: [Maybe CText]}::text[],
          #{deadlines :: [Maybe CTimestamptz]}::timestamptz[], #{indexes :: [Maybe CInt4]}::int[]
        )
        RETURNING @{id :: CInt8}, @{name :: CText}
      |]

-- | Insert the edges of a plan, from each predecessor to each successor.
insertEdgesSQL :: SchemaName -> [Int64] -> [Int64] -> Query ()
insertEdgesSQL schemaName fromSteps toSteps =
  let edges = workflowEdgesTable schemaName
   in [sql|
        INSERT INTO ${edges} (from_step, to_step)
        SELECT * FROM unnest(#{fromSteps :: [CInt8]}::bigint[], #{toSteps :: [CInt8]}::bigint[])
        ON CONFLICT DO NOTHING
      |]

-- | Take the runs row exclusive. It is the top lock of every settle, signal and
-- cancel, and it serializes them within one run.
lockRunSQL :: SchemaName -> RunId -> Query Text
lockRunSQL schemaName (RunId runId) =
  let runs = workflowRunsTable schemaName
   in [sql|SELECT @{status :: CText} FROM ${runs} WHERE id = #{runId :: CInt8} FOR UPDATE|]

-- | The step one job belongs to. No row means the job is not a step.
stepForJobSQL :: SchemaName -> Text -> Int64 -> Query (Int64, Int64, Text, Text)
stepForJobSQL schemaName queue jobId =
  let steps = workflowStepsTable schemaName
   in [sql|
        SELECT @{id :: CInt8}, @{run_id :: CInt8}, @{kind :: CText}, @{status :: CText}
        FROM ${steps}
        WHERE queue = #{queue :: CText} AND job_id = #{jobId :: CInt8}
      |]

-- | Write a step's output and mark it done. Outputs are immutable after settle, so a
-- step already done keeps the one it has.
completeStepSQL :: SchemaName -> StepId -> Maybe Value -> Query Int64
completeStepSQL schemaName (StepId stepId) output =
  let steps = workflowStepsTable schemaName
   in [sql|
        UPDATE ${steps}
        SET status = ${stepDoneLit}, output = #{output :: Maybe CJsonb}, wait_count = 0
        WHERE id = #{stepId :: CInt8} AND status <> ${stepDoneLit}
        RETURNING @{id :: CInt8}
      |]

-- | Lock this step's dependents ascending, take one off each wait count, and report
-- the ones that reached zero. Those are now ready.
advanceDependentsSQL :: SchemaName -> StepId -> Query (Int64, Text, Text, Maybe Text, Maybe Int64)
advanceDependentsSQL schemaName (StepId stepId) =
  let steps = workflowStepsTable schemaName
      edges = workflowEdgesTable schemaName
   in [sql|
        WITH locked AS (
          SELECT step.id FROM ${steps} step
          JOIN ${edges} edge ON edge.to_step = step.id
          WHERE edge.from_step = #{stepId :: CInt8} AND step.status = ${stepWaitingLit}
          ORDER BY step.id
          FOR UPDATE OF step
        ),
        bumped AS (
          UPDATE ${steps} step
          SET wait_count = GREATEST(step.wait_count - 1, 0),
              status = CASE WHEN step.wait_count <= 1 THEN ${stepReadyLit} ELSE step.status END
          WHERE step.id IN (SELECT id FROM locked)
          RETURNING step.id, step.wait_count, step.name, step.kind, step.queue, step.job_id
        )
        SELECT @{id :: CInt8}, @{name :: CText}, @{kind :: CText}, @{queue :: Maybe CText}, @{job_id :: Maybe CInt8}
        FROM bumped WHERE wait_count = 0
      |]

-- | Finish a run whose steps have all settled, with the outputs of its sinks. No row
-- means the run has steps left.
finishRunSQL :: SchemaName -> RunId -> Query Int64
finishRunSQL schemaName (RunId runId) =
  let runs = workflowRunsTable schemaName
      steps = workflowStepsTable schemaName
      edges = workflowEdgesTable schemaName
   in [sql|
        UPDATE ${runs}
        SET status = ${runDoneLit},
            finished_at = NOW(),
            output = (
              SELECT jsonb_object_agg(sink.name, COALESCE(sink.output, 'null'::jsonb))
              FROM ${steps} sink
              WHERE sink.run_id = #{runId :: CInt8}
                AND sink.status = ${stepDoneLit}
                AND NOT EXISTS (SELECT 1 FROM ${edges} edge WHERE edge.from_step = sink.id)
            )
        WHERE id = #{runId :: CInt8} AND status = ${runRunningLit}
          AND NOT EXISTS (
            SELECT 1 FROM ${steps} step
            WHERE step.run_id = #{runId :: CInt8} AND step.status IN ${unsettledLit}
          )
        RETURNING @{id :: CInt8}
      |]

-- | Close a run in a terminal status.
closeRunSQL :: SchemaName -> RunId -> RunStatus -> Query Int64
closeRunSQL schemaName (RunId runId) status =
  let runs = workflowRunsTable schemaName
      statusText = runStatusText status
   in [sql|
        UPDATE ${runs}
        SET status = #{statusText :: CText}, finished_at = NOW()
        WHERE id = #{runId :: CInt8} AND status = ${runRunningLit}
        RETURNING @{id :: CInt8}
      |]

-- | Cancel the steps of a run that have not settled, ascending. The settled ones keep
-- their outputs.
cancelStepsSQL :: SchemaName -> RunId -> Query Int64
cancelStepsSQL schemaName (RunId runId) =
  let steps = workflowStepsTable schemaName
   in [sql|
        WITH locked AS (
          SELECT id FROM ${steps}
          WHERE run_id = #{runId :: CInt8} AND status IN ${unsettledLit}
          ORDER BY id
          FOR UPDATE
        )
        UPDATE ${steps} SET status = ${stepCancelledLit}
        WHERE id IN (SELECT id FROM locked)
        RETURNING @{id :: CInt8}
      |]

-- | The jobs behind a run's unsettled steps, by queue.
runJobsSQL :: SchemaName -> RunId -> Query (Text, Int64)
runJobsSQL schemaName (RunId runId) =
  let steps = workflowStepsTable schemaName
   in [sql|
        SELECT @{queue :: CText}, @{job_id :: CInt8}
        FROM ${steps}
        WHERE run_id = #{runId :: CInt8}
          AND queue IS NOT NULL AND job_id IS NOT NULL
          AND status IN ${unsettledLit}
        ORDER BY queue, job_id
      |]

-- | A run's steps that are ready, in id order.
readyStepsSQL :: SchemaName -> RunId -> Query (Int64, Text, Text, Maybe Text, Maybe Int64)
readyStepsSQL schemaName (RunId runId) =
  let steps = workflowStepsTable schemaName
   in [sql|
        SELECT @{id :: CInt8}, @{name :: CText}, @{kind :: CText}, @{queue :: Maybe CText}, @{job_id :: Maybe CInt8}
        FROM ${steps} WHERE run_id = #{runId :: CInt8} AND status = ${stepReadyLit}
        ORDER BY id
      |]

-- | The step of a run waiting on one signal key.
waitingSignalStepSQL :: SchemaName -> RunId -> Text -> Query Int64
waitingSignalStepSQL schemaName (RunId runId) key =
  let steps = workflowStepsTable schemaName
   in [sql|
        SELECT @{id :: CInt8} FROM ${steps}
        WHERE run_id = #{runId :: CInt8} AND signal_key = #{key :: CText}
          AND kind = ${kindSignalLit} AND status IN ${unsettledLit}
        ORDER BY id
        LIMIT 1
      |]

-- | Delete the runs that finished before the cutoff. Their steps and edges cascade.
purgeFinishedRunsSQL :: SchemaName -> UTCTime -> Int -> Query Int64
purgeFinishedRunsSQL schemaName cutoff limit =
  let runs = workflowRunsTable schemaName
      capped = fromIntegral limit :: Int64
   in [sql|
        WITH expired AS (
          SELECT id FROM ${runs}
          WHERE finished_at IS NOT NULL AND finished_at < #{cutoff :: CTimestamptz}
          ORDER BY id
          LIMIT #{capped :: CInt8}
        ),
        purged AS (
          DELETE FROM ${runs} WHERE id IN (SELECT id FROM expired) RETURNING id
        )
        SELECT count(*) AS @{count :: CInt8} FROM purged
      |]

-- | The definition a run interprets, its input, and the span its jobs inherit.
runDefinitionSQL :: SchemaName -> RunId -> Query (Text, Int32, Value, Maybe Text, Maybe Text)
runDefinitionSQL schemaName (RunId runId) =
  let runs = workflowRunsTable schemaName
   in [sql|
        SELECT @{workflow :: CText}, @{version :: CInt4}, @{input :: CJsonb},
               @{traceparent :: Maybe CText}, @{tracestate :: Maybe CText}
        FROM ${runs} WHERE id = #{runId :: CInt8}
      |]

-- | How a run's steps come to exist.
runModeSQL :: SchemaName -> RunId -> Query Text
runModeSQL schemaName (RunId runId) =
  let runs = workflowRunsTable schemaName
   in [sql|SELECT @{mode :: CText} FROM ${runs} WHERE id = #{runId :: CInt8}|]

-- | What a named checkpoint of the run behind one job has stored. No row means the
-- checkpoint has yet to run, or the job belongs to no run.
checkpointOutputSQL :: SchemaName -> Text -> Int64 -> Text -> Query (Maybe Value)
checkpointOutputSQL schemaName queue jobId name =
  let steps = workflowStepsTable schemaName
   in [sql|
        SELECT @{output :: Maybe CJsonb} FROM ${steps}
        WHERE name = #{name :: CText}
          AND status = ${stepDoneLit}
          AND run_id = (
            SELECT run_id FROM ${steps} WHERE queue = #{queue :: CText} AND job_id = #{jobId :: CInt8}
          )
      |]

-- | Record a checkpoint of the run behind one job. A job no run owns records nothing.
insertCheckpointSQL :: SchemaName -> Text -> Int64 -> Text -> Value -> Query Int64
insertCheckpointSQL schemaName queue jobId name output =
  let steps = workflowStepsTable schemaName
   in [sql|
        INSERT INTO ${steps} (run_id, name, kind, status, output)
        SELECT run_id, #{name :: CText}, ${kindCheckpointLit}, ${stepDoneLit}, #{output :: CJsonb}
        FROM ${steps} WHERE queue = #{queue :: CText} AND job_id = #{jobId :: CInt8}
        RETURNING @{id :: CInt8}
      |]

-- | Finish a run with an output of its own, whatever its steps say.
finishRunWithSQL :: SchemaName -> RunId -> Maybe Value -> Query Int64
finishRunWithSQL schemaName (RunId runId) output =
  let runs = workflowRunsTable schemaName
   in [sql|
        UPDATE ${runs}
        SET status = ${runDoneLit}, finished_at = NOW(), output = #{output :: Maybe CJsonb}
        WHERE id = #{runId :: CInt8} AND status = ${runRunningLit}
        RETURNING @{id :: CInt8}
      |]

-- | Every output a run's settled steps have stored.
runOutputsSQL :: SchemaName -> RunId -> Query (Text, Maybe Value)
runOutputsSQL schemaName (RunId runId) =
  let steps = workflowStepsTable schemaName
   in [sql|
        SELECT @{name :: CText}, @{output :: Maybe CJsonb}
        FROM ${steps} WHERE run_id = #{runId :: CInt8} AND status = ${stepDoneLit}
      |]

-- | A run's step names and ids.
runStepIdsSQL :: SchemaName -> RunId -> Query (Text, Int64)
runStepIdsSQL schemaName (RunId runId) =
  let steps = workflowStepsTable schemaName
   in [sql|SELECT @{name :: CText}, @{id :: CInt8} FROM ${steps} WHERE run_id = #{runId :: CInt8}|]

-- | Point a step at the job just inserted for it.
setStepJobSQL :: SchemaName -> StepId -> Text -> Int64 -> Query Int64
setStepJobSQL schemaName (StepId stepId) queue jobId =
  let steps = workflowStepsTable schemaName
   in [sql|
        UPDATE ${steps} SET queue = #{queue :: CText}, job_id = #{jobId :: CInt8}
        WHERE id = #{stepId :: CInt8}
        RETURNING @{id :: CInt8}
      |]

-- | Count each newly inserted step's predecessors that have yet to settle, and make
-- the ones with none ready. Returns those.
reconcileNewStepsSQL :: SchemaName -> [Int64] -> Query (Int64, Text, Text, Maybe Text, Maybe Int64)
reconcileNewStepsSQL schemaName stepIds =
  let steps = workflowStepsTable schemaName
      edges = workflowEdgesTable schemaName
   in [sql|
        WITH locked AS (
          SELECT id FROM ${steps} WHERE id = ANY(#{stepIds :: [CInt8]}) AND status = ${stepWaitingLit}
          ORDER BY id
          FOR UPDATE
        ),
        pending AS (
          SELECT locked.id, count(predecessor.id) FILTER (WHERE predecessor.status <> ${stepDoneLit}) AS waiting
          FROM locked
          LEFT JOIN ${edges} edge ON edge.to_step = locked.id
          LEFT JOIN ${steps} predecessor ON predecessor.id = edge.from_step
          GROUP BY locked.id
        ),
        settled AS (
          UPDATE ${steps} step
          SET wait_count = pending.waiting,
              status = CASE WHEN pending.waiting = 0 THEN ${stepReadyLit} ELSE ${stepWaitingLit} END
          FROM pending
          WHERE step.id = pending.id
          RETURNING step.id, step.wait_count, step.name, step.kind, step.queue, step.job_id
        )
        SELECT @{id :: CInt8}, @{name :: CText}, @{kind :: CText}, @{queue :: Maybe CText}, @{job_id :: Maybe CInt8}
        FROM settled WHERE wait_count = 0
      |]

-- | A run's failed steps, oldest first.
failedStepsSQL :: SchemaName -> RunId -> Query (Int64, Text, Maybe Text, Maybe Int64)
failedStepsSQL schemaName (RunId runId) =
  let steps = workflowStepsTable schemaName
   in [sql|
        SELECT @{id :: CInt8}, @{name :: CText}, @{queue :: Maybe CText}, @{job_id :: Maybe CInt8}
        FROM ${steps} WHERE run_id = #{runId :: CInt8} AND status = ${stepFailedLit}
        ORDER BY id
      |]

-- | Put a failed run back to running.
reopenRunSQL :: SchemaName -> RunId -> Query Int64
reopenRunSQL schemaName (RunId runId) =
  let runs = workflowRunsTable schemaName
   in [sql|
        UPDATE ${runs} SET status = ${runRunningLit}, finished_at = NULL
        WHERE id = #{runId :: CInt8} AND status = ${runFailedLit}
        RETURNING @{id :: CInt8}
      |]

-- | Put a failed step back to ready.
reopenStepSQL :: SchemaName -> StepId -> Query Int64
reopenStepSQL schemaName (StepId stepId) =
  let steps = workflowStepsTable schemaName
   in [sql|
        UPDATE ${steps} SET status = ${stepReadyLit}
        WHERE id = #{stepId :: CInt8} AND status = ${stepFailedLit}
        RETURNING @{id :: CInt8}
      |]

-- | The dead-letter row one job left behind, if it is there.
deadLetterOfSQL :: SchemaName -> Text -> Int64 -> Query Int64
deadLetterOfSQL schemaName queue jobId =
  let dlq = jobQueueDLQTable schemaName queue
   in [sql|SELECT @{id :: CInt8} FROM ${dlq} WHERE job_id = #{jobId :: CInt8} ORDER BY id DESC LIMIT 1|]

-- | Mark one step failed. The run's other steps run to their own end.
failStepSQL :: SchemaName -> StepId -> Query Int64
failStepSQL schemaName (StepId stepId) =
  let steps = workflowStepsTable schemaName
   in [sql|
        UPDATE ${steps} SET status = ${stepFailedLit}
        WHERE id = #{stepId :: CInt8} AND status IN ${unsettledLit}
        RETURNING @{id :: CInt8}
      |]

-- | The failed steps of failed runs that still name a job, oldest run first.
failedRunStepsSQL :: SchemaName -> Int -> Query (Int64, Int64, Text, Int64)
failedRunStepsSQL schemaName limit =
  let runs = workflowRunsTable schemaName
      steps = workflowStepsTable schemaName
      capped = fromIntegral limit :: Int64
   in [sql|
        SELECT @{run_id :: CInt8}, @{id :: CInt8}, @{queue :: CText}, @{job_id :: CInt8}
        FROM ${steps} step
        WHERE step.status = ${stepFailedLit}
          AND step.queue IS NOT NULL AND step.job_id IS NOT NULL
          AND EXISTS (SELECT 1 FROM ${runs} run WHERE run.id = step.run_id AND run.status = ${runFailedLit})
        ORDER BY step.run_id, step.id
        LIMIT #{capped :: CInt8}
      |]

-- | Fail every signal step whose deadline has passed, and the run behind each, in one
-- pass. Takes the runs rows first and the step rows ascending, the order every path
-- shares. Returns the steps failed.
expireSignalStepsSQL :: SchemaName -> Int -> Query Int64
expireSignalStepsSQL schemaName limit =
  let runs = workflowRunsTable schemaName
      steps = workflowStepsTable schemaName
      capped = fromIntegral limit :: Int64
   in [sql|
        WITH expired AS (
          SELECT id, run_id FROM ${steps}
          WHERE kind = ${kindSignalLit} AND status IN ${unsettledLit}
            AND deadline IS NOT NULL AND deadline <= NOW()
          ORDER BY id
          LIMIT #{capped :: CInt8}
        ),
        locked_runs AS (
          SELECT id FROM ${runs}
          WHERE id IN (SELECT run_id FROM expired)
          ORDER BY id
          FOR UPDATE
        ),
        locked_steps AS (
          SELECT id FROM ${steps}
          WHERE id IN (SELECT id FROM expired) AND status IN ${unsettledLit}
          ORDER BY id
          FOR UPDATE
        ),
        failed AS (
          UPDATE ${steps} SET status = ${stepFailedLit}
          WHERE id IN (SELECT id FROM locked_steps)
          RETURNING id
        ),
        closed AS (
          UPDATE ${runs} SET status = ${runFailedLit}, finished_at = NOW()
          WHERE id IN (SELECT id FROM locked_runs) AND status = ${runRunningLit}
            AND EXISTS (SELECT 1 FROM failed)
          RETURNING id
        )
        SELECT count(*) AS @{result :: CInt8} FROM failed
      |]

-- | Put a set of failed runs and their failed steps back to work in one pass. Takes
-- the runs rows first and the step rows ascending. Returns the runs revived.
reviveStepsSQL :: SchemaName -> [Int64] -> [Int64] -> Query Int64
reviveStepsSQL schemaName runIds stepIds =
  let runs = workflowRunsTable schemaName
      steps = workflowStepsTable schemaName
   in [sql|
        WITH locked_runs AS (
          SELECT id FROM ${runs}
          WHERE id = ANY(#{runIds :: [CInt8]}::bigint[]) AND status = ${runFailedLit}
          ORDER BY id
          FOR UPDATE
        ),
        locked_steps AS (
          SELECT id FROM ${steps}
          WHERE id = ANY(#{stepIds :: [CInt8]}::bigint[]) AND status = ${stepFailedLit}
            AND run_id IN (SELECT id FROM locked_runs)
          ORDER BY id
          FOR UPDATE
        ),
        revived AS (
          UPDATE ${steps} SET status = ${stepReadyLit}
          WHERE id IN (SELECT id FROM locked_steps)
          RETURNING run_id
        ),
        reopened AS (
          UPDATE ${runs} SET status = ${runRunningLit}, finished_at = NULL
          WHERE id IN (SELECT run_id FROM revived)
          RETURNING id
        )
        SELECT count(*) AS @{result :: CInt8} FROM reopened
      |]

-- | Which of these job ids are still in one queue.
presentJobsSQL :: SchemaName -> Text -> [Int64] -> Query Int64
presentJobsSQL schemaName queue jobIds =
  let jobs = jobQueueTable schemaName queue
   in [sql|
        SELECT @{id :: CInt8} FROM ${jobs} WHERE id = ANY(#{jobIds :: [CInt8]}::bigint[])
      |]

-- | One run.
getRunSQL :: SchemaName -> RunId -> Query (Either Text RunRow)
getRunSQL schemaName (RunId runId) =
  let runs = workflowRunsTable schemaName
   in rows
        runRowCodec
        [sql|
          SELECT id, workflow, version, status, input, output, traceparent, tracestate, started_at, finished_at
          FROM ${runs} WHERE id = #{runId :: CInt8}
        |]

-- | A run's steps, in insertion order.
listStepsSQL :: SchemaName -> RunId -> Query (Either Text StepRow)
listStepsSQL schemaName (RunId runId) =
  let steps = workflowStepsTable schemaName
   in rows
        stepRowCodec
        [sql|
          SELECT id, run_id, name, kind, status, queue, job_id, wait_count, output, signal_key, deadline, step_index
          FROM ${steps} WHERE run_id = #{runId :: CInt8} ORDER BY id
        |]

runRowCodec :: RowCodec (Either Text RunRow)
runRowCodec =
  toRunRow
    <$> col "id" CInt8
    <*> col "workflow" CText
    <*> col "version" CInt4
    <*> col "status" CText
    <*> col "input" CJsonb
    <*> ncol "output" CJsonb
    <*> ncol "traceparent" CText
    <*> ncol "tracestate" CText
    <*> col "started_at" CTimestamptz
    <*> ncol "finished_at" CTimestamptz
  where
    toRunRow runId workflow version status input output traceparent tracestate startedAt finishedAt = do
      decoded <- runStatusFromText status
      pure (RunRow (RunId runId) workflow version decoded input output traceparent tracestate startedAt finishedAt)

stepRowCodec :: RowCodec (Either Text StepRow)
stepRowCodec =
  toStepRow
    <$> col "id" CInt8
    <*> col "run_id" CInt8
    <*> col "name" CText
    <*> col "kind" CText
    <*> col "status" CText
    <*> ncol "queue" CText
    <*> ncol "job_id" CInt8
    <*> col "wait_count" CInt4
    <*> ncol "output" CJsonb
    <*> ncol "signal_key" CText
    <*> ncol "deadline" CTimestamptz
    <*> ncol "step_index" CInt4
  where
    toStepRow stepId runId name kind status queue jobId waitCount output signalKey deadline stepIndex = do
      decodedKind <- stepKindFromText kind
      decodedStatus <- stepStatusFromText status
      pure
        StepRow
          { stepRowId = StepId stepId
          , stepRowRun = RunId runId
          , stepRowName = StepName name
          , stepRowKind = decodedKind
          , stepRowStatus = decodedStatus
          , stepRowQueue = queue
          , stepRowJobId = jobId
          , stepRowWaitCount = waitCount
          , stepRowOutput = output
          , stepRowSignalKey = signalKey
          , stepRowDeadline = deadline
          , stepRowIndex = stepIndex
          }

# Workflows

A workflow is a directed acyclic graph of steps. Each step is one job in one
queue. The database holds the runs, the steps and their outputs.

```haskell
{-# LANGUAGE ApplicativeDo #-}
{-# LANGUAGE DataKinds #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE QualifiedDo #-}

import Arbiter.Workflow
import Arbiter.Workflow.Graph qualified as Graph

type Fulfilment =
  '[ QueueWithResult "payment_jobs"  Payment  Text
   , QueueWithResult "stock_jobs"    Stock    Reservation
   , QueueWithResult "dispatch_jobs" Dispatch Text
   , QueueWithResult "notice_jobs"   Notice   Text
   ]

-- Both arms of the branch below embed this fragment.
notifyCustomer :: Workflow Fulfilment (Text, Text) Text
notifyCustomer = Workflow "notify-customer" 1 $ \request ->
  step "send" (uncurry Email) (use request)

fulfilOrder :: Workflow Fulfilment Order Text
fulfilOrder = Workflow "fulfil-order" 1 $ \order -> Graph.do
  paid <- stepWith "charge" chargeFor (use order)

  held <- forEach (orderLines <$> use order) $ \line ->
    step "hold" Hold (use line)

  branch
    (shippable <$> use order <*> use held)
    ( \ready -> Graph.do
        tracking <- step "ship" shipmentFor (use ready)
        arrival  <- signal "carrier-delivered" (72 * 3600)
        embed "say-it-shipped" notifyCustomer $ do
          shipment  <- use ready
          reference <- use tracking
          landed    <- use arrival
          pure (buyerOf shipment, "Order shipped as " <> reference <> ", " <> landed)
    )
    ( \short -> Graph.do
        refunded <- step "refund" (Refund . snd) (use short)
        embed "say-we-refunded" notifyCustomer $ do
          shortfall <- use short
          reference <- use refunded
          charged   <- use paid
          pure (fst shortfall, "Out of stock, refunded " <> charged <> " as " <> reference)
    )
```

A step's queue and result type come from the registry. `Graph.do` needs
`QualifiedDo`, and an `Expr` block needs `ApplicativeDo`.

| Combinator | Makes |
| --- | --- |
| `step name build input` | One job in the queue its payload names |
| `stepWith name build input` | The same, from a whole `JobWrite` |
| `signal key seconds` | A wait for an external value, with a deadline |
| `branch cond onLeft onRight` | Both arms. A run materializes one |
| `choose cond whenTrue whenFalse` | A branch on a `Bool`, with nothing to carry into the arms |
| `expand input f` | A subgraph `f` builds at settle time, from a real value |
| `forEach list f` | One subgraph for each element, merged in order |
| `embed name definition input` | Another definition's graph, under a name of its own |
| `under name graph` | The same, for a fragment built in place |

`stepWith` sets the group key, the priority, the dedup key or the attempt budget
of one step:

```haskell
chargeFor :: Order -> JobWrite Payment
chargeFor order =
  setGroupKey (Just (customer order))
    . setMaxAttempts (Just 3)
    $ defaultJob (Charge (customer order) (total order))
```

## Names

A step name is a type. Two steps that share one name do not compile:

```haskell
Workflow "fulfil-order" 1 $ \order -> Graph.do
  paid  <- stepWith "charge" chargeFor (use order)
  again <- stepWith "charge" chargeFor (use order)
  -- error: [GHC-64725] two steps are named charge
```

Each `branch` arm, each `forEach` child, each `expand` body and each `embed` or
`under` subgraph names its steps among its own. Two arms can both use the name
`each`.

A name is one path segment. A name that holds a `.` or an `@` does not compile
either: the first separates the path, and the second is what the builder names
its own steps with. The full name of a step is its path:
`say-it-shipped.send`, `expand@0.2.hold`.

## Step inputs

A step reads the outputs of the steps before it through an `Expr`:

```haskell
step "report" Report ((,) <$> use loaded <*> use files)
step "report" Report (both loaded files)
```

With `ApplicativeDo`:

```haskell
step "report" Report $ do
  loaded <- use loadedRef
  files  <- use filesRef
  pure (loaded, files)
```

Bind a plain variable in these blocks. A tuple pattern needs `Monad`, which
`Expr` does not have. For the same reason, a block that reads a value to decide
what to read next does not compile. Use `expand` to make the shape of a graph
depend on a value.

A `Ref` belongs to the graph that made it. To give one to another definition is
a type error.

## Running

```haskell
definitions :: WorkflowRegistry Fulfilment
definitions = workflows [workflow fulfilOrder, workflow notifyCustomer]

runPayments env = do
  config <- transactionalWorkerConfig 4 paymentHandler
  runSimpleDb env (runWorkerPool (withWorkflows definitions config))

placeOrder = startWorkflow definitions fulfilOrder

carrierWebhook runId note =
  sendWorkflowSignal definitions runId "carrier-delivered" (toJSON note)
```

`withWorkflows` sets the settle, the dead-letter failure path, the maintenance
sweep and the record that makes each job a schedule fires a checkpoint run. The
reaper runs the sweep on its own cadence, behind a gate of its own, so one pool
in a deployment runs it per interval.

A step's handler is an ordinary handler for its queue.

The job of a step goes in when the step becomes ready. Its payload is the
computed input, which the predecessors of the step supply.

## Storage

The three tables ship as one tracked migration:

```haskell
runMigrationsForRegistry
  (Proxy @Fulfilment)
  connStr
  "arbiter"
  defaultMigrationConfig {extraMigrations = workflowMigrations "arbiter"}
```

## Operations

| Call | Does |
| --- | --- |
| `startWorkflow definitions wf input` | Starts a run |
| `startWorkflowByName definitions name version input` | The same, from JSON |
| `getRun` / `listSteps` | Reads a run and its steps |
| `cancelRun` | Cancels the run, its steps and the jobs behind them |
| `retryRun` | Puts a failed run back to work, from the dead-letter queue if the job went there |
| `sendWorkflowSignal` | Delivers a value to a waiting signal step |
| `render` | The static graph: both arms, one node for each continuation |

The other steps of a failed run keep running. An expired signal leaves a failed
step with no job, and `retryRun` refuses that run. Start a new one instead. A run
that comes back from the dead-letter queue returns to `running` at the next
maintenance sweep.

## Checkpoints

The handler can make the steps of a run, in place of a definition:

```haskell
handler job callbacks = do
  charged  <- checkpoint job "charge"  (chargeCard order)
  invoiced <- checkpoint job "invoice" (sendInvoice order)
  ackWith callbacks job (charged <> "/" <> invoiced)

runId <- startCheckpointRun @Fulfilment "billing" 1 (toJSON order) (defaultJob (Task "go"))
```

Each checkpoint runs once for the run. A handler that fails part way runs again
and skips what it recorded. A second call under one name gives back the stored
value and does not run the action.

Use `manualWorkerConfig` or a batched pool. A checkpoint commits when the
handler reaches it, and `transactionalWorkerConfig` rolls the whole handler back
together with its checkpoints.

Checkpoints are at-least-once. An action that finishes before its row commits
runs again.

A job that belongs to no run records nothing and runs its actions each time.

`getRun`, `listSteps`, `cancelRun` and the maintenance sweep work the same for
both kinds of run.

| | Graph run | Checkpoint run |
| --- | --- | --- |
| Steps are | jobs, one for each step, any queue | calls inside one job |
| Retries and limits | the queue's | the one job's |
| A long wait | suspends the run | holds a worker |
| Shape from a value | `expand` | ordinary control flow |

See the [`Arbiter.Workflow` haddocks](https://arbiterq.dev/arbiter-workflow/Arbiter-Workflow.html)
for the full API. The example on this page compiles as
`Test.Arbiter.Workflow.Example` in the `arbiter-workflow` test suite.

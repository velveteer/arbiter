<h1 align="left">
    <img src="./arbiter.png" height=40 width=40 />
    Arbiter
</h1>

An opinionated, production-ready PostgreSQL job queue for Haskell applications.

[![Live Demo](https://img.shields.io/badge/Live_Demo-2ea44f?style=for-the-badge&logo=postgresql&logoColor=white)](https://demo.arbiterq.dev/)
[![API Docs](https://img.shields.io/badge/API_Docs-5e5086?style=for-the-badge&logo=haskell&logoColor=white)](https://velveteer.github.io/arbiter/)
[![CI](https://img.shields.io/github/actions/workflow/status/velveteer/arbiter/ci.yml?branch=main&style=for-the-badge&label=CI)](https://github.com/velveteer/arbiter/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue?style=for-the-badge)](./LICENSE)

- Transactional job processing - jobs and database operations commit together
- At-least-once delivery with visibility timeouts and heartbeats
- Per-group ordering (partitioned FIFO)
- Concurrent worker pools with `LISTEN/NOTIFY` and polling fallback
- Dead-letter queues
- Job trees with fan-out/fan-in result collection
- Cron/periodic job scheduling
- Job deduplication via unique keys
- Cross-queue per-job rate limiting with operator-tunable token-bucket policies
- Cross-queue per-job concurrency limits - at most N jobs sharing a key in flight
- Observability callbacks, structured logging
- REST API with SSE and an embedded admin UI
- File-based liveness probes for Kubernetes / systemd
- Extensive test coverage (1,000+ integration tests)

> [!NOTE]
>
> The API is subject to breaking changes. A Hackage release following PVP is tentative.

## Installation

Install directly from GitHub:

**Cabal** - add to your `cabal.project`:

```
source-repository-package
  type: git
  location: https://github.com/velveteer/arbiter.git
  tag: <commit-sha>
  subdir:
    arbiter-core
    arbiter-worker
    arbiter-simple
    arbiter-migrations
```

**Stack** - add to your `stack.yaml`:

```yaml
extra-deps:
  - git: https://github.com/velveteer/arbiter.git
    commit: <commit-sha>
    subdirs:
      - arbiter-core
      - arbiter-worker
      - arbiter-simple
      - arbiter-migrations
```

Replace `arbiter-simple` with `arbiter-orville` or `arbiter-hasql` depending on your backend.

## Quick Start

### Payload Types

Define payload types with `ToJSON` and `FromJSON` instances.

```haskell
data EmailPayload
  = SendWelcome Text Text
  | SendReceipt Text Int
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

data ImagePayload
  = ResizeImage Text Int Int
  | GenerateThumbnail Text
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)
```

### Type-Level Registry

Map queue table names to payload types at the type level.

```haskell
type AppRegistry =
  '[ '("email_queue", EmailPayload)
   , '("image_queue", ImagePayload)
   ]
```

The registry is enforced at compile time - each payload type maps to exactly one table, and duplicate table names are a type error.

### Migrations

```haskell
import Arbiter.Migrations qualified as Mig
import Data.Proxy (Proxy (..))
import System.Exit (die)

main :: IO ()
main = do
  result <- Mig.runMigrationsForRegistry (Proxy @AppRegistry) connStr "arbiter" Mig.defaultMigrationConfig
  case result of
    Mig.MigrationSuccess -> putStrLn "Migrations complete"
    Mig.MigrationError err -> die $ "Migration failed: " <> err
```

If the database user lacks `CREATE` privilege on the schema, create it manually first:

```sql
CREATE SCHEMA IF NOT EXISTS arbiter;
GRANT USAGE, CREATE ON SCHEMA arbiter TO your_app_user;
```

### Inserting Jobs

```haskell
import Arbiter.Core qualified as Arb
import Arbiter.Simple qualified as ArbS
import Data.Proxy (Proxy (..))

-- A producer doesn't need to configure a worker pool env, it can use default settings
env <- ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"

ArbS.runSimpleDb env $ do
  -- Ungrouped - processed concurrently by any available worker
  _ <- Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")

  -- Grouped - jobs with the same group key are processed serially (one at a time)
  _ <- Arb.insertJob (Arb.defaultGroupedJob "user-42" $ SendReceipt "alice@example.com" 1001)
```

`insertJob` returns `Maybe (JobRead payload)` - `Nothing` when a dedup key causes the insert to be skipped.

### Processing Jobs

```haskell
import Arbiter.Core qualified as Arb
import Arbiter.Simple qualified as ArbS
import Arbiter.Worker qualified as Worker
import Control.Monad (void)
import Control.Monad.IO.Class (liftIO)
import Database.PostgreSQL.Simple qualified as PG

main :: IO ()
main = do
  -- 1 pool of 5 concurrent worker threads, using postgresql-simple via arbiter-simple backend
  config <- Worker.transactionalWorkerConfig 5 processEmail
  poolCfg <- Worker.poolConfigForWorkers [Worker.namedWorkerPool config]
  env <- ArbS.createSimpleEnvWithConfig (Proxy @AppRegistry) connStr "arbiter" poolCfg
  ArbS.runSimpleDb env $ Worker.runWorkerPool config

processEmail :: Arb.JobHandler (ArbS.SimpleDb AppRegistry IO) EmailPayload ()
processEmail conn job = do
  case Arb.payload job of
    SendWelcome recipient name -> do
      result <- liftIO $ sendEmail recipient ("Welcome, " <> name)
      case result of
        Left err -> Arb.throwRetryable err
        Right () -> pure ()

    SendReceipt recipient orderId -> do
      -- Transactional: this INSERT and the job ack commit together
      void $ liftIO $ PG.execute conn
        "INSERT INTO email_log (recipient, order_id) VALUES (?, ?)"
        (recipient, orderId)
```

`transactionalWorkerConfig`, above, wraps each handler in a transaction and
acks for you. If the handler succeeds, the job is deleted and all its database
work commits atomically. If it throws, the transaction rolls back and the job
is retried or moved to the DLQ. The transaction stays open for the whole
handler, so the handler's writes are guaranteed to land with the ack.

`manualWorkerConfig` opens no transaction and hands you callbacks to finalize
the job yourself - ack it, fail it, cancel it, or leave it to be reprocessed.
You scope a transaction to just the writes that need one, and nothing is held
while the rest of the handler runs:

```haskell
config <- Worker.manualWorkerConfig 5 processEmail

processEmail
    :: Arb.JobRead EmailPayload
    -> Worker.BatchCallbacks (ArbS.SimpleDb AppRegistry IO) EmailPayload ()
    -> ArbS.SimpleDb AppRegistry IO ()
processEmail job cbs = do
  liftIO $ sendEmail (Arb.payload job)
  Worker.ack cbs job
```

To take several jobs per invocation and amortize work across them, see [Batched Handlers](#batched-handlers).

## Architecture

Arbiter has no broker or central coordinator. Every worker pool claims directly from PostgreSQL, so you scale by adding worker processes and there is no leader to elect or fail over.

The lifecycle under `transactionalWorkerConfig`:

1. **Claim** - the dispatcher claims visible jobs (respecting per-group ordering), increments each job's attempt count, and hides it for the visibility timeout. A heartbeat extends that timeout while the handler runs, so long jobs are not reclaimed.
2. **Run** - the worker runs the handler inside a transaction. The handler's database work, its stored result, and the ack all commit together.
3. **Success** - the job is acked and the transaction commits.
4. **Failure** - the transaction rolls back. A separate transaction retries the job with backoff, or moves it to the dead-letter queue (DLQ)
5. **Reclaim** - if another worker stole the job mid-flight (its visibility lapsed), either the heartbeat or the ack will throw an exception to skip the job(s) in an attempt to prevent duplicate work.

Delivery is at-least-once: a job redelivered after a worker crash or visibility-timeout lapse runs again, so handlers with side effects that no transaction covers should be idempotent.

Under `manualWorkerConfig` and `defaultBatchedWorkerConfig` there is no transaction in step 2 - the handler completes, fails, cancels, or nacks each job through callbacks. Claim, heartbeat, and reclaim work the same way.

### Group Ordering

A group key runs a group **one job (or batch) at a time** - serial within the group, concurrent across groups.

- **Same group key** - eligible jobs run in insertion order within priority, except a retrying job keeps the head until it succeeds or dead-letters. A delayed job yields to ready siblings, and a failing job holds the group through its backoff.
- **No group key** - run concurrently by any available worker.

## Job Features

### Priority

Jobs carry an integer `priority` (default `0`), and lower numbers are claimed first. Since every job defaults to `0`, give background work a higher number to defer it behind normal jobs.

```haskell
-- runs behind default-priority work
job = (Arb.defaultJob payload) { Arb.priority = 10 }
```

### Deduplication

Control duplicate job insertion with dedup keys:

```haskell
-- IgnoreDuplicate: silently skip if key exists
job1 = (Arb.defaultJob payload) { Arb.dedupKey = Just (IgnoreDuplicate "order-123") }

-- ReplaceDuplicate: update existing job's payload and reset attempts
job2 = (Arb.defaultJob payload) { Arb.dedupKey = Just (ReplaceDuplicate "order-123") }
```

### Job Trees (Fan-out/Fan-in)

Children run in parallel. Parents run when all of their children are acked or DLQ'd.

```haskell
import Arbiter.Core.JobTree ((<~~))
import Arbiter.Core.JobTree qualified as JT

data PipelinePayload
  = ProcessChunk Text
  | AggregateSection Text
  | Aggregate
  deriving stock (Generic)
  deriving anyclass (ToJSON, FromJSON)

myTree = Arb.defaultJob Aggregate <~~
  [ Arb.defaultJob (ProcessChunk "chunk-1")
  , Arb.defaultJob (ProcessChunk "chunk-2")
  , Arb.defaultJob (ProcessChunk "chunk-3")
  ]
Right _ <- Arb.insertJobTree myTree
```

Multi-level trees use `rollup` and `leaf`:

```haskell
myTree = JT.rollup (Arb.defaultJob Aggregate)
  [ JT.rollup (Arb.defaultJob (AggregateSection "section-1"))
      [ JT.leaf (Arb.defaultJob (ProcessChunk "leaf-1a"))
      , JT.leaf (Arb.defaultJob (ProcessChunk "leaf-1b"))
      ]
  , JT.rollup (Arb.defaultJob (AggregateSection "section-2"))
      [ JT.leaf (Arb.defaultJob (ProcessChunk "leaf-2a"))
      ]
  ]
```

A parent fetches its children's results on demand with
`Worker.mergedChildResults`, which returns the monoidal merge of its immediate
children's results plus a map of any DLQ'd immediate children. Intermediate
results are cleaned up automatically when the parent is acked.

```haskell
handler :: Arb.JobHandler (ArbS.SimpleDb AppRegistry IO) PipelinePayload [Text]
handler _conn job =
  case Arb.payload job of
    ProcessChunk name -> pure ["processed: " <> name]
    AggregateSection name -> do
      (childResults, dlqFailures) <- Worker.mergedChildResults job
      if not (null dlqFailures)
        then Arb.throwPermanent $ name <> ": has failed children"
        else processSection childResults
    Aggregate -> do
      (childResults, _) <- Worker.mergedChildResults job
      sendToS3 childResults

config <- Worker.transactionalWorkerConfig 4 handler
```

Tree-scoped cancellation:

- `throwTreeCancel` - cancels the entire tree (root and all descendants).
- `throwBranchCancel` - DLQs the current child, then cascade-cancels the parent and all siblings.

### Recipe: Chunked Data Migration

Use a job tree to replace a staging table. Each child job carries its chunk of row IDs - the tree tracks completion and the finalizer runs when all chunks are processed:

```haskell
{-# LANGUAGE OverloadedLists #-}

data MigrationJob
  = MigrateChunk [Int64]
  | MigrationComplete

rowIds <- findRowsToMigrate  -- SELECT id FROM orders WHERE needs_migration
let chunks = chunksOf 1000 rowIds  -- from the split package
    tree = Arb.defaultJob MigrationComplete
      <~~ [Arb.defaultJob (MigrateChunk ids) | ids <- chunks]
Right _ <- Arb.insertJobTree tree
```

```haskell
handler conn job = case Arb.payload job of
  MigrateChunk ids -> do
    rowCount <- migrateRows conn ids
    pure (Sum rowCount)

  MigrationComplete -> do
    (Sum totalRows, _) <- Worker.mergedChildResults job
    reportComplete totalRows
    pure (Sum totalRows)
```

### Cron Jobs

```haskell
import Arbiter.Worker.Cron qualified as Cron

Right healthCheck = Cron.cronJob
  "health-check"        -- unique name
  "*/5 * * * *"         -- every 5 minutes (UTC)
  Cron.SkipOverlap      -- skip tick if previous job is still pending/running
  (\_kind tick -> Arb.defaultJob (RunHealthCheck tick))

-- with backfill: catch up on missed ticks after downtime or scheduler delays
Right nightlyReport = Cron.cronJob
  "nightly-report"
  "0 3 * * *"           -- 03:00 UTC daily
  Cron.AllowOverlap     -- each tick produces its own job
  (\kind tick -> (Arb.defaultJob (GenerateReport tick))
    { Arb.priority = case kind of Cron.Replay -> 10; Cron.Live -> 0 })
let nightlyWithBackfill = nightlyReport { Cron.backfill = Cron.Backfill 86400 }

-- in a specific timezone (validated at construction)
Right marketOpen = Cron.cronJobInTimezone
  "market-open"
  "America/New_York"    -- IANA tz name
  "30 9 * * 1-5"        -- 09:30 local, Mon-Fri (DST-aware)
  Cron.SkipOverlap
  (\_kind tick -> Arb.defaultJob (OpeningBell tick))

config <- Worker.transactionalWorkerConfig 4 processEmail
let configWithCron = config
      { Worker.cronJobs = [healthCheck, nightlyWithBackfill, marketOpen] }
```

| Policy | Behavior |
|--------|----------|
| `SkipOverlap` | At most one pending/running job per schedule. |
| `AllowOverlap` | One job per tick. Multiple ticks can run concurrently. |

The builder receives a `TickKind` (`Live` for the current minute, `Replay`
for any catch-up tick) and the tick time.

**Timezones.** Expressions default to UTC. Use `cronJobInTimezone` with an
[IANA name](https://www.iana.org/time-zones) like `America/New_York` to run
in local time instead. DST is handled the way you'd expect: a schedule like
`30 2 * * *` quietly skips itself on the spring-forward day (when 02:30
doesn't exist locally), and a schedule like `30 1 * * *` fires once on the
fall-back day (when 01:30 happens twice).

**Backfill.** `BackfillPolicy` replays missed minutes after downtime or
scheduler pauses, bounded by a duration you specify.

**Runtime overrides.** A schedule's live config - expression, overlap,
timezone, or enabled state - is editable via the REST API or admin UI
without redeploying. Clearing an override (setting it to `null`) falls back
to the value in code.

### Error Handling

```haskell
Arb.throwRetryable "API timeout"       -- retry with backoff
Arb.throwPermanent "Invalid payload"   -- move to DLQ immediately
Arb.throwTreeCancel "Pipeline aborted" -- cancel entire tree
Arb.throwBranchCancel "Subtask failed" -- cancel current branch
Arb.throwNack                          -- reprocess later, not a failure (no attempt consumed)
```

In a batched handler, the same dispositions are available per job through the
`BatchCallbacks` record (`failRetry`, `failPermanent`, `cancelBranch`,
`cancelTree`, `nack`) so one job's outcome does not affect the rest of the
batch. A throw applies to whichever jobs the handler has not yet finalized.

Any unrecognized exception is treated as retryable. Jobs have a configurable
`maxAttempts` (default: 10). After exhausting attempts, the job moves to the
DLQ.

### Rate Limiting

Throttle jobs by an arbitrary key. A limit is shared by every queue in a
registry, so one policy can govern a resource no matter which queues touch it.

Give a payload a `HasRateLimit` instance whose `rateLimitFor` selects a policy
per job. The selector is a small DSL that the migration statically inspects to
collect and seed every policy it can reach, so there is no separate policy list
to keep in sync.

```haskell
import Arbiter.RateLimit

-- Your own functions on the payload.
isTransactional :: EmailPayload -> Bool
recipientDomain :: EmailPayload -> Text

transactional, bulk :: Policy
transactional = tokenBucket "transactional" 100 1 -- 100/second, burst 100
bulk          = tokenBucket "bulk" 1000 3600      -- 1000/hour, burst 1000

instance HasRateLimit EmailPayload where
  rateLimitFor =
    chooseWhen isTransactional
      (limitBy transactional recipientDomain)
      (limitBy bulk recipientDomain)
```

`tokenBucket prefix n period` reads as "n per period, with bursts up to n". To
bound bursts independently of the sustained rate, build a `Policy` directly and
set `policyMax` (burst) apart from `policyRefill` / `policyInterval` (rate).
Weight expensive jobs with `rateLimitCost`, or top up a bucket manually with
`addRateLimitTokens`.

A job denied by its bucket is parked, not polled: it becomes invisible until
the bucket can next afford it, then competes normally again. Throttled counts
are visible per policy in the API and admin UI, and operators can override a
policy's settings at runtime.

A fixed window is just a manual bucket: declare it with a refill of 0 and reset
it at the boundary from a cron.

```haskell
daily :: Policy
daily =
  Policy
    { policyPrefix = "daily"
    , policyMax = 1000
    , policyRefill = 0
    , policyInterval = 86400
    }

-- In an hourly/daily cron at the window boundary:
resetRateLimitBuckets "daily"
```

Bucket state is not durable by default: after a database crash or a failover,
buckets reset to full, so each key can burst up to its max once before settling
back to the sustained rate. When that overshoot is unacceptable - strict
external quotas, or manual buckets holding real credit - migrate the schema
with durable buckets, and bucket state survives restarts at some throughput
cost:

```haskell
runMigrationsForRegistry (Proxy @AppRegistry) connStr "arbiter"
  defaultMigrationConfig { rateLimitDurability = Durable }
```

Durability is a property of the migrated schema, not the registry type, so the
same registry can back an unlogged staging schema and a durable production one.

> [!IMPORTANT]
> Tokens are spent when a job is **claimed**, not when it finishes. A retry or
> a redelivery (worker crash, visibility timeout) spends again, so size
> policies against claims, not successful runs.
>
> A `rateLimitCost` above the bucket's max clamps to the max: the job drains a
> full bucket and runs, rather than blocking forever. A rate limit bounds
> arrivals over time. To bound how many jobs run at once, use a concurrency
> limit.

### Concurrency Limiting

Cap how many jobs sharing a key run at once. A `HasConcurrency` instance names
a **pool** (a prefix with a default limit) and a per-job key suffix. Keys are
global, so a pool spans every queue in a registry.

```haskell
import Arbiter.Concurrency (HasConcurrency (..), concurrencyBy, concurrencyPool)

-- At most 2 sync jobs per tenant in flight at once.
instance HasConcurrency SyncPayload where
  concurrencyFor = concurrencyBy (concurrencyPool "tenant-sync" 2) syncTenant
```

Build the selector from `noConcurrency` / `concurrencyBy` / `globalConcurrency`
/ `concurrencyByCase`. The limit lives on the pool, so every key under the
prefix shares the same cap. Operators retune a whole pool live through the API
or admin UI. An override takes precedence until cleared, then the declared
default applies again. Setting an override of 0 pauses the pool entirely:
nothing under it is claimed until the override is raised or cleared.

#### Concurrency limit 1 vs. a group key

Both cap a key to one job in flight. The difference shows up **on failure**:

| | `group_key` | concurrency limit 1 |
| --- | --- | --- |
| What it is | a scheduling primitive (ordered head per group) | a counter |
| On retry/backoff | the failing job **holds the line** - the group makes no progress until it succeeds or dead-letters | the failing job **releases its slot** - a sibling runs while it backs off |
| Ordering | eligible jobs run in insertion order within priority | none beyond the claim's sort |
| Batching | claims an ordered batch per group | N independent jobs |

Reach for a **group key** to serialize a sequence in order (event streams,
state machines) - a failing job blocks the rest until it succeeds or
dead-letters. Reach for **concurrency 1** as a mutex (one sync per tenant) -
order doesn't matter and a backing-off job yields to the others. They're
orthogonal, so a job can carry both.

> [!IMPORTANT]
> The cap counts claims: a slot is held until its job is acked, retried, or
> reclaimed, so a timed-out-but-unacked job still occupies its slot until then.
>
> Maintenance is automatic. Drained keys are cleaned up periodically, and a
> pool's in-flight accounting recovers on its own after a restart or failover.

## Worker Configuration

See the [WorkerConfig haddocks](https://velveteer.github.io/arbiter/arbiter-worker/Arbiter-Worker-Config.html) for all options.

### Batched Handlers

`defaultBatchedWorkerConfig` is `manualWorkerConfig` with more than one job per
invocation: the handler receives a batch of up to `batchSize` jobs from a
group, so it can amortize work across them. Finalize each one through the same
callbacks.

```haskell
-- defaultBatchedWorkerConfig <workerCount> <batchSize> handler
config <- Worker.defaultBatchedWorkerConfig 10 5 batchHandler

batchHandler
  :: NonEmpty (Arb.JobRead ImagePayload)
  -> Worker.BatchCallbacks (ArbS.SimpleDb AppRegistry IO) ImagePayload ()
  -> ArbS.SimpleDb AppRegistry IO ()
batchHandler jobs cbs = do
  let urls = map (getUrl . Arb.payload) (toList jobs)
  liftIO $ bulkProcess urls
  -- Bulk-ack the whole batch in one transaction.
  Worker.ackAll cbs (toList jobs)
```

Opting out of the worker transaction does not mean giving up atomicity where
you want it. Each callback runs in its own transaction, and wrapping one in
your own `withDbTransaction` commits the ack together with your writes:

```haskell
batchHandler jobs cbs =
  for_ jobs $ \job -> Arb.withDbTransaction $ do
    recordCharge (Arb.payload job)
    Worker.ack cbs job
```

So you pay for a transaction only around the work that needs one, rather than
for the whole handler. Your writes commit with the ack, but `onJobSuccess` does
not - it can fire for a job that is later reprocessed. Keep effects that must
happen exactly once in the transaction next to the ack, not in the hook.

Each job is finalized on its own via the
[`BatchCallbacks`](https://velveteer.github.io/arbiter/arbiter-worker/Arbiter-Worker-Config.html#t:BatchCallbacks)
record - `ack`/`ackAll` (per-job or bulk ack), `failRetry`/`failPermanent`,
`cancelBranch`/`cancelTree`, or `nack`. Dispositions are per job, so a failure,
cancel, or nack affects only that job - completed jobs stay done, an untouched
job is reprocessed, and hooks fire per job.

To store a result per job - for a [job tree's](#job-trees-fan-outfan-in) rollup
parent to collect - use `defaultBatchedResultWorkerConfig` and ack with
`ackWith`/`ackAllWith`:

```haskell
-- defaultBatchedResultWorkerConfig <workerCount> <batchSize> handler
config <- Worker.defaultBatchedResultWorkerConfig 10 5 scoreHandler

scoreHandler
  :: NonEmpty (Arb.JobRead ImagePayload)
  -> Worker.BatchCallbacks (ArbS.SimpleDb AppRegistry IO) ImagePayload Score
  -> ArbS.SimpleDb AppRegistry IO ()
scoreHandler jobs cbs =
  for_ jobs $ \job -> do
    score <- liftIO $ scoreImage (Arb.payload job)
    -- The score lands with the ack, for the rollup parent to collect.
    Worker.ackWith cbs job score
```

### Observability Hooks

```haskell
myHooks = Arb.defaultObservabilityHooks
  { onJobSuccess = \job startTime endTime ->
      liftIO $ recordHistogram "jobs.duration" (diffUTCTime endTime startTime)
  , onJobFailedAndMovedToDLQ = \err job ->
      liftIO $ sendAlert $ "Job " <> show (Arb.primaryKey job) <> " moved to DLQ: " <> err
  , onJobHeartbeat = \job now startTime ->
      liftIO $ recordGauge "jobs.running_duration" (realToFrac $ diffUTCTime now startTime)
  }

config <- Worker.transactionalWorkerConfig 5 handler
let instrumented = config { Worker.observabilityHooks = myHooks }
```

### Graceful Shutdown

Install signal handlers through the setup callback, which receives the shared
shutdown state. One pool or several, it's the same shape - add entries to the
list:

```haskell
import System.Posix.Signals qualified as Signals

emailConfig <- Worker.transactionalWorkerConfig 3 processEmail
imageConfig <- Worker.transactionalWorkerConfig 2 processImage

let workers = [Worker.namedWorkerPool emailConfig, Worker.namedWorkerPool imageConfig]
poolCfg <- Worker.poolConfigForWorkers workers
env <- ArbS.createSimpleEnvWithConfig (Proxy @AppRegistry) connStr "arbiter" poolCfg

ArbS.runSimpleDb env $ Worker.runWorkerPools (Proxy @AppRegistry) workers $ \state -> do
  let shutdown = Signals.Catch $ Worker.signalShutdown state
  void $ Signals.installHandler Signals.sigTERM shutdown Nothing
  void $ Signals.installHandler Signals.sigINT shutdown Nothing
```

The dispatcher stops claiming, in-flight jobs drain within `gracefulShutdownTimeout`, and the process exits.

### Backoff Strategies

```haskell
config { Worker.backoffStrategy = exponentialBackoff 2.0 3600 }  -- base^attempts, cap 1h
config { Worker.backoffStrategy = linearBackoff 30 600 }         -- +30s/attempt, cap 10m
config { Worker.backoffStrategy = constantBackoff 60 }           -- always 60s
config { Worker.backoffStrategy = Custom (\n -> fromIntegral n * 15) }

config { Worker.jitter = FullJitter }   -- random(0, delay)
config { Worker.jitter = EqualJitter }  -- delay/2 + random(0, delay/2) (default)
config { Worker.jitter = NoJitter }
```

### Wakeups (LISTEN/NOTIFY)

Workers wake immediately to claim new jobs, pause/resume, force-cancel (cancel
already claimed jobs), and cron run-now through PostgreSQL's `LISTEN/NOTIFY`
instead of waiting for the next poll. They will use the shared listener hub
that is available from the supplied `MonadArbiter` instance (e.g. from
`SimpleDb`, `HasqlDb`, your own monad).

Without a listener, jobs are still claimed and processed reliably on the
`pollInterval` cadence instead of instantly. The control paths that ride
`LISTEN/NOTIFY` fall back to slower cadences: pause/resume is reconciled at the
worker heartbeat (`workerHeartbeatInterval`), a cron run-now request waits for
the scheduler's next tick, and force-cancel interrupts a running handler at the
next job heartbeat (`jobHeartbeatInterval`) rather than instantly.

The listener is lazy: it opens no connection until a worker pool starts on the
env. Producers that only enqueue jobs never start one, so the listener stays
dormant and costs nothing. There is nothing to disable.

Once a worker pool does start, the listener holds one pool connection for as long as workers run.

On the provided backends, you can give the listener its own connection with `useDedicatedListener`:

```haskell
env <- ArbS.useDedicatedListener connStr =<< ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
```

And to run in poll-only mode, with no listener at all, use `disableListener`:

```haskell
env <- ArbS.disableListener <$> ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
```

### Other Options

- **Logging** - structured JSON to stderr, fast-logger, or a custom callback
- **Liveness probes** - file-based health check. Kubernetes example:
  ```yaml
  livenessProbe:
    exec:
      command: ["sh", "-c", "find $TMPDIR/arbiter-worker-* -mmin -5"]
    initialDelaySeconds: 30
    periodSeconds: 60
  ```
- **Pool sizing** - `poolConfigForWorkers` sizes the pool from the worker pools that will run
- **Pause/resume** - at queue, worker, or job/tree level

## REST API and Admin UI

The `arbiter-servant` and `arbiter-servant-ui` packages provide a REST API and
admin dashboard. They can be used as standalone WAI applications or integrated
into your own Servant API.

```haskell
import Arbiter.Servant qualified as Servant

config <- Servant.initArbiterServer (Proxy @AppRegistry) connStr "arbiter"
Servant.runArbiterAPI 8080 config
```

Embed as a sub-route in an existing Servant application:

```haskell
import Arbiter.Servant qualified as Arb
import Arbiter.Servant.UI qualified as ArbUI

type MyAPI =
  "api" :> MyBusinessRoutes
    :<|> "arbiter" :> (Arb.ArbiterAPI AppRegistry :<|> ArbUI.AdminUI)
```

See the [arbiter-servant-ui haddocks](https://velveteer.github.io/arbiter/arbiter-servant-ui/Arbiter-Servant-UI.html)

### Endpoints

Per-queue endpoints under `/api/v1/:queue/`:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `jobs` | List jobs |
| `POST` | `jobs` | Insert a job |
| `POST` | `jobs/batch` | Insert multiple jobs |
| `GET` | `jobs/:id` | Get job by ID |
| `DELETE` | `jobs/:id` | Cancel job (cascade-deletes children) |
| `POST` | `jobs/:id/force-cancel` | Cascade-delete and interrupt the running handler |
| `POST` | `jobs/:id/promote` | Make a delayed job immediately visible |
| `POST` | `jobs/:id/move-to-dlq` | Move job to dead-letter queue |
| `POST` | `jobs/:id/suspend` | Suspend job |
| `POST` | `jobs/:id/resume` | Resume suspended job |
| `POST` | `jobs/:id/pause-children` | Pause all visible children of a job |
| `POST` | `jobs/:id/resume-children` | Resume all suspended children |
| `GET` | `dlq` | List DLQ entries |
| `POST` | `dlq/:id/retry` | Retry from DLQ |
| `DELETE` | `dlq/:id` | Delete from DLQ |
| `POST` | `dlq/batch-delete` | Batch delete multiple DLQ entries |
| `GET` | `stats` | Queue statistics |

Global endpoints under `/api/v1/`:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `queues` | List all registered queues |
| `GET` | `queues/:queue/details` | Get queue override details |
| `POST` | `queues/:queue/pause` | Pause a queue (all workers stop claiming) |
| `POST` | `queues/:queue/resume` | Resume a paused queue |
| `GET` | `events/stream` | SSE stream for real-time notifications |
| `GET` | `cron/schedules` | List cron schedules |
| `PATCH` | `cron/schedules/:name` | Override cron expression at runtime |
| `POST` | `cron/schedules/:name/run` | Run an enabled schedule once, out of band |
| `GET` | `workers` | List registered workers |
| `POST` | `workers/:id/pause` | Pause a single worker pool |
| `POST` | `workers/:id/resume` | Resume a single worker pool |
| `GET` | `rate-limits` | List policies with bucket and throttle stats |
| `GET` | `rate-limits/:prefix/buckets` | List a prefix's per-key buckets |
| `PATCH` | `rate-limits/:prefix` | Set or clear a policy's override params |
| `POST` | `rate-limits/:prefix/reset` | Reset (clear) a prefix's buckets |
| `GET` | `concurrency` | List pools with limit and in-flight stats |
| `GET` | `concurrency/:prefix/keys` | List a pool's per-key in-flight counts |
| `PATCH` | `concurrency/:prefix` | Set or clear a pool's override limit |
| `POST` | `concurrency/reconcile` | Repair a pool's in-flight counts |

## Backend Integration

Arbiter's core is backend-agnostic via the `MonadArbiter` typeclass. Three official adapters are provided.

If you're choosing a backend based on raw throughput, consider our benchmarks:

Throughput in jobs/sec, 4 pools × 10 workers, PostgreSQL 18, Apple M5 Pro. hasql runs with prepared claim statements (the default).

**Pre-loaded queue** (1M jobs, 50k groups):

| Backend | Single | Batched | Grouped single | Grouped batched |
|---------|--------|---------|----------------|-----------------|
| hasql | 9,767 | 29,459 | 7,395 | 32,768 |
| orville | 8,219 | 23,662 | 5,477 | 22,232 |
| postgresql-simple | 7,074 | 23,912 | 4,916 | 22,488 |

**Steady-state** (10 producers inserting continuously, 5k groups):

| Backend | Single | Batched | Grouped single | Grouped batched |
|---------|--------|---------|----------------|-----------------|
| hasql | 9,994 | 19,039 | 7,832 | 18,142 |
| orville | 8,552 | 18,456 | 6,952 | 18,312 |
| postgresql-simple | 7,204 | 18,100 | 6,071 | 18,175 |

**Under a scheduled backlog** (1M jobs, 50k groups, cells are single / batched):

| Backend | ungrouped dormant | grouped stress | grouped dormant |
|---------|-------------------|----------------|-----------------|
| hasql | 9,793 / 32,432 | 3,750 / 26,679 | 7,915 / 32,983 |
| orville | 8,323 / 24,630 | 3,553 / 22,951 | 5,578 / 22,208 |
| postgresql-simple | 7,154 / 25,244 | 3,509 / 22,706 | 4,896 / 22,853 |

*stress*: a fifth of jobs scheduled seconds-out, a fifth failing once into backoff. *dormant*: half the backlog parked 30 days out.

**Admission gating** (hasql, prepared claims, 1 pool × 10 workers, 256 keys, cells are single / batched):

| | no gate | rate limit | concurrency | both |
|---------|--------|---------|----------------|-----------------|
| ungrouped | 4,353 / 17,889 | 3,795 / 10,589 | 3,709 / 10,769 | 3,190 / 7,118 |
| grouped (5k groups) | 1,143 / 7,982 | 1,061 / 5,948 | 1,038 / 6,045 | 1,027 / 4,455 |

Rate limiting and concurrency caps each cost roughly 15% in single-job mode. Grouped workloads pay almost nothing on top of the cost of grouping itself.

### arbiter-simple (postgresql-simple)

Built on `postgresql-simple` with `resource-pool`. Handlers receive a raw `Connection`. Nested transactions use savepoints automatically.

```haskell
env <- ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
ArbS.runSimpleDb env $ Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")
```

Share a transaction with external database work:

```haskell
PG.withTransaction conn $ do
  PG.execute conn "INSERT INTO orders (id) VALUES (?)" (PG.Only orderId)
  ArbS.inTransaction @AppRegistry conn "arbiter" $
    Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
```

See the [arbiter-simple haddocks](https://velveteer.github.io/arbiter/arbiter-simple/Arbiter-Simple.html)

### arbiter-orville (orville-postgresql)

Integrates with `orville-postgresql`. Handlers do not receive a connection
parameter - Orville manages connections and transactions internally. Requires a
custom monad with `MonadOrville`, `HasArbiterSchema`, and `MonadArbiter`
instances.

Because Orville does not expose its pooled connections for LISTEN/NOTIFY, the
shared listener runs on its own dedicated connection. Build a `DedicatedListen`
(from `Arbiter.Core.Listen`) with the same connection string as your Orville
pool, keep it in your reader environment, and return it from `getListener`:

```haskell
import Arbiter.Core.Listen (DedicatedListen, dedicatedListener, newDedicatedListen)

data AppEnv = AppEnv
  { appSchema  :: SchemaName
  , appOrville :: O.OrvilleState
  , appListen  :: DedicatedListen
  }

main :: IO ()
main = do
  listen <- newDedicatedListen connStr
  -- ... build AppEnv { appListen = listen, ... } and run your workers

instance MonadArbiter AppM where
  -- ... executeQuery / executeStatement / withDbTransaction / runHandlerWithConnection
  getListener = asks (Just . dedicatedListener . appListen)
```

See the [arbiter-orville haddocks](https://velveteer.github.io/arbiter/arbiter-orville/Arbiter-Orville.html)

### arbiter-hasql (hasql)

Built on `hasql` with `resource-pool`. Handlers receive a `Hasql.Connection` for typed hasql queries inside the worker transaction.

```haskell
env <- ArbH.createHasqlEnv (Proxy @AppRegistry) connStr "arbiter"
ArbH.runHasqlDb env $ Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")
```

Share a transaction with external hasql work:

```haskell
-- Session.script (hasql >= 1.10) or Session.sql (hasql < 1.10)
_ <- Hasql.use conn (Session.script "BEGIN")
ArbH.inTransaction @AppRegistry conn "arbiter" $
  Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
_ <- Hasql.use conn (Session.script "COMMIT")
```

See the [arbiter-hasql haddocks](https://velveteer.github.io/arbiter/arbiter-hasql/Arbiter-Hasql.html)

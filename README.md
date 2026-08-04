<h1 align="left">
    <img src="./arbiter.png" height=40 width=40 />
    Arbiter
</h1>

An opinionated, production-ready PostgreSQL job queue for Haskell applications.

[![Live Demo](https://img.shields.io/badge/Live_Demo-0e7490?style=for-the-badge&logo=rocket&logoColor=white)](https://demo.arbiterq.dev/)
[![API Docs](https://img.shields.io/badge/API_Docs-5e5086?style=for-the-badge&logo=haskell&logoColor=white)](https://velveteer.github.io/arbiter/)
[![CI](https://img.shields.io/github/actions/workflow/status/velveteer/arbiter/ci.yml?branch=main&style=for-the-badge&label=CI)](https://github.com/velveteer/arbiter/actions/workflows/ci.yml)

- Transactional job processing - jobs and database operations commit together
- At-least-once delivery with visibility timeouts and heartbeats
- Per-group ordering (partitioned FIFO)
- Concurrent worker pools with `LISTEN/NOTIFY` wakeups and polling fallback
- Dead-letter queues
- Opt-in archiving of completed jobs with per-job retention
- Job trees with fan-out/fan-in result collection
- Cron/periodic job scheduling
- Job deduplication via unique keys
- Cross-queue per-job rate limiting with operator-tunable token-bucket policies
- Cross-queue per-job concurrency limits - at most N jobs sharing a key in flight
- OpenTelemetry traces built in, metrics and logs via `arbiter-otel`
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

### Payload and Result Types

Define payload types with `ToJSON` and `FromJSON` instances. A queue whose
handlers produce a [result](#job-results) needs the same instances on the result
type.

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

data Score = Score
  { sharpness :: Double
  , sizeBytes :: Int
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)
```

### Type-Level Registry

Map queue table names to payload types at the type level. `Queue` declares a
queue whose handlers store no result. `QueueWithResult` adds the
[result type](#job-results) its handlers produce.

```haskell
import Arbiter.Core.QueueRegistry (Queue, QueueSpec (..))

type AppRegistry =
  '[ Queue "email_queue" EmailPayload
   , QueueWithResult "image_queue" ImagePayload Score
   ]
```

The registry is enforced at compile time - each payload type maps to exactly one
table, and a duplicate table name or a duplicate payload type is a type error.

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

### Job Results

A handler can produce a value - its **result** - by returning it under
`transactionalWorkerConfig`, or by passing it to `ackWith`/`ackAllWith` under a
manual or batched config.

Producing a result does not store it on its own - whether it is kept, and
where, depends on the job:

- **A job with a parent** (a node in a [tree](#job-trees-fan-outfan-in)) - the
  result is kept for the parent to collect with `Worker.childResults`/`Worker.mergedChildResults`,
  then cleaned up once the parent completes.
- **A standalone (root) job** - the result is recorded on the job's
  [archive](#archiving-completed-jobs) entry, if the job is archived. Without
  archiving there is nowhere to keep it, so it is dropped.

A result is stored with its `ToJSON` and read back with its `FromJSON`, so your
own records and sum types work as results directly.

To decide per run whether there is anything worth keeping, make the queue's
result type a `Maybe`. `Nothing` stores nothing at all - no archive entry
result, and no row for a rollup parent to collect.

```haskell
data SyncReport = SyncReport
  { rowsChanged :: Int
  , notes :: [Text]
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

type SyncRegistry = '[ QueueWithResult "sync_queue" SyncPayload (Maybe SyncReport) ]

syncHandler :: Arb.JobHandler (ArbS.SimpleDb SyncRegistry IO) SyncPayload (Maybe SyncReport)
syncHandler _conn job = do
  report <- runSync (Arb.payload job)
  pure $ if rowsChanged report == 0 then Nothing else Just report
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

-- One entry for the whole tree: children and parents share the queue.
type PipelineRegistry = '[ QueueWithResult "pipeline_queue" PipelinePayload [Text] ]

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
handler :: Arb.JobHandler (ArbS.SimpleDb PipelineRegistry IO) PipelinePayload [Text]
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
      pure childResults

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

type MigrationRegistry =
  '[ QueueWithResult "migration_queue" MigrationJob (Sum Int) ]

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

### Archiving Completed Jobs

Completed jobs are deleted on ack by default. Set `archiveFor` to keep a copy in
a per-queue archive for that many seconds after completion.

```haskell
job1 = (Arb.defaultJob payload) { Arb.archiveFor = Just Arb.dayRetention }       -- 24h
job2 = (Arb.defaultJob payload) { Arb.archiveFor = Just (Arb.dayRetention * 7) } -- 1 week
```

Archiving is opt-in per job (`archiveFor = Nothing` deletes as before), and
expired entries are purged automatically. If the job's handler returned a
[result](#job-results), it is kept on the archive entry - so the archive is also
where you read back what a standalone job produced. Archived jobs can be listed,
re-enqueued as fresh jobs, or deleted from the REST API and admin UI.

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
  -> Worker.BatchCallbacks (ArbS.SimpleDb AppRegistry IO) ImagePayload Score
  -> ArbS.SimpleDb AppRegistry IO ()
batchHandler jobs cbs = do
  -- bulkProcess :: [Arb.JobRead ImagePayload] -> IO [(Arb.JobRead ImagePayload, Score)]
  scored <- liftIO $ bulkProcess (toList jobs)
  -- Bulk-ack the whole batch in one transaction.
  Worker.ackAllWith cbs scored
```

Opting out of the worker transaction does not mean giving up atomicity where
you want it. Each callback runs in its own transaction, and wrapping one in
your own `withDbTransaction` commits the ack together with your writes:

```haskell
batchHandler jobs cbs =
  for_ jobs $ \job -> do
    score <- liftIO $ scoreImage (Arb.payload job)
    Arb.withDbTransaction $ do
      recordCharge (Arb.payload job)
      Worker.ackWith cbs job score
```

So you pay for a transaction only around the work that needs one, rather than
for the whole handler. Your writes commit with the ack, but `onJobSuccess` does
not - it can fire for a job that is later reprocessed. Keep effects that must
happen exactly once in the transaction next to the ack, not in the hook.

Each job is finalized on its own via the
[`BatchCallbacks`](https://velveteer.github.io/arbiter/arbiter-worker/Arbiter-Worker-Config.html#t:BatchCallbacks)
record - `ack`/`ackAll` or `ackWith`/`ackAllWith` (per-job or bulk ack), `failRetry`/`failPermanent`,
`cancelBranch`/`cancelTree`, or `nack`. Dispositions are per job, so a failure,
cancel, or nack affects only that job - completed jobs stay done, an untouched
job is reprocessed, and hooks fire per job.

`ackWith`/`ackAllWith` carry the queue's [result](#job-results) - kept for a
rollup parent to collect, or on an archived job's entry. `ack`/`ackAll` finalize
a job without storing anything, on any queue.

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
    shutdown = Signals.Catch $ Worker.shutdownPools workers
void $ Signals.installHandler Signals.sigTERM shutdown Nothing
void $ Signals.installHandler Signals.sigINT shutdown Nothing

poolCfg <- Worker.poolConfigForWorkers workers
env <- ArbS.createSimpleEnvWithConfig (Proxy @AppRegistry) connStr "arbiter" poolCfg
ArbS.runSimpleDb env $ Worker.runWorkerPools workers
```

The dispatcher stops claiming, in-flight jobs drain within
`gracefulShutdownTimeout`, and the process exits.

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
| `GET` | `archive` | List archived (completed) jobs |
| `POST` | `archive/:id/reenqueue` | Re-run an archived job as a fresh job |
| `DELETE` | `archive/:id` | Purge one archive entry |
| `POST` | `archive/batch-delete` | Batch purge archive entries |
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

## OpenTelemetry

Spans and W3C trace-context propagation are built in: an enqueue stamps the ambient
span onto the job, and a worker runs each claim inside a `process <queue>` consumer span
linked back to it. Nothing is exported until an SDK is installed, so this costs an
untraced deployment nothing.

`arbiter-otel` installs that SDK and adds metrics, gauges and OTel log records. All three
signals leave over OTLP, so point them at a collector and export from there to whatever
you run.

```haskell
import Arbiter.Otel qualified as Otel
import Arbiter.Simple (createSimpleEnv, runSimpleDb)
import Data.Text (unpack)

main :: IO ()
main = Otel.withTelemetryFromEnv $ \tel -> do
  putStrLn (unpack (Otel.telemetrySummary tel))
  env <- createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
  let pools = Otel.instrumentPools tel [namedWorkerPool emailCfg, namedWorkerPool imageCfg]

  runSimpleDb env $
    Otel.withGauges tel defaultLogConfig 15 (runWorkerPools pools)
```

`telemetrySummary` is one line naming the service and any exporter that failed to start,
which does not take the process down:

```
telemetry on, service.name=orders
```

Exporters, endpoints and intervals are the SDK's own `OTEL_*` variables, resolved by the
SDK rather than reinterpreted here: the spec's defaults apply, `OTEL_SDK_DISABLED=true`
installs nothing, and a handle that installs nothing is inert, so the wiring above is the
same either way. `withTelemetryIf`, `withTelemetry` and `withExternalTelemetry` take the
same handle from your own config, unconditionally, or from providers you installed
yourself.

### Traces

Every enqueue carries the ambient span, and every claim opens a consumer span linked
back to it - across process boundaries, and across jobs when a handler enqueues
children. No wiring, and `instrumentPools` is not needed for any of it.

The span helpers live in `Arbiter.Core.Trace`:

| Call | Use |
| --- | --- |
| `addSpanAttributes` | Annotate the job's own span from a handler |
| `withSpan` | A child span inside a handler |
| `withPublishSpan` | A producer span around an enqueue outside a handler |
| `withJobParent` | Run under the job's stored context rather than linking to it, for a queue you know is processed promptly |

A handler that overrides `getTraceContext` decides for itself what an enqueue stamps.

An enqueue over the REST API joins the request's trace on its own, given server spans from
`newOpenTelemetryWaiMiddleware` (`hs-opentelemetry-instrumentation-wai`).

### Metrics

| Source | Reported |
| --- | --- |
| Jobs | Throughput by terminal outcome, retries, claim counts, handler latency |
| Queues | Depth by status |
| Admission | What each policy admitted, keys governed, slots in use against the cap, tokens left - keyed by policy, never by the admission key |
| Reaper | Rows each op touched |
| Postgres | Connections, dead tuples, transaction age, cache hits |

- `withGauges` scans the queues your registry declares. Call it once for the process, not per pool: a second call registers the instruments twice.
- Postgres health covers arbiter's own role unless you grant it `pg_read_all_stats`.
- One replica scans per interval and the rest export its reading, so the scan cost does not grow with the fleet. An age gauge tells a fresh scan from one that stopped.
- Aggregate queue depth and Postgres health across replicas with `max`, since they are that one shared reading; sum the per-process counters and latencies. The bundled dashboard does both.

### Logs

OTel log records carrying the job's trace, id, queue, and attempt, sent alongside your
configured destination rather than instead of it.

### Local stack

Grafana's [LGTM stack](https://github.com/grafana/docker-otel-lgtm) with the arbiter
dashboard provisioned, at http://localhost:3000:

```
docker compose -f arbiter-otel/deploy/observability/compose.yaml up -d
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 \
  OTEL_SERVICE_NAME=arbiter-demo \
  cabal run arbiter-demo
```

The [live demo](https://demo.arbiterq.dev/) runs the same stack, with its dashboard at
[/dash](https://demo.arbiterq.dev/dash) as an anonymous viewer.

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
custom monad with `MonadOrville` and `MonadArbiter` instances:

```haskell
{-# LANGUAGE TypeFamilies #-}

instance MonadArbiter AppM where
  type RegistryOf AppM = AppRegistry
  type Handler AppM job result = job -> AppM result
  getSchema = asks appSchema
  -- ... executeQuery / executeStatement / withDbTransaction / runHandlerWithConnection
```

`Handler` is the shape of your handlers - Orville passes no connection, so it is
just the job. `Arb.JobHandler AppM payload result` is that shape at one queue's
job type, and is what you write in a handler's own signature.

Because Orville does not expose its pooled connections for LISTEN/NOTIFY, the
shared listener runs on its own dedicated connection. Build a `DedicatedListen`
(from `Arbiter.Core.Listen`) with the same connection string as your Orville
pool, keep it in your reader environment, and return it from `getListener`.

`createOrvilleConnectionOptions` takes an arbiter `PoolConfig`, so
`poolConfigForWorkers` sizes your Orville pool for the workers that will use it:

```haskell
import Arbiter.Core.Listen (DedicatedListen, dedicatedListener, newDedicatedListen)

data AppEnv = AppEnv
  { appSchema  :: SchemaName
  , appOrville :: O.OrvilleState
  , appListen  :: DedicatedListen
  }

main :: IO ()
main = do
  poolCfg <- Worker.poolConfigForWorkers workers
  orvillePool <- O.createConnectionPool (createOrvilleConnectionOptions connStr poolCfg)
  listen <- newDedicatedListen connStr
  let env =
        AppEnv
          { appSchema = "arbiter"
          , appOrville = O.newOrvilleState O.defaultErrorDetailLevel orvillePool
          , appListen = listen
          }
  runAppM env $ Worker.runWorkerPools workers

instance MonadArbiter AppM where
  -- ... RegistryOf / Handler / getSchema and the query methods, as above
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

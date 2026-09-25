# Worker Configuration

`WorkerConfig` holds the handler, thread count, timings, and callbacks.
Set fields before the pool starts.

`poolConfigForWorkers` sizes the database pool for a list of worker pools. Pass
the same list to `poolConfigForWorkers`, `runWorkerPools`, and
[`shutdownPools`](shutdown.md).

## Multiple Queues

One pool per queue, named with `namedWorkerPool`:

```haskell
let workers = [Worker.namedWorkerPool emailConfig, Worker.namedWorkerPool imageConfig]
```

When one pool exits, the others stop. `runWorkerPools` then throws the first
failure.

`ARBITER_ENABLED_QUEUES` selects pools by name. Unset, every pool starts. An
unknown name throws at startup.

```bash
ARBITER_ENABLED_QUEUES=email_queue,image_queue
```

## Configuration Types

| Constructor | Transaction | Finalization |
| --- | --- | --- |
| `transactionalWorkerConfig` | wraps the handler | a return acks and stores the result. An exception rolls back, then retries or moves to the DLQ. |
| `manualWorkerConfig` | none | callbacks, one job per call |
| `defaultBatchedWorkerConfig` | none | callbacks, up to `batchSize` jobs per call |

A manual or batched handler must ack, fail, or nack each job. An unfinalized
job is redelivered after its visibility timeout.

`withDbTransaction` around a callback commits the ack with the application
writes. Inside an outer transaction the callback is a savepoint. `onJobSuccess`
fires at the savepoint release, before the outer commit.

## Timings

| Field | Meaning |
| --- | --- |
| `visibilityTimeout` | how long a claim holds a job |
| `jobHeartbeatInterval` | how often the worker renews the hold |
| `maxJobDuration` | longest a handler can run |

Timing details: [Leases and Deadlines](deadlines.md).
Field reference: [`WorkerConfig` Haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html).

# Worker Configuration

A `WorkerConfig` defines a handler, thread count, timing values, and callbacks.
Configuration constructors return this record. Update its fields before pool
creation.

`poolConfigForWorkers` calculates the database pool size from a list of worker
pools. Pass the same list to `poolConfigForWorkers` and `runWorkerPools`.

## Multiple Queues

One process can run one pool for each queue. Create each configuration and name
it with `namedWorkerPool`. Pass the same list to `poolConfigForWorkers`,
`runWorkerPools`, and [`shutdownPools`](shutdown.md).

If one pool exits, Arbiter stops the other pools. After all pools stop, Arbiter
throws the first recorded failure.

`ARBITER_ENABLED_QUEUES` is a comma-separated list of pool names.
`runWorkerPools` starts the named pools. If the variable is not set, it starts
all configured pools. This variable permits different deployments to use the
same binary.

```bash
ARBITER_ENABLED_QUEUES=email_queue,image_queue
```

Arbiter checks the names against the configured pools at startup. An unknown
name causes an exception.

## Configuration Types

`transactionalWorkerConfig` runs the handler in a transaction. A normal return
acks the job and stores the returned result. An exception rolls back the work
before Arbiter retries the job or moves it to the DLQ. Arbiter finalizes the job
on each path.

`manualWorkerConfig` and `defaultBatchedWorkerConfig` do not start a handler
transaction. They supply finalization callbacks. Create a transaction for the
writes that require one. For example, an HTTP request can run without a held
database connection. The handler must ack, fail, or nack each job. Arbiter
reprocesses jobs that the handler does not finalize before visibility expires.

Wrap a callback in `withDbTransaction` to commit the ack and application writes
atomically. The callback's transaction becomes a savepoint. The success hook
runs when Arbiter releases that savepoint. It can run before the outer
transaction commits. If the outer transaction rolls back, Arbiter can process
the job again after the hook has run.

Batching is independent of transaction mode. Use
`defaultBatchedWorkerConfig` when per-job overhead is too high and each job
requires a separate disposition in one claim.

## Timings

`visibilityTimeout` is how long a claim holds a job, and `jobHeartbeatInterval`
is how often the worker renews that hold. `maxJobDuration` caps how long a
handler may run at all. See [Leases and Deadlines](deadlines.md).

See the [`WorkerConfig` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html) for all options.

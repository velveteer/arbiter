# Worker Configuration

A pool is a `WorkerConfig`: a handler, a thread count, and the timings and
callbacks around them. The config constructors return the record, and you
override any field on it before wrapping it in a pool.

`poolConfigForWorkers` sizes the database connection pool from the worker pools
you are about to run. Give it the same list you pass to `runWorkerPools`.

## Running several queues

One process can run a pool per queue. Build a config for each, name them with
`namedWorkerPool`, and pass that one list everywhere: to
`poolConfigForWorkers`, to `runWorkerPools`, and to
[`shutdownPools`](shutdown.md).

The pools share a fate. If any one of them exits, the others wind down with it,
and the first failure among them is rethrown once every pool has been joined.

`runWorkerPools` runs the pools named by `ARBITER_ENABLED_QUEUES`, a
comma-separated list, and every configured pool when the variable is unset. One
binary can therefore serve as several differently scoped deployments without a
code change:

```
ARBITER_ENABLED_QUEUES=email_queue,image_queue
```

Names are checked against the registry. One that matches no configured pool
throws at startup, which catches a typo before it silently shrinks the fleet.

## Which constructor

`transactionalWorkerConfig` runs the handler inside a transaction. Returning
acks and stores the return value as the job's result. Throwing rolls the work
back before the job retries or dead-letters. No path leaves a job unfinalized.

`manualWorkerConfig` and `defaultBatchedWorkerConfig` open no transaction and
hand you callbacks instead. Scope your own transaction to the writes that need
one, and a slow HTTP call in the middle of a handler holds no connection. In
exchange, finalizing is yours on every path: a job you neither ack, fail, nor
nack is reprocessed when its visibility lapses.

Wrap a callback in your own `withDbTransaction` for the same atomic
ack-with-writes, because the ack enlists as a savepoint. The ordering differs:
the success hook fires at savepoint release, not at your outer commit, so an
outer rollback leaves the hook fired for a job that runs again.

Batching is a separate choice. Use `defaultBatchedWorkerConfig` when per-job
overhead dominates and you want per-job dispositions inside one claim.

## Timings

`visibilityTimeout` is how long a claim holds a job, and `jobHeartbeatInterval`
is how often the worker renews that hold. `maxJobDuration` caps how long a
handler may run at all. See [Leases and Deadlines](deadlines.md).

See the [`WorkerConfig` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html) for all options.

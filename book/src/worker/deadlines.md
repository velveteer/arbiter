# Leases and Deadlines

```haskell
config { Worker.visibilityTimeout = 60 }     -- how long a claim holds a job (default)
config { Worker.jobHeartbeatInterval = 30 }  -- how often the worker renews that hold (default)
config { Worker.maxJobDuration = Just 300 }  -- longest a handler may run (default: Nothing)
```

A claim is a lease. It writes `not_visible_until` on the row, and the worker owns
the job only until that instant. A heartbeat thread renews it every
`jobHeartbeatInterval` while the handler runs, so a slow job keeps its claim
without a long `visibilityTimeout`.

Three things end a handler before it returns:

| Ends it | When | How the job settles |
| --- | --- | --- |
| Reclaim | the heartbeat finds another worker holding the row | unavailable, no retry |
| Lease fence | the lease passes with no heartbeat behind it | unavailable, no retry |
| Duration deadline | the handler outruns `maxJobDuration` | retryable failure, backoff then DLQ |

A reclaim needs the database to answer. The fence does not: it is the worker
holding itself to a deadline it already knows, for when it has lost the database
and another worker has taken the job. It is always on and takes no configuration.

## maxJobDuration

```haskell
config <- Worker.transactionalWorkerConfig 4 processReport
let reportConfig = config { Worker.maxJobDuration = Just 300 }
```

An overrun reads as a retryable failure: the job takes its backoff, dead-letters
at `maxAttempts`, and `last_error` names the duration it passed. Set it on any
handler that calls an external service, or that holds a concurrency pool slot.

> [!IMPORTANT]
> Without `maxJobDuration`, a hung handler holds its job for as long as the
> process lives. The heartbeat keeps renewing the lease of a thread going
> nowhere, and no reclaim ever comes.

## Choosing the timings

`jobHeartbeatInterval` must be less than `visibilityTimeout`, and the pool
refuses to start otherwise. A worker that loses the database keeps running until
one `visibilityTimeout` past its last successful renewal, so the wait is at most
that and at least the gap between the two settings, depending on where the
failure lands in the cycle.

A failed extend is retried, and the heartbeat tightens its cadence as the lease
runs down, so a transient error costs a retry rather than the job. The fence
trips only when no attempt lands before the lease.

Shorten `visibilityTimeout` for tighter fencing. The cost is redelivering sooner
after a worker dies.

See the [`WorkerConfig` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html) for every timing field.

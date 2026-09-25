# Leases and Deadlines

```haskell
config { Worker.visibilityTimeout = 60 }     -- how long a claim holds a job (default)
config { Worker.jobHeartbeatInterval = 30 }  -- how often the worker renews that hold (default)
config { Worker.maxJobDuration = Just 300 }  -- longest a handler may run (default: Nothing)
```

A claim is a lease on the row's `not_visible_until`. One guard per worker pool
renews due jobs in a shared statement at the `jobHeartbeatInterval` cadence.
The guard continues fencing leases if an extension stalls.

Early handler termination:

| Ends it | When | How the job settles |
| --- | --- | --- |
| Reclaim | the heartbeat finds another worker owns the row | unavailable, no retry |
| Lease fence | the lease expires after heartbeat failures | unavailable, no retry |
| Duration deadline | the handler exceeds `maxJobDuration` | retryable failure, then backoff or DLQ |

The lease fence uses a local deadline and requires no database response.
It is always active.

A batch whose lease has already expired when it reaches the guard is rejected
before its handler runs. Each of its jobs fires `onJobClaimed`, then
`onJobUnavailable`. Keep worker and database clocks synchronized.
An expired timestamp is not treated as evidence of clock skew.

The guard registers before `onJobClaimed` runs. Claim hooks receive heartbeat
protection, and their runtime counts toward `maxJobDuration`. A blocked claim hook
is interrupted by the duration deadline or by lease loss, just like a handler.

## maxJobDuration

```haskell
config <- Worker.transactionalWorkerConfig 4 processReport
let reportConfig = config { Worker.maxJobDuration = Just 300 }
```

`last_error` records the exceeded duration.

> [!IMPORTANT]
> Without `maxJobDuration`, a hung handler holds its job and renews its lease
> for the life of the process.

## Timing Constraints

`jobHeartbeatInterval` must be less than `visibilityTimeout`. The pool refuses
to start otherwise.

After the database goes away, a handler continues for at most one
`visibilityTimeout` after its last renewal. It continues for at least the
difference between the two settings. Failed extensions retry until the lease
expires.

Timing fields: [`WorkerConfig` Haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html).

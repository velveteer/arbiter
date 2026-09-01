# Leases and Deadlines

```haskell
config { Worker.visibilityTimeout = 60 }     -- how long a claim holds a job (default)
config { Worker.jobHeartbeatInterval = 30 }  -- how often the worker renews that hold (default)
config { Worker.maxJobDuration = Just 300 }  -- longest a handler may run (default: Nothing)
```

A claim is a lease. It sets `not_visible_until` on the row. The worker owns the
job until that time. A heartbeat thread renews the lease at each
`jobHeartbeatInterval` while the handler runs. This renewal permits a slow job
to use a short `visibilityTimeout`.

Three things end a handler before it returns:

| Ends it | When | How the job settles |
| --- | --- | --- |
| Reclaim | the heartbeat finds that another worker owns the row | unavailable, no retry |
| Lease fence | the lease expires after heartbeat failures | unavailable, no retry |
| Duration deadline | the handler exceeds `maxJobDuration` | retryable failure, then backoff or DLQ |

A reclaim check requires a database response. The lease fence uses the locally
stored deadline. It stops the handler if the worker cannot contact the database
and the lease expires. The fence is always active and has no configuration.

## maxJobDuration

```haskell
config <- Worker.transactionalWorkerConfig 4 processReport
let reportConfig = config { Worker.maxJobDuration = Just 300 }
```

An exceeded limit is a retryable failure. Arbiter applies the configured
backoff and moves the job to the DLQ at `maxAttempts`. `last_error` contains the
exceeded duration. Set this limit for handlers that call external services or
occupy concurrency pool slots.

> [!IMPORTANT]
> If `maxJobDuration` is not set, an unresponsive handler retains its job while
> the process runs. The heartbeat continues to renew the lease. Another worker
> cannot reclaim the job.

## Choosing the timings

`jobHeartbeatInterval` must be less than `visibilityTimeout`. The pool does not
start if the values are invalid. After a database connection failure, the
worker can continue for up to one `visibilityTimeout` after its last successful
renewal. The minimum period is the difference between the two settings. The
exact period depends on the point of failure in the heartbeat cycle.

Arbiter retries a failed extension. It shortens the interval between attempts
as the lease expiration time approaches. The fence stops the handler if all
attempts fail before expiration.

Reduce `visibilityTimeout` to stop handlers sooner after lost leases. This also
causes earlier redelivery after a worker stops.

See the [`WorkerConfig` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html) for every timing field.

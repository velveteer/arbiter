# REST API and Admin UI

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
import Arbiter.Servant qualified as Servant
import Arbiter.Servant.UI qualified as ServantUI

type MyAPI =
  "api" :> MyBusinessRoutes
    :<|> "arbiter" :> (Servant.ArbiterAPI AppRegistry :<|> ServantUI.AdminUI)
```

See the [arbiter-servant-ui haddocks](https://arbiterq.dev/arbiter-servant-ui/Arbiter-Servant-UI.html) for the UI's route type.

`POST jobs` and `POST jobs/batch` enqueue jobs. Services in other languages can
use these endpoints without an Arbiter library. The admin UI uses the other
per-queue endpoints for operator functions.

## Endpoints

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
| `POST` | `claim` | Lease visible jobs, returning each job with its lease |
| `POST` | `jobs/:id/ack` | Complete a job the lease still holds, storing its result |
| `POST` | `jobs/:id/nack` | Hand a job back without spending its attempt |
| `POST` | `jobs/:id/extend` | Push out a held lease |
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
| `GET` | `kinds` | List the payload variant labels the queue declares |

Global endpoints under `/api/v1/`:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `queues` | List all registered queues |
| `GET` | `queues/stats` | Statistics for every registered queue |
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
| `POST` | `concurrency/reconcile` | Repair the in-flight counts of every pool |
| `POST` | `maintenance` | Run one gated maintenance pass |
| `GET` | `health` | Readiness check. Returns 503 when the database is unavailable |
| `GET` | `health/live` | Liveness check. Does not query the database |

## Consuming over HTTP

`POST claim` applies the worker-pool claim operation. It uses admission tokens,
increments the attempt count, records a claimant, and makes each job invisible
for the lease period. A paused queue does not return leases. The pause applies
to HTTP consumers and worker pools.

```http
POST /api/v1/email_queue/claim
{"maxJobs": 5, "leaseSeconds": 60}
```

`maxJobs` defaults to 1 and clamps to 1000. `leaseSeconds` defaults to 60 and
clamps to 3600.

Each response job contains `claimSeq` and `claimedBy`. These fields identify the
lease. Each finalization request must include them:

```http
POST /api/v1/email_queue/jobs/41/ack
{"claimSeq": 7, "claimedBy": "0f5e...c31"}
```

On a queue declared with `QueueWithResult`, `ack` also takes the result:

```http
POST /api/v1/email_queue/jobs/41/ack
{"claimSeq": 7, "claimedBy": "0f5e...c31", "result": ["delivered"]}
```

Arbiter stores the result in the parent rollup for a child job, or in the
archive entry for an archived root job. This is the same behavior as
`ackWith`. A body that does not match the queue result type returns 400. Omit
`result` to store no result. Mounting this route requires `FromJSON` and
`ToJSON` for the result type.

`ack` completes the job. `nack` restores the used attempt and keeps the job
invisible for the remainder of its lease. `extend` sets a later lease
expiration, as a worker heartbeat does. The request body requires `seconds`.
Arbiter limits the value to 3600 and measures it from the request time. If the
lease fields do not match the row, the endpoint returns 409. Thus, a previous
holder cannot ack a reclaimed job.

These routes finalize leases created by `POST claim`. They return 409 for a
lease held by a worker pool.

The server does not renew an HTTP lease automatically. The consumer must call
`extend`. After an unextended lease expires, another consumer can claim the
job. This gives at-least-once delivery after a consumer failure.

> [!IMPORTANT]
> Arbiter does not authenticate claim or finalization requests. Add
> authentication before you expose these routes. You can use WAI middleware, a
> Servant authentication combinator, or an authenticating proxy.

## Maintenance

`POST maintenance` performs one pass of schema maintenance. It processes stale
workers, exhausted and cancelled jobs, rate-limit buckets, concurrency counts,
archive retention, and group summaries.

Concurrent callers cannot run the same operation. The default has no minimum
interval. Set `maintenanceInterval` in the server configuration to define the
minimum interval for each operation. `maintenanceTimeout` limits one statement.

`maintenanceSparseInterval` defines a separate minimum interval for
schema-wide operations. `maintenanceBucketIdle` specifies how long a
rate-limit bucket must be inactive before removal.

The response gives the affected row count for each completed operation and the
names of failed operations. Operations absent from both lists were skipped:

```json
{"ops": {"sweep-stale-workers": 2, "purge-archives": 140}, "failed": []}
```

Worker pools run this maintenance in their reaper. Use the endpoint when a
deployment does not run a worker pool.


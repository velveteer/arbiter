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

`POST jobs` and `POST jobs/batch` enqueue, so a service in any language can
produce jobs without linking arbiter. The other per-queue routes are the
operator surface the admin UI is built on.

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
| `GET` | `health` | Readiness - checks the database, 503 when unreachable |
| `GET` | `health/live` | Liveness - never touches the database |

## Consuming over HTTP

`POST claim` leases visible jobs the same way a worker pool does: it spends
admission, bumps the attempt count, stamps a claimant, and hides each job for
the lease window. A paused queue leases nothing, so a pause stops HTTP
consumers and worker pools alike.

```
POST /api/v1/email_queue/claim
{"maxJobs": 5, "leaseSeconds": 60}
```

`maxJobs` defaults to 1 and clamps to 1000. `leaseSeconds` defaults to 60 and
clamps to 3600.

Each job in the response carries `claimSeq` and `claimedBy`. Those two are the
lease, and every finalize has to present them:

```
POST /api/v1/email_queue/jobs/41/ack
{"claimSeq": 7, "claimedBy": "0f5e...c31"}
```

On a queue declared with `QueueWithResult`, `ack` also takes the result:

```
POST /api/v1/email_queue/jobs/41/ack
{"claimSeq": 7, "claimedBy": "0f5e...c31", "result": ["delivered"]}
```

The result lands where a worker's `ackWith` puts it: in the parent's rollup for
a child job, on the archive entry for an archived root. A body that does not
match the queue's result type is refused with 400. Leave `result` out to store
nothing. Because this route parses a result, mounting the API needs `FromJSON`
for the result type as well as `ToJSON`.

`ack` completes the job. `nack` hands it back without spending its attempt, and
the job stays hidden for what is left of its lease, as a worker's nack does.
`extend` pushes the lease out, which a worker's heartbeat does in-process. Its
body adds a required `seconds`, clamped to 3600 and counted from now. A lease
that no longer matches the row is refused with 409, so a job reclaimed after
its window lapsed cannot be acked by the previous holder.

These routes finalize only the leases `POST claim` hands out. A lease a worker
pool holds is refused with 409.

Nothing renews a lease on its own. A consumer that stops calling `extend` loses
the job to the next claim when the window passes, the same at-least-once
behavior a crashed worker gets.

> [!IMPORTANT]
> A claim hands out a lease on real work, and arbiter ships no authentication
> of its own. Authenticate these routes before you expose them: WAI middleware,
> a Servant auth combinator over the mounted API, or a proxy in front of the
> process.

## Maintenance

`POST maintenance` runs one pass of the schema-wide work a worker pool's reaper
would do: stale workers, exhausted and cancelled jobs, rate-limit buckets,
concurrency counts, archive retention, and group summaries.

Operations exclude each other across callers, so two servers that call at once
do the work once. Nothing is throttled by default. Raise `maintenanceInterval`
on the server config to set a minimum gap between runs of an operation.
`maintenanceTimeout` bounds a single statement.

A whole-schema operation keeps its own gap, `maintenanceSparseInterval`,
whatever `maintenanceInterval` is. `maintenanceBucketIdle` is the idle age at
which a pass prunes a rate-limit bucket.

The response reports the rows each operation touched and names the ones that
raised. An operation in neither was skipped:

```json
{"ops": {"sweep-stale-workers": 2, "purge-archives": 140}, "failed": []}
```

A deployment that runs a worker pool needs none of this, because the pool's
reaper already does it. It matters when nothing in the deployment runs a pool.


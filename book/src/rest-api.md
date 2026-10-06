# REST API and Admin UI

`arbiter-servant` and `arbiter-servant-ui` provide the REST API and admin
dashboard, as standalone WAI applications or as routes in an existing Servant
API.

```haskell
import Arbiter.Servant qualified as Servant
import Arbiter.Simple (createSimpleEnv, runSimpleDb)

env <- createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
config <- Servant.createArbiterServer (runSimpleDb env)
Servant.runArbiterAPI 8080 config
```

The server runs over any backend. Pass `runHasqlDb env` for a hasql
application. The SSE stream uses the env's listener.

Embed as a sub-route in an existing Servant application:

```haskell
import Arbiter.Servant qualified as Servant
import Arbiter.Servant.UI qualified as ServantUI

type MyAPI =
  "api" :> MyBusinessRoutes
    :<|> "arbiter" :> (Servant.ArbiterAPI AppRegistry :<|> ServantUI.AdminUI)
```

Server and route types: [arbiter-servant Haddocks](https://arbiterq.dev/arbiter-servant/Arbiter-Servant.html).
UI route type: [arbiter-servant-ui Haddocks](https://arbiterq.dev/arbiter-servant-ui/Arbiter-Servant-UI.html).

`POST jobs` and `POST jobs/batch` enqueue from any language.

## OpenAPI

`arbiter-servant-openapi` serves an OpenAPI 3 description of the API at
`/openapi.json`:

```haskell
import Arbiter.Servant (ArbiterAPI, arbiterServer, createArbiterServer)
import Arbiter.Servant.OpenApi (OpenApiAPI, openApiServer)
import Arbiter.Servant.UI (AdminUI, adminUIServer)

type MyApp = ArbiterAPI AppRegistry :<|> OpenApiAPI :<|> AdminUI

config <- createArbiterServer (runSimpleDb env)
run 8080 $
  serve (Proxy @MyApp) (arbiterServer config :<|> openApiServer @AppRegistry :<|> adminUIServer)
```

| Requirement | Detail |
| --- | --- |
| payload and result types | a `ToSchema` instance. `deriving anyclass (ToSchema)` works for generic JSON |
| `Value` payloads | the module supplies the instance |
| registry | given to `openApiServer` by type application |

> [!NOTE]
> Mount `OpenApiAPI` before `AdminUI`. `AdminUI` is a `Raw` route that matches every path.

`openApiSpec @AppRegistry` returns the same document as a `Value`.
Module: [arbiter-servant-openapi Haddocks](https://arbiterq.dev/arbiter-servant-openapi/Arbiter-Servant-OpenApi.html).

## Endpoints

Per-queue endpoints under `/api/v1/:queue/`:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `jobs` | List jobs. `payload` searches the payload text |
| `POST` | `jobs` | Insert a job |
| `POST` | `jobs/batch` | Insert multiple jobs |
| `GET` | `jobs/:id` | Get a job by ID |
| `DELETE` | `jobs/:id` | Cancel a job and delete its children |
| `POST` | `jobs/:id/force-cancel` | Cancel a job, delete its children, and interrupt the running handler |
| `POST` | `jobs/:id/promote` | Make a delayed job immediately visible |
| `POST` | `jobs/:id/reschedule` | Set when a job becomes visible |
| `POST` | `jobs/:id/move-to-dlq` | Move a job to the dead-letter queue |
| `POST` | `claim` | Lease visible jobs and return each job with its lease |
| `POST` | `jobs/:id/ack` | Complete a job the lease still holds and store its result |
| `POST` | `jobs/:id/nack` | Hand a job back without spending its attempt |
| `POST` | `jobs/:id/extend` | Extend a held lease |
| `POST` | `jobs/:id/suspend` | Suspend a job |
| `POST` | `jobs/:id/resume` | Resume a suspended job |
| `POST` | `jobs/:id/pause-children` | Pause all visible children of a job |
| `POST` | `jobs/:id/resume-children` | Resume all suspended children |
| `GET` | `dlq` | List DLQ entries. `payload` and `error` search the payload and the last error |
| `POST` | `dlq/:id/retry` | Retry a job from the DLQ, optionally with a new payload |
| `DELETE` | `dlq/:id` | Delete one DLQ entry |
| `POST` | `dlq/batch-delete` | Delete multiple DLQ entries |
| `GET` | `archive` | List archived jobs. `payload` searches the payload text |
| `POST` | `archive/:id/reenqueue` | Re-enqueue an archived job as a new job, optionally with a new payload |
| `DELETE` | `archive/:id` | Delete one archive entry |
| `POST` | `archive/batch-delete` | Delete multiple archive entries |
| `GET` | `stats` | Queue statistics |
| `GET` | `kinds` | List the kind labels the queue declares |
| `GET` | `groups` | List the queue's open groups, largest first |

Global endpoints under `/api/v1/`:

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `queues` | List all registered queues |
| `GET` | `queues/stats` | Statistics for every registered queue |
| `GET` | `queues/:queue/details` | Get the queue's pause state |
| `POST` | `queues/:queue/pause` | Pause a queue (all workers stop claiming) |
| `POST` | `queues/:queue/resume` | Resume a paused queue |
| `GET` | `events/stream` | SSE stream for real-time notifications |
| `GET` | `cron/schedules` | List cron schedules |
| `PATCH` | `cron/schedules/:name` | Override a schedule's expression, overlap policy, time zone, or enabled state |
| `POST` | `cron/schedules/:name/run` | Run an enabled schedule once, out of band |
| `GET` | `workers` | List registered workers |
| `POST` | `workers/:id/pause` | Pause a single worker |
| `POST` | `workers/:id/resume` | Resume a single worker |
| `GET` | `rate-limits` | List policies with bucket and throttle stats |
| `GET` | `rate-limits/:prefix/buckets` | List a prefix's per-key buckets |
| `PATCH` | `rate-limits/:prefix` | Set or clear a policy's override params |
| `POST` | `rate-limits/:prefix/reset` | Reset a prefix's buckets |
| `POST` | `rate-limits/:prefix/buckets/:key/tokens` | Add tokens to one bucket and wake its throttled jobs |
| `POST` | `rate-limits/prune` | Delete full buckets idle for `idle` seconds |
| `GET` | `concurrency` | List concurrency policies with limit and in-flight stats |
| `GET` | `concurrency/:prefix/keys` | List a policy's per-key in-flight counts |
| `PATCH` | `concurrency/:prefix` | Set or clear a policy's override limit |
| `POST` | `concurrency/reconcile` | Repair the in-flight counts of every policy |
| `POST` | `concurrency/prune` | Delete drained keys that have no live job |
| `POST` | `maintenance` | Run one gated maintenance pass |
| `GET` | `health` | Readiness check. Returns 503 when the database is unavailable |
| `GET` | `health/live` | Liveness check. Does not query the database |

## Operating jobs

`dlq/:id/retry` and `archive/:id/reenqueue` take an optional replacement
payload:

```http
POST /api/v1/email_queue/dlq/12/retry
Content-Type: application/json

{"payload": {"tag": "SendWelcome", "contents": "alice@example.com"}}
```

| Body | Effect |
| --- | --- |
| empty | the stored payload runs again |
| `payload` | the new payload runs. Its kind, rate-limit and concurrency keys come from it |
| a payload the queue's type rejects | 400. Nothing changes |
| a body with no `Content-Type` | 400. Nothing changes |
| a body with a content type that is not `application/json` | 415. Nothing changes |

`jobs/:id/reschedule` sets when a job becomes visible:

```http
POST /api/v1/email_queue/jobs/41/reschedule
Content-Type: application/json

{"runAt": "2026-10-01T09:00:00Z"}
```

An in-flight, suspended, cancelled or exhausted job returns 409.

## Consuming over HTTP

`POST claim` is the worker-pool claim: it spends admission tokens, increments
the attempt count, records a claimant, and hides each job for the lease. A
paused queue returns no leases.

```http
POST /api/v1/email_queue/claim
{"maxJobs": 5, "leaseSeconds": 60}
```

`maxJobs` defaults to 1 and clamps to 1000. `leaseSeconds` defaults to 60 and
clamps to 3600.

`claimSeq` and `claimedBy` in each returned job identify the lease. Every
finalization request carries them:

```http
POST /api/v1/email_queue/jobs/41/ack
{"claimSeq": 7, "claimedBy": "0f5e...c31"}
```

On a `QueueWithResult` queue, `ack` takes the result:

```http
POST /api/v1/email_queue/jobs/41/ack
{"claimSeq": 7, "claimedBy": "0f5e...c31", "result": ["delivered"]}
```

The result is stored as [`ackWith`](features/results.md) stores it. A body
that does not match the result type returns 400. Omit `result` to store none. The route needs
`FromJSON` and `ToJSON` on the result type.

| Route | Effect |
| --- | --- |
| `ack` | completes the job |
| `nack` | refunds the attempt. The job stays invisible for the rest of the lease. |
| `extend` | moves the lease expiry to `leaseSeconds` from now, at most 3600 |

A mismatched lease, or one held by a registered worker, returns 409.

The server does not renew an HTTP lease. After it expires another consumer can
claim the job.

> [!IMPORTANT]
> These routes have no authentication. Protect them with WAI middleware, a
> Servant authentication combinator, or an authenticating proxy.

## Maintenance

`POST maintenance` runs one pass over stale workers, exhausted and cancelled
jobs, rate-limit buckets, concurrency counts, archive retention, and group
summaries. Each operation runs in one caller at a time.

| Server setting | Meaning |
| --- | --- |
| `maintenanceInterval` | minimum gap between runs of one operation (default: none) |
| `maintenanceSparseInterval` | minimum gap between runs of the bucket prune and the concurrency reconcile-and-prune |
| `maintenanceTimeout` | statement timeout |
| `maintenanceBucketIdle` | idle time before a rate-limit bucket is removed |

The response lists the affected row count per completed operation and the
names of failed operations. Skipped operations appear in neither:

```json
{"ops": {"sweep-stale-workers": 2, "purge-archives": 140}, "failed": []}
```

Worker pools run the same pass in their reaper. The endpoint supports
deployments without worker pools.

# Arbiter CLI — a job queue any language can drive

Run a durable, Postgres-backed job queue where **your job handler is just an HTTP
endpoint**. Producers enqueue over REST, Arbiter claims jobs (applying priority,
group ordering, rate limits, and concurrency caps), and delivers each one to your
endpoint. Your HTTP response decides what happens next: succeed, retry, or
dead-letter. No Haskell required to produce or consume jobs.

> **Status.** The `arbiter` binary (`migrate`/`serve`), TOML config, signed
> webhook delivery, and the pull endpoints (claim/ack/nack/extend) are all
> implemented and tested. Remaining items are marked _(planned)_ in the roadmap.

## How it works

```
                enqueue (HTTP)                    claim + admission control
  your app  ───────────────────▶  Arbiter  ◀───────────────────────────────┐
 (any lang)                      (Postgres)                                  │
                                     │  deliver job (HTTP POST, signed)      │
                                     ▼                                       │
                              your handler  ── 2xx ack / 4xx DLQ / retry ────┘
                               (any lang)
```

Everything that makes Arbiter useful — ordering, rate limiting, concurrency
limits, retry backoff, and the dead-letter queue — lives on the Arbiter side, so
you get it regardless of what language your handler is written in.

## Enqueue a job

Jobs are enqueued over the REST API. A queue is just a name; the payload is any
JSON you choose.

```bash
curl -X POST http://localhost:8080/api/v1/emails/jobs \
  -H 'Content-Type: application/json' \
  -d '{
    "payload": { "to": "ada@example.com", "template": "welcome" },
    "groupKey": "ada@example.com",
    "priority": 0,
    "maxAttempts": 5
  }'
```

Only `payload` is required. Optional fields:

| Field             | Meaning                                                        |
| ----------------- | ------------------------------------------------------------- |
| `groupKey`        | Jobs sharing a key run one-at-a-time, in order.               |
| `priority`        | Lower runs first. Default `0`.                                |
| `notVisibleUntil` | ISO-8601 timestamp; delays first delivery.                    |
| `dedupKey`        | Drop or replace duplicates before they run.                   |
| `maxAttempts`     | Delivery attempts before the job dead-letters. Default `10`.  |
| `rateLimit`       | `{"prefix":…, "suffix":…, "cost":1}` — the bucket this job draws from. |
| `concurrency`     | `{"prefix":…, "suffix":…}` — the pool this job occupies while it runs. |

`prefix` names a policy you configured. `suffix` picks the bucket or pool within
it (a tenant, an account, an API key). To rate-limit sends per customer, give
every job `{"prefix": "send", "suffix": "<customer-id>"}`. A prefix that names no
configured policy is rejected, so a typo cannot quietly let jobs run unthrottled.

## Run it

Configure queues and handlers in a TOML file (see `arbiter.example.toml`):

```toml
[database]
url    = "host=localhost port=5432 user=postgres password=master dbname=postgres"
schema = "arbiter"

[server]
host = "127.0.0.1"
port = 8080

[[queue]]
name    = "emails"
  [queue.webhook]
  url         = "https://handlers.example.com/emails"
  workers     = 8
  secret      = "whsec_..."
  timeout_secs = 30
```

```bash
arbiter migrate   # create/upgrade the queue tables
arbiter serve     # run migrations, then serve the REST API + webhook workers
```

Queues are runtime-named: they come from config, not compile-time code. A queue
with a `[queue.webhook]` gets a worker that pushes its jobs to that endpoint. A
queue without one is produce/consume over HTTP only (see the pull API below).

### Tuning a queue's worker

Sensible defaults apply if you leave this out. Every duration is in seconds.

```toml
[[queue]]
name    = "emails"

  [queue.worker]
  visibility_timeout        = 60.0     # how long a claimed job stays hidden
  job_heartbeat_interval    = 30.0     # extends that while a handler runs. must be below it
  graceful_shutdown_timeout = 30.0     # on SIGTERM, wait this long for in-flight jobs (0 = forever)
  jitter                    = "equal"  # equal (default) | full | none

    [queue.worker.backoff]
    strategy = "exponential"           # exponential (default) | linear | constant
    base     = 2.0
    cap      = 3600.0
```

The full set (`poll_interval`, `worker_heartbeat_interval`, `worker_stale_threshold`,
`reaper_interval`, `reaper_timeout`) is listed in `arbiter.example.toml`. Values that
would break each other are rejected at startup rather than at 3am: a heartbeat that
cannot outpace the timeout it extends would have a job reclaimed while its handler is
still working.

The retry keys govern the pull API's `fail` route too, so a job backs off the same way
whichever consumer reported it failed. The `reaper_*` keys apply to a webhook-less
queue as well, which still runs a maintenance loop.

### Scheduled jobs (cron)

A queue can enqueue its own jobs on a schedule. Every schedule is evaluated on the
minute, and arbiter stamps the tick it fired for into the payload as `tick`:

```toml
[[queue]]
name = "emails"

  [[queue.cron]]
  name       = "email-digest"       # unique across every queue
  expression = "*/2 * * * *"        # 5-field cron
  overlap    = "skip"               # skip (default): one job pending/running at a time
                                    # allow: one job per tick, ticks may overlap
  timezone   = "America/New_York"   # optional, default UTC
  backfill   = 3600.0               # optional: replay ticks missed in the last N seconds
  payload    = { kind = "digest" }
```

Each tick enqueues a normal job, so a webhook queue delivers it and a pull consumer
claims it, whichever the queue uses:

```json
{"kind": "digest", "tick": "2026-07-13T03:00:00Z"}
```

Ticks are deduplicated across replicas, so running several `arbiter` processes fires
each tick once. An invalid expression or timezone is rejected at startup, not at the
first tick.

The file seeds a schedule; the database owns it from then on. The cron API can pause a
schedule or retarget its expression, overlap, and timezone at runtime, and those
overrides win over what the file says on every tick. Removing a `[[queue.cron]]` block
stops the schedule from firing but leaves its row behind, so it keeps showing up in the
API and the admin UI — delete or disable it there if you want it gone.

**Auth.** arbiter does none of its own — put a reverse proxy / API gateway in
front to terminate TLS and authenticate.

## Consuming without an inbound endpoint (pull)

Workers behind NAT, or batch consumers, can pull instead of receiving webhooks —
the same claim engine and admission apply:

```
POST /api/v1/queues/{queue}/claim              {"maxJobs":10}      -> {"jobs":[…]}
POST /api/v1/queues/{queue}/jobs/{id}/ack      {"attempts":N, "claimedBy":"…", "result":…}
POST /api/v1/queues/{queue}/jobs/{id}/fail     {"attempts":N, "claimedBy":"…", "error":"…"}
POST /api/v1/queues/{queue}/jobs/{id}/nack     {"attempts":N, "claimedBy":"…"}
POST /api/v1/queues/{queue}/jobs/{id}/extend   {"attempts":N, "claimedBy":"…", "visibilitySecs":300}
```

Each claimed job carries its `primaryKey`, `attempts`, and a `claimedBy` lease
token. Echo `claimedBy` and `attempts` back to prove you hold the lease. A stale
or forged lease (wrong claimant, or the job was reclaimed) returns `409`.

Report a failure with `fail`, the counterpart of a handler throwing: the job
retries with backoff, and lands in the dead-letter queue once its attempts run
out. Pass `"permanent": true` to dead-letter it immediately, or `retryDelaySecs`
to choose the delay. The response says which happened:
`{"outcome":"retry","retryInSecs":8}` or `{"outcome":"dlq","retryInSecs":null}`.
Use `nack` only to hand a job back untouched (it consumes no attempt, records no error,
and is claimable again at once, whatever lease you took), and never as a stand-in for
failure — a nacked job retries forever.

A paused queue leases nothing, so `claim` returns an empty list until it resumes.

## Receive and verify a delivery

Arbiter POSTs one job per request to your URL.

### Request

Headers:

| Header                | Value                                                    |
| --------------------- | -------------------------------------------------------- |
| `Content-Type`        | `application/json`                                       |
| `X-Arbiter-Job-Id`    | Numeric job id. Stable across retries — use for idempotency. |
| `X-Arbiter-Queue`     | Queue name.                                              |
| `X-Arbiter-Timestamp` | Unix seconds when the request was signed.               |
| `X-Arbiter-Signature` | `v1=<hex>` HMAC-SHA256 (present only when a secret is set). |

Body:

```json
{
  "job_id": 42,
  "queue": "emails",
  "attempts": 1,
  "max_attempts": 5,
  "group_key": "ada@example.com",
  "parent_id": null,
  "priority": 0,
  "payload": { "to": "ada@example.com", "template": "welcome" }
}
```

### Verify the signature

The signature is `HMAC-SHA256(secret, "<timestamp>.<raw-body>")`, hex-encoded,
prefixed with the scheme tag `v1=`. **Signing the timestamp is what makes replay
attacks detectable** — reject deliveries whose timestamp is too old, and always
compare in constant time.

Python:

```python
import hashlib, hmac, time

def verify(secret: bytes, headers, raw_body: bytes, tolerance=300) -> bool:
    ts  = headers["X-Arbiter-Timestamp"]
    sig = headers["X-Arbiter-Signature"]           # "v1=<hex>"
    if abs(time.time() - int(ts)) > tolerance:     # reject stale/replayed
        return False
    signed   = f"{ts}.".encode() + raw_body
    expected = "v1=" + hmac.new(secret, signed, hashlib.sha256).hexdigest()
    return hmac.compare_digest(sig, expected)       # constant-time
```

Node:

```js
const crypto = require("crypto");

function verify(secret, headers, rawBody, tolerance = 300) {
  const ts = headers["x-arbiter-timestamp"];
  const sig = headers["x-arbiter-signature"];           // "v1=<hex>"
  if (Math.abs(Date.now() / 1000 - Number(ts)) > tolerance) return false;
  const expected =
    "v1=" + crypto.createHmac("sha256", secret).update(`${ts}.${rawBody}`).digest("hex");
  const a = Buffer.from(sig), b = Buffer.from(expected);
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}
```

> Verify against the **raw request bytes**, before any JSON re-serialization —
> a reformatted body will not match.

### Respond

Your HTTP status decides the job's fate:

| Response                          | Outcome                                                       |
| --------------------------------- | ------------------------------------------------------------ |
| `2xx`                             | **Ack.** Job is done and removed.                            |
| `3xx`                             | **Dead-letter** immediately. Redirects are not followed.     |
| `4xx` (except `408`/`429`)        | **Dead-letter** immediately. A bad job won't retry forever.  |
| `408`, `429`, `5xx`               | **Retry** later with backoff.                                |
| timeout / connection error        | **Retry** later with backoff.                                |

## Delivery guarantees

- **At-least-once.** A job is acked only after your endpoint returns `2xx`. If your
  handler succeeds but the ack is lost, the job is redelivered — make handlers
  **idempotent**, keyed on `X-Arbiter-Job-Id`.
- **Leases.** A claimed job is invisible to other workers for a visibility window
  that Arbiter auto-extends while your handler runs, so slow handlers don't get
  their job stolen. If your worker crashes, the lease lapses and another worker
  picks the job up.
- **Backoff + DLQ.** Retries use exponential backoff with jitter. After
  `maxAttempts` the job moves to the dead-letter queue, inspectable and
  retriable from the admin UI or REST API.

## Roadmap

| Capability                                        | Status   |
| ------------------------------------------------- | -------- |
| Generic JSON worker on a runtime-named queue      | ✅ done  |
| Signed webhook delivery (timestamp + HMAC)        | ✅ done  |
| Ack / retry / dead-letter from HTTP status        | ✅ done  |
| `arbiter` binary + TOML config (`migrate`/`serve`)| ✅ done  |
| HTTP claim/ack/nack/extend (pull model)           | ✅ done  |
| OpenTelemetry metrics + tracing                    | ✅ done  |
| Rate-limit & concurrency over HTTP (pull + push)  | ✅ done  |
| Cron schedules declared in TOML                   | ✅ done  |
| Response body captured as the job result          | planned  |
| Push-async ("park until callback")                | planned  |
| Static single binary + Alpine image               | planned  |

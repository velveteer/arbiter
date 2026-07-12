# Running arbiter in production

arbiter is a single binary (`arbiter`) plus a PostgreSQL database. It serves a
REST API for producing and consuming jobs, and — for queues you configure with a
webhook — runs workers that push jobs to your HTTP handlers. Everything is
driven by one TOML file (see [`arbiter.example.toml`](../arbiter.example.toml)).

```
   producers                      arbiter (N replicas)                 handlers
  (any language) ── POST job ──▶  ┌──────────────────┐  ── POST ──▶  (any language,
                                  │  REST API        │   (signed)      incl. serverless)
   pull workers  ◀── claim/ack ──▶│  claim engine    │
  (any language)                  │  webhook workers │  ◀─────────────  PostgreSQL
                                  └──────────────────┘  (the only dependency)
```

Every replica can serve the API and run workers concurrently against the same
Postgres — the claim engine guarantees no job is processed twice. Scale by adding
replicas.

---

## 1. Inserting jobs

Producers POST to `/api/v1/queues/{queue}/jobs`. Only `payload` is required.
(arbiter does no auth; your proxy/gateway does — see §Auth.)

```bash
curl -X POST https://arbiter.example.com/api/v1/queues/emails/jobs \
  -H "Content-Type: application/json" \
  -d '{
    "payload":     { "to": "ada@example.com", "template": "welcome" },
    "groupKey":    "ada@example.com",
    "priority":    0,
    "maxAttempts": 5
  }'
```

Optional fields: `groupKey` (serial processing per key), `priority` (lower first),
`notVisibleUntil` (ISO-8601 delay), `dedupKey`, `maxAttempts`, and admission keys:

```jsonc
{
  "payload": { … },
  "rateLimit":   { "prefix": "send",   "suffix": "acme.test", "cost": 1 },
  "concurrency": { "prefix": "tenant", "suffix": "acme"                 }
}
```

`rateLimit`/`concurrency` attach the job to a policy defined in the config (see
§Admission). Python and Node are just HTTP:

```python
import requests
requests.post("https://arbiter.example.com/api/v1/queues/emails/jobs",
    json={"payload": {"to": "ada@example.com"}}).raise_for_status()
```

```js
await fetch("https://arbiter.example.com/api/v1/queues/emails/jobs", {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: JSON.stringify({ payload: { to: "ada@example.com" } }),
});
```

> **Transactional enqueue.** HTTP enqueue is not transactional with your app's own
> database writes. If you need "commit ⇒ job exists, rollback ⇒ no job", that's a
> planned SQL-function path — until then, enqueue after your transaction commits and
> make the work idempotent.

## 2. Consuming jobs

**Push (webhook)** — the simplest: configure a `[queue.webhook]` and arbiter's
workers deliver each job to your endpoint. Your HTTP status decides the outcome:
`2xx` acks, `4xx` (except `408`/`429`) dead-letters, `408`/`429`/`5xx`/timeout retry
with backoff. Requests are
HMAC-signed; verify them (see the webhook contract in
[`README.md`](../README.md)). Serverless targets (Lambda URL, Cloud Run) work as
webhook handlers.

**Pull** — for workers behind NAT or batch consumers, claim over HTTP (same
admission applies):

```
POST /api/v1/queues/{queue}/claim            {"maxJobs":10}      -> {"jobs":[…]}
POST /api/v1/queues/{queue}/jobs/{id}/fail   {"attempts":N, "claimedBy":"…", "error":"…"}
POST /api/v1/queues/{queue}/jobs/{id}/ack    {"attempts":N, "claimedBy":"…", "result":…}
POST /api/v1/queues/{queue}/jobs/{id}/nack   {"attempts":N, "claimedBy":"…"}
POST /api/v1/queues/{queue}/jobs/{id}/extend {"attempts":N, "claimedBy":"…", "visibilitySecs":300}
```

Each claimed job carries `primaryKey`, `attempts`, and a `claimedBy` lease token;
echo `claimedBy` and `attempts` back to prove you hold the lease. A stale or
forged lease returns `409`.

## 3. Running it

Build the image once (context is the repo root):

```bash
docker build -f arbiter-cli/Dockerfile -t arbiter:latest .
```

### Kubernetes

Apply [`deploy/kubernetes.yaml`](deploy/kubernetes.yaml) — a Secret, ConfigMap,
migrate Job, Deployment, Service, Ingress, and HPA:

```bash
kubectl apply -f arbiter-cli/deploy/kubernetes.yaml
```

What it sets up:

- **Migrations** run as a `Job` (`arbiter migrate`) before the rollout. `serve`
  also migrates idempotently at boot, but the Job gives you a controlled step.
- **`serve`** runs as a Deployment of N replicas. Add replicas (or the included
  HPA) to scale both API throughput and webhook workers.
- **Secrets** — the DB URL and webhook secrets live in a `Secret`, mounted as
  files, and the config references them with `url_file`/`secret_file`. The
  `ConfigMap` (arbiter.toml) holds no secrets, so it's safe to commit. For real
  clusters, feed the `Secret` from an external manager.
- **Probes** — `livenessProbe: /healthz` (process up, and deliberately
  DB-independent so a Postgres blip cannot cascade-restart the fleet) and
  `readinessProbe: /readyz` (Postgres reachable). Readiness answers "can this pod
  serve API traffic", nothing more. A stuck worker is not a traffic-gating
  condition, so it is not a probe: alert on `arbiter_queue_depth` and
  `arbiter_queue_oldest_ready_age`.
- **Auth** — arbiter does none of its own. **The Ingress/gateway owns TLS and
  authentication** (SSO, tokens, IP allowlists); arbiter speaks plain HTTP behind
  it and trusts whoever it lets through. The admin UI is for operators.

### Docker Compose (single host)

[`deploy/compose.yaml`](deploy/compose.yaml) brings up Postgres + a migrate step
+ `serve`:

```bash
docker compose -f arbiter-cli/deploy/compose.yaml up
```

### Other platforms

- **Cloud Run / Fargate / Nomad** — run the image with `serve -c <config>`;
  point it at managed Postgres (Cloud SQL/RDS/AlloyDB); put HTTPS/identity in front
  (Cloud Run ingress, an ALB, a gateway). Use `/healthz` + `/readyz`.
- **systemd** — drop the static binary on a host, `arbiter migrate` then `arbiter
  serve` as a unit; deliver secrets via `LoadCredential` and reference them with
  `*_file`.

## 4. Operations

- **Secrets** never need to be plaintext in the config: use `${ENV}` interpolation
  or `*_file` references (Docker/K8s/systemd secret mounts). arbiter fails to start
  on a missing reference.
- **Auth** — arbiter does none. Put a reverse proxy / API gateway in front to
  terminate TLS and authenticate (SSO/tokens/allowlists); it decides who reaches
  the API and admin UI.
- **Admission** — define `[[ratelimit]]`/`[[concurrency]]` policies; producers
  attach jobs with `rateLimit`/`concurrency` keys; limits are enforced at claim on
  both the pull and webhook paths.
- **Scaling** — replicas are safe to add/remove at will; the claim engine handles
  concurrency across them. Webhook worker count per queue is `[queue.webhook].workers`.
- **Migrations** — additive and tracked; run `arbiter migrate` (Job/initContainer)
  as a pre-rollout step for schema changes.

## 5. Observability

arbiter is instrumented with OpenTelemetry. A Prometheus `/metrics` endpoint is
served on a **dedicated port (default 9464)**, separate from the API. Keep it off
your public ingress and scrape the pod/Service directly (the K8s manifest sets
`prometheus.io/*` annotations; with the Prometheus Operator, use a ServiceMonitor).
Configure it under `[telemetry]`.

**OTLP export** (traces, metrics, and logs) is driven by the standard `OTEL_*` env vars —
no arbiter config. Set `OTEL_EXPORTER_OTLP_ENDPOINT` to push to a collector and `OTEL_SERVICE_NAME`
to name the service. The shared endpoint feeds every signal, as the OpenTelemetry spec
says it should: traces, metrics, and logs. Per-signal endpoints
(`OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` and friends) override it, and any signal can be
switched off with `OTEL_TRACES_EXPORTER=none` / `OTEL_METRICS_EXPORTER=none` /
`OTEL_LOGS_EXPORTER=none`.

Point the shared endpoint at a trace-only backend (Tempo, Jaeger) and it will reject the
metrics and logs, so turn those off there: `OTEL_METRICS_EXPORTER=prometheus` keeps
metrics on the scrape endpoint, and `OTEL_LOGS_EXPORTER=none` keeps logs on stdout.
Exported logs are emitted *alongside* stdout, never instead of it, so container logs
never go quiet.

The one deliberate departure from the spec: with no `OTEL_*` variable set at all, arbiter
exports nothing rather than defaulting to a collector on localhost, which a library linked
into your process has no business doing uninvited. It stays scrape-only until you ask.

What you get:

- **Jobs** — `arbiter_jobs_claimed`, `arbiter_jobs_processed{outcome}` (success /
  retry / dlq), `arbiter_job_handler_duration` (histogram), all by `queue`.
- **HTTP** — `http_server_request_duration` / `http_server_active_requests` by
  route, method, and status, with trace exemplars (the produce/pull API plane).
- **Queue depth** — `arbiter_queue_depth{queue,status}`,
  `arbiter_queue_oldest_ready_age{queue}` (backlog latency), `arbiter_workers{queue,state}`.
- **Postgres health** (a Postgres-backed queue's real failure modes) —
  `arbiter_pg_table_dead_tuples` / `arbiter_pg_table_autovacuum_age` (bloat /
  vacuum lag), `arbiter_pg_connections{state}`, `arbiter_pg_oldest_transaction_age`
  (what starves autovacuum), `arbiter_pg_xid_age` (wraparound headroom).
- **Postgres counters**, cumulative — `arbiter_pg_blocks{source}` (rate() both to
  get a live cache hit ratio), `arbiter_pg_transactions{outcome}`,
  `arbiter_pg_deadlocks`. Postgres reports these as lifetime totals, so rate them
  over a window: the lifetime average of a database that has been up for days
  barely moves during an incident.

Depth and health gauges are polled on a slow background cadence and cached, so scrape
frequency adds no database load. They describe the database rather than the process, so one
replica per interval scans it and publishes what it read for the others to export: replica
count adds no scan load either. Every pod reports the same series for them, so aggregate
across pods with `max by (...)` rather than `sum`, as the shipped dashboard does.

## Checklist

- [ ] Managed Postgres reachable from the pods; `database.url` via `url_file`/`${ENV}`
- [ ] TLS + authentication terminated at the ingress/gateway (arbiter does no auth)
- [ ] `migrate` runs before/with the rollout
- [ ] Liveness `/healthz`, readiness `/readyz` wired to probes
- [ ] Metrics port (9464) scraped directly, kept off the public ingress
- [ ] Webhook handlers verify the HMAC signature and are idempotent on `X-Arbiter-Job-Id`

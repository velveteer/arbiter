# Arbiter

A PostgreSQL job queue for Haskell applications.

- Transactional job processing: jobs and database operations commit together
- At-least-once delivery with visibility timeouts and heartbeats
- Per-group ordering (partitioned FIFO)
- Concurrent worker pools with `LISTEN/NOTIFY` wakeups and polling fallback
- Dead-letter queues
- Opt-in archiving of completed jobs with per-job retention
- Job trees with fan-out/fan-in result collection
- Cron/periodic job scheduling
- Job deduplication via unique keys
- Cross-queue per-job rate limiting with operator-tunable token-bucket policies
- Cross-queue per-job concurrency limits: at most N jobs with the same key can be in flight
- Integrated OpenTelemetry traces, with metrics and logs from `arbiter-otel`
- Observability callbacks, structured logging
- REST API with SSE and an embedded admin UI
- File-based liveness probes for Kubernetes and systemd
- More than 1,000 integration tests

> [!NOTE]
>
> The API is subject to breaking changes. A Hackage release following PVP is tentative.

# Summary

[Overview](introduction.md)

- [Installation](installation.md)
- [Quick Start](quick-start.md)
- [Architecture](architecture.md)

# Job Features

- [Priority](features/priority.md)
- [Payload Kinds](features/kinds.md)
- [Deduplication](features/deduplication.md)
- [Job Results](features/results.md)
- [Job Trees](features/job-trees.md)
- [Cron Jobs](features/cron.md)
- [Error Handling](features/error-handling.md)
- [Dead-Letter Queue](features/dead-letter-queue.md)
- [Rate Limiting](features/rate-limiting.md)
- [Concurrency Limiting](features/concurrency-limiting.md)
- [Archiving Completed Jobs](features/archiving.md)

# Workers

- [Worker Configuration](worker/configuration.md)
- [Batched Handlers](worker/batched-handlers.md)
- [Observability Hooks](worker/hooks.md)
- [Leases and Deadlines](worker/deadlines.md)
- [Graceful Shutdown](worker/shutdown.md)
- [Backoff Strategies](worker/backoff.md)
- [Wakeups (LISTEN/NOTIFY)](worker/wakeups.md)
- [Logging](worker/logging.md)
- [Liveness Probes](worker/liveness.md)
- [Pausing Work](worker/pause.md)

# Integration

- [REST API and Admin UI](rest-api.md)
- [OpenTelemetry](opentelemetry.md)
- [Backend Integration](backends/index.md)
  - [arbiter-simple](backends/simple.md)
  - [arbiter-orville](backends/orville.md)
  - [arbiter-hasql](backends/hasql.md)
  - [Writing a Backend](backends/custom.md)

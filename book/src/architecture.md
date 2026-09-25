# Architecture

Each worker pool claims jobs from PostgreSQL. There is no broker and no leader.
Worker capacity scales with process count.

<svg class="arb-diagram" viewBox="0 0 760 300" role="img" aria-label="A job moves from queued to in flight. From there it is acked, retried back to queued, or dead-lettered."><defs><marker id="arb-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor"/></marker></defs><g class="arb-node"><rect x="20" y="120" width="160" height="56" rx="8"/><text x="100" y="144">Queued</text><text x="100" y="162" class="arb-sub">visible now or later</text></g><g class="arb-node arb-active"><rect x="290" y="120" width="180" height="56" rx="8"/><text x="380" y="144">In flight</text><text x="380" y="162" class="arb-sub">hidden, heartbeating</text></g><g class="arb-node"><rect x="580" y="120" width="160" height="56" rx="8"/><text x="660" y="144">Acked</text><text x="660" y="162" class="arb-sub">archived if enabled</text></g><g class="arb-node"><rect x="290" y="16" width="180" height="48" rx="8"/><text x="380" y="45">Retry, after backoff</text></g><g class="arb-node arb-terminal"><rect x="290" y="232" width="180" height="48" rx="8"/><text x="380" y="261">Dead-letter queue</text></g><g class="arb-edge"><path d="M 180 148 L 282 148" marker-end="url(#arb-arrow)"/><text x="231" y="139">claim</text><path d="M 470 148 L 572 148" marker-end="url(#arb-arrow)"/><text x="521" y="139">success</text><path d="M 380 120 L 380 72" marker-end="url(#arb-arrow)"/><text x="392" y="100" text-anchor="start">retryable</text><path d="M 290 40 L 100 40 L 100 112" marker-end="url(#arb-arrow)"/><path d="M 380 176 L 380 224" marker-end="url(#arb-arrow)"/><text x="398" y="199" text-anchor="start">attempts spent, or permanent</text><path d="M 290 256 L 60 256 L 60 184" marker-end="url(#arb-arrow)" stroke-dasharray="4 4"/><text x="175" y="273">retry from the DLQ</text><path d="M 300 176 C 250 212, 200 212, 155 182" marker-end="url(#arb-arrow)"/><text x="228" y="216">timeout lapsed, or nack</text></g></svg>

The lifecycle under `transactionalWorkerConfig`:

1. **Claim:** The dispatcher claims visible jobs in per-group order,
   increments each attempt count, and hides each job for the visibility
   timeout. The same statement applies admission: a job with an empty
   rate-limit bucket or a full concurrency pool is skipped. A heartbeat extends
   the timeout while the handler runs.
2. **Run:** The handler runs in a transaction. Its database work, its stored
   result, and the ack commit together.
3. **Success:** The transaction commits.
4. **Failure:** The transaction rolls back. A second transaction retries the
   job with backoff or moves it to the dead-letter queue (DLQ).
5. **Reclaim:** Another worker claimed the job after the visibility timeout.
   The heartbeat or the ack throws and the worker drops the job.

Admission limits apply to every worker pool in every process and to
[REST API](rest-api.md) clients.

Delivery is at least once. Non-transactional side effects must be idempotent.

With `manualWorkerConfig` and `defaultBatchedWorkerConfig`, step 2 has no
transaction and the handler finalizes each job through callbacks.

## Group Ordering

A group key admits **one job or batch at a time**. Different groups run
concurrently.

- **Same group key:** Insertion order within priority. A retrying job stays
  first until it succeeds or moves to the DLQ. A ready job runs before a
  delayed job. The group waits through a job's backoff or rate-limit delay.
- **No group key:** Any free worker, concurrently.

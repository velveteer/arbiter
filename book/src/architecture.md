# Architecture

Arbiter does not use a broker or central coordinator. Each worker pool claims
jobs directly from PostgreSQL. Add worker processes to increase capacity. There
is no leader process.

<svg class="arb-diagram" viewBox="0 0 760 300" role="img" aria-label="A job moves from queued to in flight. From there it is acked, retried back to queued, or dead-lettered."><defs><marker id="arb-arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M 0 0 L 10 5 L 0 10 z" fill="currentColor"/></marker></defs><g class="arb-node"><rect x="20" y="120" width="160" height="56" rx="8"/><text x="100" y="144">Queued</text><text x="100" y="162" class="arb-sub">visible now or later</text></g><g class="arb-node arb-active"><rect x="290" y="120" width="180" height="56" rx="8"/><text x="380" y="144">In flight</text><text x="380" y="162" class="arb-sub">hidden, heartbeating</text></g><g class="arb-node"><rect x="580" y="120" width="160" height="56" rx="8"/><text x="660" y="144">Acked</text><text x="660" y="162" class="arb-sub">archived if enabled</text></g><g class="arb-node"><rect x="290" y="16" width="180" height="48" rx="8"/><text x="380" y="45">Retry, after backoff</text></g><g class="arb-node arb-terminal"><rect x="290" y="232" width="180" height="48" rx="8"/><text x="380" y="261">Dead-letter queue</text></g><g class="arb-edge"><path d="M 180 148 L 282 148" marker-end="url(#arb-arrow)"/><text x="231" y="139">claim</text><path d="M 470 148 L 572 148" marker-end="url(#arb-arrow)"/><text x="521" y="139">success</text><path d="M 380 120 L 380 72" marker-end="url(#arb-arrow)"/><text x="392" y="100" text-anchor="start">retryable</text><path d="M 290 40 L 100 40 L 100 112" marker-end="url(#arb-arrow)"/><path d="M 380 176 L 380 224" marker-end="url(#arb-arrow)"/><text x="398" y="199" text-anchor="start">attempts spent, or permanent</text><path d="M 290 256 L 60 256 L 60 184" marker-end="url(#arb-arrow)" stroke-dasharray="4 4"/><text x="175" y="273">retry from the DLQ</text><path d="M 300 176 C 250 212, 200 212, 155 182" marker-end="url(#arb-arrow)"/><text x="228" y="216">timeout lapsed, or nack</text></g></svg>

The lifecycle under `transactionalWorkerConfig`:

1. **Claim:** The dispatcher claims visible jobs in per-group order,
   increments each attempt count, and hides each job for the visibility
   timeout. Admission is part of the same statement: a job whose rate-limit
   bucket is empty or whose concurrency pool is full is not claimed, and a
   claimed job has already spent its tokens and taken its slot. A heartbeat
   extends the timeout while the handler runs.
2. **Run:** The worker runs the handler inside a transaction. The handler's
   database work, its stored result, and the ack commit together.
3. **Success:** The job is acked and the transaction commits.
4. **Failure:** The transaction rolls back. A separate transaction retries the
   job with backoff or moves it to the dead-letter queue (DLQ).
5. **Reclaim:** If the visibility period ended and another worker claimed the job, the
   heartbeat or the ack throws and the worker abandons the job.

The claim operation applies the admission limits. The limits apply to worker
pools in all processes and to clients that use the [REST API](rest-api.md).
Claimants do not coordinate with each other.

Delivery is at least once. Arbiter can run a job again after a worker crash or
an expired visibility timeout. Make non-transactional side effects idempotent.

With `manualWorkerConfig` and `defaultBatchedWorkerConfig`, step 2 does not
start a transaction. The handler uses callbacks to complete, fail, cancel, or
nack each job. The claim, heartbeat, and reclaim operations are unchanged.

## Group Ordering

A group key permits **one job or batch at a time** in that group. Different
groups can run concurrently.

- **Same group key:** Eligible jobs run in insertion order within each priority.
  A retrying job remains first until it succeeds or moves to the DLQ. A ready
  job can run before a delayed job. A group waits during a job's retry backoff
  or rate-limit delay.
- **No group key:** Any available worker can run the job concurrently.

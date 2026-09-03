# Dead-Letter Queue

Arbiter moves a job to its queue's DLQ after its last attempt or after a
handler calls `throwPermanent`. See [Error Handling](error-handling.md).

A `DLQJob` contains two identifiers. `dlqPrimaryKey` identifies the DLQ row.
`jobSnapshot` contains the failed job, including its identifier, payload,
attempt count, and last error. `retryFromDLQ` and `deleteDLQJob` accept the DLQ
row identifier.

```haskell
import Arbiter.Core.Job.DLQ qualified as DLQ

entries <- Arb.listDLQJobs @OrderPayload 50 0
traverse_
  (\e -> logFailure (DLQ.dlqPrimaryKey e) (Arb.lastError (DLQ.jobSnapshot e)))
  entries
```

## Retry

`retryFromDLQ` takes a DLQ row id and returns the requeued job.

```haskell
requeued <- Arb.retryFromDLQ @OrderPayload dlqId
```

A retry recovers the applicable DLQ tree in one statement. The specified row
can be any member of the tree. Arbiter restores the root, all descendants in
the DLQ, and their finalizers. A retry of one failed fan-out child also restores
failed siblings.

A restored finalizer is suspended if it has children in the DLQ or main queue.
A finalizer with no children is ready and uses its stored snapshot. Arbiter
suspends a queued rollup parent when it restores children below that parent.
Arbiter refuses to restore a child if its parent is no longer in the main queue.
This rule prevents orphan jobs.

A retried job retains its job identifier, payload, priority, group key, parent
link, attempt limit, retention, and admission keys. Arbiter clears its attempt
count and error, and makes it immediately visible. The retained identifier
preserves parent links in the restored tree.

The `parentState` in a DLQ rollup finalizer snapshot contains the collected
child results. Arbiter records this state before it deletes the children.

> [!IMPORTANT]
> A retry removes the deduplication key. After a job has entered the DLQ, a new
> insert can use the old [`IgnoreDuplicate`](deduplication.md) key.

## Deletion

`deleteDLQJob` removes one entry and `deleteDLQJobsBatch` removes several. Both
are permanent.

The [REST API and admin UI](../rest-api.md) expose the same list, retry, and
delete operations.

See the [`Arbiter.Core.Job.DLQ` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-DLQ.html) for the entry type.

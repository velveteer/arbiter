# Dead-Letter Queue

A job moves to its queue's DLQ after its last attempt or after
`throwPermanent`. Failure modes: [Error Handling](error-handling.md).

`dlqPrimaryKey` is the DLQ row id, which `retryFromDLQ` and `deleteDLQJob`
take. `jobSnapshot` is the failed job.

```haskell
import Arbiter.Core.Job.DLQ qualified as DLQ

entries <- Arb.listDLQJobs @OrderPayload 50 0
traverse_
  (\e -> logFailure (DLQ.dlqPrimaryKey e) (Arb.lastError (DLQ.jobSnapshot e)))
  entries
```

## Retry

```haskell
requeued <- Arb.retryFromDLQ @OrderPayload dlqId
```

One retry restores the whole tree that contains the row, in one statement:

- The root, every descendant in the DLQ, and their finalizers return to the
  main queue.
- A retry of one fan-out child also restores its failed siblings.
- A restored finalizer is suspended while it has children in the DLQ or main
  queue. With no children it is ready and uses its stored snapshot.
- A queued rollup parent is suspended when children are restored below it.
- A child whose parent has left the main queue is not restored.

A retried job keeps its id, payload, priority, group key, parent link, attempt
limit, retention, and admission keys. Its attempt count and error are cleared.
It is visible at once.

`parentState` in a rollup finalizer snapshot holds the child results collected
before the children were deleted.

> [!IMPORTANT]
> A retry drops the deduplication key. After a job enters the DLQ, a new insert
> can reuse its [`IgnoreDuplicate`](deduplication.md) key.

## Deletion

`deleteDLQJob` removes one entry. `deleteDLQJobsBatch` removes several. Both
are permanent.

The [REST API and admin UI](../rest-api.md) expose list, retry, and delete.

Entry type: [`Arbiter.Core.Job.DLQ` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-DLQ.html).

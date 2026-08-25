# Dead-Letter Queue

A job lands in its queue's DLQ when it spends its last attempt or when a
handler throws `throwPermanent`. See [Error Handling](error-handling.md).

A `DLQJob` carries two identities. `dlqPrimaryKey` is the DLQ row, and
`jobSnapshot` is the failed job exactly as it stood, including its payload,
attempt count, and last error. `retryFromDLQ` and `deleteDLQJob` both take the
DLQ row id, while the job's own id lives on the snapshot.

```haskell
import Arbiter.Core.Job.DLQ qualified as DLQ

entries <- Arb.listDLQJobs @OrderPayload 50 0
traverse_
  (\e -> logFailure (DLQ.dlqPrimaryKey e) (Arb.lastError (DLQ.jobSnapshot e)))
  entries
```

## Retrying

`retryFromDLQ` takes a DLQ row id and returns the requeued job.

```haskell
requeued <- Arb.retryFromDLQ @OrderPayload dlqId
```

The retry recovers a whole DLQ'd tree, whichever member you name. The root,
every DLQ'd descendant, and the finalizers above them come back in one
statement. Naming one failed child of a fan-out therefore restores its siblings
too, if they failed with it.

A finalizer comes back suspended when it has children again, whether they
returned with it or were already in the main queue. With no children it comes
back ready and runs on the snapshot it captured. A rollup parent already in the
queue is re-suspended when fresh children land under it. A root whose parent
has already left the main queue is refused, which keeps the retry from
orphaning children.

A retried job keeps its original job id, which keeps a recovered tree's parent
links valid. It carries its payload, priority, group key, parent link, attempt
limit, retention, and admission keys, and comes back with a cleared attempt
count and error and immediate visibility.

A DLQ'd rollup finalizer keeps the child results it had collected on its
snapshot's `parentState`, captured before the cascade deleted the children.

> [!IMPORTANT]
> A retry drops the dedup key. A job that was inserted under
> [`IgnoreDuplicate`](deduplication.md) no longer holds that key once it has
> been through the DLQ, and a later insert on the same key is admitted.

## Discarding

`deleteDLQJob` removes one entry and `deleteDLQJobsBatch` removes several. Both
are permanent.

The [REST API and admin UI](../rest-api.md) expose the same list, retry, and
delete operations.

See the [`Arbiter.Core.Job.DLQ` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-DLQ.html) for the entry type.

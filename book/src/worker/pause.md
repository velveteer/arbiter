# Pausing Work

Pause stops claiming. It leaves in-flight jobs alone, so a paused queue drains
what it already started.

| Scope | Function | Effect |
| --- | --- | --- |
| Queue | `setQueuePaused` | Every pool stops claiming from that queue. |
| Pool | `setWorkerPaused` | One pool stops claiming. The others carry on. |
| Job | `suspendJob`, `resumeJob` | One job stays invisible until it is resumed. |
| Subtree | `pauseChildren`, `resumeChildren` | Every claimable job below a job, at any depth. |

`pauseChildren` skips what it cannot suspend cleanly: an in-flight job, and a
job already waiting out a delay or a backoff. `resumeChildren` leaves a
finalizer suspended while its own children are still queued, since it wakes
itself once they finish.

A pause rides `LISTEN/NOTIFY` and reaches running pools at once. Without a
listener it is reconciled at the worker heartbeat, so it takes up to one
interval. See [Wakeups](wakeups.md).

The [REST API and admin UI](../rest-api.md) carry the same controls, so an
operator needs no deploy.

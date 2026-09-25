# Pausing Work

A pause stops new claims. In-flight jobs run to completion.

| Scope | Function | Effect |
| --- | --- | --- |
| Queue | `setQueuePaused` | every pool stops claiming from the queue |
| Pool | `setWorkerPaused` | one pool stops claiming |
| Job | `suspendJob`, `resumeJob` | one job stays invisible until resumed |
| Subtree | `pauseChildren`, `resumeChildren` | every claimable job below a job, at any depth |

`pauseChildren` skips in-flight jobs and jobs in a delay or backoff.
`resumeChildren` leaves a finalizer suspended while its children are queued.

`LISTEN/NOTIFY` delivers a pause at once. Without a listener, each pool reads
the pause state at its next worker heartbeat. Details: [Wakeups](wakeups.md).

The [REST API and admin UI](../rest-api.md) expose the same controls.

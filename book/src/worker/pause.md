# Pausing Work

A pause stops new claims. It does not stop in-flight jobs. These jobs can
complete while the queue is paused.

| Scope | Function | Effect |
| --- | --- | --- |
| Queue | `setQueuePaused` | Every pool stops claiming from that queue. |
| Pool | `setWorkerPaused` | One pool stops claiming. Other pools continue. |
| Job | `suspendJob`, `resumeJob` | One job stays invisible until it is resumed. |
| Subtree | `pauseChildren`, `resumeChildren` | Every claimable job below a job, at any depth. |

`pauseChildren` does not suspend in-flight jobs or jobs that have a delay or
backoff. `resumeChildren` keeps a finalizer suspended if its children are still
queued. Arbiter resumes the finalizer after its children finish.

`LISTEN/NOTIFY` sends a pause notification to running pools. Without a listener,
each pool reads the pause state at its next worker heartbeat. This can take one
`workerHeartbeatInterval`. See [Wakeups](wakeups.md).

The [REST API and admin UI](../rest-api.md) provide the same controls. An
operator can pause work without a deployment.

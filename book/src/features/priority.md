# Priority

Each job has an integer `priority`. Arbiter claims lower numbers first. The
default is `0`. Use a higher number for background work.

```haskell
-- runs behind default-priority work
job = Arb.defaultJob payload & Arb.setPriority 10
```

For equal priorities, Arbiter uses insertion order.

Priority applies during a claim. It does not preempt work that is in flight. A
new high-priority job waits for an available worker.

In a [group](../architecture.md#group-ordering), a retrying job remains first
until it succeeds or moves to the DLQ. This rule has precedence over the
priority of other jobs in that group.

A group is eligible when it has a ready job. Arbiter ranks the group by the
lowest priority number in that group, including delayed jobs. Thus, a delayed
high-priority job increases the rank of its group.

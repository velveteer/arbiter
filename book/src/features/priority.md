# Priority

Jobs carry an integer `priority` and the lower numbers are claimed first. The
default is `0`, so give background work a higher number to defer it behind
normal jobs.

```haskell
-- runs behind default-priority work
job = Arb.defaultJob payload & Arb.setPriority 10
```

Equal priorities break by insertion order. Priority reorders jobs while peers
keep their FIFO turn.

Priority applies when a job is claimed. Work already in flight runs to
completion, and a high-priority arrival waits for a worker to come free.

Inside a [group](../architecture.md#group-ordering) a retrying job outranks
priority: it holds the head of its group until it succeeds or dead-letters,
even against a higher-priority sibling.

A group becomes eligible once it has a ready job, and ranks by the lowest
priority number anywhere in the group. A high-priority job scheduled for later
therefore raises the rank of its whole group.

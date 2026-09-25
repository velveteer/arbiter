# Deduplication

A dedup key sets what happens when the key is already queued:

```haskell
-- skip the insert
job1 = Arb.defaultJob payload & Arb.setDedupKey (Just $ IgnoreDuplicate "order-123")

-- replace the queued job and make it ready
job2 = Arb.defaultJob payload & Arb.setDedupKey (Just $ ReplaceDuplicate "order-123")
```

Keys are scoped to one queue.

`ReplaceDuplicate` copies payload, priority, group key, attempt limit, admission
keys, and retention from the new job. It clears the attempt count, last error,
and claim.

A replacement is refused when the queued job is in flight, has a force-cancel
flag, or has children in the queue or DLQ. The queued job is unchanged.

| Outcome | `insertJob` returns |
| --- | --- |
| Inserted | `Just` the new job |
| Replaced | `Just` the updated job |
| Skipped by `IgnoreDuplicate` | `Nothing` |
| Replacement refused | `Nothing` |

Key type: [`Arbiter.Core.Job.Dedup` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Dedup.html).

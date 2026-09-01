# Deduplication

A dedup key decides what happens when a job's key is already queued:

```haskell
-- IgnoreDuplicate: silently skip if key exists
job1 = Arb.defaultJob payload & Arb.setDedupKey (Just $ IgnoreDuplicate "order-123")

-- ReplaceDuplicate: replace the existing job and re-arm it for a fresh run
job2 = Arb.defaultJob payload & Arb.setDedupKey (Just $ ReplaceDuplicate "order-123")
```

Keys have queue scope. Jobs that have no key cannot cause a deduplication
conflict.

`ReplaceDuplicate` copies each writable column from the new job: payload,
priority, group key, attempt limit, admission keys, and retention. It clears
the attempt count, last error, and active claim. The updated job is then ready
for a new run.

Arbiter refuses replacement when the existing job is in flight, has a
force-cancel flag, or has children in the queue or DLQ. For a refused
replacement, `insertJob` returns `Nothing` and does not change the existing job.
This return value states that the key existed. It does not state if replacement
occurred.

See the [`Arbiter.Core.Job.Dedup` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Dedup.html) for the key type.

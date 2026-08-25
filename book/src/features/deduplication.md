# Deduplication

A dedup key decides what happens when a job's key is already queued:

```haskell
-- IgnoreDuplicate: silently skip if key exists
job1 = Arb.defaultJob payload & Arb.setDedupKey (Just $ IgnoreDuplicate "order-123")

-- ReplaceDuplicate: replace the existing job and re-arm it for a fresh run
job2 = Arb.defaultJob payload & Arb.setDedupKey (Just $ ReplaceDuplicate "order-123")
```

Keys are per queue, and a job without a key never collides with anything.

`ReplaceDuplicate` takes every writable column from the new job: payload,
priority, group key, attempt limit, admission keys, and retention. It then
re-arms the row for a fresh run, clearing the attempt count, the last error,
and any outstanding claim.

Replacement is refused while the existing job is in flight, is flagged for
force-cancel, or has children in the queue or the DLQ. A refused replace looks
exactly like a skipped insert: `insertJob` returns `Nothing` and the existing
job runs on unchanged. A `Nothing` reports only that the key was already
present, whether or not your payload replaced anything.

See the [`Arbiter.Core.Job.Dedup` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Dedup.html) for the key type.

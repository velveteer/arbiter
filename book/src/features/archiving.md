# Archiving Completed Jobs

Completed jobs are deleted on ack by default. Set `archiveFor` to keep a copy
in a per-queue archive for that many seconds after completion.

```haskell
job1 = Arb.defaultJob payload & Arb.setArchiveFor (Just Arb.dayRetention)       -- 24h
job2 = Arb.defaultJob payload & Arb.setArchiveFor (Just $ Arb.dayRetention * 7) -- 1 week
```

Archiving is optional for each job. Arbiter automatically removes expired
entries. An archive entry contains the [result](results.md) from its handler.
Use the REST API or admin UI to list, re-enqueue, or delete archived jobs.

A re-enqueued job has no parent. It retains its payload and settings. Thus,
re-enqueueing one member of a completed [tree](job-trees.md) creates one
independent job. To recover a failed tree, retry it from the
[dead-letter queue](dead-letter-queue.md).

See the [`Arbiter.Core.Job.Archive` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Archive.html) for the archive row and its queries.

# Archiving Completed Jobs

An acked job is deleted. `archiveFor` keeps a copy in the queue's archive for
that many seconds:

```haskell
job1 = Arb.defaultJob payload & Arb.setArchiveFor (Just Arb.dayRetention)       -- 24h
job2 = Arb.defaultJob payload & Arb.setArchiveFor (Just $ Arb.dayRetention * 7) -- 1 week
```

The archive entry holds the handler's [result](results.md). The reaper deletes
expired entries. The REST API and admin UI list, re-enqueue, and delete
archived jobs.

A re-enqueued job keeps its payload and settings and has no parent. Retry a
failed [tree](job-trees.md) from the [dead-letter queue](dead-letter-queue.md).

Archive row and queries: [`Arbiter.Core.Job.Archive` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Archive.html).

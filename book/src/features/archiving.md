# Archiving Completed Jobs

Completed jobs are deleted on ack by default. Set `archiveFor` to keep a copy
in a per-queue archive for that many seconds after completion.

```haskell
job1 = Arb.defaultJob payload & Arb.setArchiveFor (Just Arb.dayRetention)       -- 24h
job2 = Arb.defaultJob payload & Arb.setArchiveFor (Just $ Arb.dayRetention * 7) -- 1 week
```

Archiving is opt-in per job, and expired entries are purged automatically. The
archive entry keeps the [result](results.md) its handler returned, so it is
where you read back what a standalone job produced. The REST API and admin UI
list archived jobs, re-enqueue them as fresh jobs, and delete them.

A re-enqueued job starts standalone. It carries its payload and settings, and
leaves its parent link behind. Re-running one member of a finished
[tree](job-trees.md) therefore gives you that job alone. To recover a tree that
failed, retry it from the [dead-letter queue](dead-letter-queue.md).

See the [`Arbiter.Core.Job.Archive` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Archive.html) for the archive row and its queries.

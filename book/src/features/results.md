# Job Results

A handler can produce a value - its **result** - by returning it under
`transactionalWorkerConfig`, or by passing it to `ackWith`/`ackAllWith` under a
manual or batched config.

Producing a result does not store it. Where it goes, and whether it is kept at
all, depends on the job:

- **A job with a parent** (a node in a [tree](job-trees.md)) - the
result is kept for the parent to collect with
`Worker.childResults`/`Worker.mergedChildResults`, then cleaned up once the
parent completes.
- **A standalone (root) job** - the result is recorded on the job's
[archive](archiving.md) entry, if the job is archived. Without archiving there
is nowhere to keep it, so it is dropped.

A result is stored with its `ToJSON` and read back with its `FromJSON`, so your
own records and sum types work as results directly.

A parent collects with either `Worker.childResults` or
`Worker.mergedChildResults`. The merged one needs the result type to be a
`Monoid` and combines the children for you, but it treats a result it cannot
decode as `mempty`. That matters when the result type changes shape under a
running queue: rollups quietly come back short rather than failing.
`Worker.childResults` hands you each child's `Either` and leaves the decision
to you.

Make the queue's result type a `Maybe` to decide per run whether to keep
anything. `Nothing` stores nothing: no archive entry result, and no row for a
rollup parent to collect.

```haskell
data SyncReport = SyncReport
  { rowsChanged :: Int
  , notes :: [Text]
  }
  deriving stock (Eq, Show, Generic)
  deriving anyclass (ToJSON, FromJSON)

type SyncRegistry = '[ QueueWithResult "sync_queue" SyncPayload (Maybe SyncReport) ]

syncHandler :: Arb.JobHandler (ArbS.SimpleDb SyncRegistry IO) SyncPayload (Maybe SyncReport)
syncHandler _conn job = do
  report <- runSync (Arb.payload job)
  pure $ if rowsChanged report == 0 then Nothing else Just report
```

See the [`Arbiter.Core.JobResult` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-JobResult.html) for how a result is encoded.

# Job Results

A handler can produce a **result**. With `transactionalWorkerConfig`, return the
result. With a manual or batched configuration, pass it to `ackWith` or
`ackAllWith`.

Arbiter stores a result only in these conditions:

- **Job with a parent:** Arbiter stores the result for the parent to read with
  `Worker.childResults` or `Worker.mergedChildResults`. It removes the result
  after the parent completes.
- **Standalone root job:** Arbiter stores the result in the job
  [archive](archiving.md) entry if archiving is enabled for that job. If
  archiving is disabled, Arbiter discards the result.

Arbiter uses `ToJSON` to store a result and `FromJSON` to read it. Records and
sum types with these instances can be result types.

A parent can use `Worker.childResults` or `Worker.mergedChildResults`.
`Worker.mergedChildResults` requires a `Monoid` result type and combines the
child results. It replaces a result that it cannot decode with `mempty`. A
change to the result format can therefore cause an incomplete rollup.
`Worker.childResults` returns an `Either` for each child and lets the caller
handle decode errors.

Use a `Maybe` result type to make storage conditional for each run. `Nothing`
does not create an archive result or a result row for a parent.

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

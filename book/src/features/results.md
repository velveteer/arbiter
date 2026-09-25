# Job Results

`transactionalWorkerConfig` stores a handler's return value. Manual and
batched handlers pass results to `ackWith` or `ackAllWith`.

| Job | Where the result goes |
| --- | --- |
| Has a parent | Stored for the parent. Deleted when the parent completes. |
| Root, archiving on | Stored in the [archive](archiving.md) entry. |
| Root, archiving off | Discarded. |

Result types need `ToJSON` and `FromJSON`.

A parent reads its children with one of:

| Function | Returns |
| --- | --- |
| `Worker.childResults` | One `Either` per child. |
| `Worker.mergedChildResults` | The `Monoid` sum of the results and the DLQ failures. A result that fails to decode counts as `mempty`. |

`Nothing` in a `Maybe` result stores nothing:

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

Result encoding: [`Arbiter.Core.JobResult` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-JobResult.html).

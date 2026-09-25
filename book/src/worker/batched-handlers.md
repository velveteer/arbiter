# Batched Handlers

`defaultBatchedWorkerConfig workers batchSize handler` passes up to
`batchSize` jobs per call. A grouped batch holds one group. An ungrouped batch
holds ready jobs.

```haskell
-- defaultBatchedWorkerConfig <workerCount> <batchSize> handler
config <- Worker.defaultBatchedWorkerConfig 10 5 batchHandler

batchHandler
  :: NonEmpty (Arb.JobRead ImagePayload)
  -> Worker.BatchCallbacks (ArbS.SimpleDb AppRegistry IO) ImagePayload Score
  -> ArbS.SimpleDb AppRegistry IO ()
batchHandler jobs cbs = do
  -- bulkProcess :: [Arb.JobRead ImagePayload] -> IO [(Arb.JobRead ImagePayload, Score)]
  scored <- liftIO $ bulkProcess (toList jobs)
  -- Bulk-ack the whole batch in one transaction.
  Worker.ackAllWith cbs scored
```

Each callback commits on its own. `withDbTransaction` commits a callback with
application writes:

```haskell
batchHandler jobs cbs =
  for_ jobs $ \job -> do
    score <- liftIO $ scoreImage (Arb.payload job)
    Arb.withDbTransaction $ do
      recordCharge (Arb.payload job)
      Worker.ackWith cbs job score
```

`onJobSuccess` fires after the callback's transaction or savepoint completes.
Inside an outer transaction, this is before the outer commit. Put once-only
database effects in the ack's transaction rather than in the success hook.

### If an outer transaction rolls back

A settlement callback can return successfully and still have its database changes
rolled back by a later failure:

```haskell
Arb.withDbTransaction $ do
  Worker.ack cbs job   -- succeeds and releases its savepoint
  updateSomethingElse  -- if this throws, the outer transaction rolls back
```

In this case, let the exception escape the handler. Catching it to log and then
rethrow is fine. Do not catch it and continue processing or settling those jobs.
PostgreSQL restores the job, but the worker still considers it settled and no
longer heartbeats it. Another worker can reclaim it after its lease expires.

This restriction concerns an outer rollback **after a callback returned
successfully**, not every exception thrown by a callback. It also applies to
transactions opened directly through the driver. Success hooks already fired at
savepoint release are not undone by the rollback and may fire on redelivery.
They are not proof that the outer transaction committed.

| Callback | Effect |
| --- | --- |
| `ack`, `ackAll` | complete, no result |
| `ackWith`, `ackAllWith` | complete and store the queue [result](../features/results.md) |
| `failRetry`, `failPermanent`, `cancelBranch`, `cancelTree`, `nack` | see [Error Handling](../features/error-handling.md) |
| `spawn` | insert children under the job and suspend it. See [runtime spawning](../features/job-trees.md#spawning-children-at-runtime) |

`hoistBatchCallbacks` calls the callbacks from another monad through a monad
morphism. It must keep the worker's connection and schema so each callback
joins the handler's transaction. [arbiter-orville](../backends/orville.md)
provides `orvilleBatchedHandler`, which builds that morphism from the worker's
own env.

Callback signatures: [`BatchCallbacks` Haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html#t:BatchCallbacks).

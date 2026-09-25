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

`onJobSuccess` fires outside that transaction and can fire for a job that is
later redelivered. Put once-only effects in the ack's transaction.

| Callback | Effect |
| --- | --- |
| `ack`, `ackAll` | complete, no result |
| `ackWith`, `ackAllWith` | complete and store the queue [result](../features/results.md) |
| `failRetry`, `failPermanent`, `cancelBranch`, `cancelTree`, `nack` | see [Error Handling](../features/error-handling.md) |
| `spawn` | insert children under the job and suspend it; [runtime spawning](../features/job-trees.md#spawning-children-at-runtime) |

Callback signatures: [`BatchCallbacks` Haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html#t:BatchCallbacks).

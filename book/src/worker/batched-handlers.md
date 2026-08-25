# Batched Handlers

`defaultBatchedWorkerConfig` is `manualWorkerConfig` with more than one job per
invocation: the handler receives up to `batchSize` jobs and can amortize work
across them. Grouped jobs batch within one group, and ungrouped jobs batch from
the ready pool. Finalize each one through the same callbacks.

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

Each callback runs in its own transaction. Wrap one in your own
`withDbTransaction` to commit the ack together with your writes:

```haskell
batchHandler jobs cbs =
  for_ jobs $ \job -> do
    score <- liftIO $ scoreImage (Arb.payload job)
    Arb.withDbTransaction $ do
      recordCharge (Arb.payload job)
      Worker.ackWith cbs job score
```

`onJobSuccess` does not commit with those writes, and it can fire for a job
that is later reprocessed. Put effects that must happen exactly once in the
transaction next to the ack, not in the hook.

Dispositions are per job: a failure, cancel, or nack affects only that job, completed jobs stay done, and an untouched job is reprocessed. `ackWith` and `ackAllWith` carry the queue's [result](../features/results.md), while `ack` and `ackAll` store nothing and work on any queue. The [`BatchCallbacks` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html#t:BatchCallbacks) list every disposition.

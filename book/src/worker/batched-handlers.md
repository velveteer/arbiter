# Batched Handlers

`defaultBatchedWorkerConfig` configures a manual handler that receives up to
`batchSize` jobs in each invocation. The handler can combine operations for
these jobs. A batch of grouped jobs contains one group. A batch of ungrouped
jobs contains jobs from the ready set. Use the supplied callbacks to finalize
each job.

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

Each callback runs in a separate transaction. Wrap a callback in
`withDbTransaction` to commit the ack and application writes in one
transaction:

```haskell
batchHandler jobs cbs =
  for_ jobs $ \job -> do
    score <- liftIO $ scoreImage (Arb.payload job)
    Arb.withDbTransaction $ do
      recordCharge (Arb.payload job)
      Worker.ackWith cbs job score
```

`onJobSuccess` does not commit in the transaction with these writes. It can run
for a job that Arbiter later processes again. Put effects that must occur one
time in the same transaction as the ack.

A disposition applies to one job. A failure, cancellation, or nack does not
change completed jobs in the batch. Arbiter reprocesses an unfinalized job.
`ackWith` and `ackAllWith` store the queue [result](../features/results.md).
`ack` and `ackAll` do not store a result and work with all queues. The
[`BatchCallbacks` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Config.html#t:BatchCallbacks)
list all dispositions.

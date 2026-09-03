# Error Handling

```haskell
Arb.throwRetryable "API timeout"       -- retry with backoff
Arb.throwPermanent "Invalid payload"   -- move to DLQ immediately
Arb.throwTreeCancel "Pipeline aborted" -- cancel entire tree
Arb.throwBranchCancel "Subtask failed" -- cancel current branch
Arb.throwNack                          -- reprocess later, not a failure (no attempt consumed)
```

The `BatchCallbacks` record gives a batched handler these dispositions for each
job: `failRetry`, `failPermanent`, `cancelBranch`, `cancelTree`, and `nack`.
One job's disposition does not change other completed jobs in the batch. A
thrown exception applies to all jobs that the handler has not finalized.

Other exceptions are retryable. Arbiter retries a job until it reaches
`maxAttempts`, and then moves it to the DLQ. A payload decode error is
permanent because another attempt uses the same invalid payload. Arbiter moves
such a job directly to the DLQ.

## Exception Classification

Set the disposition where the application classifies the error:

```haskell
processCharge conn job = do
  result <- liftIO $ chargeCard (Arb.payload job)
  case result of
    Left (RateLimited retryAfter) -> Arb.throwRetryable ("gateway busy: " <> retryAfter)
    Left (CardDeclined reason) -> Arb.throwPermanent ("declined: " <> reason)
    Left (BadRequest reason) -> Arb.throwPermanent reason
    Right receipt -> pure receipt
```

A retryable error uses one attempt. Arbiter retries the job after the backoff.
A permanent error moves the job and its message directly to the DLQ.

`throwNack` does not record a failure, use an attempt, or call a failure hook.
Arbiter processes the job again after the remaining visibility period. Use
`throwNack` when a valid job has an unmet precondition:

```haskell
processExport conn job = do
  ready <- liftIO $ upstreamReady (Arb.payload job)
  unless ready Arb.throwNack
  runExport conn job
```

For a tree, `throwBranchCancel` cancels the current child and its branch.
`throwTreeCancel` cancels the complete tree. Cancellations do not call the
failure hook.

## Trace Errors

A failed job adds an error status and message to its consumer span. This applies
to retries and permanent failures. A batch span can include successful jobs,
and therefore does not get an error status. The spans for failed jobs contain
the error.

A cancel or nack does not change the span status. See
[OpenTelemetry](../opentelemetry.md).

See the [`Arbiter.Core.Exceptions` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Exceptions.html) for each disposition.

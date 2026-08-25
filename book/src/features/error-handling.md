# Error Handling

```haskell
Arb.throwRetryable "API timeout"       -- retry with backoff
Arb.throwPermanent "Invalid payload"   -- move to DLQ immediately
Arb.throwTreeCancel "Pipeline aborted" -- cancel entire tree
Arb.throwBranchCancel "Subtask failed" -- cancel current branch
Arb.throwNack                          -- reprocess later, not a failure (no attempt consumed)
```

A batched handler has the same dispositions per job through the
`BatchCallbacks` record (`failRetry`, `failPermanent`, `cancelBranch`,
`cancelTree`, `nack`), so one job's outcome does not affect the rest of the
batch. A throw applies to the jobs the handler has not yet finalized.

Any other exception is retryable. A job retries until it reaches its
`maxAttempts`, then moves to the DLQ. A payload that fails to decode is the
exception: it is permanent and goes straight to the DLQ, since retrying cannot
change the outcome.

## Classifying an exception

Decide the disposition where you know what the error means:

```haskell
processCharge conn job = do
  result <- liftIO $ chargeCard (Arb.payload job)
  case result of
    Left (RateLimited retryAfter) -> Arb.throwRetryable ("gateway busy: " <> retryAfter)
    Left (CardDeclined reason) -> Arb.throwPermanent ("declined: " <> reason)
    Left (BadRequest reason) -> Arb.throwPermanent reason
    Right receipt -> pure receipt
```

A retryable error costs an attempt and comes back after the backoff. A
permanent one goes straight to the DLQ with its message and skips the attempts
the job had left.

`throwNack` is not a failure. It costs no attempt, fires no failure hook, and
the job is reprocessed when its claim's remaining visibility runs out. Use it
when the job is valid but its precondition is not yet met:

```haskell
processExport conn job = do
  ready <- liftIO $ upstreamReady (Arb.payload job)
  unless ready Arb.throwNack
  runExport conn job
```

For a tree, `throwBranchCancel` gives up on this child and the branch it hangs
from, while `throwTreeCancel` abandons the whole tree. Both are cancellations,
not failures, so neither fires the failure hook.

## Errors in traces

A job that fails marks its consumer span with an error status and the message,
whether it retries or lands in the DLQ. A batch span is the exception: it also
covers the jobs that succeeded, so it keeps its status and only the per-job
spans carry the error.

A cancel and a nack leave the span status alone. See
[OpenTelemetry](../opentelemetry.md).

See the [`Arbiter.Core.Exceptions` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Exceptions.html) for each disposition.

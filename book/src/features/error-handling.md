# Error Handling

```haskell
Arb.throwRetryable "API timeout"             -- retry with backoff
Arb.throwRetryableAfter 30 "Rate limited"    -- retry in 30 seconds
Arb.throwPermanent "Invalid payload"         -- move to the DLQ now
Arb.throwTreeCancel "Pipeline aborted"       -- delete the tree
Arb.throwBranchCancel "Subtask failed"       -- delete the branch
Arb.throwNack                                -- reprocess after the lease, no attempt spent
Arb.throwNackAfter 60                        -- reprocess in 60 seconds, no attempt spent
```

| Exception | Attempt | Job | Hooks |
| --- | --- | --- | --- |
| `throwRetryable`, any other exception | spent | retried after backoff, DLQ at `maxAttempts` | `onJobFailure`, then `onJobRetry` or `onJobFailedAndMovedToDLQ` |
| `throwRetryableAfter` | spent | retried after the delay, DLQ at `maxAttempts` | `onJobFailure`, then `onJobRetry` or `onJobFailedAndMovedToDLQ` |
| `throwPermanent`, payload decode error | spent | DLQ, with the message | `onJobFailure`, then `onJobFailedAndMovedToDLQ` |
| `throwTreeCancel` | | root and descendants deleted | `onJobCancelled` |
| `throwBranchCancel` | | parent and its descendants deleted | `onJobCancelled` |
| `throwNack` | refunded | invisible for the rest of its lease | none |
| `throwNackAfter` | refunded | invisible for the delay | none |

Delays are in seconds and round up to whole seconds. The pool's backoff and
jitter do not apply to them. A grouped job parked by either delay holds its
group until it runs, as a backoff does.

A batched handler uses `failRetry`, `failRetryAfter`, `failPermanent`,
`cancelBranch`, `cancelTree`, `nack`, and `nackAfter` on `BatchCallbacks` per
job. A thrown exception applies to every job the handler has not finalized.

## Exception Classification

```haskell
processCharge conn job = do
  result <- liftIO $ chargeCard (Arb.payload job)
  case result of
    Left (RateLimited retryAfter) -> Arb.throwRetryableAfter retryAfter "gateway busy"
    Left (CardDeclined reason) -> Arb.throwPermanent ("declined: " <> reason)
    Left (BadRequest reason) -> Arb.throwPermanent reason
    Right receipt -> pure receipt
```

`throwNackAfter` for an unmet precondition, checked again in a minute:

```haskell
processExport conn job = do
  ready <- liftIO $ upstreamReady (Arb.payload job)
  unless ready (Arb.throwNackAfter 60)
  runExport conn job
```

## Trace Errors

A failed job sets an error status and message on its consumer span. A batch
span has no error status. A cancel or nack leaves the span status unchanged.
Trace details: [OpenTelemetry](../opentelemetry.md).

Exception dispositions: [`Arbiter.Core.Exceptions` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Exceptions.html).

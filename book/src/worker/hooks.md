# Observability Hooks

`ObservabilityHooks` holds one callback per lifecycle point. Start from
`defaultObservabilityHooks` and set fields:

```haskell
myHooks = Arb.defaultObservabilityHooks
  { Arb.onJobSuccess = \job startTime endTime ->
      liftIO $ recordHistogram "jobs.duration" (diffUTCTime endTime startTime)
  , Arb.onJobFailedAndMovedToDLQ = \err job ->
      liftIO $ sendAlert (Arb.primaryKey job) err
  , Arb.onJobHeartbeat = \job now startTime ->
      liftIO $ recordGauge "jobs.running_duration" (realToFrac $ diffUTCTime now startTime)
  }

config <- Worker.transactionalWorkerConfig 5 handler
let instrumented = config { Worker.observabilityHooks = myHooks }
```

Hooks run in the pool monad.

## Hook Invocation

Every claimed job fires `onJobClaimed`, then one row of:

| Outcome | Hooks |
| --- | --- |
| The handler returns | `onJobSuccess` |
| The handler fails with attempts left | `onJobFailure`, then `onJobRetry` with the backoff |
| The handler fails permanently or spends its last attempt | `onJobFailure`, then `onJobFailedAndMovedToDLQ` |
| The handler cancels a tree or a branch | `onJobCancelled` |
| The job went away mid-flight | `onJobUnavailable` |
| The handler nacks the job | none |

`onJobHeartbeat` fires on each lease extension of a running job.

## Hook Composition

`ObservabilityHooks` is a `Monoid`. `<>` runs the left callback, then the
right. The right callback runs even when the left one throws. `withHooks`
combines a record with a configuration's hooks:

```haskell
let instrumented = Worker.withHooks (myHooks <>) config
```

## Hook Restrictions

A hook's return value is ignored. If a hook throws, the worker logs the
exception at `Warning` and carries on.

`onJobSuccess` reports a completed ack operation, not a durable outer
transaction commit. In a batched handler's outer transaction, it fires when
the callback's savepoint is released. If the outer transaction rolls back, the
job is redelivered and the hook may fire again. See
[Batched Handlers](batched-handlers.md).

Reaper activity reports through `onMaintenance` on `WorkerConfig`.

## Hooks in Another Monad

`hoistObservabilityHooks` runs hooks written in one monad from a worker in
another, through a monad morphism such as `lift`:

```haskell
let pool = config {Worker.observabilityHooks = Arb.hoistObservabilityHooks lift appHooks}
```

[arbiter-orville](../backends/orville.md) provides `orvilleHooks` for this.

Callback arguments: [`ObservabilityHooks` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Types.html#t:ObservabilityHooks).

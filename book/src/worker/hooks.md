# Observability Hooks

`ObservabilityHooks` contains callbacks for points in the job lifecycle. Start
with `defaultObservabilityHooks`, update the required fields, and assign the
record to the pool configuration. The default callbacks have no effect.

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

A hook runs in the pool monad. It can read the database and write to a metrics
client.

## Which hook fires

Each claimed job calls `onJobClaimed`. It then has one of these outcomes:

| Outcome | Hooks |
| --- | --- |
| The handler returns | `onJobSuccess` |
| The handler fails and the job has attempts left | `onJobFailure`, then `onJobRetry` with the backoff |
| The handler fails permanently, or spends its last attempt | `onJobFailure`, then `onJobFailedAndMovedToDLQ` |
| The handler cancels a tree or a branch | `onJobCancelled` |
| The job went away mid-flight | `onJobUnavailable` |
| The handler nacks the job | none |

A cancellation calls `onJobCancelled`. A nack does not call a hook.

Each `onJobFailure` call is followed by `onJobRetry` or
`onJobFailedAndMovedToDLQ`. Measure failure duration in `onJobFailure`. Count
failures in one of the two outcome hooks to prevent duplicate counts.

If a failure update finds no row, Arbiter calls `onJobUnavailable`. Another
worker owns the job and reports its outcome.

Each successful heartbeat extension for a running job calls `onJobHeartbeat`.
Reclaimed or cancelled jobs do not call this hook.

## Composing hooks

`ObservabilityHooks` is a `Monoid`. The `<>` operator runs the left callback
before the right callback at each point. Arbiter runs the right callback if the
left callback throws an exception. `withHooks` combines a record with the hooks
in an existing configuration:

```haskell
let instrumented = Worker.withHooks (myHooks <>) config
```

`arbiter-otel` uses this method to add instrumentation. Its metrics and
application hooks can use one configuration. See
[OpenTelemetry](../opentelemetry.md).

## What a hook cannot do

Arbiter discards a hook return value. It catches hook exceptions, logs them at
`Warning`, and continues the worker.

`onJobSuccess` can run for a job that Arbiter processes again. Put effects that
must occur one time in the same transaction as the ack. See
[Batched Handlers](batched-handlers.md) describes.

Reaper activity reports through `onMaintenance` on `WorkerConfig`, not through
the hooks record.

See the [`ObservabilityHooks` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Types.html#t:ObservabilityHooks) for each callback's arguments.

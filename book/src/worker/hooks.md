# Observability Hooks

`ObservabilityHooks` is a record of callbacks, one for each point in a job's
lifecycle. Override the fields you need on `defaultObservabilityHooks`, which
does nothing, then put the record on the pool's config.

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

A hook runs in the pool's own monad, so it can read the database as well as
write to a metrics client.

## Which hook fires

Every claimed job fires `onJobClaimed`, then reaches one of these outcomes:

| Outcome | Hooks |
| --- | --- |
| The handler returns | `onJobSuccess` |
| The handler fails and the job has attempts left | `onJobFailure`, then `onJobRetry` with the backoff |
| The handler fails permanently, or spends its last attempt | `onJobFailure`, then `onJobFailedAndMovedToDLQ` |
| The handler cancels a tree or a branch | `onJobCancelled` |
| The job went away mid-flight | `onJobUnavailable` |
| The handler nacks the job | none |

A cancel routes to `onJobCancelled` alone. A nack fires nothing.

`onJobFailure` always pairs with `onJobRetry` or `onJobFailedAndMovedToDLQ`.
Time failures on `onJobFailure` and count them on the other two. Counting both
records each failure twice.

When a failure's write finds no row, the job reports through
`onJobUnavailable`. Another worker holds it by then, and that worker's outcome
is the one that counts.

`onJobHeartbeat` fires for each running job on every heartbeat that extended
its visibility. A job the heartbeat finds reclaimed or cancelled does not fire
it.

## Composing hooks

`ObservabilityHooks` is a `Monoid`. Joining two records with `<>` runs both at
each point, left before right, and the right one runs however the left ended.
`withHooks` layers a record onto whatever the config already carries:

```haskell
let instrumented = Worker.withHooks (myHooks <>) config
```

`arbiter-otel` instruments a pool this way, so its metrics and your own hooks
coexist on one config. See [OpenTelemetry](../opentelemetry.md).

## What a hook cannot do

A hook only reports. Its return value is discarded, and an exception it throws
is caught and logged at `Warning`. The worker continues.

`onJobSuccess` can fire for a job that is later reprocessed. Effects that must
happen exactly once belong next to the ack, as
[Batched Handlers](batched-handlers.md) describes.

Reaper activity reports through `onMaintenance` on `WorkerConfig`, not through
the hooks record.

See the [`ObservabilityHooks` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-Job-Types.html#t:ObservabilityHooks) for each callback's arguments.

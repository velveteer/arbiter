# OpenTelemetry

Spans and W3C trace-context propagation are built in. `arbiter-otel` adds
metrics, gauges, and OTel log records over OTLP:

```haskell
import Arbiter.Otel qualified as Otel

main :: IO ()
main = do
  env <- createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"

  runSimpleDb env $
    Otel.runWorkerPools [namedWorkerPool emailCfg, namedWorkerPool imageCfg]
```

`Otel.runWorkerPools` replaces `runWorkerPools` with the same arguments. It
installs the SDK, instruments the pools, and starts the gauges. Call it once
per process.

Standard `OTEL_*` variables configure the SDK. `OTEL_SDK_DISABLED=true` turns
it off. Logs go to OTel and to the configured log destination, with the job
trace, id, queue, and attempt.

To own the telemetry handle, open it with a bracket such as `withTelemetry`,
or with `withExternalTelemetry` for providers your application owns. Pass it
and the gauge loop's base log config to `runWorkerPoolsWith`. For another pool
runner, apply `instrumentPools` to the pools and wrap the runner in
`withGauges`.

## Traces

An enqueue records the current span. A claim starts a `process <queue>`
consumer span linked to it, across processes and for jobs a handler enqueues.
A REST API enqueue joins the request trace under `newOpenTelemetryWaiMiddleware`
(`hs-opentelemetry-instrumentation-wai`).

Both spans carry the [payload kind](features/kinds.md) as `arbiter.kind`.

`Arbiter.Core.Trace` annotates a job's span, opens child spans, and wraps an
enqueue made outside a handler.

## Metrics

`arbiter-otel` reports job activity, queue depth, admission policies, reaper
activity, Arbiter table health, and PostgreSQL health.
[`Arbiter.Otel.MetricNames`](https://arbiterq.dev/arbiter-otel/Arbiter-Otel-MetricNames.html)
lists each instrument with its type, unit, and attributes.

Admission metrics are keyed by `policy`. The admitted counter and the keys and
limit gauges also carry `policy_kind`, `rate_limit` or `concurrency`. The
in-flight, busiest-key, and tokens gauges carry `policy` only.

`kind` on `arbiter.queue.depth_by_kind` and the job counters is one of the payload's `kindsFor`
labels, or absent.

PostgreSQL health outside the Arbiter role needs `pg_read_all_stats`. One
replica scans per interval and the rest export that reading.

| Aggregate across replicas with | Metrics |
| --- | --- |
| `max` | queue depth, PostgreSQL health |
| `sum` | per-process counters and latencies |

Queue and PostgreSQL gauges scan once per `OTEL_METRIC_EXPORT_INTERVAL`
(default 60s).

## Prometheus

Metrics leave over OTLP. Scrape an OTel collector.
`OTEL_METRICS_EXPORTER=prometheus` turns metrics off.

## Local Stack

`arbiter-demo/run-local.sh` runs the demo against Grafana's
[LGTM stack](https://github.com/grafana/docker-otel-lgtm) at
http://localhost:8000, dashboard at /dash. The
[live demo](https://demo.arbiterq.dev/) runs the same stack. The dashboard and
alert rules expect metrics over OTLP through a collector.

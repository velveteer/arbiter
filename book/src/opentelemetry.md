# OpenTelemetry

Arbiter includes spans and W3C trace-context propagation. These features do not
require configuration. The `arbiter-otel` package adds metrics, gauges, and
OTel log records over OTLP:

```haskell
import Arbiter.Otel qualified as Otel

main :: IO ()
main = do
  env <- createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"

  runSimpleDb env $
    Otel.runWorkerPools [namedWorkerPool emailCfg, namedWorkerPool imageCfg]
```

Use `Otel.runWorkerPools` in place of `runWorkerPools`. The arguments are the
same. It installs the SDK, instruments the pools, and starts the gauges. Call it
one time in each process.

Standard `OTEL_*` variables configure exporters, endpoints, and intervals. You
can set `OTEL_SDK_DISABLED=true`. Arbiter sends logs to OTel and to the configured
log destination. Each log contains the job trace, ID, queue, and attempt.

Use `runWorkerPoolsWith` and a bracket from `Arbiter.Otel` when you manage the
telemetry handle, install a separate SDK, or start pools by another method. The
`With` functions also accept the base log configuration for the gauge loop.

## Traces

Each enqueue records the current span. Each claim starts a `process <queue>`
consumer span and links it to the enqueue span. The link works across processes
and for jobs that a handler enqueues. A REST API enqueue joins the request trace
when the server uses `newOpenTelemetryWaiMiddleware`
(`hs-opentelemetry-instrumentation-wai`).

`Arbiter.Core.Trace` has the helpers for annotating a job's span, opening child
spans, and wrapping an enqueue made outside a handler.

## Metrics

`arbiter-otel` reports job activity, queue depth, admission policies, reaper
activity, Arbiter table health, and PostgreSQL health. The
[`Arbiter.Otel.MetricNames`](https://arbiterq.dev/arbiter-otel/Arbiter-Otel-MetricNames.html)
module defines the name and unit of each instrument.

Admission metrics use the policy as the key. They do not use admission keys.

Grant `pg_read_all_stats` to collect PostgreSQL health data outside the Arbiter
role. One replica scans during each interval. The other replicas export that
reading. Across replicas, use `max` for queue depth and PostgreSQL health. Use
`sum` for per-process counters and latencies.

Queue and Postgres gauges are scanned once per `OTEL_METRIC_EXPORT_INTERVAL`
(default 60s).

## Prometheus

Arbiter sends metrics over OTLP. Configure Prometheus to scrape an OTel
collector. Arbiter does not support `OTEL_METRICS_EXPORTER=prometheus`. This
setting disables metrics.

## Local stack

`arbiter-demo/run-local.sh` runs this repository's demo against Grafana's
[LGTM stack](https://github.com/grafana/docker-otel-lgtm) at
http://localhost:8000, with the dashboard at /dash. The
[live demo](https://demo.arbiterq.dev/) runs the same stack.

The dashboard and the alert rules assume metrics arrive over OTLP through a
collector.

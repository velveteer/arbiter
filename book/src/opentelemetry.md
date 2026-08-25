# OpenTelemetry

Spans and W3C trace-context propagation are built in and need no setup.
`arbiter-otel` adds metrics, gauges, and OTel log records over OTLP:

```haskell
import Arbiter.Otel qualified as Otel

main :: IO ()
main = do
  env <- createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"

  runSimpleDb env $
    Otel.runWorkerPools [namedWorkerPool emailCfg, namedWorkerPool imageCfg]
```

`Otel.runWorkerPools` swaps in for `runWorkerPools`, same arguments: it
installs the SDK, instruments the pools and runs the gauges. Call it once per
process, not per pool.

Exporters, endpoints and intervals come from the standard `OTEL_*` variables,
including `OTEL_SDK_DISABLED=true`. Logs are emitted alongside your configured
destination, carrying the job's trace, id, queue, and attempt.

To hold the telemetry handle yourself, install your own SDK, or drive the pools
some other way, use `runWorkerPoolsWith` with one of the brackets in
`Arbiter.Otel`. The `With` variants also take the gauge loop's base log config.

## Traces

Every enqueue stamps the ambient span and every claim opens a `process <queue>`
consumer span linked back to it, across processes and across jobs a handler
enqueues. An enqueue over the REST API joins the request's trace, given server
spans from `newOpenTelemetryWaiMiddleware`
(`hs-opentelemetry-instrumentation-wai`).

`Arbiter.Core.Trace` has the helpers for annotating a job's span, opening child
spans, and wrapping an enqueue made outside a handler.

## Metrics

`arbiter-otel` reports on jobs, queue depth, admission policies, the reaper, and the health of both arbiter's own tables and the database around them. Every instrument, with its name and unit, is defined in [`Arbiter.Otel.MetricNames`](https://arbiterq.dev/arbiter-otel/Arbiter-Otel-MetricNames.html).

Admission metrics are keyed by policy, never by the admission key.

Grant `pg_read_all_stats` for Postgres health beyond arbiter's own role. One
replica scans per interval and the rest export its reading, so aggregate queue
depth and Postgres health across replicas with `max`, and the per-process
counters and latencies with `sum`.

Queue and Postgres gauges are scanned once per `OTEL_METRIC_EXPORT_INTERVAL`
(default 60s).

## Prometheus

Metrics are pushed over OTLP, so scrape a collector rather than the process.
`OTEL_METRICS_EXPORTER=prometheus` is unsupported and leaves metrics off.

## Local stack

`arbiter-demo/run-local.sh` runs this repository's demo against Grafana's
[LGTM stack](https://github.com/grafana/docker-otel-lgtm) at
http://localhost:8000, with the dashboard at /dash. The
[live demo](https://demo.arbiterq.dev/) runs the same stack.

The dashboard and the alert rules assume metrics arrive over OTLP through a
collector.

#!/usr/bin/env python3
# A dummy producer for the arbiter CLI stack. It waits for the REST API, then
# enqueues jobs round-robin across the configured queues via
# POST /api/v1/<queue>/jobs. Arbiter's webhook workers deliver them to handler.py.
#
# Each enqueue is wrapped in an OpenTelemetry "publish" span, and the current
# trace context is injected into the request headers, so arbiter's server span
# nests under it and the whole thing shows as one trace in Tempo. A fraction of
# jobs carry a "fail" marker so the handler exercises retry and DLQ paths.
import itertools
import json
import os
import time
import urllib.error
import urllib.request

DEFAULT_QUEUES = "emails,reports,notifications"
BASE = os.environ.get("ARBITER_URL", "http://arbiter:8080").rstrip("/")
QUEUES = [q.strip() for q in os.environ.get("QUEUES", DEFAULT_QUEUES).split(",") if q.strip()]
if not QUEUES:  # QUEUES set but empty/whitespace/comma-only
    QUEUES = [q.strip() for q in DEFAULT_QUEUES.split(",")]
INTERVAL = float(os.environ.get("INTERVAL_SECS", "2"))

# OpenTelemetry is provided by apps.Dockerfile. Absent it, run without spans.
try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.propagate import inject
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    _provider = TracerProvider(resource=Resource.create({}))  # service.name from OTEL_SERVICE_NAME
    _provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(_provider)
    TRACER = trace.get_tracer("producer")
    SPAN_KIND = trace.SpanKind.PRODUCER
except Exception as e:  # SDK missing or misconfigured
    print(f"[producer] otel disabled: {e}", flush=True)
    TRACER = None

    def inject(carrier):
        return None


def _post(url, data, headers):
    req = urllib.request.Request(url, data=data, method="POST", headers=headers)
    with urllib.request.urlopen(req, timeout=10) as resp:
        return resp.status


def enqueue(queue, payload):
    url = f"{BASE}/api/v1/{queue}/jobs"
    data = json.dumps({"payload": payload}).encode()
    headers = {"Content-Type": "application/json"}
    if TRACER is None:
        return _post(url, data, headers)
    with TRACER.start_as_current_span(f"publish {queue}", kind=SPAN_KIND) as span:
        span.set_attribute("messaging.system", "arbiter")
        span.set_attribute("messaging.operation", "publish")
        span.set_attribute("messaging.destination.name", queue)
        inject(headers)  # adds traceparent for arbiter to continue
        return _post(url, data, headers)


def wait_for_arbiter():
    while True:
        try:
            urllib.request.urlopen(f"{BASE}/api/v1/queues", timeout=5).read()
            print("[producer] arbiter is up", flush=True)
            return
        except Exception as e:  # connection refused, DNS, 5xx during startup
            print(f"[producer] waiting for arbiter: {e}", flush=True)
            time.sleep(2)


def payload_for(queue, n):
    # ~1 in 25 permanent-fail, ~1 in 10 transient-fail, else a normal job.
    if n % 25 == 0:
        return {"kind": queue, "n": n, "fail": "permanent"}
    if n % 10 == 0:
        return {"kind": queue, "n": n, "fail": "retry"}
    return {"kind": queue, "n": n, "note": f"{queue} job {n}"}


if __name__ == "__main__":
    wait_for_arbiter()
    for n in itertools.count(1):
        queue = QUEUES[n % len(QUEUES)]
        payload = payload_for(queue, n)
        marker = payload.get("fail", "ok")
        try:
            status = enqueue(queue, payload)
            print(f"[producer] -> {queue} #{n} ({marker}) HTTP {status}", flush=True)
        except urllib.error.HTTPError as e:
            print(f"[producer] {queue} #{n} rejected: HTTP {e.code} {e.read()[:200]!r}", flush=True)
        except Exception as e:
            print(f"[producer] {queue} #{n} error: {e}", flush=True)
        time.sleep(INTERVAL)

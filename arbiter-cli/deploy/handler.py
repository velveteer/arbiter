#!/usr/bin/env python3
# A dummy webhook handler for the arbiter CLI stack. Arbiter's per-queue webhook
# worker POSTs one job envelope here, and the HTTP status decides the job's fate:
# 2xx acks, 4xx is permanent (straight to the DLQ), 408/429/5xx is retried.
#
# Arbiter forwards the delivery's W3C traceparent, so each request continues that
# trace as an OpenTelemetry "consume" span; the delivery then shows end to end in
# Tempo (publish -> enqueue -> process -> consume).
#
# When WEBHOOK_SECRET is set it verifies the HMAC-SHA256 signature arbiter sends:
# X-Arbiter-Signature: v1=<hex> over "<X-Arbiter-Timestamp>.<body>". A missing,
# stale, or wrong signature returns 401, so the job is DLQ'd as an auth failure.
#
# The payload's optional "fail" field plus the envelope's "attempts" drive the
# three outcomes:
#   payload.fail == "permanent"  -> 400, the job goes to the DLQ immediately
#   payload.fail == "retry"      -> 500 until attempts >= 2, then 200 (retry, then ack)
#   otherwise                    -> 200 (ack)
import hashlib
import hmac
import json
import os
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

PORT = int(os.environ.get("PORT", "9000"))
SECRET = os.environ.get("WEBHOOK_SECRET", "").encode()
MAX_SKEW = int(os.environ.get("MAX_SKEW_SECS", "300"))

# OpenTelemetry is provided by apps.Dockerfile. Absent it, run without spans.
try:
    from opentelemetry import trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
    from opentelemetry.propagate import extract
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    _provider = TracerProvider(resource=Resource.create({}))  # service.name from OTEL_SERVICE_NAME
    _provider.add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
    trace.set_tracer_provider(_provider)
    TRACER = trace.get_tracer("webhook-handler")
    SPAN_KIND = trace.SpanKind.CONSUMER
except Exception as e:  # SDK missing or misconfigured
    print(f"[handler] otel disabled: {e}", flush=True)
    TRACER = None

    def extract(carrier):
        return None


class Handler(BaseHTTPRequestHandler):
    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        if TRACER is None:
            self.handle_delivery(raw, None)
            return
        ctx = extract({k.lower(): v for k, v in self.headers.items()})
        with TRACER.start_as_current_span("consume", context=ctx, kind=SPAN_KIND) as span:
            self.handle_delivery(raw, span)

    def handle_delivery(self, raw, span):
        ok, reason = self.verify(raw)
        if not ok:
            self.finish_span(span, None, None, None, "auth-fail", 401)
            print(f"[handler] rejected: {reason}", flush=True)
            self.reply(401, reason)
            return
        try:
            env = json.loads(raw)
        except json.JSONDecodeError:
            env = None
        if not isinstance(env, dict):
            self.finish_span(span, None, None, None, "bad-json", 400)
            self.reply(400, "invalid json")
            return
        payload = env.get("payload")
        fail = payload.get("fail") if isinstance(payload, dict) else None
        attempts = env.get("attempts") or 0
        job_id = env.get("job_id")
        queue = env.get("queue")
        if fail == "permanent":
            self.done(span, job_id, queue, attempts, "permanent -> DLQ", 400, "permanent failure")
        elif fail == "retry" and attempts < 2:
            self.done(span, job_id, queue, attempts, "transient -> retry", 500, "temporary failure")
        else:
            self.done(span, job_id, queue, attempts, "ack", 200, "ok")

    def done(self, span, job_id, queue, attempts, outcome, code, msg):
        self.finish_span(span, job_id, queue, attempts, outcome, code)
        print(f"[handler] queue={queue} job={job_id} attempt={attempts} {outcome} ({code})", flush=True)
        self.reply(code, msg)

    def finish_span(self, span, job_id, queue, attempts, outcome, code):
        if span is None:
            return
        span.set_attribute("messaging.system", "arbiter")
        if queue is not None:
            span.set_attribute("messaging.destination.name", queue)
        if job_id is not None:
            span.set_attribute("messaging.message.id", str(job_id))
        if attempts is not None:
            span.set_attribute("arbiter.attempt", attempts)
        span.set_attribute("arbiter.outcome", outcome)
        span.set_attribute("http.response.status_code", code)

    def verify(self, raw):
        if not SECRET:
            return True, ""
        ts = self.headers.get("X-Arbiter-Timestamp")
        sig = self.headers.get("X-Arbiter-Signature")
        if not ts or not sig:
            return False, "missing signature headers"
        try:
            skew = abs(time.time() - int(ts))
        except ValueError:
            return False, "malformed timestamp"
        if skew > MAX_SKEW:
            return False, f"stale timestamp (skew {skew:.0f}s)"
        expected = "v1=" + hmac.new(SECRET, ts.encode() + b"." + raw, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected.encode(), sig.encode()):
            return False, "signature mismatch"
        return True, ""

    def reply(self, code, msg):
        body = msg.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/plain")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, *args):
        pass  # silence the default per-request access log


if __name__ == "__main__":
    signing = "on" if SECRET else "off"
    tracing = "on" if TRACER else "off"
    print(f"[handler] listening on :{PORT} (signatures {signing}, tracing {tracing})", flush=True)
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()

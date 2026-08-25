# Liveness Probes

The heartbeat loop touches `livenessFile` on every worker heartbeat, so a probe
can check the pool without a database round trip. The default path is
`arbiter-worker-<workerId>` in the system temporary directory.

Fail the probe on a stale file to catch a pool whose process is up but whose
workers have stopped:

```yaml
livenessProbe:
  exec:
    command: ["sh", "-c", "find $TMPDIR/arbiter-worker-* -mmin -5"]
  initialDelaySeconds: 30
  periodSeconds: 60
```

The REST API carries the other two checks: `GET health` is a readiness check
that touches the database, and `GET health/live` never does. See
[REST API and Admin UI](../rest-api.md).

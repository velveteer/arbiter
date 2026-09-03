# Liveness Probes

The heartbeat loop updates `livenessFile` at each worker heartbeat. A probe can
check this file without a database query. The default file is
`arbiter-worker-<workerId>` in the system temporary directory.

Configure the probe to fail when the file is stale:

```yaml
livenessProbe:
  exec:
    command: ["sh", "-c", "find ${TMPDIR:-/tmp}/arbiter-worker-* -mmin -5 | grep -q ."]
  initialDelaySeconds: 30
  periodSeconds: 60
```

`find` exits with status 0 when no file matches `-mmin`. Therefore, a command
that uses only `find` passes when the heartbeat is stale. The `grep -q .`
command makes the probe fail when no current heartbeat file exists. The pool
removes the file after a normal shutdown.

The REST API provides two other checks. `GET health` is a readiness check that
queries the database. `GET health/live` is a liveness check that does not query
the database. See
[REST API and Admin UI](../rest-api.md).

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

`find` exits 0 whether or not anything matched `-mmin`, so a bare `find` passes
on a stale heartbeat and only fails once the file is gone -- which is the one
case that does not want a restart, since the pool removes the file after a clean
drain. Piping to `grep -q .` makes the probe fail when nothing is fresh, and
also when the glob matched no file at all.

The REST API provides two other checks. `GET health` is a readiness check that
queries the database. `GET health/live` is a liveness check that does not query
the database. See
[REST API and Admin UI](../rest-api.md).

# Writing a Backend

A backend is a `MonadArbiter` instance. Applications that use
[arbiter-orville](orville.md) must define this instance. Use the same interface
to implement another adapter.

The class specifies a registry, handler type, and these database operations:

- `RegistryOf` specifies the queue registry for the monad.
- `Handler` specifies the handler type. It can include a connection argument if
  the database library supplies one.
- `getSchema` returns the Arbiter schema.
- `executeQuery` and `executeStatement` run a `Query`. The value contains its
  SQL text, parameters, and decoder.
- `withDbTransaction` starts a transaction or savepoint. A nested call must
  start a savepoint. This lets an ack participate in the caller's transaction.
  See [Worker Configuration](../worker/configuration.md).
- `runHandlerWithConnection` checks out a connection and runs a handler.
- `getListener` returns the shared `LISTEN/NOTIFY` listener. Return `Nothing`
  for polling mode. See [Wakeups](../worker/wakeups.md).

`executeQueryPrepared` is optional and uses `executeQuery` by default. Override
it if the backend can prepare a statement one time for each connection and
reuse the plan. The claim operation uses this method. See the performance data
in [Backend Integration](index.md).

See the [`MonadArbiter` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-MonadArbiter.html) for each method's signature.

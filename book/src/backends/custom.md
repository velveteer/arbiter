# Writing a Backend

A backend is a `MonadArbiter` instance. [arbiter-orville](orville.md) needs one
of these from you, and writing your own adapter is the same job.

The class asks for a registry, a handler shape, and the database primitives
arbiter runs everything else through:

- `RegistryOf` - the queue registry this monad serves.
- `Handler` - the shape your handlers take. Give it whatever your database
  library hands a handler, or just the job when it manages connections itself.
- `getSchema` - where the arbiter tables live.
- `executeQuery` and `executeStatement` - run a `Query`, which carries its own
  text, parameters, and decoder together.
- `withDbTransaction` - nesting must produce savepoints, not a second
  transaction. Arbiter relies on this to enlist an ack in a caller's
  transaction. See [Worker Configuration](../worker/configuration.md).
- `runHandlerWithConnection` - take a connection from the pool and run a
  handler on it.
- `getListener` - the shared `LISTEN/NOTIFY` listener, or `Nothing` for
  poll-only. See [Wakeups](../worker/wakeups.md).

`executeQueryPrepared` is optional and defaults to `executeQuery`. Override it
if the backend can prepare a statement once per connection and reuse the plan.
The claim runs through it, and the [benchmarks](index.md) show what that buys.

See the [`MonadArbiter` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-MonadArbiter.html) for each method's signature.

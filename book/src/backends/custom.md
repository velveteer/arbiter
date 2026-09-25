# Writing a Backend

A backend is a `MonadArbiter` instance. An [arbiter-orville](orville.md)
application can define one on its own monad in place of `OrvilleDb`.

| Member | |
| --- | --- |
| `RegistryOf` | the queue registry |
| `Handler` | the handler type, with a connection argument if the library has one |
| `getSchema` | the Arbiter schema |
| `executeQuery`, `executeStatement` | run a `Query`: SQL with `?` placeholders, the same with `$n` for libpq, parameters, decoder |
| `withDbTransaction` | transaction, or a savepoint when nested. See [worker configuration](../worker/configuration.md) |
| `runHandlerWithConnection` | check out a connection and run a handler |
| `getListener` | shared `LISTEN/NOTIFY` listener, or `Nothing` for polling. See [wakeups](../worker/wakeups.md) |
| `executeQueryPrepared` | optional. Defaults to `executeQuery`. Claims use it. Prepared once per connection when overridden. See [benchmarks](index.md). |

Method signatures: [`MonadArbiter` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Core-MonadArbiter.html).

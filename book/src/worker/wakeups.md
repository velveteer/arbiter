# Wakeups (LISTEN/NOTIFY)

PostgreSQL `LISTEN/NOTIFY` sends immediate notifications for new jobs, pause
and resume operations, force-cancel operations, and manual cron runs. The
`MonadArbiter` instance supplies a shared listener hub to the workers. This
applies to `SimpleDb`, `HasqlDb`, and custom monads.

If there is no listener, workers check for jobs at each `pollInterval`. Control
operations use these intervals:

| Path | Without a listener |
| --- | --- |
| Pause and resume | Reconciled at the worker heartbeat (`workerHeartbeatInterval`) |
| Cron run-now | Waits for the scheduler's next tick |
| Force-cancel | Interrupts the handler at the next job heartbeat (`jobHeartbeatInterval`) |

The environment does not open a listener connection until a worker pool starts.
A producer process that does not start a pool does not open this connection.
While workers run, the listener uses one pool connection.

On the provided backends, `useDedicatedListener` creates a separate listener
connection:

```haskell
env <- ArbS.useDedicatedListener connStr =<< ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
```

`disableListener` disables the listener and uses polling mode:

```haskell
env <- ArbS.disableListener <$> ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
```

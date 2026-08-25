# Wakeups (LISTEN/NOTIFY)

PostgreSQL's `LISTEN/NOTIFY` wakes workers immediately for new jobs,
pause/resume, force-cancel, and cron run-now, instead of making them wait for
the next poll. Workers use the shared listener hub that the `MonadArbiter`
instance supplies, whether that is `SimpleDb`, `HasqlDb`, or your own monad.

Without a listener, jobs are still claimed and processed reliably, on the
`pollInterval` cadence rather than instantly. The control paths fall back to
slower cadences too:

| Path | Without a listener |
| --- | --- |
| Pause and resume | Reconciled at the worker heartbeat (`workerHeartbeatInterval`) |
| Cron run-now | Waits for the scheduler's next tick |
| Force-cancel | Interrupts the handler at the next job heartbeat (`jobHeartbeatInterval`) |

The listener is lazy and opens no connection until a worker pool starts on the
env, so a process that only enqueues jobs never starts one. Once a pool does
start, the listener holds one pool connection for as long as workers run.

On the provided backends, `useDedicatedListener` gives the listener its own
connection instead:

```haskell
env <- ArbS.useDedicatedListener connStr =<< ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
```

`disableListener` drops it entirely and runs in poll-only mode:

```haskell
env <- ArbS.disableListener <$> ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
```

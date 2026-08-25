# Logging

A pool writes structured JSON logs. `logConfig` sets where they go and at what
level, and `defaultLogConfig` writes Info and above to stdout.

The destinations are stdout, stderr, a fast-logger `LoggerSet`, and a callback
of your own. The callback receives the level, the message, and the structured
context as `[Pair]`, which is how you fold arbiter's logs into your own logging
stack. `silentLogConfig` turns logging off.

Job context is attached for you, so a handler's own logs carry the same job
fields as the pool's.

See the [`Arbiter.Worker.Logger` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Logger.html) for `LogConfig` and every destination.

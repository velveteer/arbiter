# Logging

A pool writes structured JSON logs. `logConfig` sets the destination and level.
`defaultLogConfig` writes `Info` and higher levels to stdout.

Available destinations are stdout, stderr, a fast-logger `LoggerSet`, and a
custom callback. The callback receives the level, message, and structured
context as `[Pair]`. Use it to send Arbiter logs to an application logging
system. `silentLogConfig` disables logging.

Arbiter adds job context to handler logs and pool logs.

See the [`Arbiter.Worker.Logger` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Logger.html) for `LogConfig` and every destination.

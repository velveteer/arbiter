# Logging

A pool writes structured JSON logs. `logConfig` sets the destination and level.
`defaultLogConfig` writes `Info` and above to stdout. `silentLogConfig` writes
nothing.

| Destination | |
| --- | --- |
| stdout, stderr | |
| fast-logger `LoggerSet` | |
| callback | receives the level, message, and context as `[Pair]` |

Handler and pool logs carry the job context.

`LogConfig` and destinations: [`Arbiter.Worker.Logger` Haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Logger.html).

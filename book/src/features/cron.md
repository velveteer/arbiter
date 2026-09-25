# Cron Jobs

```haskell
import Arbiter.Worker.Cron qualified as Cron

let Right healthCheck = Cron.cronJob
      "health-check"        -- unique name
      "*/5 * * * *"         -- every 5 minutes (UTC)
      Cron.SkipOverlap      -- skip tick if previous job is still pending/running
      (\_kind tick -> Arb.defaultJob (RunHealthCheck tick))

    -- with backfill: catch up on missed ticks after downtime or scheduler delays
    Right nightlyReport = Cron.cronJob
      "nightly-report"
      "0 3 * * *"           -- 03:00 UTC daily
      Cron.AllowOverlap $ \kind tick -> -- each tick produces its own job
        let jobPriority = case kind of
              Cron.Replay -> 10
              Cron.Live -> 0
         in Arb.defaultJob (GenerateReport tick) & Arb.setPriority jobPriority
    nightlyWithBackfill = nightlyReport {Cron.backfill = Cron.Backfill 86400}

    -- in a specific timezone (validated at construction)
    Right marketOpen = Cron.cronJobInTimezone
      "market-open"
      "America/New_York"    -- IANA tz name
      "30 9 * * 1-5"        -- 09:30 local, Mon-Fri (DST-aware)
      Cron.SkipOverlap
      (\_kind tick -> Arb.defaultJob (OpeningBell tick))

config <- Worker.transactionalWorkerConfig 4 processScheduled
let configWithCron =
      config {Worker.cronJobs = [healthCheck, nightlyWithBackfill, marketOpen]}
```

| Policy | Behavior |
|--------|----------|
| `SkipOverlap` | At most one pending or running job per schedule. |
| `AllowOverlap` | One job per tick. Ticks can overlap. |

The builder receives a `TickKind` (`Live` for the current minute, `Replay` for
a catch-up tick) and the tick time.

A schedule enqueues one job per tick on its pool's queue. Tree jobs
[spawn children](job-trees.md#spawning-children-at-runtime) at runtime.

**Time zones.** Expressions are UTC. `cronJobInTimezone` takes an
[IANA name](https://www.iana.org/time-zones). `30 2 * * *` skips a spring
transition day with no 02:30. `30 1 * * *` runs once on a fall transition day
with two 01:30s. A `*` minute or hour field (`*/5 * * * *`, `0 * * * *`) runs
through both 01:00 hours.

**Backfill.** `Backfill n` replays ticks missed in the last `n` seconds after
downtime or a scheduler pause.
If a tick fails to insert, the scheduler logs the failure and continues with
later ticks. A failed tick is skipped once a later tick fires. For a `Backfill` schedule, a failed
newest tick is retried on the next pass.

**Runtime overrides.** The REST API and admin UI set a schedule's expression,
overlap policy, time zone, and enabled state. An override of `null` restores
the value from code.

Schedule type: [`Arbiter.Worker.Cron` Haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Cron.html).

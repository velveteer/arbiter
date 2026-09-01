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
| `SkipOverlap` | At most one pending/running job per schedule. |
| `AllowOverlap` | One job per tick. Multiple ticks can run concurrently. |

The builder receives a `TickKind` (`Live` for the current minute, `Replay` for
any catch-up tick) and the tick time.

A schedule enqueues jobs on its configured pool. Its builder must return the
payload type for that pool. Configure schedules for other queues on their
respective pools.

**Time zones.** Expressions use UTC by default. Use `cronJobInTimezone` and an
[IANA name](https://www.iana.org/time-zones), such as `America/New_York`, for
local time. A schedule of `30 2 * * *` does not run on a spring transition day
that has no 02:30. A schedule of `30 1 * * *` runs one time on a fall transition
day that has two occurrences of 01:30.

**Backfill.** `BackfillPolicy` replays missed minutes after downtime or a
scheduler pause. The policy duration limits the replay period.

**Runtime overrides.** Use the REST API or admin UI to change a schedule's
expression, overlap policy, time zone, and enabled state. These changes do not
require a deployment. Set an override to `null` to use the value from code.

See the [`Arbiter.Worker.Cron` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Cron.html) for the schedule type.

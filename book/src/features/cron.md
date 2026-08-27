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

A schedule enqueues onto the pool that carries it, so its builder returns that
pool's payload type. Schedules for a different queue belong on that queue's
pool.

**Timezones.** Expressions default to UTC. Use `cronJobInTimezone` with an
[IANA name](https://www.iana.org/time-zones) like `America/New_York` to run in
local time. On the spring-forward day a schedule of `30 2 * * *` does not fire,
because 02:30 does not occur locally. On the fall-back day a schedule of
`30 1 * * *` fires once, although 01:30 occurs twice.

**Backfill.** `BackfillPolicy` replays missed minutes after downtime or a
scheduler pause, bounded by a duration you give it.

**Runtime overrides.** The REST API and admin UI edit a schedule's expression,
overlap, timezone, and enabled state without a redeploy. Set an override to
`null` to fall back to the value in code.

See the [`Arbiter.Worker.Cron` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-Cron.html) for the schedule type.

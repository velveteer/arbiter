# Backoff Strategies

```haskell
config { Worker.backoffStrategy = Worker.exponentialBackoff 2.0 3600 }  -- base^attempts, cap 1h
config { Worker.backoffStrategy = Worker.linearBackoff 30 600 }         -- +30s/attempt, cap 10m
config { Worker.backoffStrategy = Worker.constantBackoff 60 }           -- always 60s
config { Worker.backoffStrategy = Worker.Custom (\n -> fromIntegral n * 15) }

config { Worker.jitter = Worker.FullJitter }   -- random(0, delay)
config { Worker.jitter = Worker.EqualJitter }  -- delay/2 + random(0, delay/2) (default)
config { Worker.jitter = Worker.NoJitter }
```

The first failure is attempt 1. With the default `EqualJitter`,
`exponentialBackoff 2.0` waits 1 to 2 seconds before the first retry.

A nack skips the backoff. The job stays invisible for the rest of its lease.
To change that, call `setVisibilityTimeout` before the nack.

Strategies and jitter modes: [`Arbiter.Worker.BackoffStrategy` Haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-BackoffStrategy.html).

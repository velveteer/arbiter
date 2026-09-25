# Backoff Strategies

```haskell
config { Worker.backoffStrategy = exponentialBackoff 2.0 3600 }  -- base^attempts, cap 1h
config { Worker.backoffStrategy = linearBackoff 30 600 }         -- +30s/attempt, cap 10m
config { Worker.backoffStrategy = constantBackoff 60 }           -- always 60s
config { Worker.backoffStrategy = Custom (\n -> fromIntegral n * 15) }

config { Worker.jitter = FullJitter }   -- random(0, delay)
config { Worker.jitter = EqualJitter }  -- delay/2 + random(0, delay/2) (default)
config { Worker.jitter = NoJitter }
```

The first failure is attempt 1. `exponentialBackoff 2.0` waits two seconds
before the first retry.

A nack skips the backoff. The job stays invisible for the rest of its lease.
Set the visibility timeout before the nack to change that.

Strategies and jitter modes: [`Arbiter.Worker.BackoffStrategy` Haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-BackoffStrategy.html).

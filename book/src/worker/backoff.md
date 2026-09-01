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

The claim operation increments the attempt count before the handler starts.
Arbiter calculates the delay from the new count. Thus, the first failure is
attempt 1, and `exponentialBackoff 2.0` gives a two-second delay before the
first retry.

Jitter changes the calculated delay. The default `EqualJitter` selects a value
between one half and all of the calculated delay. The strategy value is the
maximum delay.

A nack does not use the backoff strategy. The job remains unavailable for the
rest of its lease. Set the job visibility timeout before the nack to control
this period. See
[Error Handling](../features/error-handling.md).

See the [`Arbiter.Worker.BackoffStrategy` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-BackoffStrategy.html) for every strategy and jitter mode.

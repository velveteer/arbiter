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

The claim increments the attempt count before the handler runs, and the delay
is computed from that. A first failure therefore counts as attempt 1, and
`exponentialBackoff 2.0` waits 2 seconds before the first retry.

Jitter applies on top and decides the wait the pool actually takes. Under the
default `EqualJitter` that lands between half the computed delay and all of it.
A strategy sets the ceiling.

A nack skips all of this. Its delay is whatever is left of the claim's lease,
which you control by setting the job's visibility timeout before you nack. See
[Error Handling](../features/error-handling.md).

See the [`Arbiter.Worker.BackoffStrategy` haddocks](https://arbiterq.dev/arbiter-worker/Arbiter-Worker-BackoffStrategy.html) for every strategy and jitter mode.

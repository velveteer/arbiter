# Rate Limiting

Throttle jobs by an arbitrary key. A limit is shared by every queue in a
registry, so one policy can govern a resource no matter which queues touch it.

Give a payload a `HasRateLimit` instance whose `rateLimitFor` selects a policy
per job. The migration inspects the selector statically and seeds every policy
it can reach, so there is no separate policy list to keep in sync.

```haskell
import Arbiter.RateLimit

-- Your own functions on the payload.
isTransactional :: EmailPayload -> Bool
recipientDomain :: EmailPayload -> Text

transactional, bulk :: Policy
transactional = tokenBucket "transactional" 100 1 -- 100/second, burst 100
bulk          = tokenBucket "bulk" 1000 3600      -- 1000/hour, burst 1000

instance HasRateLimit EmailPayload where
  rateLimitFor =
    chooseWhen isTransactional
      (limitBy transactional recipientDomain)
      (limitBy bulk recipientDomain)
```

`tokenBucket prefix n period` reads as "n per period, with bursts up to n". To
bound bursts independently of the sustained rate, build a `Policy` directly and
set `policyMax` (burst) apart from `policyRefill` / `policyInterval` (rate).
Weight expensive jobs with `rateLimitCost`, or top up a bucket manually with
`addRateLimitTokens`.

A job its bucket denies is parked, not polled. It stays invisible until the
bucket can afford it, then competes normally again. The API and admin UI show
throttled counts per policy and let an operator override a policy at runtime.

A fixed window is a manual bucket: declare it with a refill of 0 and reset it
at the boundary from a cron.

```haskell
daily :: Policy
daily =
  Policy
    { policyPrefix = "daily"
    , policyMax = 1000
    , policyRefill = 0
    , policyInterval = 86400
    }

-- In an hourly/daily cron at the window boundary:
resetRateLimitBuckets (policyPrefixOf daily)
```

Bucket state is not durable by default. After a database crash or a failover,
buckets reset to full, so each key can burst to its max once before it settles
back to the sustained rate. If that overshoot is unacceptable, for strict
external quotas or for manual buckets that hold real credit, migrate the schema
with durable buckets. Bucket state then survives a restart, at some throughput
cost:

```haskell
runMigrationsForRegistry (Proxy @AppRegistry) connStr "arbiter"
  defaultMigrationConfig { rateLimitDurability = Durable }
```

Durability is a property of the migrated schema, not the registry type, so the
same registry can back an unlogged staging schema and a durable production one.

> [!IMPORTANT]
> Tokens are spent when a job is **claimed**, not when it finishes. A retry or
> a redelivery (worker crash, visibility timeout) spends again, so size
> policies against claims, not successful runs.
>
> A `rateLimitCost` above the bucket's max clamps to the max: the job drains a
> full bucket and runs, rather than blocking forever. A rate limit bounds
> arrivals over time. To bound how many jobs run at once, use a concurrency
> limit.

## When downstream returns 429

Match the response to the scope of the pushback.

**One key is throttled.** Drain that key's bucket so every job sharing it waits
for the refill, then hand this job back. Take the key off the job instead of
rebuilding it, so the suffix is the one the claim admitted against:

```haskell
import Arbiter.RateLimit (addRateLimitTokens)
import Data.Foldable (traverse_)

sendEmail job cbs = do
  outcome <- liftIO $ postToVendor (Arb.payload job)
  case outcome of
    TooManyRequests retryAfter -> do
      -- Empty the bucket. Any amount at or above its burst works, tokens floor at zero.
      traverse_ (\key -> addRateLimitTokens key (-1000)) (Arb.jobRateLimitKey (Arb.admission job))
      void $ Arb.setVisibilityTimeout retryAfter job
      Worker.nack cbs job
    Sent -> Worker.ack cbs job
```

`Nothing` means the selector puts this job under no policy, leaving no bucket
to drain. A drained bucket refills on its policy's own schedule.

> [!IMPORTANT]
> This is a manual handler on purpose. Under `transactionalWorkerConfig` the
> whole handler runs in one transaction, so throwing to retry rolls back the
> drain along with it. Manual and batched callbacks each commit on their own.

`throwRetryable` carries no delay of its own: the pool's `backoffStrategy` and
jitter decide it from the attempt count. To honor a `Retry-After`, set the
job's visibility to that window and nack it instead.

**The whole policy is too fast.** Override it with a slower version of itself
and clear the override when the vendor recovers. Both helpers take the policy
you declared, so import it instead of restating one:

```haskell
import Arbiter.RateLimit (Policy (..), clearRateLimit, setRateLimit)

import MyApp.Queue.Policies (transactional)

-- half the declared refill, burst and interval unchanged
void $ setRateLimit transactional {policyRefill = policyRefill transactional / 2}

-- back to what the code declares
void $ clearRateLimit transactional
```

`setRateLimit` overrides burst, refill, and interval together. A record update
on the declared policy changes one field and leaves the rest alone.

See the [`Arbiter.RateLimit` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-RateLimit.html) for the selector DSL and the policy type.

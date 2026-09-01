# Rate Limiting

Use an arbitrary key to limit the job rate. A policy applies to all queues in a
registry. Thus, one policy can control a resource that multiple queues use.

Define a `HasRateLimit` instance for the payload. Its `rateLimitFor` function
selects a policy for each job. The migration finds and initializes all policies
that the selector can use. You do not have to maintain a separate policy list.

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

`tokenBucket prefix n period` permits `n` jobs in each period and a burst of up
to `n` jobs. To configure the burst independently, construct a `Policy`. Set
`policyMax` for the burst. Set `policyRefill` and `policyInterval` for the
sustained rate. Use `rateLimitCost` to assign a higher cost to a job. Use
`addRateLimitTokens` to add tokens manually.

When a bucket denies a job, Arbiter makes the job invisible until sufficient
tokens are available. Arbiter does not poll the denied job. The API and admin
UI show the number of throttled jobs for each policy. An operator can also
change a policy at run time.

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

Bucket state is not durable by default. After a database crash or failover,
each bucket resets to full. Each key can then use one maximum burst before the
sustained rate applies. Use durable buckets for strict external quotas or
manual buckets that represent credit. Durable bucket state persists across a
restart, but can reduce throughput:

```haskell
runMigrationsForRegistry (Proxy @AppRegistry) connStr "arbiter"
  defaultMigrationConfig { rateLimitDurability = Durable }
```

Durability is a property of the migrated schema. It is not a property of the
registry type. The same registry can use an unlogged staging schema and a
durable production schema.

> [!IMPORTANT]
> A job uses tokens when Arbiter **claims** it. Retries and redeliveries use
> tokens again. Configure policies for the claim rate.
>
> Arbiter limits a `rateLimitCost` to the bucket maximum. A job with a higher
> cost empties a full bucket and can run. A rate limit controls arrivals over
> time. Use a concurrency limit to control the number of simultaneous jobs.

## When downstream returns 429

Select the response based on the scope of the limit.

**One key is throttled.** Empty the bucket for that key. Jobs with the same key
then wait for a refill. Nack the current job. Read the key from the job to use
the suffix that the claim operation used:

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

`Nothing` means that the selector did not assign a policy to the job. There is
no bucket to empty. An empty bucket refills according to its policy.

> [!IMPORTANT]
> This example uses a manual handler. With `transactionalWorkerConfig`, the
> bucket update and handler run in one transaction. A retry rolls back the
> bucket update. Manual and batched callbacks commit independently.

`throwRetryable` does not specify a delay. The pool calculates the delay from
the attempt count, `backoffStrategy`, and jitter. To use a `Retry-After` value,
set the job visibility period and nack the job.

**The complete policy is too fast.** Set a lower override and clear it when the
vendor recovers. Both functions accept the declared policy. Import that policy
to prevent a duplicate declaration:

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

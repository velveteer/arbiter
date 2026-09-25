# Rate Limiting

A policy limits the claim rate per key across every queue in the registry.
`HasRateLimit` selects a policy and key for each job. The migration creates
every policy the selector can return.

```haskell
import Arbiter.RateLimit

-- Application functions on the payload.
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

`tokenBucket prefix n period` admits `n` jobs per `period` seconds with a burst
of `n`. For a different burst, build a `Policy`:

| Field | Meaning |
| --- | --- |
| `policyMax` | burst |
| `policyRefill`, `policyInterval` | tokens added per interval |

`rateLimitCost` sets a job's cost above 1. `addRateLimitTokens` adjusts a
bucket's tokens.

A denied job is invisible until its bucket has enough tokens. The API and admin
UI show the throttled count per policy and accept policy changes at run time.

A fixed window is a bucket with refill 0, reset from a cron:

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

## Durability

Buckets are unlogged by default. After a crash or failover every bucket is
full. Durable buckets survive a restart and cost throughput:

```haskell
runMigrationsForRegistry (Proxy @AppRegistry) connStr "arbiter"
  defaultMigrationConfig { rateLimitDurability = Durable }
```

Durability is set per migrated schema.

> [!IMPORTANT]
> Tokens are spent at claim. Retries and redeliveries spend tokens again.
>
> A cost above `policyMax` is clamped to `policyMax`.

## HTTP 429 Responses

**One key is throttled.** Empty its bucket, set the visibility timeout to
`Retry-After`, and nack:

```haskell
import Arbiter.RateLimit (addRateLimitTokens)
import Data.Foldable (traverse_)

sendEmail job cbs = do
  outcome <- liftIO $ postToVendor (Arb.payload job)
  case outcome of
    TooManyRequests retryAfter -> do
      -- Empty the bucket. Any amount at or above the burst works. Tokens stop at zero.
      traverse_ (\key -> addRateLimitTokens key (-1000)) (Arb.jobRateLimitKey (Arb.payloadKeys job))
      void $ Arb.setVisibilityTimeout retryAfter job
      Worker.nack cbs job
    Sent -> Worker.ack cbs job
```

> [!IMPORTANT]
> With `transactionalWorkerConfig` the bucket update rolls back with the
> retry. Manual and batched callbacks commit on their own.

**Policy-wide throttling.** Override the policy. Clear the override after
recovery:

```haskell
import Arbiter.RateLimit (Policy (..), clearRateLimit, setRateLimit)

import MyApp.Queue.Policies (transactional)

-- half the declared refill, burst and interval unchanged
void $ setRateLimit transactional {policyRefill = policyRefill transactional / 2}

-- back to what the code declares
void $ clearRateLimit transactional
```

Selector DSL and policy type: [`Arbiter.RateLimit` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-RateLimit.html).

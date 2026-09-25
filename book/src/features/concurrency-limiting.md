# Concurrency Limiting

A pool is a prefix and a default limit. It caps jobs in flight per key across
every queue in the registry. `HasConcurrency` selects a pool and key for each
job.

```haskell
import Arbiter.Concurrency (ConcurrencyPolicy, HasConcurrency (..), concurrencyBy, concurrencyPool)

-- Application function on the payload.
tenantOf :: SyncPayload -> Text

-- At most 2 sync jobs per tenant in flight at once.
syncPool :: ConcurrencyPolicy
syncPool = concurrencyPool "tenant-sync" 2

instance HasConcurrency SyncPayload where
  concurrencyFor = concurrencyBy syncPool tenantOf
```

Selectors: `noConcurrency`, `concurrencyBy`, `globalConcurrency`,
`concurrencyByCase`. An operator can override the limit from the API or admin
UI. Clearing the override restores the declared default. Limit 0 admits
nothing.

## Concurrency Limit 1 and Group Keys

Both admit one in-flight job per key.

| | `group_key` | concurrency limit 1 |
| --- | --- | --- |
| What it is | a scheduling primitive (ordered head per group) | a counter |
| On retry/backoff | the failing job **stays first**. The group waits until the job succeeds or moves to the DLQ | the failing job **releases its slot**. Another job runs during the backoff |
| Ordering | insertion order within priority | claim order only |
| Batching | one ordered batch per group | N independent jobs |

A job can use both.

> [!IMPORTANT]
> A job holds a slot from claim until ack, retry, nack, or reclaim. A handler
> timeout does not release it.
>
> The reaper prunes idle keys and rebuilds in-flight counts after a restart or
> failover.

## External Limit Updates

Set the override from a handler when a vendor reports new capacity:

```haskell
import Arbiter.Concurrency (setConcurrencyLimit)

import MyApp.Queue.Policies (syncPool)

syncHandler :: Arb.JobHandler (ArbS.SimpleDb SyncRegistry IO) SyncPayload ()
syncHandler _conn job = do
  outcome <- liftIO $ runSync (Arb.payload job)
  case outcome of
    CapacityChanged seats ->
      void $ setConcurrencyLimit syncPool seats
    Ok -> pure ()
```

`clearConcurrencyLimit syncPool` removes the override. In a transactional
handler the override commits with the ack.

Selector DSL and pool type: [`Arbiter.Concurrency` Haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Concurrency.html).

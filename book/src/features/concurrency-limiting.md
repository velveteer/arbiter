# Concurrency Limiting

Set a maximum number of concurrent jobs for each key. A `HasConcurrency`
instance specifies a **pool** and a key suffix for each job. A pool consists of
a prefix and a default limit. Keys apply to all queues in a registry.

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

Build the selector with `noConcurrency`, `concurrencyBy`, `globalConcurrency`,
or `concurrencyByCase`. The pool limit applies separately to each key with that
prefix. An operator can change the pool limit through the API or admin UI. The
override applies until an operator clears it. Arbiter then uses the declared
default. A value of 0 prevents claims for all keys in the pool.

## Concurrency Limit 1 and Group Keys

Both options permit one in-flight job for each key. Their failure behavior is
different:

| | `group_key` | concurrency limit 1 |
| --- | --- | --- |
| What it is | a scheduling primitive (ordered head per group) | a counter |
| On retry/backoff | the failing job **remains first**. The group waits until the job succeeds or moves to the DLQ | the failing job **releases its slot**. Another job can run during the backoff |
| Ordering | eligible jobs run in insertion order within priority | none beyond the claim's sort |
| Batching | claims an ordered batch per group | N independent jobs |

Use a **group key** for a serial sequence, such as an event stream or state
machine. Use **concurrency 1** as a mutex, such as one synchronization per
tenant. A job can use both features.

> [!IMPORTANT]
> The limit counts claims. A job occupies a slot until Arbiter acks, retries,
> nacks, or reclaims it. An unacked job continues to occupy a slot after its
> handler times out.
>
> Arbiter periodically removes inactive keys. It also reconstructs in-flight
> counts after a restart or failover.

## External Limit Updates

A handler can update an override in response to an external capacity signal.
For example, update the override when a vendor response reports a new limit.

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

The next claim uses the new limit. This change does not require a deployment or
restart. Code with a `MonadArbiter` instance can write the override.

`clearConcurrencyLimit syncPool` removes the override. Both functions accept a
declared pool and use its prefix.

The handler in this example is transactional. The override and job ack commit
in the same transaction. If the handler throws an exception, the transaction
rolls back both changes.

See the [`Arbiter.Concurrency` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Concurrency.html) for the selector DSL and the pool type.

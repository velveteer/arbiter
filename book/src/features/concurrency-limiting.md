# Concurrency Limiting

Cap how many jobs sharing a key run at once. A `HasConcurrency` instance names
a **pool** (a prefix with a default limit) and a per-job key suffix. Keys are
global, so a pool spans every queue in a registry.

```haskell
import Arbiter.Concurrency (ConcurrencyPolicy, HasConcurrency (..), concurrencyBy, concurrencyPool)

-- Your own function on the payload.
tenantOf :: SyncPayload -> Text

-- At most 2 sync jobs per tenant in flight at once.
syncPool :: ConcurrencyPolicy
syncPool = concurrencyPool "tenant-sync" 2

instance HasConcurrency SyncPayload where
  concurrencyFor = concurrencyBy syncPool tenantOf
```

Build the selector from `noConcurrency` / `concurrencyBy` / `globalConcurrency`
/ `concurrencyByCase`. The limit is the pool's, so every key under the prefix
shares one cap. An operator retunes a whole pool at runtime through the API or
admin UI. The override takes precedence until it is cleared, and the declared
default then applies again. An override of 0 pauses the pool: nothing under it
is claimed until the override is raised or cleared.

## Concurrency limit 1 vs. a group key

Both cap a key to one job in flight. They differ on failure:

| | `group_key` | concurrency limit 1 |
| --- | --- | --- |
| What it is | a scheduling primitive (ordered head per group) | a counter |
| On retry/backoff | the failing job **holds the head** - the group makes no progress until it succeeds or dead-letters | the failing job **releases its slot** - a sibling runs while it backs off |
| Ordering | eligible jobs run in insertion order within priority | none beyond the claim's sort |
| Batching | claims an ordered batch per group | N independent jobs |

Use a **group key** to serialize a sequence (event streams, state machines).
Use **concurrency 1** as a mutex (one sync per tenant). They are orthogonal, so
a job can carry both.

> [!IMPORTANT]
> The cap counts claims: a slot is held until its job is acked, retried, or
> reclaimed, so a timed-out-but-unacked job still occupies its slot until then.
>
> Maintenance is automatic. Drained keys are cleaned up periodically, and a
> pool's in-flight accounting recovers on its own after a restart or failover.

## Limits from an external signal

An override is a runtime value, and the cap can follow a signal from outside
your code. A handler that talks to the vendor usually sees that signal first.
Retune from the handler.

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

The next claim reads the new limit. The pool tightens or opens up with no
deploy and no restart, and any code with a `MonadArbiter` can write the
override.

`clearConcurrencyLimit syncPool` drops it again. Both helpers take the pool you
declared, which pins the prefix they write to the one the instance admits
against.

The handler above is transactional, so the write commits with the job's ack. If
it then throws, the override rolls back with the rest of the handler's work.

See the [`Arbiter.Concurrency` haddocks](https://arbiterq.dev/arbiter-core/Arbiter-Concurrency.html) for the selector DSL and the pool type.

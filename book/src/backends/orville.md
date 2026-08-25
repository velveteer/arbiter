# arbiter-orville (orville-postgresql)

Built on `orville-postgresql`. Orville manages connections and transactions
itself, so handlers receive no connection parameter. This backend needs a
custom monad with `MonadOrville` and `MonadArbiter` instances:

```haskell
{-# LANGUAGE TypeFamilies #-}

instance MonadArbiter AppM where
  type RegistryOf AppM = AppRegistry
  type Handler AppM job result = job -> AppM result
  getSchema = asks appSchema
  -- ... executeQuery / executeStatement / withDbTransaction / runHandlerWithConnection
```

`Handler` is the shape of your handlers - Orville passes no connection, so it
is just the job. `Arb.JobHandler AppM payload result` is that shape at one
queue's job type, and is what you write in a handler's own signature.
[Writing a Backend](custom.md) covers the methods elided above.

Because Orville does not expose its pooled connections for LISTEN/NOTIFY, the
shared listener runs on its own dedicated connection. Build a `DedicatedListen`
(from `Arbiter.Core.Listen`) with the same connection string as your Orville
pool, keep it in your reader environment, and return it from `getListener`.

`createOrvilleConnectionOptions` takes an arbiter `PoolConfig`, so
`poolConfigForWorkers` sizes your Orville pool for the workers that will use
it:

```haskell
import Arbiter.Core.Listen (DedicatedListen, dedicatedListener, newDedicatedListen)

data AppEnv = AppEnv
  { appSchema  :: SchemaName
  , appOrville :: O.OrvilleState
  , appListen  :: DedicatedListen
  }

main :: IO ()
main = do
  poolCfg <- Worker.poolConfigForWorkers workers
  orvillePool <- O.createConnectionPool (createOrvilleConnectionOptions connStr poolCfg)
  listen <- newDedicatedListen connStr
  let env =
        AppEnv
          { appSchema = "arbiter"
          , appOrville = O.newOrvilleState O.defaultErrorDetailLevel orvillePool
          , appListen = listen
          }
  runAppM env $ Worker.runWorkerPools workers

instance MonadArbiter AppM where
  -- ... RegistryOf / Handler / getSchema and the query methods, as above
  getListener = asks (Just . dedicatedListener . appListen)
```

See the [arbiter-orville haddocks](https://arbiterq.dev/arbiter-orville/Arbiter-Orville.html) for the connection options.

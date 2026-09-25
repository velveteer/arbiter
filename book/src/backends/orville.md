# arbiter-orville (orville-postgresql)

`orville-postgresql`. Orville owns its connections and transactions. Define a
monad with `MonadOrville` and `MonadArbiter` instances:

```haskell
{-# LANGUAGE TypeFamilies #-}

instance MonadArbiter AppM where
  type RegistryOf AppM = AppRegistry
  type Handler AppM job result = job -> AppM result
  getSchema = asks appSchema
  -- ... executeQuery / executeStatement / withDbTransaction / runHandlerWithConnection
```

Handler signatures use `Arb.JobHandler AppM payload result`.
[Writing a Backend](custom.md) covers the omitted methods.

For `LISTEN/NOTIFY`, build a `Listener` with `newLibPQListener` from
`arbiter-libpq` and the pool's connection string. Keep it in the reader
environment and return it from `getListener`.

Size the Orville pool with `poolConfigForWorkers`:

```haskell
import Arbiter.Core.Listen (Listener)
import Arbiter.LibPQ (newLibPQListener)

data AppEnv = AppEnv
  { appSchema  :: SchemaName
  , appOrville :: O.OrvilleState
  , appListen  :: Listener
  }

main :: IO ()
main = do
  poolCfg <- Worker.poolConfigForWorkers workers
  orvillePool <- O.createConnectionPool (createOrvilleConnectionOptions connStr poolCfg)
  listen <- newLibPQListener connStr
  let env =
        AppEnv
          { appSchema = "arbiter"
          , appOrville = O.newOrvilleState O.defaultErrorDetailLevel orvillePool
          , appListen = listen
          }
  runAppM env $ Worker.runWorkerPools workers

instance MonadArbiter AppM where
  -- ... RegistryOf / Handler / getSchema and the query methods, as above
  getListener = asks (Just . appListen)
```

Connection options: [arbiter-orville Haddocks](https://arbiterq.dev/arbiter-orville/Arbiter-Orville.html).

# arbiter-orville (orville-postgresql)

This backend uses `orville-postgresql`. Orville manages its connections and
transactions. Handlers do not receive a connection parameter. Define a
custom monad with `MonadOrville` and `MonadArbiter` instances:

```haskell
{-# LANGUAGE TypeFamilies #-}

instance MonadArbiter AppM where
  type RegistryOf AppM = AppRegistry
  type Handler AppM job result = job -> AppM result
  getSchema = asks appSchema
  -- ... executeQuery / executeStatement / withDbTransaction / runHandlerWithConnection
```

`Handler` specifies the handler type. Because Orville does not pass a
connection, this type contains the job argument only. Use
`Arb.JobHandler AppM payload result` in handler signatures.
[Writing a Backend](custom.md) covers the methods elided above.

Orville does not expose its pooled connections for `LISTEN/NOTIFY`. Create a
`DedicatedListen` from `Arbiter.Core.Listen` with the Orville pool connection
string. Store it in the reader environment and return it from `getListener`.

`createOrvilleConnectionOptions` accepts an Arbiter `PoolConfig`. Use
`poolConfigForWorkers` to calculate the Orville pool size:

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

# arbiter-orville (orville-postgresql)

`orville-postgresql`. Orville owns its connections and transactions.
`OrvilleDb` runs over the application's own `MonadOrville` monad, so Arbiter
queries use its connection and join its open transaction:

```haskell
O.withTransaction $ do
  O.insertEntity ordersTable order
  ArbO.runOrvilleDb @AppRegistry (ArbO.OrvilleEnv "arbiter" Nothing) $
    Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
```

Handlers run in the application monad, as `JobRead payload -> AppM result`.
A handler that inserts jobs wraps those calls in `runOrvilleDb` to share its
transaction.

For `LISTEN/NOTIFY`, build a `Listener` with `newLibPQListener` from
`arbiter-libpq` and the pool's connection string, and put it in the
`OrvilleEnv`. `Nothing` polls.

Size the Orville pool with `poolConfigForWorkers`:

```haskell
import Arbiter.LibPQ (newLibPQListener)
import Arbiter.Orville qualified as ArbO

main :: IO ()
main = do
  poolCfg <- Worker.poolConfigForWorkers workers
  orvillePool <- O.createConnectionPool (ArbO.createOrvilleConnectionOptions connStr poolCfg)
  listen <- newLibPQListener connStr
  let orvilleState = O.newOrvilleState O.defaultErrorDetailLevel orvillePool
      arbiterEnv = ArbO.OrvilleEnv {ArbO.schema = "arbiter", ArbO.listener = Just listen}
  runAppM orvilleState . ArbO.runOrvilleDb arbiterEnv $
    Worker.runWorkerPools workers
```

`AppM` needs `MonadOrville` and `MonadUnliftIO`. An application that wants
`AppM` itself to be the `MonadArbiter` instance can write one from the
primitives in `Arbiter.Orville.MonadArbiter`. [Writing a Backend](custom.md)
lists the members.

Connection options: [arbiter-orville Haddocks](https://arbiterq.dev/arbiter-orville/Arbiter-Orville.html).

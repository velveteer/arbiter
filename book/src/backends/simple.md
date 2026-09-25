# arbiter-simple (postgresql-simple)

`postgresql-simple` and `resource-pool`. Handlers receive a `Connection`.
Nested transactions are savepoints.

```haskell
env <- ArbS.createSimpleEnv (Proxy @AppRegistry) connStr "arbiter"
ArbS.runSimpleDb env $ Arb.insertJob (Arb.defaultJob $ SendWelcome "alice@example.com" "Alice")
```

Share a transaction with external database work:

```haskell
PG.withTransaction conn $ do
  PG.execute conn "INSERT INTO orders (id) VALUES (?)" (PG.Only orderId)
  ArbS.inTransaction @AppRegistry conn "arbiter" $
    Arb.insertJob (Arb.defaultJob (ProcessOrder orderId))
```

Environment and pool constructors: [arbiter-simple Haddocks](https://arbiterq.dev/arbiter-simple/Arbiter-Simple.html).
